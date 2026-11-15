import io

p = 'crates/sql/src/planner.rs'
src = io.open(p, encoding='utf-8').read()

# ---- 1. PlanContext.ctes ----
old = """pub struct PlanContext<'a> {
    pub batch_size: usize,
    pub catalog: &'a dyn CatalogStore,
    pub data: Arc<dyn QueryDataSource>,
}"""
new = """pub struct PlanContext<'a> {
    pub batch_size: usize,
    pub catalog: &'a dyn CatalogStore,
    pub data: Arc<dyn QueryDataSource>,
    /// 可见的 WITH CTE(名 → 体)。对子查询/派生表同样可见(Q15 形状);
    /// 非递归:CTE 体计划时只看到它之前的 CTE。
    pub ctes: &'a [(String, Query)],
}"""
assert old in src, "PlanContext"
src = src.replace(old, new)

# imports: HashMap + FilterExec
old = "use memory::Reservation;"
if "use memory::Reservation;" in src:
    pass  # planner 无此行则跳过
if "use std::collections::HashMap;" not in src:
    src = src.replace(
        "use sqlparser::ast::{",
        "use std::collections::HashMap;\nuse sqlparser::ast::{",
        1,
    )
src = src.replace(
    "use exec::operators::seq_scan::{QueryDataSource, SeqScanExec};",
    "use exec::operators::filter::FilterExec;\nuse exec::operators::seq_scan::{QueryDataSource, SeqScanExec};",
)

# ---- 2. plan_query WITH ----
old = """pub fn plan_query(q: &Query, ctx: &PlanContext<'_>) -> Result<PlannedQuery> {
    if q.with.is_some() {
        return Err(Error::not_supported("WITH clause"));
    }"""
new = """pub fn plan_query(q: &Query, ctx: &PlanContext<'_>) -> Result<PlannedQuery> {
    if q.fetch.is_some() {"""
assert old in src, "plan_query head"
src = src.replace(old, new)

# merged CTE list + pass to plan_select
old = """    let planned = plan_select(
        select,
        order_by,
        limit_value.map(|k| (k, offset_value != 0)),
        ctx,
    )?;
    finish_with_limit(planned, offset_value, limit_value)
}"""
new = """    // WITH:自身 CTE + 外层可见 CTE 合并(同名遮蔽:自身优先)
    let mut merged_ctes: Vec<(String, &Query)> = ctx.ctes.iter().cloned().collect();
    if let Some(with) = &q.with {
        if with.recursive {
            return Err(Error::not_supported("WITH RECURSIVE"));
        }
        for cte in &with.cte_tables {
            if !cte.alias.columns.is_empty() {
                return Err(Error::not_supported("WITH cte (col) AS ... (column list)"));
            }
            merged_ctes.insert(0, (cte.alias.name.value.clone(), &cte.query));
        }
    }
    let planned = plan_select(
        select,
        order_by,
        limit_value.map(|k| (k, offset_value != 0)),
        ctx,
        &merged_ctes,
    )?;
    finish_with_limit(planned, offset_value, limit_value)
}"""
assert old in src, "plan_query tail"
src = src.replace(old, new)

# ---- 3. plan_select signature + plan_from call + EmitCtx ----
old = """fn plan_select(
    select: &Select,
    order_by: &[sqlparser::ast::OrderByExpr],
    limit_hint: Option<(u64, bool)>, // (LIMIT k, 是否有 OFFSET)
    ctx: &PlanContext<'_>,
) -> Result<PlannedQuery> {"""
new = """fn plan_select(
    select: &Select,
    order_by: &[sqlparser::ast::OrderByExpr],
    limit_hint: Option<(u64, bool)>, // (LIMIT k, 是否有 OFFSET)
    ctx: &PlanContext<'_>,
    ctes: &[(String, &Query)],
) -> Result<PlannedQuery> {"""
assert old in src, "plan_select sig"
src = src.replace(old, new)

old = """    // FROM:多表 + JOIN 步骤(逗号连接视为 CROSS JOIN)→ 有序关系步骤
    let steps = match select.from.as_slice() {
        [] => Vec::new(),
        from => plan_from(from, ctx)?,
    };"""
new = """    // FROM:多表 + JOIN 步骤(逗号连接视为 CROSS JOIN)→ 有序关系步骤;
    // 派生表/CTE 的流水线按别名挂在 pipelines,扫描时接管
    let mut pipelines: HashMap<String, BoxedExecutor> = HashMap::new();
    let steps = match select.from.as_slice() {
        [] => Vec::new(),
        from => {
            let (s, p) = plan_from(from, ctx, ctes, &mut pipelines)?;
            pipelines = p;
            s
        }
    };"""
assert old in src, "plan_from call"
src = src.replace(old, new)

old = """            scope_ref,
            plan_ctx: ctx,
        };"""
new = """            scope_ref,
            plan_ctx: ctx,
            pipelines: &mut pipelines,
        };"""
assert old in src, "EmitCtx"
src = src.replace(old, new)

old = """    scope_ref: Option<&'a Scope>,
    plan_ctx: &'a PlanContext<'a>,
}"""
new = """    scope_ref: Option<&'a Scope>,
    plan_ctx: &'a PlanContext<'a>,
    /// 派生表/CTE 的流水线(别名 → 执行器);扫描该表时 take 接管
    pipelines: &'a mut HashMap<String, BoxedExecutor>,
}"""
assert old in src, "EmitCtx struct"
src = src.replace(old, new)

# ---- 4. plan_from + push_relation ----
old = """/// FROM 子句解析:TableWithJoins 列表展开为有序步骤;逗号连接 = 无 ON 的内连接。
fn plan_from(from: &[TableWithJoins], ctx: &PlanContext<'_>) -> Result<Vec<RelationStep>> {
    fn push_relation(
        relation: &TableFactor,
        ctx: &PlanContext<'_>,
        steps: &mut Vec<RelationStep>,
    ) -> Result<()> {
        let TableFactor::Table { name, alias, .. } = relation else {
            return Err(Error::not_supported("FROM clause (subquery / function)"));
        };
        let tname = object_name_to_string(name)?;
        let schema = resolve_table(ctx.catalog, &tname)?;
        let alias = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        if steps.iter().any(|s| s.alias.eq_ignore_ascii_case(&alias)) {
            return Err(Error::sql(format!(
                "duplicate table alias '{alias}' in FROM clause"
            )));
        }
        steps.push(RelationStep {
            alias,
            schema,
            join_type: JoinType::Inner,
            on: None,
        });
        Ok(())
    }

    let mut steps: Vec<RelationStep> = Vec::new();
    for twj in from {
        // 第一个 TableWithJoins 的主关系是驱动表;后续的是逗号连接(CROSS JOIN)
        push_relation(&twj.relation, ctx, &mut steps)?;
        for join in &twj.joins {
            let (jt, on) = match &join.join_operator {
                // `JOIN` 与 `INNER JOIN` 同义(sqlparser 分列两个变体)
                JoinOperator::Join(c) | JoinOperator::Inner(c) => {
                    let JoinConstraint::On(e) = c else {
                        return Err(Error::not_supported("JOIN ... USING / NATURAL JOIN"));
                    };
                    (JoinType::Inner, Some(e.clone()))
                }
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => {
                    let JoinConstraint::On(e) = c else {
                        return Err(Error::not_supported("JOIN ... USING / NATURAL JOIN"));
                    };
                    (JoinType::LeftOuter, Some(e.clone()))
                }
                JoinOperator::CrossJoin(_) => (JoinType::Inner, None),
                other => {
                    return Err(Error::not_supported(format!("join type {other:?}")));
                }
            };
            push_relation(&join.relation, ctx, &mut steps)?;
            let j = steps.len() - 1;
            steps[j].join_type = jt;
            steps[j].on = on;
        }
    }
    Ok(steps)
}"""
new = """/// FROM 子句解析:TableWithJoins 列表展开为有序步骤;逗号连接 = 无 ON 的内连接。
/// 返回 (步骤, 派生表/CTE 流水线表)。派生表与 CTE 在计划期就地规划,
/// 流水线即数据源(别名 → 执行器);物理表走 catalog。
fn plan_from(
    from: &[TableWithJoins],
    ctx: &PlanContext<'_>,
    ctes: &[(String, &Query)],
    pipelines: &mut HashMap<String, BoxedExecutor>,
) -> Result<Vec<RelationStep>> {
    fn push_relation(
        relation: &TableFactor,
        ctx: &PlanContext<'_>,
        ctes: &[(String, &Query)],
        pipelines: &mut HashMap<String, BoxedExecutor>,
        steps: &mut Vec<RelationStep>,
    ) -> Result<()> {
        let (name, alias_opt, subquery) = match relation {
            TableFactor::Table { name, alias, .. } => {
                (Some(name), alias.as_ref().map(|a| a.name.value.clone()), None)
            }
            TableFactor::Derived { subquery, alias, .. } => {
                (None, alias.as_ref().map(|a| a.name.value.clone()), Some(subquery))
            }
            other => {
                return Err(Error::not_supported(format!("FROM clause: {other}")));
            }
        };
        let (schema, alias, pipeline) = match subquery {
            Some(q) => {
                let pq = plan_query(q, ctx)?;
                let alias =
                    alias_opt.unwrap_or_else(|| "subquery".to_string());
                let schema = TableSchema {
                    id: 0, // 无存储:扫描由流水线接管
                    name: alias.clone(),
                    columns: pq.schema.clone(),
                    primary_key: vec![],
                    version: 1,
                    indexes: vec![],
                };
                (schema, alias, Some(pq.executor))
            }
            None => {
                let tname = object_name_to_string(name.as_ref().expect("table name"))?;
                // CTE 优先于物理表;体计划只见它之前的 CTE(非递归)
                let cte_pos = ctes
                    .iter()
                    .position(|(n, _)| n.eq_ignore_ascii_case(&tname));
                if let Some(pos) = cte_pos {
                    let (cname, cte_q) = &ctes[pos];
                    let _ = cname;
                    let sub_ctx = PlanContext {
                        batch_size: ctx.batch_size,
                        catalog: ctx.catalog,
                        data: ctx.data.clone(),
                        ctes: &ctes[..pos],
                    };
                    let pq = plan_query(cte_q, &sub_ctx)?;
                    let alias = alias_opt.unwrap_or_else(|| tname.clone());
                    let schema = TableSchema {
                        id: 0,
                        name: alias.clone(),
                        columns: pq.schema.clone(),
                        primary_key: vec![],
                        version: 1,
                        indexes: vec![],
                    };
                    (schema, alias, Some(pq.executor))
                } else {
                    let schema = resolve_table(ctx.catalog, &tname)?;
                    let alias =
                        alias_opt.unwrap_or_else(|| tname.clone());
                    (schema, alias, None)
                }
            }
        };
        if steps.iter().any(|s| s.alias.eq_ignore_ascii_case(&alias)) {
            return Err(Error::sql(format!(
                "duplicate table alias '{alias}' in FROM clause"
            )));
        }
        if let Some(p) = pipeline {
            pipelines.insert(alias.clone(), p);
        }
        steps.push(RelationStep {
            alias,
            schema,
            join_type: JoinType::Inner,
            on: None,
        });
        Ok(())
    }

    let mut steps: Vec<RelationStep> = Vec::new();
    for twj in from {
        // 第一个 TableWithJoins 的主关系是驱动表;后续的是逗号连接(CROSS JOIN)
        push_relation(&twj.relation, ctx, ctes, pipelines, &mut steps)?;
        for join in &twj.joins {
            let (jt, on) = match &join.join_operator {
                // `JOIN` 与 `INNER JOIN` 同义(sqlparser 分列两个变体)
                JoinOperator::Join(c) | JoinOperator::Inner(c) => {
                    let JoinConstraint::On(e) = c else {
                        return Err(Error::not_supported("JOIN ... USING / NATURAL JOIN"));
                    };
                    (JoinType::Inner, Some(e.clone()))
                }
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => {
                    let JoinConstraint::On(e) = c else {
                        return Err(Error::not_supported("JOIN ... USING / NATURAL JOIN"));
                    };
                    (JoinType::LeftOuter, Some(e.clone()))
                }
                JoinOperator::CrossJoin(_) => (JoinType::Inner, None),
                other => {
                    return Err(Error::not_supported(format!("join type {other:?}")));
                }
            };
            push_relation(&join.relation, ctx, ctes, pipelines, &mut steps)?;
            let j = steps.len() - 1;
            steps[j].join_type = jt;
            steps[j].on = on;
        }
    }
    Ok(steps)
}"""
assert old in src, "plan_from"
src = src.replace(old, new)

# ---- 5. table_scan:derived path + signature ----
old = """fn table_scan(
    step: &RelationStep,
    preds: &[Expr],
    offset: usize,
    needed: &[bool],
    ctx: &PlanContext<'_>,
) -> Result<(BoxedExecutor, Option<String>)> {
    // 谓词下推:随解码过滤(列存路径则在列块上求值),不再叠加 FilterExec
    let rebased: Vec<Expr> = preds
        .iter()
        .map(|p| rebase_columns(p.clone(), offset))
        .collect::<Result<Vec<_>>>()?;"""
new = """fn table_scan(
    step: &RelationStep,
    preds: &[Expr],
    offset: usize,
    needed: &[bool],
    ctx: &PlanContext<'_>,
    pipelines: &mut HashMap<String, BoxedExecutor>,
) -> Result<(BoxedExecutor, Option<String>)> {
    // 谓词下推:随解码过滤(列存路径则在列块上求值),不再叠加 FilterExec
    let rebased: Vec<Expr> = preds
        .iter()
        .map(|p| rebase_columns(p.clone(), offset))
        .collect::<Result<Vec<_>>>()?;
    // 派生表/CTE:流水线即数据源(其内已含全部算子);谓词不可下推 → Filter
    if let Some(pipeline) = pipelines.remove(&step.alias) {
        let exec: BoxedExecutor = match combine_and(rebased) {
            Some(pred) => Box::new(FilterExec::new(pipeline, pred)),
            None => pipeline,
        };
        return Ok((exec, None));
    }"""
assert old in src, "table_scan"
src = src.replace(old, new)

for site in [
    """            let (exec, idx_label) = table_scan(
                &em.steps[node],
                &em.scan_preds[node],
                em.offsets[node],
                &em.needed[node],
                em.plan_ctx,
            )?;""",
    """        let (probe, probe_label) = table_scan(
            &em.steps[t],
            &em.scan_preds[t],
            t_offset,
            &em.needed[t],
            em.plan_ctx,
        )?;""",
    """        let (build, build_label) = table_scan(
            &em.steps[t],
            &em.scan_preds[t],
            t_offset,
            &em.needed[t],
            em.plan_ctx,
        )?;""",
    """    let (build, _build_label) =
        table_scan(&em.steps[k], &[], em.offsets[k], &em.needed[k], em.plan_ctx)?;""",
]:
    assert site in src, f"call site: {site[:60]}"
    new_site = site.replace("em.plan_ctx)?;", "em.plan_ctx, em.pipelines)?;").replace(
        "em.plan_ctx, &em.needed[k], em.plan_ctx)?;",
        "em.plan_ctx, &em.needed[k], em.plan_ctx, em.pipelines)?;",
    )
    src = src.replace(site, new_site)

# ---- 6. constructors: ctes field ----
src = src.replace(
    """        PlanContext {
            batch_size: ctx.batch_size,
            catalog: ctx.catalog,
            data: ctx.data.clone(),
            ctes: &ctes[..pos],
        }""",
    """        PlanContext {
            batch_size: ctx.batch_size,
            catalog: ctx.catalog,
            data: ctx.data.clone(),
            ctes: &ctes[..pos],
        }""",
)

io.open(p, 'w', encoding='utf-8', newline='').write(src)
print('planner patched')
