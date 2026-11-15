//! 相关子查询去相关。
//!
//! WHERE 中的相关子查询从表达式树上摘下,改写为流水线上的连接:
//! - `EXISTS` / `IN (SELECT ...)` → 半连接(`HashSemiJoinExec`;NOT EXISTS → 反半连接);
//!   等值相关谓词成哈希键,其余相关谓词成拼接块上的匹配期条件。
//! - 等值相关的标量聚合子查询(`x < (SELECT AVG(y) FROM t WHERE t.k = outer.k)`)
//!   → 分组拉升(相关等值的内表侧全部进 GROUP BY)+ 左外连接,值列挂合成
//!   `__sq` 表;外层比较谓词改写为对该列的引用,连接包装后过滤。
//!
//! 提升(lifting)的正确性:所有相关等值都成为分组键 ⇒ 每组恰好对应一个
//! 外层相关绑定,组内聚合值 = 原子查询对该绑定返回的值;MIN/MAX/SUM/AVG
//! 在空集上返回 NULL,与"提升后无匹配组 → LEFT JOIN 补 NULL"一致。
//! COUNT 例外(空集 0 vs NULL),明确拒绝。

use std::collections::{BTreeSet, HashMap};

use catalog::CatalogStore;
use exec::expr::{BinaryOp, Expr};
use exec::executor::BoxedExecutor;
use common::error::{Error, Result};
use common::schema::ColumnDesc;
use sqlparser::ast::{
    BinaryOperator, Expr as SE, GroupByExpr, Ident, InTarget, Query, Select, SelectItem,
    SetExpr, TableFactor, TableWithJoins,
};

use crate::optimizer::{collect_sql_columns, split_conjuncts};
use crate::planner::{plan_expr, plan_query, resolve_table, object_name_to_string, PlanContext, Scope};

/// 连接包装的类型。
pub enum SqKind {
    /// EXISTS / IN:半连接(anti = NOT EXISTS;相关 NOT IN 不支持,三值语义不同)
    Semi { anti: bool },
    /// 等值相关标量聚合:分组拉升 + 左外连接(构建侧第 0 列 = 值列)
    ScalarLift,
}

/// 一个摘下的相关子查询 → 连接包装。
pub struct SqWrap {
    pub kind: SqKind,
    pub build: BoxedExecutor,
    pub build_schema: Vec<ColumnDesc>,
    /// 探测侧键表达式(外层流水线 schema 下标)
    pub probe_keys: Vec<Expr>,
    /// 构建侧键表达式(构建输出 schema 下标)
    pub build_keys: Vec<Expr>,
    /// 半连接匹配期条件(拼接块:探测列 ++ 构建列)
    pub cond: Option<Expr>,
}

/// WHERE 相关子查询的预处理产物。
pub struct SqPlan {
    pub wraps: Vec<SqWrap>,
    /// 标量提升值列(合成 __sq 表,按 slot 序追加在真实表之后)
    pub synthetic: Vec<ColumnDesc>,
    /// 改写后的 WHERE 合取项(EXISTS/IN 已消费移除;标量比较换成 __sq 列引用;
    /// 子查询内收割的外层独占谓词排最前,优先参与扫描下推)
    pub route_conjuncts: Vec<SE>,
    /// 引用收集用(探测键 + 匹配条件),保证扫描投影不剪掉连接需要的列
    pub ref_exprs: Vec<Expr>,
}

/// WHERE 中相关子查询的识别与提升。`probe_width` = 外层真实流水线宽度。
pub fn plan_where_subqueries(
    where_expr: Option<&SE>,
    outer_scope: &Scope,
    ctx: &PlanContext<'_>,
    probe_width: usize,
) -> Result<SqPlan> {
    let mut plan = SqPlan {
        wraps: Vec::new(),
        synthetic: Vec::new(),
        route_conjuncts: Vec::new(),
        ref_exprs: Vec::new(),
    };
    let Some(w) = where_expr else {
        return Ok(plan);
    };
    let outer_sql: BTreeSet<String> =
        split_conjuncts(w).iter().map(|c| format!("{c}")).collect();
    for conj in split_conjuncts(w) {
        let consumed = match conj {
            SE::Exists { subquery, negated } => build_semi(
                subquery, *negated, None, outer_scope, ctx, probe_width, &mut plan,
            )?,
            SE::In {
                expr,
                target: InTarget::Subquery(q),
                negated,
                ..
            } => {
                if *negated {
                    // 相关 NOT IN 的三值语义(空/含 NULL 集合)与反半连接不同,
                    // 相关场景明确拒绝;非相关走原计划期求值路径
                    if is_correlated(q, outer_scope, ctx)? {
                        return Err(Error::not_supported(
                            "correlated NOT IN (subquery); rewrite as NOT EXISTS",
                        ));
                    }
                    false
                } else {
                    build_semi(
                        q, false, Some(expr), outer_scope, ctx, probe_width, &mut plan,
                    )?
                }
            }
            _ => try_scalar_lift(conj, outer_scope, ctx, probe_width, &outer_sql, &mut plan)?,
        };
        if !consumed {
            plan.route_conjuncts.push(conj.clone());
        }
    }
    Ok(plan)
}

/// 子查询是否引用外层表(存在任一合取项/表达式命中外层别名)。
fn is_correlated(q: &Query, outer_scope: &Scope, ctx: &PlanContext<'_>) -> Result<bool> {
    let Some(select) = body_select(q) else {
        return Ok(false);
    };
    let Some(selection) = &select.selection else {
        return Ok(false);
    };
    let (inner, _) = build_scopes(&select.from, ctx.catalog, outer_scope)?;
    let mut ti = false;
    let mut to = false;
    for c in split_conjuncts(selection) {
        classify_tables(c, &inner, outer_scope, &mut ti, &mut to)?;
    }
    Ok(to)
}

/// EXISTS / IN → 半连接。返回 true = 已消费。
#[allow(clippy::too_many_arguments)]
fn build_semi(
    q: &Query,
    negated: bool,
    in_lhs: Option<&SE>,
    outer_scope: &Scope,
    ctx: &PlanContext<'_>,
    probe_width: usize,
    plan: &mut SqPlan,
) -> Result<bool> {
    let Some(select) = body_select(q) else {
        return Err(Error::not_supported(
            "correlated subquery must be a plain SELECT",
        ));
    };
    let Some(a) = analyze(select, q, outer_scope, ctx)? else {
        return Ok(false); // 非相关 → 原计划期求值路径
    };
    let (wrap, _, _) = finish_decorrelated(
        q, a, outer_scope, ctx, probe_width, SqKind::Semi { anti: negated }, in_lhs,
    )?;
    plan.wraps.push(wrap);
    Ok(true)
}

/// `lhs <op> (SELECT ...)` 形状的标量提升。返回 true = 已消费。
fn try_scalar_lift(
    conj: &SE,
    outer_scope: &Scope,
    ctx: &PlanContext<'_>,
    probe_width: usize,
    outer_sql: &BTreeSet<String>,
    plan: &mut SqPlan,
) -> Result<bool> {
    let SE::BinaryOp { op, left, right } = conj else {
        return Ok(false);
    };
    if !matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    ) {
        return Ok(false);
    }
    let subq = if let SE::Subquery(q) = left.as_ref() {
        q
    } else if let SE::Subquery(q) = right.as_ref() {
        q
    } else {
        return Ok(false);
    };
    let Some(select) = body_select(subq) else {
        return Err(Error::not_supported(
            "correlated scalar subquery must be a plain SELECT",
        ));
    };
    let Some(a) = analyze(select, subq, outer_scope, ctx)? else {
        return Ok(false); // 非相关 → 原计划期求值路径
    };
    // 标量提升约束:相关谓词全为等值;无 COUNT(空组语义 0 vs NULL)
    if !a.residual.is_empty() {
        return Err(Error::not_supported(
            "correlated scalar subquery with non-equality correlation",
        ));
    }
    for item in &select.projection {
        let e = match item {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        if contains_aggregate(e, "count") {
            return Err(Error::not_supported(
                "COUNT in correlated scalar subquery (empty-group semantics differ); use MIN/MAX/SUM/AVG",
            ));
        }
    }
    let slot = plan.synthetic.len();
    let (wrap, outer_only, value_type_opt) = finish_decorrelated(
        subq,
        a,
        outer_scope,
        ctx,
        probe_width,
        SqKind::ScalarLift,
        None,
    )?;
    let value_type = value_type_opt.ok_or_else(|| Error::sql("scalar subquery produced no value"))?;
    plan.synthetic.push(ColumnDesc::new(
        format!("__sq_value_{slot}"),
        value_type,
        true,
    ));
    // 子查询内收割的外层独占谓词(与外层 WHERE 重复的跳过)
    for se in &outer_only {
        let disp = format!("{se}");
        if !outer_sql.contains(&disp) {
            plan.route_conjuncts.insert(0, se.clone());
        }
    }
    // 比较谓词改写:子查询 → __sq 值列引用(连接包装后过滤)
    let replaced =
        replace_scalar_subquery(conj, &format!("__sq.__sq_value_{slot}"));
    plan.route_conjuncts.push(replaced);
    plan.wraps.push(wrap);
    Ok(true)
}

/// 分析 + 改写 + 计划改写后的子查询,产出连接包装。
#[allow(clippy::too_many_arguments)]
fn finish_decorrelated(
    q: &Query,
    a: Analysis,
    outer_scope: &Scope,
    ctx: &PlanContext<'_>,
    probe_width: usize,
    kind: SqKind,
    in_lhs: Option<&SE>,
) -> Result<(SqWrap, Vec<SE>, Option<common::types::DataType>)> {
    let Analysis {
        inner_scope,
        combined,
        inner_width,
        equi,
        residual,
        local,
        outer_only,
    } = a;

    // 连接条件引用到的内表列(扁平内下标)→ 追加进子查询输出
    let mut refs: BTreeSet<usize> = BTreeSet::new();
    for (iside, _) in &equi {
        collect_inner_cols(iside, &inner_scope, &mut refs)?;
    }
    for r in &residual {
        collect_inner_cols_mixed(r, &combined, inner_width, &mut refs)?;
    }
    let wildcard = select_wildcard(q);
    // 追加项 AST(按内下标升序 → rank 即追加序)
    let appended: Vec<SE> = if wildcard {
        Vec::new()
    } else {
        refs.iter()
            .map(|&flat| column_se(&inner_scope, flat))
            .collect::<Result<Vec<_>>>()?
    };
    let rank: HashMap<usize, usize> =
        refs.iter().enumerate().map(|(r, &flat)| (flat, r)).collect();

    // 改写 AST:WHERE 只留内表局部合取项;标量提升注入 GROUP BY(相关等值内表侧)
    let mut q2 = q.clone();
    if let SetExpr::Select(sel) = q2.body.as_mut() {
        sel.selection = and_all(local.iter().cloned());
        if !appended.is_empty() {
            sel.projection
                .extend(appended.iter().map(|e| SelectItem::UnnamedExpr(e.clone())));
        }
        if matches!(kind, SqKind::ScalarLift) {
            sel.group_by = GroupByExpr::Expressions(
                equi.iter().map(|(is, _)| is.clone()).collect(),
                Vec::new(),
            );
        }
    }
    q2.order_by = None;
    q2.limit_clause = None;
    q2.fetch = None;

    let planned = plan_query(&q2, ctx)?;
    let build_out = planned.schema.len();
    let k = appended.len();
    if matches!(kind, SqKind::ScalarLift) && build_out - k != 1 {
        return Err(Error::sql(format!(
            "scalar subquery must return exactly one column, got {}",
            build_out - k
        )));
    }
    if matches!(kind, SqKind::Semi { .. })
        && in_lhs.is_some()
        && build_out - k != 1
    {
        return Err(Error::sql(format!(
            "IN subquery must return exactly one column, got {}",
            build_out - k
        )));
    }
    // 输出列 → 构建下标:wildcard 展开 = 内表列自然序;否则追加项在尾部
    let base = build_out - k;
    let to_build = move |flat: usize| -> usize {
        if wildcard { flat } else { base + rank[&flat] }
    };

    let mut probe_keys: Vec<Expr> = Vec::new();
    let mut build_keys: Vec<Expr> = Vec::new();
    if let Some(lhs) = in_lhs {
        probe_keys.push(plan_expr(lhs, Some(outer_scope), ctx)?);
        build_keys.push(Expr::Column(0));
    }
    for (iside, oside) in &equi {
        let bk = plan_expr(iside, Some(&inner_scope), ctx)?;
        build_keys.push(remap_build(bk, &to_build));
        probe_keys.push(plan_expr(oside, Some(outer_scope), ctx)?);
    }
    let mut cond: Option<Expr> = None;
    for r in &residual {
        let e = plan_expr(r, Some(&combined), ctx)?;
        let e = remap_cond(e, probe_width, &to_build, inner_width);
        cond = Some(match cond {
            None => e,
            Some(c) => Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(c),
                right: Box::new(e),
            },
        });
    }
    let value_type =
        matches!(kind, SqKind::ScalarLift).then(|| planned.schema[0].ty.clone());
    Ok((
        SqWrap {
            kind,
            build: planned.executor,
            build_schema: planned.schema,
            probe_keys,
            build_keys,
            cond,
        },
        outer_only,
        value_type,
    ))
}

// ---------- 子查询形状分析 ----------

struct Analysis {
    inner_scope: Scope,
    /// 内表优先、外表随后的组合 scope(残余相关谓词的计划上下文;
    /// 内表优先 = 非限定列按 SQL 作用域规则先命中内层)
    combined: Scope,
    inner_width: usize,
    /// 相关等值(内表侧, 外表侧)
    equi: Vec<(SE, SE)>,
    /// 相关非等值合取项(半连接匹配期条件)
    residual: Vec<SE>,
    /// 内表局部合取项(留在子查询 WHERE)
    local: Vec<SE>,
    /// 外表独占合取项(等价于外层过滤,收割到外层)
    outer_only: Vec<SE>,
}

/// 从 FROM 构建内层 scope(仅物理表)与组合 scope(内表优先,外表随后)。
fn build_scopes(
    from: &[TableWithJoins],
    catalog: &dyn CatalogStore,
    outer_scope: &Scope,
) -> Result<(Scope, Scope)> {
    let mut inner = Scope::empty();
    let mut push_one = |relation: &TableFactor| -> Result<()> {
        let TableFactor::Table { name, alias, .. } = relation else {
            return Err(Error::not_supported(
                "correlated subquery FROM must be physical tables",
            ));
        };
        let tname = object_name_to_string(name)?;
        let schema = resolve_table(catalog, &tname)?;
        let alias = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        inner.push(alias, &schema);
        Ok(())
    };
    for twj in from {
        push_one(&twj.relation)?;
        for j in &twj.joins {
            push_one(&j.relation)?;
        }
    }
    if inner.tables.is_empty() {
        return Err(Error::sql("correlated subquery has no FROM"));
    }
    let mut combined = Scope::empty();
    for (alias, t) in &inner.tables {
        combined.push(alias.clone(), t);
    }
    for (alias, t) in &outer_scope.tables {
        combined.push(alias.clone(), t);
    }
    Ok((inner, combined))
}

/// 子查询 WHERE 分析;非相关返回 None。
fn analyze(
    select: &Select,
    q: &Query,
    outer_scope: &Scope,
    ctx: &PlanContext<'_>,
) -> Result<Option<Analysis>> {
    // FROM 含派生表/CTE(非物理表)时无法构建内层 scope → 交还原路径
    // (非相关子查询的计划期求值支持 CTE;相关报错由其自身规划给出)
    let (inner_scope, combined) = match build_scopes(&select.from, ctx.catalog, outer_scope) {
        Ok(sc) => sc,
        Err(_) => return Ok(None),
    };
    let inner_width: usize = inner_scope.table_widths().iter().sum();
    let Some(selection) = &select.selection else {
        return Ok(None); // 无 WHERE 必然非相关
    };
    let mut equi: Vec<(SE, SE)> = Vec::new();
    let mut residual: Vec<SE> = Vec::new();
    let mut local: Vec<SE> = Vec::new();
    let mut outer_only: Vec<SE> = Vec::new();
    let mut correlated = false;
    for c in split_conjuncts(selection) {
        let mut ti = false;
        let mut to = false;
        classify_tables(c, &inner_scope, outer_scope, &mut ti, &mut to)?;
        match (ti, to) {
            (false, false) => unreachable!("classify 至少命中一侧或报错"),
            (true, false) => local.push(c.clone()),
            (false, true) => {
                correlated = true;
                outer_only.push(c.clone());
            }
            (true, true) => {
                correlated = true;
                // 相关等值:一侧内表、一侧外表
                if let SE::BinaryOp {
                    op: BinaryOperator::Eq,
                    left,
                    right,
                } = c
                {
                    let mut li = false;
                    let mut lo = false;
                    classify_tables(left, &inner_scope, outer_scope, &mut li, &mut lo)?;
                    let mut ri = false;
                    let mut ro = false;
                    classify_tables(right, &inner_scope, outer_scope, &mut ri, &mut ro)?;
                    if li && ro && !lo && !ri {
                        // 左内右外:内表侧 = left,外表侧 = right
                        equi.push((left.as_ref().clone(), right.as_ref().clone()));
                        continue;
                    }
                    if ri && lo && !ro && !li {
                        // 右内左外:内表侧 = right,外表侧 = left
                        equi.push((right.as_ref().clone(), left.as_ref().clone()));
                        continue;
                    }
                }
                residual.push(c.clone());
            }
        }
    }
    if !correlated {
        return Ok(None); // 非相关:原计划期求值路径(GROUP BY/HAVING 等形状不受限)
    }
    // 以下形状仅相关子查询不支持(标量提升注入自己的 GROUP BY)
    if select.group_by != GroupByExpr::Expressions(Vec::new(), Vec::new()) {
        return Err(Error::not_supported("correlated subquery with GROUP BY"));
    }
    if select.having.is_some() {
        return Err(Error::not_supported("correlated subquery with HAVING"));
    }
    if select.distinct.is_some() {
        return Err(Error::not_supported("correlated subquery with DISTINCT"));
    }
    if q.limit_clause.is_some() || q.fetch.is_some() {
        return Err(Error::not_supported("correlated subquery with LIMIT/OFFSET/FETCH"));
    }
    Ok(Some(Analysis {
        inner_scope,
        combined,
        inner_width,
        equi,
        residual,
        local,
        outer_only,
    }))
}

/// 标识符分类:内层优先(SQL 作用域规则)。深层子查询内容不分类。
fn classify_tables(
    e: &SE,
    inner: &Scope,
    outer: &Scope,
    ti: &mut bool,
    to: &mut bool,
) -> Result<()> {
    match e {
        SE::Identifier(id) => {
            let name = &id.value;
            let mut hits = 0;
            for (_, t) in &inner.tables {
                if t.column_index(name).is_some() {
                    hits += 1;
                }
            }
            if hits == 1 {
                *ti = true;
                return Ok(());
            }
            if hits > 1 {
                return Err(Error::catalog(format!(
                    "column reference '{name}' is ambiguous"
                )));
            }
            let mut ohits = 0;
            for (_, t) in &outer.tables {
                if t.column_index(name).is_some() {
                    ohits += 1;
                }
            }
            if ohits >= 1 {
                *to = true;
                return Ok(());
            }
            Err(Error::catalog(format!("column '{name}' not found")))
        }
        SE::CompoundIdentifier(parts) => {
            let [q, c] = parts.as_slice() else {
                return Err(Error::not_supported("qualified identifier (>2 parts)"));
            };
            if inner.resolve_qualifier(&q.value).is_some() {
                if inner.resolve(Some(&q.value), &c.value).is_ok() {
                    *ti = true;
                    return Ok(());
                }
                return Err(Error::catalog(format!(
                    "column '{}.{}' not found",
                    q.value, c.value
                )));
            }
            if outer.resolve_qualifier(&q.value).is_some() {
                if outer.resolve(Some(&q.value), &c.value).is_ok() {
                    *to = true;
                    return Ok(());
                }
                return Err(Error::catalog(format!(
                    "column '{}.{}' not found",
                    q.value, c.value
                )));
            }
            Err(Error::catalog(format!("unknown table alias '{}'", q.value)))
        }
        SE::BinaryOp { left, right, .. } => {
            classify_tables(left, inner, outer, ti, to)?;
            classify_tables(right, inner, outer, ti, to)
        }
        SE::Nested(inner_e) => classify_tables(inner_e, inner, outer, ti, to),
        SE::UnaryOp { expr, .. } => classify_tables(expr, inner, outer, ti, to),
        SE::Function(f) => {
            for a in crate::planner::function_call_args(f)? {
                classify_tables(&a, inner, outer, ti, to)?;
            }
            Ok(())
        }
        SE::Like { expr, pattern, .. } => {
            classify_tables(expr, inner, outer, ti, to)?;
            classify_tables(pattern, inner, outer, ti, to)
        }
        SE::Extract { expr, .. } => classify_tables(expr, inner, outer, ti, to),
        SE::Between { expr, low, high, .. } => {
            classify_tables(expr, inner, outer, ti, to)?;
            classify_tables(low, inner, outer, ti, to)?;
            classify_tables(high, inner, outer, ti, to)
        }
        SE::In { expr, target, .. } => {
            classify_tables(expr, inner, outer, ti, to)?;
            if let InTarget::List(list) = target {
                for item in list {
                    classify_tables(item, inner, outer, ti, to)?;
                }
            }
            Ok(()) // InTarget::Subquery:深层子查询不分类(其相关性由自身规划处理)
        }
        SE::IsNull(expr) | SE::IsNotNull(expr) => classify_tables(expr, inner, outer, ti, to),
        SE::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                classify_tables(op, inner, outer, ti, to)?;
            }
            for cw in conditions {
                classify_tables(&cw.condition, inner, outer, ti, to)?;
                classify_tables(&cw.result, inner, outer, ti, to)?;
            }
            if let Some(er) = else_result {
                classify_tables(er, inner, outer, ti, to)?;
            }
            Ok(())
        }
        SE::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            classify_tables(expr, inner, outer, ti, to)?;
            if let Some(f) = substring_from {
                classify_tables(f, inner, outer, ti, to)?;
            }
            if let Some(f) = substring_for {
                classify_tables(f, inner, outer, ti, to)?;
            }
            Ok(())
        }
        SE::Value(_) | SE::TypedString { .. } => Ok(()),
        // 深层子查询:其标识符属于更内层作用域,不参与本层分类
        // (其自身相关性由子查询规划递归处理)
        SE::Subquery(_) | SE::Exists { .. } => Ok(()),
        other => Err(Error::not_supported(format!(
            "correlated subquery predicate: {other}"
        ))),
    }
}

// ---------- AST 工具 ----------

fn body_select(q: &Query) -> Option<&Select> {
    match q.body.as_ref() {
        SetExpr::Select(s) => Some(s),
        _ => None,
    }
}

fn and_all(conjuncts: impl Iterator<Item = SE>) -> Option<SE> {
    conjuncts.reduce(|l, r| SE::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(l),
        right: Box::new(r),
    })
}

/// SELECT 列表是否含无限定 `*`(全部内表列已在输出,自然序)。
fn select_wildcard(q: &Query) -> bool {
    body_select(q).is_some_and(|s| {
        s.projection
            .iter()
            .any(|i| matches!(i, SelectItem::Wildcard(_)))
    })
}

/// 比较谓词中的标量子查询 → 合成列引用。
fn replace_scalar_subquery(e: &SE, synthetic_ref: &str) -> SE {
    fn ident_path(path: &str) -> Vec<Ident> {
        path.split('.').map(Ident::new).collect()
    }
    match e {
        SE::Subquery(_) => SE::CompoundIdentifier(ident_path(synthetic_ref)),
        SE::BinaryOp { op, left, right } => SE::BinaryOp {
            op: op.clone(),
            left: Box::new(replace_scalar_subquery(left, synthetic_ref)),
            right: Box::new(replace_scalar_subquery(right, synthetic_ref)),
        },
        SE::Nested(inner) => SE::Nested(Box::new(replace_scalar_subquery(inner, synthetic_ref))),
        other => other.clone(),
    }
}

fn contains_aggregate(e: &SE, name: &str) -> bool {
    match e {
        SE::Function(f) => {
            let n = crate::planner::object_name_to_string(&f.name)
                .unwrap_or_default()
                .to_ascii_lowercase();
            if n == name {
                return true;
            }
            crate::planner::function_call_args(f)
                .unwrap_or_default()
                .iter()
                .any(|a| contains_aggregate(a, name))
        }
        SE::BinaryOp { left, right, .. } => {
            contains_aggregate(left, name) || contains_aggregate(right, name)
        }
        SE::Nested(inner) => contains_aggregate(inner, name),
        SE::UnaryOp { expr, .. } => contains_aggregate(expr, name),
        _ => false,
    }
}

// ---------- 列引用收集与下标重映射 ----------

fn prefix(scope: &Scope) -> Vec<usize> {
    let mut out = Vec::with_capacity(scope.tables.len());
    let mut acc = 0;
    for (_, t) in &scope.tables {
        out.push(acc);
        acc += t.columns.len();
    }
    out
}/// 表达式引用的内表列 → 扁平内下标集合。
fn collect_inner_cols(e: &SE, inner: &Scope, out: &mut BTreeSet<usize>) -> Result<()> {
    let mut cols = BTreeSet::new();
    collect_sql_columns(e, inner, &mut cols)?;
    let pf = prefix(inner);
    for (pos, name) in cols {
        if let Some(ci) = inner.tables[pos].1.column_index(&name) {
            out.insert(pf[pos] + ci);
        }
    }
    Ok(())
}

/// 组合 scope(内表优先)表达式:内表部分 → 扁平内下标集合。
fn collect_inner_cols_mixed(
    e: &SE,
    combined: &Scope,
    inner_width: usize,
    out: &mut BTreeSet<usize>,
) -> Result<()> {
    let mut cols = BTreeSet::new();
    collect_sql_columns(e, combined, &mut cols)?;
    for (pos, name) in cols {
        if table_offset(combined, pos) >= inner_width {
            continue; // 外表列:探测侧直读
        }
        if let Some(ci) = combined.tables[pos].1.column_index(&name) {
            out.insert(table_offset(combined, pos) + ci);
        }
    }
    Ok(())
}

fn table_offset(scope: &Scope, pos: usize) -> usize {
    scope.tables[..pos].iter().map(|(_, t)| t.columns.len()).sum()
}

/// 内表列扁平下标 → 追加项 AST(`alias.col`)。
fn column_se(inner: &Scope, flat: usize) -> Result<SE> {
    let mut acc = 0;
    for (alias, t) in &inner.tables {
        let w = t.columns.len();
        if flat < acc + w {
            let col = t.columns[flat - acc].name.clone();
            return Ok(SE::CompoundIdentifier(vec![
                Ident::new(alias.clone()),
                Ident::new(col),
            ]));
        }
        acc += w;
    }
    Err(Error::sql("internal: join column out of inner scope"))
}

/// 构建键重映射:内下标 → 构建输出下标。
fn remap_build(e: Expr, to_build: &dyn Fn(usize) -> usize) -> Expr {
    match e {
        Expr::Column(i) => Expr::Column(to_build(i)),
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(remap_build(*left, to_build)),
            right: Box::new(remap_build(*right, to_build)),
        },
        Expr::Function { name, args } => Expr::Function {
            name,
            args: args
                .into_iter()
                .map(|a| remap_build(a, to_build))
                .collect(),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(remap_build(*expr, to_build)),
            negated,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, r)| (remap_build(c, to_build), remap_build(r, to_build)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(remap_build(*e, to_build))),
        },
        Expr::InList { expr, list, negated } => Expr::InList {
            expr: Box::new(remap_build(*expr, to_build)),
            list,
            negated,
        },
        other => other,
    }
}

/// 拼接块条件重映射:组合 scope(内表优先)下标 → (探测 ++ 构建)下标。
/// 外表部分 → 探测区(减去内表宽度);内表部分 → 构建区(probe_width + 追加位)。
fn remap_cond(
    e: Expr,
    probe_width: usize,
    to_build: &dyn Fn(usize) -> usize,
    inner_width: usize,
) -> Expr {
    match e {
        Expr::Column(i) => {
            if i < inner_width {
                Expr::Column(probe_width + to_build(i))
            } else {
                Expr::Column(i - inner_width)
            }
        }
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(remap_cond(*left, probe_width, to_build, inner_width)),
            right: Box::new(remap_cond(*right, probe_width, to_build, inner_width)),
        },
        Expr::Function { name, args } => Expr::Function {
            name,
            args: args
                .into_iter()
                .map(|a| remap_cond(a, probe_width, to_build, inner_width))
                .collect(),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(remap_cond(*expr, probe_width, to_build, inner_width)),
            negated,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, r)| {
                    (
                        remap_cond(c, probe_width, to_build, inner_width),
                        remap_cond(r, probe_width, to_build, inner_width),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|e| Box::new(remap_cond(*e, probe_width, to_build, inner_width))),
        },
        Expr::InList { expr, list, negated } => Expr::InList {
            expr: Box::new(remap_cond(*expr, probe_width, to_build, inner_width)),
            list,
            negated,
        },
        other => other,
    }
}
