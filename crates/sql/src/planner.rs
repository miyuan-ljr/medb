//! 计划器:SQL AST → 语句计划。
//!
//! - 查询:SeqScan/Values → Filter(WHERE)→ [AggHash → Filter(HAVING)] →
//!   Project(可见列 + ORDER BY 隐藏键列)→ [Sort → 砍隐藏列] → Limit;
//! - INSERT:VALUES 计划期求值收敛;INSERT SELECT 保留流水线由引擎执行收敛;
//! - UPDATE:赋值表达式针对旧行求值(标准语义:各赋值互不可见);
//! - CREATE/DROP TABLE、BEGIN/COMMIT/ROLLBACK。
//!
//! 计划器只读 catalog / 数据源接口,不做副作用;落库由 Engine 执行。

use std::sync::Arc;

use catalog::CatalogStore;
use common::error::{Error, Result};
use common::schema::{ColumnDesc, Row, TableSchema};
use common::types::{DataType, ScalarValue};
use exec::agg::{AggDesc, AggFunc};
use exec::chunk::DataChunk;
use exec::executor::BoxedExecutor;
use exec::expr::{BinaryOp, Expr};
use exec::operators::hash_agg::AggHashExec;
use exec::operators::hash_join::{HashJoinExec, JoinType};
use exec::operators::seq_scan::{QueryDataSource, SeqScanExec};
use exec::operators::sort::{SortExec, SortKey};
use exec::operators::values::ValuesExec;
use exec::operators::{filter::FilterExec, limit::LimitExec, project::ProjectExec};
use exec::vector::Vector;
use sqlparser::ast::{
    BinaryOperator, DataType as SqlType, Expr as SE, FromTable, JoinConstraint, JoinOperator,
    LimitClause, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, UnaryOperator, Value,
};

/// 计划上下文:批大小 + 元数据 + 表数据源。
pub struct PlanContext<'a> {
    pub batch_size: usize,
    pub catalog: &'a dyn CatalogStore,
    pub data: Arc<dyn QueryDataSource>,
}

/// FROM 作用域:多表(别名, schema)的有序集合,列解析返回扁平下标。
pub struct Scope {
    pub tables: Vec<(String, TableSchema)>,
}

impl Scope {
    pub fn empty() -> Self {
        Self { tables: Vec::new() }
    }

    pub fn single(t: &TableSchema) -> Self {
        Self {
            tables: vec![(t.name.clone(), t.clone())],
        }
    }

    pub fn push(&mut self, alias: String, t: &TableSchema) {
        self.tables.push((alias, t.clone()));
    }

    /// 全部表列的扁平 schema(按连接顺序拼接)。
    pub fn flat_schema(&self) -> Vec<ColumnDesc> {
        self.tables
            .iter()
            .flat_map(|(_, t)| t.columns.iter().cloned())
            .collect()
    }

    /// 每表列数(FROM 顺序)。
    pub fn table_widths(&self) -> Vec<usize> {
        self.tables.iter().map(|(_, t)| t.columns.len()).collect()
    }

    /// 解析列引用 → 扁平下标。qualifier 匹配别名或表名(大小写不敏感);
    /// 非限定列在全部表中唯一命中,多表命中视为歧义错误。
    pub fn resolve(&self, qual: Option<&str>, col: &str) -> Result<usize> {
        // 收集 (表位置, 表内列下标)
        let mut hits: Vec<(usize, usize)> = Vec::new();
        for (pos, (alias, t)) in self.tables.iter().enumerate() {
            if let Some(i) = t.column_index(col) {
                let qual_match = qual.map(|q| alias.eq_ignore_ascii_case(q)).unwrap_or(true);
                if qual_match {
                    hits.push((pos, i));
                    if qual.is_some() {
                        break;
                    }
                }
            }
        }
        match hits.len() {
            1 => {
                let (pos, i) = hits[0];
                let start: usize = self.tables[..pos]
                    .iter()
                    .map(|(_, t)| t.columns.len())
                    .sum();
                Ok(start + i)
            }
            0 => Err(Error::catalog(format!("column '{col}' not found"))),
            _ => Err(Error::catalog(format!(
                "column reference '{col}' is ambiguous"
            ))),
        }
    }

    /// 限定符(别名/表名)→ 表位置;找不到返回 None。
    pub fn resolve_qualifier(&self, qual: &str) -> Option<usize> {
        self.tables
            .iter()
            .position(|(a, _)| a.eq_ignore_ascii_case(qual))
    }
}

/// 语句计划产物。
pub enum StatementPlan {
    /// SELECT:可直接拉取的执行流水线。
    Query(PlannedQuery),
    /// INSERT VALUES:计划期已求值、收敛的整行集合。
    Insert {
        table: TableSchema,
        rows: Vec<Row>,
    },
    /// INSERT SELECT:目标列下标 + 查询流水线;引擎执行后按列收敛。
    InsertSelect {
        table: TableSchema,
        targets: Vec<usize>,
        query: PlannedQuery,
    },
    /// UPDATE:赋值下标 + 针对旧行的表达式。
    Update {
        table: TableSchema,
        assignments: Vec<(usize, Expr)>,
        predicate: Option<Expr>,
    },
    /// DELETE:谓词在执行期按批求值。
    Delete {
        table: TableSchema,
        predicate: Option<Expr>,
    },
    /// CREATE TABLE:id = 0,由 catalog 分配。
    CreateTable {
        schema: TableSchema,
    },
    /// ALTER TABLE ADD COLUMN(仅尾部追加;执行期重写行数据)。
    AlterAddColumn {
        name: String,
        column: ColumnDesc,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
    },
    Begin,
    Commit,
    Rollback,
}

/// 查询计划:执行流水线 + 静态输出 schema(供 server 生成 RowDescription)。
pub struct PlannedQuery {
    pub executor: BoxedExecutor,
    pub schema: Vec<ColumnDesc>,
}

pub fn plan_statement(stmt: &Statement, ctx: &PlanContext<'_>) -> Result<StatementPlan> {
    match stmt {
        Statement::Query(q) => plan_query(q, ctx).map(StatementPlan::Query),
        Statement::Insert(insert) => plan_insert(insert, ctx),
        Statement::Update(update) => plan_update(update, ctx),
        Statement::Delete(delete) => plan_delete(delete, ctx),
        Statement::CreateTable(create) => plan_create_table(create),
        Statement::AlterTable(alter) => {
            // v1 仅支持 ADD COLUMN
            let [op] = alter.operations.as_slice() else {
                return Err(Error::not_supported("multiple operations in ALTER TABLE"));
            };
            let sqlparser::ast::AlterTableOperation::AddColumn {
                column_def,
                if_not_exists,
                ..
            } = op
            else {
                return Err(Error::not_supported(format!(
                    "ALTER TABLE operation: {}",
                    op
                )));
            };
            let ty = map_sql_type(&column_def.data_type)?;
            let mut nullable = true;
            for opt in &column_def.options {
                match &opt.option {
                    sqlparser::ast::ColumnOption::NotNull => nullable = false,
                    sqlparser::ast::ColumnOption::Null => nullable = true,
                    _ => {}
                }
            }
            if !nullable {
                // 尾部追加非空列会让历史行违反约束 → 要求可空(默认 true),TODO
                return Err(Error::not_supported(
                    "ADD COLUMN requires a nullable column (existing rows become NULL)",
                ));
            }
            Ok(StatementPlan::AlterAddColumn {
                name: object_name_to_string(&alter.name)?,
                column: ColumnDesc::new(column_def.name.value.clone(), ty, nullable),
                if_not_exists: *if_not_exists,
            })
        }
        Statement::Drop {
            object_type, names, ..
        } if *object_type == sqlparser::ast::ObjectType::Table => {
            if names.len() != 1 {
                return Err(Error::not_supported("DROP TABLE with multiple names"));
            }
            Ok(StatementPlan::DropTable {
                name: object_name_to_string(&names[0])?,
            })
        }
        Statement::StartTransaction { .. } => Ok(StatementPlan::Begin),
        Statement::Commit { .. } => Ok(StatementPlan::Commit),
        Statement::Rollback { .. } => Ok(StatementPlan::Rollback),
        other => Err(Error::not_supported(format!(
            "statement not supported yet: {}",
            other
        ))),
    }
}

// ---------- 查询 ----------

pub fn plan_query(q: &Query, ctx: &PlanContext<'_>) -> Result<PlannedQuery> {
    if q.with.is_some() {
        return Err(Error::not_supported("WITH clause"));
    }
    if q.fetch.is_some() {
        return Err(Error::not_supported("FETCH clause"));
    }
    let SetExpr::Select(select) = &*q.body else {
        return Err(Error::not_supported("set operations (UNION/EXCEPT/...)"));
    };
    let order_by: &[sqlparser::ast::OrderByExpr] = match q.order_by.as_ref().map(|o| &o.kind) {
        Some(sqlparser::ast::OrderByKind::Expressions(exprs)) => exprs.as_slice(),
        _ => &[],
    };
    let planned = plan_select(select, order_by, ctx)?;

    // LIMIT / OFFSET
    match &q.limit_clause {
        None => Ok(planned),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if !limit_by.is_empty() {
                return Err(Error::not_supported("LIMIT BY clause"));
            }
            let limit_value = match limit {
                Some(e) => Some(literal_u64(e, "LIMIT")?),
                None => None,
            };
            let offset_value = match offset {
                Some(o) => literal_u64(&o.value, "OFFSET")?,
                None => 0,
            };
            finish_with_limit(planned, offset_value, limit_value)
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            let off = literal_u64(offset, "OFFSET")?;
            let lim = literal_u64(limit, "LIMIT")?;
            finish_with_limit(planned, off, Some(lim))
        }
    }
}

fn finish_with_limit(
    planned: PlannedQuery,
    offset: u64,
    limit: Option<u64>,
) -> Result<PlannedQuery> {
    if offset == 0 && limit.is_none() {
        return Ok(planned);
    }
    let schema = planned.schema.clone();
    let executor = Box::new(LimitExec::new(planned.executor, offset as usize, limit));
    Ok(PlannedQuery { executor, schema })
}

/// 查询是否是聚合查询:GROUP BY 非空,或投影/HAVING/ORDER BY 中出现聚合调用。
fn select_is_aggregate(select: &Select, order_by: &[sqlparser::ast::OrderByExpr]) -> bool {
    let group_nonempty = !matches!(
        &select.group_by,
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) if exprs.is_empty()
    );
    if group_nonempty {
        return true;
    }
    let mut found = false;
    for item in &select.projection {
        let e = match item {
            SelectItem::UnnamedExpr(e) => Some(e),
            SelectItem::ExprWithAlias { expr, .. } => Some(expr),
            SelectItem::ExprWithAliases { expr, .. } => Some(expr),
            _ => None,
        };
        if e.is_some_and(is_aggregate_call) {
            found = true;
        }
    }
    if found {
        return true;
    }
    if select.having.as_ref().is_some_and(is_aggregate_call) {
        return true;
    }
    order_by.iter().any(|ob| is_aggregate_call(&ob.expr))
}

fn is_aggregate_call(e: &SE) -> bool {
    match e {
        SE::Function(f) => {
            let Ok(name) = object_name_to_string(&f.name) else {
                return false;
            };
            is_aggregate_name(&name)
        }
        SE::BinaryOp { left, right, .. } => is_aggregate_call(left) || is_aggregate_call(right),
        SE::Nested(inner) => is_aggregate_call(inner),
        SE::UnaryOp { expr, .. } => is_aggregate_call(expr),
        _ => false,
    }
}

fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max"
    )
}

/// 从 AST 函数调用提取实参表达式(仅未命名参数;COUNT(*) → 空列表)。
fn function_call_args(f: &sqlparser::ast::Function) -> Result<Vec<SE>> {
    if f.filter.is_some() || f.over.is_some() || !f.within_group.is_empty() {
        return Err(Error::not_supported("FILTER/OVER/WITHIN GROUP clauses"));
    }
    let sqlparser::ast::FunctionArguments::List(list) = &f.args else {
        return Err(Error::not_supported("function argument form"));
    };
    if let Some(dup) = &list.duplicate_treatment {
        if matches!(dup, sqlparser::ast::DuplicateTreatment::Distinct) {
            return Err(Error::not_supported("DISTINCT arguments"));
        }
    }
    if !list.clauses.is_empty() {
        return Err(Error::not_supported("function argument clauses"));
    }
    let mut out = Vec::with_capacity(list.args.len());
    for fa in &list.args {
        let sqlparser::ast::FunctionArg::Unnamed(fae) = fa else {
            return Err(Error::not_supported("named function arguments"));
        };
        match fae {
            sqlparser::ast::FunctionArgExpr::Expr(e) => out.push(e.clone()),
            other => {
                return Err(Error::not_supported(format!("function argument {other:?}")));
            }
        }
    }
    Ok(out)
}

/// 标量函数实参 → 执行表达式(逐个经 plan_fn 映射)。
fn scalar_function_args(
    f: &sqlparser::ast::Function,
    mut plan_fn: impl FnMut(&SE) -> Result<Expr>,
) -> Result<Vec<Expr>> {
    let args = function_call_args(f)?;
    let mut out = Vec::with_capacity(args.len());
    for a in &args {
        out.push(plan_fn(a)?);
    }
    Ok(out)
}

fn plan_select(
    select: &Select,
    order_by: &[sqlparser::ast::OrderByExpr],
    ctx: &PlanContext<'_>,
) -> Result<PlannedQuery> {
    if select.distinct.is_some() {
        return Err(Error::not_supported("SELECT DISTINCT"));
    }
    if !select.lateral_views.is_empty() || select.into.is_some() || select.qualify.is_some() {
        return Err(Error::not_supported("unsupported select clause"));
    }

    // FROM:多表 + JOIN 步骤(逗号连接视为 CROSS JOIN)→ 有序关系步骤
    let steps = match select.from.as_slice() {
        [] => Vec::new(),
        from => plan_from(from, ctx)?,
    };
    let mut scope = Scope::empty();
    for s in &steps {
        scope.push(s.alias.clone(), &s.schema);
    }
    let scope_ref = if steps.is_empty() { None } else { Some(&scope) };
    let offsets = prefix_offsets(&scope.table_widths());
    let input_schema: Vec<ColumnDesc> = if steps.is_empty() {
        Vec::new()
    } else {
        scope.flat_schema()
    };

    // GROUP BY 项(仅形状检查;聚合路径才解析)
    let group_items: &Vec<SE> = match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => exprs,
        _ => return Err(Error::not_supported("GROUP BY clause")),
    };
    let aggregate = !group_items.is_empty() || select_is_aggregate(select, order_by);

    // ---------- 可见投影 ----------
    struct ProjectItem {
        expr: Expr,
        name: String,
        /// 改写/解析后的表达式文本(ORDER BY 表达式匹配用)
        planned_display: String,
    }
    let mut visible: Vec<ProjectItem> = Vec::new();

    // 聚合改写器(仅聚合路径使用);GROUP BY 项必须先于任何改写注册
    let mut rewriter = AggRewriter::new(scope_ref, &input_schema);
    if aggregate {
        for item in group_items {
            rewriter.add_group(item)?;
        }
    }

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                let expr = if aggregate {
                    rewriter.rewrite(e, false)?
                } else {
                    plan_expr(e, scope_ref)?
                };
                visible.push(ProjectItem {
                    planned_display: format!("{expr}"),
                    expr,
                    name: format!("{e}"),
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let e = if aggregate {
                    rewriter.rewrite(expr, false)?
                } else {
                    plan_expr(expr, scope_ref)?
                };
                visible.push(ProjectItem {
                    planned_display: format!("{e}"),
                    expr: e,
                    name: alias.value.clone(),
                });
            }
            SelectItem::ExprWithAliases { expr, aliases } => {
                let e = if aggregate {
                    rewriter.rewrite(expr, false)?
                } else {
                    plan_expr(expr, scope_ref)?
                };
                let name = aliases
                    .first()
                    .map(|a| a.value.clone())
                    .unwrap_or_else(|| format!("{expr}"));
                let planned_display = format!("{e}");
                visible.push(ProjectItem {
                    expr: e,
                    name,
                    planned_display,
                });
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                if aggregate {
                    return Err(Error::not_supported(
                        "SELECT * cannot be combined with GROUP BY / aggregates",
                    ));
                }
                if scope_ref.is_none() {
                    return Err(Error::not_supported("SELECT * without FROM clause"));
                }
                // 展开范围:全部表或限定别名的单表(扁平下标区间)
                let (lo, hi) = match item {
                    SelectItem::QualifiedWildcard(qualifier, _) => {
                        let sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(qname) =
                            qualifier
                        else {
                            return Err(Error::not_supported("SELECT <expr>.*"));
                        };
                        let q = object_name_to_string(qname)?;
                        let pos = scope.resolve_qualifier(&q).ok_or_else(|| {
                            Error::catalog(format!("unknown table qualifier '{q}'"))
                        })?;
                        (offsets[pos], offsets[pos + 1])
                    }
                    _ => (0, offsets[offsets.len() - 1]),
                };
                for idx in lo..hi {
                    let expr = Expr::Column(idx);
                    let name = input_schema[idx].name.clone();
                    visible.push(ProjectItem {
                        planned_display: format!("{expr}"),
                        expr,
                        name,
                    });
                }
            }
        }
    }
    if visible.is_empty() {
        return Err(Error::sql("empty projection"));
    }

    // ---------- ORDER BY 键解析 ----------
    // 命中已有输出列 → 直接引用;否则作为隐藏键列追加到投影尾部。
    let mut keys: Vec<SortKey> = Vec::new();
    let mut hidden: Vec<Expr> = Vec::new();
    for ob in order_by {
        let asc = match ob.options.sort {
            Some(sqlparser::ast::OrderBySort::Desc) => false,
            Some(sqlparser::ast::OrderBySort::Using(_)) => {
                return Err(Error::not_supported("ORDER BY ... USING"));
            }
            Some(sqlparser::ast::OrderBySort::Asc) | None => true,
        };
        let nulls_first = ob.options.nulls_first.unwrap_or(!asc);
        let sort_idx = match &ob.expr {
            // 序号:ORDER BY 1
            SE::Value(v) => match &v.value {
                Value::Number(text, _) => {
                    let ordinal: usize = text.parse().map_err(|_| {
                        Error::sql(format!("ORDER BY ordinal '{text}' out of range"))
                    })?;
                    if ordinal == 0 || ordinal > visible.len() {
                        return Err(Error::sql(format!(
                            "ORDER BY ordinal {ordinal} out of range (1..={})",
                            visible.len()
                        )));
                    }
                    ordinal - 1
                }
                other => {
                    let lit = value_to_scalar(other)?;
                    let expr = Expr::Literal(lit);
                    hidden.push(expr);
                    visible.len() + hidden.len() - 1
                }
            },
            // 输出列名 / 别名
            SE::Identifier(ident) => {
                match visible
                    .iter()
                    .position(|p| p.name.eq_ignore_ascii_case(&ident.value))
                {
                    Some(i) => i,
                    None => {
                        // 非输出列:作为隐藏键列(plain 路径可解析输入列;聚合路径用改写)
                        let expr = if aggregate {
                            rewriter.rewrite(&ob.expr, false)?
                        } else {
                            plan_expr(&ob.expr, scope_ref)?
                        };
                        hidden.push(expr);
                        visible.len() + hidden.len() - 1
                    }
                }
            }
            // 表达式:与投影表达式结构匹配 → 复用该输出列;否则隐藏键列
            other => {
                let key = if aggregate {
                    rewriter.rewrite(other, false)?
                } else {
                    plan_expr(other, scope_ref)?
                };
                let display = format!("{key}");
                match visible.iter().position(|p| p.planned_display == display) {
                    Some(i) => i,
                    None => {
                        hidden.push(key);
                        visible.len() + hidden.len() - 1
                    }
                }
            }
        };
        keys.push(SortKey {
            expr: Expr::Column(sort_idx),
            asc,
            nulls_first,
        });
    }

    // ---------- 输出 schema(Project 全列 = 可见 + 隐藏)----------
    // 聚合路径的"输入"是 agg 输出(groups + aggs)
    let project_input_schema: Vec<ColumnDesc> = if aggregate {
        rewriter.agg_output_schema()
    } else {
        input_schema.clone()
    };
    let mut project_schema = Vec::with_capacity(visible.len() + hidden.len());
    for p in &visible {
        let ty = expr_type(&p.expr, &project_input_schema)?;
        project_schema.push(ColumnDesc::new(p.name.clone(), ty, true));
    }
    for h in &hidden {
        let ty = expr_type(h, &project_input_schema)?;
        project_schema.push(ColumnDesc::new("__sort_key".to_string(), ty, true));
    }

    // ---------- 流水线 ----------
    // 无 FROM:单行占位批(plain 路径投影不引用列;聚合路径在其上聚合一组)
    let (mut pipeline, base_dtypes): (BoxedExecutor, Vec<DataType>) = if steps.is_empty() {
        let d = vec![ColumnDesc::new(
            "__dummy".to_string(),
            DataType::Int64,
            true,
        )];
        let dtypes = vec![DataType::Int64];
        (Box::new(ValuesExec::one_row(d)), dtypes)
    } else {
        // WHERE 合取项路由:
        // - 单表且其后无 LEFT JOIN 步骤 → 下推该表扫描(列下标重基到表内);
        // - 单表且其后有 LEFT 步骤(保留侧/空产侧)→ 挂到该 LEFT 步骤 residual;
        // - 常量谓词 → 挂第一张表扫描;
        // - 跨表等值(两侧各引一表,INNER)→ 该步骤的哈希键;
        // - 其余跨表条件 → 在其引用的最大表位置步骤之后过滤(residual)。
        let mut scan_preds: Vec<Vec<Expr>> = vec![Vec::new(); steps.len()];
        let mut step_keys: Vec<(Vec<Expr>, Vec<Expr>)> =
            vec![(Vec::new(), Vec::new()); steps.len()];
        let mut step_match: Vec<Vec<Expr>> = vec![Vec::new(); steps.len()];
        let mut step_res: Vec<Vec<Expr>> = vec![Vec::new(); steps.len()];

        // ON 条件路由(连接步骤;非哈希部分进 match_cond,匹配期判定,
        // 保证 LEFT JOIN 下未过条件的左行仍 NULL 补齐)
        for (j, step) in steps.iter().enumerate().skip(1) {
            if let Some(on) = &step.on {
                for c in split_conjuncts(on) {
                    if let Some((pk, bk)) = try_hash_eq(c, &scope, &offsets)? {
                        step_keys[j].0.push(pk);
                        step_keys[j].1.push(bk);
                        continue;
                    }
                    let e = plan_expr(c, scope_ref)?;
                    check_bool(&e, &input_schema, "join condition")?;
                    step_match[j].push(e);
                }
            }
        }

        // WHERE 路由
        if let Some(where_expr) = &select.selection {
            let first_left = steps
                .iter()
                .position(|s| s.join_type == JoinType::LeftOuter);
            for c in split_conjuncts(where_expr) {
                if aggregate && is_aggregate_call(c) {
                    return Err(Error::sql(
                        "aggregates are not allowed in WHERE (use HAVING)",
                    ));
                }
                let touched = sql_expr_tables(c, &scope)?;
                let e = plan_expr(c, scope_ref)?;
                check_bool(&e, &input_schema, "WHERE")?;
                match touched.as_slice() {
                    [] => scan_preds[0].push(e),
                    // 扁平下标入队;重基到表内局部由 table_scan 统一完成
                    [pos] => match first_left {
                        Some(l) if l > *pos => step_res[l].push(e),
                        _ => scan_preds[*pos].push(e),
                    },
                    multi => {
                        let j = *multi.last().expect("non-empty");
                        step_res[j].push(e);
                    }
                }
            }
        }

        // 逐表扫描(下推过滤)→ HashJoin 组合
        let mut exec: BoxedExecutor = table_scan(&steps[0], &scan_preds[0], 0, ctx)?;
        for j in 1..steps.len() {
            let build = table_scan(&steps[j], &scan_preds[j], offsets[j], ctx)?;
            let probe_schema = input_schema[..offsets[j]].to_vec();
            let build_schema = steps[j].schema.columns.clone();
            let (pk, bk) = step_keys[j].clone();
            exec = Box::new(HashJoinExec::new(
                exec,
                build,
                probe_schema,
                build_schema,
                pk,
                bk,
                combine_and(step_match[j].clone()),
                combine_and(step_res[j].clone()),
                steps[j].join_type,
            ));
        }
        let dtypes = input_schema.iter().map(|c| c.ty.clone()).collect();
        (exec, dtypes)
    };

    // 无 FROM 时的 WHERE(常量谓词直接过滤单行占位)
    if steps.is_empty() {
        if let Some(where_expr) = &select.selection {
            if aggregate && is_aggregate_call(where_expr) {
                return Err(Error::sql(
                    "aggregates are not allowed in WHERE (use HAVING)",
                ));
            }
            let pred = plan_expr(where_expr, None)?;
            pipeline = Box::new(FilterExec::new(pipeline, pred));
        }
    }

    // HAVING + 聚合
    if aggregate {
        let (group_exprs, aggs, agg_out_schema) = rewriter.take_pipeline_parts();
        pipeline = Box::new(AggHashExec::new(
            pipeline,
            base_dtypes,
            group_exprs,
            aggs,
            agg_out_schema,
        ));
        if let Some(having) = &select.having {
            let pred = rewriter.rewrite(having, false)?;
            match pred.return_type(&project_input_schema)? {
                Some(DataType::Boolean) | None => {}
                Some(t) => {
                    return Err(Error::sql(format!("HAVING must be boolean, got {t:?}")));
                }
            }
            pipeline = Box::new(FilterExec::new(pipeline, pred));
        }
    }

    // Project(可见 + 隐藏)
    let mut project_exprs: Vec<Expr> = visible.iter().map(|p| p.expr.clone()).collect();
    project_exprs.extend(hidden.iter().cloned());
    pipeline = Box::new(ProjectExec::new(
        pipeline,
        project_schema.clone(),
        project_exprs,
    ));

    // Sort + 砍隐藏列
    if !keys.is_empty() {
        pipeline = Box::new(SortExec::new(pipeline, keys, project_schema.clone()));
        if !hidden.is_empty() {
            let visible_n = visible.len();
            pipeline = Box::new(ProjectExec::new(
                pipeline,
                project_schema[..visible_n].to_vec(),
                (0..visible_n).map(Expr::Column).collect(),
            ));
        }
    }

    let final_schema = project_schema[..visible.len()].to_vec();
    // batch_size 由 ExecuteContext 携带,计划期不固化
    let _ = ctx.batch_size;
    Ok(PlannedQuery {
        executor: pipeline,
        schema: final_schema,
    })
}

// ---------- FROM / JOIN 规划 ----------

/// FROM 中的一个关系步骤(第 0 个为驱动表;后续为连接步骤)。
struct RelationStep {
    alias: String,
    schema: TableSchema,
    join_type: JoinType,
    on: Option<SE>,
}

/// FROM 子句解析:TableWithJoins 列表展开为有序步骤;逗号连接 = 无 ON 的内连接。
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
}

/// 单表扫描(可选下推过滤,列下标重基到表内局部)。
fn table_scan(
    step: &RelationStep,
    preds: &[Expr],
    offset: usize,
    ctx: &PlanContext<'_>,
) -> Result<BoxedExecutor> {
    let mut exec: BoxedExecutor = Box::new(SeqScanExec::new(ctx.data.clone(), step.schema.clone()));
    if !preds.is_empty() {
        let rebased = preds
            .iter()
            .map(|p| rebase_columns(p.clone(), offset))
            .collect::<Result<Vec<_>>>()?;
        if let Some(pred) = combine_and(rebased) {
            exec = Box::new(FilterExec::new(exec, pred));
        }
    }
    Ok(exec)
}

/// 宽度前缀和:表位置 k 的列占扁平区间 [offsets[k], offsets[k+1])。
fn prefix_offsets(widths: &[usize]) -> Vec<usize> {
    let mut out = Vec::with_capacity(widths.len() + 1);
    let mut acc = 0usize;
    out.push(0);
    for w in widths {
        acc += w;
        out.push(acc);
    }
    out
}

/// AND 树拆合取项(嵌套展开;返回原表达式的引用)。
fn split_conjuncts(e: &SE) -> Vec<&SE> {
    match e {
        SE::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut v = split_conjuncts(left);
            v.extend(split_conjuncts(right));
            v
        }
        other => vec![other],
    }
}

/// SQL 表达式引用到的表位置(去重升序);常量表达式返回空。
fn sql_expr_tables(e: &SE, scope: &Scope) -> Result<Vec<usize>> {
    let mut out = std::collections::BTreeSet::new();
    collect_sql_tables(e, scope, &mut out)?;
    Ok(out.into_iter().collect())
}

fn collect_sql_tables(
    e: &SE,
    scope: &Scope,
    out: &mut std::collections::BTreeSet<usize>,
) -> Result<()> {
    match e {
        SE::Identifier(ident) => {
            // 非限定列:命中该列名的全部表(命中多表 → 视为跨表条件)
            let mut any = false;
            for (pos, (_, t)) in scope.tables.iter().enumerate() {
                if find_column(t, &ident.value).is_some() {
                    out.insert(pos);
                    any = true;
                }
            }
            if !any {
                return Err(Error::catalog(format!(
                    "column '{}' not found",
                    ident.value
                )));
            }
            Ok(())
        }
        SE::CompoundIdentifier(parts) => {
            let [q, c] = parts.as_slice() else {
                return Err(Error::not_supported("qualified identifier (>2 parts)"));
            };
            let pos = scope
                .resolve_qualifier(&q.value)
                .ok_or_else(|| Error::catalog(format!("unknown table alias '{}'", q.value)))?;
            scope.resolve(Some(&q.value), &c.value)?;
            out.insert(pos);
            Ok(())
        }
        SE::BinaryOp { left, right, .. } => {
            collect_sql_tables(left, scope, out)?;
            collect_sql_tables(right, scope, out)
        }
        SE::Nested(inner) => collect_sql_tables(inner, scope, out),
        SE::UnaryOp { expr, .. } => collect_sql_tables(expr, scope, out),
        SE::Function(f) => {
            for a in function_call_args(f)? {
                collect_sql_tables(&a, scope, out)?;
            }
            Ok(())
        }
        SE::Like { expr, pattern, .. } => {
            collect_sql_tables(expr, scope, out)?;
            collect_sql_tables(pattern, scope, out)
        }
        SE::Extract { expr, .. } => collect_sql_tables(expr, scope, out),
        SE::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_sql_tables(expr, scope, out)?;
            if let Some(f) = substring_from {
                collect_sql_tables(f, scope, out)?;
            }
            if let Some(f) = substring_for {
                collect_sql_tables(f, scope, out)?;
            }
            Ok(())
        }
        _ => Ok(()), // 字面量 / 类型化字符串 / INTERVAL 等常量
    }
}

/// 尝试把 `a = b` 形态的条件转成哈希连接键:两侧各只引用一张表 {i}/{j}
/// (i < j)→ (probe 键扁平等值, build 键列下标重基到表 j 局部)。
fn try_hash_eq(c: &SE, scope: &Scope, offsets: &[usize]) -> Result<Option<(Expr, Expr)>> {
    let SE::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = c
    else {
        return Ok(None);
    };
    let lt = sql_expr_tables(left, scope)?;
    let rt = sql_expr_tables(right, scope)?;
    let (probe_sql, build_sql, j) = match (lt.as_slice(), rt.as_slice()) {
        ([i], [jj]) if i < jj => (left, right, *jj),
        ([i], [jj]) if i > jj => (right, left, *i),
        _ => return Ok(None),
    };
    let probe = plan_expr(probe_sql, Some(scope))?;
    let build = rebase_columns(plan_expr(build_sql, Some(scope))?, offsets[j])?;
    Ok(Some((probe, build)))
}

/// 表达式列下标整体平移(扁平 → 表内局部)。
fn rebase_columns(e: Expr, delta: usize) -> Result<Expr> {
    match e {
        Expr::Column(i) => {
            if i < delta {
                return Err(Error::internal("column rebasing underflow"));
            }
            Ok(Expr::Column(i - delta))
        }
        Expr::Binary { op, left, right } => Ok(Expr::binary(
            op,
            rebase_columns(*left, delta)?,
            rebase_columns(*right, delta)?,
        )),
        Expr::Function { name, args } => Ok(Expr::Function {
            name,
            args: args
                .into_iter()
                .map(|a| rebase_columns(a, delta))
                .collect::<Result<Vec<_>>>()?,
        }),
        Expr::IsNull { expr, negated } => Ok(Expr::IsNull {
            expr: Box::new(rebase_columns(*expr, delta)?),
            negated,
        }),
        other => Ok(other),
    }
}

/// 合取表达式列表 → AND 链;空列表返回 None。
fn combine_and(exprs: Vec<Expr>) -> Option<Expr> {
    let mut iter = exprs.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, e| Expr::binary(BinaryOp::And, acc, e)))
}

/// 谓词静态类型必须是布尔。
fn check_bool(e: &Expr, input: &[ColumnDesc], what: &str) -> Result<()> {
    match e.return_type(input)? {
        Some(DataType::Boolean) | None => Ok(()),
        Some(t) => Err(Error::sql(format!("{what} must be boolean, got {t:?}"))),
    }
}

/// 收集表达式中引用的输入列下标。
fn collect_column_refs(expr: &Expr, out: &mut std::collections::HashSet<usize>) {
    match expr {
        Expr::Column(i) => {
            out.insert(*i);
        }
        Expr::Literal(_) => {}
        Expr::Binary { left, right, .. } => {
            collect_column_refs(left, out);
            collect_column_refs(right, out);
        }
        Expr::Function { args, .. } => {
            for a in args {
                collect_column_refs(a, out);
            }
        }
        Expr::IsNull { expr, .. } => collect_column_refs(expr, out),
    }
}

fn expr_type(expr: &Expr, input: &[ColumnDesc]) -> Result<DataType> {
    // 裸 NULL 等无法静态推导 → BIGINT(与无 FROM SELECT 一致)
    Ok(expr.return_type(input)?.unwrap_or(DataType::Int64))
}

// ---------- 聚合改写 ----------

/// 把含聚合调用的 SQL 表达式改写为"引用聚合输出列"的执行表达式:
/// - 聚合调用 → Column(group_n + agg_idx);
/// - 与某 GROUP BY 项结构相同的子表达式 → Column(group_idx);
/// - 其余节点递归改写,叶子按普通表达式解析。
struct AggRewriter<'a> {
    scope: Option<&'a Scope>,
    input_schema: &'a [ColumnDesc],
    groups: Vec<(Expr, String)>,
    aggs: Vec<(AggDesc, String)>,
    /// 所有 GROUP BY 项引用到的输入列(允许在投影/HAVING 中出现)
    group_ref_cols: std::collections::HashSet<usize>,
}

impl<'a> AggRewriter<'a> {
    fn new(scope: Option<&'a Scope>, input_schema: &'a [ColumnDesc]) -> Self {
        Self {
            scope,
            input_schema,
            groups: Vec::new(),
            aggs: Vec::new(),
            group_ref_cols: std::collections::HashSet::new(),
        }
    }

    /// GROUP BY 项解析(纯普通表达式;不允许聚合)。
    fn add_group(&mut self, e: &SE) -> Result<()> {
        let expr = plan_expr(e, self.scope)?;
        if is_aggregate_call(e) {
            return Err(Error::sql(
                "aggregate functions are not allowed in GROUP BY",
            ));
        }
        collect_column_refs(&expr, &mut self.group_ref_cols);
        let display = format!("{expr}");
        if self.groups.iter().any(|(_, d)| *d == display) {
            return Ok(()); // 重复分组项
        }
        self.groups.push((expr, display));
        Ok(())
    }

    /// 聚合函数调用 → AggDesc(去重);返回其在聚合输出中的下标(group_n + idx)。
    fn add_agg(&mut self, f: &sqlparser::ast::Function) -> Result<usize> {
        let name = object_name_to_string(&f.name)?.to_ascii_lowercase();
        let arg_expr = self.function_single_arg(f)?;
        let func = match (name.as_str(), &arg_expr) {
            ("count", None) => AggFunc::CountStar,
            ("count", Some(_)) => AggFunc::Count,
            ("sum", _) => AggFunc::Sum,
            ("avg", _) => AggFunc::Avg,
            ("min", _) => AggFunc::Min,
            ("max", _) => AggFunc::Max,
            (other, _) => {
                return Err(Error::not_supported(format!("function {other}()")));
            }
        };
        let display = format!(
            "{}({})",
            func.name(),
            arg_expr
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "*".into())
        );
        if let Some(idx) = self.aggs.iter().position(|(_, d)| *d == display) {
            return Ok(self.groups.len() + idx);
        }
        let desc = AggDesc::new(func, arg_expr, self.input_schema)?;
        self.aggs.push((desc, display));
        Ok(self.groups.len() + self.aggs.len() - 1)
    }

    /// 单参函数的参数;COUNT(*) → None。
    fn function_single_arg(&mut self, f: &sqlparser::ast::Function) -> Result<Option<Expr>> {
        if f.filter.is_some() || f.over.is_some() || !f.within_group.is_empty() {
            return Err(Error::not_supported(
                "FILTER/OVER/WITHIN GROUP on aggregate functions",
            ));
        }
        let sqlparser::ast::FunctionArguments::List(list) = &f.args else {
            return Err(Error::not_supported("aggregate function argument form"));
        };
        if let Some(dup) = &list.duplicate_treatment {
            if matches!(dup, sqlparser::ast::DuplicateTreatment::Distinct) {
                return Err(Error::not_supported("DISTINCT aggregates"));
            }
        }
        let mut arg: Option<Expr> = None;
        let mut n = 0usize;
        for fa in &list.args {
            let sqlparser::ast::FunctionArg::Unnamed(fae) = fa else {
                return Err(Error::not_supported("named function arguments"));
            };
            n += 1;
            if n > 1 {
                return Err(Error::not_supported(
                    "aggregate functions take at most one argument",
                ));
            }
            match fae {
                sqlparser::ast::FunctionArgExpr::Expr(e) => {
                    let inner = self.rewrite(e, true)?;
                    arg = Some(inner);
                }
                sqlparser::ast::FunctionArgExpr::Wildcard => arg = None,
                other => {
                    return Err(Error::not_supported(format!("function argument {other:?}")));
                }
            }
        }
        if n == 0 {
            let name = object_name_to_string(&f.name)?.to_ascii_lowercase();
            if name != "count" {
                return Err(Error::sql(format!(
                    "{}() requires an argument",
                    name.to_uppercase()
                )));
            }
            arg = None;
        }
        Ok(arg)
    }

    /// 表达式改写主入口:
    /// 1. 聚合调用 → 引用聚合输出列;
    /// 2. 整体(或子树)与某 GROUP BY 项结构相同 → 引用分组输出列;
    /// 3. 复合节点递归改写;叶子既非 literal 也未匹配 group → 报错。
    fn rewrite(&mut self, e: &SE, inside_agg: bool) -> Result<Expr> {
        if let SE::Function(f) = e {
            let name = object_name_to_string(&f.name)?.to_ascii_lowercase();
            if is_aggregate_name(&name) {
                if inside_agg {
                    return Err(Error::sql("nested aggregate functions are not allowed"));
                }
                let idx = self.add_agg(f)?;
                return Ok(Expr::Column(idx));
            }
            // 标量函数:参数递归改写(聚合上下文中可用,如 SUM(ABS(x)))
            let args = function_call_args(f)?
                .iter()
                .map(|a| self.rewrite(a, inside_agg))
                .collect::<Result<Vec<_>>>()?;
            let planned = Expr::Function {
                name: name.clone(),
                args,
            };
            if !inside_agg {
                let display = format!("{planned}");
                if let Some(idx) = self.groups.iter().position(|(_, d)| *d == display) {
                    return Ok(Expr::Column(idx));
                }
                let mut refs = std::collections::HashSet::new();
                collect_column_refs(&planned, &mut refs);
                for r in refs {
                    if !self.group_ref_cols.contains(&r) {
                        return Err(Error::sql(format!(
                            "column #{r} must appear in the GROUP BY clause or be used in an aggregate function"
                        )));
                    }
                }
            }
            return Ok(planned);
        }

        // 整体尝试按普通表达式解析并匹配 GROUP BY 项(如 HAVING dept = 'eng')
        if !inside_agg {
            if let Some(plain) = self.try_plain(e) {
                let display = format!("{plain}");
                if let Some(idx) = self.groups.iter().position(|(_, d)| *d == display) {
                    return Ok(Expr::Column(idx));
                }
                // 叶子:literal 放行;identifier 必须已匹配 group
                match e {
                    SE::Identifier(_) | SE::CompoundIdentifier(_) => {
                        return Err(Error::sql(format!(
                            "column '{display}' must appear in the GROUP BY clause or be used in an aggregate function"
                        )));
                    }
                    SE::Value(_) => return Ok(plain),
                    _ => {}
                }
            } else if matches!(e, SE::Identifier(_) | SE::CompoundIdentifier(_)) {
                return Err(Error::sql(
                    "column must appear in the GROUP BY clause or be used in an aggregate function",
                ));
            }
        }

        let planned = match e {
            SE::BinaryOp { left, op, right } => Expr::binary(
                map_binary_op(op)?,
                self.rewrite(left, inside_agg)?,
                self.rewrite(right, inside_agg)?,
            ),
            SE::Nested(inner) => self.rewrite(inner, inside_agg)?,
            SE::UnaryOp { op, expr } => {
                let inner = self.rewrite(expr, inside_agg)?;
                match op {
                    UnaryOperator::Plus => inner,
                    UnaryOperator::Minus => match inner {
                        Expr::Literal(ScalarValue::Int64(v)) => {
                            Expr::Literal(ScalarValue::Int64(-v))
                        }
                        Expr::Literal(ScalarValue::Float64(v)) => {
                            Expr::Literal(ScalarValue::Float64(-v))
                        }
                        _ => return Err(Error::not_supported("unary minus on non-literal")),
                    },
                    other => {
                        return Err(Error::not_supported(format!("unary operator {other:?}")));
                    }
                }
            }
            other => plan_expr(other, self.scope)?,
        };
        Ok(planned)
    }

    /// 尝试按普通表达式(无聚合)解析;失败返回 None。
    fn try_plain(&self, e: &SE) -> Option<Expr> {
        match e {
            SE::Function(_) => None,
            SE::BinaryOp { left, op, right } => Some(Expr::binary(
                map_binary_op(op).ok()?,
                self.try_plain(left)?,
                self.try_plain(right)?,
            )),
            SE::Nested(inner) => self.try_plain(inner),
            SE::UnaryOp { op, expr } => {
                let inner = self.try_plain(expr)?;
                match op {
                    UnaryOperator::Plus => Some(inner),
                    UnaryOperator::Minus => match inner {
                        Expr::Literal(ScalarValue::Int64(v)) => {
                            Some(Expr::Literal(ScalarValue::Int64(-v)))
                        }
                        Expr::Literal(ScalarValue::Float64(v)) => {
                            Some(Expr::Literal(ScalarValue::Float64(-v)))
                        }
                        _ => None,
                    },
                    _ => None,
                }
            }
            other => plan_expr(other, self.scope).ok(),
        }
    }

    /// 聚合输出 schema(groups + aggs)。
    fn agg_output_schema(&self) -> Vec<ColumnDesc> {
        let mut out = Vec::new();
        for (i, (expr, _)) in self.groups.iter().enumerate() {
            let ty = expr
                .return_type(self.input_schema)
                .ok()
                .flatten()
                .unwrap_or(DataType::Int64);
            out.push(ColumnDesc::new(format!("__group{i}"), ty, true));
        }
        for (i, (desc, _)) in self.aggs.iter().enumerate() {
            out.push(ColumnDesc::new(
                format!("__agg{i}"),
                desc.out_type.clone(),
                true,
            ));
        }
        out
    }

    /// 交给 AggHashExec 的流水线部件。
    fn take_pipeline_parts(&mut self) -> (Vec<Expr>, Vec<AggDesc>, Vec<ColumnDesc>) {
        let schema = self.agg_output_schema();
        (
            self.groups.iter().map(|(e, _)| e.clone()).collect(),
            self.aggs.iter().map(|(d, _)| d.clone()).collect(),
            schema,
        )
    }
}

// ---------- INSERT ----------

fn plan_insert(insert: &sqlparser::ast::Insert, ctx: &PlanContext<'_>) -> Result<StatementPlan> {
    let sqlparser::ast::TableObject::TableName(name) = &insert.table else {
        return Err(Error::not_supported("INSERT INTO <table function>"));
    };
    let table = resolve_table(ctx.catalog, &object_name_to_string(name)?)?;

    let Some(source) = &insert.source else {
        return Err(Error::not_supported("INSERT requires a source"));
    };

    // 目标列:未给列清单 = 全列;给了则解析下标(columns 是 ObjectName 列表)
    let targets: Vec<usize> = if insert.columns.is_empty() {
        (0..table.columns.len()).collect()
    } else {
        insert
            .columns
            .iter()
            .map(|col_name| {
                let ident = object_name_to_string(col_name)?;
                find_column(&table, &ident).ok_or_else(|| {
                    Error::catalog(format!(
                        "column '{}' of table '{}' not found",
                        ident, table.name
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?
    };

    match source.body.as_ref() {
        SetExpr::Values(values) => {
            let mut rows: Vec<Row> = Vec::with_capacity(values.rows.len());
            for tuple_parens in &values.rows {
                let tuple: &Vec<SE> = &tuple_parens.content;
                if tuple.len() != targets.len() {
                    return Err(Error::sql(format!(
                        "INSERT expects {} values per tuple, got {}",
                        targets.len(),
                        tuple.len()
                    )));
                }
                let mut full: Row = vec![ScalarValue::Null; table.columns.len()];
                for (col_idx, e) in targets.iter().zip(tuple) {
                    let expr = plan_expr(e, None)?;
                    let value = eval_const_scalar(&expr)?;
                    full[*col_idx] = coerce_value(value, &table.columns[*col_idx])?;
                }
                rows.push(full);
            }
            Ok(StatementPlan::Insert { table, rows })
        }
        SetExpr::Select(select) => {
            let query = plan_select(select, &[], ctx)?;
            check_insert_select_arity(&table, &targets, &query)?;
            Ok(StatementPlan::InsertSelect {
                table,
                targets,
                query,
            })
        }
        SetExpr::Query(query) => {
            let planned = plan_query(query, ctx)?;
            check_insert_select_arity(&table, &targets, &planned)?;
            Ok(StatementPlan::InsertSelect {
                table,
                targets,
                query: planned,
            })
        }
        _ => Err(Error::not_supported(
            "INSERT source must be VALUES or SELECT",
        )),
    }
}

fn check_insert_select_arity(
    table: &TableSchema,
    targets: &[usize],
    query: &PlannedQuery,
) -> Result<()> {
    if query.schema.len() != targets.len() {
        return Err(Error::sql(format!(
            "INSERT SELECT: query produces {} columns, expected {}",
            query.schema.len(),
            targets.len()
        )));
    }
    // 静态类型检查:Int64→Float64 可提升;裸 NULL(投影类型回退 BIGINT)会被拒,
    // 放行到执行期收敛的优化 TODO
    for qi in 0..query.schema.len() {
        let ti = targets[qi];
        let col = &table.columns[ti];
        let qt = query.schema[qi].ty.clone();
        if qt != col.ty && !(qt == DataType::Int64 && col.ty == DataType::Float64) {
            return Err(Error::invalid_argument(format!(
                "INSERT SELECT column {qi}: type mismatch for '{}' ({} vs {})",
                col.name,
                col.ty.name(),
                qt.name()
            )));
        }
    }
    Ok(())
}

// ---------- UPDATE ----------

fn plan_update(update: &sqlparser::ast::Update, ctx: &PlanContext<'_>) -> Result<StatementPlan> {
    if update.from.is_some() {
        return Err(Error::not_supported("UPDATE ... FROM"));
    }
    let TableFactor::Table { name, .. } = &update.table.relation else {
        return Err(Error::not_supported("UPDATE (subquery / function)"));
    };
    let table = resolve_table(ctx.catalog, &object_name_to_string(name)?)?;

    let mut assignments: Vec<(usize, Expr)> = Vec::with_capacity(update.assignments.len());
    for a in &update.assignments {
        // target 可能带表限定(t.col),取最后一段;多列元组赋值不支持
        let sqlparser::ast::AssignmentTarget::ColumnName(col_object) = &a.target else {
            return Err(Error::not_supported("tuple assignment target"));
        };
        let col_name = object_name_to_string(col_object)?;
        let idx = find_column(&table, &col_name).ok_or_else(|| {
            Error::catalog(format!(
                "column '{}' of table '{}' not found",
                col_name, table.name
            ))
        })?;
        if assignments.iter().any(|(i, _)| *i == idx) {
            return Err(Error::sql(format!(
                "multiple assignments to column '{}'",
                col_name
            )));
        }
        let expr = plan_expr(&a.value, Some(&Scope::single(&table)))?;
        match expr.return_type(&table.columns)? {
            Some(t) if t == table.columns[idx].ty => {}
            // 字符串字面量赋给 LOB/数组/向量列:执行期按目标类型解析
            Some(DataType::Utf8)
                if matches!(
                    table.columns[idx].ty,
                    DataType::Binary | DataType::Vector(_) | DataType::Array(_)
                ) => {}
            Some(DataType::Int64) if table.columns[idx].ty == DataType::Float64 => {}
            None => {}
            Some(t) => {
                return Err(Error::invalid_argument(format!(
                    "type mismatch assigning to column '{}': expected {}, got {t:?}",
                    col_name,
                    table.columns[idx].ty.name()
                )));
            }
        }
        assignments.push((idx, expr));
    }
    if assignments.is_empty() {
        return Err(Error::sql("UPDATE without assignments"));
    }

    let predicate = match &update.selection {
        Some(e) => {
            let expr = plan_expr(e, Some(&Scope::single(&table)))?;
            match expr.return_type(&table.columns)? {
                Some(DataType::Boolean) | None => {}
                Some(t) => return Err(Error::sql(format!("WHERE must be boolean, got {t:?}"))),
            }
            Some(expr)
        }
        None => None,
    };
    Ok(StatementPlan::Update {
        table,
        assignments,
        predicate,
    })
}

// ---------- DELETE ----------

fn plan_delete(delete: &sqlparser::ast::Delete, ctx: &PlanContext<'_>) -> Result<StatementPlan> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let [twj] = &tables[..] else {
        return Err(Error::not_supported("DELETE FROM must target one table"));
    };
    if !twj.joins.is_empty() {
        return Err(Error::not_supported("DELETE with JOIN"));
    }
    let TableFactor::Table { name, .. } = &twj.relation else {
        return Err(Error::not_supported("DELETE FROM (subquery / function)"));
    };
    let table = resolve_table(ctx.catalog, &object_name_to_string(name)?)?;
    let predicate = match &delete.selection {
        Some(e) => {
            let expr = plan_expr(e, Some(&Scope::single(&table)))?;
            match expr.return_type(&table.columns)? {
                Some(DataType::Boolean) | None => {}
                Some(t) => return Err(Error::sql(format!("WHERE must be boolean, got {t:?}"))),
            }
            Some(expr)
        }
        None => None,
    };
    Ok(StatementPlan::Delete { table, predicate })
}

// ---------- CREATE / DROP TABLE ----------

fn plan_create_table(create: &sqlparser::ast::CreateTable) -> Result<StatementPlan> {
    let name = object_name_to_string(&create.name)?;
    if name.is_empty() {
        return Err(Error::sql("table name is empty"));
    }
    let mut columns = Vec::with_capacity(create.columns.len());
    let mut pk_from_column: Option<String> = None;
    for col in &create.columns {
        let ty = map_sql_type(&col.data_type)?;
        let mut nullable = true;
        let mut is_pk = false;
        for opt in &col.options {
            match &opt.option {
                sqlparser::ast::ColumnOption::NotNull => nullable = false,
                sqlparser::ast::ColumnOption::Null => nullable = true,
                sqlparser::ast::ColumnOption::PrimaryKey { .. } => {
                    is_pk = true;
                    nullable = false;
                }
                _ => {}
            }
        }
        if is_pk {
            if let Some(existing) = &pk_from_column {
                return Err(Error::sql(format!(
                    "multiple primary keys: '{}' and '{}'",
                    existing, col.name.value
                )));
            }
            pk_from_column = Some(col.name.value.clone());
        }
        columns.push(ColumnDesc::new(col.name.value.clone(), ty, nullable));
    }
    // 列名去重
    for i in 0..columns.len() {
        for later in &columns[i + 1..] {
            if columns[i].name.eq_ignore_ascii_case(&later.name) {
                return Err(Error::sql(format!(
                    "duplicate column name '{}'",
                    columns[i].name
                )));
            }
        }
    }

    // 主键:列级 PRIMARY KEY 或表级 PRIMARY KEY (...);用户主键 = 唯一索引,
    // 物理行键仍是隐藏 row_id。
    let mut primary_key = Vec::new();
    if let Some(col) = &pk_from_column {
        primary_key.push(col.clone());
    }
    for constraint in &create.constraints {
        let sqlparser::ast::TableConstraint::PrimaryKey(pk) = constraint else {
            continue;
        };
        if !primary_key.is_empty() {
            return Err(Error::sql("multiple PRIMARY KEY constraints"));
        }
        for ic in &pk.columns {
            let sqlparser::ast::Expr::Identifier(ident) = &ic.column.expr else {
                return Err(Error::not_supported("non-identifier PRIMARY KEY column"));
            };
            primary_key.push(ident.value.clone());
        }
    }
    if !primary_key.is_empty() {
        // 主键列必须存在且可排序(非 BLOB),并强制 NOT NULL
        for pk_name in &primary_key {
            let col_idx = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(pk_name))
                .ok_or_else(|| {
                    Error::catalog(format!("primary key column '{pk_name}' not found"))
                })?;
            if !columns[col_idx].ty.is_scalar() && columns[col_idx].ty != DataType::Binary {
                return Err(Error::not_supported(format!(
                    "primary key column '{}' must be a scalar or BLOB type",
                    pk_name
                )));
            }
            columns[col_idx].nullable = false;
        }
    }

    Ok(StatementPlan::CreateTable {
        schema: TableSchema {
            id: 0, // catalog 分配
            name,
            columns,
            primary_key,
            version: 1,
        },
    })
}

// ---------- 表达式 ----------

/// SQL 表达式 → 执行表达式。`scope` 提供列解析;无 FROM 时不允许列引用。
pub fn plan_expr(e: &SE, scope: Option<&Scope>) -> Result<Expr> {
    match e {
        SE::Value(v) => Ok(Expr::Literal(value_to_scalar(&v.value)?)),
        SE::Array(arr) => {
            // 数组字面量:元素必须为常量(列引用/聚合在数组里不支持)
            let mut items = Vec::with_capacity(arr.elem.len());
            for elem in &arr.elem {
                let expr = plan_expr(elem, None)?;
                match expr {
                    Expr::Literal(v) => items.push(v),
                    other => {
                        return Err(Error::not_supported(format!(
                            "array literal elements must be constants, got {other}"
                        )));
                    }
                }
            }
            Ok(Expr::Literal(ScalarValue::List(items)))
        }
        SE::TypedString(ts) => match ts.data_type {
            SqlType::Date => {
                let text = literal_string(&ts.value.value)
                    .ok_or_else(|| Error::sql("DATE literal expects a string value"))?;
                Ok(Expr::Literal(common::types::parse_date_text(&text)?))
            }
            ref other => Err(Error::not_supported(format!("typed literal {other}"))),
        },
        SE::Interval(iv) => {
            // 仅支持 DATE 算术所需的单位:INTERVAL '<n>' DAY → Int64 天数
            let Some(field) = iv.leading_field.as_ref() else {
                return Err(Error::not_supported("INTERVAL requires an explicit unit"));
            };
            if !matches!(field, sqlparser::ast::DateTimeField::Day) {
                return Err(Error::not_supported(format!("INTERVAL unit {field}")));
            }
            let SE::Value(v) = &*iv.value else {
                return Err(Error::not_supported("INTERVAL value must be a literal"));
            };
            let text = literal_string(&v.value)
                .ok_or_else(|| Error::sql("INTERVAL value expects a string literal"))?;
            let days: i64 = text
                .trim()
                .parse()
                .map_err(|_| Error::sql(format!("invalid INTERVAL day value '{text}'")))?;
            Ok(Expr::Literal(ScalarValue::Int64(days)))
        }
        SE::Extract { field, expr, .. } => {
            // EXTRACT(YEAR FROM d) → year(d);MONTH → month(d)
            let fname = match field {
                sqlparser::ast::DateTimeField::Year | sqlparser::ast::DateTimeField::Years => {
                    "year"
                }
                sqlparser::ast::DateTimeField::Month | sqlparser::ast::DateTimeField::Months => {
                    "month"
                }
                other => {
                    return Err(Error::not_supported(format!("EXTRACT field {other}")));
                }
            };
            let arg = plan_expr(expr, scope)?;
            Ok(Expr::Function {
                name: fname.to_string(),
                args: vec![arg],
            })
        }
        SE::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            // SUBSTRING(s [FROM a] [FOR b]) → substring(s, a, b);缺省 a=1,b=length(s)
            let s = plan_expr(expr, scope)?;
            let from = match substring_from {
                Some(f) => plan_expr(f, scope)?,
                None => Expr::Literal(ScalarValue::Int64(1)),
            };
            let for_len = match substring_for {
                Some(f) => plan_expr(f, scope)?,
                None => Expr::Function {
                    name: "length".to_string(),
                    args: vec![s.clone()],
                },
            };
            Ok(Expr::Function {
                name: "substring".to_string(),
                args: vec![s, from, for_len],
            })
        }
        SE::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let l = plan_expr(expr, scope)?;
            let r = plan_expr(pattern, scope)?;
            let op = if *negated {
                BinaryOp::NotLike
            } else {
                BinaryOp::Like
            };
            Ok(Expr::binary(op, l, r))
        }
        SE::Function(f) => {
            let name = object_name_to_string(&f.name)?.to_ascii_lowercase();
            if is_aggregate_name(&name) {
                return Err(Error::sql(format!(
                    "aggregate function {name}() is not allowed here (use HAVING in aggregate queries)"
                )));
            }
            if !exec::functions::is_scalar_function(&name) {
                return Err(Error::not_supported(format!("function {name}()")));
            }
            let args = scalar_function_args(f, |e| plan_expr(e, scope))?;
            Ok(Expr::Function { name, args })
        }
        SE::BinaryOp { left, op, right } => {
            let l = plan_expr(left, scope)?;
            let r = plan_expr(right, scope)?;
            Ok(Expr::binary(map_binary_op(op)?, l, r))
        }
        SE::Nested(inner) => plan_expr(inner, scope),
        SE::Identifier(ident) => {
            let Some(scope) = scope else {
                return Err(Error::not_implemented(
                    "column reference requires FROM clause",
                ));
            };
            let idx = scope.resolve(None, &ident.value)?;
            Ok(Expr::Column(idx))
        }
        SE::CompoundIdentifier(parts) => {
            let Some(scope) = scope else {
                return Err(Error::not_implemented(
                    "column reference requires FROM clause",
                ));
            };
            if parts.len() == 2 {
                let idx = scope.resolve(Some(&parts[0].value), &parts[1].value)?;
                return Ok(Expr::Column(idx));
            }
            Err(Error::not_supported(format!(
                "qualified identifier {:?}",
                parts.iter().map(|p| p.value.clone()).collect::<Vec<_>>()
            )))
        }
        SE::UnaryOp { op, expr } => match op {
            UnaryOperator::Plus => plan_expr(expr, scope),
            UnaryOperator::Minus => match plan_expr(expr, scope)? {
                Expr::Literal(ScalarValue::Int64(v)) => Ok(Expr::Literal(ScalarValue::Int64(-v))),
                Expr::Literal(ScalarValue::Float64(v)) => {
                    Ok(Expr::Literal(ScalarValue::Float64(-v)))
                }
                _ => Err(Error::not_supported("unary minus on non-literal")),
            },
            other => Err(Error::not_supported(format!("unary operator {other:?}"))),
        },
        other => Err(Error::not_supported(format!("expression: {}", other))),
    }
}

/// 字符串字面量取值(SingleQuoted/DoubleQuoted)。
fn literal_string(v: &Value) -> Option<String> {
    match v {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Some(s.clone()),
        _ => None,
    }
}

fn value_to_scalar(v: &Value) -> Result<ScalarValue> {
    match v {
        Value::Number(text, _long) => {
            if let Ok(i) = text.parse::<i64>() {
                Ok(ScalarValue::Int64(i))
            } else if let Ok(f) = text.parse::<f64>() {
                Ok(ScalarValue::Float64(f))
            } else {
                Err(Error::sql(format!("unsupported number literal '{text}'")))
            }
        }
        Value::Boolean(b) => Ok(ScalarValue::Boolean(*b)),
        Value::SingleQuotedString(s) => Ok(ScalarValue::Utf8(s.clone())),
        Value::DoubleQuotedString(s) => Ok(ScalarValue::Utf8(s.clone())),
        Value::Null => Ok(ScalarValue::Null),
        other => Err(Error::not_supported(format!("literal {other:?}"))),
    }
}

fn map_binary_op(op: &BinaryOperator) -> Result<BinaryOp> {
    match op {
        BinaryOperator::Plus => Ok(BinaryOp::Add),
        BinaryOperator::Minus => Ok(BinaryOp::Sub),
        BinaryOperator::Multiply => Ok(BinaryOp::Mul),
        BinaryOperator::Divide => Ok(BinaryOp::Div),
        BinaryOperator::Eq => Ok(BinaryOp::Eq),
        BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
        BinaryOperator::Lt => Ok(BinaryOp::Lt),
        BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
        BinaryOperator::Gt => Ok(BinaryOp::Gt),
        BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
        BinaryOperator::And => Ok(BinaryOp::And),
        BinaryOperator::Or => Ok(BinaryOp::Or),
        other => Err(Error::not_supported(format!("binary operator {}", other))),
    }
}

/// INSERT VALUES 的常量求值:在单行 dummy 批上执行表达式。
fn eval_const_scalar(e: &Expr) -> Result<ScalarValue> {
    let chunk = DataChunk::new(vec![
        Vector::from_scalars(DataType::Int64, std::iter::once(ScalarValue::Null)).unwrap(),
    ])?;
    Ok(e.eval(&chunk)?.scalar_at(0))
}

/// 按列类型收敛值:精确匹配 / Int64→Float64 提升 / NULL 检查 / 字符串字面量
/// 解析为数组、向量(PG 风格 `'[1,2]'`)。
pub fn coerce_value(v: ScalarValue, col: &ColumnDesc) -> Result<ScalarValue> {
    match v {
        ScalarValue::Null if col.nullable => Ok(ScalarValue::Null),
        ScalarValue::Null => Err(Error::sql(format!(
            "column '{}' is NOT NULL, but NULL was provided",
            col.name
        ))),
        ScalarValue::Int64(i) if col.ty == DataType::Float64 => Ok(ScalarValue::Float64(i as f64)),
        // '文本' → 二进制/数组/向量(PG 风格:'\xDEADBEEF' / '[1.0,2.0]' / '{1,2}')
        ScalarValue::Utf8(s)
            if matches!(
                col.ty,
                DataType::Vector(_) | DataType::Array(_) | DataType::Binary
            ) =>
        {
            ScalarValue::from_text(col.ty.clone(), &s)
        }
        v if v.data_type() == Some(col.ty.clone()) => Ok(v),
        // 数组:逐元素收敛(字面量元素类型可能未知或 NULL)
        ScalarValue::List(items) if matches!(col.ty, DataType::Array(_)) => {
            let DataType::Array(elem) = &col.ty else {
                unreachable!("guarded above");
            };
            let elem_desc = ColumnDesc::new("element".to_string(), (**elem).clone(), true);
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(coerce_value(item, &elem_desc)?);
            }
            Ok(ScalarValue::List(out))
        }
        v => Err(Error::invalid_argument(format!(
            "type mismatch for column '{}': expected {}, got {:?}",
            col.name,
            col.ty.name(),
            v
        ))),
    }
}

fn literal_u64(e: &SE, what: &str) -> Result<u64> {
    match e {
        SE::Value(v) => match &v.value {
            Value::Number(text, _) => text.parse::<u64>().map_err(|_| {
                Error::sql(format!(
                    "{what} must be a non-negative integer, got '{text}'"
                ))
            }),
            _ => Err(Error::sql(format!("{what} must be an integer literal"))),
        },
        _ => Err(Error::sql(format!("{what} must be an integer literal"))),
    }
}

// ---------- 名称与类型解析 ----------

fn object_name_to_string(name: &ObjectName) -> Result<String> {
    let mut parts = Vec::with_capacity(name.0.len());
    for part in &name.0 {
        match part.as_ident() {
            Some(ident) => parts.push(ident.value.clone()),
            None => return Err(Error::not_supported("non-identifier object name")),
        }
    }
    if parts.is_empty() {
        return Err(Error::sql("empty object name"));
    }
    if parts.len() > 1 {
        return Err(Error::not_supported(format!(
            "qualified object name '{}'",
            parts.join(".")
        )));
    }
    Ok(parts.remove(0))
}

fn resolve_table(catalog: &dyn CatalogStore, name: &str) -> Result<TableSchema> {
    if let Some(t) = catalog.get_table(name) {
        return Ok(t);
    }
    if let Some(t) = catalog
        .list_tables()
        .into_iter()
        .find(|t| t.name.eq_ignore_ascii_case(name))
    {
        return Ok(t);
    }
    Err(Error::catalog(format!("table '{name}' not found")))
}

fn find_column(schema: &TableSchema, name: &str) -> Option<usize> {
    if let Some(i) = schema.column_index(name) {
        return Some(i);
    }
    schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
}

fn map_sql_type(ty: &SqlType) -> Result<DataType> {
    match ty {
        SqlType::BigInt(_) | SqlType::Int8(_) | SqlType::Int8Unsigned(_) => Ok(DataType::Int64),
        SqlType::Int(_) | SqlType::Integer(_) | SqlType::Int4(_) | SqlType::Int4Unsigned(_) => {
            Ok(DataType::Int64)
        }
        SqlType::Real
        | SqlType::Float(_)
        | SqlType::Double(_)
        | SqlType::DoublePrecision
        | SqlType::DoublePrecisionUnsigned => Ok(DataType::Float64),
        // DECIMAL/NUMERIC:无精确小数类型,统一收敛到 Float64(TPC-H 数值列够用)
        SqlType::Decimal(_) | SqlType::Numeric(_) | SqlType::DecimalUnsigned(_) => {
            Ok(DataType::Float64)
        }
        SqlType::Date | SqlType::Datetime(_) | SqlType::Timestamp(_, _) => Ok(DataType::Date),
        SqlType::Boolean | SqlType::Bool => Ok(DataType::Boolean),
        SqlType::Text
        | SqlType::Varchar(_)
        | SqlType::Char(_)
        | SqlType::CharacterVarying(_)
        | SqlType::Character(_)
        | SqlType::String(_) => Ok(DataType::Utf8),
        // LOB:BLOB / BINARY / VARBINARY / BYTEA 均映射为二进制;TEXT 兼作 CLOB
        SqlType::Blob(_) | SqlType::Binary(_) | SqlType::Varbinary(_) | SqlType::Bytea => {
            Ok(DataType::Binary)
        }
        // 数组:`BIGINT[]` / `ARRAY<BIGINT>`(一维,元素必须为标量)
        SqlType::Array(elem) => match elem {
            sqlparser::ast::ArrayElemTypeDef::SquareBracket(t, _)
            | sqlparser::ast::ArrayElemTypeDef::AngleBracket(t)
            | sqlparser::ast::ArrayElemTypeDef::Parenthesis(t)
            | sqlparser::ast::ArrayElemTypeDef::ParenthesisNotNull(t)
            | sqlparser::ast::ArrayElemTypeDef::Qualified(t, _) => {
                let inner = map_sql_type(t)?;
                if !inner.is_scalar() {
                    return Err(Error::not_supported("nested array types"));
                }
                Ok(DataType::Array(Box::new(inner)))
            }
            sqlparser::ast::ArrayElemTypeDef::None => {
                Err(Error::not_supported("ARRAY without element type"))
            }
        },
        // 向量:VECTOR(n)(自定义类型)
        SqlType::Custom(name, mods) => {
            let base = object_name_to_string(name)?.to_ascii_lowercase();
            if base == "vector" {
                let dim_text = mods
                    .first()
                    .map(|m| m.to_string())
                    .ok_or_else(|| Error::sql("VECTOR requires a dimension: VECTOR(n)"))?;
                let dim: usize = dim_text
                    .parse()
                    .map_err(|_| Error::sql(format!("bad VECTOR dimension '{dim_text}'")))?;
                if dim == 0 || dim > 16384 {
                    return Err(Error::sql(format!(
                        "VECTOR dimension {dim} out of range (1..=16384)"
                    )));
                }
                return Ok(DataType::Vector(dim));
            }
            Err(Error::not_supported(format!(
                "column type {}",
                base.to_uppercase()
            )))
        }
        other => Err(Error::not_supported(format!("column type {}", other))),
    }
}

// ---------- 测试 ----------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_single;
    use catalog::InMemoryCatalog;
    use exec::executor::ExecuteContext;
    use memory::{MemoryContext, MemoryTracker};

    struct DummySource;

    impl QueryDataSource for DummySource {
        fn scan_table(&self, _table: &TableSchema) -> Result<Vec<Row>> {
            Ok(Vec::new())
        }
    }

    fn dummy_source() -> Arc<dyn QueryDataSource> {
        Arc::new(DummySource)
    }

    fn table_t() -> TableSchema {
        TableSchema {
            id: 1,
            name: "t".into(),
            columns: vec![
                ColumnDesc::new("id", DataType::Int64, false),
                ColumnDesc::new("name", DataType::Utf8, true),
                ColumnDesc::new("score", DataType::Float64, true),
            ],
            primary_key: Vec::new(),
            version: 1,
        }
    }

    fn sample_rows() -> Vec<Row> {
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Utf8("eng".into()),
                ScalarValue::Float64(100.0),
            ],
            vec![
                ScalarValue::Int64(2),
                ScalarValue::Utf8("eng".into()),
                ScalarValue::Float64(200.0),
            ],
            vec![
                ScalarValue::Int64(3),
                ScalarValue::Utf8("ops".into()),
                ScalarValue::Null,
            ],
            vec![
                ScalarValue::Int64(4),
                ScalarValue::Utf8("ops".into()),
                ScalarValue::Float64(50.0),
            ],
        ]
    }

    struct RowsSource(Vec<Row>);

    impl QueryDataSource for RowsSource {
        fn scan_table(&self, _table: &TableSchema) -> Result<Vec<Row>> {
            Ok(self.0.clone())
        }
    }

    fn run_query_over(sql: &str, rows: Vec<Row>) -> Vec<Vec<ScalarValue>> {
        let catalog = InMemoryCatalog::new();
        catalog.create_table(table_t()).unwrap();
        let stmt = parse_single(sql).unwrap();
        let StatementPlan::Query(pq) =
            plan_statement(&stmt, &make_ctx(&catalog, Arc::new(RowsSource(rows)))).unwrap()
        else {
            panic!("expected query plan");
        };
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(1024, MemoryContext::new(&root, "q", 4096));
        let mut out = Vec::new();
        let mut exec = pq.executor;
        while let Some(chunk) = exec.next_batch(&mut ctx).unwrap() {
            for i in 0..chunk.num_rows() {
                out.push(chunk.row(i));
            }
        }
        out
    }

    fn make_ctx<'a>(
        catalog: &'a InMemoryCatalog,
        data: Arc<dyn QueryDataSource>,
    ) -> PlanContext<'a> {
        PlanContext {
            batch_size: 1024,
            catalog,
            data,
        }
    }

    #[test]
    fn select_literal_and_arithmetic() {
        let catalog = InMemoryCatalog::new();
        let cases = [
            ("SELECT 1", vec![ScalarValue::Int64(1)]),
            ("SELECT 1 + 2 * 3 AS six", vec![ScalarValue::Int64(7)]),
            (
                "SELECT 'hello', 2.5",
                vec![ScalarValue::Utf8("hello".into()), ScalarValue::Float64(2.5)],
            ),
        ];
        for (sql, expect) in cases {
            let stmt = parse_single(sql).unwrap();
            let StatementPlan::Query(pq) =
                plan_statement(&stmt, &make_ctx(&catalog, dummy_source())).unwrap()
            else {
                panic!("expected query plan");
            };
            let root = MemoryTracker::new_root("t", None);
            let mut ctx = ExecuteContext::new(64, MemoryContext::new(&root, "q", 4096));
            let mut exec = pq.executor;
            let mut out = Vec::new();
            while let Some(chunk) = exec.next_batch(&mut ctx).unwrap() {
                for i in 0..chunk.num_rows() {
                    out.push(chunk.row(i));
                }
            }
            assert_eq!(out, vec![expect], "{sql}");
        }
    }

    #[test]
    fn select_star_with_filter() {
        let out = run_query_over("SELECT * FROM t WHERE id >= 3", sample_rows());
        assert_eq!(out.len(), 2);
        assert_eq!(out[0][0], ScalarValue::Int64(3));
        assert_eq!(out[1][0], ScalarValue::Int64(4));
    }

    #[test]
    fn scalar_functions() {
        // 常量上下文
        let out = run_query_over("SELECT UPPER('abc'), LENGTH('hello'), ABS(-5)", vec![]);
        assert_eq!(
            out,
            vec![vec![
                ScalarValue::Utf8("ABC".into()),
                ScalarValue::Int64(5),
                ScalarValue::Int64(5),
            ]]
        );

        // 列参数 + WHERE 中的函数
        let out = run_query_over(
            "SELECT UPPER(name) AS u, LENGTH(name) AS l FROM t WHERE LENGTH(name) >= 3",
            sample_rows(),
        );
        // name: eng/eng/ops/ops → LENGTH 全为 3
        assert_eq!(out.len(), 4);
        assert_eq!(out[0][0], ScalarValue::Utf8("ENG".into()));

        // COALESCE 跳过 NULL
        let out = run_query_over(
            "SELECT COALESCE(score, 0.0) FROM t WHERE id = 3",
            sample_rows(),
        );
        assert_eq!(out, vec![vec![ScalarValue::Float64(0.0)]]);
    }

    #[test]
    fn scalar_function_in_aggregate() {
        let out = run_query_over("SELECT SUM(ABS(score)) FROM t", sample_rows());
        // |100| + |200| + |NULL|跳过 + |50| = 350
        assert_eq!(out, vec![vec![ScalarValue::Float64(350.0)]]);
    }

    #[test]
    fn unknown_function_rejected() {
        let catalog = InMemoryCatalog::new();
        let stmt = parse_single("SELECT FOO(1)").unwrap();
        assert!(plan_statement(&stmt, &make_ctx(&catalog, dummy_source())).is_err());
    }

    #[test]
    fn group_by_count_sum_avg() {
        let out = run_query_over(
            "SELECT name, COUNT(*) AS n, SUM(score) AS s, AVG(score) AS a FROM t GROUP BY name",
            sample_rows(),
        );
        // 行排序后:eng / ops(NULL 排最前的组键排序:eng < ops)
        assert_eq!(out.len(), 2);
        let eng = out
            .iter()
            .find(|r| r[0] == ScalarValue::Utf8("eng".into()))
            .unwrap();
        assert_eq!(eng[1], ScalarValue::Int64(2));
        assert_eq!(eng[2], ScalarValue::Float64(300.0));
        assert_eq!(eng[3], ScalarValue::Float64(150.0));
        let ops = out
            .iter()
            .find(|r| r[0] == ScalarValue::Utf8("ops".into()))
            .unwrap();
        assert_eq!(ops[1], ScalarValue::Int64(2));
        assert_eq!(ops[2], ScalarValue::Float64(50.0), "SUM 忽略 NULL");
        assert_eq!(ops[3], ScalarValue::Float64(50.0));
    }

    #[test]
    fn global_aggregates() {
        let out = run_query_over(
            "SELECT COUNT(*), COUNT(score), SUM(score) FROM t",
            sample_rows(),
        );
        assert_eq!(
            out[0],
            vec![
                ScalarValue::Int64(4),
                ScalarValue::Int64(3),
                ScalarValue::Float64(350.0)
            ]
        );
    }

    #[test]
    fn global_aggregate_empty_table() {
        let out = run_query_over("SELECT COUNT(*), SUM(score) FROM t", Vec::new());
        assert_eq!(out, vec![vec![ScalarValue::Int64(0), ScalarValue::Null]]);
    }

    #[test]
    fn having_filters_groups() {
        let out = run_query_over(
            "SELECT name, SUM(score) AS s FROM t GROUP BY name HAVING SUM(score) > 200",
            sample_rows(),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][0], ScalarValue::Utf8("eng".into()));
        assert_eq!(out[0][1], ScalarValue::Float64(300.0));
    }

    #[test]
    fn expression_of_aggregates() {
        let out = run_query_over(
            "SELECT SUM(score) * 2 + COUNT(score) AS x FROM t",
            sample_rows(),
        );
        // 350 * 2 + 3
        assert_eq!(out, vec![vec![ScalarValue::Float64(703.0)]]);
    }

    #[test]
    fn non_grouped_column_rejected() {
        let catalog = InMemoryCatalog::new();
        catalog.create_table(table_t()).unwrap();
        let stmt = parse_single("SELECT id, COUNT(*) FROM t GROUP BY name").unwrap();
        assert!(plan_statement(&stmt, &make_ctx(&catalog, dummy_source())).is_err());
    }

    #[test]
    fn order_by_ordinal_name_and_expression() {
        let out = run_query_over("SELECT id, name FROM t ORDER BY 1 DESC", sample_rows());
        assert_eq!(out[0][0], ScalarValue::Int64(4));

        let out = run_query_over("SELECT id, name FROM t ORDER BY name", sample_rows());
        assert_eq!(out[0][1], ScalarValue::Utf8("eng".into()));

        // 表达式与投影匹配
        let out = run_query_over(
            "SELECT id, score * 2 AS d FROM t ORDER BY score * 2",
            sample_rows(),
        );
        // score: 100, 200, NULL, 50 → d: 200, 400, NULL(默认最小), 100
        assert_eq!(out[0][1], ScalarValue::Float64(100.0));
        assert_eq!(out[2][1], ScalarValue::Float64(400.0));
        assert_eq!(out[3][1], ScalarValue::Null);
    }

    #[test]
    fn order_by_hidden_key_column() {
        // ORDER BY 引用非输出列 → 隐藏键列
        let out = run_query_over(
            "SELECT name FROM t WHERE id <= 2 ORDER BY id DESC",
            sample_rows(),
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0][0], ScalarValue::Utf8("eng".into()));
    }

    #[test]
    fn order_by_with_aggregate_output() {
        let out = run_query_over(
            "SELECT name, COUNT(*) AS n FROM t GROUP BY name ORDER BY n DESC",
            sample_rows(),
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0][1], ScalarValue::Int64(2));
    }

    #[test]
    fn limit_after_sort() {
        let out = run_query_over(
            "SELECT id FROM t ORDER BY id DESC LIMIT 2 OFFSET 1",
            sample_rows(),
        );
        assert_eq!(
            out,
            vec![vec![ScalarValue::Int64(3)], vec![ScalarValue::Int64(2)]]
        );
    }

    #[test]
    fn insert_select_plan() {
        let catalog = InMemoryCatalog::new();
        catalog.create_table(table_t()).unwrap();
        let stmt =
            parse_single("INSERT INTO t (id, name) SELECT id, name FROM t WHERE id > 1").unwrap();
        match plan_statement(&stmt, &make_ctx(&catalog, dummy_source())).unwrap() {
            StatementPlan::InsertSelect { targets, .. } => {
                assert_eq!(targets, vec![0, 1]);
            }
            _ => panic!("expected insert-select plan"),
        }
    }

    #[test]
    fn update_plan_resolves_columns() {
        let catalog = InMemoryCatalog::new();
        catalog.create_table(table_t()).unwrap();
        let stmt = parse_single("UPDATE t SET score = score + 1, name = 'x' WHERE id = 5").unwrap();
        match plan_statement(&stmt, &make_ctx(&catalog, dummy_source())).unwrap() {
            StatementPlan::Update { assignments, .. } => {
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].0, 2);
                assert_eq!(assignments[1].0, 1);
            }
            _ => panic!("expected update plan"),
        }
    }

    #[test]
    fn update_rejects_bad_assignments() {
        let catalog = InMemoryCatalog::new();
        catalog.create_table(table_t()).unwrap();
        for sql in [
            "UPDATE t SET score = 'x' WHERE id = 1",
            "UPDATE t SET id = 1, id = 2",
            "UPDATE missing SET id = 1",
        ] {
            let stmt = parse_single(sql).unwrap();
            assert!(
                plan_statement(&stmt, &make_ctx(&catalog, dummy_source())).is_err(),
                "应被拒绝: {sql}"
            );
        }
    }

    #[test]
    fn txn_statements_plan() {
        let catalog = InMemoryCatalog::new();
        for (sql, want) in [
            ("BEGIN", "begin"),
            ("START TRANSACTION", "begin"),
            ("COMMIT", "commit"),
            ("ROLLBACK", "rollback"),
        ] {
            let stmt = parse_single(sql).unwrap();
            match plan_statement(&stmt, &make_ctx(&catalog, dummy_source())).unwrap() {
                StatementPlan::Begin => assert_eq!(want, "begin"),
                StatementPlan::Commit => assert_eq!(want, "commit"),
                StatementPlan::Rollback => assert_eq!(want, "rollback"),
                _ => panic!("expected txn plan for {sql}"),
            }
        }
    }

    #[test]
    fn unsupported_features_rejected() {
        let catalog = InMemoryCatalog::new();
        catalog.create_table(table_t()).unwrap();
        for sql in [
            "SELECT 1 UNION SELECT 2",
            "WITH x AS (SELECT 1) SELECT 1",
            "SELECT 1 LIMIT 'x'",
            "SELECT func(1)",
            "SELECT * FROM a JOIN b ON 1 = 1",
            "SELECT COUNT(DISTINCT id) FROM t",
            "SELECT name FROM t GROUP BY name HAVING score > 1", // score 未分组
        ] {
            let stmt = parse_single(sql).unwrap();
            assert!(
                plan_statement(&stmt, &make_ctx(&catalog, dummy_source())).is_err(),
                "应被拒绝: {sql}"
            );
        }
    }
}
