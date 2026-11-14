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
use exec::executor::{BoxedExecutor, ExecuteContext};
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

use crate::optimizer::{self, Estimator, split_conjuncts, sql_expr_tables};

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
    /// ANALYZE [TABLE] <name>:全量(或抽样)重建表统计。
    Analyze {
        name: Option<String>,
    },
    /// EXPLAIN <query>:只产出计划描述文本,不执行查询。
    Explain {
        /// 单列 TEXT 输出 schema("plan")
        schema: Vec<ColumnDesc>,
        /// 计划树(根在前,子节点带缩进)
        lines: Vec<String>,
    },
    Begin,
    Commit,
    Rollback,
}

/// 查询计划:执行流水线 + 静态输出 schema(供 server 生成 RowDescription)。
pub struct PlannedQuery {
    pub executor: BoxedExecutor,
    pub schema: Vec<ColumnDesc>,
    /// 计划树描述(EXPLAIN 用;普通执行忽略)
    pub explain: Vec<String>,
}

pub fn plan_statement(stmt: &Statement, ctx: &PlanContext<'_>) -> Result<StatementPlan> {
    match stmt {
        Statement::Query(q) => plan_query(q, ctx).map(StatementPlan::Query),
        Statement::Insert(insert) => plan_insert(insert, ctx),
        Statement::Update(update) => plan_update(update, ctx),
        Statement::Delete(delete) => plan_delete(delete, ctx),
        Statement::CreateTable(create) => plan_create_table(create),
        Statement::Analyze(analyze) => {
            let name = analyze
                .table_name
                .as_ref()
                .map(object_name_to_string)
                .transpose()?;
            Ok(StatementPlan::Analyze { name })
        }
        Statement::Explain { statement, .. } => {
            // v1 仅支持 EXPLAIN <SELECT>;ANALYZE 选项忽略(不实际执行)
            let Statement::Query(q) = statement.as_ref() else {
                return Err(Error::not_supported("EXPLAIN <statement> (only SELECT)"));
            };
            let pq = plan_query(q, ctx)?;
            let schema = vec![ColumnDesc::new("plan".to_string(), DataType::Utf8, true)];
            Ok(StatementPlan::Explain {
                schema,
                lines: pq.explain,
            })
        }
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
    mut planned: PlannedQuery,
    offset: u64,
    limit: Option<u64>,
) -> Result<PlannedQuery> {
    if offset == 0 && limit.is_none() {
        return Ok(planned);
    }
    // EXPLAIN 树:Limit 置于根,原计划整体缩进一级
    let label = match limit {
        Some(l) => format!("Limit (offset {offset}, take {l})"),
        None => format!("Offset ({offset})"),
    };
    let mut explain = vec![label];
    explain.extend(planned.explain.drain(..).map(|l| format!("  {l}")));
    let schema = planned.schema.clone();
    let executor = Box::new(LimitExec::new(planned.executor, offset as usize, limit));
    Ok(PlannedQuery {
        executor,
        schema,
        explain,
    })
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
pub(crate) fn function_call_args(f: &sqlparser::ast::Function) -> Result<Vec<SE>> {
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
    let mut rewriter = AggRewriter::new(scope_ref, &input_schema, ctx);
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
                    plan_expr(e, scope_ref, ctx)?
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
                    plan_expr(expr, scope_ref, ctx)?
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
                    plan_expr(expr, scope_ref, ctx)?
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
                            plan_expr(&ob.expr, scope_ref, ctx)?
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
                    plan_expr(other, scope_ref, ctx)?
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
    // 聚合路径的"输入"是 agg 输出(groups + aggs);HAVING 若引入新聚合会追加,
    // 故声明为可变并在 HAVING 改写后重算
    #[allow(unused_assignments)]
    let mut project_input_schema: Vec<ColumnDesc> = if aggregate {
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
    #[allow(unused_assignments)] // 两个分支都会覆盖初值
    let mut expl: Option<ExplNode> = None;
    let (mut pipeline, base_dtypes): (BoxedExecutor, Vec<DataType>) = if steps.is_empty() {
        let d = vec![ColumnDesc::new(
            "__dummy".to_string(),
            DataType::Int64,
            true,
        )];
        let dtypes = vec![DataType::Int64];
        expl = Some(ExplNode::leaf("Values (1 row)".to_string()));
        (Box::new(ValuesExec::one_row(d)), dtypes)
    } else {
        // 统计 + 选择性估算(数据源缺失/过期统计时可自动 ANALYZE)
        let est = Estimator::collect(&scope, &mut |t| ctx.data.table_stats(t));

        // ---------- 条件路由:WHERE + INNER ON 统一处理;LEFT ON 留在步骤上 ----------
        // - 常量谓词 → 第一张表扫描;
        // - 单表 WHERE 谓词:位置 ≥ 自身的 LEFT 步骤都会挡住下推(保留侧条件
        //   不能沉到连接之下;空产侧条件下推会把"匹配后过滤"变成"过滤后
        //   NULL 补齐"),否则下推扫描;被挡住的延迟到该 LEFT 步骤的连接后过滤;
        // - INNER ON 的单表部分恒可下推(属于该连接,先于后续 LEFT 求值);
        // - 跨表条件 → 条件池(顺序枚举时在"引用齐备"的最早步骤应用)。
        let mut scan_preds: Vec<Vec<Expr>> = vec![Vec::new(); steps.len()];
        let mut scan_sql: Vec<Vec<String>> = vec![Vec::new(); steps.len()];
        let mut scan_sels: Vec<Vec<f64>> = vec![Vec::new(); steps.len()];
        let mut deferred: Vec<(usize, Expr)> = Vec::new();
        // 持有合取项克隆(条件池借引用;克隆避免闭包生命周期纠缠)
        let mut pool_src: Vec<SE> = Vec::new();
        // LEFT 步骤的 ON:哈希等值键 + 匹配期 match_cond(非等值;LEFT 下
        // 未过条件的左行仍 NULL 补齐)
        let mut left_keys: Vec<(Vec<Expr>, Vec<Expr>)> =
            vec![(Vec::new(), Vec::new()); steps.len()];
        let mut left_match: Vec<Vec<Expr>> = vec![Vec::new(); steps.len()];

        let mut route = |c: &SE, from_where: bool| -> Result<()> {
            if from_where && aggregate && is_aggregate_call(c) {
                return Err(Error::sql(
                    "aggregates are not allowed in WHERE (use HAVING)",
                ));
            }
            let touched = sql_expr_tables(c, &scope)?;
            match touched.as_slice() {
                [] => {
                    let e = plan_expr(c, scope_ref, ctx)?;
                    check_bool(&e, &input_schema, "WHERE")?;
                    scan_preds[0].push(e);
                    scan_sql[0].push(format!("{c}"));
                    scan_sels[0].push(est.selectivity(c));
                }
                [pos] => {
                    let first_left = steps[*pos..]
                        .iter()
                        .position(|s| s.join_type == JoinType::LeftOuter)
                        .map(|off| off + pos);
                    if from_where {
                        if let Some(l) = first_left {
                            let e = plan_expr(c, scope_ref, ctx)?;
                            check_bool(&e, &input_schema, "WHERE")?;
                            deferred.push((l, e));
                            return Ok(());
                        }
                    }
                    let e = plan_expr(c, scope_ref, ctx)?;
                    check_bool(&e, &input_schema, "filter")?;
                    scan_preds[*pos].push(e);
                    scan_sql[*pos].push(format!("{c}"));
                    scan_sels[*pos].push(est.selectivity(c));
                }
                _ => pool_src.push(c.clone()),
            }
            Ok(())
        };

        for (j, step) in steps.iter().enumerate() {
            if step.join_type == JoinType::LeftOuter {
                if let Some(on) = &step.on {
                    for c in split_conjuncts(on) {
                        if let Some((pk, bk)) = try_hash_eq(c, &scope, &offsets, ctx)? {
                            left_keys[j].0.push(pk);
                            left_keys[j].1.push(bk);
                            continue;
                        }
                        let e = plan_expr(c, scope_ref, ctx)?;
                        check_bool(&e, &input_schema, "join condition")?;
                        left_match[j].push(e);
                    }
                }
            } else if let Some(on) = &step.on {
                for c in split_conjuncts(on) {
                    route(c, false)?;
                }
            }
        }
        if let Some(where_expr) = &select.selection {
            for c in split_conjuncts(where_expr) {
                route(c, true)?;
            }
        }
        let pool_refs: Vec<&SE> = pool_src.iter().collect();
        let pool = optimizer::build_pool(&pool_refs, &scope, &est)?;

        // ---------- 发射:连续 INNER 段 DP 枚举连接顺序;LEFT 步骤为屏障 ----------
        let em = EmitCtx {
            steps: &steps,
            offsets: &offsets,
            input_schema: &input_schema,
            scan_preds: &scan_preds,
            scan_sql: &scan_sql,
            scan_sels: &scan_sels,
            pool: &pool,
            est: &est,
            scope_ref,
            plan_ctx: ctx,
        };
        let mut st = PipelineState::default();
        let mut applied: std::collections::HashSet<usize> = Default::default();
        let mut pending: Vec<usize> = Vec::new();
        for k in 0..steps.len() {
            if steps[k].join_type == JoinType::LeftOuter {
                flush_group(&mut st, &em, &pending, &mut applied)?;
                pending.clear();
                emit_left_join(
                    &mut st,
                    &em,
                    k,
                    &deferred,
                    &left_keys,
                    &left_match,
                    &mut applied,
                )?;
            } else {
                pending.push(k);
            }
        }
        flush_group(&mut st, &em, &pending, &mut applied)?;

        let mut exec: BoxedExecutor = st.exec.expect("FROM pipeline must exist");
        expl = st.expl;
        // 条件池兜底:未被任何步骤应用的条件(理论上不可达)在顶层过滤
        for (pi, entry) in pool.iter().enumerate() {
            if applied.contains(&pi) {
                continue;
            }
            let e = plan_expr(entry.expr, scope_ref, ctx)?;
            check_bool(&e, &input_schema, "WHERE")?;
            exec = Box::new(FilterExec::new(exec, e));
            expl = Some(ExplNode::with(
                "Filter (leftover predicate)".to_string(),
                vec![expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()))],
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
            let pred = plan_expr(where_expr, None, ctx)?;
            pipeline = Box::new(FilterExec::new(pipeline, pred));
            expl = Some(ExplNode::with(
                "Filter".to_string(),
                vec![expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()))],
            ));
        }
    }

    // HAVING + 聚合
    if aggregate {
        // HAVING 先改写(可能注册投影中没有的新聚合),再取 agg 部件
        let mut having_pred: Option<Expr> = None;
        if let Some(having) = &select.having {
            let pred = rewriter.rewrite(having, false)?;
            // HAVING 改写可能注册新的聚合 → 重算输入 schema
            project_input_schema = rewriter.agg_output_schema();
            match pred.return_type(&project_input_schema)? {
                Some(DataType::Boolean) | None => {}
                Some(t) => {
                    return Err(Error::sql(format!("HAVING must be boolean, got {t:?}")));
                }
            }
            having_pred = Some(pred);
        }
        let (group_exprs, aggs, agg_out_schema) = rewriter.take_pipeline_parts();
        let n_groups = group_exprs.len();
        let n_aggs = aggs.len();
        pipeline = Box::new(AggHashExec::new(
            pipeline,
            base_dtypes,
            group_exprs,
            aggs,
            agg_out_schema,
        ));
        expl = Some(ExplNode::with(
            format!("Aggregate (groups={n_groups} aggs={n_aggs})"),
            vec![expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()))],
        ));
        if let Some(pred) = having_pred {
            pipeline = Box::new(FilterExec::new(pipeline, pred));
            expl = Some(ExplNode::with(
                "Filter (HAVING)".to_string(),
                vec![expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()))],
            ));
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
    expl = Some(ExplNode::with(
        format!("Project ({} cols)", project_schema.len()),
        vec![expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()))],
    ));

    // Sort + 砍隐藏列
    if !keys.is_empty() {
        let n_keys = keys.len();
        pipeline = Box::new(SortExec::new(pipeline, keys, project_schema.clone()));
        expl = Some(ExplNode::with(
            format!("Sort ({n_keys} keys)"),
            vec![expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()))],
        ));
        if !hidden.is_empty() {
            let visible_n = visible.len();
            pipeline = Box::new(ProjectExec::new(
                pipeline,
                project_schema[..visible_n].to_vec(),
                (0..visible_n).map(Expr::Column).collect(),
            ));
            expl = Some(ExplNode::with(
                "Project (drop sort keys)".to_string(),
                vec![expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()))],
            ));
        }
    }

    let final_schema = project_schema[..visible.len()].to_vec();
    // batch_size 由 ExecuteContext 携带,计划期不固化(LIMIT 由外层包装)
    let _ = ctx.batch_size;
    Ok(PlannedQuery {
        executor: pipeline,
        schema: final_schema,
        explain: expl
            .map(|root| {
                let mut lines = Vec::new();
                flatten_expl(&root, 0, &mut lines);
                lines
            })
            .unwrap_or_default(),
    })
}

// ---------- 执行计划描述(EXPLAIN) ----------

/// 计划树节点(自顶向下展示:根在前,子节点缩进)。
struct ExplNode {
    label: String,
    children: Vec<ExplNode>,
}

impl ExplNode {
    fn leaf(label: String) -> ExplNode {
        ExplNode {
            label,
            children: Vec::new(),
        }
    }

    fn with(label: String, children: Vec<ExplNode>) -> ExplNode {
        ExplNode { label, children }
    }
}

fn flatten_expl(node: &ExplNode, depth: usize, out: &mut Vec<String>) {
    out.push(format!("{}{}", "  ".repeat(depth), node.label));
    for c in &node.children {
        flatten_expl(c, depth + 1, out);
    }
}

/// 发射上下文:路由产物与计划期环境(供 flush/emit 函数共享)。
struct EmitCtx<'a> {
    steps: &'a [RelationStep],
    offsets: &'a [usize],
    input_schema: &'a [ColumnDesc],
    scan_preds: &'a [Vec<Expr>],
    scan_sql: &'a [Vec<String>],
    scan_sels: &'a [Vec<f64>],
    pool: &'a [optimizer::PoolEntry<'a>],
    est: &'a Estimator<'a>,
    scope_ref: Option<&'a Scope>,
    plan_ctx: &'a PlanContext<'a>,
}

/// 流水线发射状态。
#[derive(Default)]
struct PipelineState {
    exec: Option<BoxedExecutor>,
    /// 流水线当前列顺序(scope 位置;每步后重排为已连接集合的规范序)
    cur_order: Vec<usize>,
    /// 列下标偏移:流水线位置 i 对应扁平下标 i + col_shift。
    /// 首表扫描(非表 0)时为该表偏移;累计集合含表 0 后恒为 0。
    col_shift: usize,
    /// 流水线估计行数
    est_rows: f64,
    expl: Option<ExplNode>,
}

/// 扫描节点标签。
fn scan_label(em: &EmitCtx<'_>, t: usize, est_rows: f64) -> String {
    let mut label = format!("Scan {} rows≈{}", em.steps[t].alias, fmt_est(est_rows));
    if !em.scan_sql[t].is_empty() {
        label.push_str(&format!(" filter: [{}]", em.scan_sql[t].join(" AND ")));
    }
    label
}

fn fmt_est(v: f64) -> String {
    if v >= 10.0 {
        format!("{}", v.round() as u64)
    } else {
        format!("{v:.1}")
    }
}

/// 表集合 → 规范(FROM 序)扁平列下标。
fn canonical_flat(em: &EmitCtx<'_>, tables: &[usize]) -> Vec<usize> {
    let mut sorted: Vec<usize> = tables.to_vec();
    sorted.sort_unstable();
    let mut out = Vec::new();
    for p in sorted {
        out.extend(em.offsets[p]..em.offsets[p] + em.steps[p].schema.columns.len());
    }
    out
}

/// 把流水线列顺序重排回规范序(投影置换)。
fn permute_to_canonical(exec: BoxedExecutor, cur: &[usize], em: &EmitCtx<'_>) -> BoxedExecutor {
    let mut cur_flat: Vec<usize> = Vec::new();
    for &p in cur {
        cur_flat.extend(em.offsets[p]..em.offsets[p] + em.steps[p].schema.columns.len());
    }
    let canonical = canonical_flat(em, cur);
    let exprs: Vec<Expr> = canonical
        .iter()
        .map(|&f| {
            let pos = cur_flat
                .iter()
                .position(|&c| c == f)
                .expect("permutation source");
            Expr::Column(pos)
        })
        .collect();
    let schema: Vec<ColumnDesc> = canonical
        .iter()
        .map(|&f| em.input_schema[f].clone())
        .collect();
    Box::new(ProjectExec::new(exec, schema, exprs))
}

/// flush 一个 INNER 连接组:DP 枚举连接顺序,逐表扫描 + HashJoin 组合。
fn flush_group(
    st: &mut PipelineState,
    em: &EmitCtx<'_>,
    pending: &[usize],
    applied: &mut std::collections::HashSet<usize>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let mut nodes: Vec<usize> = pending.to_vec();
    if st.exec.is_some() {
        nodes.insert(0, optimizer::VIRTUAL);
    }
    let base: Vec<f64> = nodes
        .iter()
        .map(|&n| match n {
            optimizer::VIRTUAL => st.est_rows.max(1.0),
            p => base_of(em, p),
        })
        .collect();
    let plan = optimizer::optimize_join_order(&nodes, &base, em.pool);

    for (k, &node) in plan.order.iter().enumerate() {
        if node == optimizer::VIRTUAL {
            continue; // 首节点为既有流水线(col_shift = 0,全扁平)
        }
        if k == 0 {
            // 首表扫描:列 = 该表局部下标(偏移 = offsets[node]);
            // 后续连接的累计侧表达式按 st.col_shift 重基
            st.exec = Some(table_scan(
                &em.steps[node],
                &em.scan_preds[node],
                em.offsets[node],
                em.plan_ctx,
            )?);
            st.cur_order = vec![node];
            st.col_shift = em.offsets[node];
            st.est_rows = plan.est_rows[0];
            st.expl = Some(ExplNode::leaf(scan_label(em, node, base_of(em, node))));
            continue;
        }
        let step = &plan.steps[k - 1];
        emit_inner_join(
            st,
            em,
            node,
            step.build_accumulated,
            applied,
            plan.est_rows[k],
        )?;
    }
    Ok(())
}

/// 表的扫描基数估计(行数 × 下推谓词选择性)。
fn base_of(em: &EmitCtx<'_>, p: usize) -> f64 {
    let mut b = em.est.rows(p);
    for &s in &em.scan_sels[p] {
        b *= s;
    }
    b.max(1.0)
}

/// 等值对中属于指定表一侧的 SQL 表达式。
fn pair_side<'a>(pair: &'a optimizer::JoinPair<'a>, pos: usize) -> &'a SE {
    if pair.lo == pos {
        pair.lo_expr
    } else {
        pair.hi_expr
    }
}

/// 等值对在 prefix 中的那一侧的 scope 位置。
fn prefix_member(prefix: &[usize], pair: &optimizer::JoinPair<'_>) -> usize {
    if prefix.contains(&pair.lo) {
        pair.lo
    } else {
        pair.hi
    }
}

/// 发射一步 INNER 连接(新表 t;可选物化侧交换 = 物化累计侧、探测新表)。
fn emit_inner_join(
    st: &mut PipelineState,
    em: &EmitCtx<'_>,
    t: usize,
    build_accumulated: bool,
    applied: &mut std::collections::HashSet<usize>,
    est_out: f64,
) -> Result<()> {
    let prefix: Vec<usize> = st.cur_order.clone();
    let acc_shift = st.col_shift; // 累计侧:位置 i ↔ 扁平 i + acc_shift
    let t_offset = em.offsets[t];
    let t_width = em.steps[t].schema.columns.len();

    let mut probe_keys: Vec<Expr> = Vec::new();
    let mut build_keys: Vec<Expr> = Vec::new();
    let mut key_sql: Vec<String> = Vec::new();
    let mut residual: Vec<Expr> = Vec::new();
    let mut residual_sql: Vec<String> = Vec::new();

    for (pi, entry) in em.pool.iter().enumerate() {
        let tables = match &entry.join_pair {
            Some(p) => vec![p.lo, p.hi],
            None => entry.tables.clone(),
        };
        let contains_t = tables.contains(&t);
        let complete = tables.iter().all(|&x| x == t || prefix.contains(&x));
        let newly = tables.iter().any(|&x| !prefix.contains(&x));
        if !(complete && contains_t && newly) {
            continue;
        }
        if let Some(pair) = &entry.join_pair {
            if !build_accumulated {
                // probe = 累计(位置 = 扁平 - acc_shift),build = t(局部重基)
                let probe_side = pair_side(pair, prefix_member(&prefix, pair));
                let build_side = pair_side(pair, t);
                probe_keys.push(rebase_columns(
                    plan_expr(probe_side, em.scope_ref, em.plan_ctx)?,
                    acc_shift,
                )?);
                build_keys.push(rebase_columns(
                    plan_expr(build_side, em.scope_ref, em.plan_ctx)?,
                    t_offset,
                )?);
                key_sql.push(format!("{probe_side} = {build_side}"));
                applied.insert(pi);
                // 键来自 OR 公共键提取:键命中是必要非充分 → 完整 OR 仍需 residual
                if !entry.key_from_or {
                    continue;
                }
                let e = plan_expr(entry.expr, em.scope_ref, em.plan_ctx)?;
                check_bool(&e, em.input_schema, "join condition")?;
                let prefix_width = prefix
                    .iter()
                    .map(|&p| em.steps[p].schema.columns.len())
                    .sum();
                let e = remap_for_join(
                    e,
                    t_offset,
                    t_width,
                    prefix_width,
                    acc_shift,
                    build_accumulated,
                )?;
                residual.push(e);
                residual_sql.push(format!("{}", entry.expr));
                continue;
            }
            // 交换:probe = t 扫描(局部重基),build = 累计(重基 acc_shift)
            let probe_side = pair_side(pair, t);
            let build_side = pair_side(pair, prefix_member(&prefix, pair));
            probe_keys.push(rebase_columns(
                plan_expr(probe_side, em.scope_ref, em.plan_ctx)?,
                t_offset,
            )?);
            build_keys.push(rebase_columns(
                plan_expr(build_side, em.scope_ref, em.plan_ctx)?,
                acc_shift,
            )?);
            key_sql.push(format!("{probe_side} = {build_side}"));
            applied.insert(pi);
            // 键来自 OR 公共键提取:完整 OR 仍需 residual(同上)
            if entry.key_from_or {
                let e = plan_expr(entry.expr, em.scope_ref, em.plan_ctx)?;
                check_bool(&e, em.input_schema, "join condition")?;
                let prefix_width = prefix
                    .iter()
                    .map(|&p| em.steps[p].schema.columns.len())
                    .sum();
                let e = remap_for_join(
                    e,
                    t_offset,
                    t_width,
                    prefix_width,
                    acc_shift,
                    build_accumulated,
                )?;
                residual.push(e);
                residual_sql.push(format!("{}", entry.expr));
            }
            continue;
        }
        // 非等值/跨多表条件:连接后过滤(组合列序见 remap_for_join)
        let e = plan_expr(entry.expr, em.scope_ref, em.plan_ctx)?;
        check_bool(&e, em.input_schema, "join condition")?;
        let prefix_width = prefix
            .iter()
            .map(|&p| em.steps[p].schema.columns.len())
            .sum();
        let e = remap_for_join(
            e,
            t_offset,
            t_width,
            prefix_width,
            acc_shift,
            build_accumulated,
        )?;
        residual.push(e);
        residual_sql.push(format!("{}", entry.expr));
        applied.insert(pi);
    }

    let prefix_schema: Vec<ColumnDesc> = canonical_flat(em, &prefix)
        .iter()
        .map(|&f| em.input_schema[f].clone())
        .collect();
    let (probe_exec, build_exec, probe_schema, build_schema, children) = if build_accumulated {
        let probe = table_scan(&em.steps[t], &em.scan_preds[t], t_offset, em.plan_ctx)?;
        let build = st.exec.take().expect("accumulated pipeline");
        let expl_prev = st.expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()));
        (
            probe,
            build,
            em.steps[t].schema.columns.clone(),
            prefix_schema,
            vec![ExplNode::leaf(scan_label(em, t, base_of(em, t))), expl_prev],
        )
    } else {
        let build = table_scan(&em.steps[t], &em.scan_preds[t], t_offset, em.plan_ctx)?;
        let probe = st.exec.take().expect("accumulated pipeline");
        let expl_prev = st.expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()));
        (
            probe,
            build,
            prefix_schema,
            em.steps[t].schema.columns.clone(),
            vec![expl_prev, ExplNode::leaf(scan_label(em, t, base_of(em, t)))],
        )
    };

    let exec: BoxedExecutor = Box::new(HashJoinExec::new(
        probe_exec,
        build_exec,
        probe_schema,
        build_schema,
        probe_keys,
        build_keys,
        None,
        combine_and(residual),
        JoinType::Inner,
    ));
    let side = if build_accumulated {
        "build=accumulated"
    } else {
        "build=new"
    };
    let mut label = format!("HashJoin inner rows≈{} [{side}]", fmt_est(est_out));
    if !key_sql.is_empty() {
        label.push_str(&format!(" on: [{}]", key_sql.join(", ")));
    }
    if !residual_sql.is_empty() {
        label.push_str(&format!(" filter: [{}]", residual_sql.join(" AND ")));
    }
    st.expl = Some(ExplNode::with(label, children));

    // 列顺序:交换 = [t] ++ 前缀,否则 = 前缀 ++ [t](集合 = 前缀 ∪ {t});
    // 与规范序不一致则置换
    let mut new_cur = if build_accumulated {
        let mut c = vec![t];
        c.extend(prefix.iter().copied());
        c
    } else {
        let mut c = prefix.clone();
        c.push(t);
        c
    };
    let mut canonical = new_cur.clone();
    canonical.sort_unstable();
    let mut exec = exec;
    if new_cur != canonical {
        exec = permute_to_canonical(exec, &new_cur, em);
        new_cur = canonical;
        st.expl = Some(ExplNode::with(
            "Project (column restore)".to_string(),
            vec![st.expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()))],
        ));
    }
    st.cur_order = new_cur;
    // 规范序后,流水线位置 i ↔ 扁平 canonical_flat[0] + i
    st.col_shift = em.offsets[st.cur_order[0]];
    st.est_rows = est_out;
    st.exec = Some(exec);
    Ok(())
}

/// LEFT JOIN 发射:探测侧 = 既有流水线(保留行),构建侧 = 新表(不下推谓词,
/// 见路由注释);延迟的 WHERE 条件与本步完成的池条件在连接后过滤(WHERE 语义;
/// 不做连接键 —— 键会把不匹配行变成 NULL 补齐而非丢弃)。
#[allow(clippy::too_many_arguments)]
fn emit_left_join(
    st: &mut PipelineState,
    em: &EmitCtx<'_>,
    k: usize,
    deferred: &[(usize, Expr)],
    left_keys: &[(Vec<Expr>, Vec<Expr>)],
    left_match: &[Vec<Expr>],
    applied: &mut std::collections::HashSet<usize>,
) -> Result<()> {
    let prefix: Vec<usize> = st.cur_order.clone();
    let probe_schema: Vec<ColumnDesc> = canonical_flat(em, &prefix)
        .iter()
        .map(|&f| em.input_schema[f].clone())
        .collect();
    let build = table_scan(&em.steps[k], &[], em.offsets[k], em.plan_ctx)?;

    let mut residual: Vec<Expr> = Vec::new();
    let mut residual_sql: Vec<String> = Vec::new();
    for (l, e) in deferred {
        if *l == k {
            residual.push(e.clone());
        }
    }
    for (pi, entry) in em.pool.iter().enumerate() {
        let complete = entry.tables.iter().all(|&x| x == k || prefix.contains(&x));
        let newly = entry.tables.iter().any(|&x| !prefix.contains(&x));
        if !(complete && newly && entry.tables.contains(&k)) {
            continue;
        }
        let e = plan_expr(entry.expr, em.scope_ref, em.plan_ctx)?;
        check_bool(&e, em.input_schema, "WHERE")?;
        residual.push(e);
        residual_sql.push(format!("{}", entry.expr));
        applied.insert(pi);
    }

    let (pk, bk) = left_keys[k].clone();
    let probe_est = st.est_rows;
    let exec: BoxedExecutor = Box::new(HashJoinExec::new(
        st.exec.take().expect("accumulated pipeline"),
        build,
        probe_schema,
        em.steps[k].schema.columns.clone(),
        pk,
        bk,
        combine_and(left_match[k].clone()),
        combine_and(residual),
        JoinType::LeftOuter,
    ));
    let mut label = format!("HashJoin left rows≈{}", fmt_est(probe_est));
    if !left_keys[k].0.is_empty() {
        label.push_str(&format!(" ({} key cols)", left_keys[k].0.len()));
    }
    if !residual_sql.is_empty() {
        label.push_str(&format!(" filter: [{}]", residual_sql.join(" AND ")));
    }
    st.expl = Some(ExplNode::with(
        label,
        vec![st.expl.take().unwrap_or_else(|| ExplNode::leaf("?".into()))],
    ));
    st.cur_order.push(k); // FROM 序追加,天然规范(集合含表 0 → 全扁平)
    st.col_shift = 0;
    st.exec = Some(exec);
    Ok(())
}

/// 连接后组合列的扁平下标重映射。
///
/// - 正常(build = 新表):组合列序 = 累计规范序 ++ t 局部;
///   累计部分位置 = 扁平 - acc_shift,t 部分位置 = prefix_width + 扁平 - t_offset。
/// - 交换(build = 累计):组合列序 = t 局部 ++ 累计规范序;
///   t 部分 = 扁平 - t_offset,累计部分 = t_width + 扁平 - acc_shift。
fn remap_for_join(
    e: Expr,
    t_offset: usize,
    t_width: usize,
    prefix_width: usize,
    acc_shift: usize,
    swapped: bool,
) -> Result<Expr> {
    match e {
        Expr::Column(f) => {
            let in_t = f >= t_offset && f < t_offset + t_width;
            if swapped {
                if in_t {
                    Ok(Expr::Column(f - t_offset))
                } else {
                    Ok(Expr::Column(t_width + f - acc_shift))
                }
            } else if in_t {
                Ok(Expr::Column(prefix_width + f - t_offset))
            } else {
                Ok(Expr::Column(f - acc_shift))
            }
        }
        Expr::Binary { op, left, right } => Ok(Expr::binary(
            op,
            remap_for_join(*left, t_offset, t_width, prefix_width, acc_shift, swapped)?,
            remap_for_join(*right, t_offset, t_width, prefix_width, acc_shift, swapped)?,
        )),
        Expr::Function { name, args } => Ok(Expr::Function {
            name,
            args: args
                .into_iter()
                .map(|a| remap_for_join(a, t_offset, t_width, prefix_width, acc_shift, swapped))
                .collect::<Result<Vec<_>>>()?,
        }),
        Expr::IsNull { expr, negated } => Ok(Expr::IsNull {
            expr: Box::new(remap_for_join(
                *expr,
                t_offset,
                t_width,
                prefix_width,
                acc_shift,
                swapped,
            )?),
            negated,
        }),
        Expr::Case { whens, else_expr } => Ok(Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, r)| {
                    Ok((
                        remap_for_join(c, t_offset, t_width, prefix_width, acc_shift, swapped)?,
                        remap_for_join(r, t_offset, t_width, prefix_width, acc_shift, swapped)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            else_expr: else_expr
                .map(|e| remap_for_join(*e, t_offset, t_width, prefix_width, acc_shift, swapped))
                .transpose()?
                .map(Box::new),
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(remap_for_join(
                *expr,
                t_offset,
                t_width,
                prefix_width,
                acc_shift,
                swapped,
            )?),
            list,
            negated,
        }),
        other => Ok(other),
    }
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
    // 谓词下推:随解码过滤(列存路径则在列块上求值),不再叠加 FilterExec
    let pushdown = if preds.is_empty() {
        None
    } else {
        let rebased = preds
            .iter()
            .map(|p| rebase_columns(p.clone(), offset))
            .collect::<Result<Vec<_>>>()?;
        combine_and(rebased)
    };
    let exec: BoxedExecutor = match pushdown {
        Some(pred) => Box::new(SeqScanExec::with_pushdown(
            ctx.data.clone(),
            step.schema.clone(),
            Some(pred),
        )),
        None => Box::new(SeqScanExec::new(ctx.data.clone(), step.schema.clone())),
    };
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

/// 尝试把 `a = b` 形态的条件转成哈希连接键:两侧各只引用一张表 {i}/{j}
/// (i < j)→ (probe 键扁平等值, build 键列下标重基到表 j 局部)。
fn try_hash_eq(
    c: &SE,
    scope: &Scope,
    offsets: &[usize],
    ctx: &PlanContext<'_>,
) -> Result<Option<(Expr, Expr)>> {
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
    let probe = plan_expr(probe_sql, Some(scope), ctx)?;
    let build = rebase_columns(plan_expr(build_sql, Some(scope), ctx)?, offsets[j])?;
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
        Expr::Case { whens, else_expr } => Ok(Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, r)| Ok((rebase_columns(c, delta)?, rebase_columns(r, delta)?)))
                .collect::<Result<Vec<_>>>()?,
            else_expr: else_expr
                .map(|e| rebase_columns(*e, delta))
                .transpose()?
                .map(Box::new),
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(rebase_columns(*expr, delta)?),
            list,
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
        Expr::Case { whens, else_expr } => {
            for (c, r) in whens {
                collect_column_refs(c, out);
                collect_column_refs(r, out);
            }
            if let Some(e) = else_expr {
                collect_column_refs(e, out);
            }
        }
        Expr::InList { expr, .. } => collect_column_refs(expr, out),
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
    /// 计划上下文(子查询等需要数据源的表达式用)
    ctx: &'a PlanContext<'a>,
    groups: Vec<(Expr, String)>,
    aggs: Vec<(AggDesc, String)>,
    /// 所有 GROUP BY 项引用到的输入列(允许在投影/HAVING 中出现)
    group_ref_cols: std::collections::HashSet<usize>,
}

impl<'a> AggRewriter<'a> {
    fn new(
        scope: Option<&'a Scope>,
        input_schema: &'a [ColumnDesc],
        ctx: &'a PlanContext<'a>,
    ) -> Self {
        Self {
            scope,
            input_schema,
            ctx,
            groups: Vec::new(),
            aggs: Vec::new(),
            group_ref_cols: std::collections::HashSet::new(),
        }
    }

    /// GROUP BY 项解析(纯普通表达式;不允许聚合)。
    fn add_group(&mut self, e: &SE) -> Result<()> {
        let expr = plan_expr(e, self.scope, self.ctx)?;
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
            SE::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                // CASE 递归改写(SUM(CASE WHEN ...) 的 Q7/Q9 形状);
                // 不含聚合的条件/结果子树在此路径下要求叶子命中 GROUP BY 项
                if operand.is_some() {
                    return Err(Error::not_supported(
                        "CASE <operand> WHEN (simple form); use searched CASE",
                    ));
                }
                let mut whens = Vec::with_capacity(conditions.len());
                for cw in conditions {
                    let c = self.rewrite(&cw.condition, inside_agg)?;
                    let r = self.rewrite(&cw.result, inside_agg)?;
                    whens.push((c, r));
                }
                let else_expr = else_result
                    .as_ref()
                    .map(|e| self.rewrite(e, inside_agg))
                    .transpose()?
                    .map(Box::new);
                Expr::Case { whens, else_expr }
            }
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
            other => plan_expr(other, self.scope, self.ctx)?,
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
            other => plan_expr(other, self.scope, self.ctx).ok(),
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
                    let expr = plan_expr(e, None, ctx)?;
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
        let expr = plan_expr(&a.value, Some(&Scope::single(&table)), ctx)?;
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
            let expr = plan_expr(e, Some(&Scope::single(&table)), ctx)?;
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
            let expr = plan_expr(e, Some(&Scope::single(&table)), ctx)?;
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
pub fn plan_expr(e: &SE, scope: Option<&Scope>, ctx: &PlanContext<'_>) -> Result<Expr> {
    match e {
        SE::Value(v) => Ok(Expr::Literal(value_to_scalar(&v.value)?)),
        SE::Array(arr) => {
            // 数组字面量:元素必须为常量(列引用/聚合在数组里不支持)
            let mut items = Vec::with_capacity(arr.elem.len());
            for elem in &arr.elem {
                let expr = plan_expr(elem, None, ctx)?;
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
            let arg = plan_expr(expr, scope, ctx)?;
            Ok(Expr::Function {
                name: fname.to_string(),
                args: vec![arg],
            })
        }
        SE::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            // 仅支持搜索式 CASE WHEN cond THEN result ... [ELSE ...] END
            if operand.is_some() {
                return Err(Error::not_supported(
                    "CASE <operand> WHEN (simple form); use searched CASE WHEN <cond> THEN",
                ));
            }
            if conditions.is_empty() {
                return Err(Error::sql("CASE requires at least one WHEN branch"));
            }
            let mut whens = Vec::with_capacity(conditions.len());
            for cw in conditions {
                let c = plan_expr(&cw.condition, scope, ctx)?;
                let r = plan_expr(&cw.result, scope, ctx)?;
                whens.push((c, r));
            }
            let else_expr = else_result
                .as_ref()
                .map(|e| plan_expr(e, scope, ctx))
                .transpose()?
                .map(Box::new);
            Ok(Expr::Case { whens, else_expr })
        }
        SE::Between {
            expr,
            negated,
            low,
            high,
        } => {
            // BETWEEN 改写为比较合取/析取:e >= lo AND e <= hi(或 e < lo OR e > hi)
            let v = plan_expr(expr, scope, ctx)?;
            let lo = plan_expr(low, scope, ctx)?;
            let hi = plan_expr(high, scope, ctx)?;
            if *negated {
                Ok(Expr::binary(
                    BinaryOp::Or,
                    Expr::binary(BinaryOp::Lt, v.clone(), lo),
                    Expr::binary(BinaryOp::Gt, v, hi),
                ))
            } else {
                Ok(Expr::binary(
                    BinaryOp::And,
                    Expr::binary(BinaryOp::GtEq, v.clone(), lo),
                    Expr::binary(BinaryOp::LtEq, v, hi),
                ))
            }
        }
        SE::In {
            expr,
            target,
            negated,
            global,
        } => {
            if *global {
                return Err(Error::not_supported("GLOBAL IN"));
            }
            let inner = plan_expr(expr, scope, ctx)?;
            let values: Vec<ScalarValue> = match target {
                sqlparser::ast::InTarget::List(list) => {
                    let mut out = Vec::with_capacity(list.len());
                    for e in list {
                        // 常量元素:无列上下文解析后单批求值
                        let planned = plan_expr(e, None, ctx)?;
                        out.push(eval_const_scalar(&planned)?);
                    }
                    out
                }
                sqlparser::ast::InTarget::Subquery(q) => {
                    let (rows, width) = eval_subquery_rows(q, ctx, false)?;
                    if width != 1 {
                        return Err(Error::sql(format!(
                            "IN subquery must return exactly one column, got {width}"
                        )));
                    }
                    rows.into_iter()
                        .map(|r| r.into_iter().next().unwrap_or(ScalarValue::Null))
                        .collect()
                }
                other => {
                    return Err(Error::not_supported(format!("IN target {other}")));
                }
            };
            Ok(Expr::InList {
                expr: Box::new(inner),
                list: std::sync::Arc::new(values),
                negated: *negated,
            })
        }
        SE::Subquery(q) => {
            // 非相关标量子查询:计划期执行,恰好 1 行 × 1 列
            let (rows, width) = eval_subquery_rows(q, ctx, true)?;
            if width != 1 {
                return Err(Error::sql(format!(
                    "scalar subquery must return exactly one column, got {width}"
                )));
            }
            if rows.len() != 1 {
                return Err(Error::sql(format!(
                    "scalar subquery returned {} rows, expected exactly 1",
                    rows.len()
                )));
            }
            Ok(Expr::Literal(
                rows.into_iter().next().unwrap().into_iter().next().unwrap(),
            ))
        }
        SE::Exists { subquery, negated } => {
            // 非相关 EXISTS:计划期执行(取到首行即停),物化为布尔字面量
            let (rows, _) = eval_subquery_rows(subquery, ctx, true)?;
            let exists = !rows.is_empty();
            Ok(Expr::Literal(ScalarValue::Boolean(if *negated {
                !exists
            } else {
                exists
            })))
        }
        SE::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            // SUBSTRING(s [FROM a] [FOR b]) → substring(s, a, b);缺省 a=1,b=length(s)
            let s = plan_expr(expr, scope, ctx)?;
            let from = match substring_from {
                Some(f) => plan_expr(f, scope, ctx)?,
                None => Expr::Literal(ScalarValue::Int64(1)),
            };
            let for_len = match substring_for {
                Some(f) => plan_expr(f, scope, ctx)?,
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
            let l = plan_expr(expr, scope, ctx)?;
            let r = plan_expr(pattern, scope, ctx)?;
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
            let args = scalar_function_args(f, |e| plan_expr(e, scope, ctx))?;
            Ok(Expr::Function { name, args })
        }
        SE::BinaryOp { left, op, right } => {
            let l = plan_expr(left, scope, ctx)?;
            let r = plan_expr(right, scope, ctx)?;
            Ok(Expr::binary(map_binary_op(op)?, l, r))
        }
        SE::Nested(inner) => plan_expr(inner, scope, ctx),
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
            UnaryOperator::Plus => plan_expr(expr, scope, ctx),
            UnaryOperator::Minus => match plan_expr(expr, scope, ctx)? {
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

pub(crate) fn value_to_scalar(v: &Value) -> Result<ScalarValue> {
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

/// 计划并执行**非相关**子查询:返回结果行与列数。
/// 相关引用会因空 scope 解析失败而报"column not found"(相关子查询见路线图)。
/// `stop_at_first_row` 用于 EXISTS(取到首行即停,不物化全量)。
fn eval_subquery_rows(
    query: &Query,
    ctx: &PlanContext<'_>,
    stop_at_first_row: bool,
) -> Result<(Vec<Row>, usize)> {
    let planned = plan_query(query, ctx)?;
    let width = planned.schema.len();
    let root = memory::MemoryTracker::new_root("subquery", None);
    let mem = memory::MemoryContext::new(&root, "subquery", 1 << 20);
    let mut exec_ctx = ExecuteContext::new(ctx.batch_size, mem);
    let mut exec = planned.executor;
    let mut rows: Vec<Row> = Vec::new();
    loop {
        let Some(chunk) = exec.next_batch(&mut exec_ctx)? else {
            break;
        };
        if chunk.num_rows() > 0 {
            for i in 0..chunk.num_rows() {
                rows.push(chunk.row(i));
            }
            if stop_at_first_row {
                break;
            }
        }
    }
    Ok((rows, width))
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

    // ---------- IN / CASE / 非相关子查询 ----------

    fn query_error(sql: &str) -> String {
        let catalog = InMemoryCatalog::new();
        catalog.create_table(table_t()).unwrap();
        let stmt = parse_single(sql).unwrap();
        plan_statement(
            &stmt,
            &make_ctx(&catalog, Arc::new(RowsSource(sample_rows()))),
        )
        .err()
        .map(|e| e.to_string())
        .unwrap_or_else(|| panic!("expected error: {sql}"))
    }

    #[test]
    fn in_list_and_not_in_predicate() {
        let out = run_query_over(
            "SELECT id FROM t WHERE name IN ('eng') AND id > 1",
            sample_rows(),
        );
        assert_eq!(out, vec![vec![ScalarValue::Int64(2)]]);
        let out2 = run_query_over("SELECT id FROM t WHERE name NOT IN ('eng')", sample_rows());
        assert_eq!(
            out2,
            vec![vec![ScalarValue::Int64(3)], vec![ScalarValue::Int64(4)]]
        );
    }

    #[test]
    fn between_desugars_to_range_conjunct() {
        let out = run_query_over("SELECT id FROM t WHERE id BETWEEN 2 AND 3", sample_rows());
        assert_eq!(
            out,
            vec![vec![ScalarValue::Int64(2)], vec![ScalarValue::Int64(3)]]
        );
    }

    #[test]
    fn case_when_projection() {
        let out = run_query_over(
            "SELECT CASE WHEN score >= 100 THEN 'hi' ELSE 'lo' END FROM t ORDER BY 1",
            sample_rows(),
        );
        // score 100/200 → hi;NULL(条件未命中)/50 → lo;排序后 hi 在前
        assert_eq!(
            out,
            vec![
                vec![ScalarValue::Utf8("hi".into())],
                vec![ScalarValue::Utf8("hi".into())],
                vec![ScalarValue::Utf8("lo".into())],
                vec![ScalarValue::Utf8("lo".into())],
            ]
        );
    }

    #[test]
    fn case_inside_sum_aggregate() {
        // Q7/Q9 形状:SUM(CASE WHEN cond THEN x ELSE 0 END)
        let out = run_query_over(
            "SELECT name, SUM(CASE WHEN score >= 100 THEN 1 ELSE 0 END) AS cnt \
             FROM t GROUP BY name ORDER BY name",
            sample_rows(),
        );
        assert_eq!(
            out,
            vec![
                vec![ScalarValue::Utf8("eng".into()), ScalarValue::Int64(2)],
                vec![ScalarValue::Utf8("ops".into()), ScalarValue::Int64(0)],
            ]
        );
    }

    #[test]
    fn scalar_subquery_in_where() {
        // MIN 忽略 NULL → 50.0 → 命中 id 4
        let out = run_query_over(
            "SELECT id FROM t WHERE score = (SELECT MIN(score) FROM t)",
            sample_rows(),
        );
        assert_eq!(out, vec![vec![ScalarValue::Int64(4)]]);
    }

    #[test]
    fn in_subquery() {
        let out = run_query_over(
            "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE id >= 3)",
            sample_rows(),
        );
        assert_eq!(
            out,
            vec![vec![ScalarValue::Int64(3)], vec![ScalarValue::Int64(4)]]
        );
    }

    #[test]
    fn exists_subquery() {
        let out = run_query_over(
            "SELECT id FROM t WHERE EXISTS (SELECT id FROM t WHERE id > 3)",
            sample_rows(),
        );
        assert_eq!(out.len(), 4, "EXISTS 为真 → 保留全部外层行");
        let out2 = run_query_over(
            "SELECT id FROM t WHERE NOT EXISTS (SELECT id FROM t WHERE id > 99)",
            sample_rows(),
        );
        assert_eq!(out2.len(), 4);
    }

    #[test]
    fn correlated_subquery_rejected_with_column_error() {
        // 子查询引用外层别名 → 空 scope 解析失败(相关子查询暂不支持)
        let err =
            query_error("SELECT id FROM t WHERE EXISTS (SELECT id FROM t WHERE t.id > outer_t.id)");
        assert!(
            err.contains("unknown table alias") || err.contains("not found"),
            "{err}"
        );
        // 标量子查询返回多列 → 明确报错
        let err2 = query_error("SELECT id FROM t WHERE id = (SELECT id, name FROM t)");
        assert!(err2.contains("one column"), "{err2}");
    }
    #[test]
    fn having_aggregate_not_in_projection() {
        // HAVING 引入投影中没有的聚合 → 该聚合须进 agg 输出后再过滤
        let out = run_query_over(
            "SELECT name FROM t GROUP BY name HAVING SUM(score) > (SELECT MIN(score) FROM t) ORDER BY name",
            sample_rows(),
        );
        // eng = 300,ops = 50;MIN = 50 → ops 被滤掉,只剩 eng
        assert_eq!(out, vec![vec![ScalarValue::Utf8("eng".into())]]);
    }
    #[test]
    fn in_subquery_with_group_having_semantics() {
        // Q18 形状( planners 层):IN 子查询含 GROUP BY + HAVING 的聚合;
        // DATE/TEXT 列在表中,子查询在计划期物化为值集合
        let catalog = InMemoryCatalog::new();
        let schema = TableSchema {
            id: 1,
            name: "lineitem".into(),
            columns: vec![
                ColumnDesc::new("l_orderkey".to_string(), DataType::Int64, false),
                ColumnDesc::new("l_extendedprice".to_string(), DataType::Float64, false),
                ColumnDesc::new("l_shipdate".to_string(), DataType::Date, false),
                ColumnDesc::new("l_nation".to_string(), DataType::Utf8, false),
            ],
            primary_key: Vec::new(),
            version: 1,
        };
        catalog.create_table(schema).unwrap();
        let sql = "SELECT SUM(l_extendedprice) AS big_orders FROM lineitem WHERE l_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING COUNT(*) > 1)";
        let stmt = parse_single(sql).unwrap();
        let rows: Vec<Row> = vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Float64(100.0),
                ScalarValue::Date(9213),
                ScalarValue::Utf8("FRANCE".into()),
            ],
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Float64(50.0),
                ScalarValue::Date(9213),
                ScalarValue::Utf8("GERMANY".into()),
            ],
            vec![
                ScalarValue::Int64(2),
                ScalarValue::Float64(200.0),
                ScalarValue::Date(9213),
                ScalarValue::Utf8("GERMANY".into()),
            ],
        ];
        let StatementPlan::Query(pq) =
            plan_statement(&stmt, &make_ctx(&catalog, Arc::new(RowsSource(rows)))).unwrap()
        else {
            panic!("expected query");
        };
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(1024, MemoryContext::new(&root, "q", 4096));
        let mut exec = pq.executor;
        let mut out = Vec::new();
        while let Some(chunk) = exec.next_batch(&mut ctx).unwrap() {
            for i in 0..chunk.num_rows() {
                out.push(chunk.row(i));
            }
        }
        // 仅 orderkey 1 是"大订单"(2 行)→ 100 + 50
        assert_eq!(out, vec![vec![ScalarValue::Float64(150.0)]]);
    }
}
