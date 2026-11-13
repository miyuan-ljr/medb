//! 代价优化器:统计信息驱动的选择性/基数估算 + 连接顺序枚举。
//!
//! 输入是 SQL AST 合取项(经 AND 拆分)与每张表的 [`TableStats`];
//! 输出是 INNER 连接组的连接顺序(含每步物化侧选择),由 planner 落成
//! HashJoin 流水线。
//!
//! 代价模型(v1,流式哈希连接左深链):
//! - 扫描代价 = 估计行数;
//! - 连接代价 = 两侧读取 + 物化侧哈希表(较小侧 × BUILD_WEIGHT)+ 输出行;
//! - 连接基数 = 各表基数之积 × 跨表条件选择性(等值对 = 1/max(NDV),其余
//!   按谓词形状估:范围插值、LIKE 启发式、默认 0.25)。
//! - ≤ DP_MAX_RELS 张表按子集 DP 精确枚举(等价顺序取 FROM 书写序,保证
//!   确定性);超过则贪心(每步取使累计基数最小的下一个关系)。

use std::collections::HashMap;
use std::sync::Arc;

use common::error::{Error, Result};
use common::stats::TableStats;
use common::types::{self, ScalarValue};
use sqlparser::ast::{BinaryOperator, DataType as SqlType, Expr as SE, UnaryOperator, Value};

use crate::planner::Scope;

/// 无统计时的默认基数
pub const DEFAULT_ROWS: f64 = 1000.0;
/// 无 NDV 统计时的默认 NDV
const DEFAULT_NDV: f64 = 100.0;
/// 形状不明条件的默认选择性
pub const DEFAULT_SEL: f64 = 0.25;
/// 哈希表物化代价权重(相对流式读取;物化 = 写 + 读 + 内存驻留)
const BUILD_WEIGHT: f64 = 2.0;
/// 子集 DP 全枚举的最大关系数
pub const DP_MAX_RELS: usize = 10;
/// LIKE 谓词的启发式选择性
const LIKE_SEL: f64 = 0.1;
/// 选择性下限(避免估成 0 行导致后续估算塌缩)
const MIN_SEL: f64 = 0.001;
/// 虚拟节点:表示 JOIN 屏障之前已组合好的流水线(顺序固定在首位)
pub const VIRTUAL: usize = usize::MAX;

// ---------- AST 合取项工具(planner 共用) ----------

/// AND 树拆合取项(嵌套展开;返回原表达式的引用)。
pub(crate) fn split_conjuncts(e: &SE) -> Vec<&SE> {
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
pub(crate) fn sql_expr_tables(e: &SE, scope: &Scope) -> Result<Vec<usize>> {
    let mut out = std::collections::BTreeSet::new();
    collect_sql_tables(e, scope, &mut out)?;
    Ok(out.into_iter().collect())
}

pub(crate) fn collect_sql_tables(
    e: &SE,
    scope: &Scope,
    out: &mut std::collections::BTreeSet<usize>,
) -> Result<()> {
    match e {
        SE::Identifier(ident) => {
            // 非限定列:命中该列名的全部表(命中多表 → 视为跨表条件)
            let mut any = false;
            for (pos, (_, t)) in scope.tables.iter().enumerate() {
                if t.column_index(&ident.value).is_some() {
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
            for a in crate::planner::function_call_args(f)? {
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

// ---------- 选择性估算 ----------

/// 表达式选择性估算器:按谓词形状 + 列统计给出 0..1 的行分数。
pub struct Estimator<'a> {
    scope: &'a Scope,
    /// scope 位置 → 表统计(None = 无统计)
    stats: Vec<Option<Arc<TableStats>>>,
}

impl<'a> Estimator<'a> {
    /// 为 scope 中每张表拉取统计(provider 由 Engine 提供,可自动 ANALYZE)。
    pub fn collect(
        scope: &'a Scope,
        provider: &mut dyn FnMut(&common::schema::TableSchema) -> Option<Arc<TableStats>>,
    ) -> Estimator<'a> {
        let stats = scope.tables.iter().map(|(_, t)| provider(t)).collect();
        Estimator { scope, stats }
    }

    /// 测试/离线构造:直接给每张表的统计。
    pub fn with(scope: &'a Scope, stats: Vec<Option<Arc<TableStats>>>) -> Estimator<'a> {
        debug_assert_eq!(scope.tables.len(), stats.len(), "stats 与表数量不一致");
        Estimator { scope, stats }
    }

    fn table_stats(&self, pos: usize) -> Option<&Arc<TableStats>> {
        self.stats.get(pos).and_then(|s| s.as_ref())
    }

    /// 表基数估计(行数)。
    pub fn rows(&self, pos: usize) -> f64 {
        self.table_stats(pos)
            .map_or(DEFAULT_ROWS, |s| s.row_count as f64)
    }

    fn col_stats(&self, pos: usize, col: &str) -> Option<&common::stats::ColumnStats> {
        self.table_stats(pos).and_then(|s| s.column(col))
    }

    /// 列 NDV(无统计 → rows/10,夹到 [1, 100])。
    pub fn ndv(&self, pos: usize, col: &str) -> f64 {
        self.col_stats(pos, col)
            .map(|c| c.ndv as f64)
            .filter(|ndv| *ndv > 0.0)
            .unwrap_or_else(|| (self.rows(pos) / 10.0).clamp(1.0, DEFAULT_NDV))
    }

    pub fn null_frac(&self, pos: usize, col: &str) -> f64 {
        self.table_stats(pos).map_or(0.0, |s| s.null_frac(col))
    }

    /// 列 min/max 的 f64 视图(范围插值;任一端缺失 → None)。
    fn bounds(&self, pos: usize, col: &str) -> Option<(f64, f64)> {
        let c = self.col_stats(pos, col)?;
        let min = types::scalar_to_f64(c.min.as_ref()?)?;
        let max = types::scalar_to_f64(c.max.as_ref()?)?;
        Some((min, max))
    }

    /// 列引用解析 → (scope 位置, 列名);非列表达式返回 None。
    pub fn column_ref(&self, e: &SE) -> Option<(usize, String)> {
        match e {
            SE::Identifier(i) => self
                .scope
                .tables
                .iter()
                .enumerate()
                .find(|(_, (_, t))| t.column_index(&i.value).is_some())
                .map(|(pos, _)| (pos, i.value.clone())),
            SE::CompoundIdentifier(parts) if parts.len() == 2 => {
                let pos = self.scope.resolve_qualifier(&parts[0].value)?;
                self.scope
                    .tables
                    .get(pos)?
                    .1
                    .column_index(&parts[1].value)?;
                Some((pos, parts[1].value.clone()))
            }
            _ => None,
        }
    }

    /// 常量字面量取值(Value / DATE 字面量 / 一元负号)。
    fn literal(&self, e: &SE) -> Option<ScalarValue> {
        match e {
            SE::Value(v) => crate::planner::value_to_scalar(&v.value).ok(),
            SE::TypedString(ts) if matches!(ts.data_type, SqlType::Date) => match &ts.value.value {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    common::types::parse_date_text(s).ok()
                }
                _ => None,
            },
            SE::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => match self.literal(expr) {
                Some(ScalarValue::Int64(v)) => Some(ScalarValue::Int64(-v)),
                Some(ScalarValue::Float64(v)) => Some(ScalarValue::Float64(-v)),
                _ => None,
            },
            _ => None,
        }
    }

    /// 合取项选择性。
    pub fn selectivity(&self, e: &SE) -> f64 {
        match e {
            SE::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::Or => {
                        let (sl, sr) = (self.selectivity(left), self.selectivity(right));
                        return (sl + sr - sl * sr).clamp(MIN_SEL, 1.0);
                    }
                    BinaryOperator::And => {
                        return (self.selectivity(left) * self.selectivity(right))
                            .clamp(MIN_SEL, 1.0);
                    }
                    _ => {}
                }
                let lc = self.column_ref(left);
                let rc = self.column_ref(right);
                match (lc, rc) {
                    (Some((pos, col)), None) => self.col_vs_const(pos, &col, op, right),
                    (None, Some((pos, col))) => self.col_vs_const(pos, &col, op, left),
                    (Some((lp, lcol)), Some((rp, rcol))) => {
                        // 列 = 列:等值 → 1/max(NDV);序比较按默认
                        match op {
                            BinaryOperator::Eq => (1.0
                                / self.ndv(lp, &lcol).max(self.ndv(rp, &rcol)))
                            .clamp(MIN_SEL, 1.0),
                            BinaryOperator::NotEq => {
                                1.0 - (1.0 / self.ndv(lp, &lcol).max(self.ndv(rp, &rcol)))
                                    .clamp(MIN_SEL, 1.0)
                            }
                            _ => DEFAULT_SEL,
                        }
                    }
                    (None, None) => DEFAULT_SEL,
                }
            }
            SE::Like { negated, .. } => {
                if *negated {
                    1.0 - LIKE_SEL
                } else {
                    LIKE_SEL
                }
            }
            SE::IsNull(expr) => match self.column_ref(expr) {
                Some((pos, col)) => self.null_frac(pos, &col),
                None => 0.05,
            },
            SE::IsNotNull(expr) => match self.column_ref(expr) {
                Some((pos, col)) => 1.0 - self.null_frac(pos, &col),
                None => 0.95,
            },
            SE::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => 1.0 - self.selectivity(expr),
            _ => DEFAULT_SEL,
        }
    }

    /// 列 op 常量:等值 1/NDV(× 非空分数);范围按 min/max 插值。
    fn col_vs_const(&self, pos: usize, col: &str, op: &BinaryOperator, other: &SE) -> f64 {
        let nf = self.null_frac(pos, col);
        match op {
            BinaryOperator::Eq => {
                return (1.0 / self.ndv(pos, col)).clamp(MIN_SEL, 1.0) * (1.0 - nf);
            }
            BinaryOperator::NotEq => {
                return 1.0 - (1.0 / self.ndv(pos, col)).clamp(MIN_SEL, 1.0) * (1.0 - nf);
            }
            BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {}
            _ => return DEFAULT_SEL,
        }
        let Some(lit) = self.literal(other) else {
            return DEFAULT_SEL;
        };
        let Some((min, max)) = self.bounds(pos, col) else {
            return DEFAULT_SEL;
        };
        let Some(v) = types::scalar_to_f64(&lit) else {
            return DEFAULT_SEL;
        };
        if max <= min {
            return DEFAULT_SEL;
        }
        let frac = ((v - min) / (max - min)).clamp(0.0, 1.0);
        let sel = match op {
            BinaryOperator::Lt | BinaryOperator::LtEq => frac,
            BinaryOperator::Gt | BinaryOperator::GtEq => 1.0 - frac,
            _ => DEFAULT_SEL,
        };
        sel.clamp(MIN_SEL, 1.0)
    }
}

// ---------- 连接条件池 ----------

/// `a = b` 形态的跨表等值(两侧各只引用一张表)。
pub struct JoinPair<'a> {
    /// 较小 scope 位置一侧
    pub lo: usize,
    /// 较大 scope 位置一侧
    pub hi: usize,
    pub lo_expr: &'a SE,
    pub hi_expr: &'a SE,
    /// 连接键 NDV(= max(两侧 NDV))
    pub ndv: f64,
}

/// 跨表合取项(引用 ≥ 2 张表;连接条件池的基本单位)。
pub struct PoolEntry<'a> {
    pub expr: &'a SE,
    /// 引用的 scope 位置(升序)
    pub tables: Vec<usize>,
    pub join_pair: Option<JoinPair<'a>>,
    /// 非等值对条件的选择性(等值对用 1/ndv)
    pub sel: f64,
}

/// 构建连接条件池:从一组合取项中筛出跨表(≥2 表)条件并分类。
pub fn build_pool<'a>(
    conjuncts: &[&'a SE],
    scope: &Scope,
    est: &Estimator<'_>,
) -> Result<Vec<PoolEntry<'a>>> {
    let mut pool = Vec::new();
    for c in conjuncts {
        let tables = sql_expr_tables(c, scope)?;
        if tables.len() < 2 {
            continue;
        }
        // 等值对识别:两侧各只引用一张表
        let join_pair = match c {
            SE::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let lt = sql_expr_tables(left, scope)?;
                let rt = sql_expr_tables(right, scope)?;
                match (lt.as_slice(), rt.as_slice()) {
                    ([i], [j]) if i < j => Some(make_pair(*i, *j, left, right, est)),
                    ([i], [j]) if i > j => Some(make_pair(*j, *i, right, left, est)),
                    _ => None,
                }
            }
            _ => None,
        };
        let sel = est.selectivity(c).clamp(MIN_SEL, 1.0);
        pool.push(PoolEntry {
            expr: c,
            tables,
            join_pair,
            sel,
        });
    }
    Ok(pool)
}

/// 等值对一侧的 NDV:列引用 → 列 NDV;复合表达式 → 表基数的 1/10 启发式。
fn side_ndv(e: &SE, pos: usize, est: &Estimator<'_>) -> f64 {
    match est.column_ref(e) {
        Some((p, col)) => est.ndv(p, &col),
        None => (est.rows(pos) / 10.0).clamp(1.0, DEFAULT_NDV),
    }
}

fn make_pair<'a>(
    lo: usize,
    hi: usize,
    lo_expr: &'a SE,
    hi_expr: &'a SE,
    est: &Estimator<'_>,
) -> JoinPair<'a> {
    let ndv_lo = side_ndv(lo_expr, lo, est);
    let ndv_hi = side_ndv(hi_expr, hi, est);
    JoinPair {
        lo,
        hi,
        lo_expr,
        hi_expr,
        ndv: ndv_lo.max(ndv_hi),
    }
}

// ---------- 连接顺序枚举 ----------

/// 一步连接的计划形态。
#[derive(Debug, Clone)]
pub struct JoinStep {
    /// 新加入的节点(scope 位置或 VIRTUAL;首节点无步骤)
    pub node: usize,
    /// true = 物化累计侧(探测新表;累计侧较小时选择)
    pub build_accumulated: bool,
}

/// 连接组的计划结果。
#[derive(Debug, Clone)]
pub struct GroupPlan {
    /// 连接序列(节点 scope 位置顺序;含首节点;VIRTUAL 表示既有流水线)
    pub order: Vec<usize>,
    /// 每个前缀的估计基数(est_rows[i] = 前 i+1 个关系连接后的行数)
    pub est_rows: Vec<f64>,
    /// 连接步骤(与 order[1..] 对齐)
    pub steps: Vec<JoinStep>,
}

/// 对一组连接节点做顺序枚举。
///
/// - `nodes`:节点(scope 位置;`VIRTUAL` 表示屏障前的累计流水线,恒为首位);
/// - `base`:与 nodes 对齐的单节点基数(扫描估计,含下推谓词选择性);
/// - `pool`:跨表条件池(条件引用必须全部落在 nodes 内才会计入选择性;
///   跨越 VIRTUAL 边界的条件被保守忽略,由 planner 在发射时应用)。
pub fn optimize_join_order(nodes: &[usize], base: &[f64], pool: &[PoolEntry<'_>]) -> GroupPlan {
    debug_assert_eq!(nodes.len(), base.len());
    if nodes.len() == 1 {
        return GroupPlan {
            order: nodes.to_vec(),
            est_rows: vec![base[0].max(1.0)],
            steps: Vec::new(),
        };
    }

    if nodes.len() > DP_MAX_RELS {
        return greedy_order(nodes, base, pool);
    }

    // 节点下标 → 位;条件的生效位掩码(等值对 = 两侧;其余 = 全部引用表)
    let bit_of: HashMap<usize, usize> = nodes.iter().enumerate().map(|(i, &p)| (p, i)).collect();
    let entry_bits: Vec<Option<u32>> = pool
        .iter()
        .map(|e| {
            let tables: Vec<usize> = match &e.join_pair {
                Some(p) => vec![p.lo, p.hi],
                None => e.tables.clone(),
            };
            let mut bits = 0u32;
            for t in &tables {
                bits |= 1u32 << *bit_of.get(t)?; // 引用越界 → 该条目不计入
            }
            Some(bits)
        })
        .collect();

    let n = nodes.len();
    let full = (1u32 << n) - 1;
    let virtual_first = nodes[0] == VIRTUAL;

    // est(mask):节点基数之积 × 生效条件选择性(下限 1 行)
    let est_of = |mask: u32| -> f64 {
        let mut v: f64 = (0..n)
            .filter(|i| mask & (1 << i) != 0)
            .map(|i| base[i])
            .product();
        for (e, bits) in pool.iter().zip(&entry_bits) {
            let Some(bits) = bits else { continue };
            if bits & !mask == 0 {
                v *= match &e.join_pair {
                    Some(p) => 1.0 / p.ndv.max(1.0),
                    None => e.sel,
                };
            }
        }
        v.max(1.0)
    };

    // dp[mask] = (最小代价, 最后加入的节点位)
    let mut dp_cost = vec![f64::INFINITY; full as usize + 1];
    let mut dp_last = vec![usize::MAX; full as usize + 1];
    for i in 0..n {
        if virtual_first && i != 0 {
            continue; // 虚拟节点必须最先(只在 mask 含 bit0 时合法)
        }
        dp_cost[1 << i] = base[i];
        dp_last[1 << i] = i;
    }
    for mask in 1u32..=full {
        if mask.count_ones() < 2 || (virtual_first && mask & 1 == 0) {
            continue;
        }
        for i in 0..n {
            if mask & (1 << i) == 0 || (virtual_first && i == 0) {
                continue; // 虚拟节点不会是"最后加入"的
            }
            let sub = mask & !(1 << i);
            if dp_cost[sub as usize] == f64::INFINITY {
                continue;
            }
            let est_sub = est_of(sub);
            let join_cost = est_sub + base[i] + est_sub.min(base[i]) * BUILD_WEIGHT + est_of(mask);
            let total = dp_cost[sub as usize] + join_cost;
            if total < dp_cost[mask as usize] {
                dp_cost[mask as usize] = total;
                dp_last[mask as usize] = i;
            }
        }
    }

    // 回溯顺序
    let mut rev_order = Vec::with_capacity(n);
    let mut mask = full;
    while mask.count_ones() > 0 {
        let i = dp_last[mask as usize];
        debug_assert!(i != usize::MAX, "dp 回溯失败 mask={mask:#x}");
        if i == usize::MAX {
            break;
        }
        rev_order.push(i);
        mask &= !(1 << i);
    }
    rev_order.reverse();

    // 重建:每步的物化侧选择(累计侧 vs 新表,取小者)
    let mut est_rows = Vec::with_capacity(n);
    let mut steps = Vec::with_capacity(n - 1);
    for (k, &i) in rev_order.iter().enumerate() {
        let est_k = est_of(rev_order[..=k].iter().fold(0u32, |m, &j| m | (1 << j)));
        est_rows.push(est_k);
        if k > 0 {
            let est_prefix = est_rows[k - 1];
            steps.push(JoinStep {
                node: nodes[i],
                build_accumulated: est_prefix < base[i],
            });
        }
    }
    let order = rev_order.into_iter().map(|i| nodes[i]).collect();
    GroupPlan {
        order,
        est_rows,
        steps,
    }
}

/// 贪心(> DP_MAX_RELS):从最小基数起,每步选使累计基数最小的下一个关系。
fn greedy_order(nodes: &[usize], base: &[f64], pool: &[PoolEntry<'_>]) -> GroupPlan {
    let mut remaining: Vec<usize> = (0..nodes.len()).collect();
    let mut chosen: Vec<usize> = Vec::new();
    let mut est_rows: Vec<f64> = Vec::new();
    let mut steps: Vec<JoinStep> = Vec::new();

    while !remaining.is_empty() {
        let next = if chosen.is_empty() {
            // 起点:基数最小的节点(平局取 nodes 顺序)
            let mut best = remaining[0];
            for &i in &remaining {
                if base[i] < base[best] {
                    best = i;
                }
            }
            best
        } else {
            // 下一个:使累计基数最小的节点(粗估 = 累计 × 基数,忽略条件选择性;
            // 含等值条件的会在内层被明显压低 —— v1 简化)
            let mut best = remaining[0];
            let mut best_est = f64::INFINITY;
            for &i in &remaining {
                let mut cand = est_rows.last().copied().unwrap_or(1.0) * base[i];
                for e in pool {
                    let tables: Vec<usize> = match &e.join_pair {
                        Some(p) => vec![p.lo, p.hi],
                        None => e.tables.clone(),
                    };
                    if tables.contains(&nodes[i])
                        && tables
                            .iter()
                            .all(|t| chosen.iter().any(|&c| nodes[c] == *t))
                    {
                        cand *= match &e.join_pair {
                            Some(p) => 1.0 / p.ndv.max(1.0),
                            None => e.sel,
                        };
                    }
                }
                if cand < best_est {
                    best_est = cand;
                    best = i;
                }
            }
            best
        };
        remaining.retain(|&i| i != next);
        if chosen.is_empty() {
            chosen.push(next);
            est_rows.push(base[next].max(1.0));
            continue;
        }
        let est_prefix = *est_rows.last().expect("non-empty");
        est_rows.push((est_prefix * base[next]).max(1.0));
        steps.push(JoinStep {
            node: nodes[next],
            build_accumulated: est_prefix < base[next],
        });
        chosen.push(next);
    }
    GroupPlan {
        order: chosen.into_iter().map(|i| nodes[i]).collect(),
        est_rows,
        steps,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_single;
    use common::schema::{ColumnDesc, TableSchema};
    use common::stats::ColumnStats;
    use common::types::DataType;
    use sqlparser::ast::{SetExpr, Statement};

    fn table(id: u64, name: &str, cols: &[&str]) -> TableSchema {
        TableSchema {
            id,
            name: name.to_string(),
            columns: cols
                .iter()
                .map(|c| ColumnDesc::new(c.to_string(), DataType::Int64, true))
                .collect(),
            primary_key: vec![],
            version: 1,
        }
    }

    fn stats(rows: u64, ndv: u64) -> Arc<TableStats> {
        Arc::new(TableStats {
            row_count: rows,
            stale: false,
            columns: [(
                "k".to_string(),
                ColumnStats {
                    ndv,
                    null_count: 0,
                    min: Some(ScalarValue::Int64(0)),
                    max: Some(ScalarValue::Int64(rows as i64 - 1)),
                },
            )]
            .into_iter()
            .collect(),
        })
    }

    fn scope3() -> Scope {
        let mut s = Scope::empty();
        s.push("a".into(), &table(1, "a", &["x", "k"]));
        s.push("b".into(), &table(2, "b", &["y", "k"]));
        s.push("c".into(), &table(3, "c", &["z", "k"]));
        s
    }

    fn where_of(sql: &str) -> SE {
        let Statement::Query(q) = parse_single(sql).unwrap() else {
            panic!("not a query");
        };
        let SetExpr::Select(select) = q.body.as_ref() else {
            panic!("not a select");
        };
        select.selection.as_ref().unwrap().clone()
    }

    fn conjunct_asts(sql: &str) -> Vec<SE> {
        let w = where_of(sql);
        split_conjuncts(&w).into_iter().map(|c| c.clone()).collect()
    }

    #[test]
    fn selectivity_uses_ndv_bounds_and_defaults() {
        let scope = scope3();
        let est = Estimator::with(
            &scope,
            vec![Some(stats(1000, 100)), Some(stats(10, 5)), None],
        );
        // a.x = 5:有统计但无 x 列统计 → 默认 ndv = rows/10 = 100 → 1/100
        let sel = est.selectivity(&where_of("SELECT * FROM a WHERE x = 5"));
        assert!((sel - 0.01).abs() < 1e-9, "sel = {sel}");
        // b.k = 3:ndv 5 → 1/5(限定列,避免非限定 k 落到首表)
        let sel2 = est.selectivity(&where_of("SELECT * FROM b WHERE b.k = 3"));
        assert!((sel2 - 0.2).abs() < 1e-9, "sel = {sel2}");
        // 无统计表 c:默认 ndv = 1000/10 = 100
        let sel3 = est.selectivity(&where_of("SELECT * FROM c WHERE z = 1"));
        assert!((sel3 - 0.01).abs() < 1e-9);
        // 范围:b.k ∈ [0, 9];k < 3 → 3/9 ≈ 0.33
        let sel4 = est.selectivity(&where_of("SELECT * FROM b WHERE b.k < 3"));
        assert!((sel4 - 1.0 / 3.0).abs() < 0.02, "range sel = {sel4}");
        // LIKE 启发式
        let sel5 = est.selectivity(&where_of("SELECT * FROM a WHERE k LIKE 'ab%'"));
        assert!((sel5 - LIKE_SEL).abs() < 1e-9);
    }

    #[test]
    fn pool_classifies_join_pairs() {
        let scope = scope3();
        let est = Estimator::collect(&scope, &mut |_| Some(stats(1000, 100)));
        let cs = conjunct_asts("SELECT * FROM a, b, c WHERE a.k = b.k AND b.k = c.k AND a.x < 10");
        let refs: Vec<&SE> = cs.iter().collect();
        let pool = build_pool(&refs, &scope, &est).unwrap();
        assert_eq!(pool.len(), 2, "单表条件 a.x < 10 不入池");
        let p0 = pool[0].join_pair.as_ref().unwrap();
        assert_eq!((p0.lo, p0.hi), (0, 1));
        assert!(pool[1].join_pair.is_some());
    }

    #[test]
    fn dp_avoids_selectivity_free_prefix() {
        // 连接图:a-b(键,ndv 10)、a-c(键,ndv 10000);b 与 c 之间无连接条件。
        // b⋈c 先行会产生 ~100000 行的笛卡尔中间结果;DP 应选 a 起步的顺序,
        // 让每个前缀都经过等值条件收缩。
        let mut s = Scope::empty();
        s.push("a".into(), &table(1, "a", &["x", "k"]));
        s.push("b".into(), &table(2, "b", &["y", "k"]));
        s.push("c".into(), &table(3, "c", &["z", "k"]));
        let stats_vec = vec![
            Some(stats(10, 10)),
            Some(stats(10, 10)),
            Some(stats(10000, 10000)),
        ];
        let est = Estimator::with(&s, stats_vec);
        let cs = conjunct_asts("SELECT * FROM a, b, c WHERE a.k = b.k AND a.k = c.k");
        let refs: Vec<&SE> = cs.iter().collect();
        let pool = build_pool(&refs, &s, &est).unwrap();
        let base: Vec<f64> = (0..3).map(|i| est.rows(i)).collect();
        let plan = optimize_join_order(&[0, 1, 2], &base, &pool);
        assert_eq!(plan.order[0], 0, "最小表 a 起步(平局保持 FROM 序)");
        // 每一步的累计基数都被等值条件压住(无 100000 行中间结果)
        assert!(plan.est_rows[1] <= 10.0 + 1e-6, "est = {:?}", plan.est_rows);
        assert!(plan.est_rows[2] <= 10.0 + 1e-6, "est = {:?}", plan.est_rows);
        assert_eq!(plan.steps.len(), 2);
    }

    #[test]
    fn virtual_node_stays_first() {
        let scope = scope3();
        let stats_vec = vec![
            Some(stats(100, 10)),
            Some(stats(10, 10)),
            Some(stats(10, 10)),
        ];
        let est = Estimator::with(&scope, stats_vec);
        let cs = conjunct_asts("SELECT * FROM a, b, c WHERE a.k = b.k AND b.k = c.k");
        let refs: Vec<&SE> = cs.iter().collect();
        let pool = build_pool(&refs, &scope, &est).unwrap();
        let base = vec![100.0, 10.0, 10.0];
        let plan = optimize_join_order(&[VIRTUAL, 1, 2], &base, &pool);
        assert_eq!(plan.order[0], VIRTUAL);
        assert_eq!(plan.steps.len(), 2);
    }
}
