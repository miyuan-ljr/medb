//! Sort:全量物化 + 多键排序(键列向量化求值,行间标量比较)。
//!
//! 排序语义对齐 PG:NULL 默认排在最后(ASC);DESC 时默认 NULL 在前
//! (即 nulls_first = !asc,除非显式 NULLS FIRST/LAST)。

use common::error::Result;
use common::schema::{ColumnDesc, Row};
use common::types::DataType;
use std::cmp::Ordering;

use crate::chunk::DataChunk;
use crate::executor::{BoxedExecutor, ExecuteContext, Executor};
use crate::expr::Expr;
use crate::operators::hash_agg::compare_values;

#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}

pub struct SortExec {
    input: BoxedExecutor,
    keys: Vec<SortKey>,
    schema: Vec<ColumnDesc>,
    input_dtypes: Vec<DataType>,
    /// Top-N:下游紧跟 LIMIT k(无 OFFSET)时,流式维护前 k 行,排序 O(n log k)
    topn: Option<usize>,
    rows: Option<Vec<Row>>,
    next: usize,
}

impl SortExec {
    pub fn new(input: BoxedExecutor, keys: Vec<SortKey>, schema: Vec<ColumnDesc>) -> Self {
        let input_dtypes = schema.iter().map(|c| c.ty.clone()).collect();
        Self {
            input,
            keys,
            schema,
            input_dtypes,
            topn: None,
            rows: None,
            next: 0,
        }
    }

    /// Top-N 模式:下游紧跟 `LIMIT k`(无 OFFSET)时由 planner 启用。
    pub fn with_topn(mut self, k: usize) -> Self {
        self.topn = Some(k.max(1));
        self
    }
}

impl Executor for SortExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        if self.rows.is_none() {
            // Top-N:流式维护前 k 行(键以行式存储,随批追加 + 定期裁剪)
            if let Some(k) = self.topn {
                let mut rows: Vec<Row> = Vec::new();
                let mut key_rows: Vec<Row> = Vec::new();
                loop {
                    let Some(batch) = self.input.next_batch(ctx)? else {
                        break;
                    };
                    let key_vecs = self
                        .keys
                        .iter()
                        .map(|k| k.expr.eval(&batch))
                        .collect::<Result<Vec<_>>>()?;
                    for i in 0..batch.num_rows() {
                        rows.push(batch.row(i));
                        key_rows.push(key_vecs.iter().map(|v| v.scalar_at(i)).collect::<Row>());
                    }
                    // 累积超 4k 行 → 裁剪到前 k(分摊成本)
                    if rows.len() > k * 4 {
                        trim_topn(&mut rows, &mut key_rows, &self.keys, k);
                    }
                }
                trim_topn(&mut rows, &mut key_rows, &self.keys, k);
                let mut order: Vec<usize> = (0..rows.len()).collect();
                order.sort_by(|&a, &b| compare_key_rows(&key_rows[a], &key_rows[b], &self.keys));
                let sorted_rows: Vec<Row> = order.into_iter().map(|i| rows[i].clone()).collect();
                self.rows = Some(sorted_rows.into_iter().take(k).collect());
            } else {
                // 全量物化
                let mut rows: Vec<Row> = Vec::new();
                loop {
                    let Some(batch) = self.input.next_batch(ctx)? else {
                        break;
                    };
                    for i in 0..batch.num_rows() {
                        rows.push(batch.row(i));
                    }
                }
                if !rows.is_empty() && !self.keys.is_empty() {
                    // 键列整列求值后排序
                    let chunk = DataChunk::from_rows(&self.input_dtypes, &rows)?;
                    let key_cols = self
                        .keys
                        .iter()
                        .map(|k| k.expr.eval(&chunk))
                        .collect::<Result<Vec<_>>>()?;

                    let mut order: Vec<usize> = (0..rows.len()).collect();
                    order.sort_by(|&a, &b| {
                        for (key, col) in self.keys.iter().zip(&key_cols) {
                            let (va, vb) = (col.scalar_at(a), col.scalar_at(b));
                            let ord = match (va.is_null(), vb.is_null()) {
                                (true, true) => Ordering::Equal,
                                (true, false) => {
                                    if key.nulls_first {
                                        Ordering::Less
                                    } else {
                                        Ordering::Greater
                                    }
                                }
                                (false, true) => {
                                    if key.nulls_first {
                                        Ordering::Greater
                                    } else {
                                        Ordering::Less
                                    }
                                }
                                (false, false) => {
                                    let o = compare_values(&va, &vb).unwrap_or(Ordering::Equal);
                                    if key.asc { o } else { o.reverse() }
                                }
                            };
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                        Ordering::Equal
                    });
                    let sorted_rows: Vec<Row> = {
                        let mut out = Vec::with_capacity(rows.len());
                        for i in order {
                            out.push(rows[i].clone());
                        }
                        out
                    };
                    self.rows = Some(sorted_rows);
                } else {
                    self.rows = Some(rows);
                }
            }
        }
        let rows = self.rows.as_ref().expect("rows built above");
        if self.next >= rows.len() {
            return Ok(None);
        }
        let end = (self.next + ctx.batch_size).min(rows.len());
        let chunk =
            DataChunk::with_rows(&self.input_dtypes, &rows[self.next..end], &ctx.mem.tracker)?;
        self.next = end;
        Ok(Some(chunk))
    }

    fn schema(&self) -> &[ColumnDesc] {
        &self.schema
    }
}

/// Top-N 裁剪:保留当前排序下前 k 行(按 base 偏移回全局行号)。
/// 简单可靠版:整体排序后截断(行数被 4k 上限约束,均摊 O(n log n) 且常数小;
/// 流式小顶堆版本见路线图)。
fn trim_topn(rows: &mut Vec<Row>, key_rows: &mut Vec<Row>, keys: &[SortKey], k: usize) {
    if rows.len() <= k {
        return;
    }
    let n = rows.len();
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| compare_key_rows(&key_rows[a], &key_rows[b], keys));
    let keep: std::collections::HashSet<usize> = order[..k].iter().copied().collect();
    let mut new_rows = Vec::with_capacity(k);
    let mut new_key_rows = Vec::with_capacity(k);
    for i in 0..n {
        if keep.contains(&i) {
            new_rows.push(rows[i].clone());
            new_key_rows.push(key_rows[i].clone());
        }
    }
    *rows = new_rows;
    *key_rows = new_key_rows;
}

/// 对键行做排序比较(语义与 sort_order 一致)。
fn compare_key_rows(a: &Row, b: &Row, keys: &[SortKey]) -> Ordering {
    for (i, key) in keys.iter().enumerate() {
        let (va, vb) = (&a[i], &b[i]);
        let ord = match (va.is_null(), vb.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if key.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if key.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let o = compare_values(va, vb).unwrap_or(Ordering::Equal);
                if key.asc { o } else { o.reverse() }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecuteContext;
    use crate::operators::values::ValuesExec;
    use common::types::ScalarValue;
    use memory::{MemoryContext, MemoryTracker};

    fn schema() -> Vec<ColumnDesc> {
        vec![
            ColumnDesc::new("id", DataType::Int64, true),
            ColumnDesc::new("name", DataType::Utf8, true),
        ]
    }

    fn run(rows: Vec<Row>, keys: Vec<SortKey>) -> Vec<Vec<ScalarValue>> {
        let input = Box::new(ValuesExec::new(schema(), rows));
        let mut exec = SortExec::new(input, keys, schema());
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(2, MemoryContext::new(&root, "q", 4096));
        let mut out = Vec::new();
        while let Some(chunk) = exec.next_batch(&mut ctx).unwrap() {
            for i in 0..chunk.num_rows() {
                out.push(chunk.row(i));
            }
        }
        out
    }

    #[test]
    fn order_by_asc_and_nulls() {
        let rows = vec![
            vec![ScalarValue::Int64(3), ScalarValue::Utf8("c".into())],
            vec![ScalarValue::Null, ScalarValue::Utf8("n".into())],
            vec![ScalarValue::Int64(1), ScalarValue::Utf8("a".into())],
        ];
        let out = run(
            rows,
            vec![SortKey {
                expr: Expr::Column(0),
                asc: true,
                nulls_first: false,
            }],
        );
        assert_eq!(out[0][0], ScalarValue::Int64(1));
        assert_eq!(out[1][0], ScalarValue::Int64(3));
        assert_eq!(out[2][0], ScalarValue::Null, "ASC 默认 NULL 在最后");
    }

    #[test]
    fn order_by_desc_nulls_first_default() {
        let rows = vec![
            vec![ScalarValue::Int64(3), ScalarValue::Utf8("c".into())],
            vec![ScalarValue::Null, ScalarValue::Utf8("n".into())],
            vec![ScalarValue::Int64(1), ScalarValue::Utf8("a".into())],
        ];
        let out = run(
            rows,
            vec![SortKey {
                expr: Expr::Column(0),
                asc: false,
                nulls_first: true, // DESC 默认 NULL 在前(PG 语义)
            }],
        );
        assert_eq!(out[0][0], ScalarValue::Null);
        assert_eq!(out[1][0], ScalarValue::Int64(3));
        assert_eq!(out[2][0], ScalarValue::Int64(1));
    }

    #[test]
    fn multi_key_stable_order() {
        let rows = vec![
            vec![ScalarValue::Int64(1), ScalarValue::Utf8("b".into())],
            vec![ScalarValue::Int64(1), ScalarValue::Utf8("a".into())],
            vec![ScalarValue::Int64(0), ScalarValue::Utf8("z".into())],
        ];
        let out = run(
            rows,
            vec![
                SortKey {
                    expr: Expr::Column(0),
                    asc: true,
                    nulls_first: false,
                },
                SortKey {
                    expr: Expr::Column(1),
                    asc: true,
                    nulls_first: false,
                },
            ],
        );
        assert_eq!(out[0][1], ScalarValue::Utf8("z".into()));
        assert_eq!(out[1][1], ScalarValue::Utf8("a".into()));
        assert_eq!(out[2][1], ScalarValue::Utf8("b".into()));
    }

    #[test]
    fn empty_input() {
        let out = run(
            Vec::new(),
            vec![SortKey {
                expr: Expr::Column(0),
                asc: true,
                nulls_first: false,
            }],
        );
        assert!(out.is_empty());
    }
}
