//! HashAgg:分组聚合算子。
//!
//! 流程:拉完输入批 → 每行算分组键(标量序列作 HashMap 键)→ 更新各聚合状态
//! → 一次性产出结果行(按 batch_size 切批)。单批物化足够 v1;流式/溢出见路线图。
//!
//! null 语义:COUNT(x) 计非 null;SUM/AVG/MIN/MAX 忽略 null,全 null 结果为
//! null(除 COUNT 为 0);COUNT(*) 计行数。无 GROUP BY 时恒输出一行。

use std::collections::HashMap;

use common::error::{Error, Result};
use common::schema::{ColumnDesc, Row};
use common::types::{DataType, ScalarValue};

use crate::agg::{AggDesc, AggFunc};
use crate::chunk::DataChunk;
use crate::executor::{BoxedExecutor, ExecuteContext, Executor};
use crate::expr::Expr;
use crate::vector::Vector;

/// 分组键(标量值序列;ScalarValue 已实现 Hash/Eq,NaN 归一)。
type GroupKey = Vec<ScalarValue>;

#[derive(Debug, Clone)]
enum AggState {
    Count(u64),
    /// COUNT(DISTINCT x):去重值集合(输出集合大小)
    CountDistinct(std::collections::HashSet<ScalarValue>),
    /// SUM:按输出类型分别累计
    SumI64(Option<i64>),
    SumF64(Option<f64>),
    /// AVG:累计 sum 与计数,输出 Float64
    Avg {
        sum: f64,
        n: u64,
    },
    /// MIN/MAX:当前极值(None = 尚无有效值)
    Extreme(Option<ScalarValue>, bool /*is_max*/),
}

impl AggState {
    fn new(desc: &AggDesc) -> Self {
        match desc.func {
            AggFunc::CountStar | AggFunc::Count => {
                if desc.distinct {
                    AggState::CountDistinct(Default::default())
                } else {
                    AggState::Count(0)
                }
            }
            AggFunc::Sum => match desc.out_type {
                DataType::Float64 => AggState::SumF64(None),
                _ => AggState::SumI64(None),
            },
            AggFunc::Avg => AggState::Avg { sum: 0.0, n: 0 },
            AggFunc::Min => AggState::Extreme(None, false),
            AggFunc::Max => AggState::Extreme(None, true),
        }
    }

    fn update(&mut self, value: ScalarValue, desc: &AggDesc) -> Result<()> {
        // 所有聚合都忽略 NULL;COUNT(*) 的占位值恒为 Boolean(true),等同非空
        if value.is_null() {
            return Ok(());
        }
        match self {
            AggState::Count(n) => *n += 1,
            AggState::CountDistinct(set) => {
                set.insert(value);
            }
            AggState::SumI64(acc) => match value {
                ScalarValue::Int64(v) => {
                    *acc = Some(
                        acc.unwrap_or(0)
                            .checked_add(v)
                            .ok_or_else(|| Error::sql("integer overflow in SUM"))?,
                    );
                }
                other => return Err(type_err(desc, &other)),
            },
            AggState::SumF64(acc) => match value {
                ScalarValue::Float64(v) => *acc = Some(acc.unwrap_or(0.0) + v),
                ScalarValue::Int64(v) => *acc = Some(acc.unwrap_or(0.0) + v as f64),
                other => return Err(type_err(desc, &other)),
            },
            AggState::Avg { sum, n } => match value {
                ScalarValue::Int64(v) => {
                    *sum += v as f64;
                    *n += 1;
                }
                ScalarValue::Float64(v) => {
                    *sum += v;
                    *n += 1;
                }
                other => return Err(type_err(desc, &other)),
            },
            AggState::Extreme(acc, is_max) => {
                let replace = match acc {
                    None => true,
                    Some(cur) => {
                        let ord = compare_values(cur, &value)?;
                        if *is_max {
                            ord == std::cmp::Ordering::Less
                        } else {
                            ord == std::cmp::Ordering::Greater
                        }
                    }
                };
                if replace {
                    *acc = Some(value);
                }
            }
        }
        Ok(())
    }

    fn finish(&self) -> ScalarValue {
        match self {
            AggState::Count(n) => ScalarValue::Int64(*n as i64),
            AggState::CountDistinct(set) => ScalarValue::Int64(set.len() as i64),
            AggState::SumI64(acc) => acc.map(ScalarValue::Int64).unwrap_or(ScalarValue::Null),
            AggState::SumF64(acc) => acc.map(ScalarValue::Float64).unwrap_or(ScalarValue::Null),
            AggState::Avg { sum, n } => {
                if *n == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float64(sum / *n as f64)
                }
            }
            AggState::Extreme(acc, _) => acc.clone().unwrap_or(ScalarValue::Null),
        }
    }
}

fn type_err(desc: &AggDesc, got: &ScalarValue) -> Error {
    Error::sql(format!(
        "type mismatch in {}(): expected {}, got {:?}",
        desc.func.name(),
        desc.out_type.name(),
        got
    ))
}

/// 标量值全序比较(委托 common::types,SortExec 与聚合极值共用)。
pub fn compare_values(l: &ScalarValue, r: &ScalarValue) -> Result<std::cmp::Ordering> {
    common::types::compare_values(l, r)
}

pub struct AggHashExec {
    input: BoxedExecutor,
    group_exprs: Vec<Expr>,
    aggs: Vec<AggDesc>,
    /// 输入批的数据类型(SeqScan 等 schema 与之一致;空表占位批构造用)
    input_dtypes: Vec<DataType>,
    schema: Vec<ColumnDesc>,
    result_rows: Option<Vec<Row>>,
    next: usize,
}

impl AggHashExec {
    pub fn new(
        input: BoxedExecutor,
        input_dtypes: Vec<DataType>,
        group_exprs: Vec<Expr>,
        aggs: Vec<AggDesc>,
        schema: Vec<ColumnDesc>,
    ) -> Self {
        Self {
            input,
            group_exprs,
            aggs,
            input_dtypes,
            schema,
            result_rows: None,
            next: 0,
        }
    }

    fn build_result(&mut self, ctx: &mut ExecuteContext) -> Result<()> {
        let mut groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();

        loop {
            let Some(batch) = self.input.next_batch(ctx)? else {
                break;
            };
            // 分组键列与聚合参数列整列求值,行内取标量
            let key_cols = self
                .group_exprs
                .iter()
                .map(|e| e.eval(&batch))
                .collect::<Result<Vec<_>>>()?;
            let arg_cols = self
                .aggs
                .iter()
                .map(|d| match &d.arg {
                    Some(e) => e.eval(&batch).map(Some),
                    // COUNT(*):占位列(恒非 null,等效计行数)
                    None => Ok(None),
                })
                .collect::<Result<Vec<Option<Vector>>>>()?;

            for i in 0..batch.num_rows() {
                let key: GroupKey = key_cols.iter().map(|c| c.scalar_at(i)).collect();
                let states = groups
                    .entry(key)
                    .or_insert_with(|| self.aggs.iter().map(AggState::new).collect());
                for (pos, state) in states.iter_mut().enumerate() {
                    match &arg_cols[pos] {
                        Some(col) => state.update(col.scalar_at(i), &self.aggs[pos])?,
                        None => state.update(ScalarValue::Boolean(true), &self.aggs[pos])?,
                    }
                }
            }
        }

        let mut rows: Vec<Row> = Vec::with_capacity(groups.len());
        for (key, states) in groups {
            let mut row: Row = key;
            row.reserve(states.len());
            for state in &states {
                row.push(state.finish());
            }
            rows.push(row);
        }
        // 无 GROUP BY 时恒输出一行(空表聚合 = COUNT 0 / 其余 NULL)
        if self.group_exprs.is_empty() && rows.is_empty() {
            rows.push(
                self.aggs
                    .iter()
                    .map(|d| AggState::new(d).finish())
                    .collect(),
            );
        }
        // 排序保证输出确定性(显式 ORDER BY 由 SortExec 负责);Null 排最前
        fn cmp_row(a: &Row, b: &Row) -> std::cmp::Ordering {
            use std::cmp::Ordering;
            for (x, y) in a.iter().zip(b.iter()) {
                let ord = match (x.is_null(), y.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    (false, false) => compare_values(x, y).unwrap_or(Ordering::Equal),
                };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            a.len().cmp(&b.len())
        }
        rows.sort_by(cmp_row);
        self.result_rows = Some(rows);
        Ok(())
    }
}

impl Executor for AggHashExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        if self.result_rows.is_none() {
            self.build_result(ctx)?;
        }
        let rows = self.result_rows.as_ref().expect("result built above");
        if self.next >= rows.len() {
            return Ok(None);
        }
        let end = (self.next + ctx.batch_size).min(rows.len());
        let dtypes: Vec<DataType> = self.schema.iter().map(|c| c.ty.clone()).collect();
        let _ = &self.input_dtypes;
        let chunk = DataChunk::with_rows(&dtypes, &rows[self.next..end], &ctx.mem.tracker)?;
        self.next = end;
        Ok(Some(chunk))
    }

    fn schema(&self) -> &[ColumnDesc] {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecuteContext;
    use crate::operators::values::ValuesExec;
    use memory::{MemoryContext, MemoryTracker};
    use std::sync::Arc;

    fn schema() -> Vec<ColumnDesc> {
        vec![
            ColumnDesc::new("dept", DataType::Utf8, true),
            ColumnDesc::new("salary", DataType::Int64, true),
        ]
    }

    fn rows() -> Vec<Row> {
        vec![
            vec![ScalarValue::Utf8("eng".into()), ScalarValue::Int64(100)],
            vec![ScalarValue::Utf8("eng".into()), ScalarValue::Int64(200)],
            vec![ScalarValue::Utf8("ops".into()), ScalarValue::Null],
            vec![ScalarValue::Utf8("ops".into()), ScalarValue::Int64(50)],
            vec![ScalarValue::Null, ScalarValue::Int64(10)],
        ]
    }

    fn run(rows: Vec<Row>, groups: Vec<Expr>, aggs: Vec<AggDesc>) -> Vec<Vec<ScalarValue>> {
        let input = Box::new(ValuesExec::new(schema(), rows));
        let input_dtypes = schema().iter().map(|c| c.ty.clone()).collect();
        let out_schema = groups
            .iter()
            .enumerate()
            .map(|(i, _)| ColumnDesc::new(format!("g{i}"), DataType::Utf8, true))
            .chain(
                aggs.iter()
                    .enumerate()
                    .map(|(i, d)| ColumnDesc::new(format!("a{i}"), d.out_type.clone(), true)),
            )
            .collect();
        let mut exec = AggHashExec::new(input, input_dtypes, groups, aggs, out_schema);
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(64, MemoryContext::new(&root, "q", 4096));
        let mut out = Vec::new();
        while let Some(chunk) = exec.next_batch(&mut ctx).unwrap() {
            for i in 0..chunk.num_rows() {
                out.push(chunk.row(i));
            }
        }
        out
    }

    fn col(i: usize) -> Expr {
        Expr::Column(i)
    }

    #[test]
    fn group_by_with_count_sum_avg() {
        let aggs = vec![
            AggDesc::new(AggFunc::CountStar, None, &schema()).unwrap(),
            AggDesc::new(AggFunc::Count, Some(col(1)), &schema()).unwrap(),
            AggDesc::new(AggFunc::Sum, Some(col(1)), &schema()).unwrap(),
            AggDesc::new(AggFunc::Avg, Some(col(1)), &schema()).unwrap(),
        ];
        let out = run(rows(), vec![col(0)], aggs);
        assert_eq!(out.len(), 3);
        // 排序后:NULL 组 / eng 组 / ops 组
        let eng = &out[1];
        assert_eq!(eng[0], ScalarValue::Utf8("eng".into()));
        assert_eq!(
            eng[1..5],
            [
                ScalarValue::Int64(2),
                ScalarValue::Int64(2),
                ScalarValue::Int64(300),
                ScalarValue::Float64(150.0)
            ]
        );
        // ops:salary 一行 NULL → COUNT(*)=2, COUNT(salary)=1, SUM=50, AVG=50
        let ops = &out[2];
        assert_eq!(ops[0], ScalarValue::Utf8("ops".into()));
        assert_eq!(ops[1], ScalarValue::Int64(2));
        assert_eq!(ops[2], ScalarValue::Int64(1));
        assert_eq!(ops[3], ScalarValue::Int64(50));
        assert_eq!(ops[4], ScalarValue::Float64(50.0));
    }

    #[test]
    fn min_max_ignore_nulls() {
        let aggs = vec![
            AggDesc::new(AggFunc::Min, Some(col(1)), &schema()).unwrap(),
            AggDesc::new(AggFunc::Max, Some(col(1)), &schema()).unwrap(),
        ];
        let out = run(rows(), vec![col(0)], aggs);
        let ops = &out[2];
        assert_eq!(ops[0], ScalarValue::Utf8("ops".into()));
        assert_eq!(ops[1], ScalarValue::Int64(50), "MIN 忽略 NULL");
        assert_eq!(ops[2], ScalarValue::Int64(50));
    }

    #[test]
    fn global_aggregation_over_empty_input_yields_one_row() {
        let aggs = vec![
            AggDesc::new(AggFunc::CountStar, None, &schema()).unwrap(),
            AggDesc::new(AggFunc::Sum, Some(col(1)), &schema()).unwrap(),
        ];
        let out = run(Vec::new(), Vec::new(), aggs);
        assert_eq!(out, vec![vec![ScalarValue::Int64(0), ScalarValue::Null]]);
    }

    #[test]
    fn grouped_aggregation_over_empty_input_yields_no_rows() {
        let aggs = vec![AggDesc::new(AggFunc::CountStar, None, &schema()).unwrap()];
        let out = run(Vec::new(), vec![col(0)], aggs);
        assert!(out.is_empty());
    }

    #[test]
    fn sum_overflow_errors() {
        let big: Vec<Row> = (0..3)
            .map(|_| vec![ScalarValue::Utf8("g".into()), ScalarValue::Int64(i64::MAX)])
            .collect();
        let aggs = vec![AggDesc::new(AggFunc::Sum, Some(col(1)), &schema()).unwrap()];
        let input = Box::new(ValuesExec::new(schema(), big));
        let input_dtypes = schema().iter().map(|c| c.ty.clone()).collect();
        let mut exec = AggHashExec::new(
            input,
            input_dtypes,
            vec![col(0)],
            aggs,
            vec![ColumnDesc::new("s", DataType::Int64, true)],
        );
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(64, MemoryContext::new(&root, "q", 4096));
        assert!(exec.next_batch(&mut ctx).is_err());
    }

    #[test]
    fn float_sum_column() {
        // SUM over Float64 列
        let frows: Vec<Row> = vec![
            vec![ScalarValue::Utf8("g".into()), ScalarValue::Float64(1.5)],
            vec![ScalarValue::Utf8("g".into()), ScalarValue::Float64(2.25)],
        ];
        let fschema = vec![
            ColumnDesc::new("g", DataType::Utf8, true),
            ColumnDesc::new("v", DataType::Float64, true),
        ];
        let aggs = vec![AggDesc::new(AggFunc::Sum, Some(col(1)), &fschema.clone()).unwrap()];
        let input = Box::new(ValuesExec::new(fschema.clone(), frows));
        let input_dtypes = fschema.iter().map(|c| c.ty.clone()).collect();
        let mut exec = AggHashExec::new(
            input,
            input_dtypes,
            Vec::new(),
            aggs,
            vec![ColumnDesc::new("s", DataType::Float64, true)],
        );
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(64, MemoryContext::new(&root, "q", 4096));
        let chunk = exec.next_batch(&mut ctx).unwrap().unwrap();
        assert_eq!(chunk.row(0)[0], ScalarValue::Float64(3.75));
    }

    #[test]
    fn memory_released() {
        let aggs = vec![AggDesc::new(AggFunc::CountStar, None, &schema()).unwrap()];
        let input = Box::new(ValuesExec::new(schema(), rows()));
        let input_dtypes = schema().iter().map(|c| c.ty.clone()).collect();
        let mut exec = AggHashExec::new(
            input,
            input_dtypes,
            vec![col(0)],
            aggs,
            vec![
                ColumnDesc::new("dept", DataType::Utf8, true),
                ColumnDesc::new("c", DataType::Int64, true),
            ],
        );
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(64, MemoryContext::new(&root, "q", 4096));
        while exec.next_batch(&mut ctx).unwrap().is_some() {}
        assert_eq!(root.used(), 0);
        let _ = Arc::new(0); // keep import used
    }
}
