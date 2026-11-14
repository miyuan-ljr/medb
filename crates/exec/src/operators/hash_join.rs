//! Hash Join:等值连接(内连接 + 左外连接)。
//!
//! 构建侧(右表)整批物化为哈希表,探测侧(左表)逐批流式探测;
//! 连接键分别按探测/构建两侧的 schema 求值(计划期以列下标表达式给出)。
//! NULL 键不参与连接(标准语义);左外连接保留未命中的左行,右列补 NULL。
//! match_cond 在匹配期按对判定(ON 的非等值部分;LEFT JOIN 下
//! 未通过条件的探测行仍 NULL 补齐);residual 在输出前整批过滤
//! (WHERE 路由下来的谓词,作用于含 NULL 补齐行的完整连接结果)。

use common::error::Result;
use common::schema::{ColumnDesc, Row};
use common::types::{DataType, ScalarValue};
use std::collections::HashMap;

use crate::chunk::DataChunk;
use crate::executor::{BoxedExecutor, ExecuteContext, Executor};
use crate::expr::Expr;
use memory::Reservation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
}

pub struct HashJoinExec {
    probe: BoxedExecutor,
    build: BoxedExecutor,
    probe_keys: Vec<Expr>,
    build_keys: Vec<Expr>,
    /// 匹配期条件(逐对判定,决定是否算命中)
    match_cond: Option<Expr>,
    /// 连接后过滤条件(WHERE 路由,含 NULL 补齐行)
    residual: Option<Expr>,
    join_type: JoinType,
    schema: Vec<ColumnDesc>,
    combined_dtypes: Vec<DataType>,
    build_dtypes: Vec<DataType>,
    /// 构建侧哈希表:键 → 匹配的构建行
    table: Option<HashMap<Vec<ScalarValue>, Vec<Row>>>,
    build_null_row: Row,
    probe_done: bool,
    out: Vec<Row>,
    /// 物化内存记账(build 哈希表 + 输出缓冲),超限 → MemoryLimitExceeded
    mem: Option<Reservation>,
    mem_bytes: usize,
    next: usize,
}

impl HashJoinExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        probe: BoxedExecutor,
        build: BoxedExecutor,
        probe_schema: Vec<ColumnDesc>,
        build_schema: Vec<ColumnDesc>,
        probe_keys: Vec<Expr>,
        build_keys: Vec<Expr>,
        match_cond: Option<Expr>,
        residual: Option<Expr>,
        join_type: JoinType,
    ) -> Self {
        let build_dtypes = build_schema.iter().map(|c| c.ty.clone()).collect();
        let mut schema = probe_schema;
        schema.extend(build_schema);
        let combined_dtypes = schema.iter().map(|c| c.ty.clone()).collect();
        Self {
            probe,
            build,
            probe_keys,
            build_keys,
            match_cond,
            residual,
            join_type,
            schema,
            combined_dtypes,
            build_dtypes,
            table: None,
            build_null_row: Vec::new(),
            probe_done: false,
            out: Vec::new(),
            mem: None,
            mem_bytes: 0,
            next: 0,
        }
    }

    /// 物化增长记账:累积 Reservation 随执行器生命周期释放。
    fn track_mem(&mut self, ctx: &ExecuteContext, extra: usize) -> Result<()> {
        let new_total = self.mem_bytes + extra;
        match self.mem.as_mut() {
            Some(r) => r.resize(new_total)?,
            None => self.mem = Some(ctx.mem.tracker.try_reserve(new_total)?),
        }
        self.mem_bytes = new_total;
        Ok(())
    }

    /// 构建侧整批物化为哈希表;NULL 键不入表(不与任何行连接)。
    fn build_table(&mut self, ctx: &mut ExecuteContext) -> Result<()> {
        let mut table: HashMap<Vec<ScalarValue>, Vec<Row>> = HashMap::new();
        loop {
            let Some(batch) = self.build.next_batch(ctx)? else {
                break;
            };
            let key_cols = self
                .build_keys
                .iter()
                .map(|e| e.eval(&batch))
                .collect::<Result<Vec<_>>>()?;
            for i in 0..batch.num_rows() {
                let key: Vec<ScalarValue> = key_cols.iter().map(|c| c.scalar_at(i)).collect();
                if key.iter().any(|v| v.is_null()) {
                    continue;
                }
                let row = batch.row(i);
                self.track_mem(
                    ctx,
                    crate::chunk::row_approx_bytes(&self.build_dtypes, &row) + key.len() * 16,
                )?;
                table.entry(key).or_default().push(row);
            }
        }
        // 左外连接的 NULL 填充行模板
        self.build_null_row = vec![ScalarValue::Null; self.build_dtypes.len()];
        self.table = Some(table);
        Ok(())
    }

    /// 拉取并消化探测侧,产出全部输出行。
    /// match_cond 在匹配期按对判定(LEFT JOIN 下未过条件的探测行仍 NULL 补齐);
    /// residual 在输出前整批过滤(作用于含 NULL 补齐行的完整连接结果)。
    fn materialize(&mut self, ctx: &mut ExecuteContext) -> Result<()> {
        self.build_table(ctx)?;
        // take 出来:后面 track_mem 需要 &mut self;materialize 后哈希表不再需要
        let table = std::mem::take(&mut self.table).expect("build_table called first");
        let mut out: Vec<Row> = Vec::new();
        let left = self.join_type == JoinType::LeftOuter;

        loop {
            let Some(batch) = self.probe.next_batch(ctx)? else {
                break;
            };
            let key_cols = self
                .probe_keys
                .iter()
                .map(|e| e.eval(&batch))
                .collect::<Result<Vec<_>>>()?;
            // 按探测行分组收集候选组合行;null_ext 记录直接 NULL 补齐的探测行
            let mut cand: Vec<Vec<Row>> = vec![Vec::new(); batch.num_rows()];
            let mut null_ext: Vec<bool> = vec![false; batch.num_rows()];
            for i in 0..batch.num_rows() {
                let key: Vec<ScalarValue> = key_cols.iter().map(|c| c.scalar_at(i)).collect();
                if key.iter().any(|v| v.is_null()) {
                    null_ext[i] = left;
                    continue;
                }
                match table.get(&key) {
                    Some(matches) if !matches.is_empty() => {
                        let probe_row = batch.row(i);
                        for build_row in matches {
                            let mut row = probe_row.clone();
                            row.extend(build_row.iter().cloned());
                            cand[i].push(row);
                        }
                    }
                    _ => null_ext[i] = left,
                }
            }
            // match_cond:候选整批求值,按探测行保留通过者
            if let Some(cond) = &self.match_cond {
                let mut flat: Vec<Row> = Vec::new();
                let mut owner: Vec<usize> = Vec::new();
                for (i, rows) in cand.iter().enumerate() {
                    for r in rows {
                        flat.push(r.clone());
                        owner.push(i);
                    }
                }
                if !flat.is_empty() {
                    let chunk = DataChunk::from_rows(&self.combined_dtypes, &flat)?;
                    let mask = cond.eval(&chunk)?;
                    let mut kept: Vec<Vec<Row>> = vec![Vec::new(); batch.num_rows()];
                    for j in 0..flat.len() {
                        if !mask.null_at(j) && mask.bool_values()[j] {
                            kept[owner[j]].push(flat[j].clone());
                        }
                    }
                    cand = kept;
                }
            }
            // 产出:命中行按序输出;无命中(或全被 match_cond 滤掉)左外补 NULL
            for i in 0..batch.num_rows() {
                if null_ext[i] {
                    let mut row = batch.row(i);
                    row.extend(self.build_null_row.iter().cloned());
                    self.track_mem(
                        ctx,
                        crate::chunk::row_approx_bytes(&self.combined_dtypes, &row),
                    )?;
                    out.push(row);
                } else if cand[i].is_empty() {
                    if left {
                        let mut row = batch.row(i);
                        row.extend(self.build_null_row.iter().cloned());
                        self.track_mem(
                            ctx,
                            crate::chunk::row_approx_bytes(&self.combined_dtypes, &row),
                        )?;
                        out.push(row);
                    }
                } else {
                    for r in &cand[i] {
                        self.track_mem(
                            ctx,
                            crate::chunk::row_approx_bytes(&self.combined_dtypes, r),
                        )?;
                    }
                    out.append(&mut cand[i]);
                }
            }
        }

        // residual(WHERE 路由):组合列上整批过滤
        if let Some(residual) = &self.residual {
            let chunk = DataChunk::from_rows(&self.combined_dtypes, &out)?;
            let mask = residual.eval(&chunk)?;
            let keep: Vec<bool> = (0..out.len())
                .map(|i| mask.bool_values()[i] && !mask.null_at(i))
                .collect();
            out = out
                .into_iter()
                .zip(keep)
                .filter(|(_, k)| *k)
                .map(|(r, _)| r)
                .collect();
        }

        self.probe_done = true;
        self.out = out;
        Ok(())
    }
}

impl Executor for HashJoinExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        if !self.probe_done {
            self.materialize(ctx)?;
        }
        let rows: &[Row] = &self.out;
        if self.next >= rows.len() {
            return Ok(None);
        }
        let end = (self.next + ctx.batch_size).min(rows.len());
        let chunk = DataChunk::with_rows(
            &self.combined_dtypes,
            &rows[self.next..end],
            &ctx.mem.tracker,
        )?;
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
    use common::schema::ColumnDesc;
    use common::types::ScalarValue;
    use memory::{MemoryContext, MemoryTracker};
    use std::sync::Arc;

    fn schema(names: &[&str]) -> Vec<ColumnDesc> {
        names
            .iter()
            .map(|n| ColumnDesc::new((*n).to_string(), DataType::Int64, true))
            .collect()
    }

    fn int_rows(rows: &[(i64, i64)]) -> Vec<Row> {
        rows.iter()
            .map(|(a, b)| vec![ScalarValue::Int64(*a), ScalarValue::Int64(*b)])
            .collect()
    }

    fn drain(exec: &mut dyn Executor, root: &Arc<MemoryTracker>) -> Vec<Row> {
        let mut ctx = ExecuteContext::new(64, MemoryContext::new(root, "q", 4096));
        let mut out = Vec::new();
        while let Some(chunk) = exec.next_batch(&mut ctx).unwrap() {
            for i in 0..chunk.num_rows() {
                out.push(chunk.row(i));
            }
        }
        out
    }

    fn probe_schema() -> Vec<ColumnDesc> {
        schema(&["id", "cust"])
    }

    fn build_schema() -> Vec<ColumnDesc> {
        schema(&["cid", "region"])
    }

    #[test]
    fn inner_join_matches_keys() {
        // probe: orders(id, cust) / build: customers(cid, region)
        let probe = Box::new(ValuesExec::new(
            probe_schema(),
            int_rows(&[(1, 10), (2, 20), (3, 10)]),
        ));
        let build = Box::new(ValuesExec::new(
            build_schema(),
            int_rows(&[(10, 1), (30, 3), (10, 2)]),
        ));
        let mut exec = HashJoinExec::new(
            probe,
            build,
            probe_schema(),
            build_schema(),
            vec![Expr::Column(1)], // probe.cust
            vec![Expr::Column(0)], // build.cid
            None,
            None,
            JoinType::Inner,
        );
        let root = MemoryTracker::new_root("t", None);
        let out = drain(&mut exec, &root);
        assert_eq!(out.len(), 4); // cust=10 匹配 2 行 + cust=20 无匹配 + cust=10 又 2 行
        // 每行 4 列
        assert_eq!(out[0].len(), 4);
    }

    #[test]
    fn left_outer_preserves_unmatched() {
        let probe = Box::new(ValuesExec::new(
            probe_schema(),
            int_rows(&[(1, 10), (2, 20)]),
        ));
        let build = Box::new(ValuesExec::new(build_schema(), int_rows(&[(10, 1)])));
        let mut exec = HashJoinExec::new(
            probe,
            build,
            probe_schema(),
            build_schema(),
            vec![Expr::Column(1)],
            vec![Expr::Column(0)],
            None,
            None,
            JoinType::LeftOuter,
        );
        let root = MemoryTracker::new_root("t", None);
        let out = drain(&mut exec, &root);
        assert_eq!(out.len(), 2);
        // 未匹配行:构建列全 NULL
        assert_eq!(out[1][2], ScalarValue::Null);
        assert_eq!(out[1][3], ScalarValue::Null);
    }

    #[test]
    fn residual_filter_applies() {
        let probe = Box::new(ValuesExec::new(
            probe_schema(),
            int_rows(&[(1, 10), (2, 10)]),
        ));
        let build = Box::new(ValuesExec::new(
            build_schema(),
            int_rows(&[(10, 1), (10, 3)]),
        ));
        // residual:region = 1(组合第 4 列,布尔谓词)
        let mut exec = HashJoinExec::new(
            probe,
            build,
            probe_schema(),
            build_schema(),
            vec![Expr::Column(1)],
            vec![Expr::Column(0)],
            None,
            Some(Expr::binary(
                crate::expr::BinaryOp::Eq,
                Expr::Column(3),
                Expr::Literal(ScalarValue::Int64(1)),
            )),
            JoinType::Inner,
        );
        let root = MemoryTracker::new_root("t", None);
        let out = drain(&mut exec, &root);
        assert_eq!(out.len(), 2); // 两个探测行 region 都 = 1
        assert_eq!(out[0][0], ScalarValue::Int64(1));
    }

    #[test]
    fn null_keys_do_not_match() {
        let probe = Box::new(ValuesExec::new(
            probe_schema(),
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Null],
                vec![ScalarValue::Int64(2), ScalarValue::Int64(10)],
            ],
        ));
        let build = Box::new(ValuesExec::new(
            build_schema(),
            vec![vec![ScalarValue::Null, ScalarValue::Int64(9)]],
        ));
        let mut exec = HashJoinExec::new(
            probe,
            build,
            probe_schema(),
            build_schema(),
            vec![Expr::Column(1)],
            vec![Expr::Column(0)],
            None,
            None,
            JoinType::Inner,
        );
        let root = MemoryTracker::new_root("t", None);
        let out = drain(&mut exec, &root);
        assert_eq!(out.len(), 0, "NULL 键不参与连接(内连接)");
    }

    #[test]
    fn join_materialization_respects_memory_limit() {
        // build 侧 200 行物化进哈希表,2KB 限额 → 记账超限报错(不真耗内存)
        let rows: Vec<Row> = (0..200u64)
            .map(|i| vec![ScalarValue::Int64(i as i64), ScalarValue::Int64(1)])
            .collect();
        let build = Box::new(ValuesExec::new(build_schema(), rows));
        let probe = Box::new(ValuesExec::new(
            probe_schema(),
            vec![vec![ScalarValue::Int64(1), ScalarValue::Int64(1)]],
        ));
        let mut exec = HashJoinExec::new(
            probe,
            build,
            probe_schema(),
            build_schema(),
            vec![Expr::Column(1)],
            vec![Expr::Column(0)],
            None,
            None,
            JoinType::Inner,
        );
        let root = MemoryTracker::new_root("tiny", Some(2048));
        let err = drain_result(&mut exec, &root);
        assert!(err.is_err(), "超限应报错");
        let msg = format!("{}", err.unwrap_err());
        assert!(msg.contains("memory") || msg.contains("limit"), "{msg}");
    }

    /// drain 的 Result 版本(不 unwrap,供限额断言用)。
    fn drain_result(exec: &mut dyn Executor, root: &Arc<MemoryTracker>) -> Result<()> {
        let mut ctx = ExecuteContext::new(64, MemoryContext::new(root, "q", 4096));
        while let Some(chunk) = exec.next_batch(&mut ctx)? {
            let _ = chunk.num_rows();
        }
        Ok(())
    }
}
