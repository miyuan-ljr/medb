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
use crate::vector::Vector;
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
    /// 构建侧哈希表:键 → 构建行**引用** (批号, 行号) —— 行数据零拷贝,
    /// 构建批以 DataChunk 原样保留(记账随 chunk 自带 Reservation 存续)
    table: Option<HashMap<Vec<ScalarValue>, Vec<(u32, u32)>>>,
    /// 类型化哈希表(Int64 键快速通道)
    int_table: Option<HashMap<Vec<i64>, Vec<(u32, u32)>>>,
    /// 保留的构建批(引用指向这里;列式原样,不物化行)
    build_data: Vec<DataChunk>,
    build_null_row: Row,
    probe_done: bool,
    out_chunks: std::collections::VecDeque<DataChunk>,
    serve_off: usize,
    /// 物化内存记账(build 哈希表 + 输出缓冲),超限 → MemoryLimitExceeded
    mem: Option<Reservation>,
    mem_bytes: usize,
    /// **类型化哈希键快速通道**:全部键为单列 Int64(Expr::Column)时,
    /// 用 HashMap<i64/Vec<i64>> 直取类型化数组,免去 Vec<ScalarValue> 装箱
    int_probe_cols: Vec<usize>,
    int_build_cols: Vec<usize>,
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
        let probe_schema_ref = probe_schema.clone();
        let build_schema_ref = build_schema.clone();
        let mut schema = probe_schema;
        schema.extend(build_schema);
        let combined_dtypes = schema.iter().map(|c| c.ty.clone()).collect();
        // 类型化键探测:键必须都是单列引用且两侧类型均为 Int64(列下标与
        // schema 对齐);否则走通用 Vec<ScalarValue> 路径
        let key_cols = |keys: &[Expr], sc: &[ColumnDesc]| -> Option<Vec<usize>> {
            let mut cols = Vec::with_capacity(keys.len());
            for k in keys {
                let Expr::Column(i) = k else {
                    return None;
                };
                if sc.get(*i)?.ty != common::types::DataType::Int64 {
                    return None;
                }
                cols.push(*i);
            }
            Some(cols)
        };
        let int_probe_cols = key_cols(&probe_keys, &probe_schema_ref);
        let int_build_cols = key_cols(&build_keys, &build_schema_ref);
        let fast = match (&int_probe_cols, &int_build_cols) {
            (Some(p), Some(b)) => Some((p.clone(), b.clone())),
            _ => None,
        };
        let (int_probe_cols, int_build_cols) = fast.unwrap_or((Vec::new(), Vec::new()));
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
            int_table: None,
            build_data: Vec::new(),
            build_null_row: Vec::new(),
            probe_done: false,
            out_chunks: Default::default(),
            serve_off: 0,
            mem: None,
            mem_bytes: 0,
            int_probe_cols,
            int_build_cols,
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

    /// 构建侧入哈希表:**行数据零拷贝** —— 批以 DataChunk 原样保留,
    /// 表里只存 (批号, 行号) 引用;每批记一次哈希结构开销(引用 + 键)。
    /// NULL 键不入表(不与任何行连接)。
    /// Int64 单列键 → 类型化快速通道(直取列数组,免标量装箱)。
    fn build_table(&mut self, ctx: &mut ExecuteContext) -> Result<()> {
        let mut table: HashMap<Vec<ScalarValue>, Vec<(u32, u32)>> = HashMap::new();
        let mut int_table: HashMap<Vec<i64>, Vec<(u32, u32)>> = HashMap::new();
        let bcols = self.int_build_cols.clone();
        self.build_data.clear();
        loop {
            let Some(batch) = self.build.next_batch(ctx)? else {
                break;
            };
            let n = batch.num_rows();
            let ci = self.build_data.len() as u32;
            // 哈希结构开销批级记账;批数据本身随 chunk 保留自带记账
            let key_bytes = if bcols.is_empty() { 16 * self.build_keys.len() } else { 8 * bcols.len() };
            self.track_mem(ctx, n * (16 + key_bytes))?;
            self.build_data.push(batch);
            let batch = self.build_data.last().expect("just pushed");
            if bcols.is_empty() {
                // 通用路径
                let key_cols = self
                    .build_keys
                    .iter()
                    .map(|e| e.eval(batch))
                    .collect::<Result<Vec<_>>>()?;
                for i in 0..n {
                    let key: Vec<ScalarValue> = key_cols.iter().map(|c| c.scalar_at(i)).collect();
                    if key.iter().any(|v| v.is_null()) {
                        continue;
                    }
                    table.entry(key).or_default().push((ci, i as u32));
                }
                continue;
            };
            // 类型化路径
            let n = batch.num_rows();
            let null_bits: Vec<Vec<bool>> = bcols
                .iter()
                .map(|c| {
                    let col = batch.column(*c);
                    (0..n).map(|i| col.null_at(i)).collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            for i in 0..n {
                let mut key = Vec::with_capacity(bcols.len());
                let mut has_null = false;
                for (c, nulls) in bcols.iter().zip(&null_bits) {
                    if nulls[i] {
                        has_null = true;
                        break;
                    }
                    key.push(batch.column(*c).int64_values()[i]);
                }
                if has_null {
                    continue;
                }
                int_table.entry(key).or_default().push((ci, i as u32));
            }
        }
        // 左外连接的 NULL 填充行模板
        self.build_null_row = vec![ScalarValue::Null; self.build_dtypes.len()];
        self.table = Some(table);
        self.int_table = Some(int_table);
        Ok(())
    }

    /// 拉取并消化探测侧,**列式产出**:探测批逐行查表得 (批号, 行号) 引用,
    /// 按列 gather 组装输出块(探测列直采;构建列按连续同批引用分段 gather
    /// 后 concat)—— 全程不物化 Row。match_cond / residual 在列块上向量化
    /// 求值;LEFT 未匹配(候选为 0 或全被 match_cond 过滤)探测行以 NULL 段
    /// 追加(段内探测行序保持;跨段序 = 匹配行在前,SQL 语义无序保证)。
    fn materialize(&mut self, ctx: &mut ExecuteContext) -> Result<()> {
        self.build_table(ctx)?;
        // take 出来:后面 track_mem 需要 &mut self;materialize 后哈希表不再需要
        let int_fast = !self.int_probe_cols.is_empty();
        let probe_cols_idx = self.int_probe_cols.clone();
        let int_table = if int_fast {
            std::mem::take(&mut self.int_table).expect("int build_table called first")
        } else {
            HashMap::new()
        };
        let table = if int_fast {
            HashMap::new()
        } else {
            std::mem::take(&mut self.table).expect("build_table called first")
        };
        let build_data = std::mem::take(&mut self.build_data);
        let probe_width = self.schema.len() - self.build_dtypes.len();
        let left = self.join_type == JoinType::LeftOuter;

        loop {
            let Some(batch) = self.probe.next_batch(ctx)? else {
                break;
            };
            let n_rows = batch.num_rows();
            let key_cols = self
                .probe_keys
                .iter()
                .map(|e| e.eval(&batch))
                .collect::<Result<Vec<_>>>()?;
            let int_lookup = |i: usize| -> Option<Vec<i64>> {
                let mut key = Vec::with_capacity(probe_cols_idx.len());
                for c in &probe_cols_idx {
                    let col = batch.column(*c);
                    if col.null_at(i) {
                        return None;
                    }
                    key.push(col.int64_values()[i]);
                }
                Some(key)
            };
            let generic_lookup = |i: usize| -> Option<Vec<ScalarValue>> {
                let key: Vec<ScalarValue> = key_cols.iter().map(|c| c.scalar_at(i)).collect();
                if key.iter().any(|v| v.is_null()) {
                    return None;
                }
                Some(key)
            };
            // 1. 查表:输出行索引(探测行下标 + 构建引用),零行拷贝
            let mut probe_idx: Vec<usize> = Vec::new();
            let mut build_refs: Vec<(u32, u32)> = Vec::new();
            let mut cand_count = vec![0u32; n_rows]; // 匹配前候选数(match_cond 存活判定基线)
            for i in 0..n_rows {
                let matches = if int_fast {
                    int_lookup(i).and_then(|k| int_table.get(&k).cloned())
                } else {
                    generic_lookup(i).and_then(|k| table.get(&k).cloned())
                };
                let Some(m) = matches.filter(|m| !m.is_empty()) else {
                    continue;
                };
                cand_count[i] = m.len() as u32;
                for r in m {
                    probe_idx.push(i);
                    build_refs.push(r);
                }
            }
            if probe_idx.is_empty() && !left {
                continue;
            }
            // 2. 列式组装:探测列直采;构建列按连续同批段 gather 后 concat
            let mut cols: Vec<Vector> = Vec::with_capacity(self.schema.len());
            for c in 0..probe_width {
                cols.push(batch.column(c).gather(&probe_idx));
            }
            let runs = build_ref_runs(&build_refs);
            for c in 0..self.build_dtypes.len() {
                cols.push(gather_build_column(&build_data, &runs, c));
            }
            let mut chunk = DataChunk::with_memory(cols, &ctx.mem.tracker)?;
            // 3. match_cond:列块上向量化求值 + 按探测行统计存活数
            let mut survivors = cand_count;
            if let Some(cond) = &self.match_cond {
                let mask = cond.eval(&chunk)?;
                let sel: Vec<bool> = (0..chunk.num_rows())
                    .map(|j| !mask.null_at(j) && mask.bool_values()[j])
                    .collect();
                for (j, keep) in sel.iter().enumerate() {
                    if !*keep {
                        survivors[probe_idx[j]] -= 1;
                    }
                }
                chunk = chunk.select(&sel, &ctx.mem.tracker)?;
            }
            // 4. LEFT:候选 0 或全被过滤的探测行 → NULL 补齐段(保持探测行序)
            if left {
                let null_rows: Vec<usize> =
                    (0..n_rows).filter(|&i| survivors[i] == 0).collect();
                if !null_rows.is_empty() {
                    let mut null_cols: Vec<Vector> = Vec::with_capacity(self.schema.len());
                    for c in 0..probe_width {
                        null_cols.push(batch.column(c).gather(&null_rows));
                    }
                    for ty in &self.build_dtypes {
                        null_cols.push(Vector::from_scalars(
                            ty.clone(),
                            (0..null_rows.len()).map(|_| ScalarValue::Null),
                        )?);
                    }
                    let null_chunk = DataChunk::with_memory(null_cols, &ctx.mem.tracker)?;
                    chunk = concat_two_chunks(chunk, null_chunk)?;
                }
            }
            // 5. residual(WHERE 路由):列块上整批过滤
            if let Some(residual) = &self.residual {
                let mask = residual.eval(&chunk)?;
                let sel: Vec<bool> = (0..chunk.num_rows())
                    .map(|j| !mask.null_at(j) && mask.bool_values()[j])
                    .collect();
                chunk = chunk.select(&sel, &ctx.mem.tracker)?;
            }
            if chunk.num_rows() > 0 {
                self.out_chunks.push_back(chunk);
            }
        }

        self.probe_done = true;
        Ok(())
    }

    /// 列式产出按游标切片交付(与 SeqScan 列存路径同约定)。
    fn next_batch_serve(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        loop {
            let Some(front) = self.out_chunks.front() else {
                return Ok(None);
            };
            if self.serve_off >= front.num_rows() {
                self.out_chunks.pop_front();
                self.serve_off = 0;
                continue;
            }
            let n = (front.num_rows() - self.serve_off).min(ctx.batch_size);
            let cols = front
                .columns()
                .iter()
                .map(|v| v.slice(self.serve_off, n))
                .collect();
            let chunk = DataChunk::with_memory(cols, &ctx.mem.tracker)?;
            self.serve_off += n;
            return Ok(Some(chunk));
        }
    }
}

/// 构建引用按**连续同批**分段(保持引用顺序 = 输出行序)。
fn build_ref_runs(refs: &[(u32, u32)]) -> Vec<(u32, Vec<usize>)> {
    let mut runs: Vec<(u32, Vec<usize>)> = Vec::new();
    for (ci, ri) in refs {
        match runs.last_mut() {
            Some((c, idx)) if *c == *ci => idx.push(*ri as usize),
            _ => runs.push((*ci, vec![*ri as usize])),
        }
    }
    runs
}

/// 按分段取构建侧单列(段内 gather,跨段 concat;保持引用顺序)。
fn gather_build_column(
    build_data: &[DataChunk],
    runs: &[(u32, Vec<usize>)],
    c: usize,
) -> Vector {
    if runs.len() == 1 {
        let (ci, idx) = &runs[0];
        return build_data[*ci as usize].column(c).gather(idx);
    }
    let parts: Vec<Vector> = runs
        .iter()
        .map(|(ci, idx)| build_data[*ci as usize].column(c).gather(idx))
        .collect();
    Vector::concat(&parts).expect("same-type parts")
}

/// 两块列式拼接(同 schema)。
fn concat_two_chunks(a: DataChunk, b: DataChunk) -> Result<DataChunk> {
    let cols = a
        .columns()
        .iter()
        .zip(b.columns())
        .map(|(va, vb)| Vector::concat(&[va.clone(), vb.clone()]))
        .collect::<Result<Vec<_>>>()?;
    DataChunk::new(cols)
}

impl Executor for HashJoinExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        if !self.probe_done {
            self.materialize(ctx)?;
        }
        self.next_batch_serve(ctx)
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
