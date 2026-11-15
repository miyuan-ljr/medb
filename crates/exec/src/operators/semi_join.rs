//! Hash Semi/Anti Join:相关 EXISTS / NOT EXISTS 去相关的执行形态。
//!
//! 构建侧(子查询流水线)整批入哈希表,探测侧(外层流水线)逐行查表;
//! 输出**只保留探测列**:semi 保留命中 ≥1 条构建行的探测行,anti 保留
//! 零命中的探测行(探测键含 NULL 视为零命中,与 SQL 相关谓词三值逻辑一致)。
//! cond 在"探测列 ++ 构建列"拼接块上向量化求值(非等值相关谓词,
//! 如 Q21 的 l3.l_suppkey <> l1.l_suppkey),未存活的候选不计入命中。

use common::error::Result;
use common::schema::ColumnDesc;
use common::types::{DataType, ScalarValue};
use std::collections::HashMap;

use crate::chunk::DataChunk;
use crate::executor::{BoxedExecutor, ExecuteContext, Executor};
use crate::vector::Vector;
use crate::expr::Expr;
use crate::operators::hash_join::{build_ref_runs, gather_build_column};
use memory::Reservation;

pub struct HashSemiJoinExec {
    probe: BoxedExecutor,
    build: BoxedExecutor,
    probe_keys: Vec<Expr>,
    build_keys: Vec<Expr>,
    /// 匹配期条件(拼接块上求值;决定候选是否算命中)
    cond: Option<Expr>,
    /// true = 反半连接(NOT EXISTS)
    anti: bool,
    schema: Vec<ColumnDesc>,
    build_dtypes: Vec<DataType>,
    table: Option<HashMap<Vec<ScalarValue>, Vec<(u32, u32)>>>,
    int_table: Option<HashMap<Vec<i64>, Vec<(u32, u32)>>>,
    build_data: Vec<DataChunk>,
    probe_done: bool,
    out_chunks: std::collections::VecDeque<DataChunk>,
    serve_off: usize,
    mem: Option<Reservation>,
    mem_bytes: usize,
    int_probe_cols: Vec<usize>,
    int_build_cols: Vec<usize>,
}

impl HashSemiJoinExec {
    pub fn new(
        probe: BoxedExecutor,
        build: BoxedExecutor,
        probe_schema: Vec<ColumnDesc>,
        build_schema: Vec<ColumnDesc>,
        probe_keys: Vec<Expr>,
        build_keys: Vec<Expr>,
        cond: Option<Expr>,
        anti: bool,
    ) -> Self {
        let build_dtypes = build_schema.iter().map(|c| c.ty.clone()).collect();
        let probe_schema_ref = probe_schema.clone();
        let build_schema_ref = build_schema.clone();
        let key_cols = |keys: &[Expr], sc: &[ColumnDesc]| -> Option<Vec<usize>> {
            let mut cols = Vec::with_capacity(keys.len());
            for k in keys {
                let Expr::Column(i) = k else {
                    return None;
                };
                if sc.get(*i)?.ty != DataType::Int64 {
                    return None;
                }
                cols.push(*i);
            }
            Some(cols)
        };
        let (int_probe_cols, int_build_cols) =
            match (key_cols(&probe_keys, &probe_schema_ref), key_cols(&build_keys, &build_schema_ref)) {
                (Some(p), Some(b)) => (p, b),
                _ => (Vec::new(), Vec::new()),
            };
        Self {
            probe,
            build,
            probe_keys,
            build_keys,
            cond,
            anti,
            schema: probe_schema,
            build_dtypes,
            table: None,
            int_table: None,
            build_data: Vec::new(),
            probe_done: false,
            out_chunks: Default::default(),
            serve_off: 0,
            mem: None,
            mem_bytes: 0,
            int_probe_cols,
            int_build_cols,
        }
    }

    fn track_mem(&mut self, ctx: &mut ExecuteContext, add: usize) -> Result<()> {
        let new_total = self.mem_bytes + add;
        match &mut self.mem {
            Some(r) => r.resize(new_total)?,
            None => self.mem = Some(ctx.mem.tracker.try_reserve(new_total)?),
        }
        self.mem_bytes = new_total;
        Ok(())
    }

    /// 构建侧入哈希表(与 HashJoinExec 同约定:行数据零拷贝,批原样保留)。
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
            let key_bytes =
                if bcols.is_empty() { 16 * self.build_keys.len() } else { 8 * bcols.len() };
            self.track_mem(ctx, n * (16 + key_bytes))?;
            self.build_data.push(batch);
            let batch = self.build_data.last().expect("just pushed");
            if bcols.is_empty() {
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
            }
            // 类型化路径:键列引用提到行循环外;null 位图按需查询(零拷贝)
            let key_cols: Vec<&Vector> = bcols.iter().map(|&c| batch.column(c)).collect();
            for i in 0..n {
                let mut key = Vec::with_capacity(key_cols.len());
                let mut has_null = false;
                for col in &key_cols {
                    if col.null_at(i) {
                        has_null = true;
                        break;
                    }
                    key.push(col.int64_values()[i]);
                }
                if has_null {
                    continue;
                }
                int_table.entry(key).or_default().push((ci, i as u32));
            }
        }
        self.table = Some(table);
        self.int_table = Some(int_table);
        Ok(())
    }

    fn materialize(&mut self, ctx: &mut ExecuteContext) -> Result<()> {
        self.build_table(ctx)?;
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
        let has_cond = self.cond.is_some();

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
            // 每探测行:候选构建行引用 + 命中数
            let mut probe_idx: Vec<usize> = Vec::new();
            let mut build_refs: Vec<(u32, u32)> = Vec::new();
            let mut hits = vec![0u32; n_rows];
            let mut cand_rows = 0usize;
            for i in 0..n_rows {
                let matches = if int_fast {
                    int_lookup(i).and_then(|k| int_table.get(&k).cloned())
                } else {
                    generic_lookup(i).and_then(|k| table.get(&k).cloned())
                };
                let Some(m) = matches.filter(|m| !m.is_empty()) else {
                    continue;
                };
                cand_rows += m.len();
                for r in m {
                    probe_idx.push(i);
                    build_refs.push(r);
                }
            }
            if cand_rows > 0 {
                if let Some(cond) = &self.cond {
                    // 拼接块(探测列 ++ 构建列)上向量化求值,统计每探测行存活数
                    let mut cols: Vec<crate::vector::Vector> = Vec::new();
                    for c in 0..batch.columns().len() {
                        cols.push(batch.column(c).gather(&probe_idx));
                    }
                    let runs = build_ref_runs(&build_refs);
                    for c in 0..self.build_dtypes.len() {
                        cols.push(gather_build_column(&build_data, &runs, c));
                    }
                    let chunk = DataChunk::with_memory(cols, &ctx.mem.tracker)?;
                    let mask = cond.eval(&chunk)?;
                    for (j, pi) in probe_idx.iter().enumerate() {
                        if !mask.null_at(j) && mask.bool_values()[j] {
                            hits[*pi] += 1;
                        }
                    }
                } else {
                    for pi in &probe_idx {
                        hits[*pi] += 1;
                    }
                }
            }
            // semi 保留命中 ≥1 的探测行;anti 保留零命中(含 NULL 键)的探测行
            let keep: Vec<usize> = (0..n_rows)
                .filter(|&i| if self.anti { hits[i] == 0 } else { hits[i] > 0 })
                .collect();
            if keep.len() == n_rows && !has_cond {
                // 全保留:探测批原样交付,零拷贝
                self.out_chunks.push_back(batch);
                continue;
            }
            if keep.is_empty() {
                continue;
            }
            let cols = batch.columns().iter().map(|v| v.gather(&keep)).collect();
            let chunk = DataChunk::with_memory(cols, &ctx.mem.tracker)?;
            self.out_chunks.push_back(chunk);
        }
        self.probe_done = true;
        Ok(())
    }

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

impl Executor for HashSemiJoinExec {
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
