//! IndexScan:二级索引扫描算子(点查/范围)。
//!
//! 计划器基于统计判定索引有利时,把该列的等值/范围合取项交给数据源的
//! `scan_index`(segment 索引树 + memtable delta 合并),其余合取项作为
//! 残余谓词在物化行集上向量化过滤。行集按批切片输出(与 SeqScan 行路径
//! 同形),物化记账挂在查询配额树。

use std::sync::Arc;

use common::error::Result;
use common::schema::{ColumnDesc, Row, TableSchema};
use common::types::{DataType, ScalarValue};
use memory::Reservation;

use crate::chunk::DataChunk;
use crate::executor::{ExecuteContext, Executor};
use crate::expr::Expr;
use crate::operators::seq_scan::QueryDataSource;

pub struct IndexScanExec {
    schema: Vec<ColumnDesc>,
    dtypes: Vec<DataType>,
    source: Arc<dyn QueryDataSource>,
    table: TableSchema,
    /// 索引列(表内局部下标)
    col_idx: usize,
    /// 范围下界 (值, 是否闭)
    lo: Option<(ScalarValue, bool)>,
    /// 范围上界 (值, 是否闭)
    hi: Option<(ScalarValue, bool)>,
    /// 残余谓词(表内局部下标;索引未覆盖的条件)
    residual: Option<Expr>,
    /// 投影剪枝位图(None = 全列需要)
    needed: Option<Vec<bool>>,
    rows: Option<Vec<Row>>,
    mem: Option<Reservation>,
    next: usize,
}

impl IndexScanExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: Arc<dyn QueryDataSource>,
        table: TableSchema,
        col_idx: usize,
        lo: Option<(ScalarValue, bool)>,
        hi: Option<(ScalarValue, bool)>,
        residual: Option<Expr>,
        needed: Option<Vec<bool>>,
    ) -> Self {
        let schema = table.columns.clone();
        let dtypes = schema.iter().map(|c| c.ty.clone()).collect();
        Self {
            schema,
            dtypes,
            source,
            table,
            col_idx,
            lo,
            hi,
            residual,
            needed,
            rows: None,
            mem: None,
            next: 0,
        }
    }
}

impl Executor for IndexScanExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        if self.rows.is_none() {
            let needed_slice: &[bool] = self.needed.as_deref().unwrap_or(&[]);
            let rows = self.source.scan_index(
                &self.table,
                self.col_idx,
                self.lo.clone(),
                self.hi.clone(),
                needed_slice,
            )?;
            // 残余谓词:整批向量化求值后压缩(未命中行不进物化)
            let rows = match &self.residual {
                Some(pred) => {
                    let mut kept = Vec::with_capacity(rows.len());
                    for slice in rows.chunks(2048) {
                        let chunk = DataChunk::from_rows(&self.dtypes, slice)?;
                        let mask = pred.eval(&chunk)?;
                        let sel: Vec<bool> = (0..slice.len())
                            .map(|i| !mask.null_at(i) && mask.bool_values()[i])
                            .collect();
                        for (i, keep) in sel.iter().enumerate() {
                            if *keep {
                                kept.push(slice[i].clone());
                            }
                        }
                    }
                    kept
                }
                None => rows,
            };
            let bytes = rows
                .iter()
                .map(|r| crate::chunk::row_approx_bytes(&self.dtypes, r))
                .sum();
            let res = ctx.mem.tracker.try_reserve(bytes)?;
            self.mem = Some(res);
            self.rows = Some(rows);
        }
        let rows = self.rows.as_ref().expect("rows fetched above");
        if self.next >= rows.len() {
            self.mem = None;
            return Ok(None);
        }
        let end = (self.next + ctx.batch_size).min(rows.len());
        let chunk = DataChunk::with_rows(&self.dtypes, &rows[self.next..end], &ctx.mem.tracker)?;
        self.next = end;
        Ok(Some(chunk))
    }

    fn schema(&self) -> &[ColumnDesc] {
        &self.schema
    }
}
