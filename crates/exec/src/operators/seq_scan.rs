//! SeqScan:全表扫描算子。
//!
//! exec 不依赖 storage:数据源以 [`QueryDataSource`] trait 注入(由 server
//! 的 Engine 实现:LSM 范围扫描 + 行解码)。首批拉取时一次性物化全部行
//! (与扫描路径的"批内物化"简化一致;流式分页/谓词下推见路线图),
//! 之后按 batch_size 切批、逐批在查询配额树记账。

use std::sync::Arc;

use common::error::Result;
use common::schema::{ColumnDesc, Row, TableSchema};
use common::types::DataType;

use crate::chunk::DataChunk;
use crate::executor::{ExecuteContext, Executor};

/// 表数据源抽象(存储引擎实现)。
pub trait QueryDataSource: Send + Sync {
    /// 按 schema 拉取整表行(行内值类型应与 schema 一致)。
    fn scan_table(&self, table: &TableSchema) -> Result<Vec<Row>>;
}

pub struct SeqScanExec {
    schema: Vec<ColumnDesc>,
    dtypes: Vec<DataType>,
    source: Arc<dyn QueryDataSource>,
    table: TableSchema,
    rows: Option<Vec<Row>>,
    next: usize,
}

impl SeqScanExec {
    pub fn new(source: Arc<dyn QueryDataSource>, table: TableSchema) -> Self {
        let schema = table.columns.clone();
        let dtypes = schema.iter().map(|c| c.ty.clone()).collect();
        Self {
            schema,
            dtypes,
            source,
            table,
            rows: None,
            next: 0,
        }
    }
}

impl Executor for SeqScanExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        if self.rows.is_none() {
            self.rows = Some(self.source.scan_table(&self.table)?);
        }
        let rows = self.rows.as_ref().expect("rows fetched above");
        if self.next >= rows.len() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use common::types::ScalarValue;
    use memory::{MemoryContext, MemoryTracker};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeSource {
        rows: Vec<Row>,
        fetches: AtomicUsize,
    }

    impl QueryDataSource for FakeSource {
        fn scan_table(&self, _table: &TableSchema) -> Result<Vec<Row>> {
            self.fetches.fetch_add(1, Ordering::Relaxed);
            Ok(self.rows.clone())
        }
    }

    fn schema() -> TableSchema {
        TableSchema {
            id: 1,
            name: "t".into(),
            columns: vec![
                ColumnDesc::new("a", DataType::Int64, true),
                ColumnDesc::new("b", DataType::Utf8, true),
            ],
            primary_key: Vec::new(),
            version: 1,
        }
    }

    fn exec_ctx(root: &Arc<MemoryTracker>) -> ExecuteContext {
        ExecuteContext::new(2, MemoryContext::new(root, "q", 4096))
    }

    #[test]
    fn batches_and_lazy_fetch() {
        let rows: Vec<Row> = (0..5)
            .map(|i| vec![ScalarValue::Int64(i), ScalarValue::Utf8(format!("r{i}"))])
            .collect();
        let source = Arc::new(FakeSource {
            rows,
            fetches: AtomicUsize::new(0),
        });
        let mut exec = SeqScanExec::new(source.clone(), schema());
        assert_eq!(source.fetches.load(Ordering::Relaxed), 0, "计划期不应拉取");

        let root = MemoryTracker::new_root("t", None);
        let mut ctx = exec_ctx(&root);
        let mut out = Vec::new();
        while let Some(chunk) = exec.next_batch(&mut ctx).unwrap() {
            for i in 0..chunk.num_rows() {
                out.push(chunk.row(i)[0].clone());
            }
        }
        assert_eq!(source.fetches.load(Ordering::Relaxed), 1, "只拉取一次");
        assert_eq!(
            out,
            vec![
                ScalarValue::Int64(0),
                ScalarValue::Int64(1),
                ScalarValue::Int64(2),
                ScalarValue::Int64(3),
                ScalarValue::Int64(4)
            ]
        );
        // 再次 next_batch(结束态)不再拉取
        assert!(exec.next_batch(&mut ctx).unwrap().is_none());
        assert_eq!(source.fetches.load(Ordering::Relaxed), 1);
        assert_eq!(root.used(), 0);
    }

    #[test]
    fn empty_table_yields_schema_but_no_rows() {
        let source = Arc::new(FakeSource {
            rows: Vec::new(),
            fetches: AtomicUsize::new(0),
        });
        let mut exec = SeqScanExec::new(source, schema());
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = exec_ctx(&root);
        assert_eq!(exec.schema().len(), 2);
        assert!(exec.next_batch(&mut ctx).unwrap().is_none());
    }
}
