//! SeqScan:扫描算子(行存 / AP 列存双路径 + 谓词下推)。
//!
//! exec 不依赖 storage:数据源以 [`QueryDataSource`] trait 注入(由 server
//! 的 Engine 实现)。惰性选择读取路径:
//! ① **AP 列存**:数据源按列返回(标量表 + 无事务门控),谓词直接在列块上
//!    求值压缩 —— 免逐行解码;
//! ② **行存 + 下推**:`scan_table_filtered` 解码期过滤,未命中行不进物化
//!    (默认实现 = 整表拉取后分批过滤,引擎覆盖为并行解码过滤);
//! ③ 行存全表。
//! 之后按 batch_size 切批、逐批在查询配额树记账。

use std::sync::Arc;

use common::error::Result;
use common::schema::{ColumnDesc, Row, TableSchema};
use common::types::DataType;
use memory::Reservation;

use crate::chunk::DataChunk;
use crate::executor::{ExecuteContext, Executor};
use crate::expr::Expr;

/// 表数据源抽象(存储引擎实现)。
pub trait QueryDataSource: Send + Sync {
    /// 按 schema 拉取整表行(行内值类型应与 schema 一致)。
    fn scan_table(&self, table: &TableSchema) -> Result<Vec<Row>>;

    /// 计划期表统计(代价优化器输入)。缺失/过期时实现方可以自动 ANALYZE;
    /// 返回 None 表示无统计(计划器用默认基数估算)。
    fn table_stats(&self, table: &TableSchema) -> Option<Arc<common::stats::TableStats>> {
        let _ = table;
        None
    }

    /// **谓词下推**:拉取满足 `pred`(列下标为表内局部下标)的行。
    /// 默认实现 = 整表拉取 + 分批过滤;存储引擎应覆盖为"解码期过滤",
    /// 使未命中行不进入物化。
    fn scan_table_filtered(
        &self,
        table: &TableSchema,
        pred: &crate::expr::Expr,
    ) -> Result<Vec<Row>> {
        let rows = self.scan_table(table)?;
        let dtypes: Vec<common::types::DataType> =
            table.columns.iter().map(|c| c.ty.clone()).collect();
        let mut out = Vec::with_capacity(rows.len() / 4);
        for slice in rows.chunks(2048) {
            let chunk = crate::chunk::DataChunk::from_rows(&dtypes, slice)?;
            let mask = pred.eval(&chunk)?;
            for i in 0..slice.len() {
                if !mask.null_at(i) && mask.bool_values()[i] {
                    out.push(slice[i].clone());
                }
            }
        }
        Ok(out)
    }

    /// **AP 列存路径**:按列返回整表列向量(列序 = schema 列序;无 null 位图
    /// 时全非空)。返回 None = 该表不可列式读取(类型不支持 / 一致性门控 /
    /// 副本缺失),调用方回退行存路径。
    fn scan_columns(&self, table: &TableSchema) -> Result<Option<Vec<crate::vector::Vector>>> {
        let _ = table;
        Ok(None)
    }
}

pub struct SeqScanExec {
    schema: Vec<ColumnDesc>,
    dtypes: Vec<DataType>,
    source: Arc<dyn QueryDataSource>,
    table: TableSchema,
    /// 下推谓词(表内局部下标;解码期过滤,未命中行不进物化)
    pushdown: Option<Expr>,
    /// 行存路径:物化行集(下推过滤后)
    rows: Option<Vec<Row>>,
    /// 列存路径:整表列块(AP 路径;列向量由副本所有,块为查询内副本)
    column_chunk: Option<Option<DataChunk>>,
    /// 物化行集的内存记账(超查询/全局限额 → MemoryLimitExceeded)
    mem: Option<Reservation>,
    next: usize,
}

impl SeqScanExec {
    pub fn new(source: Arc<dyn QueryDataSource>, table: TableSchema) -> Self {
        Self::with_pushdown(source, table, None)
    }

    /// 带下推谓词构造(谓词列下标为表内局部下标)。
    pub fn with_pushdown(
        source: Arc<dyn QueryDataSource>,
        table: TableSchema,
        pushdown: Option<Expr>,
    ) -> Self {
        let schema = table.columns.clone();
        let dtypes = schema.iter().map(|c| c.ty.clone()).collect();
        Self {
            schema,
            dtypes,
            source,
            table,
            pushdown,
            rows: None,
            column_chunk: None,
            mem: None,
            next: 0,
        }
    }
}

impl Executor for SeqScanExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        // 三条路径,惰性选择:
        // ① AP 列存:数据源可按列返回(标量表 + 一致性门控),谓词在列块上求值;
        // ② 行存 + 下推:谓词随解码过滤(存储引擎覆盖实现;默认整表拉取后过滤);
        // ③ 行存全表。
        if self.column_chunk.is_none() && self.rows.is_none() {
            let columnar = self.source.scan_columns(&self.table)?;
            if let Some(mut columns) = columnar {
                // 列存路径:整表一块(列向量由副本持有,此为查询内拷贝)
                if let Some(pred) = &self.pushdown {
                    let mut chunk = DataChunk::new(columns)?;
                    let mask = pred.eval(&chunk)?;
                    let sel: Vec<bool> = (0..chunk.num_rows())
                        .map(|i| !mask.null_at(i) && mask.bool_values()[i])
                        .collect();
                    chunk = chunk.select(&sel, &ctx.mem.tracker)?;
                    columns = chunk.into_columns();
                }
                self.column_chunk = Some(Some(DataChunk::new(columns)?));
            } else {
                let rows = match &self.pushdown {
                    Some(pred) => self.source.scan_table_filtered(&self.table, pred)?,
                    None => self.source.scan_table(&self.table)?,
                };
                // 物化行集计入配额树(与 DataChunk 记账同一棵树)
                let bytes = rows
                    .iter()
                    .map(|r| crate::chunk::row_approx_bytes(&self.dtypes, r))
                    .sum();
                let res = ctx.mem.tracker.try_reserve(bytes)?;
                self.mem = Some(res);
                self.rows = Some(rows);
            }
        }
        // 列存路径:块耗尽(slot=None)→ 结束
        if self.column_chunk.is_some() {
            return self.next_columnar_batch(ctx);
        }
        let rows = self.rows.as_ref().expect("rows fetched above");
        if self.next >= rows.len() {
            // 扫完即释放物化记账(行集仍持有,但不再计入活跃内存)
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

impl SeqScanExec {
    /// 列存路径的批切分(整块 > batch_size 时用 head 逐步取出)。
    fn next_columnar_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        let slot = self.column_chunk.as_mut().expect("columnar prepared");
        let Some(chunk) = slot.take() else {
            return Ok(None);
        };
        if chunk.num_rows() <= ctx.batch_size {
            return Ok(Some(chunk));
        }
        let head = chunk.head(ctx.batch_size, &ctx.mem.tracker)?;
        // 余下部分放回(下次继续切)
        let rest_sel: Vec<bool> = (0..chunk.num_rows()).map(|i| i >= ctx.batch_size).collect();
        let rest = chunk.select(&rest_sel, &ctx.mem.tracker)?;
        *slot = Some(rest);
        Ok(Some(head))
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
