//! SeqScan:扫描算子(行存 / AP 列存双路径 + 谓词下推)。
//!
//! exec 不依赖 storage:数据源以 [`QueryDataSource`] trait 注入(由 server
//! 的 Engine 实现)。惰性选择读取路径:
//! ① **AP 列存**:数据源按列返回若干整块(segment + memtable delta,拼接
//!    语义),谓词逐块向量化求值压缩 —— 免逐行解码;
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

    /// **谓词下推 + 投影剪枝**:拉取满足 `pred`(可选;列下标为表内局部
    /// 下标)且只物化 `needed` 标记列的行(未标记列 → Null 占位)。
    /// 默认实现 = 整表拉取 + 分批过滤;存储引擎应覆盖为"解码期过滤 +
    /// 部分物化",使未命中/未引用的列不产生分配。
    fn scan_table_filtered(
        &self,
        table: &TableSchema,
        pred: Option<&crate::expr::Expr>,
        needed: &[bool],
    ) -> Result<Vec<Row>> {
        let rows = self.scan_table(table)?;
        let dtypes: Vec<common::types::DataType> =
            table.columns.iter().map(|c| c.ty.clone()).collect();
        let mut out = Vec::with_capacity(rows.len() / 4);
        for slice in rows.chunks(2048) {
            let chunk = crate::chunk::DataChunk::from_rows(&dtypes, slice)?;
            let selection: Vec<bool> = match pred {
                Some(p) => {
                    let mask = p.eval(&chunk)?;
                    (0..slice.len())
                        .map(|i| !mask.null_at(i) && mask.bool_values()[i])
                        .collect()
                }
                None => vec![true; slice.len()],
            };
            for i in 0..slice.len() {
                if selection[i] {
                    let mut row = slice[i].clone();
                    for (c, need) in needed.iter().enumerate() {
                        if !need {
                            row[c] = common::types::ScalarValue::Null;
                        }
                    }
                    out.push(row);
                }
            }
        }
        Ok(out)
    }

    /// **AP 列存路径**:按列返回若干整块列向量(列序 = schema 列序)。
    /// 多块语义 = **拼接结果**(如 segment 块 + memtable delta 块);实现方
    /// 保证跨块无重复行。每块独立走向量化谓词求值。返回 None = 该表不可
    /// 列式读取(类型不支持 / 一致性门控 / 段缺失),调用方回退行存路径。
    fn scan_columns(
        &self,
        table: &TableSchema,
    ) -> Result<Option<Vec<Vec<crate::vector::Vector>>>> {
        let _ = table;
        Ok(None)
    }

    /// 数据源当前是否支持 [`Self::scan_index`](索引访问路径)。计划器据此
    /// 决定是否生成 IndexScan(不支持时走普通扫描 + 谓词下推)。
    fn supports_index_scan(&self) -> bool {
        false
    }

    /// **二级索引访问**:返回索引列落在 [lo, hi](值, 是否闭)内的行,
    /// 含 memtable 增量(更新取新版本、删除剔除),并与 segment 行去重。
    /// `needed` 与 scan_table_filtered 同约定(空 = 全列;未标记列 Null 占位)。
    /// 默认不支持(计划器先经 [`Self::supports_index_scan`] 探测)。
    fn scan_index(
        &self,
        table: &TableSchema,
        col_idx: usize,
        lo: Option<(common::types::ScalarValue, bool)>,
        hi: Option<(common::types::ScalarValue, bool)>,
        needed: &[bool],
    ) -> Result<Vec<Row>> {
        let _ = (table, col_idx, lo, hi, needed);
        Err(common::error::Error::not_supported(
            "index scan not supported by this data source",
        ))
    }
}

pub struct SeqScanExec {
    schema: Vec<ColumnDesc>,
    dtypes: Vec<DataType>,
    source: Arc<dyn QueryDataSource>,
    table: TableSchema,
    /// 下推谓词(表内局部下标;解码期过滤,未命中行不进物化)
    pushdown: Option<Expr>,
    /// 投影剪枝位图(表内局部下标;None = 全列需要)
    needed: Option<Vec<bool>>,
    /// 行存路径:物化行集(下推过滤后)
    rows: Option<Vec<Row>>,
    /// 列存路径:整块列块队列(AP 路径;segment + memtable delta 各一块,
    /// 谓词逐块向量化求值),空队列即扫完
    column_chunks: std::collections::VecDeque<DataChunk>,
    /// 列存路径已选定(即使队列为空也不回退行存)
    columnar_started: bool,
    /// 队首块的已交付游标(批切片只拷当批,front 本体不动)
    front_off: usize,
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
        Self::with_projection(source, table, pushdown, None)
    }

    /// 带下推谓词 + 投影剪枝构造(均针对表内局部下标)。
    pub fn with_projection(
        source: Arc<dyn QueryDataSource>,
        table: TableSchema,
        pushdown: Option<Expr>,
        needed: Option<Vec<bool>>,
    ) -> Self {
        let schema = table.columns.clone();
        let dtypes = schema.iter().map(|c| c.ty.clone()).collect();
        Self {
            schema,
            dtypes,
            source,
            table,
            pushdown,
            needed,
            rows: None,
            column_chunks: Default::default(),
            columnar_started: false,
            front_off: 0,
            mem: None,
            next: 0,
        }
    }
}

impl Executor for SeqScanExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        // 三条路径,惰性选择:
        // ① AP 列存:数据源按列返回若干整块(segment + memtable delta 拼接),
        //    谓词逐块求值压缩 —— 免逐行解码;
        // ② 行存 + 下推:谓词随解码过滤(存储引擎覆盖实现;默认整表拉取后过滤);
        // ③ 行存全表。
        if self.rows.is_none() && !self.columnar_started {
            match self.source.scan_columns(&self.table)? {
                Some(chunks) => {
                    self.columnar_started = true;
                    for columns in chunks {
                        let mut chunk = DataChunk::new(columns)?;
                        if let Some(pred) = &self.pushdown {
                            let mask = pred.eval(&chunk)?;
                            let sel: Vec<bool> = (0..chunk.num_rows())
                                .map(|i| !mask.null_at(i) && mask.bool_values()[i])
                                .collect();
                            chunk = chunk.select(&sel, &ctx.mem.tracker)?;
                        }
                        if chunk.num_rows() > 0 {
                            self.column_chunks.push_back(chunk);
                        }
                    }
                }
                None => {
                    let rows = match (&self.pushdown, &self.needed) {
                        (Some(pred), Some(needed)) => {
                            self.source
                                .scan_table_filtered(&self.table, Some(pred), needed)?
                        }
                        (None, Some(needed)) => {
                            self.source.scan_table_filtered(&self.table, None, needed)?
                        }
                        (Some(pred), None) => {
                            let all = vec![true; self.table.columns.len()];
                            self.source
                                .scan_table_filtered(&self.table, Some(pred), &all)?
                        }
                        (None, None) => self.source.scan_table(&self.table)?,
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
        }
        // 列存路径:队列耗尽 → 结束
        if self.columnar_started {
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
    /// 列存路径的批切分:游标 + [`Vector::slice`]。
    /// front 本体保持不变,每批只拷贝当批交付数据(必要拷贝),
    /// 剩余行零拷贝复用 —— 消除 O(N²/batch) 的整块重压。
    fn next_columnar_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        loop {
            let Some(front) = self.column_chunks.front() else {
                return Ok(None);
            };
            if self.front_off >= front.num_rows() {
                self.column_chunks.pop_front();
                self.front_off = 0;
                continue;
            }
            let n = (front.num_rows() - self.front_off).min(ctx.batch_size);
            let cols = front
                .columns()
                .iter()
                .map(|v| v.slice(self.front_off, n))
                .collect();
            let chunk = DataChunk::with_memory(cols, &ctx.mem.tracker)?;
            self.front_off += n;
            return Ok(Some(chunk));
        }
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
            indexes: Vec::new(),
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
