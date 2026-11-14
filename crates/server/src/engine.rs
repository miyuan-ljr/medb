//! Engine:协调 catalog / LSM 行存 / 列存副本,执行语句计划。
//!
//! - 打开时从 LSM 目录区(保留表 0)恢复 catalog 元数据;
//! - INSERT/UPDATE/DELETE:行编码 → 写批(或事务缓冲)→ 列存副本;
//! - SELECT:计划 → 向量化流水线(快照 read_ts + 事务 overlay)→ 整批收集;
//! - 快照注册表推 GC 水位,压缩只回收无活跃快照依赖的版本。
//!
//! row_id 派生自 `alloc_sequences`(重启水位 = WAL/SST 最大 seq),单调不复用。

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use catalog::{CatalogStore, InMemoryCatalog};
use common::config::Config;
use common::error::{Error, Result};
use common::schema::{ColumnDesc, Row, TableSchema};
use common::stats::TableStats;
use common::types::ScalarValue;
use exec::chunk::DataChunk;
use exec::executor::ExecuteContext;
use exec::expr::Expr;
use exec::operators::seq_scan::QueryDataSource;
use exec::vector::Vector;
use memory::{MemoryContext, MemoryManager};
use parking_lot::Mutex;
use sql::parser::parse_statements;
use sql::planner::{PlanContext, PlannedQuery, StatementPlan, plan_statement};
use storage::lsm::LsmStore;
use storage::table::{catalog_key, catalog_range, row_key, row_range};
use storage::{ColumnarReplica, InMemoryColumnarReplica};
use tracing::{debug, info, warn};

/// 一条语句的执行结果(协议无关,由 handlers 映射到 pgwire Response)。
#[derive(Debug)]
pub enum Outcome {
    /// SELECT:输出 schema + 行。
    Query {
        schema: Vec<ColumnDesc>,
        rows: Vec<Row>,
    },
    /// 命令完成:prefix 形如 "INSERT 0" / "DELETE" / "UPDATE" / "BEGIN";
    /// rows 为受影响行数(仅 DML 有)。
    Tag { prefix: String, rows: Option<u64> },
}

/// 事务缓冲:语句写入暂存,COMMIT 时一次落盘(单条 WAL 记录 = 恢复原子)。
/// key → (表 schema, 编码行 / None=删除)。
#[derive(Debug, Default)]
pub struct TxnBuffer {
    writes: std::collections::BTreeMap<Vec<u8>, BufferedWrite>,
}

#[derive(Debug, Clone)]
pub struct BufferedWrite {
    pub table: Arc<TableSchema>,
    pub value: Option<Vec<u8>>,
}

impl TxnBuffer {
    pub fn put(&mut self, key: Vec<u8>, table: Arc<TableSchema>, value: Option<Vec<u8>>) {
        self.writes.insert(key, BufferedWrite { table, value });
    }

    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    /// 只读 overlay 快照(扫描合并用)。
    pub fn overlay_snapshot(&self) -> std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        self.writes
            .iter()
            .map(|(k, bw)| (k.clone(), bw.value.clone()))
            .collect()
    }

    pub fn into_writes(self) -> Vec<(Arc<TableSchema>, Vec<u8>, Option<Vec<u8>>)> {
        self.writes
            .into_iter()
            .map(|(k, bw)| (bw.table, k, bw.value))
            .collect()
    }
}

/// 表统计存储:table_id → 统计(内存缓存 + LSM 持久化)。
/// 规划期经 `QueryDataSource::table_stats` 访问;缺失/过期时自动 ANALYZE。
#[derive(Default)]
pub(crate) struct StatsStore {
    by_table: Mutex<std::collections::BTreeMap<u64, Arc<TableStats>>>,
}

impl StatsStore {
    fn get(&self, id: u64) -> Option<Arc<TableStats>> {
        self.by_table.lock().get(&id).cloned()
    }

    fn insert(&self, id: u64, stats: Arc<TableStats>) {
        self.by_table.lock().insert(id, stats);
    }

    fn remove(&self, id: u64) {
        self.by_table.lock().remove(&id);
    }

    /// 原地更新并返回更新后的快照(Arc::make_mut,共享时克隆-替换)。
    fn update<F: FnOnce(&mut TableStats)>(&self, id: u64, f: F) -> Option<Arc<TableStats>> {
        let mut map = self.by_table.lock();
        let arc = map.get_mut(&id)?;
        let stats = Arc::make_mut(arc);
        f(stats);
        Some(arc.clone())
    }
}

/// 全量(或抽样)扫描重建统计(自动 ANALYZE 与 ANALYZE 语句共用)。
/// 抽样目标 100 万行:超过时按等步长抽样,NDV 为样本下界估计。
const ANALYZE_SAMPLE_ROWS: usize = 1 << 20;

/// 解码一片 entry 并按谓词过滤(并行解码的工作单元)。
/// 组批(512 行)向量化求值谓词,命中行按序返回。
fn decode_filtered_slice(
    slice: &[(Vec<u8>, Option<Vec<u8>>)],
    table: &TableSchema,
    pred: &exec::expr::Expr,
) -> Result<Vec<Row>> {
    let dtypes: Vec<common::types::DataType> = table.columns.iter().map(|c| c.ty.clone()).collect();
    let mut out = Vec::with_capacity(slice.len() / 4);
    let mut batch: Vec<Row> = Vec::with_capacity(512);
    for (_, value) in slice {
        let Some(bytes) = value else { continue };
        let mut s = bytes.as_slice();
        let mut row = common::codec::decode_row(&mut s)?;
        // schema 前缀兼容
        if row.len() < table.columns.len() {
            row.resize(table.columns.len(), ScalarValue::Null);
        } else if row.len() > table.columns.len() {
            row.truncate(table.columns.len());
        }
        batch.push(row);
        if batch.len() >= 512 {
            let chunk = DataChunk::from_rows(&dtypes, &batch)?;
            let mask = pred.eval(&chunk)?;
            for i in 0..batch.len() {
                if !mask.null_at(i) && mask.bool_values()[i] {
                    out.push(batch[i].clone());
                }
            }
            batch.clear();
        }
    }
    if !batch.is_empty() {
        let chunk = DataChunk::from_rows(&dtypes, &batch)?;
        let mask = pred.eval(&chunk)?;
        for i in 0..batch.len() {
            if !mask.null_at(i) && mask.bool_values()[i] {
                out.push(batch[i].clone());
            }
        }
    }
    Ok(out)
}

/// 列存 ColumnVector → exec Vector;Empty(空表/不支持)→ None。
fn column_vector_to_exec(
    cv: storage::columnar::ColumnVector,
    _ty: common::types::DataType,
) -> Result<Option<Vector>> {
    use storage::columnar::ColumnValues;
    fn to_validity(nulls: Option<Vec<bool>>, len: usize) -> Option<exec::vector::Validity> {
        nulls.map(|mask| {
            let mut v = exec::vector::Validity::all_valid(len);
            for (i, is_null) in mask.iter().enumerate() {
                if *is_null {
                    v.unset(i);
                }
            }
            v
        })
    }
    // nulls 先于 values 消费(避免借用冲突)
    let values_len = cv.values_len();
    let validity = to_validity(cv.nulls, values_len);
    let v = match cv.values {
        ColumnValues::Bool(values) => {
            Vector::new(exec::vector::ColumnData::Bool(values), validity)?
        }
        ColumnValues::Int64(values) => {
            Vector::new(exec::vector::ColumnData::Int64(values), validity)?
        }
        ColumnValues::Float64(values) => {
            Vector::new(exec::vector::ColumnData::Float64(values), validity)?
        }
        ColumnValues::Utf8(values) => Vector::new(
            exec::vector::ColumnData::Utf8 {
                data: values.concat().into_bytes(),
                offsets: {
                    let mut o = Vec::with_capacity(values.len() + 1);
                    let mut acc = 0u32;
                    o.push(0);
                    for s in &values {
                        acc += s.len() as u32;
                        o.push(acc);
                    }
                    o
                },
            },
            validity,
        )?,
        ColumnValues::Date(values) => {
            Vector::new(exec::vector::ColumnData::Date(values), validity)?
        }
        ColumnValues::Empty => return Ok(None),
    };
    Ok(Some(v))
}

fn compute_table_stats(lsm: &Arc<LsmStore>, table: &TableSchema) -> TableStats {
    let (from, to) = row_range(table.id);
    let Ok(entries) = lsm.scan(&from, &to) else {
        return TableStats::default();
    };
    let rows: Vec<Row> = entries
        .into_iter()
        .filter_map(|(_, _seq, v)| v)
        .map(|bytes| {
            let mut slice = bytes.as_slice();
            let mut row = common::codec::decode_row(&mut slice)?;
            // schema 前缀兼容
            if row.len() < table.columns.len() {
                row.resize(table.columns.len(), ScalarValue::Null);
            } else if row.len() > table.columns.len() {
                row.truncate(table.columns.len());
            }
            Ok(row)
        })
        .collect::<Result<Vec<_>>>()
        .unwrap_or_default();
    common::stats::compute_from_rows(table, &rows, ANALYZE_SAMPLE_ROWS)
}

fn persist_stats(lsm: &Arc<LsmStore>, table: &TableSchema, stats: &TableStats) {
    if let Err(e) = lsm.put(&storage::table::stats_key(&table.name), &stats.encode()) {
        warn!(table = %table.name, error = %e, "stats persist failed");
    }
}

/// LSM 表数据源:整表物化(v1 简化;流式/谓词下推见路线图)。
/// 携带语句级快照(read_ts)与事务缓冲 overlay(写缓冲可见、删缓冲隐藏),
/// 以及表统计(规划期访问,缺失/过期时自动 ANALYZE)。
struct LsmDataSource {
    lsm: Arc<LsmStore>,
    read_ts: u64,
    overlay: Arc<std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    stats: Arc<StatsStore>,
    /// AP 列存副本句柄;None = 事务内(副本只见已提交状态,回退行存)
    columnar: Option<Arc<InMemoryColumnarReplica>>,
    /// 并行度(解码/过滤线程数)
    threads: usize,
}

impl QueryDataSource for LsmDataSource {
    fn scan_table(&self, table: &TableSchema) -> Result<Vec<Row>> {
        let (from, to) = row_range(table.id);
        let entries = self.lsm.scan_at(&from, &to, self.read_ts)?;
        // 事务缓冲覆盖(BTreeMap 有序,与扫描输出同序归并)
        let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            entries.into_iter().map(|(k, _seq, v)| (k, v)).collect();
        for (k, v) in self.overlay.range(from..to) {
            merged.insert(k.clone(), v.clone());
        }
        merged
            .into_values()
            .filter_map(|v| v)
            .map(|bytes| {
                let mut slice = bytes.as_slice();
                let row = common::codec::decode_row(&mut slice)?;
                // schema 前缀兼容(多版本 schema):行短 → 补 NULL;行长 → 截断
                if row.len() < table.columns.len() {
                    let mut r = row;
                    r.resize(table.columns.len(), ScalarValue::Null);
                    Ok(r)
                } else if row.len() > table.columns.len() {
                    Ok(row[..table.columns.len()].to_vec())
                } else {
                    Ok(row)
                }
            })
            .collect()
    }

    fn table_stats(&self, table: &TableSchema) -> Option<Arc<TableStats>> {
        if let Some(s) = self.stats.get(table.id) {
            if !s.needs_analyze() {
                return Some(s);
            }
        }
        // 自动 ANALYZE:全量(或抽样)重建 + 持久化
        let fresh = Arc::new(compute_table_stats(&self.lsm, table));
        self.stats.insert(table.id, fresh.clone());
        persist_stats(&self.lsm, table, &fresh);
        Some(fresh)
    }

    /// 谓词下推:解码期过滤(并行;未命中行不进物化)。
    fn scan_table_filtered(
        &self,
        table: &TableSchema,
        pred: &exec::expr::Expr,
    ) -> Result<Vec<Row>> {
        let (from, to) = row_range(table.id);
        let entries = self.lsm.scan_at(&from, &to, self.read_ts)?;
        // 事务缓冲覆盖(BTreeMap 有序,与扫描输出同序归并)
        let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            entries.into_iter().map(|(k, _seq, v)| (k, v)).collect();
        for (k, v) in self.overlay.range(from..to) {
            merged.insert(k.clone(), v.clone());
        }
        let kv: Vec<(Vec<u8>, Option<Vec<u8>>)> = merged.into_iter().collect();
        // 并行解码 + 过滤:分片各自组批求值,按片序拼接(保插入序)
        let parts = exec::parallel::scope_map_try(&kv, self.threads, |slice| {
            decode_filtered_slice(slice, table, pred)
        })?;
        Ok(parts.concat())
    }

    /// AP 列存路径:标量表 + 无事务门控;Date 列已支持;LOB/数组/向量回退。
    fn scan_columns(&self, table: &TableSchema) -> Result<Option<Vec<Vector>>> {
        let Some(replica) = &self.columnar else {
            return Ok(None);
        };
        for c in &table.columns {
            if !matches!(
                c.ty,
                common::types::DataType::Boolean
                    | common::types::DataType::Int64
                    | common::types::DataType::Float64
                    | common::types::DataType::Utf8
                    | common::types::DataType::Date
            ) {
                return Ok(None);
            }
        }
        let mut cols = Vec::with_capacity(table.columns.len());
        for (i, c) in table.columns.iter().enumerate() {
            let cv = replica.scan_column(common::ids::TableId(table.id), i, &c.ty)?;
            let Some(v) = column_vector_to_exec(cv, c.ty.clone())? else {
                return Ok(None); // 空表(或类型不符)→ 行存路径
            };
            cols.push(v);
        }
        // 列间长度一致性(副本损坏防护)
        let len = cols[0].len();
        if cols.iter().any(|v| v.len() != len) {
            return Err(Error::corruption("columnar columns misaligned"));
        }
        Ok(Some(cols))
    }
}

/// 引擎持有的活跃快照(GC 水位 = 最小活跃快照);语句结束自动注销。
pub struct SnapshotGuard {
    engine: Arc<Engine>,
    ts: u64,
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        self.engine.active_snapshots.lock().remove(&self.ts);
        self.engine.refresh_gc_watermark();
    }
}

/// 语句执行上下文:快照 + 可选事务缓冲。
pub struct StatementCtx<'a> {
    pub read_ts: u64,
    pub overlay: Arc<std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    pub txn: Option<&'a mut TxnBuffer>,
    _guard: SnapshotGuard,
}

pub struct Engine {
    config: Config,
    memory: Arc<MemoryManager>,
    catalog: Arc<InMemoryCatalog>,
    lsm: Arc<LsmStore>,
    columnar: Arc<InMemoryColumnarReplica>,
    stats: Arc<StatsStore>,
    active_snapshots: Mutex<BTreeSet<u64>>,
    /// 提交锁:唯一性探测与写盘在同一把锁下串行,消除 check-then-write 竞争
    commit_lock: Mutex<()>,
    _restored_tables: AtomicU64,
}

impl Engine {
    /// 打开引擎:恢复 LSM(catalog + 数据)。
    pub fn open(config: Config) -> Result<Arc<Self>> {
        let global_limit =
            (config.memory.global_limit_bytes != 0).then_some(config.memory.global_limit_bytes);
        let memory = Arc::new(MemoryManager::new(global_limit));

        let opts = storage::lsm::LsmOptions::from(&config.storage);
        let lsm = LsmStore::open(&config.storage.data_dir, opts, &memory.root())?;

        // 从目录区恢复表 schema(值 = serde_json 编码的 TableSchema)
        // 与表统计(键带 0x01 标记;统计键排在 catalog 键之前 → 两阶段:
        // 先收集,待 schema 全部恢复后再挂到表上)
        let catalog = Arc::new(InMemoryCatalog::new());
        let stats = Arc::new(StatsStore::default());
        let (cfrom, cto) = catalog_range();
        let mut restored = 0u64;
        let mut pending_stats: Vec<(String, Vec<u8>)> = Vec::new();
        for (key, _seq, value) in lsm.scan(&cfrom, &cto)? {
            let Some(bytes) = value else { continue };
            if key.len() > 8 && key[8] == common::stats::STATS_MARKER {
                let Ok(name) = storage::table::stats_name(&key) else {
                    continue;
                };
                pending_stats.push((name.to_string(), bytes));
                continue;
            }
            let name = storage::table::catalog_name(&key)?.to_string();
            let Some(schema) = decode_schema(&bytes) else {
                warn!(table = %name, "undecodable catalog entry skipped");
                continue;
            };
            catalog.create_table(schema)?;
            restored += 1;
        }
        let mut restored_stats = 0usize;
        for (name, bytes) in pending_stats {
            let Some(schema) = catalog.get_table(&name) else {
                continue; // 统计键无对应表(残留)→ 丢弃
            };
            if let Some(s) = TableStats::decode(&bytes) {
                stats.insert(schema.id, Arc::new(s));
                restored_stats += 1;
            }
        }
        if restored > 0 {
            info!(tables = restored, "catalog restored from lsm");
        }
        if restored_stats > 0 {
            info!(tables = restored_stats, "table stats restored from lsm");
        }

        // 列存副本是内存镜像,重启即失 —— 从 LSM 全量重建,
        // 使 AP 列存路径与行存始终一致(增量 CDC 见路线图)
        let columnar = Arc::new(InMemoryColumnarReplica::new());
        let mut mirrored_rows = 0u64;
        for schema in catalog.list_tables() {
            let (from, to) = row_range(schema.id);
            let Ok(entries) = lsm.scan(&from, &to) else {
                continue;
            };
            let schema_arc = Arc::new(schema.clone());
            for (key, _seq, value) in entries {
                let Some(bytes) = value else { continue };
                let mut slice = bytes.as_slice();
                let Ok(mut row) = common::codec::decode_row(&mut slice) else {
                    continue;
                };
                if row.len() < schema.columns.len() {
                    row.resize(schema.columns.len(), ScalarValue::Null);
                } else if row.len() > schema.columns.len() {
                    row.truncate(schema.columns.len());
                }
                if let Err(e) = columnar.upsert(&schema_arc, &key, row) {
                    warn!(table = %schema.name, error = %e, "columnar rebuild failed");
                    break;
                }
                mirrored_rows += 1;
            }
        }
        if mirrored_rows > 0 {
            info!(rows = mirrored_rows, "columnar replica rebuilt from lsm");
        }
        Ok(Arc::new(Self {
            config,
            memory,
            catalog,
            lsm,
            columnar,
            stats,
            active_snapshots: Mutex::new(BTreeSet::new()),
            commit_lock: Mutex::new(()),
            _restored_tables: AtomicU64::new(restored),
        }))
    }

    pub fn catalog(&self) -> Arc<InMemoryCatalog> {
        self.catalog.clone()
    }

    pub fn memory(&self) -> Arc<MemoryManager> {
        self.memory.clone()
    }

    pub fn lsm(&self) -> Arc<LsmStore> {
        self.lsm.clone()
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    // ---------- 快照 / 数据源 ----------

    fn register_snapshot(self: &Arc<Self>, ts: u64) -> SnapshotGuard {
        self.active_snapshots.lock().insert(ts);
        self.refresh_gc_watermark();
        SnapshotGuard {
            engine: self.clone(),
            ts,
        }
    }

    fn refresh_gc_watermark(&self) {
        let wm = self
            .active_snapshots
            .lock()
            .iter()
            .next()
            .copied()
            .unwrap_or(u64::MAX);
        self.lsm.set_gc_watermark(wm);
    }

    /// 构造语句级数据源(read_ts + overlay 编译进 source,SeqScanExec 拉取时生效)。
    fn data_source_for(
        &self,
        read_ts: u64,
        overlay: Arc<std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
        columnar_ok: bool,
    ) -> Arc<dyn QueryDataSource> {
        Arc::new(LsmDataSource {
            lsm: self.lsm.clone(),
            read_ts,
            overlay,
            stats: self.stats.clone(),
            // 事务内回退行存:副本只镜像已提交状态,看不到本事务写缓冲
            columnar: columnar_ok.then_some(self.columnar.clone()),
            threads: self.config.engine.parallelism,
        })
    }

    /// 语句上下文的数据源(快照 + overlay;无事务时允许 AP 列存路径)。
    pub(crate) fn data_source_for_ctx(&self, sctx: &StatementCtx<'_>) -> Arc<dyn QueryDataSource> {
        let columnar_ok = sctx.txn.is_none() && sctx.overlay.is_empty();
        self.data_source_for(sctx.read_ts, sctx.overlay.clone(), columnar_ok)
    }

    /// 语句开始:RC 语义下取当前 last_seq 为快照并注册(语句结束注销)。
    pub fn begin_statement<'a>(
        self: &Arc<Self>,
        txn: Option<&'a mut TxnBuffer>,
    ) -> StatementCtx<'a> {
        let read_ts = self.lsm.last_sequence();
        let overlay = txn
            .as_deref()
            .map(|t| Arc::new(t.overlay_snapshot()))
            .unwrap_or_default();
        let _guard = self.register_snapshot(read_ts);
        StatementCtx {
            read_ts,
            overlay,
            txn,
            _guard,
        }
    }

    /// 空事务缓冲下的语句上下文(autocommit)。
    pub fn begin_autocommit(self: &Arc<Self>) -> StatementCtx<'static> {
        self.begin_statement(None)
    }

    /// 手动转储:冻结 memtable 写成 L0 SST,重置 WAL。返回是否发生了 flush。
    pub fn flush(&self) -> Result<bool> {
        self.lsm.flush()
    }

    /// ANALYZE [TABLE]:全量(或抽样)重建表统计并持久化。
    /// name 为 None 时分析全部表。
    pub fn analyze_table(&self, name: Option<&str>) -> Result<Outcome> {
        let targets: Vec<TableSchema> = match name {
            Some(n) => vec![
                self.catalog
                    .get_table(n)
                    .ok_or_else(|| Error::catalog(format!("table '{n}' not found")))?,
            ],
            None => self.catalog.list_tables(),
        };
        for table in &targets {
            let fresh = Arc::new(compute_table_stats(&self.lsm, table));
            self.stats.insert(table.id, fresh.clone());
            persist_stats(&self.lsm, table, &fresh);
            info!(table = %table.name, stats = %fresh, "analyzed");
        }
        Ok(Outcome::Tag {
            prefix: "ANALYZE".into(),
            rows: None,
        })
    }

    /// 表统计只读访问(测试/诊断用)。
    pub fn table_stats(&self, table_id: u64) -> Option<Arc<TableStats>> {
        self.stats.get(table_id)
    }

    /// 存储概要(memtable 用量 / 各层文件数)。
    pub fn storage_stats(&self) -> storage::lsm::LsmStats {
        self.lsm.stats()
    }

    // ---------- 语句执行 ----------

    /// 执行一段 SQL(autocommit;每条语句独立快照)。
    pub fn execute_script(self: &Arc<Self>, sql: &str) -> Result<Vec<Outcome>> {
        let statements = parse_statements(sql)?;
        let mut outcomes = Vec::with_capacity(statements.len());
        for stmt in &statements {
            let plan = self.plan_statement_with_snapshot(stmt)?;
            let mut sctx = self.begin_autocommit();
            outcomes.push(self.execute_plan(&mut sctx, plan)?);
        }
        Ok(outcomes)
    }

    fn plan_statement_with_snapshot(
        &self,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<StatementPlan> {
        let overlay: Arc<std::collections::BTreeMap<_, _>> = Arc::new(Default::default());
        let ctx = PlanContext {
            batch_size: self.config.engine.batch_size,
            catalog: &*self.catalog,
            data: self.data_source_for(self.lsm.last_sequence(), overlay, true),
        };
        plan_statement(stmt, &ctx)
    }

    /// 在语句上下文中执行已计划语句。
    pub fn execute_plan(
        &self,
        sctx: &mut StatementCtx<'_>,
        plan: StatementPlan,
    ) -> Result<Outcome> {
        match plan {
            StatementPlan::Query(pq) => self.run_query(pq),
            StatementPlan::Insert { table, rows } => self.exec_insert(&mut sctx.txn, table, rows),
            StatementPlan::InsertSelect {
                table,
                targets,
                query,
            } => {
                let outcome = self.run_query(query)?;
                let Outcome::Query { rows, .. } = outcome else {
                    return Err(Error::internal("insert-select query returned a tag"));
                };
                let mut full_rows: Vec<Row> = Vec::with_capacity(rows.len());
                for row in rows {
                    if row.len() != targets.len() {
                        return Err(Error::internal(format!(
                            "insert-select row arity {} != expected {}",
                            row.len(),
                            targets.len()
                        )));
                    }
                    let mut full: Row = vec![ScalarValue::Null; table.columns.len()];
                    for (ti, v) in targets.iter().zip(row) {
                        full[*ti] = sql::planner::coerce_value(v, &table.columns[*ti])?;
                    }
                    full_rows.push(full);
                }
                self.exec_insert(&mut sctx.txn, table, full_rows)
            }
            StatementPlan::Update {
                table,
                assignments,
                predicate,
            } => self.exec_update(sctx, table, assignments, predicate),
            StatementPlan::Delete { table, predicate } => self.exec_delete(sctx, table, predicate),
            StatementPlan::CreateTable { schema } => self.exec_create_table(schema),
            StatementPlan::Analyze { name } => self.analyze_table(name.as_deref()),
            StatementPlan::Explain { schema, lines } => Ok(Outcome::Query {
                schema,
                rows: lines
                    .into_iter()
                    .map(|l| vec![ScalarValue::Utf8(l)])
                    .collect(),
            }),
            StatementPlan::AlterAddColumn {
                name,
                column,
                if_not_exists,
            } => self.exec_alter_add_column(&name, column, if_not_exists),
            StatementPlan::DropTable { name } => self.exec_drop_table(&name),
            StatementPlan::Begin | StatementPlan::Commit | StatementPlan::Rollback => Err(
                Error::internal("transaction control statement handled by Session"),
            ),
        }
    }

    // ---------- SELECT ----------

    fn run_query(&self, pq: PlannedQuery) -> Result<Outcome> {
        let mem = self.new_query_context();
        let mut ctx = ExecuteContext::with_threads(
            self.config.engine.batch_size,
            mem,
            self.config.engine.parallelism,
        );
        let mut exec = pq.executor;
        let dtypes: Vec<common::types::DataType> = pq.schema.iter().map(|c| c.ty.clone()).collect();
        let mut rows: Vec<Row> = Vec::new();
        // 输出物化记账(超查询/全局限额 → MemoryLimitExceeded)
        let mut mem_res: Option<memory::tracker::Reservation> = None;
        let mut mem_bytes = 0usize;
        loop {
            let Some(chunk) = exec.next_batch(&mut ctx)? else {
                break;
            };
            for i in 0..chunk.num_rows() {
                let row = chunk.row(i);
                mem_bytes += exec::chunk::row_approx_bytes(&dtypes, &row);
                match mem_res.as_mut() {
                    Some(r) => r.resize(mem_bytes)?,
                    None => mem_res = Some(ctx.mem.tracker.try_reserve(mem_bytes)?),
                }
                rows.push(row);
            }
        }
        drop(mem_res);
        debug!(rows = rows.len(), "query executed");
        Ok(Outcome::Query {
            schema: pq.schema,
            rows,
        })
    }

    fn new_query_context(&self) -> MemoryContext {
        let mem = MemoryContext::new(
            &self.memory.root(),
            "query",
            self.config.memory.arena_chunk_bytes,
        );
        mem.tracker
            .set_limit(Some(self.config.memory.query_limit_bytes));
        mem
    }

    // ---------- 写路径 ----------

    /// 应用一批写(行 + 索引条目 + 索引墓碑):
    /// - autocommit:提交锁下先做唯一性探测,再一次落 LSM(单条 WAL = 恢复原子);
    /// - 事务内:全部进缓冲,唯一性探测推迟到 COMMIT(commit_txn)。
    fn apply_batch(
        &self,
        txn: Option<&mut TxnBuffer>,
        table: &TableSchema,
        entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        unique_encs: &[(Vec<u8>, u64)], // 新写入的唯一索引编码 + 对应 row_id
        index_tombstones: &std::collections::HashSet<Vec<u8>>,
    ) -> Result<()> {
        let table_arc = Arc::new(table.clone());
        match txn {
            Some(buf) => {
                for (key, value) in entries {
                    buf.put(key, table_arc.clone(), value);
                }
            }
            None => {
                let _guard = self.commit_lock.lock();
                self.check_unique(&table_arc, unique_encs, index_tombstones)?;
                self.lsm.write_batch(&entries)?;
                // 列存副本只镜像行写(行键恒 16 字节;索引键更长)
                for (key, value) in &entries {
                    if key.len() != 16 {
                        continue; // 索引条目
                    }
                    match value {
                        Some(bytes) => {
                            let mut slice = bytes.as_slice();
                            let row = common::codec::decode_row(&mut slice)?;
                            if let Err(e) = self.columnar.upsert(&table_arc, key, row) {
                                warn!(
                                    table = %table_arc.name,
                                    error = %e,
                                    "columnar replica apply failed"
                                );
                            }
                        }
                        None => {
                            if let Err(e) =
                                self.columnar.delete(common::ids::TableId(table.id), key)
                            {
                                warn!(
                                    table = %table_arc.name,
                                    error = %e,
                                    "columnar replica delete failed"
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// 主键组件值(按 schema.primary_key 顺序);NULL 视为约束违反。
    fn pk_component_values(table: &TableSchema, row: &Row) -> Result<Vec<ScalarValue>> {
        let Some(pk_idx) = table.pk_column_indexes() else {
            return Err(Error::internal("table has no primary key"));
        };
        let mut out = Vec::with_capacity(pk_idx.len());
        for i in pk_idx {
            let v = &row[i];
            if v.is_null() {
                return Err(Error::sql(format!(
                    "primary key column '{}' must not be NULL",
                    table.columns[i].name
                )));
            }
            out.push(v.clone());
        }
        Ok(out)
    }

    fn encode_pk(table: &TableSchema, row: &Row) -> Result<Vec<u8>> {
        let mut enc = Vec::new();
        for v in Self::pk_component_values(table, row)? {
            enc.extend_from_slice(&storage::table::encode_pk_component(&v)?);
        }
        Ok(enc)
    }

    /// 唯一性探测:每个新编码在 [enc++00×8, enc++FF×8+00] 的精确区间扫描;
    /// 命中的条目若属于本批要删除/覆盖的旧键,或 row_id 相同,则放行。
    fn check_unique(
        &self,
        table: &TableSchema,
        encs: &[(Vec<u8>, u64)],
        tombstones: &std::collections::HashSet<Vec<u8>>,
    ) -> Result<()> {
        if encs.is_empty() {
            return Ok(());
        }
        for (enc, row_id) in encs {
            let (lo, hi) =
                storage::table::index_probe_range(table.id, storage::table::PK_INDEX_ID, enc);
            for (key, _seq, _value) in self.lsm.scan_at(&lo, &hi, u64::MAX)? {
                let existing = storage::table::row_id_from_entry_key(&key)?;
                if existing == *row_id {
                    continue; // 自身(UPDATE 原地写同值)
                }
                if tombstones.contains(&key) {
                    continue; // 本批删除/覆盖的旧条目
                }
                return Err(Error::sql(format!(
                    "duplicate key value violates primary key constraint on table '{}'",
                    table.name
                )));
            }
        }
        Ok(())
    }

    fn exec_insert(
        &self,
        txn: &mut Option<&mut TxnBuffer>,
        table: TableSchema,
        rows: Vec<Row>,
    ) -> Result<Outcome> {
        let n = rows.len() as u64;
        if n == 0 {
            return Ok(Outcome::Tag {
                prefix: "INSERT 0".into(),
                rows: Some(0),
            });
        }
        let base = self.lsm.alloc_sequences(n);
        if base + n - 1 > storage::table::ROW_ID_MAX {
            return Err(Error::internal("row id space exhausted"));
        }
        let mut entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(rows.len() * 2);
        let mut unique_encs: Vec<(Vec<u8>, u64)> = Vec::new();
        let has_pk = !table.primary_key.is_empty();

        for (i, row) in rows.iter().enumerate() {
            let row_id = base + i as u64;
            let key = row_key(table.id, row_id);
            entries.push((key.clone(), Some(common::codec::encode_row_to_vec(row))));
            if has_pk {
                let enc = Self::encode_pk(&table, row)?;
                entries.push((
                    storage::table::index_entry_key(
                        table.id,
                        storage::table::PK_INDEX_ID,
                        &enc,
                        row_id,
                    ),
                    Some(Vec::new()),
                ));
                unique_encs.push((enc, row_id));
            }
        }
        self.apply_batch(
            txn.as_deref_mut(),
            &table,
            entries,
            &unique_encs,
            &Default::default(),
        )?;
        // 统计增量(autocommit;事务路径由 commit_txn 统一标记过期)
        if txn.is_none() {
            let updated = self
                .stats
                .update(table.id, |s| s.record_insert(&table, &rows));
            if let Some(s) = updated {
                persist_stats(&self.lsm, &table, &s);
            }
        }
        debug!(table = %table.name, rows = rows.len(), base_seq = base, "inserted");
        Ok(Outcome::Tag {
            prefix: "INSERT 0".into(),
            rows: Some(n),
        })
    }

    /// 读取表当前可见行(快照 + overlay)+ 行键(UPDATE/DELETE 共用)。
    fn scan_table_rows(
        &self,
        sctx: &StatementCtx<'_>,
        table: &TableSchema,
    ) -> Result<(Vec<Vec<u8>>, Vec<Row>)> {
        let (from, to) = row_range(table.id);
        let entries = self.lsm.scan_at(&from, &to, sctx.read_ts)?;
        let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            entries.into_iter().map(|(k, _seq, v)| (k, v)).collect();
        for (k, v) in sctx.overlay.range(from..to) {
            merged.insert(k.clone(), v.clone());
        }

        let mut keys = Vec::with_capacity(merged.len());
        let mut rows = Vec::with_capacity(merged.len());
        for (key, value) in merged {
            let Some(bytes) = value else { continue };
            let mut slice = bytes.as_slice();
            let row = common::codec::decode_row(&mut slice)?;
            if row.len() < table.columns.len() {
                let mut r = row;
                r.resize(table.columns.len(), ScalarValue::Null);
                rows.push(r);
            } else if row.len() > table.columns.len() {
                rows.push(row[..table.columns.len()].to_vec());
            } else {
                rows.push(row);
            }
            keys.push(key);
        }
        Ok((keys, rows))
    }

    fn eval_selection(
        predicate: Option<&Expr>,
        table: &TableSchema,
        rows: &[Row],
    ) -> Result<Vec<usize>> {
        match predicate {
            None => Ok((0..rows.len()).collect()),
            Some(pred) => {
                if rows.is_empty() {
                    return Ok(Vec::new());
                }
                let columns = table
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        Vector::from_scalars(c.ty.clone(), rows.iter().map(|r| r[i].clone()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let chunk = DataChunk::new(columns)?;
                let mask = pred.eval(&chunk)?;
                Ok((0..rows.len())
                    .filter(|i| mask.bool_values()[*i] && !mask.null_at(*i))
                    .collect())
            }
        }
    }

    fn exec_update(
        &self,
        sctx: &mut StatementCtx<'_>,
        table: TableSchema,
        assignments: Vec<(usize, Expr)>,
        predicate: Option<Expr>,
    ) -> Result<Outcome> {
        let (keys, rows) = self.scan_table_rows(sctx, &table)?;
        if rows.is_empty() {
            return Ok(Outcome::Tag {
                prefix: "UPDATE".into(),
                rows: Some(0),
            });
        }

        // 谓词与全部赋值表达式都在"旧行"上整批求值(标准 SQL:各赋值互不可见)
        let columns = table
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| Vector::from_scalars(c.ty.clone(), rows.iter().map(|r| r[i].clone())))
            .collect::<Result<Vec<_>>>()?;
        let chunk = DataChunk::new(columns)?;
        let selection = Self::eval_selection(predicate.as_ref(), &table, &rows)?;
        if selection.is_empty() {
            return Ok(Outcome::Tag {
                prefix: "UPDATE".into(),
                rows: Some(0),
            });
        }
        let mut new_cols = Vec::with_capacity(assignments.len());
        for (_idx, expr) in &assignments {
            new_cols.push(expr.eval(&chunk)?);
        }

        let pk_idxs = table.pk_column_indexes();
        let pk_modified = pk_idxs
            .as_ref()
            .is_some_and(|pk| assignments.iter().any(|(i, _)| pk.contains(i)));

        let mut entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        let mut unique_encs: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut tombstones: std::collections::HashSet<Vec<u8>> = Default::default();
        for &i in &selection {
            let mut new_row = rows[i].clone();
            for (pos, &(col_idx, _)) in assignments.iter().enumerate() {
                let v = new_cols[pos].scalar_at(i);
                new_row[col_idx] = sql::planner::coerce_value(v, &table.columns[col_idx])?;
            }
            let encoded = common::codec::encode_row_to_vec(&new_row);
            entries.push((keys[i].clone(), Some(encoded)));

            if let Some(pk) = &pk_idxs {
                // 行键含 row_id:从 row_key 反解
                let Some((_, row_id)) = storage::table::parse_row_key(&keys[i]) else {
                    return Err(Error::corruption("row key arity mismatch"));
                };
                if pk_modified {
                    let old_enc = Self::encode_pk(&table, &rows[i])?;
                    let old_key = storage::table::index_entry_key(
                        table.id,
                        storage::table::PK_INDEX_ID,
                        &old_enc,
                        row_id,
                    );
                    entries.push((old_key.clone(), None));
                    tombstones.insert(old_key);

                    let new_enc = Self::encode_pk(&table, &new_row)?;
                    let new_key = storage::table::index_entry_key(
                        table.id,
                        storage::table::PK_INDEX_ID,
                        &new_enc,
                        row_id,
                    );
                    entries.push((new_key, Some(Vec::new())));
                    unique_encs.push((new_enc, row_id));
                } else {
                    let _ = pk;
                }
            }
        }
        self.apply_batch(
            sctx.txn.as_deref_mut(),
            &table,
            entries,
            &unique_encs,
            &tombstones,
        )?;
        // 统计增量:被赋值列的 min/max 扩张(autocommit)
        if sctx.txn.is_none() {
            let updated = self.stats.update(table.id, |s| {
                for (pos, &(col_idx, _)) in assignments.iter().enumerate() {
                    let vals = (0..selection.len()).map(|i| new_cols[pos].scalar_at(i));
                    s.widen_column(&table.columns[col_idx].name, vals);
                }
            });
            if let Some(s) = updated {
                persist_stats(&self.lsm, &table, &s);
            }
        }
        debug!(table = %table.name, count = selection.len(), "updated");
        Ok(Outcome::Tag {
            prefix: "UPDATE".into(),
            rows: Some(selection.len() as u64),
        })
    }

    // ---------- DELETE ----------

    fn exec_delete(
        &self,
        sctx: &mut StatementCtx<'_>,
        table: TableSchema,
        predicate: Option<Expr>,
    ) -> Result<Outcome> {
        let (keys, rows) = self.scan_table_rows(sctx, &table)?;
        let selection = Self::eval_selection(predicate.as_ref(), &table, &rows)?;
        if selection.is_empty() {
            return Ok(Outcome::Tag {
                prefix: "DELETE".into(),
                rows: Some(0),
            });
        }
        let pk_idxs = table.pk_column_indexes();
        let mut entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        let empty_unique: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut tombstones: std::collections::HashSet<Vec<u8>> = Default::default();
        for &i in &selection {
            entries.push((keys[i].clone(), None));
            if let Some(pk) = &pk_idxs {
                let mut enc = Vec::new();
                for &ci in pk {
                    enc.extend_from_slice(&storage::table::encode_pk_component(&rows[i][ci])?);
                }
                let row_id = match storage::table::parse_row_key(&keys[i]) {
                    Some((_, rid)) => rid,
                    None => {
                        return Err(Error::corruption("row key arity mismatch"));
                    }
                };
                let idx_key = storage::table::index_entry_key(
                    table.id,
                    storage::table::PK_INDEX_ID,
                    &enc,
                    row_id,
                );
                entries.push((idx_key.clone(), None));
                tombstones.insert(idx_key);
            }
        }
        self.apply_batch(
            sctx.txn.as_deref_mut(),
            &table,
            entries,
            &empty_unique,
            &tombstones,
        )?;
        // 统计增量:行数下调(autocommit;事务路径由 commit_txn 标记过期)
        if sctx.txn.is_none() {
            let updated = self
                .stats
                .update(table.id, |s| s.record_delete(selection.len() as u64));
            if let Some(s) = updated {
                persist_stats(&self.lsm, &table, &s);
            }
        }
        debug!(table = %table.name, count = selection.len(), "deleted");
        Ok(Outcome::Tag {
            prefix: "DELETE".into(),
            rows: Some(selection.len() as u64),
        })
    }

    // ---------- CREATE / DROP TABLE ----------

    fn exec_create_table(&self, schema: TableSchema) -> Result<Outcome> {
        // catalog 分配 id;随后持久化到目录区
        let created = self.catalog.create_table(schema)?;
        let bytes = encode_schema(&created)?;
        if let Err(e) = self.lsm.put(&catalog_key(&created.name), &bytes) {
            // 持久化失败 → 回滚内存 catalog,保持两侧一致
            let _ = self.catalog.drop_table(&created.name);
            return Err(e);
        }
        info!(table = %created.name, id = created.id, "table created");
        Ok(Outcome::Tag {
            prefix: "CREATE TABLE".into(),
            rows: None,
        })
    }

    /// ALTER TABLE ADD COLUMN(尾部追加;DDL 自动提交,行数据原地重写):
    /// 1. 校验列名不冲突 → 2. 重写全部行(旧行尾补 NULL)→ 3. catalog 版本 +1
    /// → 4. 持久化目录区 → 5. 列存副本整体重建。
    fn exec_alter_add_column(
        &self,
        name: &str,
        column: ColumnDesc,
        if_not_exists: bool,
    ) -> Result<Outcome> {
        let current = self
            .catalog
            .get_table(name)
            .ok_or_else(|| Error::catalog(format!("table '{name}' not found")))?;
        if let Some(_existing) = find_column_ci(&current.columns, &column.name) {
            if if_not_exists {
                return Ok(Outcome::Tag {
                    prefix: "ALTER TABLE".into(),
                    rows: None,
                });
            }
            return Err(Error::catalog(format!(
                "column '{}' of table '{name}' already exists",
                column.name
            )));
        }
        let mut next = current.clone();
        next.columns.push(column);
        next.version = current.version.max(1) + 1;

        // 1. 重写行数据(旧 schema 读 → 尾补 NULL → 新 schema 编码)
        let (from, to) = row_range(current.id);
        let entries = self.lsm.scan(&from, &to)?;
        let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len());
        for (key, _seq, value) in entries {
            let Some(bytes) = value else { continue };
            let mut slice = bytes.as_slice();
            let mut row = common::codec::decode_row(&mut slice)?;
            row.resize(next.columns.len(), ScalarValue::Null);
            batch.push((key, common::codec::encode_row_to_vec(&row)));
        }
        let batch_refs: Vec<(Vec<u8>, Option<Vec<u8>>)> = batch
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        self.lsm.write_batch(&batch_refs)?;

        // 2. catalog 版本递增
        self.catalog.update_table(next.clone())?;

        // 3. 持久化目录区
        let bytes = encode_schema(&next)?;
        if let Err(e) = self.lsm.put(&catalog_key(name), &bytes) {
            // 回滚 catalog 到旧 schema(行数据已是新格式,但旧 schema 读新行
            // 会截断尾部 —— 前缀兼容,语义一致)
            let _ = self.catalog.update_table(current);
            return Err(e);
        }

        // 4. 列存副本整体重建(简单正确;增量刷新见路线图)
        self.columnar.drop_table(common::ids::TableId(next.id))?;
        let next_arc = Arc::new(next.clone());
        for (key, value) in &batch {
            let mut slice = value.as_slice();
            let row = common::codec::decode_row(&mut slice)?;
            if let Err(e) = self.columnar.upsert(&next_arc, key, row) {
                warn!(table = name, error = %e, "columnar replica rebuild failed");
            }
        }
        info!(table = name, version = next.version, column = %next.columns.last().unwrap().name, "column added");
        Ok(Outcome::Tag {
            prefix: "ALTER TABLE".into(),
            rows: None,
        })
    }

    fn exec_drop_table(&self, name: &str) -> Result<Outcome> {
        let dropped = self.catalog.drop_table(name)?;
        // 数据打墓碑 + 目录项删除 + 统计键删除,一并进一个写批
        let (from, to) = row_range(dropped.id);
        let mut batch: Vec<(Vec<u8>, Option<Vec<u8>>)> = self
            .lsm
            .scan(&from, &to)?
            .into_iter()
            .map(|(key, _, _)| (key, None))
            .collect();
        batch.push((catalog_key(name), None));
        batch.push((storage::table::stats_key(name), None));
        self.lsm.write_batch(&batch)?;
        self.stats.remove(dropped.id);
        self.columnar.drop_table(common::ids::TableId(dropped.id))?;
        info!(table = name, "table dropped");
        Ok(Outcome::Tag {
            prefix: "DROP TABLE".into(),
            rows: None,
        })
    }

    // ---------- 事务提交 ----------

    /// COMMIT:唯一性探测(提交锁下)→ 事务缓冲一次性落盘(单条 WAL 记录 =
    /// 恢复原子)→ 应用列存副本。
    pub fn commit_txn(&self, buf: TxnBuffer) -> Result<()> {
        let writes = buf.into_writes();
        if writes.is_empty() {
            return Ok(());
        }
        let _guard = self.commit_lock.lock();

        // 唯一性探测:索引条目 = 键长 > 16 的写入(行键恒 16 字节)。
        // 命中已提交的同主键条目(rowid-in-key,探测区间精确),
        // 且该键不在本事务自己的墓碑集合中 → 冲突。
        let tombstones: std::collections::HashSet<Vec<u8>> = writes
            .iter()
            .filter(|(_, k, v)| v.is_none() && k.len() > 16)
            .map(|(_, k, _)| k.clone())
            .collect();
        for (table, key, value) in &writes {
            let Some(_) = value else { continue };
            if key.len() <= 16 {
                continue; // 行键
            }
            // 索引键布局:table_id(8) ++ [0xFF](1) ++ index_id(8) ++ enc ++ row_id(8)
            let enc = &key[17..key.len() - 8];
            let my_row_id = storage::table::row_id_from_entry_key(key)?;
            let (lo, hi) =
                storage::table::index_probe_range(table.id, storage::table::PK_INDEX_ID, enc);
            for (k, _seq, _existing) in self.lsm.scan_at(&lo, &hi, u64::MAX)? {
                let existing = storage::table::row_id_from_entry_key(&k)?;
                if existing == my_row_id {
                    continue; // 自身(理论不可达:本事务条目尚未落盘)
                }
                if tombstones.contains(&k) {
                    continue; // 本事务已删除/覆盖的旧条目
                }
                return Err(Error::sql(format!(
                    "duplicate key value violates primary key constraint on table '{}' (detected at commit)",
                    table.name
                )));
            }
        }

        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = writes
            .iter()
            .map(|(_, k, v)| (k.clone(), v.clone()))
            .collect();
        self.lsm.write_batch(&entries)?;
        // 统计:事务提交无法区分插入/更新对行数的影响 → 整体标记过期,
        // 规划期自动重算(持久化标记,重启后仍生效)
        let mut touched: std::collections::BTreeMap<u64, Arc<TableSchema>> = Default::default();
        for (table, key, _) in &writes {
            if key.len() == 16 {
                touched.insert(table.id, table.clone());
            }
        }
        for (id, table) in &touched {
            if let Some(s) = self.stats.update(*id, |s| s.stale = true) {
                persist_stats(&self.lsm, table, &s);
            }
        }
        for (table, key, value) in writes {
            if key.len() != 16 {
                continue; // 索引条目不镜像列存
            }
            match value {
                Some(encoded) => {
                    let mut slice = encoded.as_slice();
                    let row = common::codec::decode_row(&mut slice)?;
                    if let Err(e) = self.columnar.upsert(&table, &key, row) {
                        warn!(table = %table.name, error = %e, "columnar replica apply failed");
                    }
                }
                None => {
                    if let Err(e) = self.columnar.delete(common::ids::TableId(table.id), &key) {
                        warn!(table = %table.name, error = %e, "columnar replica delete failed");
                    }
                }
            }
        }
        Ok(())
    }
}

fn encode_schema(schema: &TableSchema) -> Result<Vec<u8>> {
    serde_json::to_vec(schema).map_err(|e| Error::internal(format!("encode schema: {e}")))
}

fn decode_schema(bytes: &[u8]) -> Option<TableSchema> {
    let mut schema: TableSchema = serde_json::from_slice(bytes).ok()?;
    // 旧版本目录数据无 version 字段 → 归一为 1
    if schema.version == 0 {
        schema.version = 1;
    }
    Some(schema)
}

/// 大小写不敏感列查找(ALTER 校验用)。
fn find_column_ci(columns: &[ColumnDesc], name: &str) -> Option<usize> {
    columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
}
