//! Engine:协调 catalog / LSM 行存 / 列存副本,执行语句计划。
//!
//! - 打开时从 LSM 目录区(保留表 0)恢复 catalog 元数据;
//! - INSERT/UPDATE/DELETE:行编码 → 写批(或事务缓冲)→ 列存副本;
//! - SELECT:计划 → 向量化流水线(快照 read_ts + 事务 overlay)→ 整批收集;
//! - 快照注册表推 GC 水位,压缩只回收无活跃快照依赖的版本。
//!
//! row_id 派生自 `alloc_sequences`(重启水位 = WAL/SST 最大 seq),单调不复用。

use std::collections::{BTreeSet, HashMap};
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
use storage::columnar_file::{self as columnar_file, SegmentRow};
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
    pred: Option<&exec::expr::Expr>,
    needed: &[bool],
) -> Result<Vec<Row>> {
    let dtypes: Vec<common::types::DataType> = table.columns.iter().map(|c| c.ty.clone()).collect();
    let prune = !needed.is_empty();
    let mut out = Vec::with_capacity(slice.len() / 4);
    let mut batch: Vec<Row> = Vec::with_capacity(512);
    for (_, value) in slice {
        let Some(bytes) = value else { continue };
        let mut s = bytes.as_slice();
        // 投影剪枝:未标记列跳过字节不物化(槽位 Null 占位)
        let mut row = if prune {
            common::codec::decode_row_partial(&mut s, needed)?
        } else {
            common::codec::decode_row(&mut s)?
        };
        // schema 前缀兼容
        if row.len() < table.columns.len() {
            row.resize(table.columns.len(), ScalarValue::Null);
        } else if row.len() > table.columns.len() {
            row.truncate(table.columns.len());
        }
        batch.push(row);
        if batch.len() >= 512 {
            let chunk = DataChunk::from_rows(&dtypes, &batch)?;
            let keep = match pred {
                Some(p) => {
                    let mask = p.eval(&chunk)?;
                    (0..batch.len())
                        .map(|i| !mask.null_at(i) && mask.bool_values()[i])
                        .collect::<Vec<_>>()
                }
                None => vec![true; batch.len()],
            };
            for i in 0..batch.len() {
                if keep[i] {
                    out.push(batch[i].clone());
                }
            }
            batch.clear();
        }
    }
    if !batch.is_empty() {
        let chunk = DataChunk::from_rows(&dtypes, &batch)?;
        let keep = match pred {
            Some(p) => {
                let mask = p.eval(&chunk)?;
                (0..batch.len())
                    .map(|i| !mask.null_at(i) && mask.bool_values()[i])
                    .collect::<Vec<_>>()
            }
            None => vec![true; batch.len()],
        };
        for i in 0..batch.len() {
            if keep[i] {
                out.push(batch[i].clone());
            }
        }
    }
    Ok(out)
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
    ap: Option<Arc<ApStore>>,
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

    /// 谓词下推 + 投影剪枝:解码期过滤与部分物化(并行;未命中行/未引用列
    /// 都不进物化)。needed 为空切片 = 全列需要。
    fn scan_table_filtered(
        &self,
        table: &TableSchema,
        pred: Option<&exec::expr::Expr>,
        needed: &[bool],
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
        let prune = !needed.is_empty();
        let pred_cloned = pred.cloned();
        let parts = exec::parallel::scope_map_try(&kv, self.threads, |slice| {
            decode_filtered_slice(
                slice,
                table,
                pred_cloned.as_ref(),
                if prune { needed } else { &[] },
            )
        })?;
        Ok(parts.concat())
    }

    /// **AP 列存路径**:返回拼接语义的多块列向量(segment 块 + memtable
    /// delta 块)。列存副本的目的是加速 SST 读取 —— segment 缓存保持纯净
    /// (memtable 非空时也**不标量化**),memtable 增量在查询时转向量化:
    /// 解码成独立 delta 块,被覆盖/删除的 segment 行用键成员判定整行剔除
    /// (`Vector::select`,向量化压缩)。
    /// 门控:事务内读回退行存(overlay 不含事务缓冲,保读己之写)。
    fn scan_columns(&self, table: &TableSchema) -> Result<Option<Vec<Vec<Vector>>>> {
        let Some(ap) = &self.ap else {
            return Ok(None);
        };
        // 类型门控:非标量列(Date/Binary/Array/Vector 中仅 Date 支持段内编解码
        // —— codec 全类型都支持,但保留 Binary/Array/Vector 检查以防旧段)
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
        let Some((generation, seg_max_seq)) = ap.segment_info(table.id) else {
            return Ok(None); // 无 segment → 行存路径(首次 flush 后才有)
        };
        // 取/建解码缓存(段文件 → 列向量;gen 变化自动失效)
        let cache = match ap.cache(table.id) {
            Some(c) if c.generation == generation => c,
            _ => {
                let path =
                    columnar_file::segment_path(&ap.dir(), table.id, generation, seg_max_seq);
                let seg = match columnar_file::read_segment(&path, table.id) {
                    Ok(Some(seg)) => seg,
                    Ok(None) => return Ok(None), // 段缺失(被删/代次超前)→ 行存回退
                    Err(e) => {
                        // 段损坏:派生数据不阻塞查询 → 注销代次 + 行存回退,
                        // 下次 flush 重建新段(文件按代次整份替换)。
                        warn!(table = %table.name, error = %e, "columnar segment corrupt; row-store fallback");
                        ap.remove(table.id);
                        return Ok(None);
                    }
                };
                // 列主序组装:cell 解码 → 每列 Vector(null 经 validity)
                let mut col_vals: Vec<Vec<ScalarValue>> =
                    vec![Vec::with_capacity(seg.rows.len()); table.columns.len()];
                let mut keys = Vec::with_capacity(seg.rows.len());
                for r in &seg.rows {
                    keys.push(r.key.clone());
                    for (c, cell) in r.row.iter().enumerate() {
                        col_vals[c].push(cell.clone());
                    }
                }
                let cols = table
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(c, desc)| Vector::from_scalars(desc.ty.clone(), col_vals[c].clone()))
                    .collect::<Result<Vec<_>>>()?;
                let c = Arc::new(ApCache {
                    generation: generation,
                    keys,
                    cols,
                    rows: seg.rows.len(),
                });
                ap.set_cache(table.id, c.clone());
                c
            }
        };
        // memtable 增量 → delta 块(查询时向量化;memtable seq 恒大于已
        // flush seq → delta 块中的版本恒为最新)
        let (from, to) = row_range(table.id);
        let mt = self.lsm.scan_memtable(&from, &to, self.read_ts)?;
        if mt.is_empty() {
            return Ok(Some(vec![cache.cols.clone()]));
        }
        // 影子键集:memtable 覆盖/删除的行键(update + tombstone)
        let shadow: std::collections::HashSet<&[u8]> =
            mt.iter().map(|(k, _, _)| k.as_slice()).collect();
        // delta 块:memtable 中存活的行(update 新值 + 新插入;墓碑只进影子集)
        let mut dvals: Vec<Vec<ScalarValue>> = vec![Vec::new(); table.columns.len()];
        for (k, _seq, v) in &mt {
            let Some(bytes) = v else { continue };
            let _ = k;
            let mut s = bytes.as_slice();
            let mut row = common::codec::decode_row(&mut s)?;
            if row.len() < table.columns.len() {
                row.resize(table.columns.len(), ScalarValue::Null);
            } else if row.len() > table.columns.len() {
                row.truncate(table.columns.len());
            }
            for (c, cell) in row.iter().enumerate() {
                dvals[c].push(cell.clone());
            }
        }
        let mut chunks: Vec<Vec<Vector>> = Vec::with_capacity(2);
        // segment 块:无影子键 → 缓存原样复用(零拷贝);有 → 向量化剔除
        let seg_shadowed = cache.keys.iter().any(|k| shadow.contains(k.as_slice()));
        if cache.rows > 0 {
            if seg_shadowed {
                let keep: Vec<bool> = cache
                    .keys
                    .iter()
                    .map(|k| !shadow.contains(k.as_slice()))
                    .collect();
                let cols = cache
                    .cols
                    .iter()
                    .map(|v| v.select(&keep))
                    .collect::<Result<Vec<_>>>()?;
                if cols.first().map(|v| v.len()).unwrap_or(0) > 0 {
                    chunks.push(cols);
                }
            } else {
                chunks.push(cache.cols.clone());
            }
        }
        if dvals.first().map(|v| !v.is_empty()).unwrap_or(false) {
            let cols = table
                .columns
                .iter()
                .enumerate()
                .map(|(c, desc)| Vector::from_scalars(desc.ty.clone(), dvals[c].clone()))
                .collect::<Result<Vec<_>>>()?;
            chunks.push(cols);
        }
        Ok(Some(chunks))
    }
}

/// AP 列存段存储:segment 目录 + 每表代次 + 解码列缓存。
/// 列存副本只覆盖 SST 数据;memtable 增量由读路径以 overlay 合并。
pub(crate) struct ApStore {
    dir: std::path::PathBuf,
    /// table_id → (generation, segment max_seq)。缺失 = 无段(行存回退)。
    gens: Mutex<HashMap<u64, (u64, u64)>>,
    caches: Mutex<HashMap<u64, Arc<ApCache>>>,
}

impl ApStore {
    pub fn dir(&self) -> &std::path::Path {
        &self.dir
    }

    /// 当前段 (generation, max_seq);无段返回 None。
    pub fn segment_info(&self, table_id: u64) -> Option<(u64, u64)> {
        self.gens.lock().get(&table_id).copied()
    }

    pub fn set_gen(&self, table_id: u64, generation: u64, max_seq: u64) {
        self.gens.lock().insert(table_id, (generation, max_seq));
    }

    pub fn remove(&self, table_id: u64) {
        self.gens.lock().remove(&table_id);
        self.caches.lock().remove(&table_id);
    }

    /// 删除某表全部 segment 文件并清空代次/缓存(ALTER 失效 / DROP 清理)。
    /// 文件必须当场删除:残留旧代文件会让重启后的代次发现恢复出过期 schema。
    pub fn remove_table(&self, table_id: u64) {
        self.remove_files_matching(table_id, |_| false);
        self.remove(table_id);
    }

    /// 删除某表满足条件的 segment 文件(keep 返回 true 的保留)。
    pub(crate) fn remove_files_matching(
        &self,
        table_id: u64,
        keep: impl Fn(&str) -> bool,
    ) {
        let prefix = format!("{table_id}.");
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with(&prefix) && name.ends_with(".seg") && !keep(&name) {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }

    pub fn cache(&self, table_id: u64) -> Option<Arc<ApCache>> {
        self.caches.lock().get(&table_id).cloned()
    }

    pub fn set_cache(&self, table_id: u64, cache: Arc<ApCache>) {
        self.caches.lock().insert(table_id, cache);
    }

    pub fn invalidate_cache(&self, table_id: u64) {
        self.caches.lock().remove(&table_id);
    }
}

/// 解码后的列缓存(一段 segment 的列向量 + 行键)。
pub(crate) struct ApCache {
    pub generation: u64,
    /// 行键(与列向量行序一致;delta 影子剔除用)
    pub keys: Vec<Vec<u8>>,
    pub cols: Vec<Vector>,
    pub rows: usize,
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
    ap: Arc<ApStore>,
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

        // AP 列存段:磁盘上的 segment 只覆盖 SST 数据(memtable 由读路径
        // overlay 合并)。打开时按文件名恢复每表 (代次, max_seq),解码缓存
        // 在首次 AP 查询时惰性加载。
        // 崩溃兜底:若 crash 落在 "LSM flush 完成 → segment 重建" 之间,SST
        // 会有比 segment 更新的数据(max_seq 比较,文件名自带,零 IO 检测)
        // → 该表代次视为 0,AP 回退行存,下次 flush 重建后恢复。
        let columnar_dir = config.storage.data_dir.join("columnar");
        std::fs::create_dir_all(&columnar_dir)?;
        let mut gens: HashMap<u64, (u64, u64)> = HashMap::new();
        if let Ok(entries) = std::fs::read_dir(&columnar_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                let Some((tid, generation, max_seq)) =
                    storage::columnar_file::parse_segment_file_name(name)
                else {
                    continue;
                };
                if !catalog.get_table_by_id(tid).is_some() {
                    continue; // 无主 segment(表已删)→ 留待覆盖
                }
                let e = gens.entry(tid).or_insert((0, 0));
                if generation > e.0 {
                    *e = (generation, max_seq);
                }
            }
        }
        // 陈旧检测(两段式):段名 max_seq 只含行键 seq,而 SstMeta.max_seq
        // 跨全部键空间(catalog/stats 与行数据同 SST)→ 直接比较会误报。
        // ① 廉价筛:SST 与该表行键范围重叠且 max_seq > 段 max_seq → 可疑;
        // ② 精确验证:只扫该表行键,取可见版本最大 seq 比较。
        let sst_metas = lsm.sst_metas();
        let stale_tids: Vec<u64> = gens
            .iter()
            .filter(|(tid, info)| {
                let (from, to) = storage::table::row_range(**tid);
                sst_metas
                    .iter()
                    .any(|m| m.overlaps_range(&from, &to) && m.max_seq > info.1)
            })
            .map(|(tid, _)| *tid)
            .filter(|tid| {
                // 精确验证:行键范围的最大可见 seq 仍 > 段 max_seq 才算真陈旧
                let (from, to) = storage::table::row_range(*tid);
                let row_max = lsm
                    .scan(&from, &to)
                    .map(|entries| entries.iter().map(|(_, seq, _)| *seq).max().unwrap_or(0))
                    .unwrap_or(0);
                let seg_max = gens.get(tid).map(|(_, m)| *m).unwrap_or(0);
                row_max > seg_max
            })
            .collect();
        for tid in stale_tids {
            gens.remove(&tid); // 无段 → 行存回退,下次 flush 重建
            warn!(
                table_id = tid,
                "columnar segment stale vs sst (crash between flush and rebuild?); row-store fallback until next flush"
            );
        }
        let ap = Arc::new(ApStore {
            dir: columnar_dir,
            gens: Mutex::new(gens),
            caches: Mutex::new(HashMap::new()),
        });
        let engine = Arc::new(Self {
            config,
            memory,
            catalog,
            lsm,
            ap,
            stats,
            active_snapshots: Mutex::new(BTreeSet::new()),
            commit_lock: Mutex::new(()),
            _restored_tables: AtomicU64::new(restored),
        });
        // flush 钩子:auto-flush(内存满)与手动 FLUSH 共用 → SST 一有新数据,
        // 列存 segment 立即跟进。重建失败不致命(AP 回退行存 + 陈旧检测兜底)。
        let hook_engine = engine.clone();
        engine.lsm.set_flush_hook(Some(Arc::new(move || {
            if let Err(e) = hook_engine.rebuild_columnar_segments() {
                warn!(error = %e, "columnar segment rebuild after flush failed; row-store fallback");
            }
        })));
        Ok(engine)
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
            // 事务内回退行存:segment 只覆盖已 flush 的 SST 数据,看不到本事务写缓冲
            ap: columnar_ok.then_some(self.ap.clone()),
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
    /// 列存 segment 重建由 LSM flush 钩子完成 —— auto-flush(内存满)与手动
    /// flush 走同一条路径,保证 SST 有新数据时 segment 必然跟进。
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

    /// 将全部表的 LSM 可见数据重建为列存 segment(LSM flush 钩子触发,
    /// 与 LSM 写串行 —— 钩子在 write_lock 内调用)。
    /// segment = SST 全量真值;代次 +1,旧代文件删除,解码缓存失效。
    fn rebuild_columnar_segments(&self) -> Result<()> {
        let dir = self.ap.dir();
        std::fs::create_dir_all(&dir)?;
        for schema in self.catalog.list_tables() {
            let (from, to) = row_range(schema.id);
            let Ok(entries) = self.lsm.scan(&from, &to) else {
                continue;
            };
            let mut seg_rows: Vec<SegmentRow> = Vec::with_capacity(entries.len());
            let mut max_seq = 0u64;
            for (key, seq, value) in entries {
                let Some(bytes) = value else { continue };
                let mut slice = bytes.as_slice();
                let mut row = common::codec::decode_row(&mut slice)?;
                if row.len() < schema.columns.len() {
                    row.resize(schema.columns.len(), ScalarValue::Null);
                } else if row.len() > schema.columns.len() {
                    row.truncate(schema.columns.len());
                }
                max_seq = max_seq.max(seq);
                seg_rows.push(SegmentRow { key, seq, row });
            }
            seg_rows.sort_by(|a, b| a.key.cmp(&b.key));
            let generation = self.ap.segment_info(schema.id).map_or(1, |(g, _)| g + 1);
            let path = columnar_file::segment_path(&dir, schema.id, generation, max_seq);
            columnar_file::write_segment(&path, schema.id, &seg_rows, &schema)?;
            // 清理旧代文件(保留刚写的这份)
            let new_name = path.file_name().and_then(|n| n.to_str()).map(String::from);
            self.ap
                .remove_files_matching(schema.id, |n| new_name.as_deref() == Some(n));
            self.ap.set_gen(schema.id, generation, max_seq);
            self.ap.invalidate_cache(schema.id);
            debug!(table = %schema.name, rows = seg_rows.len(), "columnar segment rebuilt");
        }
        Ok(())
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
        {
            let _guard = self.commit_lock.lock();
            self.lsm.write_batch(&batch_refs)?;
        }

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

        // 4. 列存 segment 失效:旧 segment 列数与 schema 不符,直接删文件 +
        // 清代次(下次 flush 时按新 schema 全量重建;在此之前走行存回退)
        self.ap.remove_table(next.id);
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
        {
            let _guard = self.commit_lock.lock();
            self.lsm.write_batch(&batch)?;
        }
        self.stats.remove(dropped.id);
        // 删除列存 segment 文件 + 代次/缓存
        self.ap.remove_table(dropped.id);
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
