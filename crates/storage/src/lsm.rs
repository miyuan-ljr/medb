//! LsmStore 门面:行存主库。
//!
//! 写路径:单写者锁 → 分配 seq → WAL 帧落盘 → memtable 插入;
//! memtable 限额不足时自动 flush 后重试一次。
//!
//! 读路径:memtable → 冻结表 → L0(id 降序 = 新→旧)→ L1。
//! 任一层返回 Found/Tombstone 即短路,NotFound 继续下沉。
//!
//! 恢复:重放 WAL 重建 memtable;扫 sst/ 目录按文件名 `{id:020}.{level}.sst`
//! 重建清单(两个来源恰好覆盖"已 flush 未丢 / 未 flush 在 WAL"的划分)。
//!
//! v1 已知简化(见 docs/design.md 路线图):扫描为批内物化而非流式归并、
//! 单 L1 文件全量压缩、manifest 不落盘。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::config::StorageConfig;
use common::error::{ErrorCode, Result};
use memory::MemoryTracker;
use parking_lot::{Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::compaction::{CompactionStrategy, CompactionTask, LeveledStrategy};
use crate::key::{InternalKey, KeyKind, encode_internal_key};
use crate::manifest::Manifest;
use crate::memtable::{Lookup, Memtable, ScanEntry};
use crate::sstable::{SstMeta, SstReader, SstWriter, SstWriterOptions};
use crate::wal::{WalEntry, WalOptions, WalWriter, replay};

#[derive(Debug, Clone)]
pub struct LsmOptions {
    pub write_buffer_bytes: u64,
    pub wal_segment_bytes: u64,
    pub wal_sync: bool,
    pub sst_block_size: usize,
    pub bloom_bits_per_key: usize,
    pub l0_compaction_threshold: usize,
}

impl From<&StorageConfig> for LsmOptions {
    fn from(c: &StorageConfig) -> Self {
        Self {
            write_buffer_bytes: c.write_buffer_bytes,
            wal_segment_bytes: c.wal_segment_bytes,
            wal_sync: false,
            sst_block_size: c.sst_block_size,
            bloom_bits_per_key: c.bloom_bits_per_key,
            l0_compaction_threshold: c.l0_compaction_threshold,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStats {
    pub memtable_bytes: u64,
    pub imm_count: usize,
    pub level_files: Vec<(u32, usize, u64)>,
}

pub struct LsmStore {
    dir: PathBuf,
    opts: LsmOptions,
    tracker: Arc<MemoryTracker>,
    write_lock: Mutex<()>,
    mem: RwLock<Arc<Memtable>>,
    imm: RwLock<Vec<Arc<Memtable>>>,
    manifest: Manifest,
    wal: Mutex<WalWriter>,
    last_seq: AtomicU64,
    /// GC 水位:压缩时只回收 seq ≤ 水位且被更新的可见版本遮蔽的旧版本。
    /// 由上层(Engine)按活跃快照集合推入;u64::MAX 表示无活跃快照。
    gc_watermark: AtomicU64,
    compaction: Box<dyn CompactionStrategy>,
    readers: Mutex<HashMap<u64, Arc<SstReader>>>,
    /// flush 成功后的通知钩子(auto-flush 与手动 flush 共用)。
    /// 上层(Engine)借此把 SST 新数据写入列存 segment。在 write_lock 内触发,
    /// 钩子体内不得再进入本 store 的写路径。
    flush_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl LsmStore {
    pub fn open(
        dir: &Path,
        opts: LsmOptions,
        parent_tracker: &Arc<MemoryTracker>,
    ) -> Result<Arc<Self>> {
        std::fs::create_dir_all(dir)?;
        let wal_dir = dir.join("wal");
        let sst_dir = dir.join("sst");
        std::fs::create_dir_all(&wal_dir)?;
        std::fs::create_dir_all(&sst_dir)?;

        let store_tracker = parent_tracker.new_child("storage.lsm", None);
        let mem_tracker = store_tracker.new_child("memtable", Some(opts.write_buffer_bytes));

        // 1. WAL 重放
        let memtable = Arc::new(Memtable::new(mem_tracker));
        let mut last_seq = 0u64;
        let replayed = replay(&wal_dir, |entry| {
            last_seq = last_seq.max(entry.seq);
            memtable.put_untracked(&entry.user_key, entry.seq, entry.value);
            Ok(())
        })?;
        if replayed > 0 {
            info!(
                entries = replayed,
                max_seq = last_seq,
                "wal replayed into memtable"
            );
        }

        // 2. 从目录重建 SST 清单
        let manifest = Manifest::new(next_file_id_from_dir(&sst_dir)?);
        let (mut known_ids, sst_max_seq) = rebuild_manifest_from_dir(&sst_dir, &manifest)?;
        // 序号水位必须同时覆盖 WAL 与 SST:flush 后 WAL 为空,重启时
        // 只看 WAL 会让 last_seq 回退 → row_id/seq 复用 → 覆盖历史数据。
        last_seq = last_seq.max(sst_max_seq);
        if sst_max_seq > 0 {
            debug!(sst_max_seq, "seq watermark restored from sst files");
        }

        // 3. WAL writer
        let wal = WalWriter::open(WalOptions {
            dir: wal_dir,
            segment_bytes: opts.wal_segment_bytes,
            sync: opts.wal_sync,
        })?;

        let compaction = LeveledStrategy {
            l0_threshold: opts.l0_compaction_threshold,
        };
        let store = Arc::new(Self {
            dir: dir.to_path_buf(),
            opts,
            tracker: store_tracker,
            write_lock: Mutex::new(()),
            mem: RwLock::new(memtable),
            imm: RwLock::new(Vec::new()),
            manifest,
            wal: Mutex::new(wal),
            last_seq: AtomicU64::new(last_seq),
            gc_watermark: AtomicU64::new(u64::MAX),
            compaction: Box::new(compaction),
            readers: Mutex::new(known_ids.drain().collect()),
            flush_hook: Mutex::new(None),
        });
        Ok(store)
    }

    // ---------- 写路径 ----------

    pub fn put(&self, user_key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[(user_key.to_vec(), Some(value.to_vec()))])
    }

    pub fn delete(&self, user_key: &[u8]) -> Result<()> {
        self.write_batch(&[(user_key.to_vec(), None)])
    }

    /// 原子小批:一个 seq 段、一条 WAL 记录、memtable 依次插入。
    pub fn write_batch(&self, entries: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let _writer = self.write_lock.lock();

        let base = self
            .last_seq
            .fetch_add(entries.len() as u64, Ordering::Relaxed)
            + 1;
        let wal_entries: Vec<WalEntry> = entries
            .iter()
            .enumerate()
            .map(|(i, (k, v))| WalEntry {
                seq: base + i as u64,
                user_key: k.clone(),
                value: v.clone(),
            })
            .collect();

        // 先 WAL 后 memtable:崩溃时宁可多恢复一条已写 WAL,不可丢已确认写
        self.wal.lock().append(&wal_entries)?;

        let mut retry_after_flush = false;
        for e in &wal_entries {
            let mem = self.mem.read().clone();
            if let Err(err) = mem.put(&e.user_key, e.seq, e.value.clone()) {
                if err.code() == ErrorCode::MemoryLimitExceeded && !retry_after_flush {
                    debug!("memtable full, flushing before retry");
                    retry_after_flush = true;
                    self.flush_locked()?;
                    self.mem.read().put(&e.user_key, e.seq, e.value.clone())?;
                } else {
                    return Err(err);
                }
            }
        }
        // flush 后顺手检查是否需要压缩
        if retry_after_flush {
            self.compact_locked_if_needed()?;
        }
        Ok(())
    }

    // ---------- 读路径 ----------

    /// 点查(read_ts 快照;u64::MAX = 最新)。
    pub fn get_at(&self, user_key: &[u8], read_ts: u64) -> Result<Lookup> {
        // 1. 活跃 memtable
        let lookup = self.mem.read().get_at(user_key, read_ts);
        if lookup != Lookup::NotFound {
            return Ok(lookup);
        }
        // 2. 冻结表(新→旧)
        for imm in self.imm.read().iter() {
            let lookup = imm.get_at(user_key, read_ts);
            if lookup != Lookup::NotFound {
                return Ok(lookup);
            }
        }
        // 3. 磁盘:L0 新→旧,再 L1
        let snapshot = self.manifest.snapshot();
        for level in &snapshot.levels {
            for meta in level {
                if !meta.covers(user_key) {
                    continue;
                }
                let reader = self.reader_for(meta)?;
                match reader.get_at(user_key, read_ts)? {
                    Lookup::NotFound => continue,
                    hit => return Ok(hit),
                }
            }
        }
        Ok(Lookup::NotFound)
    }

    /// 最新版本点查(read_ts = MAX)。
    pub fn get(&self, user_key: &[u8]) -> Result<Lookup> {
        self.get_at(user_key, u64::MAX)
    }

    /// 范围扫描 [from, to):每个 user_key 返回 read_ts 快照下的最新值;
    /// 墓碑的键不出现。
    pub fn scan_at(&self, from: &[u8], to: &[u8], read_ts: u64) -> Result<Vec<ScanEntry>> {
        if from >= to {
            return Ok(Vec::new());
        }
        let mut sources: Vec<Vec<(InternalKey, Option<Vec<u8>>)>> = Vec::new();

        // memtable + 冻结表(活跃表优先级最高 → 排在前面)
        let mem = self.mem.read().clone();
        let imms = self.imm.read().clone();
        let mut memtables = vec![mem];
        memtables.extend(imms);
        for mt in &memtables {
            let entries: Vec<(InternalKey, Option<Vec<u8>>)> = mt
                .scan_all()
                .into_iter()
                .filter(|(k, _, _)| k.as_slice() >= from && k.as_slice() < to)
                .map(|(k, seq, v)| {
                    let kind = if v.is_some() {
                        KeyKind::Put
                    } else {
                        KeyKind::Delete
                    };
                    (encode_internal_key(&k, seq, kind), v)
                })
                .collect();
            if !entries.is_empty() {
                sources.push(entries);
            }
        }

        // SST:同层内按覆盖范围拉取(L0 新→旧已由 manifest 顺序保证)
        let snapshot = self.manifest.snapshot();
        for level in &snapshot.levels {
            for meta in level {
                if !meta.overlaps_range(from, to) {
                    continue;
                }
                let reader = self.reader_for(meta)?;
                let entries = reader.scan(from, to)?;
                if !entries.is_empty() {
                    sources.push(entries);
                }
            }
        }

        Ok(merge_visible_per_key(sources, read_ts))
    }

    /// 最新版本扫描(read_ts = MAX)。
    pub fn scan(&self, from: &[u8], to: &[u8]) -> Result<Vec<ScanEntry>> {
        self.scan_at(from, to, u64::MAX)
    }

    /// **仅 memtable**(活跃 + 冻结表)的合并扫描:AP 列存路径用它取
    /// "尚未 flush 进 segment 的增量"(segment 只覆盖 SST 数据)。
    pub fn scan_memtable(&self, from: &[u8], to: &[u8], ts: u64) -> Result<Vec<ScanEntry>> {
        if from >= to {
            return Ok(Vec::new());
        }
        let mem = self.mem.read().clone();
        let imms = self.imm.read().clone();
        let mut memtables = vec![mem];
        memtables.extend(imms);
        let mut sources: Vec<Vec<(InternalKey, Option<Vec<u8>>)>> = Vec::new();
        for mt in &memtables {
            let entries: Vec<(InternalKey, Option<Vec<u8>>)> = mt
                .scan_all()
                .into_iter()
                .filter(|(k, _, _)| k.as_slice() >= from && k.as_slice() < to)
                .map(|(k, seq, v)| {
                    let kind = if v.is_some() {
                        KeyKind::Put
                    } else {
                        KeyKind::Delete
                    };
                    (encode_internal_key(&k, seq, kind), v)
                })
                .collect();
            if !entries.is_empty() {
                sources.push(entries);
            }
        }
        Ok(merge_visible_per_key(sources, ts))
    }

    // ---------- flush / compaction ----------

    /// 冻结当前 memtable 并写成 L0 SST,重置 WAL。可从外部手动触发。
    pub fn flush(&self) -> Result<bool> {
        let _writer = self.write_lock.lock();
        self.flush_locked()
    }

    fn flush_locked(&self) -> Result<bool> {
        let old = {
            let mut mem = self.mem.write();
            let old = mem.clone();
            *mem = Arc::new(Memtable::new(
                self.tracker
                    .new_child("memtable", Some(self.opts.write_buffer_bytes)),
            ));
            old
        };
        if old.is_empty() {
            return Ok(false);
        }
        self.imm.write().insert(0, old.clone());

        let entries = old.scan_all();
        let id = self.manifest.next_file_id();
        let meta = self.write_sst(&entries, id, 0)?;

        // SST 落盘成功后才切换内存状态并丢 WAL(顺序保证不丢数据)
        self.manifest.update(|m| m.add_l0(meta.clone()));
        self.compact_locked_if_needed()?;
        // 钩子在冻结表(imm)移除前触发:重建列存期间 imm 仍可读,
        // AP 读路径 "segment + memtable overlay" 全程无缺口。
        if let Some(hook) = self.flush_hook.lock().clone() {
            hook();
        }
        self.imm.write().retain(|m| !Arc::ptr_eq(m, &old));
        old.release_accounting();
        self.wal.lock().reset()?;
        info!(
            id,
            entries = entries.len(),
            size = meta.file_size,
            "flushed memtable to L0"
        );
        Ok(true)
    }

    /// 注册 flush 完成钩子(覆盖式;传 None 注销)。
    pub fn set_flush_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *self.flush_hook.lock() = hook;
    }

    /// 当前全部 SST 元数据(打开期做列存段新旧比对用)。
    pub fn sst_metas(&self) -> Vec<crate::sstable::SstMeta> {
        let snapshot = self.manifest.snapshot();
        let mut metas = Vec::new();
        for level in &snapshot.levels {
            metas.extend(level.iter().cloned());
        }
        metas
    }

    fn write_sst(&self, entries: &[ScanEntry], id: u64, level: u32) -> Result<SstMeta> {
        let path = self.sst_tmp_path(id);
        let file = std::fs::File::create(&path)?;
        let mut writer = SstWriter::new(
            std::io::BufWriter::new(file),
            SstWriterOptions {
                block_size: self.opts.sst_block_size,
                bloom_bits_per_key: self.opts.bloom_bits_per_key,
            },
        );
        for (user_key, seq, value) in entries {
            let kind = if value.is_some() {
                KeyKind::Put
            } else {
                KeyKind::Delete
            };
            writer.add(&encode_internal_key(user_key, *seq, kind), value.as_deref())?;
        }
        let (_out, meta) = writer.finish(id, level)?;
        // BufWriter 已在 finish 中 flush
        // 原子改名:tmp → 最终名(读者只认最终名)
        let final_path = self.sst_path(id, level);
        std::fs::rename(&path, &final_path)?;
        Ok(meta)
    }

    fn compact_locked_if_needed(&self) -> Result<bool> {
        let snapshot = self.manifest.snapshot();
        let Some(task) = self.compaction.pick(&snapshot) else {
            return Ok(false);
        };
        self.execute_compaction(&task)?;
        Ok(true)
    }

    /// 全量合并:收集全部输入条目 → 按内部键排序 → 快照感知回收:
    /// - seq > GC 水位的版本全部保留(可能仍被活跃快照看到);
    /// - 水位以下每个 user_key 只保留最新可见版本,若是墓碑则连同旧版本全部丢弃
    ///   (无更低层级,该键对所有活跃快照都不存在);
    /// → 写单个 L1 文件。
    fn execute_compaction(&self, task: &CompactionTask) -> Result<()> {
        let mut merged: Vec<(InternalKey, Option<Vec<u8>>)> = Vec::new();
        let snapshot = self.manifest.snapshot();
        for level in &snapshot.levels {
            for meta in level {
                if !task.input_ids.contains(&meta.id) {
                    continue;
                }
                let reader = self.reader_for(meta)?;
                let mut all = reader.scan(&[], &[0xffu8; 64])?;
                merged.append(&mut all);
            }
        }
        merged.sort_by(|a, b| a.0.cmp(&b.0));

        let watermark = self.gc_watermark.load(Ordering::Relaxed);
        let mut entries: Vec<ScanEntry> = Vec::new();
        let mut i = 0usize;
        while i < merged.len() {
            let Ok(decoded) = crate::key::decode_internal_key(&merged[i].0.0) else {
                i += 1;
                continue;
            };
            let user = decoded.user_key.to_vec();
            // 保留水位以上的全部版本
            let mut j = i;
            while j < merged.len() {
                let Ok(d) = crate::key::decode_internal_key(&merged[j].0.0) else {
                    break;
                };
                if d.user_key != user || d.seq <= watermark {
                    break;
                }
                if d.kind == KeyKind::Put {
                    if let Some(v) = &merged[j].1 {
                        entries.push((user.clone(), d.seq, Some(v.clone())));
                    }
                }
                j += 1;
            }
            // 水位以下最新可见版本:Put 保留,墓碑连同全部旧版本丢弃
            if j < merged.len() {
                let Ok(d) = crate::key::decode_internal_key(&merged[j].0.0) else {
                    i = j + 1;
                    continue;
                };
                if d.user_key == user && d.seq <= watermark && d.kind == KeyKind::Put {
                    if let Some(v) = &merged[j].1 {
                        entries.push((user.clone(), d.seq, Some(v.clone())));
                    }
                }
                // 跳过该 user 剩余旧版本
                while j < merged.len() {
                    let Ok(d2) = crate::key::decode_internal_key(&merged[j].0.0) else {
                        break;
                    };
                    if d2.user_key != user {
                        break;
                    }
                    j += 1;
                }
            }
            i = j.max(i + 1);
        }

        let id = self.manifest.next_file_id();
        let meta = self.write_sst(&entries, id, task.target_level)?;

        // 新文件先进清单,再删旧文件(崩溃窗口最多留下重复数据,不丢数据)
        self.manifest
            .update(|m| m.replace(&task.input_ids, meta.clone(), task.target_level as usize));
        for old_id in &task.input_ids {
            if let Some(reader) = self.readers.lock().remove(old_id) {
                let _ = reader; // 释放整文件内存缓存
            }
            for level in [0u32, 1u32] {
                let path = self.sst_path(*old_id, level);
                if path.exists() {
                    if let Err(e) = std::fs::remove_file(&path) {
                        warn!(id = old_id, error = %e, "failed to remove compacted sst");
                    }
                }
            }
        }
        info!(
            inputs = task.input_ids.len(),
            out_id = id,
            entries = entries.len(),
            "compaction done"
        );
        Ok(())
    }

    // ---------- 辅助 ----------

    fn reader_for(&self, meta: &SstMeta) -> Result<Arc<SstReader>> {
        let mut cache = self.readers.lock();
        if let Some(r) = cache.get(&meta.id) {
            return Ok(r.clone());
        }
        let path = self.sst_path(meta.id, meta.level);
        let reader = Arc::new(SstReader::open(&path, meta.id, meta.level)?);
        cache.insert(meta.id, reader.clone());
        Ok(reader)
    }

    fn sst_path(&self, id: u64, level: u32) -> PathBuf {
        self.dir.join("sst").join(format!("{id:020}.{level}.sst"))
    }

    fn sst_tmp_path(&self, id: u64) -> PathBuf {
        self.dir.join("sst").join(format!("{id:020}.sst.tmp"))
    }

    pub fn stats(&self) -> LsmStats {
        let snapshot = self.manifest.snapshot();
        LsmStats {
            memtable_bytes: self.mem.read().approximate_memory_usage(),
            imm_count: self.imm.read().len(),
            level_files: snapshot
                .levels
                .iter()
                .enumerate()
                .map(|(i, l)| {
                    (
                        i as u32,
                        l.len(),
                        l.iter().map(|m| m.file_size).sum::<u64>(),
                    )
                })
                .collect(),
        }
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_seq.load(Ordering::Relaxed)
    }

    /// 分配 n 个连续序列号,返回起始(base+1 … base+n)。
    /// 上层用它派生 row_id 等需要"重启不复用"的单调 ID。
    pub fn alloc_sequences(&self, n: u64) -> u64 {
        self.last_seq.fetch_add(n, Ordering::Relaxed) + 1
    }

    /// 更新 GC 水位(压缩只回收水位以下被遮蔽的版本)。
    pub fn set_gc_watermark(&self, ts: u64) {
        self.gc_watermark.store(ts, Ordering::Relaxed);
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn flush_wal(&self) -> Result<()> {
        self.wal.lock().flush()
    }
}

/// k 路归并:每个来源已按内部键序;每个 user_key 取 read_ts 快照下可见的
/// 最新版本(第一个 seq ≤ read_ts 的版本),墓碑吞噬该键的输出。
/// v1 采用"多来源条目合并排序"策略;流式堆归并见路线图。
fn merge_visible_per_key(
    sources: Vec<Vec<(InternalKey, Option<Vec<u8>>)>>,
    read_ts: u64,
) -> Vec<ScanEntry> {
    let mut all: Vec<(InternalKey, Option<Vec<u8>>)> = sources.into_iter().flatten().collect();
    all.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out: Vec<ScanEntry> = Vec::new();
    let mut i = 0usize;
    while i < all.len() {
        let Ok(d0) = crate::key::decode_internal_key(&all[i].0.0) else {
            i += 1;
            continue;
        };
        let user = d0.user_key.to_vec();
        // 扫完该 user_key 的整个版本段(seq 降序),输出第一条 seq ≤ read_ts 的版本
        let mut j = i;
        let mut emitted = false;
        while j < all.len() {
            let Ok(d) = crate::key::decode_internal_key(&all[j].0.0) else {
                j += 1;
                continue;
            };
            if d.user_key != user {
                break;
            }
            if !emitted && d.seq <= read_ts {
                if d.kind == KeyKind::Put {
                    if let Some(v) = &all[j].1 {
                        out.push((user.clone(), d.seq, Some(v.clone())));
                    }
                }
                // 墓碑 → 整键隐藏,不再看更旧版本
                emitted = true;
            }
            j += 1;
        }
        i = j;
    }
    out
}

fn next_file_id_from_dir(sst_dir: &Path) -> Result<u64> {
    let mut max = 0u64;
    for entry in std::fs::read_dir(sst_dir)? {
        let path = entry?.path();
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            // {id:020}.{level} 或 {id:020}.sst.tmp
            let id_part = stem.split('.').next().unwrap_or("");
            if let Ok(v) = id_part.parse::<u64>() {
                max = max.max(v);
            }
        }
    }
    Ok(max + 1)
}

fn rebuild_manifest_from_dir(
    sst_dir: &Path,
    manifest: &Manifest,
) -> Result<(HashMap<u64, Arc<SstReader>>, u64)> {
    let mut readers = HashMap::new();
    let mut max_seq = 0u64;
    for entry in std::fs::read_dir(sst_dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        // 命名 {id:020}.{level}.sst;忽略 tmp 与不认识的文件
        let Some((id, level)) = parse_sst_name(name) else {
            if name.ends_with(".tmp") {
                let _ = std::fs::remove_file(&path); // 上次 flush 中断的残留
            }
            continue;
        };
        let reader = match SstReader::open(&path, id, level) {
            Ok(r) => Arc::new(r),
            Err(e) => {
                warn!(file = name, error = %e, "corrupt sst skipped during recovery");
                continue;
            }
        };
        let meta = reader.meta().clone();
        max_seq = max_seq.max(meta.max_seq);
        manifest.update(|m| {
            if level == 0 {
                m.add_l0(meta);
            } else if (level as usize) < m.levels.len() {
                m.levels[level as usize].push(meta);
            }
        });
        readers.insert(id, reader);
    }
    Ok((readers, max_seq))
}

fn parse_sst_name(name: &str) -> Option<(u64, u32)> {
    let rest = name.strip_suffix(".sst")?;
    let (id, level) = rest.split_once('.')?;
    let id = id.parse().ok()?;
    let level = level.parse().ok()?;
    Some((id, level))
}

#[cfg(test)]
mod tests {
    use super::*;
    use memory::MemoryTracker;

    fn open_store(dir: &Path) -> Arc<LsmStore> {
        let opts = LsmOptions {
            write_buffer_bytes: 1 << 20,
            wal_segment_bytes: 1 << 20,
            wal_sync: false,
            sst_block_size: 512,
            bloom_bits_per_key: 10,
            l0_compaction_threshold: 4,
        };
        let root = MemoryTracker::new_root("test", None);
        LsmStore::open(dir, opts, &root).unwrap()
    }

    #[test]
    fn put_get_delete_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path());
        store.put(b"a", b"1").unwrap();
        store.put(b"b", b"2").unwrap();
        assert_eq!(store.get(b"a").unwrap(), Lookup::Found(b"1".to_vec()));
        store.put(b"a", b"1x").unwrap();
        assert_eq!(store.get(b"a").unwrap(), Lookup::Found(b"1x".to_vec()));
        store.delete(b"b").unwrap();
        assert_eq!(store.get(b"b").unwrap(), Lookup::Tombstone);
        assert_eq!(store.get(b"zzz").unwrap(), Lookup::NotFound);
    }

    #[test]
    fn flush_then_read_from_sst() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path());
        store.put(b"k1", b"v1").unwrap();
        store.put(b"k2", b"v2").unwrap();
        store.delete(b"k2").unwrap();
        assert!(store.flush().unwrap());

        // flush 后 SST 路径可读,墓碑语义保留
        assert_eq!(store.get(b"k1").unwrap(), Lookup::Found(b"v1".to_vec()));
        assert_eq!(store.get(b"k2").unwrap(), Lookup::Tombstone);
        let stats = store.stats();
        assert_eq!(stats.level_files[0].1, 1, "L0 应有 1 个文件");
    }

    #[test]
    fn memtable_shadows_sst() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path());
        store.put(b"k", b"old").unwrap();
        store.flush().unwrap();
        store.put(b"k", b"new").unwrap();
        assert_eq!(store.get(b"k").unwrap(), Lookup::Found(b"new".to_vec()));

        let scanned = store.scan(b"", &[0xffu8; 16]).unwrap();
        assert_eq!(scanned, vec![(b"k".to_vec(), 2, Some(b"new".to_vec()))]);
    }

    #[test]
    fn scan_merges_ranges() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path());
        for i in 0..10u32 {
            store
                .put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        store.flush().unwrap();
        for i in 5..10u32 {
            store
                .put(format!("k{i}").as_bytes(), format!("nv{i}").as_bytes())
                .unwrap();
        }
        store.delete(b"k0").unwrap();

        let out = store.scan(b"k3", b"k7").unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(out[0], (b"k3".to_vec(), 4, Some(b"v3".to_vec())));
        assert_eq!(out[3], (b"k6".to_vec(), 12, Some(b"nv6".to_vec())));
        // k0 已删,全范围不含
        let all = store.scan(b"", &[0xffu8; 16]).unwrap();
        assert_eq!(all.len(), 9);
        assert!(all.iter().all(|(k, _, _)| k != b"k0"));
    }

    #[test]
    fn recovery_from_wal_and_sst() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        {
            let store = open_store(dir);
            store.put(b"persisted", b"v1").unwrap();
            store.flush().unwrap();
            store.put(b"unflushed", b"v2").unwrap();
            // drop 不 close 也无妨:WAL 已每次 flush
        }
        {
            let store = open_store(dir);
            assert_eq!(
                store.get(b"persisted").unwrap(),
                Lookup::Found(b"v1".to_vec()),
                "SST 重建路径"
            );
            assert_eq!(
                store.get(b"unflushed").unwrap(),
                Lookup::Found(b"v2".to_vec()),
                "WAL 重放路径"
            );
            assert_eq!(store.last_sequence(), 2);
        }
    }

    #[test]
    fn seq_watermark_survives_flush_only_restart() {
        // 全部数据已 flush(WAL 为空)时,重启后序列水位必须来自 SST 的
        // max_seq,否则 row_id 会从头复用、覆盖历史行。
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        {
            let store = open_store(dir);
            store.put(b"a", b"1").unwrap();
            store.put(b"b", b"2").unwrap();
            store.flush().unwrap();
        }
        {
            let store = open_store(dir);
            assert_eq!(store.last_sequence(), 2, "SST max_seq 必须恢复水位");
            let base = store.alloc_sequences(10);
            assert_eq!(base, 3);
            assert_eq!(store.last_sequence(), 12);
        }
    }

    #[test]
    fn compaction_merges_and_drops_old_versions() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let opts = LsmOptions {
            write_buffer_bytes: 1 << 20,
            wal_segment_bytes: 1 << 20,
            wal_sync: false,
            sst_block_size: 512,
            bloom_bits_per_key: 10,
            l0_compaction_threshold: 2,
        };
        let root = MemoryTracker::new_root("test", None);
        {
            let store = LsmStore::open(dir, opts.clone(), &root).unwrap();
            store.put(b"k", b"v1").unwrap();
            store.flush().unwrap();
            store.put(b"k", b"v2").unwrap();
            store.put(b"gone", b"x").unwrap();
            store.delete(b"gone").unwrap();
            store.flush().unwrap(); // L0 = 2 文件 → 触发压缩
            assert_eq!(store.stats().level_files[0].1, 0, "L0 应被合并清空");
            assert_eq!(store.stats().level_files[1].1, 1, "L1 应有 1 个产物");
            assert_eq!(store.get(b"k").unwrap(), Lookup::Found(b"v2".to_vec()));
            assert_eq!(
                store.get(b"gone").unwrap(),
                Lookup::NotFound,
                "墓碑应被丢弃"
            );
        }
        {
            // 压缩产物恢复后仍在
            let store = LsmStore::open(dir, opts, &root).unwrap();
            assert_eq!(store.get(b"k").unwrap(), Lookup::Found(b"v2".to_vec()));
        }
    }

    #[test]
    fn memtable_limit_triggers_auto_flush() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let opts = LsmOptions {
            write_buffer_bytes: 512, // 很小,几条即满
            wal_segment_bytes: 1 << 20,
            wal_sync: false,
            sst_block_size: 512,
            bloom_bits_per_key: 10,
            l0_compaction_threshold: 100, // 不触发压缩,隔离变量
        };
        let root = MemoryTracker::new_root("test", None);
        let store = LsmStore::open(dir, opts, &root).unwrap();
        for i in 0..50u32 {
            store
                .put(format!("key{i}").as_bytes(), vec![b'v'; 60].as_slice())
                .unwrap();
        }
        // 自动 flush 发生过(L0 有文件且 memtable 回到低位)
        assert!(store.stats().level_files[0].1 >= 1);
        for i in 0..50u32 {
            let key = format!("key{i}");
            let found = store.get(key.as_bytes()).unwrap();
            assert!(
                matches!(found, Lookup::Found(_)),
                "auto-flush 后所有键仍应可读: {key}"
            );
        }
    }

    #[test]
    fn read_snapshot_semantics_mvcc() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path());
        store.put(b"k", b"v1").unwrap(); // seq 1
        let ts1 = store.last_sequence(); // 1
        store.put(b"k", b"v2").unwrap(); // seq 2
        store.delete(b"gone").unwrap(); // seq 3
        let ts3 = store.last_sequence(); // 3

        // 快照 ts1:看到 v1
        assert_eq!(
            store.get_at(b"k", ts1).unwrap(),
            Lookup::Found(b"v1".to_vec())
        );
        // 最新:看到 v2
        assert_eq!(
            store.get_at(b"k", ts3).unwrap(),
            Lookup::Found(b"v2".to_vec())
        );
        // ts 之前 delete 不存在
        assert_eq!(store.get_at(b"gone", ts1).unwrap(), Lookup::NotFound);
        assert_eq!(store.get_at(b"gone", ts3).unwrap(), Lookup::Tombstone);

        // 扫描快照:flush 后 SST + memtable 跨介质可见性一致
        store.flush().unwrap();
        store.put(b"k", b"v3").unwrap(); // seq 4(memtable)
        let scanned_ts1 = store.scan_at(b"", &[0xffu8; 16], ts1).unwrap();
        assert_eq!(
            scanned_ts1,
            vec![(b"k".to_vec(), 1, Some(b"v1".to_vec()))],
            "ts1 快照应看到 SST 中的旧版本"
        );
        assert!(!scanned_ts1.iter().any(|(k, _, _)| k == b"gone"));
        let scanned_latest = store.scan_at(b"", &[0xffu8; 16], u64::MAX).unwrap();
        assert_eq!(scanned_latest.len(), 1);
        assert_eq!(scanned_latest[0].2, Some(b"v3".to_vec()));
    }

    #[test]
    fn compaction_respects_gc_watermark() {
        let tmp = tempfile::tempdir().unwrap();
        let opts = LsmOptions {
            write_buffer_bytes: 1 << 20,
            wal_segment_bytes: 1 << 20,
            wal_sync: false,
            sst_block_size: 512,
            bloom_bits_per_key: 10,
            l0_compaction_threshold: 2,
        };
        let root = MemoryTracker::new_root("test", None);
        let store = LsmStore::open(tmp.path(), opts.clone(), &root).unwrap();
        store.put(b"k", b"v1").unwrap(); // seq 1
        let ts1 = store.last_sequence();
        store.put(b"k", b"v2").unwrap(); // seq 2
        store.flush().unwrap(); // L0=1
        store.put(b"k", b"v3").unwrap(); // seq 3
        store.flush().unwrap(); // L0=2 → 压缩(水位 MAX:旧版本全部回收)

        // 未设水位(= MAX):只有最新可见版本保留
        assert_eq!(
            store.get_at(b"k", ts1).unwrap(),
            Lookup::NotFound,
            "无活跃快照时旧版本可被回收"
        );
        assert_eq!(store.get(b"k").unwrap(), Lookup::Found(b"v3".to_vec()));

        // 重来一次,这次在触发压缩前把水位钉住 → v1 必须保留
        let tmp2 = tempfile::tempdir().unwrap();
        let store2 = LsmStore::open(tmp2.path(), opts.clone(), &root).unwrap();
        store2.put(b"k", b"v1").unwrap();
        let pin = store2.last_sequence();
        store2.set_gc_watermark(pin);
        store2.put(b"k", b"v2").unwrap();
        store2.put(b"k", b"v3").unwrap();
        store2.put(b"x", b"x").unwrap();
        store2.flush().unwrap();
        store2.put(b"y", b"y").unwrap();
        store2.flush().unwrap(); // L0 ≥ 2 → 压缩发生(水位 = pin)
        assert_eq!(
            store2.get_at(b"k", pin).unwrap(),
            Lookup::Found(b"v1".to_vec()),
            "水位钉住的快照必须仍可见"
        );
        assert_eq!(store2.get(b"k").unwrap(), Lookup::Found(b"v3".to_vec()));
    }

    #[test]
    fn concurrent_writers_are_serialized() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path());
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let store = store.clone();
                std::thread::spawn(move || {
                    for i in 0..100u32 {
                        store
                            .put(
                                format!("t{t}-k{i}").as_bytes(),
                                format!("v{t}-{i}").as_bytes(),
                            )
                            .unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        for t in 0..4u32 {
            for i in 0..100u32 {
                let key = format!("t{t}-k{i}");
                match store.get(key.as_bytes()).unwrap() {
                    Lookup::Found(v) => assert_eq!(v, format!("v{t}-{i}").into_bytes()),
                    other => panic!("missing {key}: {other:?}"),
                }
            }
        }
    }
}
