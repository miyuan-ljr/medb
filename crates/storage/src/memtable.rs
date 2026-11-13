//! Memtable:并发跳表(crossbeam-skiplist)实现的有序内存表。
//!
//! - 写:每次写入按条目近似字节数在配额树记账,超过 write_buffer 即可触发 flush;
//! - 读:从下界哨兵起迭代,同一 user_key 第一个命中即最新版本;
//! - 值 `None` 表示墓碑(删除)。
//!
//! 竞态说明:并发覆盖写同一 key 时记账可能多记(重写不回退旧额度),
//! flush 时按累计额度一次性归还 —— 宁多勿漏。

use std::sync::Arc;

use common::error::Result;
use crossbeam_skiplist::SkipMap;
use memory::{MemoryTracker, Reservation};
use parking_lot::Mutex;

use crate::key::{InternalKey, KeyKind, decode_internal_key, encode_internal_key, lower_bound_key};

/// 点查结果。Tombstone 与 NotFound 区分开:上层据此短路,不必再查 SST。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    Found(Vec<u8>),
    Tombstone,
    NotFound,
}

/// 扫描条目:(user_key, seq, value=None 为墓碑)。
pub type ScanEntry = (Vec<u8>, u64, Option<Vec<u8>>);

/// 累计记账:一条覆盖全表当前字节数的 Reservation,put 时 resize 增量。
struct Accounting {
    reservation: Reservation,
    total: usize,
}

pub struct Memtable {
    map: SkipMap<InternalKey, Option<Vec<u8>>>,
    tracker: Arc<MemoryTracker>,
    accounting: Mutex<Accounting>,
}

impl Memtable {
    pub fn new(tracker: Arc<MemoryTracker>) -> Self {
        // 0 字节预留不可能失败,只为了拿到预留链
        let reservation = tracker
            .try_reserve(0)
            .expect("zero-byte reservation cannot fail");
        Self {
            map: SkipMap::new(),
            tracker,
            accounting: Mutex::new(Accounting {
                reservation,
                total: 0,
            }),
        }
    }

    fn entry_cost(user_key: &[u8], value: Option<&[u8]>) -> usize {
        // 键 + 值 + trailer + 节点开销粗略常数
        user_key.len() + value.map_or(0, |v| v.len()) + crate::key::TRAILER_LEN + 64
    }

    /// 增量记账:失败时额度不变、条目不插入。
    fn reserve(&self, add: usize) -> Result<()> {
        let mut acc = self.accounting.lock();
        let new_total = acc.total + add;
        acc.reservation.resize(new_total)?;
        acc.total = new_total;
        Ok(())
    }

    /// 正常写入:先记账后插入。
    pub fn put(&self, user_key: &[u8], seq: u64, value: Option<Vec<u8>>) -> Result<()> {
        let cost = Self::entry_cost(user_key, value.as_deref());
        self.reserve(cost)?;
        self.insert(user_key, seq, value);
        Ok(())
    }

    /// 恢复期免限额插入(WAL 重放;不计入记账额度)。
    pub fn put_untracked(&self, user_key: &[u8], seq: u64, value: Option<Vec<u8>>) {
        self.insert(user_key, seq, value);
    }

    fn insert(&self, user_key: &[u8], seq: u64, value: Option<Vec<u8>>) {
        let kind = if value.is_some() {
            KeyKind::Put
        } else {
            KeyKind::Delete
        };
        self.map
            .insert(encode_internal_key(user_key, seq, kind), value);
    }

    /// 点查:返回 user_key 在 read_ts 快照下可见的最新版本(seq ≤ read_ts 的第一条)。
    /// 内部键序保证:同 user_key 版本按 seq 降序,过新版本顺序跳过。
    pub fn get_at(&self, user_key: &[u8], read_ts: u64) -> Lookup {
        for entry in self.map.range(lower_bound_key(user_key)..) {
            let Ok(decoded) = decode_internal_key(entry.key().as_ref()) else {
                break;
            };
            if decoded.user_key != user_key {
                break;
            }
            if decoded.seq > read_ts {
                continue; // 版本对当前快照不可见
            }
            return match entry.value() {
                Some(v) => Lookup::Found(v.clone()),
                None => Lookup::Tombstone,
            };
        }
        Lookup::NotFound
    }

    /// 最新版本语义(read_ts = MAX)。
    pub fn get(&self, user_key: &[u8]) -> Lookup {
        self.get_at(user_key, u64::MAX)
    }

    /// 全量扫描(供 flush 与测试)。跳表迭代即内部键序 = user_key 升序、版本新在前。
    pub fn scan_all(&self) -> Vec<ScanEntry> {
        self.map
            .iter()
            .filter_map(|e| {
                let decoded = decode_internal_key(e.key().as_ref()).ok()?;
                Some((decoded.user_key.to_vec(), decoded.seq, e.value().clone()))
            })
            .collect()
    }

    pub fn approximate_memory_usage(&self) -> u64 {
        self.accounting.lock().total as u64
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// flush 完成时调用:整表额度归还。
    pub fn release_accounting(&self) {
        let mut acc = self.accounting.lock();
        *acc = Accounting {
            reservation: self
                .tracker
                .try_reserve(0)
                .expect("zero-byte reservation cannot fail"),
            total: 0,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> Memtable {
        Memtable::new(MemoryTracker::new_root("mt", None))
    }

    #[test]
    fn put_get_overwrite_and_delete() {
        let mt = table();
        mt.put(b"k", 1, Some(b"v1".to_vec())).unwrap();
        assert_eq!(mt.get(b"k"), Lookup::Found(b"v1".to_vec()));
        mt.put(b"k", 2, Some(b"v2".to_vec())).unwrap();
        assert_eq!(mt.get(b"k"), Lookup::Found(b"v2".to_vec()));
        mt.put(b"k", 3, None).unwrap();
        assert_eq!(mt.get(b"k"), Lookup::Tombstone);
        assert_eq!(mt.get(b"other"), Lookup::NotFound);
    }

    #[test]
    fn get_takes_newest_version() {
        let mt = table();
        // 乱序插入,最新版本是 seq=5
        mt.put(b"k", 1, Some(b"old".to_vec())).unwrap();
        mt.put(b"k", 5, Some(b"new".to_vec())).unwrap();
        assert_eq!(mt.get(b"k"), Lookup::Found(b"new".to_vec()));
    }

    #[test]
    fn prefix_keys_do_not_collide() {
        let mt = table();
        mt.put(b"ab", 1, Some(b"1".to_vec())).unwrap();
        mt.put(b"abc", 1, Some(b"2".to_vec())).unwrap();
        assert_eq!(mt.get(b"ab"), Lookup::Found(b"1".to_vec()));
        assert_eq!(mt.get(b"abc"), Lookup::Found(b"2".to_vec()));
    }

    #[test]
    fn scan_all_is_sorted_newest_first_per_key() {
        let mt = table();
        mt.put(b"b", 1, Some(b"b1".to_vec())).unwrap();
        mt.put(b"a", 3, None).unwrap();
        mt.put(b"a", 2, Some(b"a2".to_vec())).unwrap();
        mt.put(b"c", 1, Some(b"c1".to_vec())).unwrap();
        let entries = mt.scan_all();
        let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _, _)| k.clone()).collect();
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(entries[0], (b"a".to_vec(), 3, None));
        assert_eq!(entries[1], (b"a".to_vec(), 2, Some(b"a2".to_vec())));
    }

    #[test]
    fn quota_limit_blocks_writes() {
        let root = MemoryTracker::new_root("mt", Some(200));
        let mt = Memtable::new(root.new_child("memtable", Some(200)));
        mt.put(b"k1", 1, Some(vec![0u8; 100])).unwrap();
        assert!(
            mt.put(b"k2", 2, Some(vec![0u8; 100])).is_err(),
            "第二条应超过 200 字节限额"
        );
        assert_eq!(mt.get(b"k2"), Lookup::NotFound);
        assert_eq!(mt.approximate_memory_usage(), 175);
        mt.release_accounting();
        assert_eq!(mt.approximate_memory_usage(), 0);
        assert_eq!(root.used(), 0);
    }
}
