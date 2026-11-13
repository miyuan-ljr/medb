//! 层级清单(L0/L1)+ 文件号分配。v1 常驻内存,启动时从 sst/ 目录文件名重建;
//! manifest 落盘(VersionEdit 日志)见路线图。

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::sstable::SstMeta;

pub const NUM_LEVELS: usize = 2;

#[derive(Debug, Clone, Default)]
pub struct LevelManifest {
    /// levels[0] = L0(新→旧,id 降序);levels[1] = L1(压缩产物,可重叠)。
    pub levels: Vec<Vec<SstMeta>>,
}

impl LevelManifest {
    pub fn new(num_levels: usize) -> Self {
        Self {
            levels: vec![Vec::new(); num_levels],
        }
    }

    pub fn add_l0(&mut self, meta: SstMeta) {
        self.levels[0].insert(0, meta);
    }

    /// 原子地移除 inputs 并把产物加入 target_level。
    pub fn replace(&mut self, remove_ids: &[u64], add: SstMeta, target_level: usize) {
        for level in &mut self.levels {
            level.retain(|m| !remove_ids.contains(&m.id));
        }
        if target_level < self.levels.len() {
            self.levels[target_level].push(add);
        }
    }

    pub fn total_files(&self) -> usize {
        self.levels.iter().map(|l| l.len()).sum()
    }

    pub fn total_bytes(&self) -> u64 {
        self.levels
            .iter()
            .flat_map(|l| l.iter())
            .map(|m| m.file_size)
            .sum()
    }
}

pub struct Manifest {
    levels: Mutex<LevelManifest>,
    next_file_id: AtomicU64,
}

impl Manifest {
    pub fn new(next_file_id: u64) -> Self {
        Self {
            levels: Mutex::new(LevelManifest::new(NUM_LEVELS)),
            next_file_id: AtomicU64::new(next_file_id),
        }
    }

    /// 分配并返回当前文件号(从构造时的 start 开始)。
    pub fn next_file_id(&self) -> u64 {
        self.next_file_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn peek_file_id(&self) -> u64 {
        self.next_file_id.load(Ordering::Relaxed)
    }

    /// 一致性快照(读路径构建读集用)。
    pub fn snapshot(&self) -> LevelManifest {
        self.levels.lock().clone()
    }

    pub fn update(&self, f: impl FnOnce(&mut LevelManifest)) {
        let mut levels = self.levels.lock();
        f(&mut levels);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: u64) -> SstMeta {
        SstMeta {
            id,
            level: 0,
            file_size: 100,
            num_entries: 10,
            min_user_key: b"a".to_vec(),
            max_user_key: b"z".to_vec(),
            min_seq: 1,
            max_seq: 10,
        }
    }

    #[test]
    fn l0_ordering_newest_first() {
        let mut lm = LevelManifest::new(NUM_LEVELS);
        lm.add_l0(meta(1));
        lm.add_l0(meta(2));
        lm.add_l0(meta(3));
        assert_eq!(
            lm.levels[0].iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![3, 2, 1]
        );
    }

    #[test]
    fn replace_removes_from_all_levels() {
        let mut lm = LevelManifest::new(NUM_LEVELS);
        lm.add_l0(meta(1));
        lm.add_l0(meta(2));
        let merged = SstMeta {
            level: 1,
            ..meta(9)
        };
        lm.replace(&[1, 2], merged, 1);
        assert!(lm.levels[0].is_empty());
        assert_eq!(lm.levels[1].len(), 1);
        assert_eq!(lm.levels[1][0].id, 9);
    }

    #[test]
    fn file_id_monotonic() {
        let m = Manifest::new(10);
        assert_eq!(m.next_file_id(), 10);
        assert_eq!(m.next_file_id(), 11);
        assert_eq!(m.peek_file_id(), 12);
    }
}
