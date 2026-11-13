//! 压缩策略。
//!
//! v1 实现是"简化 Leveled":L0 文件数达到阈值 → 把 L0+L1 全量合并为单个 L1 文件。
//! 正确性优先(单一 L1 文件永不重叠,读路径无需多文件 recency 推理);
//! 分级部分合并 / 限流 / manifold 合并见路线图。

use crate::manifest::LevelManifest;

#[derive(Debug, Clone)]
pub struct CompactionTask {
    pub input_ids: Vec<u64>,
    pub target_level: u32,
}

pub trait CompactionStrategy: Send + Sync {
    fn pick(&self, levels: &LevelManifest) -> Option<CompactionTask>;
}

pub struct LeveledStrategy {
    pub l0_threshold: usize,
}

impl CompactionStrategy for LeveledStrategy {
    fn pick(&self, levels: &LevelManifest) -> Option<CompactionTask> {
        let l0 = levels.levels.first()?;
        if l0.len() < self.l0_threshold {
            return None;
        }
        let mut input_ids: Vec<u64> = l0.iter().map(|m| m.id).collect();
        if let Some(l1) = levels.levels.get(1) {
            input_ids.extend(l1.iter().map(|m| m.id));
        }
        Some(CompactionTask {
            input_ids,
            target_level: 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::SstMeta;

    fn meta(id: u64) -> SstMeta {
        SstMeta {
            id,
            level: 0,
            file_size: 1,
            num_entries: 1,
            min_user_key: b"a".to_vec(),
            max_user_key: b"z".to_vec(),
            min_seq: 0,
            max_seq: 0,
        }
    }

    #[test]
    fn no_task_below_threshold() {
        let mut lm = LevelManifest::new(2);
        lm.add_l0(meta(1));
        let s = LeveledStrategy { l0_threshold: 4 };
        assert!(s.pick(&lm).is_none());
    }

    #[test]
    fn task_covers_l0_and_l1() {
        let mut lm = LevelManifest::new(2);
        lm.add_l0(meta(1));
        lm.add_l0(meta(2));
        lm.add_l0(meta(3));
        lm.levels[1].push(SstMeta {
            level: 1,
            ..meta(8)
        });
        let s = LeveledStrategy { l0_threshold: 3 };
        let task = s.pick(&lm).unwrap();
        assert_eq!(task.target_level, 1);
        // L0 新→旧
        assert_eq!(task.input_ids, vec![3, 2, 1, 8]);
    }
}
