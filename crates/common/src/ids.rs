//! ID 类型与单调生成器。

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableId(pub u64);

/// 进程内单调递增 ID 生成器(文件号、表 ID 等共用)。
#[derive(Debug)]
pub struct IdGenerator(AtomicU64);

impl IdGenerator {
    pub fn new(start: u64) -> Self {
        Self(AtomicU64::new(start))
    }

    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn peek(&self) -> u64 {
        self.0.load(Ordering::Relaxed) + 1
    }

    /// 把计数器推进到至少 `last_used`(next() 返回值 > last_used)。
    /// 恢复持久化对象时调用:恢复的 id 不经过 next(),不推进就会撞车。
    pub fn bump_past(&self, last_used: u64) {
        let mut cur = self.0.load(Ordering::Relaxed);
        while cur < last_used {
            match self
                .0
                .compare_exchange(cur, last_used, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
}
