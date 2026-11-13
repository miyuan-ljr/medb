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
}
