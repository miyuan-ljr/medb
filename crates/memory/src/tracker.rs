//! 层级化内存配额树。
//!
//! 结构:全局根(limit 来自配置)→ 会话 → 查询 → 算子,每个节点带可选限额。
//! `try_reserve` 自节点向上逐层记账,任一层超限则整链回滚并返回
//! `MemoryLimitExceeded`;`Reservation` drop 时沿链释放,杜绝泄漏。
//!
//! 记账是"逻辑额度"而非精确 malloc 钩子:对象在创建前预估字节数并预留,
//! 释放时归还。粒度由调用方决定(memtable 按条目、DataChunk 按列容量)。

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Weak};

use common::error::{Error, Result};
use parking_lot::Mutex;

const UNLIMITED: i64 = -1;

/// 全局内存管理器:持有配额树根。
pub struct MemoryManager {
    root: Arc<MemoryTracker>,
}

impl MemoryManager {
    pub fn new(global_limit: Option<u64>) -> Self {
        Self {
            root: MemoryTracker::new_root("global", global_limit),
        }
    }

    pub fn root(&self) -> Arc<MemoryTracker> {
        self.root.clone()
    }

    pub fn used(&self) -> u64 {
        self.root.used()
    }

    pub fn snapshot(&self) -> TrackerSnapshot {
        self.root.snapshot()
    }
}

#[derive(Debug, Clone)]
pub struct TrackerSnapshot {
    pub name: String,
    pub used: u64,
    pub limit: Option<u64>,
    pub children: Vec<TrackerSnapshot>,
}

impl TrackerSnapshot {
    /// 缩进打印子树(日志/系统视图用)。
    pub fn render(&self) -> String {
        let mut out = String::new();
        fn walk(node: &TrackerSnapshot, depth: usize, out: &mut String) {
            let indent = "  ".repeat(depth);
            let limit = node
                .limit
                .map(|l| format!("{l}"))
                .unwrap_or_else(|| "unlimited".to_string());
            out.push_str(&format!(
                "{indent}{name}: {used} bytes (limit {limit})\n",
                name = node.name,
                used = node.used
            ));
            for c in &node.children {
                walk(c, depth + 1, out);
            }
        }
        walk(self, 0, &mut out);
        out
    }
}

pub struct MemoryTracker {
    name: String,
    limit: AtomicI64,
    used: AtomicI64,
    parent: Option<Weak<MemoryTracker>>,
    children: Mutex<Vec<Weak<MemoryTracker>>>,
}

impl std::fmt::Debug for MemoryTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryTracker")
            .field("name", &self.name)
            .field("used", &self.used())
            .field("limit", &self.limit())
            .finish()
    }
}

impl MemoryTracker {
    pub fn new_root(name: impl Into<String>, limit: Option<u64>) -> Arc<Self> {
        Arc::new(Self::new(name.into(), limit, None))
    }

    /// 创建子节点并挂到父节点(父节点只持 Weak,子节点 drop 后自动清理)。
    pub fn new_child(self: &Arc<Self>, name: impl Into<String>, limit: Option<u64>) -> Arc<Self> {
        let child = Arc::new(Self::new(name.into(), limit, Some(Arc::downgrade(self))));
        let mut kids = self.children.lock();
        kids.retain(|w| w.upgrade().is_some());
        kids.push(Arc::downgrade(&child));
        child
    }

    fn new(name: String, limit: Option<u64>, parent: Option<Weak<MemoryTracker>>) -> Self {
        Self {
            name,
            limit: AtomicI64::new(limit.map_or(UNLIMITED, |v| v as i64)),
            used: AtomicI64::new(0),
            parent,
            children: Mutex::new(Vec::new()),
        }
    }

    /// 自身到根的强引用链(0 号是自身)。
    fn path_to_root(self: &Arc<Self>) -> Vec<Arc<MemoryTracker>> {
        let mut path = vec![self.clone()];
        let mut current = self.clone();
        while let Some(parent) = current.parent.as_ref().and_then(|w| w.upgrade()) {
            path.push(parent.clone());
            current = parent;
        }
        path
    }

    /// 预留 bytes;任一祖先超限则回滚并返回 MemoryLimitExceeded。
    pub fn try_reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation> {
        if bytes == 0 {
            return Ok(Reservation {
                path: self.path_to_root(),
                bytes: 0,
            });
        }
        let path = self.path_to_root();
        let delta = bytes as i64;
        for (idx, node) in path.iter().enumerate() {
            node.used.fetch_add(delta, Ordering::Relaxed);
            let limit = node.limit.load(Ordering::Relaxed);
            if limit >= 0 && node.used.load(Ordering::Relaxed) > limit {
                // 回滚已记账的本层及之前各层
                for node in &path[..=idx] {
                    node.used.fetch_sub(delta, Ordering::Relaxed);
                }
                let node = &path[idx];
                return Err(Error::MemoryLimitExceeded {
                    tracker: node.name.clone(),
                    used: node.used(),
                    limit: limit as u64,
                    requested: bytes as u64,
                });
            }
        }
        Ok(Reservation { path, bytes })
    }

    /// 沿自身到根释放 bytes。仅用于与 reserve 对称的独立调用;成对使用请走 Reservation。
    pub fn release(self: &Arc<Self>, bytes: usize) {
        let delta = bytes as i64;
        for node in self.path_to_root() {
            node.used.fetch_sub(delta, Ordering::Relaxed);
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed).max(0) as u64
    }

    pub fn limit(&self) -> Option<u64> {
        let v = self.limit.load(Ordering::Relaxed);
        (v >= 0).then_some(v as u64)
    }

    pub fn set_limit(&self, limit: Option<u64>) {
        self.limit
            .store(limit.map_or(UNLIMITED, |v| v as i64), Ordering::Relaxed);
    }

    /// 递归快照;已释放的子节点自动跳过。
    pub fn snapshot(self: &Arc<Self>) -> TrackerSnapshot {
        let children = self
            .children
            .lock()
            .iter()
            .filter_map(|w| w.upgrade())
            .map(|c| c.snapshot())
            .collect();
        TrackerSnapshot {
            name: self.name.clone(),
            used: self.used(),
            limit: self.limit(),
            children,
        }
    }
}

/// RAII 记账凭证:drop 时沿预留链逐层归还。不可 Clone(避免双重释放)。
#[derive(Debug)]
pub struct Reservation {
    path: Vec<Arc<MemoryTracker>>,
    bytes: usize,
}

impl Reservation {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// 调整预留额度(扩容/缩容,如 arena 追加 chunk)。
    /// 先归还旧额度再重新预留;失败时旧额度已释放,返回 Err。
    pub fn resize(&mut self, new_bytes: usize) -> Result<()> {
        if new_bytes == self.bytes {
            return Ok(());
        }
        let holder = std::mem::replace(
            self,
            Reservation {
                path: Vec::new(),
                bytes: 0,
            },
        );
        let owner = holder.path[0].clone();
        drop(holder); // 归还旧额度
        *self = owner.try_reserve(new_bytes)?;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let delta = self.bytes as i64;
        for node in &self.path {
            node.used.fetch_sub(delta, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_child_accounting() {
        let root = MemoryTracker::new_root("root", None);
        let child = root.new_child("query", None);
        let r = child.try_reserve(100).unwrap();
        assert_eq!(child.used(), 100);
        assert_eq!(root.used(), 100);
        drop(r);
        assert_eq!(child.used(), 0);
        assert_eq!(root.used(), 0);
    }

    #[test]
    fn child_limit_exceeded() {
        let root = MemoryTracker::new_root("root", None);
        let child = root.new_child("query", Some(50));
        let err = child.try_reserve(100).unwrap_err();
        assert_eq!(err.code(), common::error::ErrorCode::MemoryLimitExceeded);
        assert_eq!(root.used(), 0, "超限时根不应留下记账");
        let ok = child.try_reserve(50).unwrap();
        assert_eq!(child.used(), 50);
        drop(ok);
    }

    #[test]
    fn root_limit_rollback_child() {
        let root = MemoryTracker::new_root("root", Some(100));
        let child = root.new_child("query", None);
        let r1 = child.try_reserve(60).unwrap();
        assert!(child.try_reserve(60).is_err(), "应被根限额拦下");
        assert_eq!(root.used(), 60);
        assert_eq!(child.used(), 60);
        drop(r1);
    }

    #[test]
    fn resize_releases_and_reacquires() {
        let root = MemoryTracker::new_root("root", Some(200));
        let child = root.new_child("q", None);
        let mut r = child.try_reserve(50).unwrap();
        r.resize(150).unwrap();
        assert_eq!(root.used(), 150);
        assert!(r.resize(300).is_err());
        assert_eq!(root.used(), 0, "resize 失败应已释放旧额度");
    }

    #[test]
    fn concurrent_reserves_never_exceed_limit() {
        let root = MemoryTracker::new_root("root", Some(10_000));
        let child = root.new_child("q", None);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let child = child.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        match child.try_reserve(7) {
                            Ok(r) => drop(r),
                            Err(_) => {}
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(root.used(), 0);
    }

    #[test]
    fn snapshot_prunes_dead_children() {
        let root = MemoryTracker::new_root("root", None);
        {
            let _tmp = root.new_child("temp", None);
            assert_eq!(root.snapshot().children.len(), 1);
        }
        // Weak 子节点被 drop 后,snapshot 不再列出
        assert_eq!(root.snapshot().children.len(), 0);
    }

    #[test]
    fn zero_bytes_reservation_is_free() {
        let root = MemoryTracker::new_root("root", Some(0));
        root.try_reserve(0).unwrap();
        assert!(root.try_reserve(1).is_err());
    }
}
