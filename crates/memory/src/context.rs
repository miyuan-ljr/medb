//! 会话/查询级内存上下文 = 配额节点 + 短命对象 arena。
//!
//! 一个查询一个 `MemoryContext`;arena 的 chunk 记账也走同一 tracker,
//! 因此查询内存统计与限额天然一致。查询结束后整体 drop,配额随之释放。

use std::sync::Arc;

use crate::arena::Arena;
use crate::tracker::MemoryTracker;

pub struct MemoryContext {
    pub tracker: Arc<MemoryTracker>,
    pub arena: Arena,
}

impl MemoryContext {
    /// 在 parent(通常是会话节点)下创建查询上下文;子节点默认不限额,继承父级约束。
    pub fn new(parent: &Arc<MemoryTracker>, name: &str, arena_chunk_bytes: usize) -> Self {
        let tracker = parent.new_child(name, None);
        let arena = Arena::with_tracker(arena_chunk_bytes, Some(tracker.clone()));
        Self { tracker, arena }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_counts_arena_and_releases_on_drop() {
        let root = MemoryTracker::new_root("root", None);
        let ctx = MemoryContext::new(&root, "query-1", 4096);
        assert_eq!(ctx.tracker.name(), "query-1");
        let before = root.used();
        // 分配会触发一个 arena chunk 记账
        ctx.arena
            .alloc(std::alloc::Layout::new::<[u8; 1024]>())
            .unwrap();
        assert!(root.used() > before);
        drop(ctx);
        assert_eq!(root.used(), 0);
    }
}
