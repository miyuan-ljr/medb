//! medb 内存分配机制,分三层,职责清晰、可独立演进:
//!
//! 1. **全局分配器**([`global`]):进程级 mimalloc(feature 开关,可回退系统分配器)。
//! 2. **配额树**([`tracker`]):`MemoryTracker` 组成 全局 → 会话 → 查询 的层级限额,
//!    `Reservation` 以 RAII 方式记账/回滚,任一节点超限即失败。
//! 3. **短命对象 arena**([`arena`]):查询生命周期的 bump allocator,整块归还,
//!    避免碎片化,配合配额树统计。
//!
//! 约定:凡是"一次性分配、整批释放"的对象(列缓冲、临时字符串堆)走 arena;
//! 生命周期跨越查询的对象(缓冲池页、SST 读缓存)走配额树 + 全局分配器。

pub mod arena;
pub mod context;
pub mod global;
pub mod tracker;

pub use arena::Arena;
pub use context::MemoryContext;
pub use tracker::{MemoryManager, MemoryTracker, Reservation, TrackerSnapshot};
