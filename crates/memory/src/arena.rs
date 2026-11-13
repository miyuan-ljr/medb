//! 线程安全的 bump(arena)分配器。
//!
//! 设计:
//! - 按需申请 chunk(默认 64KiB,单对象更大时按需扩容),chunk 内 bump 分配 O(1);
//! - 每个 chunk 对应配额树上一条 `Reservation`,reset/drop 时整块归还;
//! - 指针生命周期约定:在 arena `reset()` 或 drop 之前有效。调用方通过
//!   `alloc_slice` 等安全封装使用;直接持有 `NonNull` 属于 unsafe 边界。
//!
//! TODO(路线图):chunk 空闲池复用、对齐探测优化、区分 string-heap 与 vector-buffer 两个 arena。

use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::error::{Error, Result};
use common::util::align_up;
use parking_lot::Mutex;

use crate::tracker::{MemoryTracker, Reservation};

const CHUNK_BASE_ALIGN: usize = 64; // chunk 起始对齐到 cache line

pub struct Arena {
    tracker: Option<Arc<MemoryTracker>>,
    default_chunk_bytes: usize,
    inner: Mutex<Inner>,
    allocated_total: AtomicU64,
    reset_count: AtomicU64,
}

struct Chunk {
    base: NonNull<u8>,
    cap: usize,
    pos: usize,
    _reservation: Option<Reservation>,
}

impl Drop for Chunk {
    fn drop(&mut self) {
        // Reservation 字段在 drop(self) 体之后释放,配额随后归零
        let layout = Layout::from_size_align(self.cap, CHUNK_BASE_ALIGN)
            .expect("chunk layout must round-trip");
        unsafe { dealloc(self.base.as_ptr(), layout) };
    }
}

#[derive(Default)]
struct Inner {
    chunks: Vec<Chunk>,
}

// 所有状态都在 Mutex 之后;chunk 指针只被所有者读取,Send/Sync 安全。
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    pub fn new(default_chunk_bytes: usize) -> Self {
        Self::with_tracker(default_chunk_bytes, None)
    }

    pub fn with_tracker(default_chunk_bytes: usize, tracker: Option<Arc<MemoryTracker>>) -> Self {
        Self {
            tracker,
            default_chunk_bytes: default_chunk_bytes.max(1),
            inner: Mutex::new(Inner::default()),
            allocated_total: AtomicU64::new(0),
            reset_count: AtomicU64::new(0),
        }
    }

    /// 按 layout 分配;size 为 0 时返回对齐的悬垂指针(合法,不可解引用)。
    pub fn alloc(&self, layout: Layout) -> Result<NonNull<u8>> {
        if layout.size() == 0 {
            return Ok(
                NonNull::new(layout.align() as *mut u8).expect("alignment is always non-zero")
            );
        }

        let mut inner = self.inner.lock();
        if let Some(last) = inner.chunks.last_mut() {
            let start = align_up(last.pos, layout.align());
            if start + layout.size() <= last.cap {
                last.pos = start + layout.size();
                let ptr = unsafe { last.base.as_ptr().add(start) };
                return Ok(NonNull::new(ptr).expect("bumped pointer within chunk"));
            }
        }

        self.new_chunk(layout.size(), &mut inner)?;
        let last = inner.chunks.last_mut().expect("chunk just pushed");
        let start = align_up(0, layout.align());
        last.pos = start + layout.size();
        let ptr = unsafe { last.base.as_ptr().add(start) };
        Ok(NonNull::new(ptr).expect("bumped pointer within fresh chunk"))
    }

    /// 拷贝一段 Copy 类型数据进 arena,返回其切片指针。
    pub fn alloc_slice<T: Copy>(&self, values: &[T]) -> Result<NonNull<[T]>> {
        let layout = Layout::array::<T>(values.len())
            .map_err(|_| Error::invalid_argument("arena: array layout overflow"))?;
        let ptr = self.alloc(layout)?;
        unsafe {
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr.as_ptr() as *mut T, values.len());
            Ok(NonNull::from(std::slice::from_raw_parts_mut(
                ptr.as_ptr() as *mut T,
                values.len(),
            )))
        }
    }

    fn new_chunk(&self, min_bytes: usize, inner: &mut Inner) -> Result<()> {
        let cap = self
            .default_chunk_bytes
            .max(align_up(min_bytes, CHUNK_BASE_ALIGN));
        let reservation = match &self.tracker {
            Some(t) => Some(t.try_reserve(cap)?),
            None => None,
        };
        let layout = Layout::from_size_align(cap, CHUNK_BASE_ALIGN)
            .map_err(|_| Error::Internal("arena: chunk layout overflow".to_string()))?;
        let base = unsafe { alloc(layout) };
        if base.is_null() {
            drop(reservation);
            return Err(Error::Internal(format!(
                "arena: failed to allocate {cap} byte chunk"
            )));
        }
        self.allocated_total
            .fetch_add(cap as u64, Ordering::Relaxed);
        inner.chunks.push(Chunk {
            base: NonNull::new(base).expect("alloc returned non-null"),
            cap,
            pos: 0,
            _reservation: reservation,
        });
        Ok(())
    }

    /// 释放全部 chunk,先前返回的指针全部失效。
    pub fn reset(&self) {
        let mut inner = self.inner.lock();
        inner.chunks.clear();
        self.reset_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn live_bytes(&self) -> u64 {
        self.inner.lock().chunks.iter().map(|c| c.cap as u64).sum()
    }

    pub fn allocated_total(&self) -> u64 {
        self.allocated_total.load(Ordering::Relaxed)
    }

    pub fn chunk_count(&self) -> usize {
        self.inner.lock().chunks.len()
    }

    pub fn reset_count(&self) -> u64 {
        self.reset_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alignment_is_honored() {
        let arena = Arena::new(4096);
        let p8 = arena.alloc(Layout::new::<u8>()).unwrap();
        assert_eq!(p8.as_ptr() as usize % 1, 0);
        let p64 = arena.alloc(Layout::new::<u64>()).unwrap();
        assert_eq!(p64.as_ptr() as usize % 8, 0);
        let p16 = arena
            .alloc(Layout::from_size_align(3, 16).unwrap())
            .unwrap();
        assert_eq!(p16.as_ptr() as usize % 16, 0);
    }

    #[test]
    fn data_written_survives_across_allocs() {
        let arena = Arena::new(1024);
        let a = arena.alloc_slice(&[1i64, 2, 3]).unwrap();
        let b = arena.alloc_slice(&[4u8; 500]).unwrap();
        unsafe {
            assert_eq!(a.as_ref(), &[1i64, 2, 3]);
            assert_eq!(b.as_ref()[499], 4);
        }
    }

    #[test]
    fn grows_beyond_default_chunk() {
        let arena = Arena::new(1024);
        for i in 0..1000u64 {
            let p = arena.alloc(Layout::new::<u64>()).unwrap();
            unsafe { *(p.as_ptr() as *mut u64) = i };
        }
        assert!(arena.chunk_count() > 1);
        assert_eq!(arena.live_bytes(), arena.allocated_total());
    }

    #[test]
    fn reset_releases_tracker_quota_and_chunks() {
        let root = MemoryTracker::new_root("root", Some(4096));
        let arena = Arena::with_tracker(1024, Some(root.clone()));
        for _ in 0..10 {
            arena.alloc(Layout::new::<[u8; 256]>()).unwrap();
        }
        assert!(root.used() > 0);
        let chunks_before = arena.chunk_count();
        arena.reset();
        assert_eq!(root.used(), 0);
        assert_eq!(arena.chunk_count(), 0);
        assert_eq!(arena.reset_count(), 1);
        assert_eq!(chunks_before > 0, true);
    }

    #[test]
    fn oversize_allocation_gets_dedicated_chunk() {
        let arena = Arena::new(1024);
        let p = arena
            .alloc(Layout::from_size_align(8000, 64).unwrap())
            .unwrap();
        unsafe {
            std::ptr::write_bytes(p.as_ptr(), 0xab, 8000);
        }
        assert!(arena.live_bytes() >= 8000);
    }

    #[test]
    fn tracker_limit_blocks_chunk_growth() {
        let root = MemoryTracker::new_root("root", Some(2048));
        let arena = Arena::with_tracker(1024, Some(root.clone()));
        arena.alloc(Layout::new::<[u8; 1024]>()).unwrap();
        arena.alloc(Layout::new::<[u8; 1024]>()).unwrap();
        assert!(arena.alloc(Layout::new::<[u8; 1024]>()).is_err());
    }

    #[test]
    fn concurrent_allocs_are_consistent() {
        let arena = std::sync::Arc::new(Arena::new(4096));
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let arena = arena.clone();
                std::thread::spawn(move || {
                    for i in 0..1000u32 {
                        let p = arena.alloc(Layout::new::<u32>()).unwrap();
                        unsafe { *(p.as_ptr() as *mut u32) = (t as u32) ^ i };
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert!(arena.live_bytes() > 0);
    }

    #[test]
    fn zero_sized_alloc_returns_dangling() {
        let arena = Arena::new(64);
        let p = arena.alloc(Layout::new::<()>()).unwrap();
        let _ = p.as_ptr() as usize; // 可取地址,不可解引用
    }
}
