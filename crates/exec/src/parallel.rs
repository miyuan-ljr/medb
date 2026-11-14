//! 单机多线程执行辅助(v1 并行框架)。
//!
//! 框架形态:输入切片 + `std::thread::scope` 作用域线程,每线程处理连续
//! 分片,结果按分片序拼接 —— 保序,且无需 'static 数据(splitting borrow)。
//!
//! v1 应用点:LSM 解码 + 谓词过滤(engine 侧)、HashJoin 探测行组装。
//! 路线图:morsel-driven 流水线并行、并行哈希聚合(分区归并)。

use common::error::Result;

/// 有效线程数:上限钳制到元素数,至少 1。
pub fn threads_for(requested: usize, items: usize) -> usize {
    requested.max(1).min(items.max(1))
}

/// 对 `items` 按 `threads` 个连续分片并行执行 `f`,结果按分片序返回。
/// `f` 内部错误随分片传播(首个错误优先,其余分片结果丢弃)。
pub fn scope_map<T, R>(items: &[T], threads: usize, f: impl Fn(&[T]) -> R + Sync) -> Vec<R>
where
    T: Sync,
    R: Send,
{
    let n = threads_for(threads, items.len());
    if n <= 1 {
        return vec![f(items)];
    }
    let chunk_len = items.len().div_ceil(n);
    std::thread::scope(|scope| {
        let handles: Vec<_> = items
            .chunks(chunk_len)
            .map(|slice| scope.spawn(|| f(slice)))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("parallel worker panicked"))
            .collect()
    })
}

/// 同 [`scope_map`],但分片函数可失败。
pub fn scope_map_try<T, R>(
    items: &[T],
    threads: usize,
    f: impl Fn(&[T]) -> Result<R> + Sync,
) -> Result<Vec<R>>
where
    T: Sync,
    R: Send,
{
    let n = threads_for(threads, items.len());
    if n <= 1 {
        return f(items).map(|r| vec![r]);
    }
    let chunk_len = items.len().div_ceil(n);
    std::thread::scope(|scope| {
        let handles: Vec<_> = items
            .chunks(chunk_len)
            .map(|slice| scope.spawn(|| f(slice)))
            .collect();
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.join().expect("parallel worker panicked")?);
        }
        Ok(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_map_preserves_order_and_handles_single() {
        let items: Vec<u64> = (0..1000).collect();
        let out = scope_map(&items, 4, |sl| sl.iter().map(|x| x * 2).collect::<Vec<_>>());
        let flat: Vec<u64> = out.concat();
        assert_eq!(flat, items.iter().map(|x| x * 2).collect::<Vec<_>>());
        // 单元素/单线程
        let out1 = scope_map(&items[..1], 8, |sl| sl.to_vec());
        assert_eq!(out1, vec![vec![0]]);
    }

    #[test]
    fn scope_map_try_propagates_error() {
        let items = [1u64, 2, 3];
        let err = scope_map_try(&items, 2, |sl| {
            if sl.contains(&2) {
                Err(common::error::Error::sql("boom"))
            } else {
                Ok(sl.to_vec())
            }
        });
        assert!(err.is_err());
    }
}
