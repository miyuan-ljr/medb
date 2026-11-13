//! 全局分配器。
//!
//! Windows/MSVC 下 jemalloc 构建困难,故选 mimalloc:纯 C 源码 + cc 构建,
//! 多线程扩展性好,且对大对象(如 SST 读缓存)有独立路径。
//! 关闭 feature `mimalloc` 即回退系统分配器。

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// 当前生效的全局分配器名(启动日志输出,便于确认构建配置)。
pub fn allocator_name() -> &'static str {
    #[cfg(feature = "mimalloc")]
    {
        "mimalloc"
    }
    #[cfg(not(feature = "mimalloc"))]
    {
        "system"
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn allocator_name_reports_build_config() {
        #[cfg(feature = "mimalloc")]
        assert_eq!(super::allocator_name(), "mimalloc");
        #[cfg(not(feature = "mimalloc"))]
        assert_eq!(super::allocator_name(), "system");
    }
}
