//! medb 存储引擎:LSM tree 行存主库 + HTAP 列存副本钩子。
//!
//! 模块划分:
//! - [`key`]:内部键编码与比较器;
//! - [`memtable`]:并发跳表内存表(配额树记账);
//! - [`wal`]:段式 WAL(帧 + CRC,崩溃尾部截断);
//! - [`sstable`]:SST 格式(block / bloom / index / 读写器);
//! - [`manifest`]:层级清单与文件号;
//! - [`compaction`]:压缩策略 trait + Leveled 简化实现;
//! - [`lsm`]:LsmStore 门面(写路径 / 读路径 / flush / 压缩 / 恢复);
//! - [`table`]:表数据键布局(行键 / 目录键 / 范围 / 可排序编码);
//! - [`columnar_file`]:列存 segment 文件(HTAP AP 路径);
//! - [`columnar_index`]:segment 上的二级索引树文件。

pub mod columnar_file;
pub mod columnar_index;
pub mod compaction;
pub mod key;
pub mod lsm;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod table;
pub mod wal;

pub use compaction::{CompactionStrategy, CompactionTask, LeveledStrategy};
pub use key::{InternalKey, KeyKind};
pub use lsm::{LsmOptions, LsmStats, LsmStore};
pub use memtable::Lookup;
pub use sstable::SstMeta;
