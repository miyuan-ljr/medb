//! 全局配置(TOML)。所有子节都带默认值,最小可用配置即空文件。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:55432";
/// 全局内存上限(配额树根):默认 30 GiB,超出即拒绝分配(MemoryLimitExceeded)
pub const DEFAULT_GLOBAL_LIMIT_BYTES: u64 = 30 * 1024 * 1024 * 1024;
pub const DEFAULT_QUERY_LIMIT_BYTES: u64 = 256 << 20; // 256 MiB
pub const DEFAULT_SESSION_LIMIT_BYTES: u64 = 64 << 20; // 64 MiB
pub const DEFAULT_ARENA_CHUNK_BYTES: usize = 64 << 10; // 64 KiB
pub const DEFAULT_BATCH_SIZE: usize = 2048;
/// 并行度上限(v1 单机多线程:扫描解码 / JOIN 探测)。
pub const DEFAULT_PARALLELISM: usize = 8;
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 64 << 20;
pub const DEFAULT_WRITE_BUFFER_BYTES: u64 = 32 << 20;
pub const DEFAULT_SST_BLOCK_SIZE: usize = 16 << 10;
pub const DEFAULT_BLOOM_BITS_PER_KEY: usize = 10;
pub const DEFAULT_L0_COMPACTION_THRESHOLD: usize = 4;
pub const DEFAULT_DATA_DIR: &str = "./data";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub memory: MemoryConfig,
    pub engine: EngineConfig,
    pub storage: StorageConfig,
    pub log: LogConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            memory: MemoryConfig::default(),
            engine: EngineConfig::default(),
            storage: StorageConfig::default(),
            log: LogConfig::default(),
        }
    }
}

impl Config {
    /// 读取指定配置文件;文件不存在或不可解析时返回错误。
    pub fn load(path: &Path) -> Result<Config> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            Error::invalid_argument(format!("cannot read config {}: {e}", path.display()))
        })?;
        toml::from_str(&text)
            .map_err(|e| Error::invalid_argument(format!("bad config {}: {e}", path.display())))
    }

    /// path 为 None 或文件不存在时返回默认配置(默认路径约定,适合开发期)。
    pub fn load_or_default(path: Option<&Path>) -> Result<(Config, bool)> {
        match path {
            Some(p) if p.exists() => Ok((Self::load(p)?, true)),
            _ => Ok((Config::default(), false)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// PG wire 监听地址。默认 55432,避免与本机真实 PostgreSQL(5432)冲突。
    pub listen_addr: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: DEFAULT_LISTEN_ADDR.to_string(),
        }
    }
}

/// 内存分配机制的全局参数(配额树根限额、arena 块大小)。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryConfig {
    pub global_limit_bytes: u64,
    pub query_limit_bytes: u64,
    pub session_limit_bytes: u64,
    pub arena_chunk_bytes: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            global_limit_bytes: DEFAULT_GLOBAL_LIMIT_BYTES,
            query_limit_bytes: DEFAULT_QUERY_LIMIT_BYTES,
            session_limit_bytes: DEFAULT_SESSION_LIMIT_BYTES,
            arena_chunk_bytes: DEFAULT_ARENA_CHUNK_BYTES,
        }
    }
}

/// 执行引擎参数。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EngineConfig {
    /// 向量化批大小(行/批)。
    pub batch_size: usize,
    /// 并行度(扫描解码 / JOIN 探测构建的线程数)。默认 = CPU 核数,上限 8。
    pub parallelism: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            parallelism: DEFAULT_PARALLELISM,
        }
    }
}

/// 存储引擎(LSM)参数。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub wal_segment_bytes: u64,
    /// memtable 限额,超过触发 flush。
    pub write_buffer_bytes: u64,
    pub sst_block_size: usize,
    pub bloom_bits_per_key: usize,
    /// L0 文件数达到该阈值触发压缩。
    pub l0_compaction_threshold: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            wal_segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
            write_buffer_bytes: DEFAULT_WRITE_BUFFER_BYTES,
            sst_block_size: DEFAULT_SST_BLOCK_SIZE,
            bloom_bits_per_key: DEFAULT_BLOOM_BITS_PER_KEY,
            l0_compaction_threshold: DEFAULT_L0_COMPACTION_THRESHOLD,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogConfig {
    /// 未设置 RUST_LOG 时使用的默认级别。
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}
