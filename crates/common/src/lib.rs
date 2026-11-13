//! medb 公共基础库:错误类型、数据类型、schema、行编码、配置与通用工具。
//!
//! 该 crate 处于依赖图最底层,除标准生态外不依赖任何内部 crate。

pub mod codec;
pub mod config;
pub mod error;
pub mod ids;
pub mod schema;
pub mod types;
pub mod util;
