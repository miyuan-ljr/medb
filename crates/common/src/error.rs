//! 统一错误类型。错误码用于后续映射 PG SQLSTATE 与监控打点。

/// 顶层错误分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Io,
    Corruption,
    InvalidArgument,
    NotImplemented,
    NotSupported,
    MemoryLimitExceeded,
    Catalog,
    Sql,
    Internal,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("data corruption: {0}")]
    Corruption(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("not implemented: {0}")]
    NotImplemented(String),

    #[error("not supported: {0}")]
    NotSupported(String),

    #[error(
        "memory limit exceeded: tracker '{tracker}' used {used}/{limit} bytes, requested {requested} more"
    )]
    MemoryLimitExceeded {
        tracker: String,
        used: u64,
        limit: u64,
        requested: u64,
    },

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("sql error: {0}")]
    Sql(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    pub fn corruption(msg: impl Into<String>) -> Self {
        Error::Corruption(msg.into())
    }

    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Error::InvalidArgument(msg.into())
    }

    pub fn not_implemented(feature: impl Into<String>) -> Self {
        Error::NotImplemented(feature.into())
    }

    pub fn not_supported(msg: impl Into<String>) -> Self {
        Error::NotSupported(msg.into())
    }

    pub fn catalog(msg: impl Into<String>) -> Self {
        Error::Catalog(msg.into())
    }

    pub fn sql(msg: impl Into<String>) -> Self {
        Error::Sql(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal(msg.into())
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            Error::Io(_) => ErrorCode::Io,
            Error::Corruption(_) => ErrorCode::Corruption,
            Error::InvalidArgument(_) => ErrorCode::InvalidArgument,
            Error::NotImplemented(_) => ErrorCode::NotImplemented,
            Error::NotSupported(_) => ErrorCode::NotSupported,
            Error::MemoryLimitExceeded { .. } => ErrorCode::MemoryLimitExceeded,
            Error::Catalog(_) => ErrorCode::Catalog,
            Error::Sql(_) => ErrorCode::Sql,
            Error::Internal(_) => ErrorCode::Internal,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
