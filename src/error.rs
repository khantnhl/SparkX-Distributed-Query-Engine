use thiserror::Error;

pub type Result<T> = std::result::Result<T, SparkXError>;

#[derive(Debug, Error)]
pub enum SparkXError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("SQL parse error: {0}")]
    Sql(#[from] sqlparser::parser::ParserError),

    #[error("planning error: {0}")]
    Planning(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("resource not found: {0}")]
    NotFound(String),

    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("query was cancelled")]
    Cancelled,

    #[error("operator channel closed unexpectedly")]
    ChannelClosed,
}

impl SparkXError {
    pub fn planning(message: impl Into<String>) -> Self {
        Self::Planning(message.into())
    }

    pub fn execution(message: impl Into<String>) -> Self {
        Self::Execution(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported(message.into())
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }

    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::ResourceExhausted(message.into())
    }
}
