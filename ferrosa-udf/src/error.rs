//! Error types for UDF execution.

use thiserror::Error;

/// Errors from UDF compilation, execution, or resource limits.
#[derive(Debug, Error)]
pub enum UdfError {
    #[error("compilation failed: {0}")]
    CompilationFailed(String),

    #[error("function not found: {keyspace}.{name}")]
    NotFound { keyspace: String, name: String },

    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    /// A guest tried to grow linear memory past the configured sandbox limit.
    #[error(
        "guest memory limit exceeded: {what} would reach {requested_bytes} bytes, \
         over the configured {config_key} = {configured_bytes} bytes"
    )]
    MemoryLimitExceeded {
        /// Config key that set the limit (TOML `[udf] max_memory_bytes`).
        config_key: &'static str,
        /// The configured limit in bytes.
        configured_bytes: usize,
        /// Size the guest asked for, in bytes.
        requested_bytes: usize,
        /// What was being grown (`linear memory`).
        what: &'static str,
    },

    /// The sandbox configuration is unusable (rejected at startup).
    #[error("invalid UDF sandbox config: {0}")]
    InvalidConfig(String),

    #[error("execution failed: {0}")]
    ExecutionFailed(String),

    #[error("type mismatch: {0}")]
    TypeMismatch(String),

    #[error("WASM binary too large: {size} bytes exceeds {max} byte limit")]
    BinaryTooLarge { size: usize, max: usize },

    #[error("function key is invalid or has been evicted")]
    KeyInvalid,
}
