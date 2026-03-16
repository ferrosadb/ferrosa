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

    #[error("execution failed: {0}")]
    ExecutionFailed(String),

    #[error("type mismatch: {0}")]
    TypeMismatch(String),

    #[error("WASM binary too large: {size} bytes exceeds {max} byte limit")]
    BinaryTooLarge { size: usize, max: usize },
}
