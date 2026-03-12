//! Error types for ferrosa-schema.

use std::fmt;

/// Errors that can occur in schema operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum SchemaError {}

impl fmt::Display for SchemaError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

impl std::error::Error for SchemaError {}

/// Result type alias for schema operations.
pub type Result<T> = std::result::Result<T, SchemaError>;
