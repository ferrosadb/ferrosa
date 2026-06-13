//! Error types for the graph engine.

use std::fmt;

/// Errors produced by graph query processing.
#[derive(Debug)]
pub enum GraphError {
    /// Cypher parse error.
    Parse(crate::parser::ParseError),
    /// Schema validation error (bad label, missing property, etc.).
    Validation(String),
    /// Permission denied.
    PermissionDenied(String),
    /// Query exceeded resource limits.
    ResourceLimit(String),
    /// A Neo4j-style schema/constraint violation, e.g. a plain `DELETE n`
    /// on a node that still has surviving relationships (URS-QEC-D02).
    ConstraintViolation(String),
    /// Query exceeded time limit.
    Timeout,
    /// Storage engine error.
    Storage(ferrosa_common::Error),
    /// Schema error.
    Schema(ferrosa_schema::SchemaError),
    /// Internal error.
    Internal(String),
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(e) => write!(f, "parse error: {e}"),
            Self::Validation(msg) => write!(f, "validation error: {msg}"),
            Self::PermissionDenied(msg) => write!(f, "permission denied: {msg}"),
            Self::ResourceLimit(msg) => write!(f, "resource limit: {msg}"),
            Self::ConstraintViolation(msg) => write!(f, "constraint violation: {msg}"),
            Self::Timeout => write!(f, "query timeout"),
            Self::Storage(e) => write!(f, "storage error: {e}"),
            Self::Schema(e) => write!(f, "schema error: {e}"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for GraphError {}

impl From<crate::parser::ParseError> for GraphError {
    fn from(e: crate::parser::ParseError) -> Self {
        Self::Parse(e)
    }
}

impl From<ferrosa_common::Error> for GraphError {
    fn from(e: ferrosa_common::Error) -> Self {
        Self::Storage(e)
    }
}

impl From<ferrosa_schema::SchemaError> for GraphError {
    fn from(e: ferrosa_schema::SchemaError) -> Self {
        Self::Schema(e)
    }
}

impl From<ferrosa_cluster::error::ClusterError> for GraphError {
    fn from(e: ferrosa_cluster::error::ClusterError) -> Self {
        Self::Storage(ferrosa_common::Error::InvalidData(e.to_string()))
    }
}

/// Result type alias for graph operations.
pub type Result<T> = std::result::Result<T, GraphError>;
