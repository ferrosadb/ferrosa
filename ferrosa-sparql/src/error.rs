//! SPARQL endpoint error types.

use std::fmt;

/// Errors from the SPARQL endpoint.
#[derive(Debug)]
pub enum SparqlError {
    /// SPARQL parse error (invalid syntax).
    Parse(String),
    /// Query planning error (unsupported feature, unknown prefix, etc.).
    Plan(String),
    /// Execution error (storage failure, timeout, etc.).
    Execution(String),
    /// The requested keyspace does not exist or has no RDF triples table.
    KeyspaceNotFound(String),
    /// Authentication or authorization failure.
    AccessDenied(String),
    /// Internal error.
    Internal(String),
}

impl fmt::Display for SparqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(msg) => write!(f, "SPARQL parse error: {msg}"),
            Self::Plan(msg) => write!(f, "SPARQL plan error: {msg}"),
            Self::Execution(msg) => write!(f, "SPARQL execution error: {msg}"),
            Self::KeyspaceNotFound(ks) => write!(f, "keyspace not found: {ks}"),
            Self::AccessDenied(msg) => write!(f, "access denied: {msg}"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for SparqlError {}

impl From<ferrosa_common::Error> for SparqlError {
    fn from(e: ferrosa_common::Error) -> Self {
        Self::Execution(e.to_string())
    }
}

impl From<ferrosa_cluster::error::ClusterError> for SparqlError {
    fn from(e: ferrosa_cluster::error::ClusterError) -> Self {
        Self::Execution(e.to_string())
    }
}
