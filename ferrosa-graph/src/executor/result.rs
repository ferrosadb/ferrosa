//! Graph query result type.

use serde::Serialize;

/// Result of a graph query execution.
#[derive(Debug, Clone, Serialize)]
pub struct GraphResult {
    /// Column names.
    pub columns: Vec<String>,
    /// Rows of values (each row has one value per column).
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Execution statistics.
    pub stats: QueryStats,
}

/// Statistics for a graph query execution.
#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryStats {
    pub vertices_read: usize,
    pub edges_read: usize,
    pub vertices_written: usize,
    pub vertices_deleted: usize,
    pub execution_ms: u64,
}
