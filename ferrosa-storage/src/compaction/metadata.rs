//! Metadata types for compaction decisions.

use std::path::PathBuf;

/// Lightweight metadata about an SSTable, used for compaction strategy decisions
/// without reading the SSTable itself.
#[derive(Debug, Clone)]
pub struct SSTableMetadata {
    /// Unique identifier for this SSTable.
    pub id: String,
    /// Path to the SSTable directory on disk.
    pub path: PathBuf,
    /// Total size in bytes (all components).
    pub size_bytes: u64,
    /// Minimum token in this SSTable.
    pub min_token: i64,
    /// Maximum token in this SSTable.
    pub max_token: i64,
    /// Minimum cell timestamp.
    pub min_timestamp: i64,
    /// Maximum cell timestamp.
    pub max_timestamp: i64,
    /// Number of partitions in this SSTable.
    pub partition_count: u64,
}

/// A compaction task: the set of input SSTables to merge into a single output.
#[derive(Debug, Clone)]
pub struct CompactionTask {
    /// SSTables to merge.
    pub inputs: Vec<SSTableMetadata>,
    /// Directory to write the output SSTable.
    pub output_dir: PathBuf,
}
