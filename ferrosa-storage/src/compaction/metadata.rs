//! Metadata types for compaction decisions.

use std::path::PathBuf;

use ferrosa_common::schema::TableSchema;

use crate::TableId;

/// Lightweight metadata about an SSTable, used for compaction strategy decisions
/// without reading the SSTable itself.
#[derive(Debug, Clone)]
pub struct SSTableMetadata {
    /// Unique identifier for this SSTable (file generation number).
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
    /// `true` when this SSTable's key bounds are NOT byte-comparable-decodable
    /// (legacy Cassandra-format file). Such files store a wide partition's rows
    /// in an order the streaming fragment read path mis-handles, so they must be
    /// rewritten (compacted) into byte-comparable order regardless of size tier.
    /// See `legacy_rewrite_tasks` and t_a0f922a3.
    pub legacy_format: bool,
}

/// A compaction task: the set of input SSTables to merge into a single output.
#[derive(Debug, Clone)]
pub struct CompactionTask {
    /// SSTables to merge.
    pub inputs: Vec<SSTableMetadata>,
    /// Directory to write the output SSTable.
    pub output_dir: PathBuf,
    /// Schema of the table being compacted; used to build the SerializationHeader.
    pub schema: TableSchema,
    /// Identifies which table this compaction is for, so `poll_compactions()`
    /// can route the result to the correct `TableStore`.
    pub table_id: TableId,
}
