//! Secondary and vector index implementations for Ferrosa.
//!
//! This crate provides B-tree, hash, composite, phonetic, filtered, and vector
//! secondary indexes that map column values to row positions within SSTables.
//!
//! # Secondary Indexes
//!
//! - **B-tree** ([`btree`]): Sorted index supporting point lookups and range scans.
//! - **Hash** ([`hash`]): Hash-based index for O(1) point lookups.
//! - **Composite** ([`composite`]): Multi-column index supporting full-key and prefix lookups.
//! - **Phonetic** ([`phonetic`]): Fuzzy name matching via Soundex, Metaphone, Double Metaphone,
//!   or Caverphone encoding.
//! - **Filtered** ([`filtered`]): Wraps another index, applying a predicate to filter rows
//!   during build.
//!
//! # Vector Indexes
//!
//! - **HNSW** ([`vector::hnsw`]): Hierarchical Navigable Small World graph for ANN search.
//! - **IVFFlat** ([`vector::ivfflat`]): Inverted File with Flat vectors for ANN search.

pub mod btree;
pub mod composite;
pub mod filtered;
pub mod fulltext;
pub mod geo;
pub mod hash;
pub mod phonetic;
pub mod vector;

pub use filtered::evaluate_predicate;
pub use phonetic::PhoneticAlgorithm;

use ferrosa_common::CellValue;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Bound;
use std::path::PathBuf;

// ── Error types ──────────────────────────────────────────────────────────────

/// Errors produced by index operations.
#[derive(Debug)]
pub enum IndexError {
    /// An I/O error from the filesystem.
    Io(std::io::Error),
    /// The requested operation is not supported by this index type.
    Unsupported(String),
    /// The index data is corrupt or cannot be deserialized.
    Corrupt(String),
    /// A required column was missing from the row.
    MissingColumn(usize),
    /// Data format / serialization error.
    Format(String),
    /// Dimension mismatch between query vector and indexed vectors.
    DimensionMismatch { expected: usize, got: usize },
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexError::Io(e) => write!(f, "index I/O error: {e}"),
            IndexError::Unsupported(msg) => write!(f, "unsupported: {msg}"),
            IndexError::Corrupt(msg) => write!(f, "corrupt index: {msg}"),
            IndexError::MissingColumn(idx) => write!(f, "missing column at position {idx}"),
            IndexError::Format(msg) => write!(f, "format: {msg}"),
            IndexError::DimensionMismatch { expected, got } => {
                write!(f, "dimension mismatch: expected {expected}, got {got}")
            }
        }
    }
}

impl std::error::Error for IndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IndexError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for IndexError {
    fn from(e: std::io::Error) -> Self {
        IndexError::Io(e)
    }
}

impl From<serde_json::Error> for IndexError {
    fn from(e: serde_json::Error) -> Self {
        IndexError::Format(e.to_string())
    }
}

/// Convenience alias for index results.
pub type IndexResult<T> = Result<T, IndexError>;

// ── Core types ───────────────────────────────────────────────────────────────

/// Identifies which type of index to build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    Composite,
    Phonetic,
    Filtered,
    Vector,
    FullText,
    Geo,
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexType::BTree => write!(f, "btree"),
            IndexType::Hash => write!(f, "hash"),
            IndexType::Composite => write!(f, "composite"),
            IndexType::Phonetic => write!(f, "phonetic"),
            IndexType::Vector => write!(f, "vector"),
            IndexType::Filtered => write!(f, "filtered"),
            IndexType::FullText => write!(f, "fulltext"),
            IndexType::Geo => write!(f, "geo"),
        }
    }
}

/// Distance metric for vector similarity search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance.
    L2,
    /// Cosine distance: `1 - cosine_similarity`.
    Cosine,
    /// Negative inner (dot) product: `-dot(a, b)`.
    InnerProduct,
}

impl fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DistanceMetric::L2 => f.write_str("l2"),
            DistanceMetric::Cosine => f.write_str("cosine"),
            DistanceMetric::InnerProduct => f.write_str("inner_product"),
        }
    }
}

/// The key bytes used for index lookups.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexKey(pub Vec<u8>);

/// Position of a row within an SSTable, identified by partition key and
/// optional clustering key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowPosition {
    /// Partition key bytes.
    pub partition_key: Vec<u8>,
    /// Clustering key bytes (empty if the table has no clustering columns).
    pub clustering_key: Vec<u8>,
}

/// Metadata about a single file that belongs to an index.
#[derive(Debug, Clone)]
pub struct IndexFileMeta {
    /// Path to the index file on disk.
    pub path: PathBuf,
    /// Size in bytes.
    pub size: u64,
}

/// Collection of files that make up an index on disk.
#[derive(Debug, Clone)]
pub struct IndexFiles {
    /// The primary data file.
    pub data: IndexFileMeta,
}

/// Configuration for building an index.
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Which type of index to build.
    pub index_type: IndexType,
    /// Column positions within each row to index. For single-column indexes
    /// this has one element; for composite indexes it has multiple.
    pub column_positions: Vec<usize>,
    /// Directory where index files will be written.
    pub output_dir: PathBuf,
    /// A name for the index (used in file naming).
    pub name: String,
}

/// Bit flags describing what an index can do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexCapabilities(u32);

impl IndexCapabilities {
    pub const POINT_LOOKUP: Self = Self(0b0001);
    pub const RANGE_SCAN: Self = Self(0b0010);
    pub const PHONETIC: Self = Self(0b0100);

    /// Returns true if `self` includes all capabilities in `other`.
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for IndexCapabilities {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

// ── Traits ───────────────────────────────────────────────────────────────────

/// Builds an index by receiving rows one at a time, then writing the index
/// to disk.
pub trait IndexBuilder: Send {
    /// Add a row to the index.
    fn add_row(
        &mut self,
        partition_key: &[u8],
        clustering_key: &[u8],
        cells: &[CellValue],
        column_positions: &[usize],
    ) -> IndexResult<()>;

    /// Finalize the index and write it to disk. Returns file metadata.
    fn finish(self: Box<Self>) -> IndexResult<IndexFiles>;
}

/// Reads an index that has been written to disk.
pub trait IndexReader: Send + Sync {
    /// Look up all rows whose indexed column(s) exactly match `key`.
    fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>>;

    /// Return all rows whose indexed column(s) fall within `[start, end)`.
    fn range(
        &self,
        start: Bound<&IndexKey>,
        end: Bound<&IndexKey>,
    ) -> IndexResult<Vec<RowPosition>>;

    /// Return the nearest row(s) to the given key.
    fn nearest(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>>;

    /// Capabilities of this index.
    fn capabilities(&self) -> IndexCapabilities;
}

/// Factory for creating builders and readers for a specific index type.
pub trait IndexFactory: Send + Sync {
    /// Create a new builder that will write an index to `config.output_dir`.
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>>;

    /// Open an existing index from the given files.
    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>>;

    /// The type of index this factory produces.
    fn index_type(&self) -> IndexType;

    /// Capabilities of indexes produced by this factory.
    fn capabilities(&self) -> IndexCapabilities;
}

// ── Filter predicate types ───────────────────────────────────────────────────

/// Comparison operators for filter predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOp {
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
}

/// A filter predicate applied to a specific column during index building.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterPredicate {
    /// The column position to filter on.
    pub column_position: usize,
    /// The comparison operator.
    pub op: FilterOp,
    /// The value to compare against.
    pub value: Vec<u8>,
}

impl FilterPredicate {
    /// Serialize this predicate to a JSON string suitable for stashing in an
    /// index `options` map (the `value` bytes are already in storage encoding,
    /// so the round-trip is exact and type-system independent).
    pub fn to_option_string(&self) -> IndexResult<String> {
        serde_json::to_string(self).map_err(IndexError::from)
    }

    /// Reconstruct a predicate from the JSON produced by
    /// [`to_option_string`](Self::to_option_string). Returns `None` when the
    /// string is absent or does not deserialize, so callers can fail safe.
    pub fn from_option_string(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }
}

// ── Vector helpers ───────────────────────────────────────────────────────────

/// Decode a byte slice into a vector of f32 values.
///
/// Bytes are **big-endian**, matching the CQL native-protocol encoding of
/// `vector<float, N>` cells (`ferrosa_cql::types::encode_value`). Vector index
/// cells are populated straight from those stored cell bytes, so the codec must
/// agree with CQL or the index ranks byte-swapped garbage.
pub fn bytes_to_vec_f32(bytes: &[u8]) -> Result<Vec<f32>, IndexError> {
    if !bytes.len().is_multiple_of(4) {
        return Err(IndexError::Format(format!(
            "vector byte length {} is not a multiple of 4",
            bytes.len()
        )));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

/// Encode a vector of f32 values into **big-endian** bytes, matching the CQL
/// native-protocol `vector<float, N>` cell encoding (see [`bytes_to_vec_f32`]).
pub fn vec_f32_to_bytes(vec: &[f32]) -> Vec<u8> {
    vec.iter().flat_map(|f| f.to_be_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_to_vec_f32_roundtrip() {
        let original = vec![1.0_f32, 2.5, -3.0, 0.0];
        let bytes = vec_f32_to_bytes(&original);
        let decoded = bytes_to_vec_f32(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn bytes_to_vec_f32_bad_length() {
        let bytes = vec![0u8, 1, 2]; // 3 bytes, not multiple of 4
        assert!(bytes_to_vec_f32(&bytes).is_err());
    }

    #[test]
    fn bytes_to_vec_f32_decodes_cql_big_endian_cell_bytes() {
        // CQL `vector<float, N>` cells are stored big-endian (CQL native
        // protocol order), exactly as `ferrosa_cql::types::encode_value`
        // produces them. The index codec MUST agree, otherwise the memtable
        // vector index is populated with byte-swapped garbage and ANN ranking
        // is wrong. Build the cell bytes the way CQL does and confirm we
        // recover the original floats.
        let original = [1.0_f32, 0.0, -2.5];
        let mut cql_cell = Vec::new();
        for f in original {
            cql_cell.extend_from_slice(&f.to_be_bytes());
        }
        let decoded = bytes_to_vec_f32(&cql_cell).unwrap();
        assert_eq!(decoded, original.to_vec());
    }

    #[test]
    fn vec_f32_to_bytes_matches_cql_big_endian_encoding() {
        let v = [1.0_f32, 0.0, -2.5];
        let mut cql_cell = Vec::new();
        for f in v {
            cql_cell.extend_from_slice(&f.to_be_bytes());
        }
        assert_eq!(vec_f32_to_bytes(&v), cql_cell);
    }
}
