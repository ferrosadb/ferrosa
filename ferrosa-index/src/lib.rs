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

pub use filtered::{
    evaluate_clause, evaluate_predicate, evaluate_predicate_row, query_clause_implies,
    query_constraint_implies_predicate, query_constraint_implies_predicate_clause,
};
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

/// A single comparison clause of a (possibly multi-column) filter predicate:
/// `column_position <op> value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterClause {
    /// The column position to filter on.
    pub column_position: usize,
    /// The comparison operator.
    pub op: FilterOp,
    /// The value to compare against (storage encoding).
    pub value: Vec<u8>,
}

impl FilterClause {
    /// Construct a single clause.
    pub fn new(column_position: usize, op: FilterOp, value: Vec<u8>) -> Self {
        Self {
            column_position,
            op,
            value,
        }
    }
}

/// A filter predicate applied during index building: a **conjunction** of one
/// or more [`FilterClause`]s. A row is retained only when EVERY clause holds
/// (logical AND). A single-clause predicate is the common case and is preserved
/// for backward compatibility on the wire (see this type's custom `Deserialize`,
/// which still accepts the legacy flat single-clause JSON).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FilterPredicate {
    /// Wire format version. `1` is the legacy single-clause flat shape (decoded
    /// transparently); `2` is the conjunction shape serialized here.
    pub version: u8,
    /// The conjoined clauses. Non-empty for a well-formed predicate.
    pub clauses: Vec<FilterClause>,
}

/// Current serialized wire version for the conjunction shape.
const FILTER_PREDICATE_VERSION: u8 = 2;

impl FilterPredicate {
    /// Construct a single-column predicate (the common case).
    pub fn single(column_position: usize, op: FilterOp, value: Vec<u8>) -> Self {
        Self {
            version: FILTER_PREDICATE_VERSION,
            clauses: vec![FilterClause::new(column_position, op, value)],
        }
    }

    /// Construct a conjunction predicate from clauses. The caller provides at
    /// least one clause; an empty conjunction retains nothing useful and is
    /// rejected by [`evaluate_predicate`].
    pub fn conjunction(clauses: Vec<FilterClause>) -> Self {
        Self {
            version: FILTER_PREDICATE_VERSION,
            clauses,
        }
    }

    /// The conjoined clauses.
    pub fn clauses(&self) -> &[FilterClause] {
        &self.clauses
    }

    /// Serialize this predicate to a JSON string suitable for stashing in an
    /// index `options` map (the clause `value` bytes are already in storage
    /// encoding, so the round-trip is exact and type-system independent).
    pub fn to_option_string(&self) -> IndexResult<String> {
        serde_json::to_string(self).map_err(IndexError::from)
    }

    /// Reconstruct a predicate from the JSON produced by
    /// [`to_option_string`](Self::to_option_string), OR from the legacy
    /// single-clause flat shape (`{"column_position":..,"op":..,"value":..}`).
    /// Returns `None` when the string is absent or does not deserialize, so
    /// callers can fail safe.
    pub fn from_option_string(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }
}

impl<'de> Deserialize<'de> for FilterPredicate {
    /// Accept BOTH wire shapes:
    /// - conjunction (v2): `{"version":2,"clauses":[{...},...]}`
    /// - legacy single clause (v1): `{"column_position":..,"op":..,"value":..}`
    ///
    /// The legacy shape is the exact JSON the original single-clause
    /// `FilterPredicate` serialized, so old `system_schema.indexes` rows and
    /// in-flight build requests keep deserializing after the upgrade.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            // v2 fields
            version: Option<u8>,
            clauses: Option<Vec<FilterClause>>,
            // v1 (legacy flat single-clause) fields
            column_position: Option<usize>,
            op: Option<FilterOp>,
            value: Option<Vec<u8>>,
        }

        let wire = Wire::deserialize(deserializer)?;
        if let Some(clauses) = wire.clauses {
            return Ok(FilterPredicate {
                version: wire.version.unwrap_or(FILTER_PREDICATE_VERSION),
                clauses,
            });
        }
        match (wire.column_position, wire.op, wire.value) {
            (Some(column_position), Some(op), Some(value)) => {
                Ok(FilterPredicate::single(column_position, op, value))
            }
            _ => Err(serde::de::Error::custom(
                "FilterPredicate JSON has neither a `clauses` array (v2) nor a legacy \
                 `column_position`/`op`/`value` triple (v1)",
            )),
        }
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

#[cfg(test)]
mod bincode_compat_tests {
    use super::*;

    #[test]
    fn bincode_roundtrip_single_clause_predicate() {
        let predicate = FilterPredicate::single(2, FilterOp::Eq, b"active".to_vec());
        let encoded = bincode::serialize(&predicate).expect("serialize");
        let decoded: FilterPredicate = bincode::deserialize(&encoded).expect("deserialize");
        assert_eq!(decoded, predicate);
    }

    #[test]
    fn bincode_roundtrip_conjunction_predicate() {
        let predicate = FilterPredicate::conjunction(vec![
            FilterClause::new(1, FilterOp::Gt, vec![0, 0, 0, 21]),
            FilterClause::new(2, FilterOp::Eq, b"active".to_vec()),
        ]);
        let encoded = bincode::serialize(&predicate).expect("serialize");
        let decoded: FilterPredicate = bincode::deserialize(&encoded).expect("deserialize");
        assert_eq!(decoded, predicate);
    }

    #[test]
    fn bincode_decodes_legacy_flat_predicate() {
        #[derive(serde::Serialize)]
        struct LegacyFilterPredicate {
            column_position: usize,
            op: FilterOp,
            value: Vec<u8>,
        }
        let legacy = LegacyFilterPredicate {
            column_position: 3,
            op: FilterOp::Eq,
            value: b"active".to_vec(),
        };
        let legacy_bytes = bincode::serialize(&legacy).expect("serialize legacy");
        let decoded: FilterPredicate =
            bincode::deserialize(&legacy_bytes).expect("deserialize legacy into new FilterPredicate");
        assert_eq!(decoded.clauses().len(), 1);
        let clause = &decoded.clauses()[0];
        assert_eq!(clause.column_position, 3);
        assert!(matches!(clause.op, FilterOp::Eq));
        assert_eq!(clause.value, b"active");
    }

    #[test]
    fn bincode_index_type_variant_tag_stability() {
        let cases: &[(IndexType, u32)] = &[
            (IndexType::BTree, 0),
            (IndexType::Hash, 1),
            (IndexType::Composite, 2),
            (IndexType::Phonetic, 3),
            (IndexType::Filtered, 4),
            (IndexType::Vector, 5),
            (IndexType::FullText, 6),
            (IndexType::Geo, 7),
        ];
        for (variant, expected_tag) in cases {
            let bytes = bincode::serialize(variant).expect("serialize");
            let tag = u32::from_le_bytes(bytes[..4].try_into().unwrap());
            assert_eq!(tag, *expected_tag, "IndexType variant {:?} has wrong tag", variant);
        }
    }
}
