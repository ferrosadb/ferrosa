//! Secondary and vector index implementations for Ferrosa.
//!
//! This crate provides:
//! - Core index traits ([`IndexFactory`], [`IndexBuilder`], [`IndexReader`])
//! - Distance metrics and vector distance functions
//! - HNSW (Hierarchical Navigable Small World) graph index for ANN search
//! - IVFFlat (Inverted File with Flat vectors) index for ANN search
//!
//! # Vector Storage
//!
//! Vectors are stored in [`CellValue`](ferrosa_common::CellValue) as raw bytes:
//! each `f32` component serialized via `f32::to_le_bytes()`, concatenated.

pub mod vector;

use std::fmt;
use std::path::Path;

use ferrosa_common::CellValue;

// ---------------------------------------------------------------------------
// RowPosition — locates a row within an SSTable / data file
// ---------------------------------------------------------------------------

/// A pointer to a row's position within a data file.
///
/// Used by index readers to map index entries back to the underlying row data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RowPosition {
    /// Byte offset of the row within the data file.
    pub offset: u64,
}

impl RowPosition {
    pub fn new(offset: u64) -> Self {
        Self { offset }
    }
}

// ---------------------------------------------------------------------------
// DistanceMetric — which distance function to use for vector search
// ---------------------------------------------------------------------------

/// Distance metric for vector similarity search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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

// ---------------------------------------------------------------------------
// IndexCapability — what an index can do
// ---------------------------------------------------------------------------

/// Capabilities that an index reader may advertise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexCapability {
    /// Point lookup by exact value.
    Lookup,
    /// Ordered range scan.
    Range,
    /// Approximate nearest-neighbor search on vectors.
    Nearest,
}

// ---------------------------------------------------------------------------
// IndexResult — a single result from an index query
// ---------------------------------------------------------------------------

/// A single result from an index query, pairing a row position with a score.
///
/// For distance-based searches lower scores are better; for point lookups the
/// score is typically 0.0.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexResult {
    pub position: RowPosition,
    pub score: f32,
}

// ---------------------------------------------------------------------------
// IndexError
// ---------------------------------------------------------------------------

/// Errors that index operations may produce.
#[derive(Debug)]
pub enum IndexError {
    /// The requested operation is not supported by this index type.
    Unsupported(String),
    /// An I/O error occurred during index build or read.
    Io(std::io::Error),
    /// Data format / serialization error.
    Format(String),
    /// Dimension mismatch between query vector and indexed vectors.
    DimensionMismatch { expected: usize, got: usize },
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexError::Unsupported(msg) => write!(f, "unsupported: {msg}"),
            IndexError::Io(e) => write!(f, "io: {e}"),
            IndexError::Format(msg) => write!(f, "format: {msg}"),
            IndexError::DimensionMismatch { expected, got } => {
                write!(f, "dimension mismatch: expected {expected}, got {got}")
            }
        }
    }
}

impl std::error::Error for IndexError {}

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

// ---------------------------------------------------------------------------
// Core traits
// ---------------------------------------------------------------------------

/// Creates index builders and readers for a particular index type.
pub trait IndexFactory: Send + Sync {
    /// Create a builder that will write an index to `dir`.
    fn create_builder(&self, dir: &Path) -> Result<Box<dyn IndexBuilder>, IndexError>;

    /// Open a previously built index from `dir` for reading.
    fn open_reader(&self, dir: &Path) -> Result<Box<dyn IndexReader>, IndexError>;
}

/// Incrementally builds an index from row data, then finalises it to disk.
pub trait IndexBuilder: Send {
    /// Add one row to the index.
    ///
    /// `position` identifies the row in the underlying data file.
    /// `value` is the cell from the indexed column.
    fn add_row(&mut self, position: RowPosition, value: &CellValue) -> Result<(), IndexError>;

    /// Finalize the index, flushing all data to disk.
    fn finish(&mut self) -> Result<(), IndexError>;
}

/// Reads a previously built index and answers queries.
pub trait IndexReader: Send + Sync {
    /// Which capabilities this reader supports.
    fn capabilities(&self) -> Vec<IndexCapability>;

    /// Point lookup: find rows whose indexed value equals `value`.
    fn lookup(&self, value: &CellValue) -> Result<Vec<IndexResult>, IndexError>;

    /// Range scan: find rows whose indexed value is in `[start, end]`.
    fn range(&self, start: &CellValue, end: &CellValue) -> Result<Vec<IndexResult>, IndexError>;

    /// Nearest-neighbor search: find the `k` closest vectors to `query`.
    ///
    /// `ef_search` controls the size of the candidate list (higher = more
    /// accurate but slower). Implementations may ignore it if not applicable.
    fn nearest(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<IndexResult>, IndexError>;
}

// ---------------------------------------------------------------------------
// Helpers: vector ↔ bytes conversion
// ---------------------------------------------------------------------------

/// Decode a byte slice into a vector of f32 values (little-endian).
pub fn bytes_to_vec_f32(bytes: &[u8]) -> Result<Vec<f32>, IndexError> {
    if !bytes.len().is_multiple_of(4) {
        return Err(IndexError::Format(format!(
            "vector byte length {} is not a multiple of 4",
            bytes.len()
        )));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

/// Encode a vector of f32 values into bytes (little-endian).
pub fn vec_f32_to_bytes(vec: &[f32]) -> Vec<u8> {
    vec.iter().flat_map(|f| f.to_le_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_position_new() {
        let pos = RowPosition::new(42);
        assert_eq!(pos.offset, 42);
    }

    #[test]
    fn distance_metric_display() {
        assert_eq!(DistanceMetric::L2.to_string(), "l2");
        assert_eq!(DistanceMetric::Cosine.to_string(), "cosine");
        assert_eq!(DistanceMetric::InnerProduct.to_string(), "inner_product");
    }

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
}
