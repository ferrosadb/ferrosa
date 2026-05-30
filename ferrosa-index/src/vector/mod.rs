//! Vector similarity search: distance functions, HNSW, and IVFFlat indexes.
//!
//! # Distance Functions
//!
//! Three distance metrics are supported:
//! - **L2** (Euclidean): `sqrt(sum((a_i - b_i)^2))`
//! - **Cosine**: `1 - dot(a,b) / (||a|| * ||b||)`
//! - **Inner Product**: `-dot(a, b)` (negated so lower = more similar)
//!
//! # Dimension Limits
//!
//! PostgreSQL/pgvector uses 2000 dimensions; we allow up to 4096 for f32
//! vectors and 8192 for f16 vectors, with a performance warning above 2048.

pub mod hnsw;
pub mod ivfflat;
pub mod quantized;

use std::path::Path;

use ferrosa_common::CellValue;

// Re-export shared types from crate root that vector code needs.
pub use crate::{bytes_to_vec_f32, vec_f32_to_bytes, DistanceMetric, IndexError};

// ---------------------------------------------------------------------------
// Dimension constants
// ---------------------------------------------------------------------------

/// Maximum supported dimensions for f32 vectors.
pub const VECTOR_MAX_DIMENSIONS_F32: u32 = 4096;

/// Maximum supported dimensions for f16 vectors.
pub const VECTOR_MAX_DIMENSIONS_F16: u32 = 8192;

/// Dimensions above this threshold trigger a performance warning.
pub const VECTOR_PERF_WARNING_THRESHOLD: u32 = 2048;

// ---------------------------------------------------------------------------
// Vector-specific types (separate from secondary index types in crate root)
// ---------------------------------------------------------------------------

/// A pointer to a row's position within a data file (byte offset).
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

/// Generation-aware identity for a vector result row during multi-source merges.
///
/// Row offsets are only unique within one source. Persisted vector sidecars from
/// different SSTable generations can legitimately both return offset 0, so ANN
/// merge/dedup keys must include the generation whenever the row came from an
/// SSTable. Memtable rows use `generation = None` because they have not been
/// assigned a persisted SSTable generation yet.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct VectorRowRef {
    /// SSTable generation for persisted sidecar rows; `None` for active memtable rows.
    pub generation: Option<u64>,
    /// Byte offset or in-memory placeholder offset reported by the vector index.
    pub offset: u64,
}

impl VectorRowRef {
    /// Identity for an active memtable result that does not yet have an SSTable generation.
    pub fn memtable(position: RowPosition) -> Self {
        Self {
            generation: None,
            offset: position.offset,
        }
    }

    /// Identity for a persisted SSTable sidecar result.
    pub fn sstable(generation: u64, position: RowPosition) -> Self {
        Self {
            generation: Some(generation),
            offset: position.offset,
        }
    }
}

/// A single result from a vector index query, pairing a row position with a score.
///
/// For distance-based searches lower scores are better; for point lookups the
/// score is typically 0.0.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexResult {
    pub position: RowPosition,
    pub score: f32,
}

/// Capabilities that a vector index reader may advertise.
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
// Vector index traits
// ---------------------------------------------------------------------------

/// Factory for creating vector index builders and readers.
pub trait IndexFactory: Send + Sync {
    /// Create a builder that will write a vector index to `dir`.
    fn create_builder(&self, dir: &Path) -> Result<Box<dyn IndexBuilder>, IndexError>;

    /// Open a previously built vector index from `dir` for reading.
    fn open_reader(&self, dir: &Path) -> Result<Box<dyn IndexReader>, IndexError>;
}

/// Incrementally builds a vector index from row data, then finalises it to disk.
pub trait IndexBuilder: Send {
    /// Add one row to the index.
    fn add_row(&mut self, position: RowPosition, value: &CellValue) -> Result<(), IndexError>;

    /// Finalize the index, flushing all data to disk.
    fn finish(&mut self) -> Result<(), IndexError>;
}

/// Reads a previously built vector index and answers queries.
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
// Distance functions
// ---------------------------------------------------------------------------

/// Euclidean (L2) distance between two vectors.
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Cosine distance: `1 - cosine_similarity(a, b)`.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    let denom = norm_a * norm_b;
    if denom == 0.0 {
        return 1.0;
    }
    1.0 - dot / denom
}

/// Negative inner (dot) product distance: `-dot(a, b)`.
pub fn inner_product_distance(a: &[f32], b: &[f32]) -> f32 {
    -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
}

/// Compute the distance between two vectors using the specified metric.
pub fn distance(metric: &DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        DistanceMetric::L2 => l2_distance(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::InnerProduct => inner_product_distance(a, b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_same_vector_is_zero() {
        let v = vec![1.0, 2.0, 3.0];
        assert_eq!(l2_distance(&v, &v), 0.0);
    }

    #[test]
    fn l2_known_value_3_4_triangle() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        assert!((l2_distance(&a, &b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn l2_unit_difference() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 0.0, 0.0];
        assert!((l2_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal_vectors_equal_one() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_same_direction_approx_zero() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![2.0, 4.0, 6.0];
        assert!(cosine_distance(&a, &b).abs() < 1e-6);
    }

    #[test]
    fn cosine_opposite_direction_approx_two() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        assert!((cosine_distance(&a, &b) - 2.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_zero_vector_returns_one() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 2.0, 3.0];
        assert_eq!(cosine_distance(&a, &b), 1.0);
    }

    #[test]
    fn cosine_both_zero_vectors_returns_one() {
        let a = vec![0.0, 0.0];
        let b = vec![0.0, 0.0];
        assert_eq!(cosine_distance(&a, &b), 1.0);
    }

    #[test]
    fn inner_product_known_value() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        assert!((inner_product_distance(&a, &b) - (-32.0)).abs() < 1e-6);
    }

    #[test]
    fn inner_product_orthogonal_is_zero() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        assert_eq!(inner_product_distance(&a, &b), 0.0);
    }

    #[test]
    fn distance_dispatch_l2() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        assert!((distance(&DistanceMetric::L2, &a, &b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn distance_dispatch_cosine() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        assert!((distance(&DistanceMetric::Cosine, &a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn distance_dispatch_inner_product() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        assert!((distance(&DistanceMetric::InnerProduct, &a, &b) - (-32.0)).abs() < 1e-6);
    }

    #[test]
    fn dimension_constants_exist() {
        assert_eq!(VECTOR_MAX_DIMENSIONS_F32, 4096);
        assert_eq!(VECTOR_MAX_DIMENSIONS_F16, 8192);
        assert_eq!(VECTOR_PERF_WARNING_THRESHOLD, 2048);
    }
}
