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
mod hnsw_binary;
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

/// Independent partial sums per distance. A plain `iter().sum::<f32>()` is
/// one serial chain of dependent adds, which the compiler may not reorder
/// (float addition is not associative), so it runs a lane at a time; eight
/// independent sums vectorize. Results can differ from the serial sum in the
/// last bits.
const LANES: usize = 8;

/// `Σ f(a[i], b[i])`, accumulated in [`LANES`] independent sums.
#[inline(always)]
fn lane_sum(a: &[f32], b: &[f32], f: impl Fn(f32, f32) -> f32) -> f32 {
    let len = a.len().min(b.len());
    let (a, b) = (&a[..len], &b[..len]);
    let mut acc = [0.0f32; LANES];
    let (a_blocks, a_rest) = a.as_chunks::<LANES>();
    let (b_blocks, b_rest) = b.as_chunks::<LANES>();
    for (xa, xb) in a_blocks.iter().zip(b_blocks) {
        for ((sum, &x), &y) in acc.iter_mut().zip(xa).zip(xb) {
            *sum += f(x, y);
        }
    }
    let rest: f32 = a_rest.iter().zip(b_rest).map(|(&x, &y)| f(x, y)).sum();
    acc.iter().sum::<f32>() + rest
}

/// Euclidean (L2) distance between two vectors.
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    lane_sum(a, b, |x, y| (x - y) * (x - y)).sqrt()
}

/// Cosine distance: `1 - cosine_similarity(a, b)`.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot = lane_sum(a, b, |x, y| x * y);
    let norm_a = lane_sum(a, a, |x, _| x * x).sqrt();
    let norm_b = lane_sum(b, b, |x, _| x * x).sqrt();
    let denom = norm_a * norm_b;
    if denom == 0.0 {
        return 1.0;
    }
    1.0 - dot / denom
}

/// Negative inner (dot) product distance: `-dot(a, b)`.
pub fn inner_product_distance(a: &[f32], b: &[f32]) -> f32 {
    -lane_sum(a, b, |x, y| x * y)
}

/// Compute the distance between two vectors using the specified metric.
pub fn distance(metric: &DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        DistanceMetric::L2 => l2_distance(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::InnerProduct => inner_product_distance(a, b),
    }
}

/// [`lane_sum`] with `a` given as little-endian `f32` bytes, read where they
/// lie: the shape of a vector in a mapped sidecar, which need not be 4-byte
/// aligned. Same lanes in the same order, so the same result.
#[inline(always)]
fn lane_sum_le(a: &[u8], b: &[f32], f: impl Fn(f32, f32) -> f32) -> f32 {
    let (a_words, _) = a.as_chunks::<4>();
    let len = a_words.len().min(b.len());
    let (a_words, b) = (&a_words[..len], &b[..len]);
    let mut acc = [0.0f32; LANES];
    let (a_blocks, a_rest) = a_words.as_chunks::<LANES>();
    let (b_blocks, b_rest) = b.as_chunks::<LANES>();
    for (xa, xb) in a_blocks.iter().zip(b_blocks) {
        for ((sum, &x), &y) in acc.iter_mut().zip(xa).zip(xb) {
            *sum += f(f32::from_le_bytes(x), y);
        }
    }
    let rest: f32 = a_rest
        .iter()
        .zip(b_rest)
        .map(|(&x, &y)| f(f32::from_le_bytes(x), y))
        .sum();
    acc.iter().sum::<f32>() + rest
}

/// [`distance`] from a stored vector, as little-endian `f32` bytes, to
/// `query`: bit-for-bit what `distance(metric, stored, query)` returns for
/// the decoded vector, without decoding it.
pub fn distance_le(metric: &DistanceMetric, stored: &[u8], query: &[f32]) -> f32 {
    match metric {
        DistanceMetric::L2 => lane_sum_le(stored, query, |x, y| (x - y) * (x - y)).sqrt(),
        DistanceMetric::Cosine => {
            let dot = lane_sum_le(stored, query, |x, y| x * y);
            let norm_stored = lane_sum_le(stored, query, |x, _| x * x).sqrt();
            let norm_query = lane_sum(query, query, |x, _| x * x).sqrt();
            let denom = norm_stored * norm_query;
            if denom == 0.0 {
                return 1.0;
            }
            1.0 - dot / denom
        }
        DistanceMetric::InnerProduct => -lane_sum_le(stored, query, |x, y| x * y),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The serial reference each lane-split metric must agree with: the
    /// formulas as first written, summed in index order in f64.
    fn reference(metric: DistanceMetric, a: &[f32], b: &[f32]) -> f64 {
        let pairs = a.iter().zip(b).map(|(&x, &y)| (f64::from(x), f64::from(y)));
        match metric {
            DistanceMetric::L2 => pairs.map(|(x, y)| (x - y).powi(2)).sum::<f64>().sqrt(),
            DistanceMetric::InnerProduct => -pairs.map(|(x, y)| x * y).sum::<f64>(),
            DistanceMetric::Cosine => {
                let (dot, na, nb) = pairs.fold((0.0, 0.0, 0.0), |(d, na, nb), (x, y)| {
                    (d + x * y, na + x * x, nb + y * y)
                });
                if na * nb == 0.0 {
                    1.0
                } else {
                    1.0 - dot / (na.sqrt() * nb.sqrt())
                }
            }
        }
    }

    /// Every length from empty through several lane widths plus a
    /// remainder, so each element is counted exactly once whether it falls
    /// in a full lane block or in the tail.
    #[test]
    fn every_metric_matches_the_serial_formula_at_every_length() {
        for len in 0..=67usize {
            let a: Vec<f32> = (0..len).map(|i| ((i * 7 % 13) as f32) - 6.0).collect();
            let b: Vec<f32> = (0..len)
                .map(|i| ((i * 5 % 11) as f32) * 0.5 - 2.0)
                .collect();
            for metric in [
                DistanceMetric::L2,
                DistanceMetric::Cosine,
                DistanceMetric::InnerProduct,
            ] {
                let got = f64::from(distance(&metric, &a, &b));
                let want = reference(metric, &a, &b);
                assert!(
                    (got - want).abs() <= 1e-4 * want.abs().max(1.0),
                    "{metric:?} at length {len}: got {got}, want {want}"
                );
            }
        }
    }

    /// A stored vector read in place gives exactly the distance its decoded
    /// copy does, so a search over a mapped sidecar ranks as a search over
    /// the decoded graph did. Bytes start off a word boundary on purpose.
    #[test]
    fn a_vector_read_in_place_scores_bit_for_bit_as_its_decoded_copy() {
        for len in 0..=67usize {
            let a: Vec<f32> = (0..len).map(|i| ((i * 7 % 13) as f32) - 6.25).collect();
            let b: Vec<f32> = (0..len)
                .map(|i| ((i * 5 % 11) as f32) * 0.5 - 2.0)
                .collect();
            let mut stored = vec![0u8];
            stored.extend(a.iter().flat_map(|x| x.to_le_bytes()));
            for metric in [
                DistanceMetric::L2,
                DistanceMetric::Cosine,
                DistanceMetric::InnerProduct,
            ] {
                let in_place = distance_le(&metric, &stored[1..], &b);
                let decoded = distance(&metric, &a, &b);
                assert_eq!(
                    in_place.to_bits(),
                    decoded.to_bits(),
                    "{metric:?} at length {len}: {in_place} vs {decoded}"
                );
            }
        }
    }

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
