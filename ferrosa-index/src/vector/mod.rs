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

use crate::DistanceMetric;

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
// Distance functions
// ---------------------------------------------------------------------------

/// Euclidean (L2) distance between two vectors.
///
/// Returns `sqrt(sum((a_i - b_i)^2))`.
///
/// # Panics
///
/// Does not panic but produces meaningless results if `a` and `b` differ in
/// length (the shorter is zero-padded implicitly by `zip`).
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Cosine distance: `1 - cosine_similarity(a, b)`.
///
/// Returns a value in `[0, 2]` where 0 means identical direction and 2 means
/// opposite direction. Returns 1.0 if either vector has zero norm.
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
///
/// Negated so that "more similar" (larger dot product) maps to a *lower*
/// distance value, consistent with the convention that lower = better.
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

    // -----------------------------------------------------------------------
    // L2 distance
    // -----------------------------------------------------------------------

    #[test]
    fn l2_same_vector_is_zero() {
        let v = vec![1.0, 2.0, 3.0];
        assert_eq!(l2_distance(&v, &v), 0.0);
    }

    #[test]
    fn l2_known_value_3_4_triangle() {
        // Distance between (0,0) and (3,4) = 5.0
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

    // -----------------------------------------------------------------------
    // Cosine distance
    // -----------------------------------------------------------------------

    #[test]
    fn cosine_orthogonal_vectors_equal_one() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_same_direction_approx_zero() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![2.0, 4.0, 6.0]; // same direction, different magnitude
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

    // -----------------------------------------------------------------------
    // Inner product distance
    // -----------------------------------------------------------------------

    #[test]
    fn inner_product_known_value() {
        // dot([1,2,3], [4,5,6]) = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        // inner_product_distance = -32
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

    // -----------------------------------------------------------------------
    // dispatch
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Constants exist
    // -----------------------------------------------------------------------

    #[test]
    fn dimension_constants_exist() {
        assert_eq!(VECTOR_MAX_DIMENSIONS_F32, 4096);
        assert_eq!(VECTOR_MAX_DIMENSIONS_F16, 8192);
        assert_eq!(VECTOR_PERF_WARNING_THRESHOLD, 2048);
    }
}
