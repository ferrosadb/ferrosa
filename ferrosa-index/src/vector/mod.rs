//! Vector distance functions and index implementations.
//!
//! Provides L2, cosine, and inner product distance metrics along with
//! HNSW and IVFFlat approximate nearest-neighbor index structures.

pub mod hnsw;
pub mod ivfflat;

use crate::DistanceMetric;

/// Maximum number of f32 dimensions supported.
pub const VECTOR_MAX_DIMENSIONS_F32: u32 = 4096;

/// Maximum number of f16 dimensions supported.
pub const VECTOR_MAX_DIMENSIONS_F16: u32 = 8192;

/// Dimension count above which a performance warning should be emitted.
pub const VECTOR_PERF_WARNING_THRESHOLD: u32 = 2048;

/// Squared Euclidean (L2) distance between two vectors.
///
/// Returns the sum of squared differences. Panics if lengths differ.
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "vector dimensions must match");
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum()
}

/// Cosine distance between two vectors: `1 - cosine_similarity`.
///
/// Returns 1.0 if either vector has zero magnitude. Panics if lengths differ.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "vector dimensions must match");
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 {
        1.0
    } else {
        1.0 - dot / denom
    }
}

/// Negative inner product distance: `-dot(a, b)`.
///
/// This formulation makes larger inner products correspond to smaller distances,
/// which is the convention used by pgvector and Cassandra SAI. Panics if lengths differ.
pub fn inner_product_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "vector dimensions must match");
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    -dot
}

/// Compute distance using the specified metric.
pub fn distance(metric: &DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        DistanceMetric::L2 => l2_distance(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::InnerProduct => inner_product_distance(a, b),
    }
}

/// Parse a byte slice of little-endian f32 values into a vector.
pub fn bytes_to_vector(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_identical_vectors() {
        let a = vec![1.0, 2.0, 3.0];
        assert_eq!(l2_distance(&a, &a), 0.0);
    }

    #[test]
    fn l2_known_distance() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        // L2 squared = 9 + 16 = 25
        assert!((l2_distance(&a, &b) - 25.0).abs() < 1e-6);
    }

    #[test]
    fn l2_negative_components() {
        let a = vec![-1.0, -2.0];
        let b = vec![1.0, 2.0];
        // (2)^2 + (4)^2 = 4 + 16 = 20
        assert!((l2_distance(&a, &b) - 20.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_identical_vectors() {
        let a = vec![1.0, 2.0, 3.0];
        assert!(cosine_distance(&a, &a).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal_vectors() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_opposite_vectors() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        assert!((cosine_distance(&a, &b) - 2.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_zero_vector() {
        let a = vec![0.0, 0.0];
        let b = vec![1.0, 0.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn inner_product_known() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        // dot = 4+10+18 = 32, distance = -32
        assert!((inner_product_distance(&a, &b) - (-32.0)).abs() < 1e-6);
    }

    #[test]
    fn inner_product_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        assert!((inner_product_distance(&a, &b)).abs() < 1e-6);
    }

    #[test]
    fn distance_dispatch() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        assert_eq!(distance(&DistanceMetric::L2, &a, &b), l2_distance(&a, &b));
        assert_eq!(
            distance(&DistanceMetric::Cosine, &a, &b),
            cosine_distance(&a, &b)
        );
        assert_eq!(
            distance(&DistanceMetric::InnerProduct, &a, &b),
            inner_product_distance(&a, &b)
        );
    }

    #[test]
    fn bytes_to_vector_roundtrip() {
        let original = vec![1.0f32, -2.5, 3.125, 0.0];
        let bytes: Vec<u8> = original.iter().flat_map(|f| f.to_le_bytes()).collect();
        let recovered = bytes_to_vector(&bytes);
        assert_eq!(original, recovered);
    }

    #[test]
    fn bytes_to_vector_empty() {
        let recovered = bytes_to_vector(&[]);
        assert!(recovered.is_empty());
    }

    #[test]
    fn bytes_to_vector_ignores_trailing() {
        // 5 bytes -> only 1 complete f32 (4 bytes), trailing byte ignored
        let val = 42.0f32;
        let mut bytes = val.to_le_bytes().to_vec();
        bytes.push(0xFF);
        let recovered = bytes_to_vector(&bytes);
        assert_eq!(recovered, vec![42.0]);
    }

    #[test]
    #[should_panic(expected = "vector dimensions must match")]
    fn l2_mismatched_dimensions() {
        l2_distance(&[1.0, 2.0], &[1.0]);
    }

    #[test]
    #[should_panic(expected = "vector dimensions must match")]
    fn cosine_mismatched_dimensions() {
        cosine_distance(&[1.0, 2.0], &[1.0]);
    }

    #[test]
    #[should_panic(expected = "vector dimensions must match")]
    fn inner_product_mismatched_dimensions() {
        inner_product_distance(&[1.0, 2.0], &[1.0]);
    }

    #[test]
    fn constants_are_reasonable() {
        // Verify the dimension limits form a valid hierarchy
        let f32_max = VECTOR_MAX_DIMENSIONS_F32;
        let f16_max = VECTOR_MAX_DIMENSIONS_F16;
        let perf_warn = VECTOR_PERF_WARNING_THRESHOLD;
        assert!(f32_max <= f16_max);
        assert!(perf_warn <= f32_max);
    }
}
