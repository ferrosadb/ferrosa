//! IVFFlat (Inverted File with Flat vectors) index for approximate
//! nearest-neighbor search.
//!
//! IVFFlat partitions the vector space using k-means clustering:
//! - At build time, all vectors are accumulated, then k-means is run to
//!   compute `lists` centroids. Each vector is assigned to its nearest
//!   centroid.
//! - At query time, the `probes` nearest centroids are found, and a
//!   brute-force search is performed within those clusters.
//!
//! This is a simpler approach than HNSW with lower build cost but
//! potentially less accurate results (depending on `probes`).

use std::path::Path;

use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use crate::vector::distance;
use crate::{
    bytes_to_vec_f32, DistanceMetric, IndexBuilder, IndexCapability, IndexError, IndexFactory,
    IndexReader, IndexResult, RowPosition,
};
use ferrosa_common::CellValue;

// ---------------------------------------------------------------------------
// Serializable index data
// ---------------------------------------------------------------------------

/// A single entry in an inverted list: a row position and its vector.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IvfEntry {
    position: RowPosition,
    vector: Vec<f32>,
}

/// The serialized IVFFlat index, stored as JSON on disk.
#[derive(Debug, Serialize, Deserialize)]
struct IvfFlatData {
    /// Centroid vectors, one per cluster.
    centroids: Vec<Vec<f32>>,
    /// Inverted lists: `lists[i]` contains the entries assigned to centroid `i`.
    lists: Vec<Vec<IvfEntry>>,
    /// Number of clusters.
    num_lists: usize,
    /// Distance metric.
    metric: DistanceMetric,
    /// Vector dimensionality (0 if empty).
    dimensions: usize,
}

// ---------------------------------------------------------------------------
// K-means implementation
// ---------------------------------------------------------------------------

/// Run k-means clustering on the given vectors.
///
/// Returns `(centroids, assignments)` where `assignments[i]` is the cluster
/// index for `vectors[i]`.
fn kmeans(
    vectors: &[Vec<f32>],
    k: usize,
    metric: &DistanceMetric,
    max_iterations: usize,
) -> (Vec<Vec<f32>>, Vec<usize>) {
    if vectors.is_empty() || k == 0 {
        return (Vec::new(), Vec::new());
    }

    let k = k.min(vectors.len());
    let dim = vectors[0].len();

    // Random initialization: pick k distinct vectors as initial centroids
    let mut rng = rand::thread_rng();
    let mut indices: Vec<usize> = (0..vectors.len()).collect();
    indices.shuffle(&mut rng);
    let mut centroids: Vec<Vec<f32>> = indices[..k].iter().map(|&i| vectors[i].clone()).collect();

    let mut assignments = vec![0usize; vectors.len()];

    for _ in 0..max_iterations {
        // Assignment step: assign each vector to nearest centroid
        let mut changed = false;
        for (i, vec) in vectors.iter().enumerate() {
            let mut best_cluster = 0;
            let mut best_dist = f32::INFINITY;
            for (c, centroid) in centroids.iter().enumerate() {
                let d = distance(metric, vec, centroid);
                if d < best_dist {
                    best_dist = d;
                    best_cluster = c;
                }
            }
            if assignments[i] != best_cluster {
                assignments[i] = best_cluster;
                changed = true;
            }
        }

        if !changed {
            break;
        }

        // Update step: recompute centroids as mean of assigned vectors
        let mut sums = vec![vec![0.0f64; dim]; k];
        let mut counts = vec![0usize; k];

        for (i, vec) in vectors.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for (j, &val) in vec.iter().enumerate() {
                sums[c][j] += val as f64;
            }
        }

        for c in 0..k {
            if counts[c] > 0 {
                for j in 0..dim {
                    centroids[c][j] = (sums[c][j] / counts[c] as f64) as f32;
                }
            }
            // If a cluster is empty, keep its centroid unchanged
        }
    }

    (centroids, assignments)
}

// ---------------------------------------------------------------------------
// IvfFlatFactory
// ---------------------------------------------------------------------------

/// Factory for creating IVFFlat index builders and readers.
pub struct IvfFlatFactory {
    /// Number of clusters (inverted lists).
    pub lists: usize,
    /// Distance metric.
    pub metric: DistanceMetric,
}

impl IvfFlatFactory {
    pub fn new(lists: usize, metric: DistanceMetric) -> Self {
        Self { lists, metric }
    }
}

impl IndexFactory for IvfFlatFactory {
    fn create_builder(&self, dir: &Path) -> Result<Box<dyn IndexBuilder>, IndexError> {
        Ok(Box::new(IvfFlatBuilder {
            vectors: Vec::new(),
            positions: Vec::new(),
            num_lists: self.lists,
            metric: self.metric,
            dir: dir.to_path_buf(),
        }))
    }

    fn open_reader(&self, dir: &Path) -> Result<Box<dyn IndexReader>, IndexError> {
        let db_path = dir.join("ivfflat.db");
        let data_bytes = std::fs::read(&db_path)?;
        let data: IvfFlatData = serde_json::from_slice(&data_bytes)?;
        Ok(Box::new(IvfFlatReader { data }))
    }
}

// ---------------------------------------------------------------------------
// IvfFlatBuilder
// ---------------------------------------------------------------------------

/// Accumulates all vectors, then runs k-means on finish.
pub struct IvfFlatBuilder {
    vectors: Vec<Vec<f32>>,
    positions: Vec<RowPosition>,
    num_lists: usize,
    metric: DistanceMetric,
    dir: std::path::PathBuf,
}

impl IndexBuilder for IvfFlatBuilder {
    fn add_row(&mut self, position: RowPosition, value: &CellValue) -> Result<(), IndexError> {
        let bytes = value
            .value
            .as_ref()
            .ok_or_else(|| IndexError::Format("cannot index tombstone cell".to_string()))?;
        let vector = bytes_to_vec_f32(bytes)?;
        self.vectors.push(vector);
        self.positions.push(position);
        Ok(())
    }

    fn finish(&mut self) -> Result<(), IndexError> {
        std::fs::create_dir_all(&self.dir)?;

        let data = if self.vectors.is_empty() {
            IvfFlatData {
                centroids: Vec::new(),
                lists: Vec::new(),
                num_lists: self.num_lists,
                metric: self.metric,
                dimensions: 0,
            }
        } else {
            let dim = self.vectors[0].len();
            let (centroids, assignments) = kmeans(&self.vectors, self.num_lists, &self.metric, 100);
            let k = centroids.len();

            let mut lists: Vec<Vec<IvfEntry>> = vec![Vec::new(); k];
            for (i, (vec, &pos)) in self.vectors.iter().zip(self.positions.iter()).enumerate() {
                lists[assignments[i]].push(IvfEntry {
                    position: pos,
                    vector: vec.clone(),
                });
            }

            IvfFlatData {
                centroids,
                lists,
                num_lists: k,
                metric: self.metric,
                dimensions: dim,
            }
        };

        let json = serde_json::to_vec(&data)?;
        std::fs::write(self.dir.join("ivfflat.db"), json)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// IvfFlatReader
// ---------------------------------------------------------------------------

/// Reads a previously built IVFFlat index and answers nearest-neighbor queries.
pub struct IvfFlatReader {
    data: IvfFlatData,
}

impl IndexReader for IvfFlatReader {
    fn capabilities(&self) -> Vec<IndexCapability> {
        vec![IndexCapability::Nearest]
    }

    fn lookup(&self, _value: &CellValue) -> Result<Vec<IndexResult>, IndexError> {
        Err(IndexError::Unsupported(
            "IVFFlat index does not support point lookups".to_string(),
        ))
    }

    fn range(&self, _start: &CellValue, _end: &CellValue) -> Result<Vec<IndexResult>, IndexError> {
        Err(IndexError::Unsupported(
            "IVFFlat index does not support range scans".to_string(),
        ))
    }

    fn nearest(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<IndexResult>, IndexError> {
        if self.data.centroids.is_empty() {
            return Ok(Vec::new());
        }

        // Validate dimensions
        if query.len() != self.data.dimensions {
            return Err(IndexError::DimensionMismatch {
                expected: self.data.dimensions,
                got: query.len(),
            });
        }

        // Determine number of probes: default 1, but ef_search can override
        let probes = if ef_search > 0 {
            ef_search.min(self.data.centroids.len())
        } else {
            1
        };

        // Find the nearest `probes` centroids
        let mut centroid_dists: Vec<(f32, usize)> = self
            .data
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (distance(&self.data.metric, query, c), i))
            .collect();
        centroid_dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        // Brute-force search within the selected clusters
        let mut candidates: Vec<(f32, RowPosition)> = Vec::new();
        for &(_, cluster_idx) in centroid_dists.iter().take(probes) {
            for entry in &self.data.lists[cluster_idx] {
                let d = distance(&self.data.metric, query, &entry.vector);
                candidates.push((d, entry.position));
            }
        }

        // Sort by distance and return top k
        candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(k);

        Ok(candidates
            .into_iter()
            .map(|(score, position)| IndexResult { position, score })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vec_f32_to_bytes;

    fn make_vector_cell(v: &[f32]) -> CellValue {
        CellValue::live(vec_f32_to_bytes(v), 1)
    }

    #[test]
    fn ivfflat_build_and_nearest() {
        let dir = tempfile::tempdir().unwrap();
        // Two clear clusters: cluster A near (10, 0, 0), cluster B near (0, 10, 0)
        let factory = IvfFlatFactory::new(2, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();

        // Cluster A
        builder
            .add_row(RowPosition::new(0), &make_vector_cell(&[10.0, 0.0, 0.0]))
            .unwrap();
        builder
            .add_row(RowPosition::new(100), &make_vector_cell(&[10.1, 0.1, 0.0]))
            .unwrap();
        builder
            .add_row(RowPosition::new(200), &make_vector_cell(&[9.9, -0.1, 0.0]))
            .unwrap();

        // Cluster B
        builder
            .add_row(RowPosition::new(300), &make_vector_cell(&[0.0, 10.0, 0.0]))
            .unwrap();
        builder
            .add_row(RowPosition::new(400), &make_vector_cell(&[0.1, 10.1, 0.0]))
            .unwrap();
        builder
            .add_row(RowPosition::new(500), &make_vector_cell(&[-0.1, 9.9, 0.0]))
            .unwrap();

        builder.finish().unwrap();

        // Query near cluster A with probes=1
        let reader = factory.open_reader(dir.path()).unwrap();
        let results = reader.nearest(&[10.0, 0.0, 0.0], 2, 1).unwrap();
        assert_eq!(results.len(), 2);
        // Both results should be from cluster A (offsets 0, 100, or 200)
        for r in &results {
            assert!(
                r.position.offset <= 200,
                "expected cluster A result, got offset {}",
                r.position.offset
            );
        }

        // Query near cluster B with probes=1
        let results = reader.nearest(&[0.0, 10.0, 0.0], 2, 1).unwrap();
        assert_eq!(results.len(), 2);
        for r in &results {
            assert!(
                r.position.offset >= 300,
                "expected cluster B result, got offset {}",
                r.position.offset
            );
        }
    }

    #[test]
    fn ivfflat_empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let factory = IvfFlatFactory::new(4, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();
        builder.finish().unwrap();

        let reader = factory.open_reader(dir.path()).unwrap();
        let results = reader.nearest(&[1.0, 0.0, 0.0], 5, 1).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn ivfflat_lookup_returns_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let factory = IvfFlatFactory::new(4, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();
        builder.finish().unwrap();

        let reader = factory.open_reader(dir.path()).unwrap();
        let cell = CellValue::live(b"hello".to_vec(), 1);
        let result = reader.lookup(&cell);
        assert!(result.is_err());
        match result {
            Err(IndexError::Unsupported(_)) => {}
            other => panic!("expected Unsupported, got {:?}", other),
        }
    }

    #[test]
    fn ivfflat_range_returns_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let factory = IvfFlatFactory::new(4, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();
        builder.finish().unwrap();

        let reader = factory.open_reader(dir.path()).unwrap();
        let cell = CellValue::live(b"x".to_vec(), 1);
        let result = reader.range(&cell, &cell);
        assert!(result.is_err());
        match result {
            Err(IndexError::Unsupported(_)) => {}
            other => panic!("expected Unsupported, got {:?}", other),
        }
    }

    #[test]
    fn ivfflat_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let factory = IvfFlatFactory::new(4, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();
        builder.finish().unwrap();

        let reader = factory.open_reader(dir.path()).unwrap();
        let caps = reader.capabilities();
        assert_eq!(caps, vec![IndexCapability::Nearest]);
    }

    #[test]
    fn ivfflat_dimension_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let factory = IvfFlatFactory::new(2, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();
        builder
            .add_row(RowPosition::new(0), &make_vector_cell(&[1.0, 0.0, 0.0]))
            .unwrap();
        builder.finish().unwrap();

        let reader = factory.open_reader(dir.path()).unwrap();
        let result = reader.nearest(&[1.0, 0.0], 1, 1);
        assert!(result.is_err());
        match result {
            Err(IndexError::DimensionMismatch {
                expected: 3,
                got: 2,
            }) => {}
            other => panic!("expected DimensionMismatch, got {:?}", other),
        }
    }

    #[test]
    fn ivfflat_multi_probe_finds_cross_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let factory = IvfFlatFactory::new(2, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();

        // Cluster A: near (10, 0)
        builder
            .add_row(RowPosition::new(0), &make_vector_cell(&[10.0, 0.0]))
            .unwrap();
        builder
            .add_row(RowPosition::new(100), &make_vector_cell(&[10.1, 0.1]))
            .unwrap();

        // Cluster B: near (0, 10)
        builder
            .add_row(RowPosition::new(200), &make_vector_cell(&[0.0, 10.0]))
            .unwrap();
        builder
            .add_row(RowPosition::new(300), &make_vector_cell(&[0.1, 10.1]))
            .unwrap();

        builder.finish().unwrap();

        // With probes=2, we search both clusters, so k=4 should return all
        let reader = factory.open_reader(dir.path()).unwrap();
        let results = reader.nearest(&[5.0, 5.0], 4, 2).unwrap();
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn kmeans_basic() {
        // Two well-separated clusters
        let vectors = vec![
            vec![0.0, 0.0],
            vec![0.1, 0.1],
            vec![10.0, 10.0],
            vec![10.1, 10.1],
        ];
        let (centroids, assignments) = kmeans(&vectors, 2, &DistanceMetric::L2, 100);
        assert_eq!(centroids.len(), 2);
        // Vectors 0 and 1 should be in the same cluster
        assert_eq!(assignments[0], assignments[1]);
        // Vectors 2 and 3 should be in the same cluster
        assert_eq!(assignments[2], assignments[3]);
        // The two groups should be in different clusters
        assert_ne!(assignments[0], assignments[2]);
    }
}
