//! IVFFlat (Inverted File with Flat vectors) index.
//!
//! Partitions the vector space into Voronoi cells using k-means clustering.
//! At query time, probes the nearest centroids and scans the corresponding
//! inverted lists. Supports [`IndexCapabilities::NEAREST`] only.

use crate::{
    DistanceMetric, IndexBuilder, IndexCapabilities, IndexConfig, IndexError, IndexFactory,
    IndexFileMeta, IndexFiles, IndexKey, IndexReader, IndexResult, IndexType, RowPosition,
    VectorMethod,
};
use ferrosa_common::CellValue;
use serde::{Deserialize, Serialize};
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

use super::distance;

/// Factory for creating IVFFlat index builders and readers.
pub struct IvfFlatFactory;

impl IndexFactory for IvfFlatFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        let (lists, metric, dimensions) = match &config.index_type {
            IndexType::Vector {
                method: VectorMethod::IvfFlat { lists },
                metric,
                dimensions,
            } => (*lists, metric.clone(), *dimensions),
            _ => {
                return Err(IndexError::Build(
                    "IvfFlatFactory requires Vector/IvfFlat index type".into(),
                ))
            }
        };
        Ok(Box::new(IvfFlatBuilder {
            vectors: Vec::new(),
            positions: Vec::new(),
            n_lists: lists as usize,
            metric,
            dimensions: dimensions as usize,
            config: config.clone(),
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        let data = std::fs::read(&files.data_path)?;
        let index: SerializedIvfFlat = serde_json::from_slice(&data)
            .map_err(|e| IndexError::Query(format!("deserialize IVFFlat index: {e}")))?;
        Ok(Box::new(IvfFlatReader { index }))
    }

    fn merge(
        &self,
        readers: Vec<Box<dyn IndexReader>>,
        builder: Box<dyn IndexBuilder>,
    ) -> IndexResult<IndexFiles> {
        let _ = readers;
        builder.finish()
    }
}

/// Accumulates vectors during index build. On finish, runs k-means clustering
/// and assigns each vector to its nearest centroid.
struct IvfFlatBuilder {
    vectors: Vec<Vec<f32>>,
    positions: Vec<RowPosition>,
    n_lists: usize,
    metric: DistanceMetric,
    dimensions: usize,
    config: IndexConfig,
}

impl IndexBuilder for IvfFlatBuilder {
    fn add_row(
        &mut self,
        partition_key: &[u8],
        clustering_key: &[u8],
        cells: &[(u16, CellValue)],
    ) -> IndexResult<()> {
        let col_pos = self
            .config
            .column_positions
            .first()
            .copied()
            .ok_or_else(|| IndexError::Build("no column positions configured".into()))?;

        let cell = cells
            .iter()
            .find(|(pos, _)| *pos as usize == col_pos)
            .map(|(_, cv)| cv);

        let cell = match cell {
            Some(c) => c,
            None => return Ok(()),
        };

        if cell.is_tombstone() {
            return Ok(());
        }

        let raw = match &cell.value {
            Some(v) => v,
            None => return Ok(()),
        };

        let vec = super::bytes_to_vector(raw);
        if vec.len() != self.dimensions {
            return Err(IndexError::Build(format!(
                "expected {} dimensions, got {}",
                self.dimensions,
                vec.len()
            )));
        }

        self.vectors.push(vec);
        self.positions.push(RowPosition {
            partition_key: partition_key.to_vec(),
            clustering_key: clustering_key.to_vec(),
        });

        Ok(())
    }

    fn finish(self: Box<Self>) -> IndexResult<IndexFiles> {
        let index = build_ivfflat(
            &self.vectors,
            &self.positions,
            self.n_lists,
            &self.metric,
            self.dimensions,
        );

        let row_count = self.positions.len() as u64;
        let data = serde_json::to_vec(&index)
            .map_err(|e| IndexError::Build(format!("serialize IVFFlat index: {e}")))?;

        let data_path = self.config.output_dir.join(format!(
            "{}-{}.db",
            self.config.sstable_prefix, self.config.index_name
        ));
        let meta_path = self.config.output_dir.join(format!(
            "{}-{}.meta",
            self.config.sstable_prefix, self.config.index_name
        ));

        std::fs::create_dir_all(&self.config.output_dir)?;
        std::fs::write(&data_path, &data)?;

        let checksum = crc32_simple(&data);
        let file_size = data.len() as u64;
        let build_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let meta = IndexFileMeta {
            index_type: self.config.index_type.clone(),
            index_name: self.config.index_name.clone(),
            row_count,
            build_timestamp,
            sstable_id: self.config.sstable_prefix.clone(),
            file_size,
            checksum,
        };

        let meta_json = serde_json::to_vec(&meta)
            .map_err(|e| IndexError::Build(format!("meta serialization: {e}")))?;
        std::fs::write(&meta_path, &meta_json)?;

        Ok(IndexFiles {
            data_path,
            meta_path,
            meta,
        })
    }
}

/// Reads a serialized IVFFlat index and performs ANN queries by probing
/// the nearest centroids.
struct IvfFlatReader {
    index: SerializedIvfFlat,
}

impl IndexReader for IvfFlatReader {
    fn lookup(&self, _key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "point lookup not supported by IVFFlat index".into(),
        ))
    }

    fn range(
        &self,
        _start: Bound<&IndexKey>,
        _end: Bound<&IndexKey>,
    ) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "range scan not supported by IVFFlat index".into(),
        ))
    }

    fn nearest(
        &self,
        query: &[f32],
        k: usize,
        _ef_search: Option<u16>,
    ) -> IndexResult<Vec<(RowPosition, f32)>> {
        if self.index.lists.is_empty() {
            return Ok(Vec::new());
        }
        let results = search_ivfflat(&self.index, query, k);
        Ok(results)
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::NEAREST
    }
}

// ---------------------------------------------------------------------------
// Serialized structures
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedIvfFlat {
    metric: DistanceMetric,
    dimensions: usize,
    n_probes: usize,
    centroids: Vec<Vec<f32>>,
    lists: Vec<InvertedList>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InvertedList {
    vectors: Vec<Vec<f32>>,
    positions: Vec<RowPosition>,
}

// ---------------------------------------------------------------------------
// K-means clustering and index construction
// ---------------------------------------------------------------------------

fn build_ivfflat(
    vectors: &[Vec<f32>],
    positions: &[RowPosition],
    n_lists: usize,
    metric: &DistanceMetric,
    dimensions: usize,
) -> SerializedIvfFlat {
    if vectors.is_empty() {
        return SerializedIvfFlat {
            metric: metric.clone(),
            dimensions,
            n_probes: 1,
            centroids: Vec::new(),
            lists: Vec::new(),
        };
    }

    // Clamp n_lists to number of vectors
    let k = n_lists.min(vectors.len());

    // Run k-means to find centroids
    let centroids = kmeans(vectors, k, dimensions, metric, 20);

    // Assign each vector to its nearest centroid
    let mut lists: Vec<InvertedList> = (0..k)
        .map(|_| InvertedList {
            vectors: Vec::new(),
            positions: Vec::new(),
        })
        .collect();

    for (i, vec) in vectors.iter().enumerate() {
        let nearest = find_nearest_centroid(vec, &centroids, metric);
        lists[nearest].vectors.push(vec.clone());
        lists[nearest].positions.push(positions[i].clone());
    }

    // Default probe count: sqrt(n_lists), at least 1
    let n_probes = ((k as f64).sqrt().ceil() as usize).max(1);

    SerializedIvfFlat {
        metric: metric.clone(),
        dimensions,
        n_probes,
        centroids,
        lists,
    }
}

/// Simple k-means clustering with random initialization.
fn kmeans(
    vectors: &[Vec<f32>],
    k: usize,
    dimensions: usize,
    metric: &DistanceMetric,
    max_iterations: usize,
) -> Vec<Vec<f32>> {
    if vectors.len() <= k {
        // Fewer vectors than clusters: each vector is its own centroid
        return vectors.to_vec();
    }

    // Initialize centroids by selecting k evenly-spaced vectors.
    // This is more deterministic than random init for testing.
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
    let step = vectors.len() as f64 / k as f64;
    for i in 0..k {
        let idx = (i as f64 * step) as usize;
        centroids.push(vectors[idx].clone());
    }

    let mut assignments = vec![0usize; vectors.len()];

    for _ in 0..max_iterations {
        // Assignment step: assign each vector to nearest centroid
        let mut changed = false;
        for (i, vec) in vectors.iter().enumerate() {
            let nearest = find_nearest_centroid(vec, &centroids, metric);
            if assignments[i] != nearest {
                assignments[i] = nearest;
                changed = true;
            }
        }

        if !changed {
            break;
        }

        // Update step: recompute centroids
        let mut sums = vec![vec![0.0f32; dimensions]; k];
        let mut counts = vec![0usize; k];

        for (i, vec) in vectors.iter().enumerate() {
            let cluster = assignments[i];
            counts[cluster] += 1;
            for (j, &val) in vec.iter().enumerate() {
                sums[cluster][j] += val;
            }
        }

        for c in 0..k {
            if counts[c] > 0 {
                for j in 0..dimensions {
                    centroids[c][j] = sums[c][j] / counts[c] as f32;
                }
            }
            // If a cluster is empty, keep its centroid unchanged
        }
    }

    centroids
}

/// Find the index of the nearest centroid to a given vector.
fn find_nearest_centroid(vec: &[f32], centroids: &[Vec<f32>], metric: &DistanceMetric) -> usize {
    let mut best_idx = 0;
    let mut best_dist = f32::INFINITY;
    for (i, centroid) in centroids.iter().enumerate() {
        let d = distance(metric, vec, centroid);
        if d < best_dist {
            best_dist = d;
            best_idx = i;
        }
    }
    best_idx
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

fn search_ivfflat(index: &SerializedIvfFlat, query: &[f32], k: usize) -> Vec<(RowPosition, f32)> {
    if index.centroids.is_empty() {
        return Vec::new();
    }

    // Find the n_probes nearest centroids
    let mut centroid_dists: Vec<(f32, usize)> = index
        .centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (distance(&index.metric, query, c), i))
        .collect();
    centroid_dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let n_probes = index.n_probes.min(centroid_dists.len());

    // Scan the inverted lists for the nearest centroids
    let mut candidates: Vec<(f32, RowPosition)> = Vec::new();
    for &(_, list_idx) in centroid_dists.iter().take(n_probes) {
        let list = &index.lists[list_idx];
        for (i, vec) in list.vectors.iter().enumerate() {
            let d = distance(&index.metric, query, vec);
            candidates.push((d, list.positions[i].clone()));
        }
    }

    // Sort by distance, take top k
    candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    candidates.truncate(k);

    candidates
        .into_iter()
        .map(|(dist, pos)| (pos, dist))
        .collect()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn crc32_simple(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    let poly: u32 = 0xEDB8_8320;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ poly;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IndexConfig;

    fn make_config(dir: &std::path::Path) -> IndexConfig {
        IndexConfig {
            index_type: IndexType::Vector {
                method: VectorMethod::IvfFlat { lists: 2 },
                metric: DistanceMetric::L2,
                dimensions: 3,
            },
            column_positions: vec![0],
            output_dir: dir.to_path_buf(),
            sstable_prefix: "sstable-001".into(),
            index_name: "idx_vec".into(),
        }
    }

    fn vector_cell(v: &[f32]) -> CellValue {
        let bytes: Vec<u8> = v.iter().flat_map(|f| f.to_le_bytes()).collect();
        CellValue::live(bytes, 1000)
    }

    #[test]
    fn build_and_query_nearest() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = IvfFlatFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        let vecs = [
            [1.0f32, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
            [1.0, 1.0, 1.0],
        ];

        for (i, v) in vecs.iter().enumerate() {
            let pk = format!("pk{}", i);
            let ck = format!("ck{}", i);
            builder
                .add_row(pk.as_bytes(), ck.as_bytes(), &[(0, vector_cell(v))])
                .unwrap();
        }

        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 4);

        let reader = factory.open_reader(&files).unwrap();

        // Query near [1,1,1] - should find [1,1,1] as closest
        let results = reader.nearest(&[1.0, 1.0, 1.0], 2, None).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].0.partition_key, b"pk3");
        assert!((results[0].1).abs() < 1e-6);
    }

    #[test]
    fn empty_index_nearest() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = IvfFlatFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let results = reader.nearest(&[1.0, 0.0, 0.0], 5, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn lookup_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = IvfFlatFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let result = reader.lookup(&IndexKey::Bytes(b"test".to_vec()));
        assert!(matches!(result, Err(IndexError::Unsupported(_))));
    }

    #[test]
    fn range_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = IvfFlatFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let result = reader.range(Bound::Unbounded, Bound::Unbounded);
        assert!(matches!(result, Err(IndexError::Unsupported(_))));
    }

    #[test]
    fn capabilities_nearest_only() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = IvfFlatFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::NEAREST));
        assert!(!caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(!caps.contains(IndexCapabilities::RANGE_SCAN));
    }

    #[test]
    fn nearest_returns_sorted_by_distance() {
        let dir = tempfile::tempdir().unwrap();
        // Use a single list to ensure all vectors are probed
        let config = IndexConfig {
            index_type: IndexType::Vector {
                method: VectorMethod::IvfFlat { lists: 1 },
                metric: DistanceMetric::L2,
                dimensions: 3,
            },
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            sstable_prefix: "sstable-001".into(),
            index_name: "idx_vec".into(),
        };
        let factory = IvfFlatFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        let vecs = [
            [10.0f32, 0.0, 0.0],
            [0.0, 0.0, 0.0],
            [5.0, 0.0, 0.0],
            [2.0, 0.0, 0.0],
        ];

        for (i, v) in vecs.iter().enumerate() {
            let pk = format!("pk{}", i);
            builder
                .add_row(pk.as_bytes(), b"ck", &[(0, vector_cell(v))])
                .unwrap();
        }

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let results = reader.nearest(&[0.0, 0.0, 0.0], 4, None).unwrap();
        assert_eq!(results.len(), 4);
        for pair in results.windows(2) {
            assert!(
                pair[0].1 <= pair[1].1,
                "results should be sorted by distance"
            );
        }
    }

    #[test]
    fn kmeans_convergence() {
        // Two well-separated clusters
        let vectors = vec![
            vec![0.0, 0.0],
            vec![0.1, 0.0],
            vec![0.0, 0.1],
            vec![10.0, 10.0],
            vec![10.1, 10.0],
            vec![10.0, 10.1],
        ];
        let centroids = kmeans(&vectors, 2, 2, &DistanceMetric::L2, 50);
        assert_eq!(centroids.len(), 2);

        // Each centroid should be near one of the two clusters
        let near_origin = centroids.iter().any(|c| c[0] < 1.0 && c[1] < 1.0);
        let near_ten = centroids.iter().any(|c| c[0] > 9.0 && c[1] > 9.0);
        assert!(near_origin, "should have a centroid near origin");
        assert!(near_ten, "should have a centroid near (10,10)");
    }
}
