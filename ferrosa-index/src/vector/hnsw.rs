//! HNSW (Hierarchical Navigable Small World) graph index for approximate
//! nearest-neighbor search.
//!
//! The HNSW graph is a multi-layer structure where:
//! - Each node is a vector with connections to neighboring vectors
//! - Higher layers are sparser, enabling fast coarse navigation
//! - The base layer (layer 0) contains all vectors
//! - Layer assignment follows an exponential distribution:
//!   `l = floor(-ln(rand()) * mL)` where `mL = 1 / ln(m)`
//!
//! # References
//!
//! Malkov & Yashunin, "Efficient and robust approximate nearest neighbor
//! search using Hierarchical Navigable Small World graphs" (2018).

use std::collections::BinaryHeap;
use std::path::Path;

use rand::RngExt;
use serde::{Deserialize, Serialize};

use super::distance;
use super::{
    bytes_to_vec_f32, DistanceMetric, IndexBuilder, IndexCapability, IndexError, IndexFactory,
    IndexReader, IndexResult, RowPosition,
};
use ferrosa_common::CellValue;

// ---------------------------------------------------------------------------
// Serializable graph structure
// ---------------------------------------------------------------------------

/// The serialized HNSW graph, stored as JSON on disk.
#[derive(Debug, Serialize, Deserialize)]
struct HnswGraphData {
    /// `layers[l][node_id]` = list of neighbor node IDs at layer `l`.
    layers: Vec<Vec<Vec<usize>>>,
    /// `vectors[node_id]` = the vector for that node.
    vectors: Vec<Vec<f32>>,
    /// `positions[node_id]` = the row position for that node.
    positions: Vec<RowPosition>,
    /// Entry point node ID (if the graph is non-empty).
    entry_point: Option<usize>,
    /// The highest occupied layer index.
    max_layer: usize,
    /// Max connections per node per layer.
    m: usize,
    /// Construction-time search width.
    ef_construction: usize,
    /// Distance metric.
    metric: DistanceMetric,
}

// ---------------------------------------------------------------------------
// In-memory graph used during construction
// ---------------------------------------------------------------------------

/// In-memory HNSW graph, used for building.
struct HnswGraph {
    layers: Vec<Vec<Vec<usize>>>,
    vectors: Vec<Vec<f32>>,
    positions: Vec<RowPosition>,
    entry_point: Option<usize>,
    max_layer: usize,
    m: usize,
    ef_construction: usize,
    metric: DistanceMetric,
}

impl HnswGraph {
    fn new(m: usize, ef_construction: usize, metric: DistanceMetric) -> Self {
        Self {
            layers: vec![Vec::new()], // start with layer 0
            vectors: Vec::new(),
            positions: Vec::new(),
            entry_point: None,
            max_layer: 0,
            m,
            ef_construction,
            metric,
        }
    }

    /// Assign a random layer for a new node.
    fn random_layer(&self) -> usize {
        let mut rng = rand::rng();
        let ml = 1.0 / (self.m as f64).ln();
        let r: f64 = rng.random();
        // Avoid -ln(0) by clamping
        let r = r.max(1e-15);
        (-r.ln() * ml).floor() as usize
    }

    /// Insert a vector into the graph.
    fn insert(&mut self, vector: Vec<f32>, position: RowPosition) {
        let node_id = self.vectors.len();
        let node_layer = self.random_layer();

        self.vectors.push(vector);
        self.positions.push(position);

        // Ensure we have enough layers
        while self.layers.len() <= node_layer {
            self.layers.push(Vec::new());
        }
        // Ensure each layer has a slot for this node
        for layer in self.layers.iter_mut() {
            while layer.len() <= node_id {
                layer.push(Vec::new());
            }
        }

        if self.entry_point.is_none() {
            // First node: just set as entry point
            self.entry_point = Some(node_id);
            self.max_layer = node_layer;
            return;
        }

        let entry = self.entry_point.unwrap();

        // Phase 1: Greedily descend from the top layer down to node_layer + 1
        let mut current_entry = entry;
        let top = self.max_layer;
        if top > node_layer {
            for layer_idx in (node_layer + 1..=top).rev() {
                current_entry = self.greedy_closest(layer_idx, current_entry, node_id);
            }
        }

        // Phase 2: For layers node_layer down to 0, search and connect
        let search_top = node_layer.min(self.max_layer);
        for layer_idx in (0..=search_top).rev() {
            let neighbors =
                self.search_layer(layer_idx, current_entry, node_id, self.ef_construction);
            // Keep the M closest
            let m_max = if layer_idx == 0 { self.m * 2 } else { self.m };
            let selected: Vec<usize> = neighbors.into_iter().take(m_max).collect();

            // Connect node to selected neighbors (bidirectional)
            for &neighbor in &selected {
                self.layers[layer_idx][node_id].push(neighbor);
                self.layers[layer_idx][neighbor].push(node_id);

                // Prune neighbor's connections if over limit
                if self.layers[layer_idx][neighbor].len() > m_max {
                    self.prune_connections(layer_idx, neighbor, m_max);
                }
            }

            if !selected.is_empty() {
                current_entry = selected[0];
            }
        }

        // Update entry point if this node's layer is higher
        if node_layer > self.max_layer {
            self.entry_point = Some(node_id);
            self.max_layer = node_layer;
        }
    }

    /// Greedily find the closest node to `target` starting from `entry`
    /// in a single layer.
    fn greedy_closest(&self, layer: usize, entry: usize, target: usize) -> usize {
        let target_vec = &self.vectors[target];
        let mut current = entry;
        let mut current_dist = distance(&self.metric, &self.vectors[current], target_vec);

        loop {
            let mut changed = false;
            if layer < self.layers.len() {
                for &neighbor in &self.layers[layer][current] {
                    let d = distance(&self.metric, &self.vectors[neighbor], target_vec);
                    if d < current_dist {
                        current = neighbor;
                        current_dist = d;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        current
    }

    /// Search a layer for the `ef` closest nodes to the target node.
    /// Returns node IDs sorted by distance (closest first).
    fn search_layer(&self, layer: usize, entry: usize, target: usize, ef: usize) -> Vec<usize> {
        self.search_layer_by_vec(layer, entry, &self.vectors[target].clone(), ef)
    }

    /// Search a layer for the `ef` closest nodes to a query vector.
    fn search_layer_by_vec(
        &self,
        layer: usize,
        entry: usize,
        query: &[f32],
        ef: usize,
    ) -> Vec<usize> {
        if layer >= self.layers.len() {
            return Vec::new();
        }

        let entry_dist = distance(&self.metric, &self.vectors[entry], query);

        // Min-heap for candidates (to explore)
        let mut candidates: BinaryHeap<std::cmp::Reverse<OrdF32Node>> = BinaryHeap::new();
        // Max-heap for results (to track worst in result set)
        let mut results: BinaryHeap<OrdF32Node> = BinaryHeap::new();
        let mut visited = std::collections::HashSet::new();

        candidates.push(std::cmp::Reverse(OrdF32Node {
            dist: entry_dist,
            id: entry,
        }));
        results.push(OrdF32Node {
            dist: entry_dist,
            id: entry,
        });
        visited.insert(entry);

        while let Some(std::cmp::Reverse(current)) = candidates.pop() {
            let worst_dist = results.peek().map(|n| n.dist).unwrap_or(f32::INFINITY);
            if current.dist > worst_dist && results.len() >= ef {
                break;
            }

            if layer < self.layers.len() && current.id < self.layers[layer].len() {
                for &neighbor in &self.layers[layer][current.id] {
                    if visited.contains(&neighbor) {
                        continue;
                    }
                    visited.insert(neighbor);

                    let d = distance(&self.metric, &self.vectors[neighbor], query);
                    let worst_dist = results.peek().map(|n| n.dist).unwrap_or(f32::INFINITY);

                    if d < worst_dist || results.len() < ef {
                        candidates.push(std::cmp::Reverse(OrdF32Node {
                            dist: d,
                            id: neighbor,
                        }));
                        results.push(OrdF32Node {
                            dist: d,
                            id: neighbor,
                        });
                        if results.len() > ef {
                            results.pop(); // remove the worst
                        }
                    }
                }
            }
        }

        // Extract and sort by distance
        let mut result_vec: Vec<(f32, usize)> =
            results.into_iter().map(|n| (n.dist, n.id)).collect();
        result_vec.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        result_vec.into_iter().map(|(_, id)| id).collect()
    }

    /// Prune a node's connections to keep only the `max_conn` closest.
    fn prune_connections(&mut self, layer: usize, node: usize, max_conn: usize) {
        let node_vec = self.vectors[node].clone();
        let mut scored: Vec<(f32, usize)> = self.layers[layer][node]
            .iter()
            .map(|&n| (distance(&self.metric, &self.vectors[n], &node_vec), n))
            .collect();
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(max_conn);
        self.layers[layer][node] = scored.into_iter().map(|(_, id)| id).collect();
    }

    /// Convert to serializable data.
    fn to_data(&self) -> HnswGraphData {
        HnswGraphData {
            layers: self.layers.clone(),
            vectors: self.vectors.clone(),
            positions: self.positions.clone(),
            entry_point: self.entry_point,
            max_layer: self.max_layer,
            m: self.m,
            ef_construction: self.ef_construction,
            metric: self.metric,
        }
    }
}

// ---------------------------------------------------------------------------
// Ordered f32 wrapper for BinaryHeap
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct OrdF32Node {
    dist: f32,
    id: usize,
}

impl PartialEq for OrdF32Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for OrdF32Node {}

impl PartialOrd for OrdF32Node {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF32Node {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

// ---------------------------------------------------------------------------
// HnswFactory
// ---------------------------------------------------------------------------

/// Factory for creating HNSW index builders and readers.
pub struct HnswFactory {
    /// Max connections per node per layer.
    pub m: usize,
    /// Construction-time search width.
    pub ef_construction: usize,
    /// Distance metric.
    pub metric: DistanceMetric,
}

impl HnswFactory {
    pub fn new(m: usize, ef_construction: usize, metric: DistanceMetric) -> Self {
        Self {
            m,
            ef_construction,
            metric,
        }
    }
}

impl IndexFactory for HnswFactory {
    fn create_builder(&self, dir: &Path) -> Result<Box<dyn IndexBuilder>, IndexError> {
        Ok(Box::new(HnswBuilder {
            graph: HnswGraph::new(self.m, self.ef_construction, self.metric),
            dir: dir.to_path_buf(),
        }))
    }

    fn open_reader(&self, dir: &Path) -> Result<Box<dyn IndexReader>, IndexError> {
        let db_path = dir.join("hnsw.db");
        let data_bytes = std::fs::read(&db_path)?;
        let data: HnswGraphData = serde_json::from_slice(&data_bytes)?;
        Ok(Box::new(HnswReader { data }))
    }
}

// ---------------------------------------------------------------------------
// HnswBuilder
// ---------------------------------------------------------------------------

/// Builds an HNSW index by inserting vectors one at a time.
pub struct HnswBuilder {
    graph: HnswGraph,
    dir: std::path::PathBuf,
}

impl IndexBuilder for HnswBuilder {
    fn add_row(&mut self, position: RowPosition, value: &CellValue) -> Result<(), IndexError> {
        let bytes = value
            .value
            .as_ref()
            .ok_or_else(|| IndexError::Format("cannot index tombstone cell".to_string()))?;
        let vector = bytes_to_vec_f32(bytes)?;
        self.graph.insert(vector, position);
        Ok(())
    }

    fn finish(&mut self) -> Result<(), IndexError> {
        std::fs::create_dir_all(&self.dir)?;
        let data = self.graph.to_data();
        let json = serde_json::to_vec(&data)?;
        std::fs::write(self.dir.join("hnsw.db"), json)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Public free functions for ferrosa-storage integration
// ---------------------------------------------------------------------------

/// Build an HNSW graph from drained memtable entries and serialize it to JSON.
///
/// Called by `ferrosa-storage::TableStore` at flush time to persist the
/// in-memory vector index as a `{gen}-VEC-{index_name}.db` sidecar file.
///
/// # Errors
///
/// Returns `IndexError::Format` if JSON serialization fails.
pub fn build_and_serialize(
    m: usize,
    ef_construction: usize,
    metric: DistanceMetric,
    entries: Vec<(RowPosition, Vec<f32>)>,
) -> Result<Vec<u8>, IndexError> {
    let mut graph = HnswGraph::new(m, ef_construction, metric);
    for (pos, vector) in entries {
        graph.insert(vector, pos);
    }
    let data = graph.to_data();
    serde_json::to_vec(&data).map_err(|e| IndexError::Format(format!("HNSW serialize failed: {e}")))
}

/// Deserialize a vector sidecar byte blob and search it for the `k`
/// approximate nearest neighbors of `query`.
///
/// Called by `ferrosa-storage::TableStore::ann_search` to query HNSW sidecar
/// files written at flush time via [`build_and_serialize`].
///
/// # Errors
///
/// Returns `IndexError::Format` if `bytes` is not valid JSON-encoded
/// `HnswGraphData`, or `IndexError::DimensionMismatch` if the query
/// dimension does not match the graph.
pub fn search_from_bytes(
    bytes: &[u8],
    query: &[f32],
    k: usize,
    ef_search: usize,
) -> Result<Vec<IndexResult>, IndexError> {
    let data: HnswGraphData = serde_json::from_slice(bytes)
        .map_err(|e| IndexError::Format(format!("HNSW deserialize failed: {e}")))?;
    let reader = HnswReader { data };
    reader.nearest(query, k, ef_search)
}

// ---------------------------------------------------------------------------
// HnswReader
// ---------------------------------------------------------------------------

/// Reads a previously built HNSW index and answers nearest-neighbor queries.
pub struct HnswReader {
    data: HnswGraphData,
}

impl HnswReader {
    /// Reconstruct a searchable in-memory graph from the stored data.
    fn to_graph(&self) -> HnswGraph {
        HnswGraph {
            layers: self.data.layers.clone(),
            vectors: self.data.vectors.clone(),
            positions: self.data.positions.clone(),
            entry_point: self.data.entry_point,
            max_layer: self.data.max_layer,
            m: self.data.m,
            ef_construction: self.data.ef_construction,
            metric: self.data.metric,
        }
    }
}

impl IndexReader for HnswReader {
    fn capabilities(&self) -> Vec<IndexCapability> {
        vec![IndexCapability::Nearest]
    }

    fn lookup(&self, _value: &CellValue) -> Result<Vec<IndexResult>, IndexError> {
        Err(IndexError::Unsupported(
            "HNSW index does not support point lookups".to_string(),
        ))
    }

    fn range(&self, _start: &CellValue, _end: &CellValue) -> Result<Vec<IndexResult>, IndexError> {
        Err(IndexError::Unsupported(
            "HNSW index does not support range scans".to_string(),
        ))
    }

    fn nearest(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<IndexResult>, IndexError> {
        if self.data.entry_point.is_none() || self.data.vectors.is_empty() {
            return Ok(Vec::new());
        }

        // Validate dimensions
        let dim = self.data.vectors[0].len();
        if query.len() != dim {
            return Err(IndexError::DimensionMismatch {
                expected: dim,
                got: query.len(),
            });
        }

        let graph = self.to_graph();
        let entry = self.data.entry_point.unwrap();

        // Phase 1: Greedy descent from top layer to layer 1
        let mut current_entry = entry;
        let query_dist = |node: usize| distance(&self.data.metric, &self.data.vectors[node], query);

        for layer_idx in (1..=self.data.max_layer).rev() {
            // Greedy search: move to the closest neighbor in this layer
            loop {
                let mut changed = false;
                if layer_idx < graph.layers.len() && current_entry < graph.layers[layer_idx].len() {
                    for &neighbor in &graph.layers[layer_idx][current_entry] {
                        if query_dist(neighbor) < query_dist(current_entry) {
                            current_entry = neighbor;
                            changed = true;
                        }
                    }
                }
                if !changed {
                    break;
                }
            }
        }

        // Phase 2: Search layer 0 with ef_search width
        let ef = ef_search.max(k);
        let result_ids = graph.search_layer_by_vec(0, current_entry, query, ef);

        // Return top k
        let results: Vec<IndexResult> = result_ids
            .into_iter()
            .take(k)
            .map(|id| IndexResult {
                position: self.data.positions[id],
                score: distance(&self.data.metric, &self.data.vectors[id], query),
            })
            .collect();

        Ok(results)
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
    fn hnsw_build_and_nearest() {
        let dir = tempfile::tempdir().unwrap();
        let factory = HnswFactory::new(16, 200, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();

        // Insert 4 vectors
        let vectors: Vec<Vec<f32>> = vec![
            vec![1.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0],
            vec![0.0, 0.0, 1.0],
            vec![0.9, 0.1, 0.0],
        ];
        for (i, v) in vectors.iter().enumerate() {
            builder
                .add_row(RowPosition::new(i as u64 * 100), &make_vector_cell(v))
                .unwrap();
        }
        builder.finish().unwrap();

        // Query [1,0,0] with k=2
        let reader = factory.open_reader(dir.path()).unwrap();
        let results = reader.nearest(&[1.0, 0.0, 0.0], 2, 50).unwrap();
        assert_eq!(results.len(), 2);

        // First result should be exact match at offset 0
        assert_eq!(results[0].position.offset, 0);
        assert!(results[0].score < 1e-6); // distance ~0

        // Second result should be [0.9, 0.1, 0.0] at offset 300
        assert_eq!(results[1].position.offset, 300);
    }

    #[test]
    fn hnsw_empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let factory = HnswFactory::new(16, 200, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();
        builder.finish().unwrap();

        let reader = factory.open_reader(dir.path()).unwrap();
        let results = reader.nearest(&[1.0, 0.0, 0.0], 5, 50).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn hnsw_lookup_returns_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let factory = HnswFactory::new(16, 200, DistanceMetric::L2);
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
    fn hnsw_range_returns_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let factory = HnswFactory::new(16, 200, DistanceMetric::L2);
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
    fn hnsw_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let factory = HnswFactory::new(16, 200, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();
        builder.finish().unwrap();

        let reader = factory.open_reader(dir.path()).unwrap();
        let caps = reader.capabilities();
        assert_eq!(caps, vec![IndexCapability::Nearest]);
    }

    #[test]
    fn hnsw_dimension_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let factory = HnswFactory::new(16, 200, DistanceMetric::L2);
        let mut builder = factory.create_builder(dir.path()).unwrap();
        builder
            .add_row(RowPosition::new(0), &make_vector_cell(&[1.0, 0.0, 0.0]))
            .unwrap();
        builder.finish().unwrap();

        let reader = factory.open_reader(dir.path()).unwrap();
        // Query with wrong dimension (2 instead of 3)
        let result = reader.nearest(&[1.0, 0.0], 1, 50);
        assert!(result.is_err());
        match result {
            Err(IndexError::DimensionMismatch {
                expected: 3,
                got: 2,
            }) => {}
            other => panic!("expected DimensionMismatch, got {:?}", other),
        }
    }
}
