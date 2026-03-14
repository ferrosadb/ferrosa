//! HNSW (Hierarchical Navigable Small World) vector index.
//!
//! Implements approximate nearest-neighbor search using a multi-layer
//! navigable small world graph. Supports [`IndexCapabilities::NEAREST`] only.

use crate::{
    DistanceMetric, IndexBuilder, IndexCapabilities, IndexConfig, IndexError, IndexFactory,
    IndexFileMeta, IndexFiles, IndexKey, IndexReader, IndexResult, IndexType, RowPosition,
    VectorMethod,
};
use ferrosa_common::CellValue;
use serde::{Deserialize, Serialize};
use std::collections::BinaryHeap;
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

use super::distance;

/// Factory for creating HNSW index builders and readers.
pub struct HnswFactory;

impl IndexFactory for HnswFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        let (m, ef_construction, metric, dimensions) = match &config.index_type {
            IndexType::Vector {
                method: VectorMethod::Hnsw { m, ef_construction },
                metric,
                dimensions,
            } => (*m, *ef_construction, metric.clone(), *dimensions),
            _ => {
                return Err(IndexError::Build(
                    "HnswFactory requires Vector/Hnsw index type".into(),
                ))
            }
        };
        Ok(Box::new(HnswBuilder {
            vectors: Vec::new(),
            positions: Vec::new(),
            m: m as usize,
            ef_construction: ef_construction as usize,
            metric,
            dimensions: dimensions as usize,
            config: config.clone(),
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        let data = std::fs::read(&files.data_path)?;
        let graph: SerializedGraph = serde_json::from_slice(&data)
            .map_err(|e| IndexError::Query(format!("deserialize HNSW graph: {e}")))?;
        Ok(Box::new(HnswReader { graph }))
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

/// Accumulates vectors during index construction and builds the HNSW graph on finish.
struct HnswBuilder {
    vectors: Vec<Vec<f32>>,
    positions: Vec<RowPosition>,
    m: usize,
    ef_construction: usize,
    metric: DistanceMetric,
    dimensions: usize,
    config: IndexConfig,
}

impl IndexBuilder for HnswBuilder {
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
        let graph = build_graph(
            &self.vectors,
            &self.positions,
            self.m,
            self.ef_construction,
            &self.metric,
        );

        let row_count = self.positions.len() as u64;
        let data = serde_json::to_vec(&graph)
            .map_err(|e| IndexError::Build(format!("serialize HNSW graph: {e}")))?;

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

/// Reads a serialized HNSW graph and performs ANN queries.
struct HnswReader {
    graph: SerializedGraph,
}

impl IndexReader for HnswReader {
    fn lookup(&self, _key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "point lookup not supported by HNSW index".into(),
        ))
    }

    fn range(
        &self,
        _start: Bound<&IndexKey>,
        _end: Bound<&IndexKey>,
    ) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "range scan not supported by HNSW index".into(),
        ))
    }

    fn nearest(
        &self,
        query: &[f32],
        k: usize,
        ef_search: Option<u16>,
    ) -> IndexResult<Vec<(RowPosition, f32)>> {
        if self.graph.nodes.is_empty() {
            return Ok(Vec::new());
        }
        let ef = ef_search.map(|v| v as usize).unwrap_or(k.max(10));
        let results = search_graph(&self.graph, query, k, ef);
        Ok(results)
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::NEAREST
    }
}

// ---------------------------------------------------------------------------
// Serialized graph structures
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedGraph {
    metric: DistanceMetric,
    m: usize,
    max_layer: usize,
    entry_point: Option<usize>,
    nodes: Vec<GraphNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphNode {
    vector: Vec<f32>,
    position: RowPosition,
    layer: usize,
    /// Neighbors at each layer: neighbors[l] contains node indices for layer l.
    neighbors: Vec<Vec<usize>>,
}

// ---------------------------------------------------------------------------
// Graph construction
// ---------------------------------------------------------------------------

/// Assign a random layer to a new node using an exponential distribution.
/// The probability of being assigned to layer l is (1/m)^l * (1 - 1/m).
fn random_layer(m: usize) -> usize {
    // Use a simple approach: keep flipping a biased coin.
    let ml = 1.0 / (m as f64).ln();
    let r: f64 = rand::random();
    // floor(-ln(r) * ml) gives an exponential distribution
    let layer = (-r.ln() * ml).floor() as usize;
    // Cap at a reasonable maximum to prevent pathological cases
    layer.min(16)
}

fn build_graph(
    vectors: &[Vec<f32>],
    positions: &[RowPosition],
    m: usize,
    ef_construction: usize,
    metric: &DistanceMetric,
) -> SerializedGraph {
    if vectors.is_empty() {
        return SerializedGraph {
            metric: metric.clone(),
            m,
            max_layer: 0,
            entry_point: None,
            nodes: Vec::new(),
        };
    }

    let m_max = m;
    let m_max0 = m * 2; // Double connections at layer 0

    let mut nodes: Vec<GraphNode> = Vec::with_capacity(vectors.len());
    let mut entry_point: usize = 0;

    // Insert first node
    let first_layer = random_layer(m);
    nodes.push(GraphNode {
        vector: vectors[0].clone(),
        position: positions[0].clone(),
        layer: first_layer,
        neighbors: vec![Vec::new(); first_layer + 1],
    });
    let mut max_layer: usize = first_layer;

    // Insert remaining nodes
    for i in 1..vectors.len() {
        let node_layer = random_layer(m);
        let new_node_idx = nodes.len();

        nodes.push(GraphNode {
            vector: vectors[i].clone(),
            position: positions[i].clone(),
            layer: node_layer,
            neighbors: vec![Vec::new(); node_layer + 1],
        });

        // Greedy search from top layer down to node_layer + 1
        let mut current = entry_point;
        for layer in (node_layer + 1..=max_layer).rev() {
            current = greedy_closest(&nodes, &vectors[i], current, layer, metric);
        }

        // For layers min(node_layer, max_layer) down to 0, find ef_construction nearest
        // and connect to the M nearest.
        let start_layer = node_layer.min(max_layer);
        let mut candidates = vec![current];

        for layer in (0..=start_layer).rev() {
            // Search for nearest neighbors at this layer
            let nearest = search_layer(
                &nodes,
                &vectors[i],
                &candidates,
                ef_construction,
                layer,
                metric,
            );

            // Select M nearest to connect
            let m_layer = if layer == 0 { m_max0 } else { m_max };
            let selected = select_neighbors(&nodes, &vectors[i], &nearest, m_layer, metric);

            // Bidirectional connections
            nodes[new_node_idx].neighbors[layer] = selected.clone();
            for &neighbor_idx in &selected {
                nodes[neighbor_idx].neighbors[layer].push(new_node_idx);
                // Prune if over capacity
                let max_neighbors = if layer == 0 { m_max0 } else { m_max };
                if nodes[neighbor_idx].neighbors[layer].len() > max_neighbors {
                    let nv = nodes[neighbor_idx].vector.clone();
                    let current_neighbors = nodes[neighbor_idx].neighbors[layer].clone();
                    let pruned =
                        select_neighbors(&nodes, &nv, &current_neighbors, max_neighbors, metric);
                    nodes[neighbor_idx].neighbors[layer] = pruned;
                }
            }

            candidates = nearest;
        }

        if node_layer > max_layer {
            max_layer = node_layer;
            entry_point = new_node_idx;
        }
    }

    SerializedGraph {
        metric: metric.clone(),
        m,
        max_layer,
        entry_point: Some(entry_point),
        nodes,
    }
}

/// Greedily walk to the closest node at a given layer.
fn greedy_closest(
    nodes: &[GraphNode],
    query: &[f32],
    start: usize,
    layer: usize,
    metric: &DistanceMetric,
) -> usize {
    let mut current = start;
    let mut current_dist = distance(metric, query, &nodes[current].vector);
    loop {
        let mut changed = false;
        let neighbors = if layer < nodes[current].neighbors.len() {
            &nodes[current].neighbors[layer]
        } else {
            break;
        };
        for &neighbor in neighbors {
            let d = distance(metric, query, &nodes[neighbor].vector);
            if d < current_dist {
                current = neighbor;
                current_dist = d;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    current
}

/// Search a single layer starting from the given entry points, returning up to
/// `ef` nearest node indices.
fn search_layer(
    nodes: &[GraphNode],
    query: &[f32],
    entry_points: &[usize],
    ef: usize,
    layer: usize,
    metric: &DistanceMetric,
) -> Vec<usize> {
    // Min-heap for candidates (closest first)
    let mut candidates: BinaryHeap<MinDist> = BinaryHeap::new();
    // Max-heap for results (furthest first, so we can evict)
    let mut results: BinaryHeap<MaxDist> = BinaryHeap::new();
    let mut visited = std::collections::HashSet::new();

    for &ep in entry_points {
        if visited.insert(ep) {
            let d = distance(metric, query, &nodes[ep].vector);
            candidates.push(MinDist { dist: d, idx: ep });
            results.push(MaxDist { dist: d, idx: ep });
        }
    }

    while let Some(MinDist {
        dist: c_dist,
        idx: c_idx,
    }) = candidates.pop()
    {
        let furthest_result = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
        if c_dist > furthest_result && results.len() >= ef {
            break;
        }

        let neighbors = if layer < nodes[c_idx].neighbors.len() {
            &nodes[c_idx].neighbors[layer]
        } else {
            continue;
        };

        for &neighbor in neighbors {
            if visited.insert(neighbor) {
                let d = distance(metric, query, &nodes[neighbor].vector);
                let furthest = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
                if results.len() < ef || d < furthest {
                    candidates.push(MinDist {
                        dist: d,
                        idx: neighbor,
                    });
                    results.push(MaxDist {
                        dist: d,
                        idx: neighbor,
                    });
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
    }

    results.into_iter().map(|r| r.idx).collect()
}

/// Select up to `m` nearest neighbors from candidates.
fn select_neighbors(
    nodes: &[GraphNode],
    query: &[f32],
    candidates: &[usize],
    m: usize,
    metric: &DistanceMetric,
) -> Vec<usize> {
    let mut scored: Vec<(f32, usize)> = candidates
        .iter()
        .map(|&idx| (distance(metric, query, &nodes[idx].vector), idx))
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(m);
    scored.into_iter().map(|(_, idx)| idx).collect()
}

// ---------------------------------------------------------------------------
// Graph search (query time)
// ---------------------------------------------------------------------------

fn search_graph(
    graph: &SerializedGraph,
    query: &[f32],
    k: usize,
    ef: usize,
) -> Vec<(RowPosition, f32)> {
    let entry = match graph.entry_point {
        Some(ep) => ep,
        None => return Vec::new(),
    };

    // Greedy descent from top layer to layer 1
    let mut current = entry;
    for layer in (1..=graph.max_layer).rev() {
        current = greedy_closest(&graph.nodes, query, current, layer, &graph.metric);
    }

    // Search layer 0 with ef candidates
    let results = search_layer(&graph.nodes, query, &[current], ef, 0, &graph.metric);

    // Score and sort
    let mut scored: Vec<(f32, usize)> = results
        .iter()
        .map(|&idx| {
            (
                distance(&graph.metric, query, &graph.nodes[idx].vector),
                idx,
            )
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(k);

    scored
        .into_iter()
        .map(|(dist, idx)| (graph.nodes[idx].position.clone(), dist))
        .collect()
}

// ---------------------------------------------------------------------------
// Heap helpers (ordered wrappers for BinaryHeap)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct MinDist {
    dist: f32,
    idx: usize,
}

impl PartialEq for MinDist {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist && self.idx == other.idx
    }
}
impl Eq for MinDist {}
impl PartialOrd for MinDist {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MinDist {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering so BinaryHeap acts as a min-heap
        other
            .dist
            .partial_cmp(&self.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| other.idx.cmp(&self.idx))
    }
}

#[derive(Debug, Clone)]
struct MaxDist {
    dist: f32,
    idx: usize,
}

impl PartialEq for MaxDist {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist && self.idx == other.idx
    }
}
impl Eq for MaxDist {}
impl PartialOrd for MaxDist {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MaxDist {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Natural ordering for max-heap
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.idx.cmp(&other.idx))
    }
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
                method: VectorMethod::Hnsw {
                    m: 4,
                    ef_construction: 16,
                },
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
        let factory = HnswFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        // Four vectors in 3D
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
        assert!((results[0].1).abs() < 1e-6); // distance to itself is 0
    }

    #[test]
    fn empty_index_nearest() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = HnswFactory;
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
        let factory = HnswFactory;
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
        let factory = HnswFactory;
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
        let factory = HnswFactory;
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
        let config = make_config(dir.path());
        let factory = HnswFactory;
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

        let results = reader.nearest(&[0.0, 0.0, 0.0], 4, Some(16)).unwrap();
        // Verify distances are non-decreasing
        for pair in results.windows(2) {
            assert!(
                pair[0].1 <= pair[1].1,
                "results should be sorted by distance"
            );
        }
    }
}
