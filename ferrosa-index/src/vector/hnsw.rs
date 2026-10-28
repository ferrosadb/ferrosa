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
///
/// Generic in how it holds a vector, so a build can borrow vectors that are
/// already in memory (`&[f32]`, or the memtable's shared `Arc<[f32]>`)
/// instead of copying them in.
struct HnswGraph<V = Vec<f32>> {
    layers: Vec<Vec<Vec<usize>>>,
    vectors: Vec<V>,
    positions: Vec<RowPosition>,
    entry_point: Option<usize>,
    max_layer: usize,
    m: usize,
    ef_construction: usize,
    metric: DistanceMetric,
    /// Scratch for the build's searches and prunes; never serialized.
    scratch: BuildScratch,
}

/// What one insert needs and the next reuses: the visited stamps, the layer
/// search's heaps and its answer, and the prune's scores. Allocated once per
/// build instead of once per layer of every insert.
#[derive(Default)]
struct BuildScratch {
    visits: VisitStamps,
    layer: LayerScratch,
    selected: Vec<OrdF32Node>,
    scored: Vec<(f32, usize)>,
}

impl<V: AsRef<[f32]>> HnswGraph<V> {
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
            scratch: BuildScratch::default(),
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
    fn insert(&mut self, vector: V, position: RowPosition) {
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
                current_entry = infallible(greedy_closest_in(
                    &*self,
                    layer_idx,
                    current_entry,
                    self.vectors[node_id].as_ref(),
                ));
            }
        }

        // Phase 2: For layers node_layer down to 0, search and connect
        let search_top = node_layer.min(self.max_layer);
        // Taken out for the searches, which borrow the graph shared.
        let mut scratch = std::mem::take(&mut self.scratch);
        for layer_idx in (0..=search_top).rev() {
            self.search_layer(
                layer_idx,
                current_entry,
                node_id,
                self.ef_construction,
                &mut scratch,
            );
            // Keep the M closest
            let m_max = if layer_idx == 0 { self.m * 2 } else { self.m };
            scratch.selected.truncate(m_max);

            // Connect node to selected neighbors (bidirectional)
            for neighbor in scratch.selected.iter().map(|n| n.id) {
                link(&mut self.layers[layer_idx][node_id], neighbor, m_max);
                link(&mut self.layers[layer_idx][neighbor], node_id, m_max);

                // Prune neighbor's connections if over limit
                if self.layers[layer_idx][neighbor].len() > m_max {
                    self.prune_connections(layer_idx, neighbor, m_max, &mut scratch.scored);
                }
            }

            if let Some(nearest) = scratch.selected.first() {
                current_entry = nearest.id;
            }
        }
        self.scratch = scratch;

        // Update entry point if this node's layer is higher
        if node_layer > self.max_layer {
            self.entry_point = Some(node_id);
            self.max_layer = node_layer;
        }
    }

    /// Search a layer for the `ef` closest nodes to node `target`, nearest
    /// first, into `scratch.selected`.
    fn search_layer(
        &self,
        layer: usize,
        entry: usize,
        target: usize,
        ef: usize,
        scratch: &mut BuildScratch,
    ) {
        // Borrowed: the search only reads the graph, so the target vector
        // needs no copy (one per layer per insert used to be made here).
        infallible(search_layer_in(
            self,
            LayerQuery {
                layer,
                entry,
                query: self.vectors[target].as_ref(),
                ef,
            },
            scratch.visits.begin(self.vectors.len()),
            &mut scratch.layer,
            &mut scratch.selected,
        ))
    }

    /// Prune a node's connections to keep only the `max_conn` closest,
    /// scoring them in `scored`, which the build reuses.
    fn prune_connections(
        &mut self,
        layer: usize,
        node: usize,
        max_conn: usize,
        scored: &mut Vec<(f32, usize)>,
    ) {
        // Scored under a shared borrow, so the node's vector is not copied.
        let node_vec = self.vectors[node].as_ref();
        scored.clear();
        scored.extend(self.layers[layer][node].iter().map(|&n| {
            (
                distance(&self.metric, self.vectors[n].as_ref(), node_vec),
                n,
            )
        }));
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(max_conn);
        // Rewritten in place: the list keeps its allocation.
        let neighbours = &mut self.layers[layer][node];
        neighbours.clear();
        neighbours.extend(scored.iter().map(|&(_, id)| id));
    }

    /// The graph's tables, borrowed, for the binary writer.
    fn tables(&self) -> super::hnsw_binary::Tables<'_, V> {
        super::hnsw_binary::Tables {
            layers: &self.layers,
            vectors: &self.vectors,
            positions: &self.positions,
            entry_point: self.entry_point,
            max_layer: self.max_layer,
            m: self.m,
            ef_construction: self.ef_construction,
            metric: self.metric,
        }
    }
}

impl HnswGraph<Vec<f32>> {
    /// The graph as JSON serializes it, borrowed rather than copied.
    fn data_ref(&self) -> HnswGraphDataRef<'_> {
        HnswGraphDataRef {
            layers: &self.layers,
            vectors: &self.vectors,
            positions: &self.positions,
            entry_point: self.entry_point,
            max_layer: self.max_layer,
            m: self.m,
            ef_construction: self.ef_construction,
            metric: self.metric,
        }
    }
}

/// The graph being built: every node it names exists, so reading it cannot
/// fail.
impl<V: AsRef<[f32]>> GraphView for HnswGraph<V> {
    type Error = std::convert::Infallible;
    type Neighbours<'g>
        = std::iter::Copied<std::slice::Iter<'g, usize>>
    where
        Self: 'g;

    fn neighbours(&self, layer: usize, node: usize) -> Result<Self::Neighbours<'_>, Self::Error> {
        let list = self
            .layers
            .get(layer)
            .and_then(|nodes| nodes.get(node))
            .map_or(&[][..], Vec::as_slice);
        Ok(list.iter().copied())
    }

    fn distance_to(&self, node: usize, query: &[f32]) -> Result<f32, Self::Error> {
        Ok(distance(&self.metric, self.vectors[node].as_ref(), query))
    }
}

/// [`HnswGraphData`] by reference, so serializing a graph does not first
/// clone every vector and neighbour list. Same fields in the same order, so
/// it writes byte-for-byte the JSON `HnswGraphData` reads back.
#[derive(Serialize)]
struct HnswGraphDataRef<'a> {
    layers: &'a [Vec<Vec<usize>>],
    vectors: &'a [Vec<f32>],
    positions: &'a [RowPosition],
    entry_point: Option<usize>,
    max_layer: usize,
    m: usize,
    ef_construction: usize,
    metric: DistanceMetric,
}

/// A graph a search can walk: the builder's own, a decoded legacy JSON
/// sidecar, or a binary sidecar read where it lies. One search serves all
/// three, so they cannot drift apart.
pub(super) trait GraphView {
    /// What reading the graph can fail with: nothing, for the graph being
    /// built; a malformed sidecar, for one read from bytes.
    type Error;
    /// One node's neighbour ids at one layer.
    type Neighbours<'g>: Iterator<Item = usize>
    where
        Self: 'g;
    /// Node `node`'s neighbours at `layer`; none above the top layer.
    fn neighbours(&self, layer: usize, node: usize) -> Result<Self::Neighbours<'_>, Self::Error>;
    /// The distance from node `node`'s vector to `query`.
    fn distance_to(&self, node: usize, query: &[f32]) -> Result<f32, Self::Error>;
}

/// Add `id` to a neighbour list that holds at most `max_conn`. The list
/// overshoots by one before it is pruned, so it is sized for that the first
/// time: grown by doubling it would end at twice the size it needs.
fn link(list: &mut Vec<usize>, id: usize, max_conn: usize) {
    if list.capacity() == 0 {
        list.reserve_exact(max_conn + 1);
    }
    list.push(id);
}

/// The value of a search that cannot fail.
fn infallible<T>(result: Result<T, std::convert::Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(never) => match never {},
    }
}

/// One layer search: from `entry`, the `ef` nodes of `layer` nearest
/// `query`.
struct LayerQuery<'q> {
    layer: usize,
    entry: usize,
    query: &'q [f32],
    ef: usize,
}

/// A layer search's heaps, emptied and reused by the next search.
#[derive(Default)]
struct LayerScratch {
    /// Min-heap of the nodes still to explore.
    candidates: BinaryHeap<std::cmp::Reverse<OrdF32Node>>,
    /// Max-heap of the best `ef` found, worst on top.
    results: BinaryHeap<OrdF32Node>,
}

/// Search one layer of `graph` for the `ef` nodes nearest the query, into
/// `found`, nearest first, each with its distance.
fn search_layer_in<G: GraphView>(
    graph: &G,
    q: LayerQuery<'_>,
    visited: &mut impl Visited,
    scratch: &mut LayerScratch,
    found: &mut Vec<OrdF32Node>,
) -> Result<(), G::Error> {
    let LayerQuery {
        layer,
        entry,
        query,
        ef,
    } = q;
    let entry_dist = graph.distance_to(entry, query)?;

    let LayerScratch {
        candidates,
        results,
    } = scratch;
    candidates.clear();
    results.clear();

    candidates.push(std::cmp::Reverse(OrdF32Node {
        dist: entry_dist,
        id: entry,
    }));
    results.push(OrdF32Node {
        dist: entry_dist,
        id: entry,
    });
    visited.first_visit(entry);

    while let Some(std::cmp::Reverse(current)) = candidates.pop() {
        let worst_dist = results.peek().map_or(f32::INFINITY, |n| n.dist);
        if current.dist > worst_dist && results.len() >= ef {
            break;
        }

        for neighbor in graph.neighbours(layer, current.id)? {
            if !visited.first_visit(neighbor) {
                continue;
            }

            let d = graph.distance_to(neighbor, query)?;
            let worst_dist = results.peek().map_or(f32::INFINITY, |n| n.dist);

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

    // Nearest first. `OrdF32Node` orders by distance alone, so this is the
    // order `into_sorted_vec` gave, without giving up either buffer.
    found.clear();
    found.extend(results.drain());
    found.sort_unstable();
    Ok(())
}

/// From `entry`, follow `layer`'s edges to the node nearest `query`.
/// Terminates: each move strictly lowers the distance.
fn greedy_closest_in<G: GraphView>(
    graph: &G,
    layer: usize,
    entry: usize,
    query: &[f32],
) -> Result<usize, G::Error> {
    let mut current = entry;
    let mut current_dist = graph.distance_to(current, query)?;
    loop {
        let mut changed = false;
        for neighbor in graph.neighbours(layer, current)? {
            let d = graph.distance_to(neighbor, query)?;
            if d < current_dist {
                current = neighbor;
                current_dist = d;
                changed = true;
            }
        }
        if !changed {
            return Ok(current);
        }
    }
}

/// The `k` nodes of `graph` nearest `query`, nearest first with their
/// distances: a greedy descent from `entry` on the top layer down to layer
/// 1, then a search of layer 0 `ef_search` wide.
pub(super) fn nearest_in<G: GraphView>(
    graph: &G,
    entry: usize,
    max_layer: usize,
    query: &[f32],
    k: usize,
    ef_search: usize,
) -> Result<Vec<(usize, f32)>, G::Error> {
    let mut current = entry;
    for layer in (1..=max_layer).rev() {
        current = greedy_closest_in(graph, layer, current, query)?;
    }
    let mut found = Vec::new();
    search_layer_in(
        graph,
        LayerQuery {
            layer: 0,
            entry: current,
            query,
            ef: ef_search.max(k),
        },
        // Sized by the nodes the search reaches, not by the graph.
        &mut std::collections::HashSet::new(),
        &mut LayerScratch::default(),
        &mut found,
    )?;
    Ok(found.into_iter().take(k).map(|n| (n.id, n.dist)).collect())
}

/// The nodes one layer search has already reached.
trait Visited {
    /// Mark `id` reached; `false` if it already was.
    fn first_visit(&mut self, id: usize) -> bool;
}

/// A reader's per-query set: sized by the nodes the search reaches, not by
/// the graph.
impl Visited for std::collections::HashSet<usize> {
    fn first_visit(&mut self, id: usize) -> bool {
        self.insert(id)
    }
}

/// The builder's visited set, reused across every search of a build: one
/// stamp per node, and a search is "this epoch". Starting a search is O(1)
/// and a visit is an index, where a fresh `HashSet` per search cost an
/// allocation and a SipHash per neighbour.
#[derive(Default)]
struct VisitStamps {
    stamps: Vec<u32>,
    epoch: u32,
}

impl VisitStamps {
    /// Start a search over a graph of `nodes` nodes.
    fn begin(&mut self, nodes: usize) -> &mut Self {
        if self.stamps.len() < nodes {
            self.stamps.resize(nodes, 0);
        }
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            // Wrapped: stale stamps could equal the new epoch. Clear once.
            self.stamps.fill(0);
            self.epoch = 1;
        }
        self
    }
}

impl Visited for VisitStamps {
    fn first_visit(&mut self, id: usize) -> bool {
        let stamp = &mut self.stamps[id];
        let first = *stamp != self.epoch;
        *stamp = self.epoch;
        first
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
        let file = std::fs::File::create(self.dir.join("hnsw.db"))?;
        let mut out = std::io::BufWriter::new(file);
        serde_json::to_writer(&mut out, &self.graph.data_ref())?;
        std::io::Write::flush(&mut out)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Public free functions for ferrosa-storage integration
// ---------------------------------------------------------------------------

/// Build an HNSW graph from drained memtable entries and encode it as a
/// binary sidecar (see [`build_and_write`]).
///
/// Called by `ferrosa-storage::TableStore` at flush time to persist the
/// in-memory vector index as a `{gen}-VEC-{index_name}.db` sidecar file.
///
/// # Errors
///
/// Returns `IndexError::Format` if the entries differ in dimension.
pub fn build_and_serialize<V: AsRef<[f32]>>(
    m: usize,
    ef_construction: usize,
    metric: DistanceMetric,
    entries: impl IntoIterator<Item = (RowPosition, V)>,
) -> Result<Vec<u8>, IndexError> {
    let mut bytes = Vec::new();
    build_and_write(m, ef_construction, metric, entries, &mut bytes)?;
    Ok(bytes)
}

/// Build an HNSW graph from `entries` and write it to `out` as a binary
/// sidecar, the layout a search reads in place (`hnsw_binary`).
///
/// The graph holds each vector as given: owned vectors are moved in, and
/// borrowed ones (`&[f32]`, `Arc<[f32]>`) are not copied at all. It is
/// written from its own tables straight into `out`, so the encoding is never
/// held whole. `out` should be buffered.
///
/// # Errors
///
/// `IndexError::Format` if the entries differ in dimension or a write to
/// `out` fails.
pub fn build_and_write<V: AsRef<[f32]>, W: std::io::Write>(
    m: usize,
    ef_construction: usize,
    metric: DistanceMetric,
    entries: impl IntoIterator<Item = (RowPosition, V)>,
    out: W,
) -> Result<(), IndexError> {
    let mut graph = HnswGraph::new(m, ef_construction, metric);
    for (pos, vector) in entries {
        graph.insert(vector, pos);
    }
    super::hnsw_binary::write(out, &graph.tables())
}

/// Search a vector sidecar for the `k` approximate nearest neighbors of
/// `query`.
///
/// A binary sidecar is searched where it lies: only the nodes the search
/// visits are read, and nothing proportional to the graph is allocated, so
/// `bytes` should be a mapped file. A legacy JSON sidecar, written before the
/// binary format, is decoded and then searched; it is replaced by a binary
/// one when its generation is next rebuilt or compacted.
///
/// # Errors
///
/// Returns `IndexError::Format` if `bytes` are neither a sound binary sidecar
/// nor valid JSON-encoded `HnswGraphData`, or if the search reaches a damaged
/// part of the graph; `IndexError::DimensionMismatch` if the query dimension
/// does not match the graph.
pub fn search_from_bytes(
    bytes: &[u8],
    query: &[f32],
    k: usize,
    ef_search: usize,
) -> Result<Vec<IndexResult>, IndexError> {
    if super::hnsw_binary::is_binary(bytes) {
        return super::hnsw_binary::FlatGraph::open(bytes)?.nearest(query, k, ef_search);
    }
    let data: HnswGraphData = serde_json::from_slice(bytes)
        .map_err(|e| IndexError::Format(format!("HNSW deserialize failed: {e}")))?;
    let reader = HnswReader { data };
    reader.nearest(query, k, ef_search)
}

/// What a serialized HNSW sidecar holds, as [`inspect_bytes`] reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswSidecarStats {
    /// Vectors (graph nodes) in the sidecar.
    pub vectors: usize,
    /// Their common dimension; `None` for an empty sidecar.
    pub dimension: Option<usize>,
}

/// Check a vector sidecar is internally consistent, without searching it.
///
/// A binary sidecar is checked where it lies: every offset and neighbour id,
/// with no allocation proportional to it. A legacy JSON sidecar is streamed:
/// every vector and neighbour list is checked as it is parsed and none is
/// kept. Either way memory is a few counters whatever the sidecar's size,
/// and a sidecar that passes is one every search can walk.
///
/// # Errors
///
/// `IndexError::Format` if the bytes do not decode, if the node, position
/// and layer tables disagree in length, if the entry point, the top layer or
/// a neighbour is out of range, or if two vectors differ in dimension.
pub fn inspect_bytes(bytes: &[u8]) -> Result<HnswSidecarStats, IndexError> {
    use serde::de::DeserializeSeed;
    if super::hnsw_binary::is_binary(bytes) {
        return super::hnsw_binary::FlatGraph::open(bytes)?.validate();
    }
    let mut de = serde_json::Deserializer::from_slice(bytes);
    let shape = shape::SidecarShapeSeed
        .deserialize(&mut de)
        .and_then(|shape| de.end().map(|()| shape))
        .map_err(|e| IndexError::Format(format!("HNSW deserialize failed: {e}")))?;
    shape.stats()
}

/// What [`inspect_bytes`] keeps while it streams a sidecar: counts and
/// maxima, never the vectors or neighbour lists themselves.
mod shape {
    use std::fmt;

    use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
    use serde::Deserialize;

    use super::{DistanceMetric, HnswSidecarStats, IndexError, RowPosition};

    /// One layer: how many node slots it has and its largest neighbour id.
    #[derive(Debug, Default)]
    pub(super) struct LayerShape {
        nodes: usize,
        max_neighbour: Option<usize>,
    }

    /// The shape of a whole sidecar, as streamed.
    #[derive(Debug)]
    pub(super) struct SidecarShape {
        layers: Vec<LayerShape>,
        vectors: usize,
        dimension: Option<usize>,
        mixed_dimensions: bool,
        positions: usize,
        entry_point: Option<usize>,
        max_layer: usize,
    }

    impl SidecarShape {
        /// The same consistency rules the graph decode was held to.
        pub(super) fn stats(&self) -> Result<HnswSidecarStats, IndexError> {
            let nodes = self.vectors;
            let malformed =
                |what: &str| IndexError::Format(format!("HNSW sidecar is malformed: {what}"));
            if self.positions != nodes {
                return Err(malformed("positions and vectors differ in length"));
            }
            if self.entry_point.is_some_and(|entry| entry >= nodes)
                || (nodes > 0) != self.entry_point.is_some()
            {
                return Err(malformed("entry point does not match the nodes"));
            }
            // The search descends from the top layer: one past the layers
            // has nothing to descend.
            if nodes > 0 && self.max_layer >= self.layers.len() {
                return Err(malformed("the top layer is past the layers"));
            }
            for layer in &self.layers {
                if layer.nodes != nodes {
                    return Err(malformed("a layer does not cover every node"));
                }
                if layer.max_neighbour.is_some_and(|max| max >= nodes) {
                    return Err(malformed("a neighbour is out of range"));
                }
            }
            if self.mixed_dimensions {
                return Err(malformed("vectors differ in dimension"));
            }
            Ok(HnswSidecarStats {
                vectors: nodes,
                dimension: self.dimension,
            })
        }
    }

    #[derive(Deserialize)]
    #[serde(field_identifier, rename_all = "snake_case")]
    enum Field {
        Layers,
        Vectors,
        Positions,
        EntryPoint,
        MaxLayer,
        M,
        EfConstruction,
        Metric,
        #[serde(other)]
        Unknown,
    }

    /// Streams the top-level object. Requires every field the graph type
    /// requires (`entry_point` is optional there too), refuses duplicates,
    /// and ignores unknown fields, as the derived decode did.
    pub(super) struct SidecarShapeSeed;

    impl<'de> DeserializeSeed<'de> for SidecarShapeSeed {
        type Value = SidecarShape;
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<SidecarShape, D::Error> {
            d.deserialize_struct("HnswGraphData", FIELDS, self)
        }
    }

    const FIELDS: &[&str] = &[
        "layers",
        "vectors",
        "positions",
        "entry_point",
        "max_layer",
        "m",
        "ef_construction",
        "metric",
    ];

    fn once<T, E: de::Error>(slot: &mut Option<T>, value: T, name: &'static str) -> Result<(), E> {
        if slot.replace(value).is_some() {
            return Err(E::duplicate_field(name));
        }
        Ok(())
    }

    impl<'de> Visitor<'de> for SidecarShapeSeed {
        type Value = SidecarShape;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("an HNSW sidecar object")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<SidecarShape, A::Error> {
            let mut layers = None;
            let mut vectors = None;
            let mut positions = None;
            let mut entry_point: Option<Option<usize>> = None;
            let (mut max_layer, mut m, mut ef) = (None, None, None);
            let mut metric: Option<DistanceMetric> = None;
            while let Some(field) = map.next_key::<Field>()? {
                match field {
                    Field::Layers => once(&mut layers, map.next_value_seed(Layers)?, "layers")?,
                    Field::Vectors => once(&mut vectors, map.next_value_seed(Vectors)?, "vectors")?,
                    Field::Positions => once(
                        &mut positions,
                        map.next_value_seed(Count::<RowPosition>::default())?,
                        "positions",
                    )?,
                    Field::EntryPoint => once(&mut entry_point, map.next_value()?, "entry_point")?,
                    Field::MaxLayer => {
                        once(&mut max_layer, map.next_value::<usize>()?, "max_layer")?
                    }
                    Field::M => once(&mut m, map.next_value::<usize>()?, "m")?,
                    Field::EfConstruction => {
                        once(&mut ef, map.next_value::<usize>()?, "ef_construction")?
                    }
                    Field::Metric => once(&mut metric, map.next_value()?, "metric")?,
                    Field::Unknown => {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
            }
            let missing = |name| de::Error::missing_field(name);
            let max_layer = max_layer.ok_or_else(|| missing("max_layer"))?;
            m.ok_or_else(|| missing("m"))?;
            ef.ok_or_else(|| missing("ef_construction"))?;
            metric.ok_or_else(|| missing("metric"))?;
            let (vectors, dimension, mixed_dimensions) =
                vectors.ok_or_else(|| missing("vectors"))?;
            Ok(SidecarShape {
                layers: layers.ok_or_else(|| missing("layers"))?,
                vectors,
                dimension,
                mixed_dimensions,
                positions: positions.ok_or_else(|| missing("positions"))?,
                entry_point: entry_point.flatten(),
                max_layer,
            })
        }
    }

    /// `layers`: a list of layers, each a list of per-node neighbour lists.
    struct Layers;

    impl<'de> DeserializeSeed<'de> for Layers {
        type Value = Vec<LayerShape>;
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            d.deserialize_seq(self)
        }
    }

    impl<'de> Visitor<'de> for Layers {
        type Value = Vec<LayerShape>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a list of HNSW layers")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut layers = Vec::new();
            while let Some(layer) = seq.next_element_seed(Layer)? {
                layers.push(layer);
            }
            Ok(layers)
        }
    }

    /// One layer: a list of neighbour lists, one per node.
    struct Layer;

    impl<'de> DeserializeSeed<'de> for Layer {
        type Value = LayerShape;
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            d.deserialize_seq(self)
        }
    }

    impl<'de> Visitor<'de> for Layer {
        type Value = LayerShape;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("an HNSW layer")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut layer = LayerShape::default();
            while let Some(max) = seq.next_element_seed(MaxNeighbour)? {
                layer.nodes += 1;
                layer.max_neighbour = layer.max_neighbour.max(max);
            }
            Ok(layer)
        }
    }

    /// One node's neighbour list, reduced to its largest id.
    struct MaxNeighbour;

    impl<'de> DeserializeSeed<'de> for MaxNeighbour {
        type Value = Option<usize>;
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            d.deserialize_seq(self)
        }
    }

    impl<'de> Visitor<'de> for MaxNeighbour {
        type Value = Option<usize>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a list of neighbour ids")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut max = None;
            while let Some(id) = seq.next_element::<usize>()? {
                max = max.max(Some(id));
            }
            Ok(max)
        }
    }

    /// `vectors`: (count, first vector's dimension, whether any differs).
    struct Vectors;

    impl<'de> DeserializeSeed<'de> for Vectors {
        type Value = (usize, Option<usize>, bool);
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            d.deserialize_seq(self)
        }
    }

    impl<'de> Visitor<'de> for Vectors {
        type Value = (usize, Option<usize>, bool);
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a list of vectors")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let (mut count, mut dimension, mut mixed) = (0usize, None, false);
            while let Some(len) = seq.next_element_seed(Count::<f32>::default())? {
                count += 1;
                match dimension {
                    None => dimension = Some(len),
                    Some(first) => mixed |= first != len,
                }
            }
            Ok((count, dimension, mixed))
        }
    }

    /// A list of `T`, each decoded (so a wrongly typed element is refused)
    /// and dropped; only the length is kept.
    struct Count<T>(std::marker::PhantomData<T>);

    impl<T> Default for Count<T> {
        fn default() -> Self {
            Self(std::marker::PhantomData)
        }
    }

    impl<'de, T: Deserialize<'de>> DeserializeSeed<'de> for Count<T> {
        type Value = usize;
        fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<usize, D::Error> {
            d.deserialize_seq(self)
        }
    }

    impl<'de, T: Deserialize<'de>> Visitor<'de> for Count<T> {
        type Value = usize;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a list")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<usize, A::Error> {
            let mut count = 0usize;
            while seq.next_element::<T>()?.is_some() {
                count += 1;
            }
            Ok(count)
        }
    }
}

// ---------------------------------------------------------------------------
// HnswReader
// ---------------------------------------------------------------------------

/// Reads a previously built HNSW index and answers nearest-neighbor queries.
pub struct HnswReader {
    data: HnswGraphData,
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
        let data = &self.data;
        let (Some(entry), Some(first)) = (data.entry_point, data.vectors.first()) else {
            return Ok(Vec::new());
        };
        if query.len() != first.len() {
            return Err(IndexError::DimensionMismatch {
                expected: first.len(),
                got: query.len(),
            });
        }
        if data.max_layer >= data.layers.len() {
            return Err(malformed("the top layer is past the layers"));
        }
        // Searched where it was decoded: no copy of the graph per query.
        nearest_in(data, entry, data.max_layer, query, k, ef_search)?
            .into_iter()
            .map(|(id, score)| {
                let position = *data
                    .positions
                    .get(id)
                    .ok_or_else(|| malformed("a node has no position"))?;
                Ok(IndexResult { position, score })
            })
            .collect()
    }
}

/// A sidecar that does not hold together.
fn malformed(what: &str) -> IndexError {
    IndexError::Format(format!("HNSW sidecar is malformed: {what}"))
}

/// A legacy JSON sidecar, decoded. Nothing in it was checked on the way in,
/// so every lookup is: a bad id fails the search, never panics it.
impl GraphView for HnswGraphData {
    type Error = IndexError;
    type Neighbours<'g> = std::iter::Copied<std::slice::Iter<'g, usize>>;

    fn neighbours(&self, layer: usize, node: usize) -> Result<Self::Neighbours<'_>, IndexError> {
        let list = self
            .layers
            .get(layer)
            .and_then(|nodes| nodes.get(node))
            .map_or(&[][..], Vec::as_slice);
        Ok(list.iter().copied())
    }

    fn distance_to(&self, node: usize, query: &[f32]) -> Result<f32, IndexError> {
        let vector = self
            .vectors
            .get(node)
            .ok_or_else(|| malformed("a neighbour is out of range"))?;
        if vector.len() != query.len() {
            return Err(malformed("vectors differ in dimension"));
        }
        Ok(distance(&self.metric, vector, query))
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
    fn inspect_reports_count_and_dimension_and_refuses_damage() {
        let entries: Vec<(RowPosition, Vec<f32>)> = (0..20)
            .map(|i| (RowPosition::new(i), vec![i as f32, 1.0, 2.0]))
            .collect();
        let bytes = build_and_serialize(8, 50, DistanceMetric::L2, entries).unwrap();
        assert_eq!(
            inspect_bytes(&bytes).unwrap(),
            HnswSidecarStats {
                vectors: 20,
                dimension: Some(3)
            }
        );
        assert!(
            inspect_bytes(&bytes[..bytes.len() / 2]).is_err(),
            "truncated"
        );
        let empty = build_and_serialize(
            8,
            50,
            DistanceMetric::L2,
            Vec::<(RowPosition, Vec<f32>)>::new(),
        )
        .unwrap();
        assert_eq!(inspect_bytes(&empty).unwrap().vectors, 0);
        let mixed = vec![
            (RowPosition::new(0), vec![1.0, 2.0]),
            (RowPosition::new(1), vec![1.0, 2.0, 3.0]),
        ];
        assert!(
            build_and_serialize(8, 50, DistanceMetric::L2, mixed.clone()).is_err(),
            "the writer refuses mixed dimensions"
        );
        let legacy = legacy_encoding(8, 50, DistanceMetric::L2, mixed);
        assert!(
            inspect_bytes(&legacy).is_err(),
            "a legacy sidecar of mixed dimensions"
        );
    }

    /// The graph `entries` build, in both encodings: binary, as sidecars are
    /// written now, and the legacy JSON the same graph was written as before
    /// (`data_ref` is that writer, unchanged).
    fn both_encodings(
        m: usize,
        ef_construction: usize,
        metric: DistanceMetric,
        entries: Vec<(RowPosition, Vec<f32>)>,
    ) -> (Vec<u8>, Vec<u8>) {
        let graph = graph_of(m, ef_construction, metric, entries);
        let mut binary = Vec::new();
        super::super::hnsw_binary::write(&mut binary, &graph.tables()).unwrap();
        let json = serde_json::to_vec(&graph.data_ref()).unwrap();
        (binary, json)
    }

    /// The legacy JSON alone, for a graph the binary writer would refuse.
    fn legacy_encoding(
        m: usize,
        ef_construction: usize,
        metric: DistanceMetric,
        entries: Vec<(RowPosition, Vec<f32>)>,
    ) -> Vec<u8> {
        serde_json::to_vec(&graph_of(m, ef_construction, metric, entries).data_ref()).unwrap()
    }

    fn graph_of(
        m: usize,
        ef_construction: usize,
        metric: DistanceMetric,
        entries: Vec<(RowPosition, Vec<f32>)>,
    ) -> HnswGraph {
        let mut graph = HnswGraph::new(m, ef_construction, metric);
        for (pos, vector) in entries {
            graph.insert(vector, pos);
        }
        graph
    }

    /// A deterministic, well-spread vector.
    fn spread(i: usize, dim: usize) -> Vec<f32> {
        (0..dim)
            .map(|d| (((i * 31 + d * 17) % 97) as f32 / 97.0) - 0.5)
            .collect()
    }

    /// The upgrade path: a sidecar written before the binary format is
    /// still searched and checked, and answers exactly as the binary
    /// encoding of the same graph does — same rows, same scores to the bit,
    /// for every metric.
    #[test]
    fn a_binary_sidecar_answers_exactly_as_the_legacy_json_of_the_same_graph() {
        for metric in [
            DistanceMetric::L2,
            DistanceMetric::Cosine,
            DistanceMetric::InnerProduct,
        ] {
            let entries = (0..300)
                .map(|i| (RowPosition::new(i as u64 * 10), spread(i, 8)))
                .collect();
            let (binary, legacy) = both_encodings(8, 40, metric, entries);
            assert!(super::super::hnsw_binary::is_binary(&binary));
            assert!(!super::super::hnsw_binary::is_binary(&legacy));
            assert_eq!(
                inspect_bytes(&binary).unwrap(),
                inspect_bytes(&legacy).unwrap()
            );
            for q in [0usize, 7, 150, 299, 1_000] {
                let query = spread(q, 8);
                let from_binary = search_from_bytes(&binary, &query, 10, 32).unwrap();
                let from_legacy = search_from_bytes(&legacy, &query, 10, 32).unwrap();
                assert_eq!(from_binary.len(), 10, "{metric:?} q={q}");
                let shape = |r: &[IndexResult]| -> Vec<(u64, u32)> {
                    r.iter()
                        .map(|r| (r.position.offset, r.score.to_bits()))
                        .collect()
                };
                assert_eq!(shape(&from_binary), shape(&from_legacy), "{metric:?} q={q}");
            }
        }
    }

    /// A stored vector is its own nearest neighbour: the in-place search
    /// reads the vectors and the positions where they lie.
    #[test]
    fn a_binary_sidecar_finds_each_of_its_vectors() {
        let entries: Vec<(RowPosition, Vec<f32>)> = (0..30)
            .map(|i| (RowPosition::new(i), vec![i as f32, (30 - i) as f32]))
            .collect();
        let mut written = Vec::new();
        build_and_write(8, 40, DistanceMetric::L2, entries, &mut written).unwrap();
        for i in 0..30u64 {
            let nearest = search_from_bytes(&written, &[i as f32, (30 - i) as f32], 1, 16).unwrap();
            assert_eq!(nearest[0].position, RowPosition::new(i));
            assert_eq!(nearest[0].score, 0.0);
        }
        assert!(matches!(
            search_from_bytes(&written, &[1.0], 1, 16),
            Err(IndexError::DimensionMismatch {
                expected: 2,
                got: 1
            })
        ));
        let empty = build_and_serialize(
            8,
            40,
            DistanceMetric::L2,
            Vec::<(RowPosition, Vec<f32>)>::new(),
        )
        .unwrap();
        assert!(search_from_bytes(&empty, &[1.0, 2.0], 3, 16)
            .unwrap()
            .is_empty());
    }

    /// One edit that breaks a sound binary sidecar of `NODES` 2-d nodes.
    type BinaryDamage = fn(&mut Vec<u8>);

    /// Every damage is refused by the check, and no damage panics a search:
    /// it answers or it fails, loudly.
    #[test]
    fn a_damaged_binary_sidecar_is_refused_and_never_panics_a_search() {
        const NODES: usize = 12;
        let entries: Vec<(RowPosition, Vec<f32>)> = (0..NODES)
            .map(|i| (RowPosition::new(i as u64), vec![i as f32, 1.0]))
            .collect();
        let sound = build_and_serialize(4, 20, DistanceMetric::L2, entries).unwrap();
        assert_eq!(inspect_bytes(&sound).unwrap().vectors, NODES);
        // Layer 0's offsets and ids, past the header, vectors and positions.
        let offsets0 = 64 + NODES * 2 * 4 + NODES * 8;
        let ids0 = offsets0 + (NODES + 1) * 8;
        fn put_u64(b: &mut [u8], at: usize, v: u64) {
            b[at..at + 8].copy_from_slice(&v.to_le_bytes());
        }
        let cases: [(&str, BinaryDamage); 10] = [
            ("truncated by a byte", |b| {
                b.pop();
            }),
            ("a trailing byte", |b| b.push(0)),
            ("an unknown version", |b| b[4] = 9),
            ("an unknown metric", |b| b[8] = 9),
            ("the entry point out of range", |b| {
                put_u64(b, 40, NODES as u64)
            }),
            ("no layers", |b| put_u64(b, 56, 0)),
            ("an absurd layer count", |b| put_u64(b, 56, u64::MAX)),
            ("the top layer past the layers", |b| put_u64(b, 48, 200)),
            ("a neighbour past the last node", |b| {
                let at = 64 + NODES * 2 * 4 + NODES * 8 + (NODES + 1) * 8;
                b[at..at + 4].copy_from_slice(&(NODES as u32).to_le_bytes());
            }),
            ("neighbour offsets out of order", |b| {
                put_u64(b, 64 + NODES * 2 * 4 + NODES * 8 + 8, u64::MAX)
            }),
        ];
        assert!(ids0 < sound.len(), "the fixture has layer-0 ids to damage");
        for (what, damage) in cases {
            let mut bytes = sound.clone();
            damage(&mut bytes);
            assert!(inspect_bytes(&bytes).is_err(), "{what} must be refused");
            for q in 0..NODES {
                // Either answer is acceptable; a panic is not.
                let _answered_or_refused =
                    search_from_bytes(&bytes, &[q as f32, 1.0], 3, 8).map(|r| r.len());
            }
        }
    }

    /// One edit that breaks a sound sidecar.
    type Damage = fn(&mut serde_json::Value);

    /// A sound legacy sidecar, as JSON, with one damage applied by `damage`.
    fn damaged(damage: impl FnOnce(&mut serde_json::Value)) -> Vec<u8> {
        let entries: Vec<(RowPosition, Vec<f32>)> = (0..12)
            .map(|i| (RowPosition::new(i), vec![i as f32, 1.0]))
            .collect();
        let (_, bytes) = both_encodings(4, 20, DistanceMetric::L2, entries);
        let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        damage(&mut json);
        serde_json::to_vec(&json).unwrap()
    }

    /// The streaming inspector refuses every damage the graph decode refused,
    /// and accepts what it accepted. Written with the streaming rewrite of
    /// `inspect_bytes`; against the decode it replaced these all pass too, so
    /// this is a parity guard, not the red (the red is the engine test
    /// `vector_self_heal_memory_bound`). The one addition, a top layer past
    /// the layers, came with the shared search, which refuses it: a sidecar
    /// the check passes must be one every search can walk.
    #[test]
    fn inspect_refuses_each_inconsistency_the_graph_decode_refused() {
        let sound = damaged(|_| {});
        assert_eq!(inspect_bytes(&sound).unwrap().vectors, 12);
        let cases: [(&str, Damage); 9] = [
            ("a neighbour past the last node", |j| {
                j["layers"][0][0] = serde_json::json!([12])
            }),
            ("a layer short of a node", |j| {
                j["layers"][0].as_array_mut().unwrap().pop();
            }),
            ("a position missing", |j| {
                j["positions"].as_array_mut().unwrap().pop();
            }),
            ("the entry point out of range", |j| {
                j["entry_point"] = serde_json::json!(12)
            }),
            ("no entry point over nodes", |j| {
                j["entry_point"] = serde_json::Value::Null
            }),
            ("a required field missing", |j| {
                j.as_object_mut().unwrap().remove("metric");
            }),
            ("a vector element that is not a number", |j| {
                j["vectors"][3][0] = serde_json::json!("x")
            }),
            ("a negative neighbour id", |j| {
                j["layers"][0][1] = serde_json::json!([-1])
            }),
            ("the top layer past the layers", |j| {
                j["max_layer"] = serde_json::json!(200)
            }),
        ];
        for (what, damage) in cases {
            assert!(
                inspect_bytes(&damaged(damage)).is_err(),
                "{what} must be refused"
            );
        }
        let unknown = damaged(|j| j["future_field"] = serde_json::json!({"x": [1, 2]}));
        assert_eq!(
            inspect_bytes(&unknown).unwrap().vectors,
            12,
            "an unknown field is ignored, as the derived decode ignores it"
        );
        let mut trailing = sound.clone();
        trailing.extend_from_slice(b" {}");
        assert!(inspect_bytes(&trailing).is_err(), "trailing bytes");
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
