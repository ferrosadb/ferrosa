//! The binary HNSW sidecar: a graph laid out so a search reads it where it
//! lies, a mapped file, without decoding it.
//!
//! The JSON encoding it replaces had to be parsed whole before a single
//! distance could be taken, so every ANN query paid a full decode of every
//! sidecar it touched: a heap the size of the graph, per query. Here a search
//! touches only the nodes it visits, and the page cache, not the heap, holds
//! the file.
//!
//! Layout, every integer little-endian:
//!
//! | bytes | field |
//! |---|---|
//! | 4 | magic `FHNW` |
//! | 4 | format version (1) |
//! | 4 | metric: 0 L2, 1 cosine, 2 inner product |
//! | 4 | dimension |
//! | 8 | nodes |
//! | 8 | `m` |
//! | 8 | `ef_construction` |
//! | 8 | entry point, `u64::MAX` for none |
//! | 8 | top layer |
//! | 8 | layers |
//! | nodes x dimension x 4 | vectors, `f32` |
//! | nodes x 8 | row positions |
//! | per layer: (nodes + 1) x 8, then n x 4 | neighbour offsets, then the neighbour ids (`u32`) they index |
//!
//! Nothing is aligned, so a sidecar may sit at any offset in any buffer.
//!
//! Reading checks the layout's arithmetic once ([`FlatGraph::open`]) and
//! every id it follows as it follows it, so a damaged sidecar fails the
//! search loudly instead of panicking it; [`FlatGraph::validate`] checks
//! every offset and id up front, for the self-heal check.

use std::io::Write;

use super::hnsw::{nearest_in, GraphView, HnswSidecarStats};
use super::{distance_le, DistanceMetric, IndexError, IndexResult, RowPosition};

const MAGIC: &[u8; 4] = b"FHNW";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 64;
const NO_ENTRY: u64 = u64::MAX;
/// More layers than any graph grows: with `m >= 2` the expected top layer of
/// 2^32 nodes is about 32. A header claiming more is damaged, and is refused
/// before anything is sized by it.
const MAX_LAYERS: usize = 128;

/// Whether `bytes` are a binary sidecar (and not a legacy JSON one).
pub(super) fn is_binary(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

/// A graph's tables, borrowed for writing.
pub(super) struct Tables<'a, V> {
    pub(super) layers: &'a [Vec<Vec<usize>>],
    pub(super) vectors: &'a [V],
    pub(super) positions: &'a [RowPosition],
    pub(super) entry_point: Option<usize>,
    pub(super) max_layer: usize,
    pub(super) m: usize,
    pub(super) ef_construction: usize,
    pub(super) metric: DistanceMetric,
}

fn metric_code(metric: DistanceMetric) -> u32 {
    match metric {
        DistanceMetric::L2 => 0,
        DistanceMetric::Cosine => 1,
        DistanceMetric::InnerProduct => 2,
    }
}

fn metric_of(code: u32) -> Option<DistanceMetric> {
    match code {
        0 => Some(DistanceMetric::L2),
        1 => Some(DistanceMetric::Cosine),
        2 => Some(DistanceMetric::InnerProduct),
        _ => None,
    }
}

fn malformed(what: &str) -> IndexError {
    IndexError::Format(format!("binary HNSW sidecar is malformed: {what}"))
}

fn write_failed(e: std::io::Error) -> IndexError {
    IndexError::Format(format!("HNSW sidecar write failed: {e}"))
}

/// Write `tables` to `out`, streamed: nothing proportional to the graph is
/// allocated. `out` should be buffered.
///
/// # Errors
///
/// The tables do not describe one graph (a layer or the positions miss a
/// node, the vectors differ in dimension, more than `u32::MAX` nodes), or a
/// write fails.
pub(super) fn write<V: AsRef<[f32]>>(
    mut out: impl Write,
    tables: &Tables<'_, V>,
) -> Result<(), IndexError> {
    let nodes = tables.vectors.len();
    let dimension = tables.vectors.first().map_or(0, |v| v.as_ref().len());
    check_tables(tables, nodes, dimension)?;

    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(MAGIC);
    header[4..8].copy_from_slice(&VERSION.to_le_bytes());
    header[8..12].copy_from_slice(&metric_code(tables.metric).to_le_bytes());
    header[12..16].copy_from_slice(&(dimension as u32).to_le_bytes());
    let words = [
        nodes as u64,
        tables.m as u64,
        tables.ef_construction as u64,
        tables.entry_point.map_or(NO_ENTRY, |e| e as u64),
        tables.max_layer as u64,
        tables.layers.len() as u64,
    ];
    for (slot, word) in header[16..].as_chunks_mut::<8>().0.iter_mut().zip(words) {
        *slot = word.to_le_bytes();
    }
    out.write_all(&header).map_err(write_failed)?;

    for vector in tables.vectors {
        for x in vector.as_ref() {
            out.write_all(&x.to_le_bytes()).map_err(write_failed)?;
        }
    }
    for position in tables.positions {
        out.write_all(&position.offset.to_le_bytes())
            .map_err(write_failed)?;
    }
    for layer in tables.layers {
        let mut offset = 0u64;
        out.write_all(&offset.to_le_bytes()).map_err(write_failed)?;
        for list in layer {
            offset += list.len() as u64;
            out.write_all(&offset.to_le_bytes()).map_err(write_failed)?;
        }
        for &id in layer.iter().flatten() {
            out.write_all(&(id as u32).to_le_bytes())
                .map_err(write_failed)?;
        }
    }
    Ok(())
}

/// The invariants the writer relies on, so it never writes a sidecar the
/// reader would refuse.
fn check_tables<V: AsRef<[f32]>>(
    tables: &Tables<'_, V>,
    nodes: usize,
    dimension: usize,
) -> Result<(), IndexError> {
    let refuse = |what: &str| IndexError::Format(format!("cannot write HNSW sidecar: {what}"));
    if u32::try_from(nodes).is_err() || u32::try_from(dimension).is_err() {
        return Err(refuse("more nodes or dimensions than the format addresses"));
    }
    if tables.positions.len() != nodes {
        return Err(refuse("positions and vectors differ in length"));
    }
    if tables.vectors.iter().any(|v| v.as_ref().len() != dimension) {
        return Err(refuse("vectors differ in dimension"));
    }
    if tables.layers.is_empty()
        || tables.layers.len() > MAX_LAYERS
        || tables.max_layer >= tables.layers.len()
    {
        return Err(refuse("the layers do not match the top layer"));
    }
    if tables.layers.iter().any(|layer| layer.len() != nodes) {
        return Err(refuse("a layer does not cover every node"));
    }
    if (nodes > 0) != tables.entry_point.is_some_and(|e| e < nodes) {
        return Err(refuse("entry point does not match the nodes"));
    }
    Ok(())
}

/// Where one layer lies in the sidecar.
#[derive(Debug, Clone, Copy)]
struct LayerAt {
    /// `nodes + 1` offsets into this layer's ids.
    offsets_at: usize,
    ids_at: usize,
    ids: usize,
}

/// A binary sidecar, read where it lies.
#[derive(Debug)]
pub(super) struct FlatGraph<'a> {
    bytes: &'a [u8],
    metric: DistanceMetric,
    dimension: usize,
    nodes: usize,
    entry_point: Option<usize>,
    max_layer: usize,
    vectors_at: usize,
    positions_at: usize,
    /// One per layer: a handful, bounded by [`MAX_LAYERS`].
    layers: Vec<LayerAt>,
}

fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    let word = bytes.get(at..at.checked_add(4)?)?;
    Some(u32::from_le_bytes(word.try_into().ok()?))
}

fn u64_at(bytes: &[u8], at: usize) -> Option<u64> {
    let word = bytes.get(at..at.checked_add(8)?)?;
    Some(u64::from_le_bytes(word.try_into().ok()?))
}

/// `a * b + c`, or `None` if it overflows.
fn span(a: usize, b: usize, c: usize) -> Option<usize> {
    a.checked_mul(b)?.checked_add(c)
}

impl<'a> FlatGraph<'a> {
    /// Read the header and lay out the sections. Checks that the sections
    /// fill `bytes` exactly; reads nothing proportional to the graph.
    ///
    /// # Errors
    ///
    /// `IndexError::Format` if the header is damaged or the sections do not
    /// add up to the sidecar's length.
    pub(super) fn open(bytes: &'a [u8]) -> Result<Self, IndexError> {
        if bytes.len() < HEADER_LEN || !is_binary(bytes) {
            return Err(malformed("no header"));
        }
        let field = |at| u64_at(bytes, at).ok_or_else(|| malformed("no header"));
        let version = u32_at(bytes, 4).ok_or_else(|| malformed("no header"))?;
        if version != VERSION {
            return Err(malformed(&format!("unknown format version {version}")));
        }
        let metric = u32_at(bytes, 8)
            .and_then(metric_of)
            .ok_or_else(|| malformed("unknown metric"))?;
        let dimension = u32_at(bytes, 12).ok_or_else(|| malformed("no header"))? as usize;
        let as_usize =
            |word: u64| usize::try_from(word).map_err(|_| malformed("a count overflows"));
        let nodes = as_usize(field(16)?)?;
        let entry = field(40)?;
        let max_layer = as_usize(field(48)?)?;
        let layer_count = as_usize(field(56)?)?;
        if layer_count == 0 || layer_count > MAX_LAYERS || max_layer >= layer_count {
            return Err(malformed("the layers do not match the top layer"));
        }
        let entry_point = (entry != NO_ENTRY).then(|| as_usize(entry)).transpose()?;
        if (nodes > 0) != entry_point.is_some_and(|e| e < nodes) {
            return Err(malformed("entry point does not match the nodes"));
        }

        let overflow = || malformed("a section overflows");
        let vectors_at = HEADER_LEN;
        let positions_at = span(nodes, dimension * 4, vectors_at).ok_or_else(overflow)?;
        let mut cursor = span(nodes, 8, positions_at).ok_or_else(overflow)?;
        let mut layers = Vec::with_capacity(layer_count);
        for _ in 0..layer_count {
            let offsets_at = cursor;
            let ids_at = span(nodes + 1, 8, offsets_at).ok_or_else(overflow)?;
            let ids = u64_at(bytes, span(nodes, 8, offsets_at).ok_or_else(overflow)?)
                .ok_or_else(|| malformed("shorter than its sections"))?;
            let ids = as_usize(ids)?;
            cursor = span(ids, 4, ids_at).ok_or_else(overflow)?;
            layers.push(LayerAt {
                offsets_at,
                ids_at,
                ids,
            });
        }
        if cursor != bytes.len() {
            return Err(malformed("its sections do not fill it exactly"));
        }
        Ok(Self {
            bytes,
            metric,
            dimension,
            nodes,
            entry_point,
            max_layer,
            vectors_at,
            positions_at,
            layers,
        })
    }

    /// Node `node`'s neighbour ids at `layer`, unchecked against the nodes.
    fn id_bytes(&self, layer: &LayerAt, node: usize) -> Result<&'a [u8], IndexError> {
        if node >= self.nodes {
            return Err(malformed("a neighbour is out of range"));
        }
        let offset = |i: usize| {
            u64_at(self.bytes, layer.offsets_at + i * 8)
                .and_then(|o| usize::try_from(o).ok())
                .ok_or_else(|| malformed("shorter than its sections"))
        };
        let (start, end) = (offset(node)?, offset(node + 1)?);
        if start > end || end > layer.ids {
            return Err(malformed("neighbour offsets are out of order"));
        }
        Ok(&self.bytes[layer.ids_at + start * 4..layer.ids_at + end * 4])
    }

    /// Node `node`'s row position.
    fn position(&self, node: usize) -> Result<RowPosition, IndexError> {
        if node >= self.nodes {
            return Err(malformed("a neighbour is out of range"));
        }
        u64_at(self.bytes, self.positions_at + node * 8)
            .map(RowPosition::new)
            .ok_or_else(|| malformed("shorter than its sections"))
    }

    /// The `k` approximate nearest neighbours of `query`.
    ///
    /// # Errors
    ///
    /// `IndexError::DimensionMismatch` for a query of another dimension;
    /// `IndexError::Format` if the search reaches a damaged part.
    pub(super) fn nearest(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<IndexResult>, IndexError> {
        let Some(entry) = self.entry_point else {
            return Ok(Vec::new());
        };
        if query.len() != self.dimension {
            return Err(IndexError::DimensionMismatch {
                expected: self.dimension,
                got: query.len(),
            });
        }
        nearest_in(self, entry, self.max_layer, query, k, ef_search)?
            .into_iter()
            .map(|(id, score)| {
                Ok(IndexResult {
                    position: self.position(id)?,
                    score,
                })
            })
            .collect()
    }

    /// Check every offset and every neighbour id, without allocating.
    ///
    /// # Errors
    ///
    /// `IndexError::Format` for the first offset out of order or id out of
    /// range.
    pub(super) fn validate(&self) -> Result<HnswSidecarStats, IndexError> {
        for layer in &self.layers {
            // The offsets run from 0 to the id count the layout was sized
            // by, never backwards, so every id belongs to exactly one node.
            if u64_at(self.bytes, layer.offsets_at) != Some(0) {
                return Err(malformed("neighbour offsets do not start at 0"));
            }
            for node in 0..self.nodes {
                for word in self.id_bytes(layer, node)?.as_chunks::<4>().0 {
                    if u32::from_le_bytes(*word) as usize >= self.nodes {
                        return Err(malformed("a neighbour is out of range"));
                    }
                }
            }
        }
        Ok(HnswSidecarStats {
            vectors: self.nodes,
            dimension: (self.nodes > 0).then_some(self.dimension),
        })
    }
}

/// Node ids read from a neighbour list, each checked against the nodes as
/// it is read.
pub(super) struct Ids<'a> {
    words: std::slice::Iter<'a, [u8; 4]>,
}

impl Iterator for Ids<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        self.words.next().map(|w| u32::from_le_bytes(*w) as usize)
    }
}

impl<'a> GraphView for FlatGraph<'a> {
    type Error = IndexError;
    type Neighbours<'g>
        = Ids<'g>
    where
        Self: 'g;

    fn neighbours(&self, layer: usize, node: usize) -> Result<Ids<'_>, IndexError> {
        let Some(at) = self.layers.get(layer) else {
            return Ok(Ids { words: [].iter() });
        };
        let ids = self.id_bytes(at, node)?.as_chunks::<4>().0;
        // Checked here so the search never follows an id to nowhere.
        if ids
            .iter()
            .any(|w| u32::from_le_bytes(*w) as usize >= self.nodes)
        {
            return Err(malformed("a neighbour is out of range"));
        }
        Ok(Ids { words: ids.iter() })
    }

    fn distance_to(&self, node: usize, query: &[f32]) -> Result<f32, IndexError> {
        if node >= self.nodes {
            return Err(malformed("a neighbour is out of range"));
        }
        let width = self.dimension * 4;
        let at = self.vectors_at + node * width;
        Ok(distance_le(
            &self.metric,
            &self.bytes[at..at + width],
            query,
        ))
    }
}
