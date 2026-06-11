//! A bulk-loaded, read-only R-tree over axis-aligned bounding boxes.
//!
//! This is the reusable spatial-candidate structure for the geo layer. It is
//! built once from a slice of `(bbox, value)` pairs using **Sort-Tile-Recursive
//! (STR)** packing, then answers `query_bbox` (all values whose bbox overlaps a
//! query box). It is **bounded**: the tree is a flat `Vec` of nodes, querying is
//! iterative (an explicit stack, no recursion), and every result is materialised
//! from a finite candidate set.
//!
//! In the `ST_WITHIN` query path it prunes which of several query polygons a
//! given point could fall inside: index each polygon's bbox, then `query_bbox`
//! with the point's degenerate bbox to get the small set of candidate polygons
//! to run the exact point-in-polygon test against. The same structure is the
//! natural home for stored-geometry indexing once a GEOMETRY column type lands.

/// An axis-aligned bounding box in `(lat, lon)` space. Stored as inclusive
/// `min`/`max` corners. Built so that a single point is the degenerate box with
/// `min == max`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RtreeBbox {
    /// Minimum corner `(lat, lon)`.
    pub min: (f64, f64),
    /// Maximum corner `(lat, lon)`.
    pub max: (f64, f64),
}

impl RtreeBbox {
    /// Construct a bbox from two corners in any order, normalising to
    /// `min`/`max`.
    pub fn new(a: (f64, f64), b: (f64, f64)) -> Self {
        RtreeBbox {
            min: (a.0.min(b.0), a.1.min(b.1)),
            max: (a.0.max(b.0), a.1.max(b.1)),
        }
    }

    /// The degenerate bbox of a single `(lat, lon)` point.
    pub fn point(lat: f64, lon: f64) -> Self {
        RtreeBbox {
            min: (lat, lon),
            max: (lat, lon),
        }
    }

    /// True if this box overlaps `other` (touching edges count as overlap).
    pub fn overlaps(&self, other: &RtreeBbox) -> bool {
        self.min.0 <= other.max.0
            && self.max.0 >= other.min.0
            && self.min.1 <= other.max.1
            && self.max.1 >= other.min.1
    }

    /// The smallest box enclosing both `self` and `other`.
    fn union(&self, other: &RtreeBbox) -> RtreeBbox {
        RtreeBbox {
            min: (self.min.0.min(other.min.0), self.min.1.min(other.min.1)),
            max: (self.max.0.max(other.max.0), self.max.1.max(other.max.1)),
        }
    }

    /// Centre latitude — the key for the first STR sort pass.
    fn center_lat(&self) -> f64 {
        (self.min.0 + self.max.0) / 2.0
    }

    /// Centre longitude — the key for the second STR sort pass.
    fn center_lon(&self) -> f64 {
        (self.min.1 + self.max.1) / 2.0
    }
}

/// Maximum entries per node. A small fixed fan-out keeps node bboxes tight and
/// bounds the per-node scan during a query.
const NODE_CAPACITY: usize = 8;

/// A node in the flat R-tree. Leaves hold value indices; internal nodes hold
/// child node indices. Both carry the bbox enclosing their subtree.
#[derive(Debug, Clone)]
struct Node {
    bbox: RtreeBbox,
    /// For a leaf: indices into `values`. For an internal node: indices into
    /// `nodes`.
    children: Vec<usize>,
    is_leaf: bool,
}

/// A bulk-loaded R-tree mapping axis-aligned bboxes to opaque values `T`.
#[derive(Debug, Clone)]
pub struct Rtree<T> {
    nodes: Vec<Node>,
    /// `(bbox, value)` for every leaf entry, indexed by leaf `children`.
    values: Vec<(RtreeBbox, T)>,
    /// Index of the root node in `nodes`, or `None` for an empty tree.
    root: Option<usize>,
}

impl<T> Rtree<T> {
    /// Bulk-load an R-tree from `(bbox, value)` pairs using STR packing.
    ///
    /// The entries are sorted by centre latitude, sliced into
    /// `ceil(sqrt(n / capacity))` vertical strips, each strip sorted by centre
    /// longitude and packed into leaves of up to [`NODE_CAPACITY`] entries.
    /// Internal levels are then packed bottom-up the same way until a single
    /// root remains. Tree height is `O(log n)` so the iterative query stack is
    /// bounded.
    pub fn bulk_load(entries: Vec<(RtreeBbox, T)>) -> Self {
        let values = entries;
        if values.is_empty() {
            return Rtree {
                nodes: Vec::new(),
                values,
                root: None,
            };
        }

        let mut nodes: Vec<Node> = Vec::new();

        // ── Build leaf level over value indices, STR-ordered. ──
        let mut order: Vec<usize> = (0..values.len()).collect();
        order.sort_by(|&a, &b| {
            values[a]
                .0
                .center_lat()
                .partial_cmp(&values[b].0.center_lat())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        str_sort_within_strips(&mut order, |idx| values[idx].0.center_lon(), values.len());

        let mut level: Vec<usize> = Vec::new();
        for chunk in order.chunks(NODE_CAPACITY) {
            let children = chunk.to_vec();
            let bbox = enclosing_bbox(children.iter().map(|&i| values[i].0));
            nodes.push(Node {
                bbox,
                children,
                is_leaf: true,
            });
            level.push(nodes.len() - 1);
        }

        // ── Pack internal levels bottom-up until one root remains. ──
        // Bounded: each level shrinks by at least a factor of NODE_CAPACITY, so
        // the number of iterations is O(log_capacity(n)).
        while level.len() > 1 {
            let mut ordered = level.clone();
            ordered.sort_by(|&a, &b| {
                nodes[a]
                    .bbox
                    .center_lat()
                    .partial_cmp(&nodes[b].bbox.center_lat())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            str_sort_within_strips(
                &mut ordered,
                |idx| nodes[idx].bbox.center_lon(),
                level.len(),
            );

            let mut next: Vec<usize> = Vec::new();
            for chunk in ordered.chunks(NODE_CAPACITY) {
                let children = chunk.to_vec();
                let bbox = enclosing_bbox(children.iter().map(|&i| nodes[i].bbox));
                nodes.push(Node {
                    bbox,
                    children,
                    is_leaf: false,
                });
                next.push(nodes.len() - 1);
            }
            level = next;
        }

        let root = level.first().copied();
        Rtree {
            nodes,
            values,
            root,
        }
    }

    /// Return references to every value whose bbox overlaps `query`.
    ///
    /// Traversal is iterative with an explicit stack (no recursion). The stack
    /// depth is bounded by tree height (`O(log n)`) and the result set by the
    /// number of stored entries, so the call cannot blow the native stack or
    /// allocate unboundedly.
    pub fn query_bbox(&self, query: &RtreeBbox) -> Vec<&T> {
        let mut out = Vec::new();
        let Some(root) = self.root else {
            return out;
        };
        let mut stack: Vec<usize> = vec![root];
        while let Some(node_idx) = stack.pop() {
            let node = &self.nodes[node_idx];
            if !node.bbox.overlaps(query) {
                continue;
            }
            if node.is_leaf {
                for &val_idx in &node.children {
                    let (bbox, value) = &self.values[val_idx];
                    if bbox.overlaps(query) {
                        out.push(value);
                    }
                }
            } else {
                stack.extend_from_slice(&node.children);
            }
        }
        out
    }

    /// Number of stored `(bbox, value)` entries.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// True if the tree holds no entries.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Smallest bbox enclosing an iterator of bboxes. Panics on an empty iterator —
/// callers always pass at least one (a node always has children).
fn enclosing_bbox(mut boxes: impl Iterator<Item = RtreeBbox>) -> RtreeBbox {
    let first = boxes
        .next()
        .expect("node must have at least one child bbox");
    boxes.fold(first, |acc, b| acc.union(&b))
}

/// STR second pass: partition the lat-sorted `order` into vertical strips of
/// `~sqrt(total/capacity) * capacity` entries, then sort each strip by the
/// `lon_key`. `order` is mutated in place.
fn str_sort_within_strips<F>(order: &mut [usize], lon_key: F, total: usize)
where
    F: Fn(usize) -> f64,
{
    if order.len() <= NODE_CAPACITY {
        order.sort_by(|&a, &b| {
            lon_key(a)
                .partial_cmp(&lon_key(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        return;
    }
    let leaf_count = total.div_ceil(NODE_CAPACITY);
    let strip_count = (leaf_count as f64).sqrt().ceil() as usize;
    let strip_count = strip_count.max(1);
    let strip_len = (order.len().div_ceil(strip_count)).max(1);
    for strip in order.chunks_mut(strip_len) {
        strip.sort_by(|&a, &b| {
            lon_key(a)
                .partial_cmp(&lon_key(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tree_returns_nothing() {
        let tree: Rtree<u32> = Rtree::bulk_load(vec![]);
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
        let hits = tree.query_bbox(&RtreeBbox::point(0.0, 0.0));
        assert!(hits.is_empty());
    }

    #[test]
    fn single_entry_overlap_and_miss() {
        let tree = Rtree::bulk_load(vec![(RtreeBbox::new((0.0, 0.0), (1.0, 1.0)), 42u32)]);
        assert_eq!(tree.len(), 1);
        let hit = tree.query_bbox(&RtreeBbox::point(0.5, 0.5));
        assert_eq!(hit, vec![&42]);
        let miss = tree.query_bbox(&RtreeBbox::point(5.0, 5.0));
        assert!(miss.is_empty());
    }

    #[test]
    fn point_query_selects_overlapping_boxes_only() {
        // Three disjoint boxes; a point inside the middle one hits only it.
        let entries = vec![
            (RtreeBbox::new((0.0, 0.0), (1.0, 1.0)), "a"),
            (RtreeBbox::new((10.0, 10.0), (11.0, 11.0)), "b"),
            (RtreeBbox::new((20.0, 20.0), (21.0, 21.0)), "c"),
        ];
        let tree = Rtree::bulk_load(entries);
        let hits = tree.query_bbox(&RtreeBbox::point(10.5, 10.5));
        assert_eq!(hits, vec![&"b"]);
    }

    #[test]
    fn overlapping_boxes_all_returned() {
        let entries = vec![
            (RtreeBbox::new((0.0, 0.0), (2.0, 2.0)), 1u32),
            (RtreeBbox::new((1.0, 1.0), (3.0, 3.0)), 2u32),
            (RtreeBbox::new((100.0, 100.0), (101.0, 101.0)), 3u32),
        ];
        let tree = Rtree::bulk_load(entries);
        let mut hits: Vec<u32> = tree
            .query_bbox(&RtreeBbox::point(1.5, 1.5))
            .into_iter()
            .copied()
            .collect();
        hits.sort_unstable();
        assert_eq!(hits, vec![1, 2]);
    }

    #[test]
    fn many_entries_exhaustive_against_brute_force() {
        // Build a 12x12 grid of unit cells (144 entries → forces a multi-level
        // tree given NODE_CAPACITY=8), then verify query results match a brute
        // force overlap scan for several query boxes.
        let mut entries = Vec::new();
        for i in 0..12i32 {
            for j in 0..12i32 {
                let id = (i * 12 + j) as u32;
                entries.push((
                    RtreeBbox::new((i as f64, j as f64), (i as f64 + 0.9, j as f64 + 0.9)),
                    id,
                ));
            }
        }
        let brute = entries.clone();
        let tree = Rtree::bulk_load(entries);
        assert_eq!(tree.len(), 144);

        let queries = [
            RtreeBbox::point(3.5, 4.5),
            RtreeBbox::new((2.0, 2.0), (5.5, 5.5)),
            RtreeBbox::new((-10.0, -10.0), (-1.0, -1.0)), // no overlap
            RtreeBbox::new((0.0, 0.0), (11.9, 11.9)),     // everything
        ];
        for q in queries {
            let mut got: Vec<u32> = tree.query_bbox(&q).into_iter().copied().collect();
            got.sort_unstable();
            let mut want: Vec<u32> = brute
                .iter()
                .filter(|(b, _)| b.overlaps(&q))
                .map(|(_, v)| *v)
                .collect();
            want.sort_unstable();
            assert_eq!(got, want, "mismatch for query {q:?}");
        }
    }

    #[test]
    fn bbox_overlaps_touching_edges() {
        let a = RtreeBbox::new((0.0, 0.0), (1.0, 1.0));
        let b = RtreeBbox::new((1.0, 1.0), (2.0, 2.0));
        assert!(a.overlaps(&b), "edge-touching boxes overlap");
        let c = RtreeBbox::new((1.001, 1.001), (2.0, 2.0));
        assert!(!a.overlaps(&c));
    }

    #[test]
    fn new_normalises_corner_order() {
        let a = RtreeBbox::new((2.0, 3.0), (0.0, 1.0));
        assert_eq!(a.min, (0.0, 1.0));
        assert_eq!(a.max, (2.0, 3.0));
    }
}
