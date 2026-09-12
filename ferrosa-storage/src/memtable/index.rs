//! Persistent (functional) red-black tree for memtable-level secondary indexing.
//!
//! Insert produces a new root via O(log n) path-copying — the original tree
//! is never mutated. The current root is stored behind `ArcSwap<Option<Node>>`
//! so readers load a snapshot atomically (no locks, no contention).
//!
//! Based on Okasaki's persistent red-black tree (Purely Functional Data
//! Structures, 1998), adapted for Rust with `Arc` for structural sharing.

use std::ops::ControlFlow;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_index::{IndexKey, RowPosition};
use parking_lot::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Color {
    Red,
    Black,
}

/// A node in the persistent red-black tree.
///
/// All fields are immutable after construction. Children and values are
/// wrapped in `Arc` for O(1) structural sharing during path-copy.
#[derive(Debug, Clone)]
pub(crate) struct Node {
    color: Color,
    key: IndexKey,
    values: Vec<RowPosition>,
    left: Option<Arc<Node>>,
    right: Option<Arc<Node>>,
}

/// One index key's postings, pinned to the tree snapshot they came from.
/// Sorted by row position and unique (see [`MemtableIndex::insert`]).
pub struct PostingList {
    node: Arc<Node>,
}

impl PostingList {
    /// The postings, in row order.
    pub fn as_slice(&self) -> &[RowPosition] {
        &self.node.values
    }
}

/// Lock-free persistent red-black tree for memtable secondary indexing.
///
/// Writers acquire a `Mutex` (serializes inserts — the memtable write path
/// is already single-writer-per-partition, so this adds negligible contention).
/// Readers use `ArcSwap::load()` for a wait-free snapshot.
pub struct MemtableIndex {
    root: ArcSwap<Option<Arc<Node>>>,
    write_lock: Mutex<()>,
}

impl Default for MemtableIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl MemtableIndex {
    pub fn new() -> Self {
        Self {
            root: ArcSwap::from_pointee(None),
            write_lock: Mutex::new(()),
        }
    }

    /// Insert a key-value pair. Produces a new root via path-copying.
    /// Thread-safe: serialized by write_lock; readers see atomic swap.
    pub fn insert(&self, key: IndexKey, pos: RowPosition) {
        let _guard = self.write_lock.lock();
        let current_root = self.root.load();
        let current: Option<Arc<Node>> = (**current_root).clone();
        let new_root = Self::insert_node(current, key, pos);
        // Force root to black (Okasaki invariant)
        let blackened = Arc::new(Node {
            color: Color::Black,
            ..(*new_root).clone()
        });
        self.root.store(Arc::new(Some(blackened)));
    }

    /// Lookup all RowPositions for an exact key.
    pub fn lookup(&self, key: &IndexKey) -> Vec<RowPosition> {
        let guard = self.root.load();
        Self::lookup_in((**guard).as_ref(), key)
    }

    /// Visit exact-key postings without cloning the entire posting list.
    /// Returning `Break` stops the traversal immediately, which lets a
    /// downstream page or stream apply back-pressure at the index boundary.
    pub fn visit(&self, key: &IndexKey, visitor: &mut dyn FnMut(RowPosition) -> ControlFlow<()>) {
        let guard = self.root.load();
        Self::visit_in((**guard).as_ref(), key, visitor);
    }

    /// The key's postings in row order, pinned: the returned list holds its
    /// tree node, so it stays valid and unchanged however many inserts land
    /// while the caller walks it. `None` when the key has no postings.
    pub fn posting_list(&self, key: &IndexKey) -> Option<PostingList> {
        let guard = self.root.load();
        let mut node = (**guard).as_ref();
        while let Some(n) = node {
            node = match key.cmp(&n.key) {
                std::cmp::Ordering::Less => n.left.as_ref(),
                std::cmp::Ordering::Greater => n.right.as_ref(),
                std::cmp::Ordering::Equal => {
                    return Some(PostingList {
                        node: Arc::clone(n),
                    })
                }
            };
        }
        None
    }

    /// Range query: returns all RowPositions for keys in [start, end] inclusive.
    pub fn range(&self, start: &IndexKey, end: &IndexKey) -> Vec<RowPosition> {
        let guard = self.root.load();
        let mut results = Vec::new();
        Self::range_collect((**guard).as_ref(), start, end, &mut results);
        results
    }

    /// In-order iterator over all (key, positions) pairs.
    ///
    /// This COPIES the whole index. Prefer [`visit_all`](Self::visit_all)
    /// wherever the postings are consumed once, which is every caller on a
    /// hot path: a flush writing its sidecar reads the tree exactly once and
    /// has no use for a second copy of it.
    pub fn iter(&self) -> impl Iterator<Item = (IndexKey, Vec<RowPosition>)> {
        let guard = self.root.load();
        let mut entries = Vec::new();
        Self::collect_all((**guard).as_ref(), &mut entries);
        entries.into_iter()
    }

    /// Visit every posting in `(key, row)` order, copying nothing.
    ///
    /// The walk holds an explicit stack of the nodes on the current path —
    /// O(log n) pointers, no recursion — and lends each entry to the visitor
    /// in place. In-order traversal yields keys ascending, and a node's
    /// postings are already sorted and unique (see [`insert`](Self::insert)),
    /// so the sequence is exactly the order a sidecar wants.
    ///
    /// Returning `Break` stops the walk immediately, so a consumer that
    /// fails part-way (a write error, a full page) does not pay for the rest.
    pub fn visit_all(
        &self,
        visitor: &mut dyn FnMut(&IndexKey, &RowPosition) -> ControlFlow<()>,
    ) -> ControlFlow<()> {
        let guard = self.root.load();
        Self::walk((**guard).as_ref(), visitor)
    }

    /// Pin the tree as it stands now, for a consumer that will read it later.
    /// Holding the root is the snapshot: nothing is copied.
    pub fn pin(&self) -> IndexSnapshot {
        IndexSnapshot {
            root: (**self.root.load()).clone(),
        }
    }

    /// The in-order walk both [`visit_all`](Self::visit_all) and a pinned
    /// [`IndexSnapshot`] share.
    fn walk(
        root: Option<&Arc<Node>>,
        visitor: &mut dyn FnMut(&IndexKey, &RowPosition) -> ControlFlow<()>,
    ) -> ControlFlow<()> {
        let mut stack: Vec<&Arc<Node>> = Vec::new();
        let mut next = root;
        loop {
            while let Some(node) = next {
                stack.push(node);
                next = node.left.as_ref();
            }
            let Some(node) = stack.pop() else {
                return ControlFlow::Continue(());
            };
            for position in &node.values {
                visitor(&node.key, position)?;
            }
            next = node.right.as_ref();
        }
    }

    /// Take a snapshot of the current root for persistence guarantees.
    /// Used by flush path to capture immutable tree before SSTable sidecar write.
    #[allow(dead_code)]
    pub(crate) fn snapshot(&self) -> Option<Arc<Node>> {
        let guard = self.root.load();
        (**guard).clone()
    }

    /// Lookup against a previously captured snapshot.
    /// Used by read path to query against a point-in-time tree.
    #[allow(dead_code)]
    pub(crate) fn lookup_snapshot(
        snapshot: &Option<Arc<Node>>,
        key: &IndexKey,
    ) -> Vec<RowPosition> {
        Self::lookup_in(snapshot.as_ref(), key)
    }

    // -- Private helpers --

    fn insert_node(node: Option<Arc<Node>>, key: IndexKey, pos: RowPosition) -> Arc<Node> {
        match node {
            None => Arc::new(Node {
                color: Color::Red,
                key,
                values: vec![pos],
                left: None,
                right: None,
            }),
            Some(n) => match key.cmp(&n.key) {
                std::cmp::Ordering::Less => {
                    let new_left = Self::insert_node(n.left.clone(), key, pos);
                    Self::balance(
                        n.color,
                        n.key.clone(),
                        n.values.clone(),
                        Some(new_left),
                        n.right.clone(),
                    )
                }
                std::cmp::Ordering::Greater => {
                    let new_right = Self::insert_node(n.right.clone(), key, pos);
                    Self::balance(
                        n.color,
                        n.key.clone(),
                        n.values.clone(),
                        n.left.clone(),
                        Some(new_right),
                    )
                }
                std::cmp::Ordering::Equal => {
                    // Same key: insert the position in row order, once. Ordered
                    // postings let an index read merge this list with the
                    // sidecars' and resume from a cursor holding nothing but
                    // the previous row (t_50c8bc7d). A rewrite of a row posts
                    // the same position again, which is not a second match.
                    let Err(at) = n.values.binary_search(&pos) else {
                        return n;
                    };
                    let mut new_values = n.values.clone();
                    new_values.insert(at, pos);
                    Arc::new(Node {
                        color: n.color,
                        key: n.key.clone(),
                        values: new_values,
                        left: n.left.clone(),
                        right: n.right.clone(),
                    })
                }
            },
        }
    }

    /// Okasaki's balance operation: fixes red-red violations after insert.
    /// Four symmetric cases, each producing a balanced red-black subtree.
    fn balance(
        color: Color,
        key: IndexKey,
        values: Vec<RowPosition>,
        left: Option<Arc<Node>>,
        right: Option<Arc<Node>>,
    ) -> Arc<Node> {
        // Only rebalance black nodes (red nodes propagate up)
        if color == Color::Black {
            // Case 1: left-left red-red
            if let Some(ref l) = left {
                if l.color == Color::Red {
                    if let Some(ref ll) = l.left {
                        if ll.color == Color::Red {
                            return Arc::new(Node {
                                color: Color::Red,
                                key: l.key.clone(),
                                values: l.values.clone(),
                                left: Some(Arc::new(Node {
                                    color: Color::Black,
                                    key: ll.key.clone(),
                                    values: ll.values.clone(),
                                    left: ll.left.clone(),
                                    right: ll.right.clone(),
                                })),
                                right: Some(Arc::new(Node {
                                    color: Color::Black,
                                    key: key.clone(),
                                    values: values.clone(),
                                    left: l.right.clone(),
                                    right: right.clone(),
                                })),
                            });
                        }
                    }
                    // Case 2: left-right red-red
                    if let Some(ref lr) = l.right {
                        if lr.color == Color::Red {
                            return Arc::new(Node {
                                color: Color::Red,
                                key: lr.key.clone(),
                                values: lr.values.clone(),
                                left: Some(Arc::new(Node {
                                    color: Color::Black,
                                    key: l.key.clone(),
                                    values: l.values.clone(),
                                    left: l.left.clone(),
                                    right: lr.left.clone(),
                                })),
                                right: Some(Arc::new(Node {
                                    color: Color::Black,
                                    key: key.clone(),
                                    values: values.clone(),
                                    left: lr.right.clone(),
                                    right: right.clone(),
                                })),
                            });
                        }
                    }
                }
            }
            // Case 3: right-left red-red
            if let Some(ref r) = right {
                if r.color == Color::Red {
                    if let Some(ref rl) = r.left {
                        if rl.color == Color::Red {
                            return Arc::new(Node {
                                color: Color::Red,
                                key: rl.key.clone(),
                                values: rl.values.clone(),
                                left: Some(Arc::new(Node {
                                    color: Color::Black,
                                    key: key.clone(),
                                    values: values.clone(),
                                    left: left.clone(),
                                    right: rl.left.clone(),
                                })),
                                right: Some(Arc::new(Node {
                                    color: Color::Black,
                                    key: r.key.clone(),
                                    values: r.values.clone(),
                                    left: rl.right.clone(),
                                    right: r.right.clone(),
                                })),
                            });
                        }
                    }
                    // Case 4: right-right red-red
                    if let Some(ref rr) = r.right {
                        if rr.color == Color::Red {
                            return Arc::new(Node {
                                color: Color::Red,
                                key: r.key.clone(),
                                values: r.values.clone(),
                                left: Some(Arc::new(Node {
                                    color: Color::Black,
                                    key: key.clone(),
                                    values: values.clone(),
                                    left: left.clone(),
                                    right: r.left.clone(),
                                })),
                                right: Some(Arc::new(Node {
                                    color: Color::Black,
                                    key: rr.key.clone(),
                                    values: rr.values.clone(),
                                    left: rr.left.clone(),
                                    right: rr.right.clone(),
                                })),
                            });
                        }
                    }
                }
            }
        }
        // No rebalance needed
        Arc::new(Node {
            color,
            key,
            values,
            left,
            right,
        })
    }

    fn lookup_in(node: Option<&Arc<Node>>, key: &IndexKey) -> Vec<RowPosition> {
        match node {
            None => vec![],
            Some(n) => match key.cmp(&n.key) {
                std::cmp::Ordering::Less => Self::lookup_in(n.left.as_ref(), key),
                std::cmp::Ordering::Greater => Self::lookup_in(n.right.as_ref(), key),
                std::cmp::Ordering::Equal => n.values.clone(),
            },
        }
    }

    fn visit_in(
        node: Option<&Arc<Node>>,
        key: &IndexKey,
        visitor: &mut dyn FnMut(RowPosition) -> ControlFlow<()>,
    ) {
        match node {
            None => {}
            Some(n) => match key.cmp(&n.key) {
                std::cmp::Ordering::Less => Self::visit_in(n.left.as_ref(), key, visitor),
                std::cmp::Ordering::Greater => Self::visit_in(n.right.as_ref(), key, visitor),
                std::cmp::Ordering::Equal => {
                    for position in &n.values {
                        if visitor(position.clone()).is_break() {
                            break;
                        }
                    }
                }
            },
        }
    }

    fn range_collect(
        node: Option<&Arc<Node>>,
        start: &IndexKey,
        end: &IndexKey,
        results: &mut Vec<RowPosition>,
    ) {
        if let Some(n) = node {
            if n.key > *start {
                Self::range_collect(n.left.as_ref(), start, end, results);
            }
            if n.key >= *start && n.key <= *end {
                results.extend(n.values.iter().cloned());
            }
            if n.key < *end {
                Self::range_collect(n.right.as_ref(), start, end, results);
            }
        }
    }

    fn collect_all(node: Option<&Arc<Node>>, entries: &mut Vec<(IndexKey, Vec<RowPosition>)>) {
        if let Some(n) = node {
            Self::collect_all(n.left.as_ref(), entries);
            entries.push((n.key.clone(), n.values.clone()));
            Self::collect_all(n.right.as_ref(), entries);
        }
    }
}

/// One index's postings, pinned to the tree as it stood at a chosen instant.
///
/// The tree is persistent, so holding its root is the whole snapshot: no
/// copying, and inserts that land afterwards build new nodes without
/// disturbing these. That matters for a flush, which must write the sidecar
/// for the rows it put in the SSTable and no others — pinning at the moment
/// the memtable is swapped out fixes which rows those are, whatever the write
/// path does next, and whenever the bytes are actually written.
pub struct IndexSnapshot {
    root: Option<Arc<Node>>,
}

impl IndexSnapshot {
    /// Visit every pinned posting in `(key, row)` order, copying nothing.
    pub fn visit_all(
        &self,
        visitor: &mut dyn FnMut(&IndexKey, &RowPosition) -> ControlFlow<()>,
    ) -> ControlFlow<()> {
        MemtableIndex::walk(self.root.as_ref(), visitor)
    }

    /// True when the snapshot holds no postings. Every node carries at least
    /// one (`insert` is the only way to make one), so an absent root and an
    /// empty index are the same thing.
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }
}

/// A pinned index writes its own sidecar, straight out of the tree.
///
/// The first error the writer reports stops the walk and is returned: a
/// half-written sidecar is never published (the writer renames into place
/// only on success), so a failure here leaves the previous file untouched.
impl crate::index::sidecar::SidecarSource for IndexSnapshot {
    fn visit(
        &self,
        visitor: &mut dyn FnMut(&IndexKey, &RowPosition) -> ferrosa_index::IndexResult<()>,
    ) -> ferrosa_index::IndexResult<()> {
        let mut failure = None;
        // The walk's own `Break` carries nothing: the error it stopped for is
        // in `failure`, which is what this returns.
        let _stopped = self.visit_all(&mut |key, position| match visitor(key, position) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => {
                failure = Some(error);
                ControlFlow::Break(())
            }
        });
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn is_empty(&self) -> bool {
        IndexSnapshot::is_empty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_index::{IndexKey, RowPosition};

    fn pos(pk: &[u8], ck: &[u8]) -> RowPosition {
        RowPosition {
            partition_key: pk.to_vec(),
            clustering_key: ck.to_vec(),
        }
    }

    #[test]
    fn insert_and_lookup_roundtrip() {
        let index = MemtableIndex::new();
        let key = IndexKey(b"alice".to_vec());
        let row = pos(b"pk1", b"ck1");

        index.insert(key.clone(), row.clone());

        let results = index.lookup(&key);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], row);
    }

    /// A key's postings are kept in row order — (partition key, clustering)
    /// — and each row once, however they were written. Ordered postings are
    /// what let an index read merge its sources and resume from a cursor
    /// without holding the result (t_50c8bc7d); a rewrite of the same row
    /// must not post it twice.
    #[test]
    fn a_keys_postings_are_kept_in_row_order_and_unique() {
        let index = MemtableIndex::new();
        let key = IndexKey(b"tenant-a".to_vec());
        for row in [
            pos(b"pk3", b""),
            pos(b"pk1", b"ck2"),
            pos(b"pk2", b"ck1"),
            pos(b"pk1", b"ck1"),
            pos(b"pk1", b"ck2"),
        ] {
            index.insert(key.clone(), row);
        }

        let mut visited = Vec::new();
        index.visit(&key, &mut |row| {
            visited.push(row);
            ControlFlow::Continue(())
        });
        assert_eq!(
            visited,
            vec![
                pos(b"pk1", b"ck1"),
                pos(b"pk1", b"ck2"),
                pos(b"pk2", b"ck1"),
                pos(b"pk3", b""),
            ]
        );
    }

    #[test]
    fn lookup_missing_key_returns_empty() {
        let index = MemtableIndex::new();
        let results = index.lookup(&IndexKey(b"ghost".to_vec()));
        assert!(results.is_empty());
    }

    #[test]
    fn multiple_rows_same_key() {
        let index = MemtableIndex::new();
        let key = IndexKey(b"shared".to_vec());

        index.insert(key.clone(), pos(b"pk1", b"ck1"));
        index.insert(key.clone(), pos(b"pk2", b"ck2"));

        let results = index.lookup(&key);
        assert_eq!(results.len(), 2);
        let pks: Vec<&[u8]> = results.iter().map(|r| r.partition_key.as_slice()).collect();
        assert!(pks.contains(&b"pk1".as_slice()));
        assert!(pks.contains(&b"pk2".as_slice()));
    }

    #[test]
    fn range_query_returns_correct_subset() {
        let index = MemtableIndex::new();
        index.insert(IndexKey(b"aaa".to_vec()), pos(b"pk1", b"ck1"));
        index.insert(IndexKey(b"bbb".to_vec()), pos(b"pk2", b"ck2"));
        index.insert(IndexKey(b"ccc".to_vec()), pos(b"pk3", b"ck3"));
        index.insert(IndexKey(b"ddd".to_vec()), pos(b"pk4", b"ck4"));

        let results = index.range(&IndexKey(b"bbb".to_vec()), &IndexKey(b"ccc".to_vec()));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk2");
        assert_eq!(results[1].partition_key, b"pk3");
    }

    #[test]
    fn empty_tree_range_returns_empty() {
        let index = MemtableIndex::new();
        let results = index.range(&IndexKey(b"a".to_vec()), &IndexKey(b"z".to_vec()));
        assert!(results.is_empty());
    }

    #[test]
    fn iter_returns_all_entries_sorted() {
        let index = MemtableIndex::new();
        index.insert(IndexKey(b"ccc".to_vec()), pos(b"pk3", b"ck3"));
        index.insert(IndexKey(b"aaa".to_vec()), pos(b"pk1", b"ck1"));
        index.insert(IndexKey(b"bbb".to_vec()), pos(b"pk2", b"ck2"));

        let entries: Vec<_> = index.iter().collect();
        assert_eq!(entries.len(), 3);
        // Keys must be in sorted order
        assert_eq!(entries[0].0, IndexKey(b"aaa".to_vec()));
        assert_eq!(entries[1].0, IndexKey(b"bbb".to_vec()));
        assert_eq!(entries[2].0, IndexKey(b"ccc".to_vec()));
    }

    #[test]
    fn insert_is_persistent_original_unchanged() {
        // Core FP invariant: insert returns a new tree, original is unchanged
        let index = MemtableIndex::new();
        index.insert(IndexKey(b"first".to_vec()), pos(b"pk1", b"ck1"));

        // Take a snapshot of the current root
        let snapshot = index.snapshot();

        // Insert another entry
        index.insert(IndexKey(b"second".to_vec()), pos(b"pk2", b"ck2"));

        // The live index has both entries
        assert_eq!(index.lookup(&IndexKey(b"first".to_vec())).len(), 1);
        assert_eq!(index.lookup(&IndexKey(b"second".to_vec())).len(), 1);

        // The snapshot only has the first entry
        assert_eq!(
            MemtableIndex::lookup_snapshot(&snapshot, &IndexKey(b"first".to_vec())).len(),
            1
        );
        assert_eq!(
            MemtableIndex::lookup_snapshot(&snapshot, &IndexKey(b"second".to_vec())).len(),
            0
        );
    }
}

#[cfg(test)]
mod concurrent_tests {
    use super::*;
    use ferrosa_index::{IndexKey, RowPosition};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn concurrent_read_write_10_threads() {
        let index = Arc::new(MemtableIndex::new());
        let num_threads = 10;
        let inserts_per_thread = 100;

        // Spawn writer threads
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let idx = Arc::clone(&index);
                thread::spawn(move || {
                    for i in 0..inserts_per_thread {
                        let key = IndexKey(format!("t{t}-k{i}").into_bytes());
                        let pos = RowPosition {
                            partition_key: format!("pk-{t}-{i}").into_bytes(),
                            clustering_key: vec![],
                        };
                        idx.insert(key, pos);
                    }
                })
            })
            .collect();

        // Concurrent reader thread
        let reader_idx = Arc::clone(&index);
        let reader = thread::spawn(move || {
            for _ in 0..500 {
                // Lookups must never panic or return corrupt data
                let _ = reader_idx.lookup(&IndexKey(b"t0-k0".to_vec()));
                let _ = reader_idx.range(&IndexKey(b"a".to_vec()), &IndexKey(b"z".to_vec()));
            }
        });

        for h in handles {
            h.join().unwrap();
        }
        reader.join().unwrap();

        // After all writers finish, every inserted key should be findable
        for t in 0..num_threads {
            for i in 0..inserts_per_thread {
                let key = IndexKey(format!("t{t}-k{i}").into_bytes());
                let results = index.lookup(&key);
                assert_eq!(results.len(), 1, "missing entry for t{t}-k{i}");
            }
        }
    }

    // ── Task 4.5: Concurrent stress test ─────────────────────────────────────

    #[test]
    fn concurrent_stress_10_writers_10_readers() {
        let index = Arc::new(MemtableIndex::new());
        let barrier = Arc::new(std::sync::Barrier::new(20));

        let writers: Vec<_> = (0..10)
            .map(|t| {
                let idx = Arc::clone(&index);
                let bar = Arc::clone(&barrier);
                thread::spawn(move || {
                    bar.wait();
                    for i in 0..1000 {
                        let key = IndexKey(format!("t{t}-k{i}").into_bytes());
                        let pos = RowPosition {
                            partition_key: format!("pk-{t}-{i}").into_bytes(),
                            clustering_key: vec![],
                        };
                        idx.insert(key, pos);
                    }
                })
            })
            .collect();

        let readers: Vec<_> = (0..10)
            .map(|_| {
                let idx = Arc::clone(&index);
                let bar = Arc::clone(&barrier);
                thread::spawn(move || {
                    bar.wait();
                    for _ in 0..2000 {
                        let _ = idx.lookup(&IndexKey(b"t0-k0".to_vec()));
                        let _ = idx.range(&IndexKey(b"a".to_vec()), &IndexKey(b"z".to_vec()));
                    }
                })
            })
            .collect();

        for h in writers {
            h.join().unwrap();
        }
        for h in readers {
            h.join().unwrap();
        }

        // Verify all entries present
        for t in 0..10 {
            for i in 0..1000 {
                let key = IndexKey(format!("t{t}-k{i}").into_bytes());
                assert_eq!(index.lookup(&key).len(), 1, "missing t{t}-k{i}");
            }
        }
    }
}
