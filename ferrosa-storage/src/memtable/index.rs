//! Persistent (functional) red-black tree for memtable-level secondary indexing.
//!
//! Insert produces a new root via O(log n) path-copying — the original tree
//! is never mutated. The current root is stored behind `ArcSwap<Option<Node>>`
//! so readers load a snapshot atomically (no locks, no contention).
//!
//! Based on Okasaki's persistent red-black tree (Purely Functional Data
//! Structures, 1998), adapted for Rust with `Arc` for structural sharing.

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

    /// Range query: returns all RowPositions for keys in [start, end] inclusive.
    pub fn range(&self, start: &IndexKey, end: &IndexKey) -> Vec<RowPosition> {
        let guard = self.root.load();
        let mut results = Vec::new();
        Self::range_collect((**guard).as_ref(), start, end, &mut results);
        results
    }

    /// In-order iterator over all (key, positions) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (IndexKey, Vec<RowPosition>)> {
        let guard = self.root.load();
        let mut entries = Vec::new();
        Self::collect_all((**guard).as_ref(), &mut entries);
        entries.into_iter()
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
                    // Same key: append the new position to the values list
                    let mut new_values = n.values.clone();
                    new_values.push(pos);
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
}
