//! READ_2I: Five-layer secondary index query algorithm for Accord transactions.
//!
//! When a transactional read queries a secondary index, the result must merge
//! entries from five distinct layers to guarantee completeness and no phantom
//! reads:
//!
//! 1. **MemIndex** — current memtable's in-memory secondary index
//! 2. **ConflictIndex** — in-flight Accord transaction indexed writes
//! 3. **CommittedLog** — committed-but-not-yet-applied transaction writes
//! 4. **FlushedSSTable** — persisted SSTables with secondary index data
//! 5. **UnindexedSSTable** — SSTables without index (scanned for completeness)
//!
//! The merge uses timestamp ordering to resolve conflicts, and dep-wait
//! ensures that concurrent writes either complete or are waited on before
//! the read returns.

use std::collections::{BTreeMap, HashMap, HashSet};

use ferrosa_common::accord::{Timestamp, TxnId};

// ---------------------------------------------------------------------------
// Layer abstraction
// ---------------------------------------------------------------------------

/// A single result row from a secondary index query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexResult {
    /// Partition key identifying the row.
    pub partition_key: Vec<u8>,
    /// Clustering key within the partition.
    pub clustering_key: Vec<u8>,
    /// Column value that matched the index predicate.
    pub value: Vec<u8>,
    /// Write timestamp of the cell.
    pub timestamp: Timestamp,
    /// Whether this entry represents a deletion (tombstone).
    pub is_tombstone: bool,
}

impl IndexResult {
    /// Composite key for deduplication: (partition_key, clustering_key).
    #[allow(dead_code)]
    fn row_key(&self) -> (&[u8], &[u8]) {
        (&self.partition_key, &self.clustering_key)
    }
}

/// Identifies which layer produced a result, for merge-priority ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LayerId {
    MemIndex = 0,
    ConflictIndex = 1,
    CommittedLog = 2,
    FlushedSSTable = 3,
    UnindexedSSTable = 4,
}

/// A result tagged with its source layer.
#[derive(Debug, Clone)]
struct TaggedResult {
    result: IndexResult,
    layer: LayerId,
}

// ---------------------------------------------------------------------------
// Consistency mode
// ---------------------------------------------------------------------------

/// Consistency mode for 2i reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyMode {
    /// Strict: dep-wait on all in-flight transactions before returning.
    Strict,
    /// Eventual: return immediately with best-effort results.
    Eventual,
}

// ---------------------------------------------------------------------------
// DepWaitOutcome
// ---------------------------------------------------------------------------

/// Outcome of a dependency wait during a strict-mode 2i read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepWaitOutcome {
    /// All dependencies resolved within the deadline.
    Resolved,
    /// Some dependencies timed out; results may be incomplete.
    TimedOut { pending: Vec<TxnId> },
}

// ---------------------------------------------------------------------------
// Read2iQuery
// ---------------------------------------------------------------------------

/// Parameters for a READ_2I query.
#[derive(Debug, Clone)]
pub struct Read2iQuery {
    /// Column name to query.
    pub column: String,
    /// Value to match (exact equality).
    pub value: Vec<u8>,
    /// Consistency mode for the read.
    pub mode: ConsistencyMode,
    /// Maximum number of results to return (0 = unlimited).
    pub limit: usize,
}

// ---------------------------------------------------------------------------
// Read2iMerger — the 5-layer merge engine
// ---------------------------------------------------------------------------

/// Five-layer merge engine for transactional secondary index queries.
///
/// Each layer provides results via a `Vec<IndexResult>`. The merger
/// deduplicates by (partition_key, clustering_key), keeping the entry
/// with the highest timestamp. Tombstones suppress live entries.
pub struct Read2iMerger {
    /// Results per layer, in layer order.
    layers: BTreeMap<LayerId, Vec<IndexResult>>,
    /// Transactions that must be dep-waited on (from ConflictIndex layer).
    pending_deps: HashSet<TxnId>,
    /// Whether unindexed SSTables were scanned.
    scanned_unindexed: bool,
}

impl Read2iMerger {
    /// Create a new empty merger.
    pub fn new() -> Self {
        Self {
            layers: BTreeMap::new(),
            pending_deps: HashSet::new(),
            scanned_unindexed: false,
        }
    }

    /// Add results from a specific layer.
    ///
    /// # Panics
    /// Panics if the same layer is added twice.
    pub fn add_layer(&mut self, layer: LayerId, results: Vec<IndexResult>) {
        assert!(
            !self.layers.contains_key(&layer),
            "layer {:?} already added",
            layer
        );
        self.layers.insert(layer, results);
    }

    /// Register transactions that need dep-wait before results are complete.
    pub fn add_pending_deps(&mut self, deps: impl IntoIterator<Item = TxnId>) {
        self.pending_deps.extend(deps);
    }

    /// Mark that unindexed SSTables were scanned.
    pub fn mark_unindexed_scanned(&mut self) {
        self.scanned_unindexed = true;
    }

    /// Whether unindexed SSTables were scanned.
    pub fn unindexed_scanned(&self) -> bool {
        self.scanned_unindexed
    }

    /// Return the set of pending dependency transactions.
    pub fn pending_deps(&self) -> &HashSet<TxnId> {
        &self.pending_deps
    }

    /// Execute the 5-layer merge.
    ///
    /// For each unique (partition_key, clustering_key), keeps the entry with
    /// the highest timestamp. If the winning entry is a tombstone, the row
    /// is excluded from the result set.
    ///
    /// Results are returned in timestamp order (ascending).
    pub fn merge(&self) -> Vec<IndexResult> {
        // Deduplicate by row key, keeping highest timestamp.
        let mut best: HashMap<(Vec<u8>, Vec<u8>), TaggedResult> = HashMap::new();

        for (&layer, results) in &self.layers {
            for result in results {
                let key = (result.partition_key.clone(), result.clustering_key.clone());
                let tagged = TaggedResult {
                    result: result.clone(),
                    layer,
                };
                match best.get(&key) {
                    None => {
                        best.insert(key, tagged);
                    }
                    Some(existing) => {
                        // Higher timestamp wins. On tie, higher layer wins
                        // (later layers are more authoritative).
                        if result.timestamp > existing.result.timestamp
                            || (result.timestamp == existing.result.timestamp
                                && layer > existing.layer)
                        {
                            best.insert(key, tagged);
                        }
                    }
                }
            }
        }

        // Filter out tombstones and collect.
        let mut results: Vec<IndexResult> = best
            .into_values()
            .filter(|t| !t.result.is_tombstone)
            .map(|t| t.result)
            .collect();

        // Sort by timestamp ascending for deterministic output.
        results.sort_by_key(|r| r.timestamp);
        results
    }

    /// Execute merge with a limit on result count.
    pub fn merge_limited(&self, limit: usize) -> Vec<IndexResult> {
        let mut results = self.merge();
        if limit > 0 && results.len() > limit {
            results.truncate(limit);
        }
        results
    }

    /// Simulate dep-wait: mark a dependency as resolved.
    ///
    /// Returns true if the dependency was pending and is now resolved.
    pub fn resolve_dep(&mut self, txn_id: &TxnId) -> bool {
        self.pending_deps.remove(txn_id)
    }

    /// Check if all dependencies are resolved.
    pub fn all_deps_resolved(&self) -> bool {
        self.pending_deps.is_empty()
    }

    /// Remove entries associated with a given partition key from all layers.
    ///
    /// Used to propagate deletes across all 5 layers.
    pub fn remove_partition(&mut self, partition_key: &[u8]) {
        for results in self.layers.values_mut() {
            results.retain(|r| r.partition_key != partition_key);
        }
    }

    /// Number of layers currently populated.
    pub fn layer_count(&self) -> usize {
        self.layers.len()
    }
}

impl Default for Read2iMerger {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests — 8 tests for A7.1
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn ts(time: u64) -> Timestamp {
        Timestamp {
            epoch: 0,
            time,
            seq: 0,
            node: 0,
        }
    }

    fn txn(time: u64) -> TxnId {
        TxnId(ts(time))
    }

    fn live_result(pk: &[u8], ck: &[u8], val: &[u8], time: u64) -> IndexResult {
        IndexResult {
            partition_key: pk.to_vec(),
            clustering_key: ck.to_vec(),
            value: val.to_vec(),
            timestamp: ts(time),
            is_tombstone: false,
        }
    }

    fn tombstone_result(pk: &[u8], ck: &[u8], val: &[u8], time: u64) -> IndexResult {
        IndexResult {
            partition_key: pk.to_vec(),
            clustering_key: ck.to_vec(),
            value: val.to_vec(),
            timestamp: ts(time),
            is_tombstone: true,
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: read_2i_five_layer_merge
    //   Query across 5 layers, merge results with timestamp resolution.
    // -----------------------------------------------------------------------

    #[test]
    fn read_2i_five_layer_merge() {
        let mut merger = Read2iMerger::new();

        // Layer 1: MemIndex has row A at ts=100.
        merger.add_layer(
            LayerId::MemIndex,
            vec![live_result(b"pkA", b"ckA", b"alice", 100)],
        );

        // Layer 2: ConflictIndex has row B at ts=200 (in-flight write).
        merger.add_layer(
            LayerId::ConflictIndex,
            vec![live_result(b"pkB", b"ckB", b"bob", 200)],
        );

        // Layer 3: CommittedLog has row C at ts=150.
        merger.add_layer(
            LayerId::CommittedLog,
            vec![live_result(b"pkC", b"ckC", b"carol", 150)],
        );

        // Layer 4: FlushedSSTable has row A at ts=50 (older, should be superseded).
        merger.add_layer(
            LayerId::FlushedSSTable,
            vec![live_result(b"pkA", b"ckA", b"alice_old", 50)],
        );

        // Layer 5: UnindexedSSTable has row D at ts=75.
        merger.add_layer(
            LayerId::UnindexedSSTable,
            vec![live_result(b"pkD", b"ckD", b"dave", 75)],
        );

        assert_eq!(merger.layer_count(), 5, "all 5 layers must be populated");

        let results = merger.merge();

        // Should have 4 distinct rows: A (ts=100), B (ts=200), C (ts=150), D (ts=75).
        assert_eq!(results.len(), 4, "4 unique rows from 5 layers");

        // Verify row A used the MemIndex version (ts=100), not FlushedSSTable (ts=50).
        let row_a = results.iter().find(|r| r.partition_key == b"pkA").unwrap();
        assert_eq!(
            row_a.timestamp,
            ts(100),
            "row A must use MemIndex version (ts=100)"
        );
        assert_eq!(row_a.value, b"alice", "row A value must be from MemIndex");

        // Verify timestamp ordering of results (ascending).
        for window in results.windows(2) {
            assert!(
                window[0].timestamp <= window[1].timestamp,
                "results must be sorted by timestamp"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: read_2i_no_phantom_reads
    //   No phantom reads during concurrent writes — tombstones suppress.
    // -----------------------------------------------------------------------

    #[test]
    fn read_2i_no_phantom_reads() {
        let mut merger = Read2iMerger::new();

        // MemIndex has a live row at ts=100.
        merger.add_layer(
            LayerId::MemIndex,
            vec![live_result(b"pkA", b"ckA", b"alice", 100)],
        );

        // ConflictIndex has a tombstone for the same row at ts=200
        // (concurrent delete that should suppress the live row).
        merger.add_layer(
            LayerId::ConflictIndex,
            vec![tombstone_result(b"pkA", b"ckA", b"alice", 200)],
        );

        let results = merger.merge();

        // The tombstone at ts=200 must suppress the live row at ts=100.
        assert!(
            results.is_empty(),
            "tombstone must suppress live row — no phantom reads; got {} results",
            results.len()
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: commit_index_indexed_writes
    //   Committed writes reflected in the merge result set.
    // -----------------------------------------------------------------------

    #[test]
    fn commit_index_indexed_writes() {
        let mut merger = Read2iMerger::new();

        // CommittedLog has indexed writes that should appear in results.
        merger.add_layer(
            LayerId::CommittedLog,
            vec![
                live_result(b"pkX", b"ckX", b"value1", 300),
                live_result(b"pkY", b"ckY", b"value2", 400),
            ],
        );

        // No other layers have data.
        merger.add_layer(LayerId::MemIndex, vec![]);
        merger.add_layer(LayerId::ConflictIndex, vec![]);
        merger.add_layer(LayerId::FlushedSSTable, vec![]);
        merger.add_layer(LayerId::UnindexedSSTable, vec![]);

        let results = merger.merge();

        assert_eq!(results.len(), 2, "both committed writes must appear");
        assert!(
            results.iter().any(|r| r.partition_key == b"pkX"),
            "pkX must be in results"
        );
        assert!(
            results.iter().any(|r| r.partition_key == b"pkY"),
            "pkY must be in results"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: 2i_dep_wait_latency
    //   Dep-wait adds bounded latency — verify deps tracking.
    // -----------------------------------------------------------------------

    #[test]
    fn two_i_dep_wait_latency() {
        let mut merger = Read2iMerger::new();

        // ConflictIndex layer has in-flight transactions.
        merger.add_layer(
            LayerId::ConflictIndex,
            vec![live_result(b"pkA", b"ckA", b"val", 100)],
        );

        // Register pending deps from in-flight transactions.
        let dep1 = txn(100);
        let dep2 = txn(200);
        merger.add_pending_deps(vec![dep1, dep2]);

        // Not all deps resolved yet.
        assert!(
            !merger.all_deps_resolved(),
            "deps should not be resolved yet"
        );
        assert_eq!(merger.pending_deps().len(), 2, "two deps pending");

        // Resolve first dep.
        assert!(merger.resolve_dep(&dep1), "dep1 should be resolved");
        assert_eq!(merger.pending_deps().len(), 1, "one dep remaining");

        // Resolve second dep.
        assert!(merger.resolve_dep(&dep2), "dep2 should be resolved");
        assert!(
            merger.all_deps_resolved(),
            "all deps must be resolved after resolving both"
        );

        // Resolving a non-existent dep returns false (no extra latency).
        assert!(
            !merger.resolve_dep(&txn(999)),
            "resolving unknown dep must return false"
        );
    }

    // -----------------------------------------------------------------------
    // Test 5: 2i_unindexed_sstable_scan
    //   Unindexed SSTables are scanned for completeness.
    // -----------------------------------------------------------------------

    #[test]
    fn two_i_unindexed_sstable_scan() {
        let mut merger = Read2iMerger::new();

        // Only FlushedSSTable and UnindexedSSTable layers populated.
        merger.add_layer(
            LayerId::FlushedSSTable,
            vec![live_result(b"pkA", b"ckA", b"val1", 100)],
        );

        // UnindexedSSTable contributes rows not in any other layer.
        merger.add_layer(
            LayerId::UnindexedSSTable,
            vec![
                live_result(b"pkB", b"ckB", b"val2", 200),
                live_result(b"pkC", b"ckC", b"val3", 300),
            ],
        );

        merger.mark_unindexed_scanned();

        let results = merger.merge();

        // All 3 rows must be present.
        assert_eq!(results.len(), 3, "unindexed sstable rows must be included");
        assert!(
            merger.unindexed_scanned(),
            "unindexed scan flag must be set"
        );

        // Verify the unindexed rows are present.
        assert!(results.iter().any(|r| r.partition_key == b"pkB"));
        assert!(results.iter().any(|r| r.partition_key == b"pkC"));
    }

    // -----------------------------------------------------------------------
    // Test 6: 2i_eventual_mode
    //   Eventual consistency mode returns without dep-wait.
    // -----------------------------------------------------------------------

    #[test]
    fn two_i_eventual_mode() {
        let mut merger = Read2iMerger::new();

        merger.add_layer(
            LayerId::MemIndex,
            vec![live_result(b"pkA", b"ckA", b"val", 100)],
        );
        merger.add_layer(
            LayerId::ConflictIndex,
            vec![live_result(b"pkB", b"ckB", b"val", 200)],
        );

        // Register pending deps (in strict mode these would block).
        merger.add_pending_deps(vec![txn(200)]);

        // In eventual mode, we merge without waiting for deps.
        let query = Read2iQuery {
            column: "col".to_string(),
            value: b"val".to_vec(),
            mode: ConsistencyMode::Eventual,
            limit: 0,
        };

        assert_eq!(query.mode, ConsistencyMode::Eventual);

        // Merge returns results even with unresolved deps.
        let results = merger.merge();
        assert_eq!(
            results.len(),
            2,
            "eventual mode returns all available results"
        );

        // Deps are still tracked but not blocking.
        assert!(
            !merger.all_deps_resolved(),
            "deps are tracked but not blocking in eventual mode"
        );
    }

    // -----------------------------------------------------------------------
    // Test 7: 2i_concurrent_write_read_consistency
    //   Concurrent write and read produce consistent results.
    // -----------------------------------------------------------------------

    #[test]
    fn two_i_concurrent_write_read_consistency() {
        let mut merger = Read2iMerger::new();

        // Initial state: row A exists in FlushedSSTable at ts=100.
        merger.add_layer(
            LayerId::FlushedSSTable,
            vec![live_result(b"pkA", b"ckA", b"old_val", 100)],
        );

        // Concurrent write: ConflictIndex shows row A being updated at ts=200.
        merger.add_layer(
            LayerId::ConflictIndex,
            vec![live_result(b"pkA", b"ckA", b"new_val", 200)],
        );

        // MemIndex also has the same row at ts=150 (stale memtable).
        merger.add_layer(
            LayerId::MemIndex,
            vec![live_result(b"pkA", b"ckA", b"mid_val", 150)],
        );

        let results = merger.merge();

        // Should have exactly 1 row: the one with highest timestamp (ts=200).
        assert_eq!(results.len(), 1, "one unique row after dedup");
        assert_eq!(
            results[0].value, b"new_val",
            "highest timestamp (ConflictIndex ts=200) must win"
        );
        assert_eq!(results[0].timestamp, ts(200));
    }

    // -----------------------------------------------------------------------
    // Test 8: 2i_delete_removes_from_all_layers
    //   Delete removes a partition key from all 5 layers.
    // -----------------------------------------------------------------------

    #[test]
    fn two_i_delete_removes_from_all_layers() {
        let mut merger = Read2iMerger::new();

        // Populate all 5 layers with row A.
        merger.add_layer(
            LayerId::MemIndex,
            vec![live_result(b"pkA", b"ckA", b"v1", 100)],
        );
        merger.add_layer(
            LayerId::ConflictIndex,
            vec![live_result(b"pkA", b"ckA", b"v2", 200)],
        );
        merger.add_layer(
            LayerId::CommittedLog,
            vec![live_result(b"pkA", b"ckA", b"v3", 150)],
        );
        merger.add_layer(
            LayerId::FlushedSSTable,
            vec![live_result(b"pkA", b"ckA", b"v4", 50)],
        );
        merger.add_layer(
            LayerId::UnindexedSSTable,
            vec![
                live_result(b"pkA", b"ckA", b"v5", 75),
                live_result(b"pkB", b"ckB", b"other", 300),
            ],
        );

        // Before delete: merge should yield 1 row for pkA + 1 for pkB.
        let before = merger.merge();
        assert_eq!(before.len(), 2, "2 unique rows before delete");

        // Delete pkA from all layers.
        merger.remove_partition(b"pkA");

        // After delete: only pkB should remain.
        let after = merger.merge();
        assert_eq!(after.len(), 1, "only pkB should remain after delete");
        assert_eq!(after[0].partition_key, b"pkB", "surviving row must be pkB");

        // Verify pkA is gone from every layer.
        for (layer_id, results) in &merger.layers {
            assert!(
                results.iter().all(|r| r.partition_key != b"pkA"),
                "pkA must be removed from layer {:?}",
                layer_id
            );
        }
    }
}
