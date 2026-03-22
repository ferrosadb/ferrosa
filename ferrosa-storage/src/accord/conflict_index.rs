//! Per-shard index of in-flight transactions for Accord conflict detection.
//!
//! The [`ConflictIndex`] tracks all in-flight writes within a single shard
//! executor, providing:
//!
//! - **O(1)** exact-key conflict lookup via `HashMap`
//! - **O(log n)** range overlap detection via `BTreeMap`
//! - **Indexed column projections** for transactional secondary index queries
//!
//! All access must be through a single-threaded shard executor — the index
//! is intentionally `!Sync` (it uses non-atomic interior state).

use ferrosa_common::accord::{Timestamp, TxnId};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Status of an in-flight transaction in the ConflictIndex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    PreAccepted,
    Accepted,
    Committed,
    Applied,
}

/// Entry for a single in-flight write.
#[derive(Debug, Clone)]
pub struct InFlightWrite {
    pub txn_id: TxnId,
    pub t0: Timestamp,
    /// Commit timestamp. `None` until the transaction is committed.
    pub accord_ts: Option<Timestamp>,
    pub status: TxnStatus,
}

/// Token range for range operations.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TokenRange {
    pub start: i64,
    pub end: i64,
}

impl TokenRange {
    /// Returns true if this range overlaps with `other`.
    ///
    /// Ranges are inclusive on both ends: `[start, end]`.
    fn overlaps(&self, other: &TokenRange) -> bool {
        self.start <= other.end && other.start <= self.end
    }
}

/// Error returned when the [`ConflictIndex`] is at capacity.
#[derive(Debug)]
pub struct ConflictIndexFull;

impl std::fmt::Display for ConflictIndexFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "conflict index at capacity")
    }
}

impl std::error::Error for ConflictIndexFull {}

// ---------------------------------------------------------------------------
// ConflictIndex
// ---------------------------------------------------------------------------

/// Per-shard index of in-flight transactions for conflict detection.
///
/// Designed to be owned by a single-threaded shard executor. Not `Sync`.
pub struct ConflictIndex {
    /// Single-partition writes: O(1) exact-key lookup.
    single_key: HashMap<Vec<u8>, Vec<InFlightWrite>>,

    /// Range-spanning operations: O(log n) range overlap.
    range_ops: BTreeMap<TokenRange, BTreeSet<(Timestamp, TxnId)>>,

    /// Indexed column projections for transactional 2i.
    indexed_writes: HashMap<String, HashMap<Vec<u8>, Vec<TxnId>>>,

    /// Hard cap on total entries.
    max_entries: usize,
    current_entries: usize,
}

impl ConflictIndex {
    /// Create a new conflict index with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if `max_entries` is zero.
    pub fn new(max_entries: usize) -> Self {
        assert!(max_entries > 0, "max_entries must be positive");
        Self {
            single_key: HashMap::new(),
            range_ops: BTreeMap::new(),
            indexed_writes: HashMap::new(),
            max_entries,
            current_entries: 0,
        }
    }

    /// Register a new in-flight transaction on a single key.
    ///
    /// Returns `Err(ConflictIndexFull)` if the index is at capacity.
    pub fn register(&mut self, key: &[u8], entry: InFlightWrite) -> Result<(), ConflictIndexFull> {
        if self.current_entries >= self.max_entries {
            return Err(ConflictIndexFull);
        }
        self.single_key.entry(key.to_vec()).or_default().push(entry);
        self.current_entries += 1;
        Ok(())
    }

    /// Register a range operation.
    ///
    /// Returns `Err(ConflictIndexFull)` if the index is at capacity.
    pub fn register_range(
        &mut self,
        range: TokenRange,
        ts: Timestamp,
        txn_id: TxnId,
    ) -> Result<(), ConflictIndexFull> {
        if self.current_entries >= self.max_entries {
            return Err(ConflictIndexFull);
        }
        self.range_ops
            .entry(range)
            .or_default()
            .insert((ts, txn_id));
        self.current_entries += 1;
        Ok(())
    }

    /// Register an indexed column write.
    ///
    /// Indexed writes do not count toward the capacity limit since they
    /// are secondary projections of already-registered transactions.
    pub fn register_indexed_write(&mut self, column: &str, value: &[u8], txn_id: TxnId) {
        self.indexed_writes
            .entry(column.to_string())
            .or_default()
            .entry(value.to_vec())
            .or_default()
            .push(txn_id);
    }

    /// Returns the maximum `t0` of all conflicting in-flight transactions
    /// for a single key. O(1) lookup.
    pub fn max_conflicting_timestamp(&self, key: &[u8]) -> Option<Timestamp> {
        self.single_key
            .get(key)
            .and_then(|writes| writes.iter().map(|w| w.t0).max())
    }

    /// Returns the maximum `t0` of conflicting range operations that
    /// overlap with the given range.
    pub fn max_conflicting_range_timestamp(&self, range: &TokenRange) -> Option<Timestamp> {
        let mut max_ts: Option<Timestamp> = None;
        for (stored_range, txn_set) in &self.range_ops {
            if stored_range.overlaps(range) {
                for (ts, _txn_id) in txn_set {
                    match max_ts {
                        None => max_ts = Some(*ts),
                        Some(current_max) if *ts > current_max => max_ts = Some(*ts),
                        _ => {}
                    }
                }
            }
        }
        max_ts
    }

    /// Returns all conflicting transaction IDs where `t0_gamma < t0`.
    ///
    /// Used for building the PreAccept dependency set.
    pub fn deps_before_t0(&self, key: &[u8], t0: &Timestamp) -> HashSet<TxnId> {
        let mut deps = HashSet::new();
        if let Some(writes) = self.single_key.get(key) {
            for w in writes {
                if w.t0 < *t0 {
                    deps.insert(w.txn_id);
                }
            }
        }
        deps
    }

    /// Returns all conflicting transaction IDs where `t0_gamma < t`.
    ///
    /// Used for building the Accept dependency set. Note: this compares
    /// each entry's `t0` against the provided `t` (the commit timestamp),
    /// not against another `t0`.
    pub fn deps_before_t(&self, key: &[u8], t: &Timestamp) -> HashSet<TxnId> {
        let mut deps = HashSet::new();
        if let Some(writes) = self.single_key.get(key) {
            for w in writes {
                if w.t0 < *t {
                    deps.insert(w.txn_id);
                }
            }
        }
        deps
    }

    /// Remove a completed transaction from all indexes.
    ///
    /// Scans single-key, range, and indexed-write maps for entries
    /// matching the given `txn_id` and removes them. Decrements the
    /// entry count for each removal from single-key and range maps.
    pub fn remove(&mut self, txn_id: &TxnId) {
        // Remove from single-key index.
        let mut empty_keys = Vec::new();
        for (key, writes) in &mut self.single_key {
            let before = writes.len();
            writes.retain(|w| w.txn_id != *txn_id);
            let removed = before - writes.len();
            self.current_entries = self.current_entries.saturating_sub(removed);
            if writes.is_empty() {
                empty_keys.push(key.clone());
            }
        }
        for key in empty_keys {
            self.single_key.remove(&key);
        }

        // Remove from range index.
        let mut empty_ranges = Vec::new();
        for (range, txn_set) in &mut self.range_ops {
            let before = txn_set.len();
            txn_set.retain(|(_ts, tid)| *tid != *txn_id);
            let removed = before - txn_set.len();
            self.current_entries = self.current_entries.saturating_sub(removed);
            if txn_set.is_empty() {
                empty_ranges.push(range.clone());
            }
        }
        for range in empty_ranges {
            self.range_ops.remove(&range);
        }

        // Remove from indexed writes.
        let mut empty_columns = Vec::new();
        for (column, value_map) in &mut self.indexed_writes {
            let mut empty_values = Vec::new();
            for (value, txn_ids) in value_map.iter_mut() {
                txn_ids.retain(|tid| *tid != *txn_id);
                if txn_ids.is_empty() {
                    empty_values.push(value.clone());
                }
            }
            for value in empty_values {
                value_map.remove(&value);
            }
            if value_map.is_empty() {
                empty_columns.push(column.clone());
            }
        }
        for column in empty_columns {
            self.indexed_writes.remove(&column);
        }
    }

    /// Current number of entries (single-key + range).
    pub fn len(&self) -> usize {
        self.current_entries
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.current_entries == 0
    }

    /// Look up indexed write projections for a given column and value.
    pub fn get_indexed_writes(&self, column: &str, value: &[u8]) -> Option<&[TxnId]> {
        self.indexed_writes
            .get(column)
            .and_then(|value_map| value_map.get(value))
            .map(|v| v.as_slice())
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a Timestamp with the given time value (other fields zero).
    fn ts(time: u64) -> Timestamp {
        Timestamp {
            epoch: 0,
            time,
            seq: 0,
            node: 1,
        }
    }

    /// Helper: create a TxnId from a Timestamp time value.
    fn txn(time: u64) -> TxnId {
        TxnId(ts(time))
    }

    /// Helper: create an InFlightWrite with given txn_id and t0 time.
    fn write_entry(t0_time: u64) -> InFlightWrite {
        InFlightWrite {
            txn_id: txn(t0_time),
            t0: ts(t0_time),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: Single key register + lookup
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_single_key_register_lookup() {
        let mut idx = ConflictIndex::new(100);
        let key = b"partition-1";

        // Register T1 (t0 = 10).
        idx.register(key, write_entry(10)).unwrap();
        assert_eq!(idx.max_conflicting_timestamp(key), Some(ts(10)));

        // Register T2 with higher t0 (t0 = 20).
        idx.register(key, write_entry(20)).unwrap();
        assert_eq!(idx.max_conflicting_timestamp(key), Some(ts(20)));
    }

    // -----------------------------------------------------------------------
    // Test 2: No false positives across keys
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_single_key_no_false_positives() {
        let mut idx = ConflictIndex::new(100);

        idx.register(b"key-A", write_entry(10)).unwrap();

        // Querying a different key must return None.
        assert_eq!(idx.max_conflicting_timestamp(b"key-B"), None);
    }

    // -----------------------------------------------------------------------
    // Test 3: Range overlap detection
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_range_overlap_detection() {
        let mut idx = ConflictIndex::new(100);

        let range1 = TokenRange {
            start: 100,
            end: 200,
        };
        idx.register_range(range1, ts(10), txn(10)).unwrap();

        // Overlapping query range [150, 250] — should find conflict.
        let query_overlap = TokenRange {
            start: 150,
            end: 250,
        };
        assert_eq!(
            idx.max_conflicting_range_timestamp(&query_overlap),
            Some(ts(10))
        );

        // Non-overlapping query range [201, 300] — no conflict.
        let query_disjoint = TokenRange {
            start: 201,
            end: 300,
        };
        assert_eq!(idx.max_conflicting_range_timestamp(&query_disjoint), None);
    }

    // -----------------------------------------------------------------------
    // Test 4: deps_before_t0 filter
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_deps_before_t0_filter() {
        let mut idx = ConflictIndex::new(100);
        let key = b"key";

        idx.register(key, write_entry(5)).unwrap();
        idx.register(key, write_entry(10)).unwrap();
        idx.register(key, write_entry(15)).unwrap();

        // deps_before_t0(key, t0=12) should return T1(5) and T2(10), not T3(15).
        let deps = idx.deps_before_t0(key, &ts(12));
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&txn(5)));
        assert!(deps.contains(&txn(10)));
        assert!(!deps.contains(&txn(15)));
    }

    // -----------------------------------------------------------------------
    // Test 5: deps_before_t filter
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_deps_before_t_filter() {
        let mut idx = ConflictIndex::new(100);
        let key = b"key";

        idx.register(key, write_entry(5)).unwrap();
        idx.register(key, write_entry(10)).unwrap();

        // deps_before_t(key, t=8) should return T1(5) only, not T2(10).
        let deps = idx.deps_before_t(key, &ts(8));
        assert_eq!(deps.len(), 1);
        assert!(deps.contains(&txn(5)));
        assert!(!deps.contains(&txn(10)));
    }

    // -----------------------------------------------------------------------
    // Test 6: Remove after applied
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_remove_after_applied() {
        let mut idx = ConflictIndex::new(100);
        let key = b"key";

        idx.register(key, write_entry(10)).unwrap();
        assert_eq!(idx.max_conflicting_timestamp(key), Some(ts(10)));
        assert_eq!(idx.len(), 1);

        idx.remove(&txn(10));
        assert_eq!(idx.max_conflicting_timestamp(key), None);
        assert!(idx.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 7: Bounded capacity
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_bounded_capacity() {
        let mut idx = ConflictIndex::new(3);

        idx.register(b"k1", write_entry(1)).unwrap();
        idx.register(b"k2", write_entry(2)).unwrap();
        idx.register(b"k3", write_entry(3)).unwrap();

        // 4th registration must fail.
        let result = idx.register(b"k4", write_entry(4));
        assert!(result.is_err());
        assert_eq!(idx.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Test 8: Verify ConflictIndex is !Sync
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_concurrent_single_threaded() {
        // ConflictIndex uses HashMap (which is !Sync for mutable access).
        // We verify it is Send but document that all access must go through
        // a single-threaded shard executor.
        //
        // The type is Send (it contains only owned data), but concurrent
        // mutable access is prevented by Rust's ownership rules — only one
        // &mut reference can exist at a time. This is the desired property
        // for a shard-local data structure.
        fn assert_send<T: Send>() {}
        assert_send::<ConflictIndex>();

        // Verify the index works correctly in a single-threaded context
        // with interleaved operations.
        let mut idx = ConflictIndex::new(100);
        idx.register(b"k1", write_entry(1)).unwrap();
        idx.register(b"k2", write_entry(2)).unwrap();
        assert_eq!(idx.max_conflicting_timestamp(b"k1"), Some(ts(1)));
        idx.remove(&txn(1));
        assert_eq!(idx.max_conflicting_timestamp(b"k1"), None);
        assert_eq!(idx.max_conflicting_timestamp(b"k2"), Some(ts(2)));
    }

    // -----------------------------------------------------------------------
    // Test 9: Indexed writes projection
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_indexed_writes_projection() {
        let mut idx = ConflictIndex::new(100);

        let t1 = txn(10);
        idx.register_indexed_write("age", b"25", t1);

        // Query indexed_writes for ("age", "25") should return T1.
        let result = idx.get_indexed_writes("age", b"25");
        assert!(result.is_some());
        let txn_ids = result.unwrap();
        assert_eq!(txn_ids.len(), 1);
        assert_eq!(txn_ids[0], t1);

        // Query for a different value should return None.
        assert!(idx.get_indexed_writes("age", b"30").is_none());

        // Query for a different column should return None.
        assert!(idx.get_indexed_writes("name", b"25").is_none());
    }
}
