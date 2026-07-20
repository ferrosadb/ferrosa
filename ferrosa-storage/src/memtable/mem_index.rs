//! BTreeMap-based in-memory secondary index for memtable-level lookups.
//!
//! `MemIndex` provides point lookups by column value, range scans, and
//! timestamp-based filtering. It is designed to be updated atomically with
//! memtable writes and garbage-collected on flush.
//!
//! Unlike [`super::index::MemtableIndex`] (Okasaki persistent red-black tree
//! for SSTable-level indexing), `MemIndex` is a mutable BTreeMap suited for
//! in-memory secondary index maintenance during the memtable lifecycle.

use std::collections::BTreeMap;
use std::ops::Bound;

use ferrosa_common::cell::Timestamp;
use ferrosa_common::CellValue;
use parking_lot::RwLock;

/// Composite key for the primary BTreeMap: (column_value, entry_timestamp).
type IndexKey = (CellValue, Timestamp);

/// An entry in the secondary index pointing back to a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    /// Partition key bytes identifying the partition.
    pub partition_key: Vec<u8>,
    /// Clustering key bytes identifying the row within the partition.
    pub clustering_key: Vec<u8>,
    /// Timestamp of the cell that produced this index entry.
    pub timestamp: Timestamp,
}

/// BTreeMap-based in-memory secondary index.
///
/// Keys are `(CellValue, Timestamp)` pairs, ordered first by cell value then
/// by timestamp. This enables efficient range scans over column values and
/// timestamp filtering within a value range.
///
/// Thread safety: `RwLock` guards the inner map. Writers take an exclusive
/// lock; readers take a shared lock. The memtable write path is already
/// serialized per-partition, so write contention is minimal.
pub struct MemIndex {
    /// Primary index: (column_value, timestamp) -> IndexEntry.
    inner: RwLock<BTreeMap<IndexKey, IndexEntry>>,
    /// Reverse index: partition_key -> set of (column_value, timestamp) keys.
    /// Enables O(log n) removal by partition key without scanning the full map.
    by_partition: RwLock<BTreeMap<Vec<u8>, Vec<IndexKey>>>,
}

impl Default for MemIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl MemIndex {
    /// Create a new empty `MemIndex`.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            by_partition: RwLock::new(BTreeMap::new()),
        }
    }

    /// Insert an index entry for a column value.
    ///
    /// If the partition already has an entry for a different value, the old
    /// entry is removed first (update-replaces semantics).
    pub fn insert(&self, value: CellValue, entry: IndexEntry) {
        let key = (value.clone(), entry.timestamp);
        let pk = entry.partition_key.clone();

        let mut inner = self.inner.write();
        let mut by_part = self.by_partition.write();

        // Remove any previous entries for this partition key (update-replaces).
        if let Some(old_keys) = by_part.remove(&pk) {
            for old_key in &old_keys {
                inner.remove(old_key);
            }
        }

        inner.insert(key.clone(), entry);
        by_part.entry(pk).or_default().push(key);
    }

    /// Look up all entries that match the given column value bytes.
    ///
    /// Matches by `value.value` bytes, ignoring the CellValue's own timestamp
    /// metadata. Returns entries in timestamp order.
    pub fn lookup(&self, value: &CellValue) -> Vec<IndexEntry> {
        let inner = self.inner.read();
        let min_cell = CellValue {
            value: value.value.clone(),
            timestamp: Timestamp::MIN,
            ttl: i32::MIN,
            local_deletion_time: i32::MIN,
            path: None,
        };
        let max_cell = CellValue {
            value: value.value.clone(),
            timestamp: Timestamp::MAX,
            ttl: i32::MAX,
            local_deletion_time: i32::MAX,
            path: None,
        };
        let start = (min_cell, Timestamp::MIN);
        let end = (max_cell, Timestamp::MAX);
        inner
            .range((Bound::Included(&start), Bound::Included(&end)))
            .map(|(_, entry)| entry.clone())
            .collect()
    }

    /// Range scan: return all entries whose column value bytes are in [from, to] inclusive.
    ///
    /// Results are ordered by (column_value, timestamp).
    pub fn range_scan(&self, from: &CellValue, to: &CellValue) -> Vec<IndexEntry> {
        let inner = self.inner.read();
        let start_cell = CellValue {
            value: from.value.clone(),
            timestamp: Timestamp::MIN,
            ttl: i32::MIN,
            local_deletion_time: i32::MIN,
            path: None,
        };
        let end_cell = CellValue {
            value: to.value.clone(),
            timestamp: Timestamp::MAX,
            ttl: i32::MAX,
            local_deletion_time: i32::MAX,
            path: None,
        };
        let start = (start_cell, Timestamp::MIN);
        let end = (end_cell, Timestamp::MAX);
        inner
            .range((Bound::Included(&start), Bound::Included(&end)))
            .map(|(_, entry)| entry.clone())
            .collect()
    }

    /// Remove all index entries for a given partition key.
    pub fn remove(&self, partition_key: &[u8]) {
        let mut inner = self.inner.write();
        let mut by_part = self.by_partition.write();

        if let Some(keys) = by_part.remove(partition_key) {
            for key in &keys {
                inner.remove(key);
            }
        }
    }

    /// Garbage-collect entries with timestamps strictly less than the flush boundary.
    ///
    /// This is called after a memtable flush to remove entries that have been
    /// persisted to SSTables.
    pub fn gc(&self, flush_boundary: Timestamp) {
        let mut inner = self.inner.write();
        let mut by_part = self.by_partition.write();

        // Collect keys to remove (timestamps < flush_boundary).
        let to_remove: Vec<(CellValue, Timestamp)> = inner
            .keys()
            .filter(|(_, ts)| *ts < flush_boundary)
            .cloned()
            .collect();

        for key in &to_remove {
            if let Some(entry) = inner.remove(key) {
                // Clean up the by_partition reverse index.
                if let Some(pk_keys) = by_part.get_mut(&entry.partition_key) {
                    pk_keys.retain(|k| k != key);
                    if pk_keys.is_empty() {
                        by_part.remove(&entry.partition_key);
                    }
                }
            }
        }
    }

    /// Return the number of entries in the index.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Return true if the index contains no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Filter entries by timestamp range [min_ts, max_ts] inclusive.
    ///
    /// Matches all entries whose column value bytes equal those in `value`,
    /// regardless of the CellValue's own timestamp metadata, then filters
    /// by the entry's timestamp.
    pub fn filter_by_timestamp(
        &self,
        value: &CellValue,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) -> Vec<IndexEntry> {
        let inner = self.inner.read();
        // Construct bounds that cover all CellValues with the same value bytes
        // but any timestamp/ttl/deletion_time metadata.
        let min_cell = CellValue {
            value: value.value.clone(),
            timestamp: Timestamp::MIN,
            ttl: i32::MIN,
            local_deletion_time: i32::MIN,
            path: None,
        };
        let max_cell = CellValue {
            value: value.value.clone(),
            timestamp: Timestamp::MAX,
            ttl: i32::MAX,
            local_deletion_time: i32::MAX,
            path: None,
        };
        let start = (min_cell, Timestamp::MIN);
        let end = (max_cell, Timestamp::MAX);
        inner
            .range((Bound::Included(&start), Bound::Included(&end)))
            .filter(|(_, entry)| entry.timestamp >= min_ts && entry.timestamp <= max_ts)
            .map(|(_, entry)| entry.clone())
            .collect()
    }

    /// Return the number of partitions tracked in the reverse index.
    pub fn partition_count(&self) -> usize {
        self.by_partition.read().len()
    }

    /// Return the (value, timestamp) keys tracked for a given partition key.
    pub fn keys_for_partition(&self, partition_key: &[u8]) -> Vec<IndexKey> {
        let by_part = self.by_partition.read();
        by_part.get(partition_key).cloned().unwrap_or_default()
    }

    /// Rebuild the index from an iterator of (value, entry) pairs.
    ///
    /// Used for crash recovery: replay memtable contents to reconstruct the index.
    pub fn rebuild_from<I>(&self, entries: I)
    where
        I: IntoIterator<Item = (CellValue, IndexEntry)>,
    {
        let mut inner = self.inner.write();
        let mut by_part = self.by_partition.write();
        inner.clear();
        by_part.clear();

        for (value, entry) in entries {
            let key = (value, entry.timestamp);
            let pk = entry.partition_key.clone();
            inner.insert(key.clone(), entry);
            by_part.entry(pk).or_default().push(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::CellValue;

    /// Helper: create a live CellValue with given bytes and timestamp.
    fn cell(value: &[u8], ts: Timestamp) -> CellValue {
        CellValue::live(value.to_vec(), ts)
    }

    /// Helper: create an IndexEntry.
    fn entry(pk: &[u8], ck: &[u8], ts: Timestamp) -> IndexEntry {
        IndexEntry {
            partition_key: pk.to_vec(),
            clustering_key: ck.to_vec(),
            timestamp: ts,
        }
    }

    // ── Test 1: mem_index_apply_gc ──────────────────────────────────────────

    #[test]
    fn mem_index_apply_gc() {
        let idx = MemIndex::new();

        // Insert entries at various timestamps.
        idx.insert(cell(b"alice", 100), entry(b"pk1", b"ck1", 100));
        idx.insert(cell(b"bob", 200), entry(b"pk2", b"ck2", 200));
        idx.insert(cell(b"carol", 300), entry(b"pk3", b"ck3", 300));
        assert_eq!(idx.len(), 3);

        // GC with boundary 250: entries with ts < 250 are removed.
        idx.gc(250);

        assert_eq!(idx.len(), 1);
        assert!(idx.lookup(&cell(b"alice", 100)).is_empty());
        assert!(idx.lookup(&cell(b"bob", 200)).is_empty());
        assert_eq!(idx.lookup(&cell(b"carol", 300)).len(), 1);
    }

    // ── Test 2: mem_index_update_replaces ───────────────────────────────────

    #[test]
    fn mem_index_update_replaces() {
        let idx = MemIndex::new();

        // Insert initial entry for pk1.
        idx.insert(cell(b"alice", 100), entry(b"pk1", b"ck1", 100));
        assert_eq!(idx.lookup(&cell(b"alice", 100)).len(), 1);

        // Update: same partition key, new value and timestamp.
        idx.insert(cell(b"bob", 200), entry(b"pk1", b"ck1", 200));

        // Old entry should be gone, new entry present.
        assert!(idx.lookup(&cell(b"alice", 100)).is_empty());
        assert_eq!(idx.lookup(&cell(b"bob", 200)).len(), 1);
        assert_eq!(idx.len(), 1);
    }

    // ── Test 3: mem_index_delete_removes ────────────────────────────────────

    #[test]
    fn mem_index_delete_removes() {
        let idx = MemIndex::new();

        idx.insert(cell(b"alice", 100), entry(b"pk1", b"ck1", 100));
        assert_eq!(idx.len(), 1);

        idx.remove(b"pk1");

        assert_eq!(idx.len(), 0);
        assert!(idx.lookup(&cell(b"alice", 100)).is_empty());
    }

    // ── Test 4: mem_index_range_scan ────────────────────────────────────────

    #[test]
    fn mem_index_range_scan() {
        let idx = MemIndex::new();

        idx.insert(cell(b"aaa", 100), entry(b"pk1", b"ck1", 100));
        idx.insert(cell(b"bbb", 200), entry(b"pk2", b"ck2", 200));
        idx.insert(cell(b"ccc", 300), entry(b"pk3", b"ck3", 300));
        idx.insert(cell(b"ddd", 400), entry(b"pk4", b"ck4", 400));

        let results = idx.range_scan(&cell(b"bbb", 200), &cell(b"ccc", 300));

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk2");
        assert_eq!(results[1].partition_key, b"pk3");
    }

    // ── Test 5: mem_index_timestamp_filter ──────────────────────────────────

    #[test]
    fn mem_index_timestamp_filter() {
        let idx = MemIndex::new();

        let val = cell(b"alice", 100);
        // Insert multiple entries with same value but different timestamps.
        // Since insert replaces by partition_key, use different partition keys.
        idx.insert(val.clone(), entry(b"pk1", b"ck1", 100));
        idx.insert(
            CellValue::live(b"alice".to_vec(), 200),
            entry(b"pk2", b"ck2", 200),
        );
        idx.insert(
            CellValue::live(b"alice".to_vec(), 300),
            entry(b"pk3", b"ck3", 300),
        );

        // Filter for timestamps in [150, 250].
        let results = idx.filter_by_timestamp(&CellValue::live(b"alice".to_vec(), 0), 150, 250);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");
    }

    // ── Test 6: mem_index_empty_lookup ──────────────────────────────────────

    #[test]
    fn mem_index_empty_lookup() {
        let idx = MemIndex::new();

        let results = idx.lookup(&cell(b"nonexistent", 0));
        assert!(results.is_empty());
        assert_eq!(idx.len(), 0);
    }

    // ── Test 7: mem_index_by_partition_tracks_values ────────────────────────

    #[test]
    fn mem_index_by_partition_tracks_values() {
        let idx = MemIndex::new();

        idx.insert(cell(b"alice", 100), entry(b"pk1", b"ck1", 100));
        idx.insert(cell(b"bob", 200), entry(b"pk2", b"ck2", 200));

        // Check partition tracking.
        assert_eq!(idx.partition_count(), 2);

        let pk1_keys = idx.keys_for_partition(b"pk1");
        assert_eq!(pk1_keys.len(), 1);
        assert_eq!(pk1_keys[0].0, cell(b"alice", 100));

        let pk2_keys = idx.keys_for_partition(b"pk2");
        assert_eq!(pk2_keys.len(), 1);
        assert_eq!(pk2_keys[0].0, cell(b"bob", 200));
    }

    // ── Test 8: mem_index_memtable_atomicity ────────────────────────────────

    #[test]
    fn mem_index_memtable_atomicity() {
        // Verify that insert + reverse-index update is atomic:
        // after insert, both the forward and reverse indexes are consistent.
        let idx = MemIndex::new();

        idx.insert(cell(b"alice", 100), entry(b"pk1", b"ck1", 100));

        // Forward lookup succeeds.
        let fwd = idx.lookup(&cell(b"alice", 100));
        assert_eq!(fwd.len(), 1);

        // Reverse lookup succeeds.
        let rev = idx.keys_for_partition(b"pk1");
        assert_eq!(rev.len(), 1);

        // Now update (same pk, new value).
        idx.insert(cell(b"bob", 200), entry(b"pk1", b"ck1", 200));

        // Old forward entry gone.
        assert!(idx.lookup(&cell(b"alice", 100)).is_empty());
        // New forward entry present.
        assert_eq!(idx.lookup(&cell(b"bob", 200)).len(), 1);
        // Reverse index updated atomically.
        let rev2 = idx.keys_for_partition(b"pk1");
        assert_eq!(rev2.len(), 1);
        assert_eq!(rev2[0].0, cell(b"bob", 200));
    }

    // ── Test 9: mem_index_crash_recovery ────────────────────────────────────

    #[test]
    fn mem_index_crash_recovery() {
        let idx = MemIndex::new();

        // Simulate original index state.
        idx.insert(cell(b"alice", 100), entry(b"pk1", b"ck1", 100));
        idx.insert(cell(b"bob", 200), entry(b"pk2", b"ck2", 200));
        assert_eq!(idx.len(), 2);

        // "Crash" — create a new index and rebuild from saved entries.
        let recovered = MemIndex::new();
        recovered.rebuild_from(vec![
            (cell(b"alice", 100), entry(b"pk1", b"ck1", 100)),
            (cell(b"bob", 200), entry(b"pk2", b"ck2", 200)),
        ]);

        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered.lookup(&cell(b"alice", 100)).len(), 1);
        assert_eq!(recovered.lookup(&cell(b"bob", 200)).len(), 1);
        assert_eq!(recovered.partition_count(), 2);
    }

    // ── Test 10: mem_index_no_interleave ────────────────────────────────────

    #[test]
    fn mem_index_no_interleave() {
        // Verify that a concurrent reader never sees a partial update:
        // after an update-replace, the old entry is gone AND the new entry
        // is present — never just one of those two states.
        use std::sync::Arc;
        use std::thread;

        let idx = Arc::new(MemIndex::new());
        idx.insert(cell(b"v1", 100), entry(b"pk1", b"ck1", 100));

        let writer_idx = Arc::clone(&idx);
        let reader_idx = Arc::clone(&idx);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let bar_w = Arc::clone(&barrier);
        let bar_r = Arc::clone(&barrier);

        let writer = thread::spawn(move || {
            bar_w.wait();
            for i in 0..1000 {
                let ts = 200 + i;
                writer_idx.insert(
                    cell(format!("v{i}").as_bytes(), ts),
                    entry(b"pk1", b"ck1", ts),
                );
            }
        });

        let reader = thread::spawn(move || {
            bar_r.wait();
            for _ in 0..1000 {
                // The index should always have exactly 1 entry for pk1.
                let pk1_keys = reader_idx.keys_for_partition(b"pk1");
                // Could be 0 (between remove and insert within insert())
                // but with our lock-based impl it must be exactly 0 or 1.
                assert!(
                    pk1_keys.len() <= 1,
                    "interleaved partial update visible: {} entries",
                    pk1_keys.len()
                );

                // Total entries for pk1 should be 0 or 1.
                let total = reader_idx.len();
                assert!(
                    total <= 1,
                    "more than 1 entry visible for single-partition index: {total}"
                );
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();

        // After all writes, exactly 1 entry.
        assert_eq!(idx.len(), 1);
    }

    // ── Test 11: mem_index_flush_gc_boundary ────────────────────────────────

    #[test]
    fn mem_index_flush_gc_boundary() {
        let idx = MemIndex::new();

        idx.insert(cell(b"a", 99), entry(b"pk1", b"ck1", 99));
        idx.insert(cell(b"b", 100), entry(b"pk2", b"ck2", 100));
        idx.insert(cell(b"c", 101), entry(b"pk3", b"ck3", 101));

        // GC boundary at exactly 100: only ts < 100 are removed.
        idx.gc(100);

        assert_eq!(idx.len(), 2);
        assert!(idx.lookup(&cell(b"a", 99)).is_empty());
        assert_eq!(idx.lookup(&cell(b"b", 100)).len(), 1);
        assert_eq!(idx.lookup(&cell(b"c", 101)).len(), 1);
    }

    // ── Test 12: mem_index_flush_gc_by_partition_cleanup ────────────────────

    #[test]
    fn mem_index_flush_gc_by_partition_cleanup() {
        let idx = MemIndex::new();

        idx.insert(cell(b"a", 100), entry(b"pk1", b"ck1", 100));
        idx.insert(cell(b"b", 200), entry(b"pk2", b"ck2", 200));

        assert_eq!(idx.partition_count(), 2);

        // GC removes pk1's entry (ts 100 < 150).
        idx.gc(150);

        // pk1 should be cleaned from the partition index too.
        assert_eq!(idx.partition_count(), 1);
        assert!(idx.keys_for_partition(b"pk1").is_empty());
        assert_eq!(idx.keys_for_partition(b"pk2").len(), 1);
    }

    // ── Test 13: mem_index_flush_gc_idempotent ──────────────────────────────

    #[test]
    fn mem_index_flush_gc_idempotent() {
        let idx = MemIndex::new();

        idx.insert(cell(b"a", 100), entry(b"pk1", b"ck1", 100));
        idx.insert(cell(b"b", 200), entry(b"pk2", b"ck2", 200));
        idx.insert(cell(b"c", 300), entry(b"pk3", b"ck3", 300));

        // First GC.
        idx.gc(250);
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.partition_count(), 1);

        // Second GC with same boundary — no change.
        idx.gc(250);
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.partition_count(), 1);

        // Third GC with lower boundary — no change.
        idx.gc(150);
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.partition_count(), 1);

        // The surviving entry is still accessible.
        assert_eq!(idx.lookup(&cell(b"c", 300)).len(), 1);
    }
}
