//! Lock-free composition of memtable, flush, and SSTable reads.
//!
//! [`TableStore`] coordinates the three tiers of the storage engine:
//!
//! 1. **Active memtable** — absorbs all writes via a lock-free ArcSwap view.
//! 2. **Flushing memtable** — captured during a flush; remains readable until
//!    the SSTable is built and swapped in.
//! 3. **SSTables** — immutable, ordered newest-first. The read path queries
//!    all sources and merges results with cell-level last-write-wins.
//!
//! The read path is lock-free: it uses `ArcSwap::load()` to atomically
//! snapshot the current view without blocking any writer or flusher.
//! Flush serialization is enforced by a `parking_lot::Mutex`.

use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::TableSchema;
use ferrosa_common::Result;
use ferrosa_sstable::io::ReadAt;
use ferrosa_sstable::reader::SSTableReader;
use ferrosa_sstable::types::{Partition, Row};
use ferrosa_sstable::writer::SSTableWriter;
use ferrosa_sstable::WriteOptions;

use crate::flush::{self, FlushTarget};
#[cfg(not(feature = "skiplist-memtable"))]
use crate::memtable::sharded::ShardedBTreeMemtable;
#[cfg(feature = "skiplist-memtable")]
use crate::memtable::skiplist::SkipListMemtable;
use crate::memtable::Memtable;
use crate::merge;

/// Atomic snapshot of the storage engine's current state.
///
/// Held inside an [`ArcSwap`] so any thread can load a consistent view
/// without locking. The `Arc` fields inside ensure the data structures
/// remain alive as long as any reader holds a guard.
struct StoreView<R: ReadAt + Send + Sync + 'static> {
    /// The active memtable: accepts all current writes.
    active: Arc<dyn Memtable>,
    /// A memtable that has been swapped out and is being flushed.
    /// Readable during the flush; `None` when no flush is in progress.
    flushing: Option<Arc<dyn Memtable>>,
    /// Completed SSTables, newest first.
    sstables: Arc<Vec<Arc<SSTableReader<R>>>>,
}

/// Single-table storage engine: lock-free reads, serialized flushes.
///
/// `F` is the flush destination (in-memory for tests, file-based for
/// production). `F::Reader` must be `ReadAt + Send + Sync + 'static`
/// so the resulting `SSTableReader` can be held inside the shared view.
pub struct TableStore<F: FlushTarget> {
    schema: TableSchema,
    view: ArcSwap<StoreView<F::Reader>>,
    /// Serializes concurrent flushes. The read/write paths never touch this.
    flush_guard: Mutex<()>,
    flush_target: F,
    options: WriteOptions,
}

fn new_memtable() -> Arc<dyn Memtable> {
    #[cfg(feature = "skiplist-memtable")]
    {
        Arc::new(SkipListMemtable::new())
    }
    #[cfg(not(feature = "skiplist-memtable"))]
    {
        Arc::new(ShardedBTreeMemtable::with_default_shards())
    }
}

impl<F: FlushTarget> TableStore<F> {
    /// Create a new `TableStore` with an empty memtable and no SSTables.
    pub fn new(schema: TableSchema, flush_target: F, options: WriteOptions) -> Self {
        let active: Arc<dyn Memtable> = new_memtable();
        let initial_view = StoreView {
            active,
            flushing: None,
            sstables: Arc::new(vec![]),
        };
        Self {
            schema,
            view: ArcSwap::from_pointee(initial_view),
            flush_guard: Mutex::new(()),
            flush_target,
            options,
        }
    }

    /// Write a row into the active memtable.
    ///
    /// Loads the current view atomically, then delegates to the memtable's
    /// `put`. No lock is taken; the ArcSwap guard provides the necessary
    /// lifetime without blocking.
    pub fn write(&self, key: &DecoratedKey, row: Row) -> Result<()> {
        let guard = self.view.load();
        guard.active.put(key, row, &self.schema)
    }

    /// Read a partition by merging all sources: active memtable, flushing
    /// memtable (if present), and SSTables (newest first).
    ///
    /// Returns `None` if no source contains the key. If multiple sources
    /// return data for the same key, `merge_partitions` applies cell-level
    /// last-write-wins semantics.
    pub fn read(&self, key: &DecoratedKey) -> Result<Option<Partition>> {
        let guard = self.view.load();

        let mut sources: Vec<Partition> = Vec::new();

        // Active memtable
        if let Some(p) = guard.active.get(key)? {
            sources.push((*p).clone());
        }

        // Flushing memtable
        if let Some(ref flushing) = guard.flushing {
            if let Some(p) = flushing.get(key)? {
                sources.push((*p).clone());
            }
        }

        // SSTables, newest first
        for sstable in guard.sstables.iter() {
            if let Some(p) = sstable.get_partition(key)? {
                sources.push(p);
            }
        }

        if sources.is_empty() {
            return Ok(None);
        }

        Ok(Some(merge::merge_partitions(sources)))
    }

    /// Flush the active memtable to an SSTable.
    ///
    /// The flush sequence:
    /// 1. Lock the flush mutex (serializes concurrent flush calls).
    /// 2. Install a fresh active memtable; move the old one to `flushing`.
    /// 3. Snapshot the flushing memtable.
    /// 4. If the snapshot is empty, clear `flushing` and return (no-op).
    /// 5. Build the SSTable via [`SSTableWriter`] and [`FlushTarget::flush`].
    /// 6. Prepend the new reader to the SSTable list and clear `flushing`.
    pub fn flush(&self) -> Result<()> {
        let _guard = self.flush_guard.lock();

        // Step 1: Swap in a fresh active memtable, move old to flushing.
        let new_active: Arc<dyn Memtable> = new_memtable();
        let old_view = self.view.load();
        let old_active = Arc::clone(&old_view.active);
        let current_sstables = Arc::clone(&old_view.sstables);
        // Drop the guard before storing (ArcSwap does not require it, but
        // dropping early avoids holding a pinned epoch longer than needed).
        drop(old_view);

        self.view.store(Arc::new(StoreView {
            active: new_active,
            flushing: Some(Arc::clone(&old_active)),
            sstables: Arc::clone(&current_sstables),
        }));

        // Step 2: Snapshot the flushing memtable.
        let mut partitions = old_active.snapshot();

        // Step 3: No-op if the memtable was empty.
        if partitions.is_empty() {
            // Re-load the live view to get current sstables (not the stale
            // capture from the top of flush) — defensive against future
            // changes to locking discipline.
            let live = self.view.load();
            self.view.store(Arc::new(StoreView {
                active: Arc::clone(&live.active),
                flushing: None,
                sstables: Arc::clone(&live.sstables),
            }));
            return Ok(());
        }

        // Step 4: Sort partitions by key (required by SSTableWriter).
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        // Step 5: Build the SSTable.
        // Force compression off — there is a known CRC mismatch between
        // SSTableWriter and SSTableReader for compressed data.
        let mut options = self.options.clone();
        options.compression = None;

        let header = flush::build_serialization_header(&self.schema, &partitions);
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p)?;
        }
        let output = writer.finish()?;
        let reader = self.flush_target.flush(output)?;
        let new_reader = Arc::new(reader);

        // Step 6: Prepend new SSTable, clear flushing.
        let current_view = self.view.load();
        let mut new_sstables = vec![new_reader];
        new_sstables.extend(current_view.sstables.iter().cloned());

        self.view.store(Arc::new(StoreView {
            active: Arc::clone(&current_view.active),
            flushing: None,
            sstables: Arc::new(new_sstables),
        }));

        Ok(())
    }

    /// Reads partitions from the memtable in token order with an optional
    /// token range filter and limit.
    ///
    /// Currently scans the active memtable only (full snapshot, then filter).
    /// This is O(N) in the memtable size — acceptable for an initial impl
    /// but should be optimized with a range-aware iterator when the
    /// SkipListMemtable is available. SSTable range reads will be added
    /// when the SSTable reader supports range iteration.
    pub fn read_range(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
    ) -> Result<Vec<Partition>> {
        let guard = self.view.load();
        let snapshot = guard.active.snapshot();

        let filtered: Vec<Partition> = snapshot
            .into_iter()
            .filter(|p| {
                if let Some(s) = start {
                    if p.key < *s {
                        return false;
                    }
                }
                if let Some(e) = end {
                    if p.key > *e {
                        return false;
                    }
                }
                true
            })
            .take(limit)
            .collect();

        Ok(filtered)
    }

    /// Number of SSTables currently in the store.
    pub fn sstable_count(&self) -> usize {
        self.view.load().sstables.len()
    }

    /// Approximate memory usage of the active memtable in bytes.
    pub fn memtable_size(&self) -> usize {
        self.view.load().active.size_bytes()
    }

    /// Number of partitions in the active memtable.
    pub fn memtable_partition_count(&self) -> usize {
        self.view.load().active.partition_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::flush::InMemoryFlushTarget;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::PartitionKey;
    use ferrosa_common::schema::ColumnDefinition;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        }
    }

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_row(value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01], // Int32Type = 4 bytes big-endian
            cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    fn test_store() -> TableStore<InMemoryFlushTarget> {
        TableStore::new(
            test_schema(),
            InMemoryFlushTarget,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        )
    }

    // -------------------------------------------------------------------------
    // Test 1: write then read from memtable
    // -------------------------------------------------------------------------
    #[test]
    fn write_then_read_from_memtable() {
        let store = test_store();
        let key = make_key("pk1");
        store.write(&key, make_row(b"hello", 1000)).unwrap();

        let result = store.read(&key).unwrap();
        assert!(result.is_some(), "expected Some partition");
        let partition = result.unwrap();
        assert_eq!(partition.rows.len(), 1);
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"hello".as_slice())
        );
    }

    // -------------------------------------------------------------------------
    // Test 2: read non-existent key returns None
    // -------------------------------------------------------------------------
    #[test]
    fn read_nonexistent_returns_none() {
        let store = test_store();
        let key = make_key("ghost");
        assert!(store.read(&key).unwrap().is_none());
    }

    // -------------------------------------------------------------------------
    // Test 3: memtable size and partition count stats
    // -------------------------------------------------------------------------
    #[test]
    fn memtable_size_and_count() {
        let store = test_store();
        assert_eq!(store.memtable_partition_count(), 0);
        assert_eq!(store.memtable_size(), 0);

        store.write(&make_key("k1"), make_row(b"v1", 1000)).unwrap();
        assert_eq!(store.memtable_partition_count(), 1);
        assert!(store.memtable_size() > 0);

        store.write(&make_key("k2"), make_row(b"v2", 1000)).unwrap();
        assert_eq!(store.memtable_partition_count(), 2);
    }

    // -------------------------------------------------------------------------
    // Test 4: flush creates an SSTable and clears the memtable
    // -------------------------------------------------------------------------
    #[test]
    fn flush_creates_sstable() {
        let store = test_store();
        store.write(&make_key("k1"), make_row(b"v1", 1000)).unwrap();
        assert_eq!(store.sstable_count(), 0);

        store.flush().unwrap();

        assert_eq!(store.sstable_count(), 1);
        assert_eq!(store.memtable_partition_count(), 0);
    }

    // -------------------------------------------------------------------------
    // Test 5: write, flush, read back from SSTable
    // -------------------------------------------------------------------------
    #[test]
    fn read_after_flush_finds_partition() {
        let store = test_store();
        let key = make_key("pk_flushed");
        store.write(&key, make_row(b"flushed_val", 2000)).unwrap();
        store.flush().unwrap();

        let result = store.read(&key).unwrap();
        assert!(result.is_some(), "expected partition from SSTable");
        let partition = result.unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"flushed_val".as_slice())
        );
    }

    // -------------------------------------------------------------------------
    // Test 6: write, flush, write again, read merges both sources
    // -------------------------------------------------------------------------
    #[test]
    fn write_flush_write_read_merges_sources() {
        let store = test_store();
        let key = make_key("shared_key");

        // Write old value and flush to SSTable.
        store.write(&key, make_row(b"old_val", 1000)).unwrap();
        store.flush().unwrap();

        // Write newer value — stays in memtable.
        store.write(&key, make_row(b"new_val", 2000)).unwrap();

        let result = store.read(&key).unwrap();
        assert!(result.is_some());
        let partition = result.unwrap();
        // Cell-level LWW: timestamp 2000 wins.
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"new_val".as_slice())
        );
        assert_eq!(partition.rows[0].cells[0].1.timestamp, 2000);
    }

    // -------------------------------------------------------------------------
    // Test 7: multiple flushes accumulate SSTables, all readable
    // -------------------------------------------------------------------------
    #[test]
    fn multiple_flushes_accumulate_sstables() {
        let store = test_store();

        // First flush: k1
        store.write(&make_key("k1"), make_row(b"v1", 1000)).unwrap();
        store.flush().unwrap();
        assert_eq!(store.sstable_count(), 1);

        // Second flush: k2
        store.write(&make_key("k2"), make_row(b"v2", 2000)).unwrap();
        store.flush().unwrap();
        assert_eq!(store.sstable_count(), 2);

        // Both partitions should be readable.
        let r1 = store.read(&make_key("k1")).unwrap();
        assert!(r1.is_some(), "k1 should be readable from first SSTable");
        assert_eq!(
            r1.unwrap().rows[0].cells[0].1.value.as_deref(),
            Some(b"v1".as_slice())
        );

        let r2 = store.read(&make_key("k2")).unwrap();
        assert!(r2.is_some(), "k2 should be readable from second SSTable");
        assert_eq!(
            r2.unwrap().rows[0].cells[0].1.value.as_deref(),
            Some(b"v2".as_slice())
        );
    }

    // -------------------------------------------------------------------------
    // Test 8: read_range returns partitions in order
    // -------------------------------------------------------------------------
    #[test]
    fn read_range_returns_partitions_in_order() {
        let store = test_store();
        // Write several partitions.
        for i in 0..5 {
            let key = make_key(&format!("k{i}"));
            store
                .write(&key, make_row(format!("v{i}").as_bytes(), 1000))
                .unwrap();
        }

        let results = store.read_range(None, None, 100).unwrap();
        assert_eq!(results.len(), 5);
        // Should be in token order.
        for window in results.windows(2) {
            assert!(window[0].key <= window[1].key);
        }
    }

    // -------------------------------------------------------------------------
    // Test 9: read_range with limit
    // -------------------------------------------------------------------------
    #[test]
    fn read_range_with_limit() {
        let store = test_store();
        for i in 0..10 {
            store
                .write(&make_key(&format!("k{i}")), make_row(b"v", 1000))
                .unwrap();
        }
        let results = store.read_range(None, None, 3).unwrap();
        assert_eq!(results.len(), 3);
    }

    // -------------------------------------------------------------------------
    // Test 10: flush on an empty memtable is a no-op
    // -------------------------------------------------------------------------
    #[test]
    fn flush_empty_memtable_is_noop() {
        let store = test_store();
        assert_eq!(store.sstable_count(), 0);

        store.flush().unwrap();

        assert_eq!(
            store.sstable_count(),
            0,
            "empty flush should not create SSTable"
        );
    }
}
