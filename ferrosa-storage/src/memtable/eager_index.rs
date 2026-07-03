//! Eager index build on memtable flush.
//!
//! When a memtable is flushed to an SSTable, the eager index build hook
//! schedules a high-priority index build so that sidecar indexes stay
//! current. This keeps the in-memory `MemtableIndex` (Layer 4) at 0–1
//! entries in steady state — only entries from writes that arrived *after*
//! the flush need to live in memory.
//!
//! The same hook fires after compaction produces a new SSTable, ensuring
//! that the merged output has an up-to-date sidecar index.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::index::{BuildPriority, IndexBuildJob, IndexBuildScheduler};

/// Event emitted when a flush or compaction completes.
///
/// Contains the metadata needed to schedule index build jobs for the
/// newly produced SSTable.
#[derive(Debug, Clone)]
pub struct FlushCompleteEvent {
    /// Generation / ID of the newly created SSTable.
    pub sstable_id: String,
    /// Keyspace of the table.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// Secondary index declarations: `(index_name, column_position)`.
    pub indexed_columns: Vec<(String, usize)>,
}

/// Hook that fires on flush/compaction completion to schedule eager index builds.
///
/// Each call to `on_flush_complete()` submits one `IndexBuildJob` per declared
/// secondary index at `BuildPriority::High`. The scheduler processes high-priority
/// jobs before normal ones, keeping the MemtableIndex bounded.
pub struct EagerIndexBuilder {
    scheduler: Arc<IndexBuildScheduler>,
    /// Counter for builds submitted — used by tests to verify the hook fires.
    builds_submitted: AtomicUsize,
}

impl EagerIndexBuilder {
    /// Create a new eager index builder backed by the given scheduler.
    pub fn new(scheduler: Arc<IndexBuildScheduler>) -> Self {
        Self {
            scheduler,
            builds_submitted: AtomicUsize::new(0),
        }
    }

    /// Schedule high-priority index builds for a newly flushed SSTable.
    ///
    /// Returns the number of jobs submitted. Returns 0 if the table has
    /// no secondary indexes or if all submissions fail.
    pub fn on_flush_complete(&self, event: &FlushCompleteEvent) -> usize {
        let mut submitted = 0;
        for (index_name, col_pos) in &event.indexed_columns {
            let job = IndexBuildJob {
                sstable_id: event.sstable_id.clone(),
                index_name: index_name.clone(),
                // NOTE: hardcoded `BTree` because `FlushCompleteEvent` only
                // carries `(index_name, column_position)` and has no source
                // for the index's real `IndexType`. This `EagerIndexBuilder`
                // is the un-wired helper — the live flush/compaction eager
                // rebuild runs through `engine::eager_index_build_job`, which
                // reads the real type from `TableStore::index_type_for`. If
                // this builder is ever wired in, thread the declared
                // `IndexType` through `FlushCompleteEvent::indexed_columns`
                // (or a parallel field) before using it for non-BTree indexes.
                index_type: ferrosa_index::IndexType::BTree,
                table: (event.keyspace.clone(), event.table.clone()),
                priority: BuildPriority::High,
                enqueued_at: std::time::Instant::now(),
                column_position: *col_pos,
                clustering_source: None,
                filter_predicate: None,
            };
            if self.scheduler.submit(job).is_ok() {
                submitted += 1;
            }
        }
        self.builds_submitted
            .fetch_add(submitted, Ordering::Relaxed);
        submitted
    }

    /// Schedule high-priority index builds after compaction produces a new SSTable.
    ///
    /// Identical to `on_flush_complete()` — compaction output needs the same
    /// eager index treatment to keep sidecar indexes current.
    pub fn on_compaction_complete(&self, event: &FlushCompleteEvent) -> usize {
        self.on_flush_complete(event)
    }

    /// Total number of index build jobs submitted since creation.
    pub fn builds_submitted(&self) -> usize {
        self.builds_submitted.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::tracker::IndexStateTracker;
    use std::sync::Arc;

    /// Helper: create a scheduler with a stub backend and 1 worker thread.
    fn test_scheduler() -> (Arc<IndexBuildScheduler>, Arc<IndexStateTracker>) {
        let tracker = Arc::new(IndexStateTracker::new());
        let scheduler = Arc::new(IndexBuildScheduler::new(1, Arc::clone(&tracker)));
        (scheduler, tracker)
    }

    /// Helper: register an index in the tracker so builds can be tracked.
    fn register_index(tracker: &IndexStateTracker, ks: &str, table: &str, index: &str) {
        tracker.register_index(ks, table, index);
    }

    // ── Test 1: eager_index_build_on_flush ──────────────────────────────

    #[test]
    fn eager_index_build_on_flush() {
        let (scheduler, tracker) = test_scheduler();
        register_index(&tracker, "test_ks", "test_table", "idx_val");

        let builder = EagerIndexBuilder::new(Arc::clone(&scheduler));

        let event = FlushCompleteEvent {
            sstable_id: "1".to_string(),
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            indexed_columns: vec![("idx_val".to_string(), 0)],
        };

        let submitted = builder.on_flush_complete(&event);
        assert_eq!(submitted, 1, "flush should trigger one index build job");
        assert_eq!(builder.builds_submitted(), 1);

        // Give the worker thread time to process the job.
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Verify the tracker was updated: the SSTable should be marked indexed.
        let state = tracker
            .get_state("test_ks", "test_table", "idx_val")
            .expect("index state should exist");
        assert!(
            state.indexed_sstables.contains("1"),
            "SSTable 1 should be marked as indexed after eager build"
        );

        scheduler.shutdown();
    }

    // ── Test 2: eager_index_build_layer4_bounded ────────────────────────

    #[test]
    fn eager_index_build_layer4_bounded() {
        use crate::flush::InMemoryFlushTarget;
        use crate::store::TableStore;
        use ferrosa_common::cell::CellValue;
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        use ferrosa_sstable::WriteOptions;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions::default(),
            vec![("idx_val".to_string(), 0)],
        );

        // Write several rows to populate the memtable index.
        for i in 0..10 {
            let key = DecoratedKey::new(PartitionKey::new(format!("pk{i}").into_bytes()));
            let row = Row {
                clustering: vec![],
                cells: vec![(
                    0,
                    CellValue::live(format!("value_{i}").into_bytes(), 1000 + i),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000 + i),
            };
            store.write(&key, row).unwrap();
        }

        // Before flush: memtable index should have 10 entries.
        let pre_flush_count = store.memtable_index_entry_count("idx_val");
        assert_eq!(pre_flush_count, 10, "pre-flush: 10 entries in MemIndex");

        // Flush: moves the old memtable (and its index) to an SSTable,
        // installs a fresh empty memtable and fresh empty indexes.
        store.flush().unwrap();

        // After flush: the active MemtableIndex should be empty (0 entries)
        // because the flush swapped in fresh indexes.
        let post_flush_count = store.memtable_index_entry_count("idx_val");
        assert!(
            post_flush_count <= 1,
            "post-flush: MemIndex should have 0-1 entries, got {post_flush_count}"
        );

        // Write one more row — this should be the only entry in the fresh MemIndex.
        let key = DecoratedKey::new(PartitionKey::new(b"pk_new".to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"new_value".to_vec(), 2000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(2000),
        };
        store.write(&key, row).unwrap();

        let final_count = store.memtable_index_entry_count("idx_val");
        assert!(
            final_count <= 1,
            "after one write post-flush: MemIndex should have exactly 1 entry, got {final_count}"
        );
    }

    // ── Test 3: eager_index_build_after_compaction ──────────────────────

    #[test]
    fn eager_index_build_after_compaction() {
        let (scheduler, tracker) = test_scheduler();
        register_index(&tracker, "test_ks", "test_table", "idx_val");

        let builder = EagerIndexBuilder::new(Arc::clone(&scheduler));

        // Simulate two flushes producing SSTables 1 and 2.
        let flush_event_1 = FlushCompleteEvent {
            sstable_id: "1".to_string(),
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            indexed_columns: vec![("idx_val".to_string(), 0)],
        };
        builder.on_flush_complete(&flush_event_1);

        let flush_event_2 = FlushCompleteEvent {
            sstable_id: "2".to_string(),
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            indexed_columns: vec![("idx_val".to_string(), 0)],
        };
        builder.on_flush_complete(&flush_event_2);

        // Simulate compaction merging SSTables 1+2 into SSTable 3.
        let compaction_event = FlushCompleteEvent {
            sstable_id: "3".to_string(),
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            indexed_columns: vec![("idx_val".to_string(), 0)],
        };
        let submitted = builder.on_compaction_complete(&compaction_event);
        assert_eq!(
            submitted, 1,
            "compaction should trigger one index build job"
        );

        // Total: 3 jobs submitted (2 flushes + 1 compaction).
        assert_eq!(builder.builds_submitted(), 3);

        // Give the worker time to process all jobs.
        std::thread::sleep(std::time::Duration::from_millis(300));

        // Verify the compacted SSTable is marked indexed.
        let state = tracker
            .get_state("test_ks", "test_table", "idx_val")
            .expect("index state should exist");
        assert!(
            state.indexed_sstables.contains("3"),
            "compacted SSTable 3 should be marked as indexed"
        );

        // The earlier SSTables should also be indexed.
        assert!(
            state.indexed_sstables.contains("1"),
            "SSTable 1 should still be marked"
        );
        assert!(
            state.indexed_sstables.contains("2"),
            "SSTable 2 should still be marked"
        );

        scheduler.shutdown();
    }
}
