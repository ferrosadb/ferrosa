//! Sharded BTreeMap memtable implementation.
//!
//! Uses 64 shards (configurable) of `parking_lot::RwLock<BTreeMap>` to
//! distribute write contention. Shard selection: `key.token.0 as u64 % num_shards`.
//!
//! This is the initial implementation behind the `Memtable` trait. The trait
//! enables swapping to a lock-free structure (crossbeam-skiplist, Okasaki-style
//! persistent structures) without changing consumer code.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::TableSchema;
use ferrosa_common::Result;
use ferrosa_sstable::types::{DeletionTime, Partition, Row};

use super::Memtable;

/// Default number of shards. 64 gives good contention distribution on
/// modern multi-core systems without excessive overhead.
const DEFAULT_NUM_SHARDS: usize = 64;

/// Sharded BTreeMap-based memtable.
///
/// Each shard is an independently-locked `BTreeMap<DecoratedKey, Arc<Partition>>`.
/// Writes lock a single shard (determined by token hash), so concurrent writes
/// to different shards never contend.
pub struct ShardedBTreeMemtable {
    /// The shards. Public for test visibility (e.g., `multi_shard_distribution`).
    pub(crate) shards: Vec<RwLock<BTreeMap<DecoratedKey, Arc<Partition>>>>,
    /// Number of shards.
    num_shards: usize,
    /// Approximate total memory usage in bytes. Updated on each put.
    size: AtomicUsize,
    /// Number of distinct partitions stored.
    count: AtomicUsize,
    /// Number of times a shard write lock experienced contention
    /// (try_write failed, had to block). Zero contention is the ideal case.
    pub write_contention_count: AtomicUsize,
}

impl ShardedBTreeMemtable {
    /// Create a new sharded memtable with the given number of shards.
    pub fn new(num_shards: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        let shards = (0..num_shards)
            .map(|_| RwLock::new(BTreeMap::new()))
            .collect();
        Self {
            shards,
            num_shards,
            size: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            write_contention_count: AtomicUsize::new(0),
        }
    }

    /// Create a new sharded memtable with the default 64 shards.
    pub fn with_default_shards() -> Self {
        Self::new(DEFAULT_NUM_SHARDS)
    }

    /// Determine which shard a key belongs to.
    fn shard_index(&self, key: &DecoratedKey) -> usize {
        (key.token.0 as u64 % self.num_shards as u64) as usize
    }

    /// Estimate the in-memory size of a partition in bytes.
    fn estimate_partition_size(partition: &Partition) -> usize {
        let mut size = std::mem::size_of::<Partition>();
        // Key bytes
        size += partition.key.key.as_bytes().len();
        // Static row
        if let Some(ref sr) = partition.static_row {
            size += Self::estimate_row_size(sr);
        }
        // Regular rows
        for row in &partition.rows {
            size += Self::estimate_row_size(row);
        }
        size
    }

    /// Estimate the in-memory size of a row in bytes.
    fn estimate_row_size(row: &Row) -> usize {
        let mut size = std::mem::size_of::<Row>();
        size += row.clustering.len();
        for (_, cell) in &row.cells {
            size += std::mem::size_of::<(u16, ferrosa_common::cell::CellValue)>();
            if let Some(ref v) = cell.value {
                size += v.len();
            }
        }
        size
    }
}

impl Memtable for ShardedBTreeMemtable {
    fn put(&self, key: &DecoratedKey, row: Row, schema: &TableSchema) -> Result<()> {
        // Fail-loud guard: reject mis-sized cells before they reach the
        // memtable. Without this check the `now()`-into-TimeUUID bug
        // would land an 8-byte cell in a 16-byte column, wedging every
        // subsequent flush attempt.
        super::validate_row_against_schema(&row, schema)?;
        let idx = self.shard_index(key);
        let mut shard = match self.shards[idx].try_write() {
            Some(guard) => guard,
            None => {
                // Lock was contended — record and fall back to blocking.
                self.write_contention_count.fetch_add(1, Ordering::Relaxed);
                self.shards[idx].write()
            }
        };

        if let Some(existing) = shard.get_mut(key) {
            // Compute old size for delta
            let old_size = Self::estimate_partition_size(existing);

            // We need to mutate the partition inside the Arc. Since we hold the
            // write lock, no other thread can be reading/writing this shard.
            // Use Arc::make_mut for copy-on-write if there are other Arc refs.
            let partition = Arc::make_mut(existing);
            merge_row_into_partition(partition, row);

            let new_size = Self::estimate_partition_size(partition);
            // Update size delta (could be negative if overwrite with smaller value,
            // but we use wrapping arithmetic via AtomicUsize)
            if new_size >= old_size {
                self.size.fetch_add(new_size - old_size, Ordering::Relaxed);
            } else {
                self.size.fetch_sub(old_size - new_size, Ordering::Relaxed);
            }
        } else {
            // New partition
            let partition = Partition {
                key: key.clone(),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![row],
            };
            let size = Self::estimate_partition_size(&partition);
            shard.insert(key.clone(), Arc::new(partition));
            self.count.fetch_add(1, Ordering::Relaxed);
            self.size.fetch_add(size, Ordering::Relaxed);
        }

        Ok(())
    }

    fn get(&self, key: &DecoratedKey) -> Result<Option<Arc<Partition>>> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        Ok(shard.get(key).cloned())
    }

    fn snapshot(&self) -> Vec<Partition> {
        // Collect partitions from each shard in parallel using scoped threads.
        // Each shard's BTreeMap is already sorted by DecoratedKey (token order),
        // so we get pre-sorted vectors that we merge with k_way_merge.
        let shard_data: Vec<Vec<Partition>> = std::thread::scope(|s| {
            let handles: Vec<_> = self
                .shards
                .iter()
                .map(|shard| {
                    s.spawn(|| {
                        let guard = shard.read();
                        guard
                            .values()
                            .map(|arc| Partition::clone(arc))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        k_way_merge(shard_data)
    }

    fn snapshot_range_limited(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
    ) -> Vec<Partition> {
        if limit == 0 {
            return Vec::new();
        }
        // Each shard contributes at most `limit` matches, so this avoids the
        // previous all-memtable materialization while preserving global token
        // order after the k-way merge. The over-read is bounded by
        // num_shards * limit, not table cardinality.
        let shard_data: Vec<Vec<Partition>> = std::thread::scope(|s| {
            let handles: Vec<_> = self
                .shards
                .iter()
                .map(|shard| {
                    s.spawn(|| {
                        let guard = shard.read();
                        guard
                            .iter()
                            .filter(|(key, _)| {
                                start.is_none_or(|s| *key >= s) && end.is_none_or(|e| *key <= e)
                            })
                            .take(limit)
                            .map(|(_, arc)| Partition::clone(arc))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        k_way_merge(shard_data).into_iter().take(limit).collect()
    }

    fn size_bytes(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn partition_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

/// Merge a row into an existing partition using cell-level last-write-wins.
///
/// Binary searches the partition's rows by clustering key. If a row with the
/// same clustering key exists, merges cells (newer timestamp wins per cell).
/// Otherwise inserts the row at the correct sorted position.
pub(crate) fn merge_row_into_partition(partition: &mut Partition, new_row: Row) {
    // Binary search by clustering key
    let pos = partition
        .rows
        .binary_search_by(|existing| existing.clustering.cmp(&new_row.clustering));

    match pos {
        Ok(idx) => {
            // Row with same clustering key exists — merge cells
            let existing_row = &mut partition.rows[idx];

            // Update row-level deletion: newer tombstone wins (LWW).
            if new_row.deletion.marked_for_delete_at > existing_row.deletion.marked_for_delete_at {
                existing_row.deletion = new_row.deletion;
            }

            // Update primary key liveness to the newer timestamp
            if new_row.primary_key_liveness.timestamp > existing_row.primary_key_liveness.timestamp
            {
                existing_row.primary_key_liveness = new_row.primary_key_liveness;
            }

            // Merge cells: for each cell in the new row, apply LWW
            for (col_idx, new_cell) in new_row.cells {
                // Find existing cell with same column index
                let cell_pos = existing_row
                    .cells
                    .binary_search_by_key(&col_idx, |(idx, _)| *idx);

                match cell_pos {
                    Ok(ci) => {
                        // Same column exists — LWW by timestamp
                        if new_cell.timestamp > existing_row.cells[ci].1.timestamp {
                            existing_row.cells[ci].1 = new_cell;
                        }
                    }
                    Err(ci) => {
                        // New column — insert at sorted position
                        existing_row.cells.insert(ci, (col_idx, new_cell));
                    }
                }
            }
        }
        Err(idx) => {
            // No row with this clustering key — insert at sorted position
            partition.rows.insert(idx, new_row);
        }
    }
}

/// K-way merge of pre-sorted partition vectors into a single sorted vector.
///
/// Uses a simple cursor-based approach: maintain an index into each vector,
/// repeatedly pick the minimum-key partition across all cursors, advance
/// that cursor.
fn k_way_merge(mut sources: Vec<Vec<Partition>>) -> Vec<Partition> {
    // Filter out empty sources
    sources.retain(|s| !s.is_empty());

    if sources.is_empty() {
        return Vec::new();
    }
    if sources.len() == 1 {
        return sources.into_iter().next().unwrap();
    }

    let total: usize = sources.iter().map(|s| s.len()).sum();
    let mut result = Vec::with_capacity(total);
    let mut cursors: Vec<usize> = vec![0; sources.len()];

    for _ in 0..total {
        // Find the source with the smallest current element
        let mut min_source = None;
        for (i, cursor) in cursors.iter().enumerate() {
            if *cursor < sources[i].len() {
                match min_source {
                    None => min_source = Some(i),
                    Some(current_min) => {
                        if sources[i][*cursor].key < sources[current_min][cursors[current_min]].key
                        {
                            min_source = Some(i);
                        }
                    }
                }
            }
        }

        if let Some(src) = min_source {
            // We can't move out of the Vec while other cursors reference it,
            // so we clone. This is acceptable for snapshot which is called
            // infrequently (once at flush time).
            result.push(sources[src][cursors[src]].clone());
            cursors[src] += 1;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::ColumnDefinition;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    fn test_schema() -> TableSchema {
        TableSchema {
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
        }
    }

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_row(column_index: u16, value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering: vec![],
            cells: vec![(column_index, CellValue::live(value.to_vec(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    /// Schema with a TimeUUID column at index 0. Used for the fail-loud
    /// guard regression — see specs/in-process/bug-memtable-flush-wedge-
    /// truncated-timeuuid-from-now-function.md.
    fn timeuuid_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "call_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.TimeUUIDType".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    /// Regression for the memtable-flush wedge: an 8-byte cell whose
    /// declared column type is TimeUUID must be rejected at `put` time
    /// (fail-loud), not silently inserted only to fail at flush time.
    /// Before the fix the row would be accepted, durable in the commit
    /// log, and would wedge every subsequent flush.
    #[test]
    fn put_rejects_8_byte_value_in_timeuuid_column() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = timeuuid_schema();
        let key = make_key("pk1");
        // 8-byte payload — exactly the buggy `now()` Timestamp shape.
        let row = make_row(0, &[0u8; 8], 1000);
        let result = mem.put(&key, row, &schema);
        assert!(
            result.is_err(),
            "memtable must reject 8-byte cell in TimeUUID column"
        );
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("16") && err.contains("8"),
            "error must cite expected vs actual length, got: {err}"
        );
    }

    /// Production-observed wedge variant: the malformed bytes are in
    /// `row.clustering` (8 bytes) on a TimeUUID-clustered table. The
    /// fail-loud guard must reject this at `put` time as well — the
    /// per-cell validator alone misses clustering bytes.
    #[test]
    fn put_rejects_8_byte_clustering_in_timeuuid_clustered_table() {
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo};
        let mem = ShardedBTreeMemtable::new(4);
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tool_usage_log".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "call_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.TimeUUIDType".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: Default::default(),
        };
        let key = make_key("pk1");
        let row = Row {
            clustering: vec![0u8; 8], // wrong: TimeUUID needs 16
            cells: vec![],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        let result = mem.put(&key, row, &schema);
        assert!(
            result.is_err(),
            "memtable must reject 8-byte clustering on TimeUUID column"
        );
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("16") && err.contains("8"),
            "error must cite expected vs actual length, got: {err}"
        );
    }

    /// Partition-level DELETE produces a Row with empty clustering, no
    /// cells, and a non-LIVE deletion marker. The strict clustering-shape
    /// guard above must let this through — a partition tombstone has no
    /// clustering by construction.
    ///
    /// Regression for the integration-PR follow-up where the timeuuid
    /// guard was over-broad and rejected `DELETE FROM t WHERE pk = ?` on
    /// any clustered table (CI: Example CQL Scripts /
    /// examples/cql-comprehensive/queries.cql:90).
    #[test]
    fn put_accepts_partition_tombstone_on_clustered_table() {
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo};
        let mem = ShardedBTreeMemtable::new(4);
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "delete_test".to_string(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: Default::default(),
        };
        let key = make_key("pk1");
        let row = Row {
            clustering: vec![],
            cells: vec![],
            deletion: DeletionTime::new(2000, 100),
            primary_key_liveness: LivenessInfo::NONE,
        };
        mem.put(&key, row, &schema)
            .expect("partition tombstone must be accepted on clustered table");
    }

    /// 16-byte TimeUUID cell must be accepted (control case for the
    /// fail-loud guard above).
    #[test]
    fn put_accepts_16_byte_timeuuid_value() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = timeuuid_schema();
        let key = make_key("pk1");
        let row = make_row(0, &[0u8; 16], 1000);
        mem.put(&key, row, &schema).unwrap();
    }

    #[test]
    fn put_then_get_returns_partition() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        let key = make_key("pk1");
        let row = make_row(0, b"hello", 1000);
        mem.put(&key, row, &schema).unwrap();
        let result = mem.get(&key).unwrap();
        assert!(result.is_some());
        let partition = result.unwrap();
        assert_eq!(partition.rows.len(), 1);
        assert_eq!(partition.rows[0].cells.len(), 1);
        assert_eq!(partition.rows[0].cells[0].0, 0);
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"hello".as_slice())
        );
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let mem = ShardedBTreeMemtable::new(4);
        let key = make_key("missing");
        assert!(mem.get(&key).unwrap().is_none());
    }

    #[test]
    fn partition_count_and_size_bytes() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        assert_eq!(mem.partition_count(), 0);
        assert_eq!(mem.size_bytes(), 0);
        mem.put(&make_key("k1"), make_row(0, b"v1", 1000), &schema)
            .unwrap();
        assert_eq!(mem.partition_count(), 1);
        assert!(mem.size_bytes() > 0);
        mem.put(&make_key("k2"), make_row(0, b"v2", 1000), &schema)
            .unwrap();
        assert_eq!(mem.partition_count(), 2);
    }

    #[test]
    fn put_merge_on_write_newer_timestamp_wins() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        let key = make_key("pk1");
        mem.put(&key, make_row(0, b"old", 1000), &schema).unwrap();
        mem.put(&key, make_row(0, b"new", 2000), &schema).unwrap();
        let partition = mem.get(&key).unwrap().unwrap();
        assert_eq!(partition.rows.len(), 1);
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"new".as_slice())
        );
        assert_eq!(partition.rows[0].cells[0].1.timestamp, 2000);
        assert_eq!(mem.partition_count(), 1);
    }

    #[test]
    fn put_merge_on_write_older_timestamp_loses() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        let key = make_key("pk1");
        mem.put(&key, make_row(0, b"new", 2000), &schema).unwrap();
        mem.put(&key, make_row(0, b"old", 1000), &schema).unwrap();
        let partition = mem.get(&key).unwrap().unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"new".as_slice())
        );
        assert_eq!(partition.rows[0].cells[0].1.timestamp, 2000);
    }

    #[test]
    fn put_different_columns_merge() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        let key = make_key("pk1");
        mem.put(&key, make_row(0, b"val0", 1000), &schema).unwrap();
        mem.put(&key, make_row(1, b"val1", 1000), &schema).unwrap();
        let partition = mem.get(&key).unwrap().unwrap();
        assert_eq!(partition.rows[0].cells.len(), 2);
        assert_eq!(partition.rows[0].cells[0].0, 0);
        assert_eq!(partition.rows[0].cells[1].0, 1);
    }

    #[test]
    fn snapshot_returns_token_sorted() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        for i in 0..20 {
            let key = make_key(&format!("key_{i}"));
            mem.put(&key, make_row(0, format!("v{i}").as_bytes(), 1000), &schema)
                .unwrap();
        }
        let snapshot = mem.snapshot();
        assert_eq!(snapshot.len(), 20);
        for window in snapshot.windows(2) {
            assert!(
                window[0].key <= window[1].key,
                "snapshot not in token order: {:?} > {:?}",
                window[0].key.token,
                window[1].key.token
            );
        }
    }

    #[test]
    fn multi_shard_distribution() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        for i in 0..100 {
            let key = make_key(&format!("key_{i}"));
            mem.put(&key, make_row(0, b"v", 1000), &schema).unwrap();
        }
        assert_eq!(mem.partition_count(), 100);
        let non_empty = mem.shards.iter().filter(|s| !s.read().is_empty()).count();
        assert!(
            non_empty >= 2,
            "expected distribution across shards, got {non_empty}"
        );
    }

    #[test]
    fn concurrent_puts_no_data_loss() {
        use std::thread;
        let mem = Arc::new(ShardedBTreeMemtable::new(4));
        let schema = Arc::new(test_schema());
        let num_threads = 8;
        let keys_per_thread = 50;
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let mem = Arc::clone(&mem);
                let schema = Arc::clone(&schema);
                thread::spawn(move || {
                    for k in 0..keys_per_thread {
                        let key = make_key(&format!("t{t}_k{k}"));
                        let row = make_row(0, format!("v{t}_{k}").as_bytes(), 1000 + t as i64);
                        mem.put(&key, row, &schema).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(mem.partition_count(), num_threads * keys_per_thread);
        for t in 0..num_threads {
            for k in 0..keys_per_thread {
                let key = make_key(&format!("t{t}_k{k}"));
                assert!(mem.get(&key).unwrap().is_some(), "missing t{t}_k{k}");
            }
        }
    }

    #[test]
    fn write_contention_counter_starts_at_zero() {
        let mem = ShardedBTreeMemtable::with_default_shards();
        assert_eq!(
            mem.write_contention_count.load(Ordering::Relaxed),
            0,
            "contention counter should start at zero"
        );

        // Write a single key — no contention expected.
        let key = make_key("single");
        let row = make_row(0, b"val", 1000);
        mem.put(&key, row, &test_schema()).unwrap();
        // Counter should still be zero (single-threaded, uncontended).
        assert_eq!(
            mem.write_contention_count.load(Ordering::Relaxed),
            0,
            "single-threaded write should not show contention"
        );
    }
}
