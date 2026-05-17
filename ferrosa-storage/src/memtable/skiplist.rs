//! Lock-free memtable backed by `crossbeam_skiplist::SkipMap`.
//!
//! All operations are lock-free: reads use `SkipMap::get()` + `ArcSwap::load()`,
//! writes use `SkipMap::get_or_insert()` + CAS loop on `ArcSwap`.
//!
//! # Merge-on-write protocol
//!
//! 1. `get_or_insert_with()` atomically inserts an empty partition if key is new.
//! 2. CAS loop on the per-partition `ArcSwap<Partition>` merges the row.
//! 3. If another thread updates the same partition concurrently, the CAS retries.
//!
//! This eliminates all lock contention — different partitions never interact,
//! and same-partition contention is handled by the non-blocking CAS loop.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use crossbeam_skiplist::SkipMap;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::TableSchema;
use ferrosa_common::Result;
use ferrosa_sstable::types::{DeletionTime, Partition, Row};

use super::Memtable;

/// Lock-free memtable using crossbeam-skiplist.
pub struct SkipListMemtable {
    map: SkipMap<DecoratedKey, ArcSwap<Partition>>,
    size: AtomicUsize,
    count: AtomicUsize,
}

impl SkipListMemtable {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            size: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
        }
    }
}

impl Default for SkipListMemtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable for SkipListMemtable {
    fn put(&self, key: &DecoratedKey, row: Row, schema: &TableSchema) -> Result<()> {
        // Fail-loud guard: reject mis-sized cells before they reach the
        // memtable (mirrors the check in `ShardedBTreeMemtable::put`).
        super::validate_row_against_schema(&row, schema)?;
        // Atomically insert an empty partition if the key is new.
        // No side effects in the closure — count is tracked via was_empty
        // inside the CAS loop, which serializes concurrent writers.
        let entry = self.map.get_or_insert_with(key.clone(), || {
            ArcSwap::new(Arc::new(Partition {
                key: key.clone(),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![],
            }))
        });

        // CAS loop to merge the row into the partition.
        // load_full() returns Arc directly (not a Guard), so Arc::ptr_eq works.
        loop {
            let current = entry.value().load_full();
            let was_empty = current.rows.is_empty();
            let old_size = estimate_partition_size(&current);

            let mut merged = (*current).clone();
            super::sharded::merge_row_into_partition(&mut merged, row.clone());
            let new_size = estimate_partition_size(&merged);

            let new_arc = Arc::new(merged);
            let prev = entry.value().compare_and_swap(&current, new_arc);
            if Arc::ptr_eq(&prev, &current) {
                // CAS succeeded. The CAS serializes concurrent writers, so
                // exactly one thread sees was_empty=true for a new partition.
                if was_empty {
                    self.count.fetch_add(1, Ordering::Relaxed);
                }
                if new_size >= old_size {
                    self.size.fetch_add(new_size - old_size, Ordering::Relaxed);
                } else {
                    self.size.fetch_sub(old_size - new_size, Ordering::Relaxed);
                }
                return Ok(());
            }
            // CAS failed — another thread updated; retry with their version.
        }
    }

    fn get(&self, key: &DecoratedKey) -> Result<Option<Arc<Partition>>> {
        Ok(self.map.get(key).map(|entry| {
            let guard = entry.value().load();
            Arc::clone(&guard)
        }))
    }

    fn snapshot(&self) -> Vec<Partition> {
        // SkipMap iterates in key order (DecoratedKey: token then key bytes).
        self.map
            .iter()
            .map(|entry| {
                let guard = entry.value().load();
                (**guard).clone()
            })
            .collect()
    }

    fn snapshot_range_limited(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
    ) -> Vec<Partition> {
        self.map
            .iter()
            .filter(|entry| {
                let key = entry.key();
                start.is_none_or(|s| key >= s) && end.is_none_or(|e| key <= e)
            })
            .take(limit)
            .map(|entry| {
                let guard = entry.value().load();
                (**guard).clone()
            })
            .collect()
    }

    fn range_iter<'a>(
        &'a self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> Box<dyn Iterator<Item = Partition> + Send + 'a> {
        // Clone the bounds so the returned iterator owns its filter
        // predicate state — `&DecoratedKey` doesn't live long enough
        // for the iterator's lifetime.
        let start = start.cloned();
        let end = end.cloned();
        Box::new(
            self.map
                .iter()
                .filter(move |entry| {
                    let key = entry.key();
                    start.as_ref().is_none_or(|s| key >= s) && end.as_ref().is_none_or(|e| key <= e)
                })
                .map(|entry| {
                    let guard = entry.value().load();
                    (**guard).clone()
                }),
        )
    }

    fn size_bytes(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn partition_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

fn estimate_partition_size(partition: &Partition) -> usize {
    let mut size = std::mem::size_of::<Partition>();
    size += partition.key.key.as_bytes().len();
    if let Some(ref sr) = partition.static_row {
        size += estimate_row_size(sr);
    }
    for row in &partition.rows {
        size += estimate_row_size(row);
    }
    size
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
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

    #[test]
    fn put_then_get() {
        let mem = SkipListMemtable::new();
        let schema = test_schema();
        let key = make_key("pk1");
        mem.put(&key, make_row(0, b"hello", 1000), &schema).unwrap();
        let result = mem.get(&key).unwrap();
        assert!(result.is_some());
        let partition = result.unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"hello".as_slice())
        );
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let mem = SkipListMemtable::new();
        assert!(mem.get(&make_key("missing")).unwrap().is_none());
    }

    #[test]
    fn merge_on_write_newer_wins() {
        let mem = SkipListMemtable::new();
        let schema = test_schema();
        let key = make_key("pk1");
        mem.put(&key, make_row(0, b"old", 1000), &schema).unwrap();
        mem.put(&key, make_row(0, b"new", 2000), &schema).unwrap();
        let partition = mem.get(&key).unwrap().unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"new".as_slice())
        );
        assert_eq!(partition.rows[0].cells[0].1.timestamp, 2000);
        assert_eq!(mem.partition_count(), 1);
    }

    #[test]
    fn merge_on_write_older_loses() {
        let mem = SkipListMemtable::new();
        let schema = test_schema();
        let key = make_key("pk1");
        mem.put(&key, make_row(0, b"new", 2000), &schema).unwrap();
        mem.put(&key, make_row(0, b"old", 1000), &schema).unwrap();
        let partition = mem.get(&key).unwrap().unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"new".as_slice())
        );
    }

    #[test]
    fn different_columns_merge() {
        let mem = SkipListMemtable::new();
        let schema = test_schema();
        let key = make_key("pk1");
        mem.put(&key, make_row(0, b"v0", 1000), &schema).unwrap();
        mem.put(&key, make_row(1, b"v1", 1000), &schema).unwrap();
        let partition = mem.get(&key).unwrap().unwrap();
        assert_eq!(partition.rows[0].cells.len(), 2);
    }

    #[test]
    fn snapshot_returns_sorted() {
        let mem = SkipListMemtable::new();
        let schema = test_schema();
        for i in 0..20 {
            let key = make_key(&format!("key_{i}"));
            mem.put(&key, make_row(0, format!("v{i}").as_bytes(), 1000), &schema)
                .unwrap();
        }
        let snapshot = mem.snapshot();
        assert_eq!(snapshot.len(), 20);
        for window in snapshot.windows(2) {
            assert!(window[0].key <= window[1].key);
        }
    }

    /// ADR-020 lazy range_iter contract for the Skiplist memtable.
    /// Iterates without materializing the full Vec; partitions come
    /// out in token order; honors start/end bounds.
    #[test]
    fn range_iter_yields_sorted_and_honors_bounds() {
        let mem = SkipListMemtable::new();
        let schema = test_schema();
        for i in 0..20 {
            let key = make_key(&format!("key_{i:02}"));
            mem.put(&key, make_row(0, format!("v{i}").as_bytes(), 1000), &schema)
                .unwrap();
        }
        // Unbounded → all 20.
        let all: Vec<_> = mem.range_iter(None, None).collect();
        assert_eq!(all.len(), 20);
        for w in all.windows(2) {
            assert!(w[0].key <= w[1].key);
        }
        // Bounded — call .take(5) to prove the iterator stops pulling
        // after the consumer is done (laziness in action).
        let first_5: Vec<_> = mem.range_iter(None, None).take(5).collect();
        assert_eq!(first_5.len(), 5);
    }

    #[test]
    fn partition_count_and_size() {
        let mem = SkipListMemtable::new();
        let schema = test_schema();
        assert_eq!(mem.partition_count(), 0);
        assert_eq!(mem.size_bytes(), 0);
        mem.put(&make_key("k1"), make_row(0, b"v1", 1000), &schema)
            .unwrap();
        assert_eq!(mem.partition_count(), 1);
        assert!(mem.size_bytes() > 0);
    }

    #[test]
    fn concurrent_puts_no_data_loss() {
        use std::thread;
        let mem = Arc::new(SkipListMemtable::new());
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
                assert!(mem.get(&make_key(&format!("t{t}_k{k}"))).unwrap().is_some());
            }
        }
    }
}
