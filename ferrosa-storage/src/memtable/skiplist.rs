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
    fn put(&self, key: &DecoratedKey, row: Row, _schema: &TableSchema) -> Result<()> {
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
}
