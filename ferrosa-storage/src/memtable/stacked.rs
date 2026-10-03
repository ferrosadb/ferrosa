//! Module: two swapped-out memtables read as one.
//! Responsibility: keep the rows of a flush that failed after its memtable swap
//!   readable and flushable when the next flush swaps out another memtable.
//! Correctness: every read merges both layers with the normal read-path merge,
//!   so the result equals what the two memtables hold together; the stack is
//!   read-only, because a swapped-out memtable takes no writes.
//! Last revised: 2026-10-03
//! Last changed: New module (t_7681b32b). A store view has ONE `flushing` slot;
//!   a second flush after a failed one overwrote it, and the failed memtable's
//!   rows vanished from reads until a restart replayed the commit log.

use std::iter::Peekable;
use std::sync::Arc;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::{Error, Result, TableSchema};
use ferrosa_sstable::types::{Partition, Row};

use super::Memtable;

/// `newer` stacked over `older`, both already swapped out of the active view.
///
/// Built only by the flush swap, when the `flushing` slot still holds the
/// memtable of a flush that did not finish. Nesting is bounded by the number of
/// consecutive failed flushes, each of which is reported by the flush
/// supervisor in the `ferrosa` binary.
pub struct StackedMemtable {
    newer: Arc<dyn Memtable>,
    older: Arc<dyn Memtable>,
}

impl StackedMemtable {
    pub fn stack(newer: Arc<dyn Memtable>, older: Arc<dyn Memtable>) -> Arc<dyn Memtable> {
        Arc::new(Self { newer, older })
    }
}

impl Memtable for StackedMemtable {
    fn put(&self, key: &DecoratedKey, _row: Row, schema: &TableSchema) -> Result<()> {
        Err(Error::InvalidFormat(format!(
            "write to {}.{} key {key:?} reached a swapped-out (stacked) memtable; \
             writes must go to the active memtable",
            schema.keyspace, schema.table
        )))
    }

    fn get(&self, key: &DecoratedKey) -> Result<Option<Arc<Partition>>> {
        match (self.newer.get(key)?, self.older.get(key)?) {
            (Some(newer), Some(older)) => Ok(Some(Arc::new(crate::merge::merge_partitions(vec![
                (*newer).clone(),
                (*older).clone(),
            ])))),
            (newer, older) => Ok(newer.or(older)),
        }
    }

    fn snapshot(&self) -> Vec<Partition> {
        self.range_iter(None, None).collect()
    }

    fn snapshot_range_limited(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
    ) -> Vec<Partition> {
        self.range_iter(start, end).take(limit).collect()
    }

    fn range_iter<'a>(
        &'a self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> Box<dyn Iterator<Item = Partition> + Send + 'a> {
        Box::new(MergeByKey {
            newer: self.newer.range_iter(start, end).peekable(),
            older: self.older.range_iter(start, end).peekable(),
        })
    }

    fn size_bytes(&self) -> usize {
        self.newer
            .size_bytes()
            .saturating_add(self.older.size_bytes())
    }

    /// An upper bound: a partition present in both layers counts twice.
    fn partition_count(&self) -> usize {
        self.newer
            .partition_count()
            .saturating_add(self.older.partition_count())
    }

    fn min_timestamp(&self) -> i64 {
        self.newer.min_timestamp().min(self.older.min_timestamp())
    }
}

/// Merge-join of two key-ordered partition streams; equal keys are merged.
struct MergeByKey<A: Iterator<Item = Partition>, B: Iterator<Item = Partition>> {
    newer: Peekable<A>,
    older: Peekable<B>,
}

impl<A, B> Iterator for MergeByKey<A, B>
where
    A: Iterator<Item = Partition>,
    B: Iterator<Item = Partition>,
{
    type Item = Partition;

    fn next(&mut self) -> Option<Partition> {
        let order = match (self.newer.peek(), self.older.peek()) {
            (None, None) => return None,
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some(newer), Some(older)) => newer.key.cmp(&older.key),
        };
        match order {
            std::cmp::Ordering::Less => self.newer.next(),
            std::cmp::Ordering::Greater => self.older.next(),
            std::cmp::Ordering::Equal => {
                let newer = self.newer.next()?;
                let older = self.older.next()?;
                Some(crate::merge::merge_partitions(vec![newer, older]))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::sharded::ShardedBTreeMemtable;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::PartitionKey;
    use ferrosa_common::schema::ColumnDefinition;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    fn schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
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
            extensions: Default::default(),
        }
    }

    fn key(name: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec()))
    }

    fn row(ck: i32, value: &str, timestamp: i64) -> Row {
        Row {
            clustering: ck.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(value.as_bytes().to_vec(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    fn memtable(rows: &[(&str, i32, &str, i64)]) -> Arc<dyn Memtable> {
        let memtable: Arc<dyn Memtable> = Arc::new(ShardedBTreeMemtable::with_default_shards());
        for (name, ck, value, timestamp) in rows {
            memtable
                .put(&key(name), row(*ck, value, *timestamp), &schema())
                .expect("put");
        }
        memtable
    }

    fn value_of(partition: &Partition, ck: i32) -> Vec<u8> {
        let row = partition
            .rows
            .iter()
            .find(|row| row.clustering == ck.to_be_bytes())
            .unwrap_or_else(|| panic!("clustering {ck} missing"));
        row.cells[0].1.value.clone().expect("live value")
    }

    #[test]
    fn both_layers_are_visible_and_shared_partitions_merge_last_write_wins() {
        let older = memtable(&[("a", 1, "old-a1", 10), ("shared", 1, "old", 10)]);
        let newer = memtable(&[("b", 1, "new-b1", 20), ("shared", 1, "new", 20)]);
        newer
            .put(&key("shared"), row(2, "new-only", 20), &schema())
            .expect("put");
        let stacked = StackedMemtable::stack(newer, older);

        let shared = stacked.get(&key("shared")).expect("get").expect("present");
        assert_eq!(value_of(&shared, 1), b"new", "the newer cell wins");
        assert_eq!(value_of(&shared, 2), b"new-only");
        assert!(stacked.get(&key("a")).expect("get").is_some());
        assert!(stacked.get(&key("b")).expect("get").is_some());
        assert!(stacked.get(&key("absent")).expect("get").is_none());

        let snapshot = stacked.snapshot();
        assert_eq!(
            snapshot.len(),
            3,
            "one partition per key, shared merged once"
        );
        assert!(
            snapshot.windows(2).all(|pair| pair[0].key < pair[1].key),
            "the snapshot stays key-ordered"
        );
    }

    #[test]
    fn a_stacked_memtable_refuses_writes() {
        let stacked = StackedMemtable::stack(memtable(&[]), memtable(&[]));
        let refused = stacked.put(&key("k"), row(1, "v", 1), &schema());
        assert!(
            refused.is_err(),
            "a swapped-out memtable must not take writes"
        );
    }
}
