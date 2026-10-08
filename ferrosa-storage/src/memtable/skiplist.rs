//! Memtable backed by `crossbeam_skiplist::SkipMap`.
//!
//! # Concurrency model
//!
//! The **index** is lock-free: `SkipMap` gives concurrent iteration and lookup,
//! so reads and scans (`get`, `range_iter`) never block a writer and never
//! materialize. The **per-partition value** is guarded by a `parking_lot::RwLock`
//! so a write can merge a row into the partition **in place**.
//!
//! # Why an in-place merge (and not CAS-publish a new partition)
//!
//! This memtable previously published each merged partition through an
//! `arc_swap::ArcSwap`, re-writing the whole partition per row:
//!
//! 1. `get_or_insert_with()` inserts an empty partition if the key is new.
//! 2. A CAS loop `clone()`s the whole partition, merges one row, and swaps in a
//!    fresh `Arc`.
//!
//! `arc_swap` offers no `DerefMut` on its guard, so CAS-publishing a changed
//! value *requires* building a new value — the clone is structural, one deep
//! clone plus a fresh `Arc` per write, and it walks every row twice to re-size
//! the partition. That is O(rows-in-partition) per write, so filling one
//! partition is O(N^2). On a live node replaying a commit log it dominated:
//! 52% of wall time in `Arc<Partition>::drop_slow` -> jemalloc free, ~95% CPU for
//! 18+ minutes with no progress and no CQL listener (replay is a barrier).
//! Measured: 1000 writes into a growing partition cost 2,109,500 allocations
//! (~2109/write), and the cost grew as the partition grew.
//!
//! The merge is done in place instead, exactly as `ShardedBTreeMemtable::put`
//! does (`Arc::make_mut` + `merge_row_into_partition`). The lock is held only
//! for the merge of one row; `SkipMap::insert` is lock-free, so distinct
//! partitions never contend, and a same-partition CAS retry loop is replaced by
//! a short critical section. Readers (`get`) clone the `Arc` under a read lock
//! and release it immediately.
//!
//! Atomic merge: `super::sharded::merge_row_into_partition` only mutates the
//! partition after both rows are validated (see
//! `normalize_collection_rows_for_merge`, which parses every blob first and
//! applies second), so a rejected row leaves the partition exactly as it was.
//! The `validate_row_against_schema` guard runs before the lock is taken.
//!
//! See `ferrosa-storage/tests/memtable_write_alloc_bound.rs` for the regression
//! guard: writes to an existing partition must not allocate per write.

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::TableSchema;
use ferrosa_common::Result;
use ferrosa_sstable::types::{DeletionTime, Partition, Row};

use super::Memtable;

/// Memtable using crossbeam-skiplist for the index and a per-partition
/// `RwLock<Arc<Partition>>` for the value (in-place merge; see module docs).
pub struct SkipListMemtable {
    map: SkipMap<DecoratedKey, RwLock<Arc<Partition>>>,
    size: AtomicUsize,
    count: AtomicUsize,
    /// Smallest timestamp of anything accepted by `put` (`i64::MAX` when empty).
    min_ts: AtomicI64,
    /// Every accepted `put`, so a reader can tell in O(1) whether anything was
    /// written since it looked (see [`Memtable::write_epoch`]).
    write_epoch: AtomicUsize,
}

impl SkipListMemtable {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            size: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            min_ts: AtomicI64::new(i64::MAX),
            write_epoch: AtomicUsize::new(0),
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
        // memtable (mirrors the check in `ShardedBTreeMemtable::put`). Runs
        // before any lock is taken, so a rejected row never mutates the table.
        super::validate_row_against_schema(&row, schema)?;
        // Lowered before the row is visible; see `ShardedBTreeMemtable::put`.
        self.min_ts
            .fetch_min(super::row_min_timestamp(&row), Ordering::SeqCst);

        // Take the index entry. A write to a partition already in the table
        // looks the key up BY REFERENCE: `SkipMap`'s insert path needs an owned
        // key, so calling `get_or_insert_with` unconditionally would clone the
        // key, and `DecoratedKey`'s clone is a heap allocation (its key bytes
        // are a `Vec`). That was the last allocation on the hot path. Only the
        // first write to a key pays for the owned key the insert requires;
        // `get_or_insert_with` still resolves a concurrent first-insert race
        // atomically (its closure runs at most once per key). No side effects in
        // the closure.
        let entry = match self.map.get(key) {
            Some(existing) => existing,
            None => self.map.get_or_insert_with(key.clone(), || {
                RwLock::new(Arc::new(Partition {
                    key: key.clone(),
                    deletion: DeletionTime::LIVE,
                    static_row: None,
                    rows: vec![],
                }))
            }),
        };

        // Merge the row IN PLACE under the per-partition write lock. `Arc::make_mut`
        // reuses the partition's buffer when this memtable holds the only `Arc`
        // (the common case: readers clone the `Arc` only briefly), so an
        // existing partition is not re-written per row. Holding only this
        // partition's lock — never the map's — keeps distinct partitions
        // independent and contention short. This is what removes the O(N)
        // clone-per-write that made filling a partition O(N^2); see module docs.
        let mut guard = entry.value().write();
        let was_empty = guard.rows.is_empty();
        let old_size = estimate_partition_size(&guard);

        let partition = Arc::make_mut(&mut guard);
        super::sharded::merge_row_into_partition(partition, row, schema)?;

        // Record the write only after it is in the partition, so a rejected row
        // cannot make a reader believe something landed (I-1: the epoch must be
        // exact in the conservative direction — never skip a real late write).
        self.write_epoch.fetch_add(1, Ordering::Release);

        let new_size = estimate_partition_size(partition);
        if new_size >= old_size {
            self.size.fetch_add(new_size - old_size, Ordering::Relaxed);
        } else {
            self.size.fetch_sub(old_size - new_size, Ordering::Relaxed);
        }
        if was_empty {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn get(&self, key: &DecoratedKey) -> Result<Option<Arc<Partition>>> {
        // Clone the `Arc` under a brief read lock, then release it. Cheap (one
        // atomic refcount bump) and never deep-copies the partition.
        Ok(self
            .map
            .get(key)
            .map(|entry| Arc::clone(&entry.value().read())))
    }

    fn snapshot(&self) -> Vec<Partition> {
        // SkipMap iterates in key order (DecoratedKey: token then key bytes).
        self.map
            .iter()
            .map(|entry| (**entry.value().read()).clone())
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
            .map(|entry| (**entry.value().read()).clone())
            .collect()
    }

    fn range_iter<'a>(
        &'a self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> Box<dyn Iterator<Item = Arc<Partition>> + Send + 'a> {
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
                // Hand out the stored `Arc<Partition>` (one atomic refcount
                // bump per entry) — never deep-clone on a read-only scan.
                .map(|entry| Arc::clone(&entry.value().read())),
        )
    }

    fn size_bytes(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    fn partition_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Borrowed read-only scan: the fix for the t512 regression.
    ///
    /// `range_iter` hands the consumer an owned `Arc<Partition>`. A consumer
    /// that *holds* it (the fulltext build, the flush late-writer drain) raises
    /// the partition's strong count to > 1, so the next `put` on that partition
    /// finds `Arc::make_mut` with refcount > 1 and deep-clones the whole
    /// partition before merging — O(rows-in-partition) per write, i.e. the
    /// O(N^2) fill pathology `e440b60f` removed. Measured by
    /// `tests/refcount_cow_probe.rs`: 1 write = 1210 allocations while a reader
    /// holds the `Arc`, vs 3 when it does not.
    ///
    /// Here the guard is held for the callback and the value is borrowed, never
    /// cloned out, so the memtable stays the sole owner of each partition and
    /// concurrent writes keep merging in place.
    fn for_each_partition(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        f: &mut dyn FnMut(&Partition),
    ) {
        // Bounds are compared, not cloned: `DecoratedKey`'s clone allocates.
        let in_range =
            |key: &DecoratedKey| start.is_none_or(|s| key >= s) && end.is_none_or(|e| key <= e);
        for entry in self.map.iter() {
            if !in_range(entry.key()) {
                continue;
            }
            // Hold this partition's read lock across the callback. The `Arc` is
            // borrowed — no clone, so no refcount inflation and no COW for a
            // concurrent writer (I-2). The lock is released when this iteration
            // step ends, so a writer waits at most one callback (I-5).
            let guard = entry.value().read();
            f(&guard);
        }
    }

    /// Bounded-hold variant for callbacks that do real work per row.
    ///
    /// Clones the partition under the read guard, **releases it**, then calls
    /// `f` with the owned copy. A concurrent writer to that partition therefore
    /// waits for one memcpy rather than for the whole analysis — which is what
    /// the fulltext index build needs (it analyzes text and folds a
    /// term-frequency map per row). The `Arc` is never cloned out, so the
    /// memtable keeps sole ownership and writes still merge in place.
    fn for_each_partition_cloned(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        f: &mut dyn FnMut(&Partition),
    ) {
        let in_range =
            |key: &DecoratedKey| start.is_none_or(|s| key >= s) && end.is_none_or(|e| key <= e);
        for entry in self.map.iter() {
            if !in_range(entry.key()) {
                continue;
            }
            // Scope the guard so it is dropped before `f` runs.
            //
            // `(**guard).clone()` — a DEEP `Partition` clone, deliberately not
            // `guard.clone()`. `entry.value()` is `RwLock<Arc<Partition>>`, so
            // `guard.clone()` would resolve through `Deref` to `Arc::clone`: a
            // refcount bump, not a copy. That would leave the strong count at 2
            // for the duration of `f`, so a concurrent `put` would find
            // `Arc::make_mut` with refcount > 1 and copy-on-write the whole
            // partition — re-introducing the very regression this exists to fix,
            // and doing it silently (an Arc clone is correct-looking and cheap).
            let owned = {
                let guard = entry.value().read();
                (**guard).clone()
            };
            debug_assert_eq!(
                std::sync::Arc::strong_count(&entry.value().read()),
                1,
                "the memtable must remain the sole owner of a partition across a \
                 read-only scan, or concurrent writes stop merging in place"
            );
            f(&owned);
        }
    }

    /// Every accepted `put` on this memtable; see [`Memtable::write_epoch`].
    fn write_epoch(&self) -> u64 {
        self.write_epoch.load(Ordering::Acquire) as u64
    }

    fn min_timestamp(&self) -> i64 {
        self.min_ts.load(Ordering::SeqCst)
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

    /// The read path must NOT deep-clone live row data out of the memtable on a
    /// scan. `range_iter` hands out the *stored* `Arc<Partition>` (one atomic
    /// refcount bump); it never allocates a fresh `Arc` wrapping a cloned
    /// `Partition`. Regression guard for the #1 read-path lever: the memtable
    /// `Partition`/`Vec<Row>` clone measured ~7.5 % of read-path CPU.
    ///
    /// Negative control: if `range_iter` goes back to yielding `Partition` by
    /// value (deep clone), the addresses in `second` differ from `first` and the
    /// assertion fails.
    #[test]
    fn range_iter_does_not_deep_clone_partition_bodies() {
        let mem = SkipListMemtable::new();
        let schema = test_schema();
        for i in 0..20 {
            let key = make_key(&format!("key_{i:02}"));
            mem.put(&key, make_row(0, format!("v{i}").as_bytes(), 1000), &schema)
                .unwrap();
        }

        // `Arc::as_ptr` is the address of the shared allocation. If the scan
        // yields the stored Arc, two independent scans observe the SAME
        // addresses; a deep-clone-and-rewrap allocates fresh ones each scan.
        // Both scans' `Arc`s are held alive, so the allocator cannot hand the
        // second scan the addresses the first dropped (which would make a
        // deep-cloning implementation pass spuriously).
        let first_arcs: Vec<Arc<Partition>> = mem.range_iter(None, None).collect();
        let second_arcs: Vec<Arc<Partition>> = mem.range_iter(None, None).collect();
        let first: Vec<*const Partition> = first_arcs.iter().map(Arc::as_ptr).collect();
        let second: Vec<*const Partition> = second_arcs.iter().map(Arc::as_ptr).collect();

        assert_eq!(first.len(), 20);
        assert_eq!(
            first, second,
            "range_iter allocated a fresh Arc per item (addresses changed between \
             scans): it deep-cloned the partition body instead of handing out the \
             stored Arc<Partition>"
        );
        // And both scans really point INTO the memtable's shared storage: the
        // refcount of every entry is >= 2 while both scans hold it.
        assert!(
            second_arcs.iter().all(|p| Arc::strong_count(p) >= 2),
            "a scanned Arc had refcount 1 while two scans held it — it was not \
             the stored Arc"
        );
    }

    /// The read-path consumers that only *read* partitions (the fulltext index
    /// build and the flush late-writer check) scan through `range_iter` rather
    /// than `snapshot`. That substitution is only sound while the two agree
    /// exactly, so pin it: same partitions, same order, same content. A
    /// divergence would silently drop or reorder a user's partitions.
    #[test]
    fn range_iter_and_snapshot_agree_exactly() {
        let mem = SkipListMemtable::new();
        let schema = test_schema();
        for i in 0..20 {
            let key = make_key(&format!("key_{i:02}"));
            mem.put(&key, make_row(0, format!("v{i}").as_bytes(), 1000), &schema)
                .unwrap();
        }

        let snapshot = mem.snapshot();
        let scanned: Vec<Arc<Partition>> = mem.range_iter(None, None).collect();
        assert_eq!(snapshot.len(), scanned.len());
        for (from_snapshot, from_scan) in snapshot.iter().zip(scanned.iter()) {
            // Same key order...
            assert_eq!(from_snapshot.key, from_scan.key);
            // ...and the same bytes, so a reader cannot tell them apart.
            assert_eq!(from_snapshot.rows.len(), from_scan.rows.len());
            for (a, b) in from_snapshot.rows.iter().zip(from_scan.rows.iter()) {
                assert_eq!(a.clustering, b.clustering);
                assert_eq!(a.cells, b.cells);
            }
        }
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
