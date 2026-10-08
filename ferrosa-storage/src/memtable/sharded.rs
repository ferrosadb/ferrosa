//! Sharded BTreeMap memtable implementation.
//!
//! Uses 64 shards (configurable) of `parking_lot::RwLock<BTreeMap>` to
//! distribute write contention. Shard selection: `key.token.0 as u64 % num_shards`.
//!
//! This is the initial implementation behind the `Memtable` trait. The trait
//! enables swapping to a lock-free structure (crossbeam-skiplist, Okasaki-style
//! persistent structures) without changing consumer code.
//!
//! Correctness: `range_iter` yields every in-range partition exactly once in
//! global token order while retaining only `O(num_shards)` memory (it re-seeks
//! each shard under a short-lived lock instead of materializing the range);
//! `snapshot` returns the same partitions owned, for the flush, which mutates
//! them in place. A bounded `range_iter` agrees with `snapshot` filtered to the
//! same bounds, and an inverted bound yields empty rather than panicking.
//! Last revised: 2026-10-08
//! Last changed: Made `range_iter` lazy — it no longer pre-collects the range.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::TableSchema;
use ferrosa_common::Result;
use ferrosa_sstable::types::{DeletionTime, Partition, Row};

use super::Memtable;

/// Default number of shards. 64 gives good contention distribution on
/// modern multi-core systems without excessive overhead.
pub const DEFAULT_NUM_SHARDS: usize = 64;

/// Process-wide override for the default shard count. Set once at
/// `StorageEngine` construction from `StorageEngineConfig.memtable_num_shards`
/// (which honors `FERROSA_MEMTABLE_NUM_SHARDS` and falls back to 64).
/// `with_default_shards` reads this; explicit `new(num_shards)` callers
/// (mostly tests that want low-shard counts) ignore it.
static CONFIGURED_NUM_SHARDS: AtomicUsize = AtomicUsize::new(DEFAULT_NUM_SHARDS);

/// Set the process-wide default shard count. Called once at
/// `StorageEngine` construction. Idempotent; later calls overwrite,
/// but in practice the engine is constructed once per process.
/// Values of 0 are ignored so a misconfigured env var never wedges
/// every new memtable; the previous value (default 64) wins.
pub fn set_configured_num_shards(n: usize) {
    if n == 0 {
        return;
    }
    CONFIGURED_NUM_SHARDS.store(n, Ordering::Relaxed);
}

/// Read the process-wide default shard count. `with_default_shards`
/// uses this so a runtime config change is picked up by every memtable
/// allocated after the override is set.
pub fn configured_num_shards() -> usize {
    CONFIGURED_NUM_SHARDS.load(Ordering::Relaxed)
}

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
    /// Smallest timestamp of anything accepted by `put` (`i64::MAX` when empty).
    /// Lowered BEFORE the row is stored, so it never reads higher than the data.
    min_ts: AtomicI64,
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
            min_ts: AtomicI64::new(i64::MAX),
        }
    }

    /// Create a new sharded memtable with the configured default shard
    /// count (`FERROSA_MEMTABLE_NUM_SHARDS`, falling back to
    /// `DEFAULT_NUM_SHARDS`). Use `new(num_shards)` to override.
    pub fn with_default_shards() -> Self {
        Self::new(configured_num_shards())
    }

    /// Determine which shard a key belongs to.
    fn shard_index(&self, key: &DecoratedKey) -> usize {
        (key.token.0 as u64 % self.num_shards as u64) as usize
    }

    /// Seek one shard's first entry at or after `start` (inclusive), bounded by
    /// `end`. `None` bounds are unbounded. An inverted range (`start > end`)
    /// yields `None` rather than panicking: `BTreeMap::range` panics on a
    /// reversed range, and a caller that computes an empty token range must get
    /// an empty scan, not a panic on the storage thread.
    ///
    /// Holds the shard's read lock only for the duration of the seek — a
    /// `BTreeMap` range scan touching `O(log n)` nodes — never across the
    /// consumer's processing. Used by [`ShardedRangeIter`] to keep a scan's
    /// retained memory at `O(num_shards)` instead of materializing the range.
    fn seek_shard_start(
        &self,
        src: usize,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> Option<Arc<Partition>> {
        use std::ops::Bound;
        if let (Some(s), Some(e)) = (start, end) {
            if s > e {
                return None;
            }
        }
        let shard = self.shards[src].read();
        let lo = match start {
            Some(s) => Bound::Included(s),
            None => Bound::Unbounded,
        };
        let hi = match end {
            Some(e) => Bound::Included(e),
            None => Bound::Unbounded,
        };
        shard.range((lo, hi)).next().map(|(_, arc)| Arc::clone(arc))
    }

    /// Seek one shard's next entry strictly after `after`, bounded by `end` —
    /// the resume step of a lazy merge. See [`Self::seek_shard_start`].
    fn seek_shard(
        &self,
        src: usize,
        after: &DecoratedKey,
        end: Option<&DecoratedKey>,
    ) -> Option<Arc<Partition>> {
        use std::ops::Bound;
        if end.is_some_and(|e| after > e) {
            return None;
        }
        let shard = self.shards[src].read();
        let hi = match end {
            Some(e) => Bound::Included(e),
            None => Bound::Unbounded,
        };
        shard
            .range((Bound::Excluded(after), hi))
            .next()
            .map(|(_, arc)| Arc::clone(arc))
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
            if let Some(ref p) = cell.path {
                size += p.len();
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
        // Record the timestamp before the row becomes visible (SeqCst pairs with
        // `min_timestamp`), so a compaction that reads the minimum and then sees the
        // data can never have seen a higher minimum than the data holds.
        self.min_ts
            .fetch_min(super::row_min_timestamp(&row), Ordering::SeqCst);
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
            merge_row_into_partition(partition, row, schema)?;

            let new_size = Self::estimate_partition_size(partition);
            // Update size delta (could be negative if overwrite with smaller value,
            // but we use wrapping arithmetic via AtomicUsize)
            if new_size >= old_size {
                self.size.fetch_add(new_size - old_size, Ordering::Relaxed);
            } else {
                self.size.fetch_sub(old_size - new_size, Ordering::Relaxed);
            }
        } else {
            // New partition. A partition-tombstone marker (empty clustering,
            // no cells, non-LIVE deletion) is a partition-level DELETE: lift it
            // into `Partition::deletion` instead of storing a phantom row, so a
            // subsequent read suppresses every clustered row at or below the
            // tombstone timestamp (see `super::is_partition_tombstone`).
            // The same merge as an existing partition, so a static-row marker
            // lands in `Partition::static_row` here too.
            let mut partition = Partition {
                key: key.clone(),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: Vec::with_capacity(1),
            };
            merge_row_into_partition(&mut partition, row, schema)?;
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

    fn range_iter<'a>(
        &'a self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> Box<dyn Iterator<Item = Arc<Partition>> + Send + 'a> {
        // Lazy k-way merge: one heap entry per shard, no per-partition collect.
        // The iterator re-seeks each shard under a short-lived read lock when it
        // needs that shard's next key. Materializing the in-range `Arc`s up front
        // (one `Vec` per shard) retained `O(partitions_in_range)` pointers for the
        // scan's whole lifetime; the trait contract is `O(num_shards)`.
        Box::new(ShardedRangeIter::new(self, start.cloned(), end.cloned()))
    }

    fn size_bytes(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn partition_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    fn min_timestamp(&self) -> i64 {
        self.min_ts.load(Ordering::SeqCst)
    }
}

/// Merge a row into an existing partition using cell-level last-write-wins.
///
/// Binary searches the partition's rows by clustering key. If a row with the
/// same clustering key exists, merges cells (newer timestamp wins per cell).
/// Otherwise inserts the row at the correct sorted position.
pub(crate) fn merge_row_into_partition(
    partition: &mut Partition,
    new_row: Row,
    schema: &TableSchema,
) -> Result<()> {
    // A partition-tombstone marker is a partition-level DELETE: merge it into
    // `Partition::deletion` (newer tombstone wins, LWW) rather than storing it
    // as a clustered row. Otherwise it would sit in `rows` as a phantom
    // empty-clustering row and suppress nothing, silently dropping the delete.
    if super::is_partition_tombstone(&new_row) {
        if new_row.deletion.marked_for_delete_at > partition.deletion.marked_for_delete_at {
            partition.deletion = new_row.deletion;
        }
        return Ok(());
    }

    // A static-row marker (empty clustering + cells on a clustered table) is
    // the partition's static row: merge it into `Partition::static_row` with
    // the same cell-level rules as a clustered row, never into `rows`.
    if super::is_static_row_marker(&new_row, schema) {
        match partition.static_row.as_mut() {
            Some(existing) => merge_into_existing_row(existing, new_row, schema)?,
            None => partition.static_row = Some(new_row),
        }
        return Ok(());
    }

    // A clustered row may carry static cells (CQL writes them with the row).
    // A static column has one value per partition, so lift them into the
    // static row here, exactly as the flush does (crate::ordinal_space), and
    // every read before and after a flush sees the same partition.
    let new_row = lift_static_cells(partition, new_row, schema)?;

    // Binary search by clustering key
    let pos = partition
        .rows
        .binary_search_by(|existing| existing.clustering.cmp(&new_row.clustering));

    match pos {
        // Row with same clustering key exists — merge cells
        Ok(idx) => merge_into_existing_row(&mut partition.rows[idx], new_row, schema)?,
        Err(idx) => {
            // No row with this clustering key — insert at sorted position
            partition.rows.insert(idx, new_row);
        }
    }
    Ok(())
}

/// On a clustered table, move `row`'s static cells (flat ordinals
/// `0..static_columns.len()`) into `partition`'s static row and return the
/// row without them. The row itself stays, with its clustering, liveness and
/// regular cells. Without clustering columns a partition has one row and no
/// separate static row (Cassandra refuses statics there); the flush still
/// moves such cells into the SSTable static row, and readers overlay it.
fn lift_static_cells(partition: &mut Partition, mut row: Row, schema: &TableSchema) -> Result<Row> {
    let static_count = schema.static_columns.len();
    if static_count == 0
        || schema.clustering_columns.is_empty()
        || !row
            .cells
            .iter()
            .any(|(idx, _)| usize::from(*idx) < static_count)
    {
        return Ok(row);
    }
    let (statics, regulars): (Vec<_>, Vec<_>) = std::mem::take(&mut row.cells)
        .into_iter()
        .partition(|(idx, _)| usize::from(*idx) < static_count);
    row.cells = regulars;
    let marker = Row {
        clustering: Vec::new(),
        cells: statics,
        deletion: DeletionTime::LIVE,
        primary_key_liveness: ferrosa_sstable::types::LivenessInfo::NONE,
    };
    match partition.static_row.as_mut() {
        Some(existing) => merge_into_existing_row(existing, marker, schema)?,
        None => partition.static_row = Some(marker),
    }
    Ok(row)
}

/// Merge `new_row` into `existing_row` (same clustering, or both the static
/// row) using row-level LWW for deletion and liveness and cell-level LWW for
/// cells.
fn merge_into_existing_row(
    existing_row: &mut Row,
    mut new_row: Row,
    schema: &TableSchema,
) -> Result<()> {
    super::normalize_collection_rows_for_merge(existing_row, &mut new_row, schema)?;

    // Update row-level deletion: newer tombstone wins (LWW).
    if new_row.deletion.marked_for_delete_at > existing_row.deletion.marked_for_delete_at {
        existing_row.deletion = new_row.deletion;
    }

    // Update primary key liveness to the newer timestamp
    if new_row.primary_key_liveness.timestamp > existing_row.primary_key_liveness.timestamp {
        existing_row.primary_key_liveness = new_row.primary_key_liveness;
    }

    // Merge cells, keyed by (column index, cell path). A simple column
    // has one cell (path == None); a complex (collection) column has many
    // cells sharing the column index, one per element, distinguished by
    // path. Cells stay sorted by (col_idx, path) — for simple cells (all
    // None) this is identical to the historical col_idx order, so existing
    // data and searches are unaffected. Reconciliation uses the CRDT rule
    // (higher timestamp wins; tombstone wins an equal-timestamp tie), which
    // is what makes concurrent per-element appends converge.
    for (col_idx, new_cell) in new_row.cells {
        let key = (col_idx, &new_cell.path);
        let cell_pos = existing_row
            .cells
            .binary_search_by(|(idx, cell)| (*idx, &cell.path).cmp(&key));

        match cell_pos {
            Ok(ci) => {
                let existing_cell = &existing_row.cells[ci].1;
                existing_row.cells[ci].1 = ferrosa_common::reconcile(existing_cell, &new_cell);
            }
            Err(ci) => {
                // New (col_idx, path) — insert at sorted position.
                existing_row.cells.insert(ci, (col_idx, new_cell));
            }
        }
    }
    Ok(())
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

// ---------------------------------------------------------------------------
// ShardedRangeIter: lazy k-way merge across shards
// ---------------------------------------------------------------------------
//
// Yields one `Arc<Partition>` at a time in global token order. Retains exactly
// one heap entry per shard (`O(num_shards)`), never `O(partitions_in_range)`:
// the previous form collected every in-range `Arc` into one `Vec` per shard and
// handed the whole `Vec<Vec<…>>` to the iterator, which pinned `8 B per
// partition` for the scan's entire lifetime — for a paging cursor that parks the
// iterator across pages, tens of MB per open cursor.
//
// Instead of holding a shard's read guard for the iterator's lifetime (which
// would block every write to that shard while a scan is parked), the iterator
// keeps only the *key* it last consumed per shard and re-seeks the next entry
// under a short-lived read lock when it advances that shard. A shard's read
// guard is therefore held only for the duration of one `next()` seek, never
// across the consumer's processing.

/// Heap entry: (key, source_idx) where the heap is min-keyed.
/// Wraps the comparison so `BinaryHeap` (which is a max-heap) becomes
/// a min-heap.
struct ShardHeapEntry {
    key: DecoratedKey,
    src: usize,
}
impl PartialEq for ShardHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.src == other.src
    }
}
impl Eq for ShardHeapEntry {}
impl PartialOrd for ShardHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for ShardHeapEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reverse the key comparison so the BinaryHeap (max-heap)
        // pops the smallest key first. Tie-break on src so the heap
        // is total-ordered.
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.src.cmp(&self.src))
    }
}

/// Lazily merge a sharded memtable's partitions in global token order.
///
/// Memory is `O(num_shards)`: one `ShardedRangeIterItem` (key + `Arc`) per shard
/// plus the heap over those keys. Nothing is materialized for partitions that
/// have not been asked for.
///
/// ## Snapshot consistency
///
/// The merge is consistent for a memtable that is being written: entries are
/// yielded in token order, and an entry already handed to the consumer is never
/// yielded twice — the advance step seeks strictly *after* the key just handed
/// out, and that key is a shard's own BTreeMap order, so it cannot recur. A
/// partition inserted *behind* an already-consumed key is not revisited by this
/// iterator (range semantics do not promise it); a new partition ahead of the
/// cursor is picked up normally.
pub(crate) struct ShardedRangeIter<'a> {
    mem: &'a ShardedBTreeMemtable,
    end: Option<DecoratedKey>,
    /// Per-shard head: the entry whose key currently sits in the heap, so
    /// `next` can hand out the stored `Arc` without seeking twice.
    heads: Vec<Option<Arc<Partition>>>,
    /// Shard indices that still have a head, keyed by that head's key.
    heap: BinaryHeap<ShardHeapEntry>,
}

impl<'a> ShardedRangeIter<'a> {
    pub(crate) fn new(
        mem: &'a ShardedBTreeMemtable,
        start: Option<DecoratedKey>,
        end: Option<DecoratedKey>,
    ) -> Self {
        let mut heads: Vec<Option<Arc<Partition>>> = vec![None; mem.shards.len()];
        let mut heap = BinaryHeap::with_capacity(mem.shards.len());
        for (src, slot) in heads.iter_mut().enumerate() {
            if let Some(arc) = mem.seek_shard_start(src, start.as_ref(), end.as_ref()) {
                heap.push(ShardHeapEntry {
                    key: arc.key.clone(),
                    src,
                });
                *slot = Some(arc);
            }
        }
        Self {
            mem,
            end,
            heads,
            heap,
        }
    }

    /// Advance shard `src` past `after` (the key just yielded) and queue its
    /// new head, if any.
    fn advance_shard(&mut self, src: usize, after: &DecoratedKey) {
        match self.mem.seek_shard(src, after, self.end.as_ref()) {
            Some(arc) => {
                self.heap.push(ShardHeapEntry {
                    key: arc.key.clone(),
                    src,
                });
                self.heads[src] = Some(arc);
            }
            None => self.heads[src] = None,
        }
    }
}

impl Iterator for ShardedRangeIter<'_> {
    type Item = Arc<Partition>;

    fn next(&mut self) -> Option<Arc<Partition>> {
        // Pop the smallest head. The heap entry carries the key, so take the
        // stored `Arc` and advance that shard strictly past it (exclusive, so a
        // key can never be yielded twice).
        let entry = self.heap.pop()?;
        let src = entry.src;
        let arc = self.heads[src]
            .take()
            .expect("a shard with a heap entry always has a head");
        self.advance_shard(src, &entry.key);
        Some(arc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::ColumnDefinition;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    /// The configured-num-shards override is what
    /// `with_default_shards()` reads. Pin both directions of the
    /// contract: a valid override changes the shard count of newly-
    /// created memtables; a zero override is ignored so a
    /// misconfigured env var can't wedge every memtable.
    #[test]
    fn with_default_shards_honors_configured_override() {
        // Capture the current value so this test stays hermetic in
        // the presence of FERROSA_MEMTABLE_NUM_SHARDS or earlier
        // `set_configured_num_shards` calls in the same process.
        let original = configured_num_shards();

        set_configured_num_shards(128);
        let mem = ShardedBTreeMemtable::with_default_shards();
        assert_eq!(mem.shards.len(), 128);

        set_configured_num_shards(8);
        let mem = ShardedBTreeMemtable::with_default_shards();
        assert_eq!(mem.shards.len(), 8);

        // Zero override is rejected — the prior value (8) stands.
        set_configured_num_shards(0);
        let mem = ShardedBTreeMemtable::with_default_shards();
        assert_eq!(
            mem.shards.len(),
            8,
            "set_configured_num_shards(0) must not wedge memtable creation",
        );

        // Restore so adjacent tests see the original value.
        set_configured_num_shards(original);
    }

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

    /// A row carrying one **complex** (collection) element cell: column `col`,
    /// cell path `path`, live value `value` at `timestamp`.
    fn complex_row(col: u16, path: &[u8], value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering: vec![],
            cells: vec![(
                col,
                CellValue::live(value.to_vec(), timestamp).with_path(path.to_vec()),
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    fn empty_partition() -> Partition {
        Partition {
            key: make_key("pk"),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![],
        }
    }

    #[test]
    fn merge_keeps_distinct_complex_cells_per_path() {
        // Two per-element appends to the same collection column (col 0) with distinct
        // paths both survive — the convergence a single whole-collection cell cannot
        // express, and the reason a list-append no longer needs a read-modify-write.
        let mut p = empty_partition();
        merge_row_into_partition(&mut p, complex_row(0, b"pA", b"a", 10), &test_schema()).unwrap();
        merge_row_into_partition(&mut p, complex_row(0, b"pB", b"b", 11), &test_schema()).unwrap();
        let cells = &p.rows[0].cells;
        assert_eq!(cells.len(), 2, "both element cells retained");
        // Cells stay sorted by (col, path): pA before pB.
        assert_eq!(cells[0].1.value.as_deref(), Some(b"a".as_slice()));
        assert_eq!(cells[0].1.path.as_deref(), Some(b"pA".as_slice()));
        assert_eq!(cells[1].1.value.as_deref(), Some(b"b".as_slice()));
    }

    #[test]
    fn merge_reconciles_same_path_by_lww() {
        let mut p = empty_partition();
        merge_row_into_partition(&mut p, complex_row(0, b"pA", b"old", 10), &test_schema())
            .unwrap();
        merge_row_into_partition(&mut p, complex_row(0, b"pA", b"new", 20), &test_schema())
            .unwrap();
        assert_eq!(
            p.rows[0].cells.len(),
            1,
            "same (col,path) reconciled, not duplicated"
        );
        assert_eq!(
            p.rows[0].cells[0].1.value.as_deref(),
            Some(b"new".as_slice())
        );
        // A stale write (lower ts) does not win.
        merge_row_into_partition(&mut p, complex_row(0, b"pA", b"stale", 5), &test_schema())
            .unwrap();
        assert_eq!(
            p.rows[0].cells[0].1.value.as_deref(),
            Some(b"new".as_slice())
        );
    }

    #[test]
    fn merge_same_path_tombstone_wins_equal_timestamp() {
        let mut p = empty_partition();
        merge_row_into_partition(&mut p, complex_row(0, b"pA", b"v", 10), &test_schema()).unwrap();
        // A remove (tombstone) at the SAME timestamp wins the tie — element gone.
        let mut remove = complex_row(0, b"pA", b"v", 10);
        remove.cells[0].1 = CellValue::tombstone(10, i32::MAX).with_path(b"pA".to_vec());
        merge_row_into_partition(&mut p, remove, &test_schema()).unwrap();
        assert!(
            p.rows[0].cells[0].1.is_tombstone(),
            "tombstone wins the equal-ts tie"
        );
    }

    #[test]
    fn merge_rejects_path_cell_for_scalar_column() {
        let mut p = empty_partition();
        merge_row_into_partition(&mut p, make_row(0, b"s", 10), &test_schema()).unwrap();
        let err = merge_row_into_partition(&mut p, complex_row(0, b"pA", b"c", 11), &test_schema())
            .unwrap_err();
        assert!(err.to_string().contains("non-collection column val"));
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

    /// A partition-tombstone marker must be lifted into `Partition::deletion`
    /// (LWW), not stored as a phantom empty-clustering row. Otherwise a
    /// whole-partition `DELETE FROM t WHERE pk = ?` suppresses nothing and the
    /// rows silently survive (the bug the batch primitive exposed).
    #[test]
    fn put_partition_tombstone_sets_partition_deletion_not_a_row() {
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo};
        let mem = ShardedBTreeMemtable::new(4);
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
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
        };
        let key = make_key("pk1");

        // Seed two clustered rows (ts=100), then a partition tombstone (ts=200).
        let row1 = Row {
            clustering: 1i32.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(b"a".to_vec(), 100))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(100),
        };
        let row2 = Row {
            clustering: 2i32.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(b"b".to_vec(), 100))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(100),
        };
        mem.put(&key, row1, &schema).unwrap();
        mem.put(&key, row2, &schema).unwrap();
        mem.put(
            &key,
            Row {
                clustering: vec![],
                cells: vec![],
                deletion: DeletionTime::new(200, 0),
                primary_key_liveness: LivenessInfo::NONE,
            },
            &schema,
        )
        .unwrap();

        let partition = mem.get(&key).unwrap().expect("partition present");
        // The tombstone is lifted to the partition, not stored as a phantom row.
        assert_eq!(
            partition.deletion.marked_for_delete_at, 200,
            "partition-level deletion must carry the tombstone timestamp"
        );
        assert!(
            partition.rows.iter().all(|r| !r.clustering.is_empty()),
            "no empty-clustering phantom tombstone row may be stored in rows"
        );
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
    fn min_timestamp_is_max_when_empty_then_tracks_the_lowest_write() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        assert_eq!(mem.min_timestamp(), i64::MAX, "nothing written yet");
        mem.put(&make_key("a"), make_row(0, b"x", 5_000), &schema)
            .unwrap();
        assert_eq!(mem.min_timestamp(), 5_000);
        // A later write with an OLDER timestamp (a replayed hint, a client-supplied
        // timestamp) lowers it; a newer one does not raise it.
        mem.put(&make_key("b"), make_row(0, b"y", 1_000), &schema)
            .unwrap();
        mem.put(&make_key("c"), make_row(0, b"z", 9_000), &schema)
            .unwrap();
        assert_eq!(mem.min_timestamp(), 1_000);
    }

    #[test]
    fn a_rejected_write_does_not_lower_min_timestamp() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = timeuuid_schema();
        // 8-byte value in a TimeUUID column is rejected before it is stored.
        assert!(mem
            .put(&make_key("a"), make_row(0, &[0u8; 8], 1), &schema)
            .is_err());
        assert_eq!(mem.min_timestamp(), i64::MAX);
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

    /// ADR-020 lazy range_iter contract for the Sharded memtable.
    /// The k-way merge across shards must produce partitions in
    /// global token order, independent of which shard each partition
    /// landed in (shard selection is by `token % num_shards`, so
    /// adjacent tokens scatter across shards).
    #[test]
    fn range_iter_merges_shards_into_global_token_order() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        for i in 0..100 {
            let key = make_key(&format!("k_{i:03}"));
            mem.put(&key, make_row(0, format!("v{i}").as_bytes(), 1000), &schema)
                .unwrap();
        }
        let collected: Vec<_> = mem.range_iter(None, None).collect();
        assert_eq!(collected.len(), 100);
        for window in collected.windows(2) {
            assert!(
                window[0].key <= window[1].key,
                "range_iter not in token order across shards: {:?} > {:?}",
                window[0].key.token,
                window[1].key.token,
            );
        }
        // Same output as the eager snapshot path, verifying merge
        // correctness against the existing implementation.
        let snapshot = mem.snapshot();
        assert_eq!(
            collected.iter().map(|p| p.key.clone()).collect::<Vec<_>>(),
            snapshot.iter().map(|p| p.key.clone()).collect::<Vec<_>>()
        );
    }

    /// A bounded scan must agree with the eager snapshot path for the same
    /// bounds. `range_iter` is lazy now (it re-seeks each shard as it advances
    /// rather than collecting the range first), so the two implementations could
    /// disagree on where the bounds land — half-open vs inclusive, or an
    /// off-by-one at either end.
    #[test]
    fn range_iter_bounded_matches_snapshot_within_bounds() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        for i in 0..100 {
            let key = make_key(&format!("k_{i:03}"));
            mem.put(&key, make_row(0, format!("v{i}").as_bytes(), 1000), &schema)
                .unwrap();
        }
        // Every (start, end) pair over a handful of keys, plus unbounded ends.
        let bounds = [
            None,
            Some(make_key("k_000")),
            Some(make_key("k_037")),
            Some(make_key("k_099")),
        ];
        for start in bounds.iter() {
            for end in bounds.iter() {
                let got: Vec<_> = mem.range_iter(start.as_ref(), end.as_ref()).collect();
                let want: Vec<_> = mem
                    .snapshot()
                    .into_iter()
                    .filter(|p| {
                        start.as_ref().is_none_or(|s| p.key >= *s)
                            && end.as_ref().is_none_or(|e| p.key <= *e)
                    })
                    .collect();
                assert_eq!(
                    got.iter().map(|p| p.key.clone()).collect::<Vec<_>>(),
                    want.iter().map(|p| p.key.clone()).collect::<Vec<_>>(),
                    "range_iter disagreed with snapshot for start={start:?} end={end:?}"
                );
            }
        }
    }

    /// Degenerate shapes: an empty memtable yields nothing, and a single shard
    /// still merges (the heap has one entry, so `next` must not assume >= 2).
    #[test]
    fn range_iter_handles_empty_and_single_shard() {
        let schema = test_schema();
        let empty = ShardedBTreeMemtable::new(4);
        assert_eq!(empty.range_iter(None, None).count(), 0);

        let one = ShardedBTreeMemtable::new(1);
        for i in 0..10 {
            let key = make_key(&format!("k_{i:02}"));
            one.put(&key, make_row(0, format!("v{i}").as_bytes(), 1000), &schema)
                .unwrap();
        }
        let keys: Vec<_> = one.range_iter(None, None).map(|p| p.key.clone()).collect();
        assert_eq!(keys.len(), 10);
        for w in keys.windows(2) {
            assert!(w[0] <= w[1], "single-shard scan out of order");
        }
    }

    /// The lazy merge must not yield the same partition twice, nor drop one that
    /// sits strictly ahead of the cursor. This is the shape a pre-materialized
    /// implementation gets for free and a re-seeking one can get wrong.
    #[test]
    fn range_iter_yields_each_partition_exactly_once() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        for i in 0..200 {
            let key = make_key(&format!("k_{i:03}"));
            mem.put(&key, make_row(0, format!("v{i}").as_bytes(), 1000), &schema)
                .unwrap();
        }
        let scanned: Vec<_> = mem.range_iter(None, None).map(|p| p.key.clone()).collect();
        assert_eq!(scanned.len(), 200, "every partition must be yielded once");
        let unique: std::collections::BTreeSet<_> = scanned.iter().cloned().collect();
        assert_eq!(
            unique.len(),
            200,
            "a partition was yielded more than once (the merge re-served a key)"
        );
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

    const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";
    const LIST_OF_TEXT: &str =
        "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)";
    const SET_OF_TEXT: &str =
        "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)";

    fn column(name: &str, type_name: &str) -> ColumnDefinition {
        ColumnDefinition {
            name: name.to_string(),
            type_name: type_name.to_string(),
        }
    }

    /// Ordinals: static `s` = 0, regular `a` = 1, regular `l` (list) = 2.
    /// `l` is regular position 0 + 1 static, so a lookup that ignores the
    /// static offset reads position 2 of `regular_columns` (out of range).
    fn schema_static_text_regular_text_and_list() -> TableSchema {
        TableSchema {
            static_columns: vec![column("s", UTF8)],
            regular_columns: vec![column("a", UTF8), column("l", LIST_OF_TEXT)],
            ..test_schema()
        }
    }

    /// A legacy whole-collection blob: `count`, then length-prefixed elements.
    fn collection_blob(elements: &[&[u8]]) -> Vec<u8> {
        let mut blob = (elements.len() as i32).to_be_bytes().to_vec();
        for element in elements {
            blob.extend_from_slice(&(element.len() as i32).to_be_bytes());
            blob.extend_from_slice(element);
        }
        blob
    }

    fn legacy_blob_row(col: u16, elements: &[&[u8]], timestamp: i64) -> Row {
        make_row(col, &collection_blob(elements), timestamp)
    }

    /// The merge normalizer must resolve a cell ordinal the way the schema
    /// defines it (statics first, then regulars), or it expands a blob under
    /// the wrong column's type -- or rejects it as "outside the regular
    /// schema" -- on any table that has a static column.
    #[test]
    fn merge_expands_a_legacy_list_blob_on_a_table_with_static_columns() {
        let schema = schema_static_text_regular_text_and_list();
        let list_ordinal = schema.column_index("l").unwrap();
        assert_eq!(list_ordinal, 2, "schema ordering contract: statics first");

        let mut p = empty_partition();
        merge_row_into_partition(
            &mut p,
            complex_row(list_ordinal, b"pA", b"first", 10),
            &schema,
        )
        .unwrap();
        merge_row_into_partition(
            &mut p,
            legacy_blob_row(list_ordinal, &[b"x", b"y"], 20),
            &schema,
        )
        .unwrap();

        let list_cells: Vec<_> = p.rows[0]
            .cells
            .iter()
            .filter(|(idx, _)| *idx == list_ordinal)
            .collect();
        assert!(
            list_cells
                .iter()
                .all(|(_, c)| c.path.is_some() || c.is_tombstone()),
            "no pathless live cell may remain in a complex column: {list_cells:?}"
        );
        let live_values: Vec<_> = list_cells
            .iter()
            .filter(|(_, c)| c.path.is_some() && !c.is_tombstone() && c.timestamp == 20)
            .map(|(_, c)| c.value.clone().unwrap())
            .collect();
        assert_eq!(live_values, vec![b"x".to_vec(), b"y".to_vec()]);
    }

    /// A collection in a STATIC column sits at an ordinal below
    /// `static_columns.len()`, so it must be looked up in `static_columns`.
    #[test]
    fn merge_expands_a_legacy_set_blob_in_a_static_column() {
        let schema = TableSchema {
            static_columns: vec![column("ss", SET_OF_TEXT)],
            regular_columns: vec![column("a", UTF8)],
            ..test_schema()
        };
        let mut p = empty_partition();
        merge_row_into_partition(&mut p, complex_row(0, b"k1", b"", 10), &schema).unwrap();
        merge_row_into_partition(&mut p, legacy_blob_row(0, &[b"k2"], 20), &schema).unwrap();

        let paths: Vec<_> = p.rows[0]
            .cells
            .iter()
            .filter(|(idx, c)| *idx == 0 && !c.is_tombstone())
            .map(|(_, c)| c.path.clone())
            .collect();
        assert_eq!(
            paths,
            vec![Some(b"k1".to_vec()), Some(b"k2".to_vec())],
            "both set elements live under the static column"
        );
    }

    /// On a clustered table a static column has one value per partition: a
    /// static cell written with a clustered row lands in the static row, the
    /// newest write winning, and the clustered rows keep only their regulars.
    #[test]
    fn static_cells_written_with_clustered_rows_merge_into_the_static_row() {
        let schema = TableSchema {
            clustering_columns: vec![column("ck", "org.apache.cassandra.db.marshal.Int32Type")],
            static_columns: vec![column("s", UTF8)],
            regular_columns: vec![column("a", UTF8)],
            ..test_schema()
        };
        let row = |ck: i32, s: &[u8], a: &[u8], ts: i64| Row {
            clustering: ck.to_be_bytes().to_vec(),
            cells: vec![
                (0, CellValue::live(s.to_vec(), ts)),
                (1, CellValue::live(a.to_vec(), ts)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(ts),
        };
        let mut p = empty_partition();
        merge_row_into_partition(&mut p, row(1, b"s-new", b"a1", 20), &schema).unwrap();
        merge_row_into_partition(&mut p, row(2, b"s-old", b"a2", 10), &schema).unwrap();

        let statics = &p.static_row.as_ref().expect("static row").cells;
        assert_eq!(statics, &vec![(0, CellValue::live(b"s-new".to_vec(), 20))]);
        assert_eq!(p.rows.len(), 2);
        assert_eq!(
            p.rows[0].cells,
            vec![(1, CellValue::live(b"a1".to_vec(), 20))]
        );
        assert_eq!(
            p.rows[1].cells,
            vec![(1, CellValue::live(b"a2".to_vec(), 10))]
        );
    }

    /// Other columns' cells must come through the normalizer untouched, still
    /// under their own ordinal.
    #[test]
    fn merge_normalization_leaves_other_columns_on_their_own_ordinals() {
        let schema = schema_static_text_regular_text_and_list();
        let mut p = empty_partition();
        let mut first = complex_row(2, b"pA", b"first", 10);
        first
            .cells
            .insert(0, (1, CellValue::live(b"text".to_vec(), 10)));
        merge_row_into_partition(&mut p, first, &schema).unwrap();
        merge_row_into_partition(&mut p, legacy_blob_row(2, &[b"x"], 20), &schema).unwrap();

        let a_cells: Vec<_> = p.rows[0].cells.iter().filter(|(i, _)| *i == 1).collect();
        assert_eq!(a_cells.len(), 1);
        assert_eq!(a_cells[0].1.value.as_deref(), Some(b"text".as_slice()));
        assert!(a_cells[0].1.path.is_none());
    }
}
