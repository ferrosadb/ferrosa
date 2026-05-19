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

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::TableSchema;
use ferrosa_common::Result;
use ferrosa_index::{IndexKey, RowPosition};
use ferrosa_sstable::io::ReadAt;
use ferrosa_sstable::reader::SSTableReader;
use ferrosa_sstable::types::{Partition, Row};
use ferrosa_sstable::writer::SSTableWriter;
use ferrosa_sstable::WriteOptions;

use ferrosa_index::DistanceMetric;

use crate::flush::{self, FlushTarget};
use crate::index::sidecar::SidecarReader;
use crate::memtable::index::MemtableIndex;
#[cfg(not(feature = "skiplist-memtable"))]
use crate::memtable::sharded::ShardedBTreeMemtable;
#[cfg(feature = "skiplist-memtable")]
use crate::memtable::skiplist::SkipListMemtable;
use crate::memtable::vector_index::VectorMemtableIndex;
use crate::memtable::Memtable;
use crate::merge;

/// Maximum number of row positions collected from secondary index before
/// returning an error. Prevents OOM from high-cardinality queries.
const INDEX_RESULT_CAP: usize = 10_000;
const RANGE_READ_MATERIALIZATION_CAP: usize = 10_000;

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
    /// Stable generation IDs and file directories for each SSTable, parallel to `sstables`.
    /// The String is the gen (used for file names and swap matching).
    /// The PathBuf is the directory containing the SSTable files.
    sstable_ids: Arc<Vec<(String, std::path::PathBuf)>>,
    /// Per-index MemtableIndex companions for the active memtable, keyed by
    /// index name. Swapped atomically alongside the active memtable during flush.
    indexes: Arc<HashMap<String, Arc<MemtableIndex>>>,
    /// Per-SSTable sidecar index readers, parallel to `sstables`.
    /// Each entry maps index_name -> SidecarReader for that SSTable.
    sidecar_indexes: Arc<Vec<Arc<HashMap<String, SidecarReader>>>>,
    /// In-memory vector indexes for the active memtable, keyed by index name.
    /// Each holds the accumulated vectors for one vector column.
    /// Drained at flush time and used to build persistent HNSW sidecar files.
    vector_indexes: Arc<HashMap<String, Arc<VectorMemtableIndex>>>,
}

impl<R: ReadAt + Send + Sync + 'static> StoreView<R> {
    /// Check the three-parallel-vector invariant:
    /// `sstables.len() == sstable_ids.len() == sidecar_indexes.len()`.
    ///
    /// When violated, logs a loud `tracing::error!` with full lengths and a
    /// caller-supplied tag so logs point at the offending construction site.
    /// Also `debug_assert!`s so unit tests fail at the exact write site.
    ///
    /// Previously, violations were silently masked downstream: `sstable_metadata`
    /// synthesized fake integer IDs (`format!("{}", i + 1)`) for SSTables without
    /// a matching `sstable_ids` entry, and compaction then tried to read
    /// `<i+1>-Data.db` files that were never written, driving the node toward OOM.
    fn check_invariants(&self, tag: &'static str) {
        let n_sst = self.sstables.len();
        let n_ids = self.sstable_ids.len();
        let n_side = self.sidecar_indexes.len();
        if n_sst != n_ids || n_sst != n_side {
            tracing::error!(
                tag,
                sstables_len = n_sst,
                sstable_ids_len = n_ids,
                sidecar_indexes_len = n_side,
                "StoreView invariant violated: parallel vectors desynced at construction"
            );
        }
        debug_assert_eq!(
            n_sst, n_ids,
            "StoreView@{tag}: sstables ({n_sst}) != sstable_ids ({n_ids})"
        );
        debug_assert_eq!(
            n_sst, n_side,
            "StoreView@{tag}: sstables ({n_sst}) != sidecar_indexes ({n_side})"
        );
    }
}

/// Configuration for a single vector index on a table column.
///
/// Immutable after registration — parameters control the in-memory and
/// persistent HNSW graph built at flush time.
#[derive(Clone, Debug)]
pub struct VectorIndexConfig {
    /// Unique name for this index (matches the column name by convention).
    pub index_name: String,
    /// Column ordinal (the `u16` tag in `Row.cells`) holding vector values.
    pub column_position: usize,
    /// Distance metric for similarity comparisons.
    pub metric: DistanceMetric,
    /// HNSW `m` parameter: max connections per node per layer.
    pub m: usize,
    /// HNSW `ef_construction` parameter: search width during build.
    pub ef_construction: usize,
}

/// Single-table storage engine: lock-free reads, serialized flushes.
///
/// `F` is the flush destination (in-memory for tests, file-based for
/// production). `F::Reader` must be `ReadAt + Send + Sync + 'static`
/// so the resulting `SSTableReader` can be held inside the shared view.
pub struct TableStore<F: FlushTarget> {
    /// Current schema for this table. Wrapped in `ArcSwap` so `ALTER TABLE`
    /// can atomically swap in a new schema without blocking reads, writes,
    /// or in-flight flushes. Prior to this indirection, `ALTER TABLE ADD
    /// COLUMN` left the schema stale and the flush path produced silently
    /// corrupt SSTables (bug-sstable-writer-produces-zero-byte-rows-db.md).
    schema: ArcSwap<TableSchema>,
    view: ArcSwap<StoreView<F::Reader>>,
    /// Serializes concurrent flushes. The read/write paths never touch this.
    flush_guard: Mutex<()>,
    /// Write barrier: writes hold shared (read), flush holds exclusive (write)
    /// during the memtable swap. This ensures no writer is mid-put when the
    /// active memtable is swapped, preventing writes to a stale memtable.
    write_barrier: parking_lot::RwLock<()>,
    /// Counter of SSTable read errors during get_partition.
    pub sstable_read_errors: std::sync::atomic::AtomicU64,
    pub(crate) flush_target: F,
    options: WriteOptions,
    /// Secondary index declarations: `(index_name, column_position)` pairs.
    /// Column position is the index into `Row.cells` by column ordinal
    /// (matching the `u16` tag in each cell tuple).
    indexed_columns: Vec<(String, usize)>,
    /// Full-text index declarations: `(index_name, column_position)` pairs.
    /// Built as FTI sidecar files during flush.
    fulltext_indexes: Vec<(String, usize)>,
    /// Vector index configurations. Immutable after registration.
    /// At flush time each declared vector index is drained from the memtable
    /// and persisted as a `{gen}-VEC-{index_name}.db` HNSW sidecar file.
    vector_index_configs: Vec<VectorIndexConfig>,
    /// Monotonic generation counter for stable SSTable IDs.
    /// Incremented on each flush. Used by compaction swap to identify
    /// exactly which SSTables to remove.
    next_gen: std::sync::atomic::AtomicU64,
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

/// Build a fresh `HashMap` of empty `MemtableIndex` instances, one per
/// declared secondary index.
fn new_indexes(indexed_columns: &[(String, usize)]) -> Arc<HashMap<String, Arc<MemtableIndex>>> {
    let map: HashMap<String, Arc<MemtableIndex>> = indexed_columns
        .iter()
        .map(|(name, _)| (name.clone(), Arc::new(MemtableIndex::new())))
        .collect();
    Arc::new(map)
}

/// Build a fresh `HashMap` of empty `VectorMemtableIndex` instances, one per
/// declared vector index configuration.
fn new_vector_indexes(
    configs: &[VectorIndexConfig],
) -> Arc<HashMap<String, Arc<VectorMemtableIndex>>> {
    let map: HashMap<String, Arc<VectorMemtableIndex>> = configs
        .iter()
        .map(|cfg| {
            (
                cfg.index_name.clone(),
                Arc::new(VectorMemtableIndex::new(
                    cfg.metric,
                    cfg.m,
                    cfg.ef_construction,
                )),
            )
        })
        .collect();
    Arc::new(map)
}

/// Filters sidecar entries to remove references to deleted partitions.
///
/// After compaction merges partitions, some entries in the collected
/// sidecar map may reference partition keys that were removed (tombstoned).
/// This function removes those stale entries and drops any index whose
/// entry list becomes empty as a result.
pub fn filter_tombstoned_sidecar_entries(
    entries: &mut HashMap<String, Vec<(IndexKey, RowPosition)>>,
    live_partition_keys: &std::collections::HashSet<Vec<u8>>,
) {
    for positions in entries.values_mut() {
        positions.retain(|(_key, pos)| live_partition_keys.contains(&pos.partition_key));
    }
    entries.retain(|_, positions| !positions.is_empty());
}

impl<F: FlushTarget> TableStore<F> {
    /// Create a new `TableStore` with an empty memtable and no SSTables.
    pub fn new(schema: TableSchema, flush_target: F, options: WriteOptions) -> Self {
        Self::new_with_indexes(schema, flush_target, options, vec![])
    }

    /// Create a `TableStore` with secondary index declarations.
    ///
    /// `indexed_columns` is a list of `(index_name, column_position)` pairs.
    /// The column position is the ordinal used as the `u16` tag in
    /// `Row.cells` — e.g., 0 for the first regular column.
    pub fn new_with_indexes(
        schema: TableSchema,
        flush_target: F,
        options: WriteOptions,
        indexed_columns: Vec<(String, usize)>,
    ) -> Self {
        let active: Arc<dyn Memtable> = new_memtable();
        let indexes = new_indexes(&indexed_columns);
        let initial_view = StoreView {
            active,
            flushing: None,
            sstables: Arc::new(vec![]),
            sstable_ids: Arc::new(vec![]),
            indexes,
            sidecar_indexes: Arc::new(vec![]),
            vector_indexes: Arc::new(HashMap::new()),
        };
        initial_view.check_invariants("new:empty");
        Self {
            schema: ArcSwap::from_pointee(schema),
            view: ArcSwap::from_pointee(initial_view),
            flush_guard: Mutex::new(()),
            flush_target,
            options,
            indexed_columns,
            fulltext_indexes: vec![],
            vector_index_configs: vec![],
            next_gen: std::sync::atomic::AtomicU64::new(1),
            write_barrier: parking_lot::RwLock::new(()),
            sstable_read_errors: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Create a `TableStore` with an initial set of SSTable readers already loaded.
    ///
    /// Used during crash recovery to populate the store with SSTables that
    /// were flushed before the crash. The readers must be ordered newest first.
    /// `initial_sidecars` is a parallel vec — position `i` is the sidecar map
    /// for `initial_sstables[i]`. If empty or shorter than `initial_sstables`,
    /// the remaining positions get empty sidecar maps.
    /// `indexed_columns` declares secondary indexes for new writes; use an empty
    /// vec if no indexes are active on this table.
    pub fn new_with_sstables(
        schema: TableSchema,
        flush_target: F,
        options: WriteOptions,
        initial_sstables: Vec<Arc<SSTableReader<F::Reader>>>,
        initial_sidecars: Vec<Arc<HashMap<String, SidecarReader>>>,
        initial_ids: Vec<(String, std::path::PathBuf)>,
    ) -> Self {
        Self::new_with_sstables_and_indexes(
            schema,
            flush_target,
            options,
            initial_sstables,
            initial_sidecars,
            initial_ids,
            vec![],
        )
    }

    /// Like [`Self::new_with_sstables`] but also registers secondary index declarations
    /// so that new writes populate the memtable index.
    ///
    /// `initial_ids` must be parallel to `initial_sstables` — each entry is
    /// `(gen_str, sstable_dir)` where `gen_str` matches the on-disk file name
    /// prefix `{gen_str}-Data.db`. **Do not pass synthetic IDs** (e.g.
    /// `1..=N`): compaction constructs paths from these IDs and will ENOENT
    /// on every task if they don't match real files, driving the node toward
    /// OOM via retry storms.
    pub fn new_with_sstables_and_indexes(
        schema: TableSchema,
        flush_target: F,
        options: WriteOptions,
        initial_sstables: Vec<Arc<SSTableReader<F::Reader>>>,
        initial_sidecars: Vec<Arc<HashMap<String, SidecarReader>>>,
        initial_ids: Vec<(String, std::path::PathBuf)>,
        indexed_columns: Vec<(String, usize)>,
    ) -> Self {
        let active: Arc<dyn Memtable> = new_memtable();
        let indexes = new_indexes(&indexed_columns);
        let sidecar_count = initial_sstables.len();

        // Pad sidecar list with empty maps if shorter than the SSTable list.
        let mut sidecars: Vec<Arc<HashMap<String, SidecarReader>>> = initial_sidecars;
        while sidecars.len() < sidecar_count {
            sidecars.push(Arc::new(HashMap::new()));
        }

        // Fail loud if caller didn't provide a matching IDs vec. Previous
        // behavior silently synthesized fake integer IDs here — that masked
        // the invariant violation and produced phantom `{n}-Data.db` paths.
        assert_eq!(
            initial_sstables.len(),
            initial_ids.len(),
            "new_with_sstables_and_indexes: initial_sstables ({}) and initial_ids ({}) \
             must have equal length — one (gen_str, dir) per SSTable reader",
            initial_sstables.len(),
            initial_ids.len()
        );
        let initial_view = StoreView {
            active,
            flushing: None,
            sstables: Arc::new(initial_sstables),
            sstable_ids: Arc::new(initial_ids),
            indexes,
            sidecar_indexes: Arc::new(sidecars),
            vector_indexes: Arc::new(HashMap::new()),
        };
        initial_view.check_invariants("new_with_sstables");
        Self {
            schema: ArcSwap::from_pointee(schema),
            view: ArcSwap::from_pointee(initial_view),
            flush_guard: Mutex::new(()),
            flush_target,
            options,
            indexed_columns,
            fulltext_indexes: vec![],
            vector_index_configs: vec![],
            next_gen: std::sync::atomic::AtomicU64::new(1),
            write_barrier: parking_lot::RwLock::new(()),
            sstable_read_errors: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Atomically swap in a new schema. Called when `ALTER TABLE` mutates
    /// the set of columns so that subsequent flushes build the
    /// `SerializationHeader` with up-to-date `num_columns`, avoiding the
    /// writer's out-of-range-col_idx panic
    /// (see bug-sstable-writer-produces-zero-byte-rows-db.md).
    pub fn update_schema(&self, new_schema: TableSchema) {
        self.schema.store(Arc::new(new_schema));
    }

    /// Return a guard over the current schema. Holding the guard keeps the
    /// schema `Arc` alive; `ALTER TABLE` can still swap in a new schema
    /// concurrently.
    pub fn schema(&self) -> arc_swap::Guard<Arc<TableSchema>> {
        self.schema.load()
    }

    /// Directory under which this store's SSTable components and
    /// quarantine files live. Used by the engine's replay path to open a
    /// `QuarantineWriter` for malformed rows that the per-cell validator
    /// rejects (Layer 3 of the timeuuid-flush-wedge fix).
    pub fn flush_dir(&self) -> &std::path::Path {
        self.flush_target.base_dir()
    }

    /// Estimate on-disk bytes that a full table scan would need to touch.
    ///
    /// This intentionally counts only SSTable component files already present
    /// on local disk. It is a cheap planner signal for expensive read shapes
    /// such as arbitrary unbounded `ORDER BY`; it is not a billing-accurate
    /// byte counter and does not include active/flushing memtable contents.
    pub fn estimated_disk_scan_bytes(&self) -> u64 {
        let guard = self.view.load();
        guard
            .sstable_ids
            .iter()
            .map(|(gen, dir)| dir.join(format!("{gen}-Data.db")))
            .filter_map(|path| std::fs::metadata(path).ok().map(|meta| meta.len()))
            .sum()
    }

    /// Write a row into the active memtable and update secondary indexes.
    ///
    /// Loads the current view atomically, then delegates to the memtable's
    /// `put`. After the memtable write, each declared secondary index is
    /// updated by extracting the indexed column value from the row cells.
    /// No lock is taken on the read/write path; the ArcSwap guard provides
    /// the necessary lifetime without blocking.
    pub fn write(&self, key: &DecoratedKey, row: Row) -> Result<()> {
        let guard = self.view.load();

        // Secondary index maintenance: extract indexed column values and insert
        // before the memtable put (which consumes the row reference via move).
        if !self.indexed_columns.is_empty() {
            for (index_name, col_pos) in &self.indexed_columns {
                if let Some(cell) = row.cells.iter().find(|(idx, _)| *idx as usize == *col_pos) {
                    if let Some(ref value) = cell.1.value {
                        let index_key = IndexKey(value.clone());
                        let row_pos = RowPosition {
                            partition_key: key.key.as_bytes().to_vec(),
                            clustering_key: row.clustering.clone(),
                        };
                        if let Some(idx) = guard.indexes.get(index_name) {
                            idx.insert(index_key, row_pos);
                        }
                    }
                    // If value is None (tombstone), skip — no index entry for deletions
                }
            }
        }

        // Vector index maintenance: extract vector column values and insert into
        // the in-memory HNSW/brute-force index for the active memtable.
        // Row byte offset is unknown until the SSTable is written; we use 0 as a
        // placeholder. The drain→HNSW build at flush time re-inserts with
        // the final on-disk offset. For now, the memtable search is by position
        // within the memtable (ordering only), not absolute file offset.
        if !self.vector_index_configs.is_empty() {
            for cfg in &self.vector_index_configs {
                if let Some(cell) = row
                    .cells
                    .iter()
                    .find(|(idx, _)| *idx as usize == cfg.column_position)
                {
                    if let Some(ref value) = cell.1.value {
                        if let Ok(vector) = ferrosa_index::bytes_to_vec_f32(value) {
                            // Use a sequential position based on current index size
                            // (placeholder offset; not a true file offset).
                            let pos = ferrosa_index::vector::RowPosition::new(
                                guard
                                    .vector_indexes
                                    .get(&cfg.index_name)
                                    .map(|vi| vi.len() as u64)
                                    .unwrap_or(0),
                            );
                            if let Some(vi) = guard.vector_indexes.get(&cfg.index_name) {
                                vi.insert(pos, vector);
                            }
                        }
                        // If bytes_to_vec_f32 fails, the cell contains non-vector
                        // data — skip silently (the schema enforces the type).
                    }
                }
            }
        }

        // Hold the write barrier (shared) during the memtable put.
        // This prevents the flush from swapping the active memtable while
        // we're writing. Re-load the view INSIDE the barrier to ensure we
        // write to the current active, not a stale one.
        let _wb = self.write_barrier.read();
        let current = self.view.load();
        let schema = self.schema.load();
        current.active.put(key, row, &schema)
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

        // SSTables, newest first.
        // Tolerate I/O errors from individual SSTables — a corrupt or
        // format-incompatible SSTable should not prevent reading data
        // that exists in other SSTables or the memtable (FRSA-BUG-026).
        for (i, sstable) in guard.sstables.iter().enumerate() {
            match sstable.get_partition(key) {
                Ok(Some(p)) => {
                    sources.push(p);
                }
                Ok(None) => {}
                Err(e) => {
                    // Detailed diagnostic for truncated SSTable investigation.
                    let id_info = guard
                        .sstable_ids
                        .get(i)
                        .map(|(id, path)| format!("id={id} path={path:?}"))
                        .unwrap_or_else(|| format!("index={i}"));
                    let data_len = sstable.data_file_length().unwrap_or(0);
                    tracing::error!(
                        %e,
                        %id_info,
                        data_file_len = data_len,
                        sstable_count = guard.sstables.len(),
                        key = ?key.key.as_bytes(),
                        "SSTable read error: skipping corrupt partition — data may be incomplete"
                    );
                    self.sstable_read_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
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
        // Take the write barrier (exclusive) to ensure no writer is mid-put
        // during the swap. This is the critical section: after the swap, all
        // new writes go to the new active memtable, and the old memtable
        // contains a complete snapshot.
        let new_active: Arc<dyn Memtable> = new_memtable();
        let fresh_indexes = new_indexes(&self.indexed_columns);
        let fresh_vector_indexes = new_vector_indexes(&self.vector_index_configs);
        let (old_active, old_view_flushing, old_indexes, old_vector_indexes) = {
            let _wb = self.write_barrier.write(); // block all writers
            let old_view = self.view.load();
            let old_active = Arc::clone(&old_view.active);
            let old_view_flushing = old_view.flushing.clone();
            let old_indexes = Arc::clone(&old_view.indexes);
            let old_vector_indexes = Arc::clone(&old_view.vector_indexes);
            let current_sstables = Arc::clone(&old_view.sstables);
            let current_ids = Arc::clone(&old_view.sstable_ids);
            let current_sidecars = Arc::clone(&old_view.sidecar_indexes);
            drop(old_view);

            let new_view = StoreView {
                active: new_active,
                flushing: Some(Arc::clone(&old_active)),
                sstables: Arc::clone(&current_sstables),
                sstable_ids: Arc::clone(&current_ids),
                indexes: fresh_indexes,
                sidecar_indexes: Arc::clone(&current_sidecars),
                vector_indexes: fresh_vector_indexes,
            };
            new_view.check_invariants("flush:swap_active");
            self.view.store(Arc::new(new_view));
            // Write barrier released here — writers resume with the new active.
            (
                old_active,
                old_view_flushing,
                old_indexes,
                old_vector_indexes,
            )
        };

        // Step 2: Snapshot the flushing memtable.
        // Also capture any late writes from the PREVIOUS flushing memtable
        // (kept alive since the last flush). These are writes that landed
        // between the previous snapshot and the view swap.
        let prev_flushing_present = old_view_flushing.is_some();
        let mut partitions = old_active.snapshot();
        if let Some(ref prev_flushing) = old_view_flushing {
            let prev_parts = prev_flushing.snapshot();
            // Merge previous flushing data with current snapshot.
            // When the same partition key exists in both, MERGE the rows
            // (different clustering keys = different rows that must all
            // be included). The old code skipped the entire partition
            // from prev_flushing if the key existed in the current
            // snapshot, silently dropping rows with different clustering
            // keys — this was the P0 data loss bug.
            let mut existing_map: std::collections::BTreeMap<
                ferrosa_common::key::DecoratedKey,
                usize,
            > = partitions
                .iter()
                .enumerate()
                .map(|(i, p)| (p.key.clone(), i))
                .collect();
            for p in prev_parts {
                if let Some(&idx) = existing_map.get(&p.key) {
                    // Same partition key: merge rows from both.
                    partitions[idx].rows.extend(p.rows);
                } else {
                    let idx = partitions.len();
                    existing_map.insert(p.key.clone(), idx);
                    partitions.push(p);
                }
            }
        }

        let total_rows: usize = partitions.iter().map(|p| p.rows.len()).sum();
        tracing::debug!(
            partitions = partitions.len(),
            total_rows,
            prev_flushing = prev_flushing_present,
            "flush: memtable snapshot captured"
        );

        // Step 3: No-op if the memtable was empty.
        if partitions.is_empty() {
            // Re-load the live view to get current sstables (not the stale
            // capture from the top of flush) — defensive against future
            // changes to locking discipline.
            let live = self.view.load();
            let new_view = StoreView {
                active: Arc::clone(&live.active),
                flushing: None,
                sstables: Arc::clone(&live.sstables),
                sstable_ids: Arc::clone(&live.sstable_ids),
                indexes: Arc::clone(&live.indexes),
                sidecar_indexes: Arc::clone(&live.sidecar_indexes),
                vector_indexes: Arc::clone(&live.vector_indexes),
            };
            new_view.check_invariants("flush:clear_flushing");
            self.view.store(Arc::new(new_view));
            return Ok(());
        }

        // Step 4: Sort partitions by key (required by SSTableWriter).
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        // Step 5: Build the SSTable.
        // Force compression off — there is a known CRC mismatch between
        // SSTableWriter and SSTableReader for compressed data.
        let mut options = self.options.clone();
        options.compression = None;

        let schema = self.schema.load();

        // Step 5a: Quarantine-on-flush guard (Layer 2 of the timeuuid-flush-
        // wedge fix). Filter every partition's rows through the per-cell
        // length validator. Rows that fail are written as JSON lines to
        // `<flush_dir>/quarantine/<ks>.<table>.<ts>.jsonl` and removed from
        // the partition before serialisation. Layer 1 (`Memtable::put`)
        // rejects new bad writes fail-loud; Layer 2 here is the salvage
        // path for memtables that were populated before Layer 1 landed
        // (e.g., on the wedged ferrosa-memory cluster recovery). The
        // `QuarantineWriter` is constructed lazily on the first bad row
        // so a flush with zero quarantined rows leaves no trace on disk
        // — important because the engine restart-scan also uses
        // `<table_dir>/quarantine/` for SSTable corruption forensics.
        // See specs/in-process/bug-memtable-flush-wedge-truncated-
        // timeuuid-from-now-function.md.
        let quarantine_dir = self.flush_target.base_dir().to_path_buf();
        let mut quarantine_writer: Option<crate::quarantine::QuarantineWriter> = None;
        let mut total_quarantined = 0usize;
        for p in partitions.iter_mut() {
            if p.rows.is_empty() {
                continue;
            }
            let ks = schema.keyspace.clone();
            let tbl = schema.table.clone();
            let dir = quarantine_dir.clone();
            let n = crate::quarantine::filter_partition_rows(
                p,
                &schema,
                &mut quarantine_writer,
                || crate::quarantine::QuarantineWriter::new(&dir, &ks, &tbl),
            )?;
            total_quarantined += n;
        }
        // Drop partitions that lost all their rows to quarantine.
        partitions.retain(|p| !p.rows.is_empty() || p.static_row.is_some());

        if total_quarantined > 0 {
            tracing::error!(
                keyspace = %schema.keyspace,
                table = %schema.table,
                quarantined_rows = total_quarantined,
                quarantine_file = ?quarantine_writer.as_ref().map(|w| w.path().display().to_string()),
                "flush: quarantined malformed rows — see quarantine file for forensic record"
            );
        }

        let header = flush::build_serialization_header(&schema, &partitions);
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p)?;
        }
        let output = writer.finish()?;
        let reader = self.flush_target.flush(output)?;
        let new_reader = Arc::new(reader);

        // Step 5b: Build sidecar readers from the old memtable indexes and
        // persist them to disk so they survive process restarts.
        // Use the flush target's generation as the SSTable ID so it matches
        // the file names on disk (critical for compaction to find files).
        // Also advance next_gen to stay in sync.
        let gen = self.flush_target.last_generation();
        // Keep next_gen at least as high as the flush target gen + 1.
        self.next_gen
            .fetch_max(gen + 1, std::sync::atomic::Ordering::SeqCst);
        let mut raw_sidecar_entries: HashMap<String, Vec<(IndexKey, RowPosition)>> = HashMap::new();
        let mut sidecar_map: HashMap<String, SidecarReader> = HashMap::new();
        for (index_name, memtable_idx) in old_indexes.iter() {
            let entries: Vec<(IndexKey, Vec<RowPosition>)> = memtable_idx.iter().collect();
            // Flatten: each (key, positions) pair becomes multiple (key, pos) entries
            let flat_entries: Vec<(IndexKey, RowPosition)> = entries
                .into_iter()
                .flat_map(|(key, positions)| {
                    positions.into_iter().map(move |pos| (key.clone(), pos))
                })
                .collect();
            if !flat_entries.is_empty() {
                raw_sidecar_entries.insert(index_name.clone(), flat_entries.clone());
                sidecar_map.insert(
                    index_name.clone(),
                    SidecarReader::from_entries(flat_entries),
                );
            }
        }

        // Persist sidecar files to disk (no-op for in-memory flush targets).
        if let Err(e) = self.flush_target.write_sidecars(gen, &raw_sidecar_entries) {
            tracing::error!(%e, gen, "store: sidecar persist failed");
        }

        // Step 5c: Build FTI sidecar files for any full-text indexes.
        for (index_name, col_pos) in &self.fulltext_indexes {
            let mut fti_builder = ferrosa_index::fulltext::builder::FullTextIndexBuilder::new();
            for partition in &partitions {
                let pk_bytes = partition.key.key.as_bytes().to_vec();
                // Extract the text value from the target column.
                let mut text = String::new();
                for row in &partition.rows {
                    for (col_idx, cell) in &row.cells {
                        if *col_idx as usize == *col_pos {
                            if let Some(ref val) = cell.value {
                                if let Ok(s) = std::str::from_utf8(val) {
                                    text.push_str(s);
                                    text.push(' ');
                                }
                            }
                        }
                    }
                }
                if !text.is_empty() {
                    fti_builder.add_document(pk_bytes, text.trim());
                }
            }
            match fti_builder.finish() {
                Ok(fti_bytes) => {
                    if let Err(e) = self
                        .flush_target
                        .write_fti_sidecar(gen, index_name, &fti_bytes)
                    {
                        tracing::error!(%e, %index_name, gen, "store: FTI sidecar write failed");
                    }
                }
                Err(e) => {
                    tracing::error!(%e, %index_name, gen, "store: FTI build failed");
                }
            }
        }

        // Step 5e: Drain vector memtable indexes and persist as HNSW sidecar files.
        //
        // Each declared vector index is drained from the old memtable's
        // `VectorMemtableIndex`, a full HNSW graph is built from the drained
        // vectors, serialized to JSON, and written via the flush target.
        //
        // Fail policy (Fail Loud, Never Fake):
        //   - If serialization fails: ERROR log + panic in debug builds.
        //   - If the persist call fails: ERROR log + panic in debug builds.
        //   - Never silently skip: a missing vector sidecar causes ANN queries
        //     to fall back to full scans without the caller knowing.
        for cfg in &self.vector_index_configs {
            if let Some(vi) = old_vector_indexes.get(&cfg.index_name) {
                let drained = vi.drain();
                if drained.is_empty() {
                    continue;
                }

                // Build HNSW graph and serialize via the public API.
                match ferrosa_index::vector::hnsw::build_and_serialize(
                    cfg.m,
                    cfg.ef_construction,
                    cfg.metric,
                    drained,
                ) {
                    Ok(vec_bytes) => {
                        if let Err(e) =
                            self.flush_target
                                .write_vector_sidecar(gen, &cfg.index_name, &vec_bytes)
                        {
                            tracing::error!(%e, index_name = %cfg.index_name, gen,
                                "store: vector sidecar persist failed");
                            #[cfg(debug_assertions)]
                            panic!("vector sidecar persist failed: {e}");
                        } else {
                            tracing::debug!(index_name = %cfg.index_name, gen,
                                "flush: vector sidecar written");
                        }
                    }
                    Err(e) => {
                        tracing::error!(%e, index_name = %cfg.index_name, gen,
                            "store: vector sidecar serialization failed");
                        #[cfg(debug_assertions)]
                        panic!("vector sidecar serialize failed: {e}");
                    }
                }
            }
        }

        // Step 5d: Drain late writers. Any writer that loaded the view before
        // step 1 may have written to old_active AFTER our snapshot. Those writes
        // would be lost when we clear `flushing`. Re-snapshot the old memtable
        // and replay any entries not in the original flush to the new active.
        let late_partitions = old_active.snapshot();
        if !late_partitions.is_empty() {
            let current_view = self.view.load();
            let schema = self.schema.load();
            let flushed_by_key: std::collections::BTreeMap<_, _> =
                partitions.iter().map(|p| (p.key.clone(), p)).collect();
            for p in &late_partitions {
                if late_partition_needs_replay(&flushed_by_key, p) {
                    // Late write into either a brand-new partition or an existing
                    // partition that changed after the flush snapshot. Replay the
                    // current partition image into the new active memtable so the
                    // post-swap view retains those rows.
                    for row in &p.rows {
                        if let Err(e) = current_view.active.put(&p.key, row.clone(), &schema) {
                            tracing::error!(%e, "flush: late-writer replay put failed");
                        }
                    }
                }
            }
            drop(current_view);
        }

        // Step 6: Prepend new SSTable and sidecar, clear flushing.
        tracing::debug!(
            gen,
            prior_sstable_count = self.sstable_count(),
            "flush: SSTable written, updating view"
        );
        let current_view = self.view.load();
        let mut new_sstables = vec![new_reader];
        new_sstables.extend(current_view.sstables.iter().cloned());

        // Use the actual base directory from the flush target, not empty PathBuf.
        // An empty path causes ID collisions with compaction output:
        // swap_compacted_sstables matches by ID only, so a flush SSTable with
        // the same gen as a compaction input gets incorrectly removed during swap.
        let flush_dir = self.flush_target.base_dir().to_path_buf();
        let mut new_ids = vec![(format!("{gen}"), flush_dir)];
        new_ids.extend(current_view.sstable_ids.iter().cloned());

        let mut new_sidecars = vec![Arc::new(sidecar_map)];
        new_sidecars.extend(current_view.sidecar_indexes.iter().cloned());

        // Keep the old flushing memtable alive until the NEXT flush
        // replaces it. This ensures any late writers (threads that loaded
        // the view before step 1 and haven't written yet) can still write
        // to the old memtable and their data remains readable via the
        // flushing slot. The next flush's step 1 will atomically replace
        // flushing, at which point ArcSwap guarantees all prior readers
        // have released their guards.
        let new_view = StoreView {
            active: Arc::clone(&current_view.active),
            flushing: Some(old_active),
            sstables: Arc::new(new_sstables),
            sstable_ids: Arc::new(new_ids),
            indexes: Arc::clone(&current_view.indexes),
            sidecar_indexes: Arc::new(new_sidecars),
            vector_indexes: Arc::clone(&current_view.vector_indexes),
        };
        new_view.check_invariants("flush:install_new_sstable");
        self.view.store(Arc::new(new_view));

        Ok(())
    }

    /// Reads partitions from the memtable in token order with an optional
    /// token range filter and limit.
    ///
    /// Bounds partition materialization and can optionally bound retained rows
    /// per partition for safe LIMIT-first scan shapes.
    pub fn read_range(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
    ) -> Result<Vec<Partition>> {
        self.read_range_limited_rows(start, end, limit, 0)
    }

    /// COUNT(*) fast path. Returns the total row count for
    /// `[start, end]` without ever decoding cell payloads:
    /// SSTables go through `next_partition_metadata`, memtables
    /// contribute their already-in-memory partitions, and
    /// `merge::merge_partitions` does row-level dedup via clustering
    /// keys for correctness across sources and replicas. Memory
    /// peak: one merged Partition's metadata at a time.
    ///
    /// Runs on the blocking pool because the merger drives sync
    /// SSTable reads. Returns the count rather than a stream so
    /// the caller doesn't even allocate per-partition.
    pub fn count_range(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> Result<u64> {
        let view = self.view.load_full();
        let start_owned = start.cloned();
        let end_owned = end.cloned();
        let active_iter = view
            .active
            .range_iter(start_owned.as_ref(), end_owned.as_ref());
        let flushing_iter = view
            .flushing
            .as_ref()
            .map(|f| f.range_iter(start_owned.as_ref(), end_owned.as_ref()));
        let sstables_slice = &view.sstables[..];

        let mut merger = crate::range_merger::merger_for_metadata_sources(
            active_iter,
            flushing_iter,
            sstables_slice,
            start_owned,
            end_owned,
        )?;

        let mut total: u64 = 0;
        // Each row in the merged partition contributes 1 to the
        // count unless its tombstone marker covers it.
        // `merge::apply_deletions` (called inside the merger) has
        // already dropped fully-shadowed rows, so rows.len() is the
        // live count. Static rows count as one row (Cassandra
        // semantics for COUNT(*) include the static row when
        // present).
        while let Some(p) = merger.next_merged_partition()? {
            total = total.saturating_add(p.rows.len() as u64);
            if p.static_row.is_some() {
                total = total.saturating_add(1);
            }
        }
        Ok(total)
    }

    /// Projection-aware variant of `range_iter`. SSTable cells
    /// whose ordinals are NOT in `wanted` are byte-skipped via
    /// `DataReader::read_cell_skip` — saves one syscall + one heap
    /// alloc + the value-byte memcpy per skipped cell. Memtable
    /// partitions retain their full cells (already in memory).
    ///
    /// Takes `wanted` by value so the spawned blocking task can
    /// move it in; the returned stream has no borrow.
    pub fn range_iter_projected(
        &self,
        wanted: Vec<u16>,
        partition_limit: Option<usize>,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Partition>> + Send>> {
        // Buffer is intentionally small. The producer runs on a
        // spawn_blocking thread and per-partition body decode on cold
        // cache is the dominant cost (wide rows + embedding cells +
        // dedup across multiple SSTable runs sharing a key). A larger
        // buffer turns a `LIMIT N` scan into a `LIMIT N + buffer`
        // scan because the producer races ahead before the consumer
        // can drop the stream; we measured ~32 s cold-cache walls on
        // a 1.7 GB table for `LIMIT 5` with buffer=64. With buffer=4
        // *and* `partition_limit` pushed into the producer loop, the
        // producer stops cleanly after N emissions.
        const STREAM_BUFFER: usize = 4;

        let view = self.view.load_full();
        let start_owned = start.cloned();
        let end_owned = end.cloned();
        let wanted_owned = wanted;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Partition>>(STREAM_BUFFER);

        tokio::task::spawn_blocking(move || {
            let active_iter = view
                .active
                .range_iter(start_owned.as_ref(), end_owned.as_ref());
            let flushing_iter = view
                .flushing
                .as_ref()
                .map(|f| f.range_iter(start_owned.as_ref(), end_owned.as_ref()));
            let sstables_slice = &view.sstables[..];

            let mut merger = match crate::range_merger::merger_for_projected_sources(
                active_iter,
                flushing_iter,
                sstables_slice,
                &wanted_owned,
                start_owned,
                end_owned,
            ) {
                Ok(m) => m,
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                    return;
                }
            };

            let cap = partition_limit.unwrap_or(usize::MAX);
            let mut emitted: usize = 0;
            loop {
                if emitted >= cap {
                    return;
                }
                match merger.next_merged_partition() {
                    Ok(Some(partition)) => {
                        if tx.blocking_send(Ok(partition)).is_err() {
                            return;
                        }
                        emitted += 1;
                    }
                    Ok(None) => return,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e));
                        return;
                    }
                }
            }
        });

        Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }))
    }

    /// ADR-020 lazy range iterator. Returns an async `Stream` that
    /// yields every partition in `[start, end]` one at a time —
    /// memtable + flushing memtable + SSTables k-way merged, with
    /// same-key partitions merged and deletions suppressed inline.
    ///
    /// Memory profile: peak is O(num_sources) partitions held by
    /// the merger, regardless of total table size. Unlike
    /// `read_range_limited_rows` there is no
    /// `RANGE_READ_MATERIALIZATION_CAP`; the caller drives the rate
    /// of consumption via mpsc back-pressure (`STREAM_BUFFER` items).
    pub fn range_iter(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Partition>> + Send>> {
        /// Per-stream channel buffer. Kept small because per-partition
        /// decode on cold cache is expensive (wide rows + cell decode)
        /// and a `LIMIT N` consumer should pay for ~N body decodes,
        /// not N + buffer_capacity. See the matching constant in
        /// `range_iter_projected` for the LIMIT-pushdown rationale.
        const STREAM_BUFFER: usize = 4;

        let view = self.view.load_full();
        let start_owned = start.cloned();
        let end_owned = end.cloned();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Partition>>(STREAM_BUFFER);

        tokio::task::spawn_blocking(move || {
            // Build source iterators — these borrow from `view`
            // (memtable Arcs and per-SSTable Arcs) which the closure
            // owns for the task's full lifetime, so there is no
            // self-referential lifetime problem.
            let active_iter = view
                .active
                .range_iter(start_owned.as_ref(), end_owned.as_ref());
            let flushing_iter = view
                .flushing
                .as_ref()
                .map(|f| f.range_iter(start_owned.as_ref(), end_owned.as_ref()));
            let sstables_slice = &view.sstables[..];

            let mut merger = match crate::range_merger::merger_for_sources(
                active_iter,
                flushing_iter,
                sstables_slice,
                start_owned,
                end_owned,
            ) {
                Ok(m) => m,
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                    return;
                }
            };

            loop {
                match merger.next_merged_partition() {
                    Ok(Some(partition)) => {
                        if tx.blocking_send(Ok(partition)).is_err() {
                            // Consumer dropped (cancelled stream).
                            return;
                        }
                    }
                    Ok(None) => return,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e));
                        return;
                    }
                }
            }
        });

        Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }))
    }

    /// Read all partitions whose tokens fall in `[start_token, end_token)`,
    /// up to `limit` matching partitions.
    ///
    /// Anti-entropy repair needs "every partition in this Merkle leaf's
    /// token sub-range." The existing key-bounded read API can't answer
    /// that because partition keys hash to tokens; a contiguous token
    /// range is a discontiguous key range.
    ///
    /// Streaming implementation: each source (active memtable, flushing
    /// memtable, every SSTable) is walked one partition at a time via its
    /// lazy iterator. Partitions outside `[start_token, end_token)` are
    /// dropped without ever entering the result `Vec`, and once a single
    /// SSTable's iterator passes `end_token` we stop iterating it (SSTable
    /// partitions are stored in token order). Peak working-set memory is
    /// therefore `O(matches_in_range)` — one in-range partition copy per
    /// hit, plus one in-flight clone per source — NOT `O(table_size)`.
    /// This is what makes repair viable on a multi-GB table in a 2 GB
    /// container: a typical Merkle leaf has < 10 partitions, so peak
    /// is a few hundred KB per session.
    ///
    /// Returns an empty vector when the range is empty (`start >= end`)
    /// or `limit == 0`.
    pub fn read_token_range(
        &self,
        start_token: i64,
        end_token: i64,
        limit: usize,
    ) -> Result<Vec<Partition>> {
        if start_token >= end_token || limit == 0 {
            return Ok(Vec::new());
        }
        let guard = self.view.load();
        let in_range = |t: i64| t >= start_token && t < end_token;
        let mut matched: Vec<Partition> = Vec::new();

        // Active memtable: lazy iter. Per-partition deep clone happens
        // only when we advance the iterator, and only matches survive.
        for p in guard.active.range_iter(None, None) {
            if matched.len() >= limit {
                break;
            }
            if in_range(p.key.token.0) {
                matched.push(p);
            }
        }

        // Flushing memtable (if any).
        if matched.len() < limit {
            if let Some(ref flushing) = guard.flushing {
                for p in flushing.range_iter(None, None) {
                    if matched.len() >= limit {
                        break;
                    }
                    if in_range(p.key.token.0) {
                        matched.push(p);
                    }
                }
            }
        }

        // SSTables: walk each via `partitions_iter()` (yields one
        // partition at a time). SSTable partitions are token-ordered,
        // so we bail out of this SSTable's iterator as soon as we see
        // a token `>= end_token`. We also jump straight to the first
        // partition with `token >= start_token` via `seek_to_token`
        // (O(log N) via the SSTable's lazy `partition_token_offsets`
        // cache) so each repair session pays O(matches), not
        // O(table_size).
        for (i, sstable) in guard.sstables.iter().enumerate() {
            if matched.len() >= limit {
                break;
            }
            let mut iter = match sstable.partitions_iter() {
                Ok(it) => it,
                Err(e) => {
                    let id = guard
                        .sstable_ids
                        .get(i)
                        .map(|(gen, dir)| format!("{}/{gen}", dir.display()))
                        .unwrap_or_else(|| format!("index={i}"));
                    tracing::warn!(
                        sstable = %id,
                        "read_token_range: skipping SSTable with broken iterator: {e}"
                    );
                    continue;
                }
            };
            if let Err(e) = iter.seek_to_token(start_token) {
                let id = guard
                    .sstable_ids
                    .get(i)
                    .map(|(gen, dir)| format!("{}/{gen}", dir.display()))
                    .unwrap_or_else(|| format!("index={i}"));
                tracing::warn!(
                    sstable = %id,
                    "read_token_range: seek_to_token failed, falling back to full scan: {e}"
                );
                // Iter is still at byte 0; the per-partition token
                // filter below will handle correctness, just slower.
            }
            while matched.len() < limit {
                match iter.next_partition() {
                    Ok(Some(p)) => {
                        let t = p.key.token.0;
                        if t >= end_token {
                            break; // SSTable is token-sorted — done with this source.
                        }
                        if t >= start_token {
                            matched.push(p);
                        }
                    }
                    Ok(None) => break, // EOF.
                    Err(e) => {
                        tracing::warn!("read_token_range: SSTable partition decode error: {e}");
                        break;
                    }
                }
            }
        }

        // Cross-source dedup + cell-level merge (same shape as
        // `read_range_limited_rows` so range and token reads return
        // semantically identical results for the same window).
        matched.sort_by(|a, b| a.key.cmp(&b.key));
        let mut merged: Vec<Partition> = Vec::new();
        for p in matched {
            if let Some(last) = merged.last_mut() {
                if last.key == p.key {
                    *last = merge::merge_partitions(vec![last.clone(), p]);
                    continue;
                }
            }
            merged.push(p);
        }
        for p in &mut merged {
            merge::apply_deletions(p);
        }
        Ok(merged.into_iter().take(limit).collect())
    }

    pub fn read_range_limited_rows(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
        row_limit: usize,
    ) -> Result<Vec<Partition>> {
        if limit > RANGE_READ_MATERIALIZATION_CAP {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "range read limit {limit} exceeds materialization cap {RANGE_READ_MATERIALIZATION_CAP}; use a paged/streaming read path"
            )));
        }

        let guard = self.view.load();

        // Collect partitions from bounded sources only. This is still a
        // materializing read path, so fail closed once the requested window is
        // exhausted instead of continuing to decode arbitrary table volume.
        let mut all_partitions: Vec<Partition> = Vec::new();

        let trim_rows = |partitions: &mut Vec<Partition>| {
            if row_limit > 0 {
                for partition in partitions {
                    partition.rows.truncate(row_limit);
                }
            }
        };

        // Active memtable: clone only the requested window, not the whole
        // active memtable. This keeps CQL LIMIT/page-size scans from hanging
        // behind full-table materialization.
        let mut active = guard.active.snapshot_range_limited(start, end, limit);
        trim_rows(&mut active);
        all_partitions.extend(active);

        // Flushing memtable
        if all_partitions.len() < limit {
            if let Some(ref flushing) = guard.flushing {
                let remaining = limit.saturating_sub(all_partitions.len());
                let mut flushing_parts = flushing.snapshot_range_limited(start, end, remaining);
                trim_rows(&mut flushing_parts);
                all_partitions.extend(flushing_parts);
            }
        }

        // SSTables — read only the remaining budget from each, and when a row
        // cap is requested skip unretained rows while decoding instead of
        // materializing full wide partitions and truncating afterwards.
        for (i, sstable) in guard.sstables.iter().enumerate() {
            let remaining = limit.saturating_sub(all_partitions.len());
            if remaining == 0 {
                break;
            }
            match sstable.read_partitions_limited_rows(remaining, row_limit) {
                Ok(parts) => all_partitions.extend(parts),
                Err(e) => {
                    let id = guard
                        .sstable_ids
                        .get(i)
                        .map(|(gen, dir)| format!("{}/{gen}", dir.display()))
                        .unwrap_or_else(|| format!("index={i}"));
                    tracing::warn!(
                        sstable = %id,
                        "read_range: skipping corrupted SSTable: {e}"
                    );
                }
            }
        }

        // Deduplicate and merge partitions with the same key
        all_partitions.sort_by(|a, b| a.key.cmp(&b.key));
        let mut merged: Vec<Partition> = Vec::new();
        for p in all_partitions {
            if let Some(last) = merged.last_mut() {
                if last.key == p.key {
                    *last = merge::merge_partitions(vec![last.clone(), p]);
                    continue;
                }
            }
            merged.push(p);
        }

        // Apply deletion suppression to all partitions. Partitions that
        // came from a single source (no multi-source merge above) still
        // need row-level and partition-level deletions applied because
        // the memtable's merge-on-write sets deletion markers but does
        // not suppress the covered cells.
        for p in &mut merged {
            merge::apply_deletions(p);
        }

        // Apply range filter and limit
        let filtered: Vec<Partition> = merged
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

    /// Query by secondary index: looks up the index key in the memtable
    /// index (and, in future, all SSTable sidecar indexes), fetches the
    /// matching partitions, and returns merged results.
    ///
    /// Returns an error if the number of matching row positions exceeds
    /// `INDEX_RESULT_CAP` (10,000) to prevent OOM on high-cardinality
    /// index values. The error message suggests `ALLOW FILTERING` for
    /// unbounded scans.
    ///
    /// Deduplicates by `(partition_key, clustering_key)` so that the same
    /// row appearing in both memtable and sidecar indexes is returned once.
    pub fn read_by_index(&self, index_name: &str, key: &IndexKey) -> Result<Vec<Partition>> {
        let guard = self.view.load();

        let mut positions: Vec<RowPosition> = Vec::new();
        let mut append_positions = |batch: Vec<RowPosition>| -> Result<()> {
            if positions.len().saturating_add(batch.len()) > INDEX_RESULT_CAP {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "secondary index query exceeded {} row limit; \
                     use ALLOW FILTERING for unbounded scans",
                    INDEX_RESULT_CAP
                )));
            }
            positions.extend(batch);
            Ok(())
        };

        // 1. Query memtable index
        if let Some(idx) = guard.indexes.get(index_name) {
            append_positions(idx.lookup(key))?;
        }

        // 2. Query SSTable sidecar indexes
        for sidecar in guard.sidecar_indexes.iter() {
            if let Some(reader) = sidecar.get(index_name) {
                if let Ok(results) = reader.lookup(key) {
                    append_positions(results)?;
                }
            }
        }

        // 4. Deduplicate by (partition_key, clustering_key)
        let mut seen = std::collections::HashSet::new();
        positions.retain(|p| seen.insert((p.partition_key.clone(), p.clustering_key.clone())));

        // 5. Fetch actual partitions by partition key
        let mut partitions = Vec::new();
        for pos in &positions {
            let dk = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
                pos.partition_key.clone(),
            ));
            if let Ok(Some(partition)) = self.read(&dk) {
                partitions.push(partition);
            }
        }

        Ok(partitions)
    }

    /// Retrieve a named memtable-level secondary index.
    ///
    /// Returns `None` if no index with the given name was declared at
    /// Dynamically adds a secondary index. Future writes will be indexed.
    pub fn add_index(&mut self, index_name: String, column_position: usize) {
        self.indexed_columns
            .push((index_name.clone(), column_position));
        let current = self.view.load();
        let mut new_indexes = (*current.indexes).clone();
        new_indexes.insert(index_name, Arc::new(MemtableIndex::new()));
        let new_view = StoreView {
            active: Arc::clone(&current.active),
            flushing: current.flushing.clone(),
            sstables: Arc::clone(&current.sstables),
            sstable_ids: Arc::clone(&current.sstable_ids),
            indexes: Arc::new(new_indexes),
            sidecar_indexes: Arc::clone(&current.sidecar_indexes),
            vector_indexes: Arc::clone(&current.vector_indexes),
        };
        new_view.check_invariants("update_indexes");
        self.view.store(Arc::new(new_view));
    }

    /// Retrieve a named memtable-level secondary index.
    ///
    /// Returns `None` if no index with the given name was declared at
    /// construction time. The returned `Arc` is a snapshot — it remains
    /// valid even after a flush swaps in fresh indexes.
    pub fn get_memtable_index(&self, name: &str) -> Option<Arc<MemtableIndex>> {
        let guard = self.view.load();
        guard.indexes.get(name).cloned()
    }

    /// Returns the current secondary index declarations.
    pub fn indexed_columns(&self) -> &[(String, usize)] {
        &self.indexed_columns
    }

    /// Returns the current full-text index declarations.
    pub fn fulltext_indexes(&self) -> &[(String, usize)] {
        &self.fulltext_indexes
    }

    /// Register a full-text index for this table.
    pub fn add_fulltext_index(&mut self, index_name: String, column_position: usize) {
        if !self.fulltext_indexes.iter().any(|(n, _)| n == &index_name) {
            self.fulltext_indexes.push((index_name, column_position));
        }
    }

    /// Register a vector index for this table.
    ///
    /// Idempotent: calling twice with the same `index_name` is a no-op.
    /// Updates both `vector_index_configs` and the live `StoreView` so that
    /// subsequent writes begin populating the in-memory vector index
    /// immediately.
    pub fn add_vector_index(&mut self, config: VectorIndexConfig) {
        if self
            .vector_index_configs
            .iter()
            .any(|c| c.index_name == config.index_name)
        {
            return; // already registered
        }

        // Insert an empty VectorMemtableIndex into the current view so that
        // writes made after this call are indexed immediately.
        let current = self.view.load();
        let mut new_vi = (*current.vector_indexes).clone();
        new_vi.insert(
            config.index_name.clone(),
            Arc::new(VectorMemtableIndex::new(
                config.metric,
                config.m,
                config.ef_construction,
            )),
        );
        let new_view = StoreView {
            active: Arc::clone(&current.active),
            flushing: current.flushing.clone(),
            sstables: Arc::clone(&current.sstables),
            sstable_ids: Arc::clone(&current.sstable_ids),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::clone(&current.sidecar_indexes),
            vector_indexes: Arc::new(new_vi),
        };
        new_view.check_invariants("add_vector_index");
        self.view.store(Arc::new(new_view));
        self.vector_index_configs.push(config);
    }

    /// Perform an approximate nearest-neighbor search across memtable and
    /// all flushed SSTable vector sidecars.
    ///
    /// Searches the active (and optionally flushing) memtable via brute-force,
    /// then queries each SSTable's persisted HNSW sidecar via the flush target.
    /// Results from all sources are merged, deduplicated by `position.offset`,
    /// sorted ascending by score, and truncated to `k`.
    ///
    /// Returns `Ok(Vec::new())` when the index has no data or no sidecar
    /// exists for `index_name`.
    pub fn ann_search(
        &self,
        index_name: &str,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<ferrosa_index::vector::IndexResult>> {
        use ferrosa_index::vector::IndexResult;
        use std::collections::HashMap as StdHashMap;

        let guard = self.view.load();
        let mut merged: StdHashMap<u64, IndexResult> = StdHashMap::new();

        // 1. Search active memtable.
        if let Some(vi) = guard.vector_indexes.get(index_name) {
            let results = vi.search(query, k, ef_search).map_err(|e| {
                ferrosa_common::Error::InvalidData(format!("ann_search memtable failed: {e}"))
            })?;
            for r in results {
                merged.insert(r.position.offset, r);
            }
        }

        // 2. Search flushing memtable is handled by the existing vector_indexes
        // snapshot: the flushing memtable's VectorMemtableIndex is drained at
        // flush start, so active is the only in-flight index we need to query.

        // 3. Search each SSTable's persisted HNSW sidecar.
        for (gen_str, _dir) in guard.sstable_ids.iter() {
            if let Ok(gen) = gen_str.parse::<u64>() {
                if let Some(vec_bytes) = self.flush_target.read_vector_sidecar(gen, index_name) {
                    match ferrosa_index::vector::hnsw::search_from_bytes(
                        &vec_bytes, query, k, ef_search,
                    ) {
                        Ok(results) => {
                            for r in results {
                                merged.insert(r.position.offset, r);
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                %e, index_name, gen,
                                "ann_search: HNSW sidecar search failed"
                            );
                        }
                    }
                }
            }
        }

        // 4. Deduplicate, sort ascending by score, truncate to k.
        let mut all: Vec<IndexResult> = merged.into_values().collect();
        all.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(k);
        Ok(all)
    }

    /// Returns the generation number of the most recently flushed SSTable.
    pub fn last_flush_generation(&self) -> u64 {
        self.flush_target.last_generation()
    }

    /// Returns generation IDs for all SSTables currently in the store.
    ///
    /// Used by `add_index` to submit backfill jobs for existing SSTables.
    /// Returns IDs based on the flush target's generation counter: the most
    /// recent flush is `last_generation`, and prior ones count down from there.
    pub fn sstable_generation_ids(&self) -> Vec<String> {
        let count = self.sstable_count();
        let last_gen = self.flush_target.last_generation();
        // Generations are numbered 1..=last_gen.
        // The store holds `count` SSTables (may be fewer than last_gen after compaction).
        // Return the most recent `count` generation IDs.
        if count == 0 || last_gen == 0 {
            return vec![];
        }
        let start = last_gen.saturating_sub(count as u64) + 1;
        (start..=last_gen).map(|g| format!("{g}")).collect()
    }

    /// Number of SSTables currently in the store.
    pub fn sstable_count(&self) -> usize {
        self.view.load().sstables.len()
    }

    /// Allocate a new unique SSTable generation ID.
    pub fn next_sstable_id(&self) -> String {
        format!(
            "{}",
            self.next_gen
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        )
    }

    /// Advance the internal generation counter to at least `min_gen + 1`.
    /// Also advances the flush target's generation so file names don't collide.
    pub fn advance_gen_past(&self, min_gen: u64) {
        self.next_gen
            .fetch_max(min_gen + 1, std::sync::atomic::Ordering::SeqCst);
        self.flush_target.advance_generation(min_gen);
    }

    /// Approximate memory usage of the active memtable in bytes.
    pub fn memtable_size(&self) -> usize {
        self.view.load().active.size_bytes()
    }

    /// Number of partitions in the active memtable.
    pub fn memtable_partition_count(&self) -> usize {
        self.view.load().active.partition_count()
    }

    /// Number of entries in the active MemtableIndex for the given index name.
    ///
    /// Returns 0 if the index does not exist. Used to verify that eager
    /// index builds keep the in-memory index bounded.
    pub fn memtable_index_entry_count(&self, index_name: &str) -> usize {
        let guard = self.view.load();
        guard
            .indexes
            .get(index_name)
            .map(|idx| idx.iter().count())
            .unwrap_or(0)
    }

    /// Truncate (clear) all data: replaces the memtable with a fresh one
    /// and drops all SSTable references.
    ///
    /// Existing readers holding `Arc` references to the old memtable or
    /// SSTables will complete normally; the data is freed once those
    /// references drop. On-disk SSTable files remain until GC.
    pub fn truncate(&self) {
        let _guard = self.flush_guard.lock();
        let new_view = StoreView {
            active: new_memtable(),
            flushing: None,
            sstables: Arc::new(vec![]),
            sstable_ids: Arc::new(vec![]),
            indexes: new_indexes(&self.indexed_columns),
            sidecar_indexes: Arc::new(vec![]),
            vector_indexes: new_vector_indexes(&self.vector_index_configs),
        };
        new_view.check_invariants("truncate");
        self.view.store(Arc::new(new_view));
    }

    /// Atomically replace input SSTables with a compacted output SSTable.
    ///
    /// Identifies input SSTables by their `(id, path)` pair — not just by ID —
    /// because different directories (flush vs compaction) can produce the same
    /// generation number. Matching on both fields prevents accidental removal
    /// of an SSTable that happens to share a gen with an input in a different dir.
    pub fn swap_compacted_sstables(
        &self,
        input_ids: &[(String, std::path::PathBuf)],
        output_id: String,
        output_path: std::path::PathBuf,
        add: Arc<SSTableReader<F::Reader>>,
        output_sidecars: HashMap<String, SidecarReader>,
    ) -> Result<()> {
        let _guard = self.flush_guard.lock();
        let current = self.view.load();

        // Keep SSTables whose ID is NOT in the compaction input set.
        // Match on ID only — the path in the view may be empty (from flush)
        // while the compaction task resolves it to the table directory. Matching
        // on (id, path) caused inputs to never be removed, leaving stale
        // references to deleted files that silently lost data on reads.
        let input_id_set: std::collections::HashSet<&str> =
            input_ids.iter().map(|(id, _)| id.as_str()).collect();

        let mut new_sstables = Vec::with_capacity(current.sstables.len());
        let mut new_ids = Vec::with_capacity(current.sstable_ids.len());
        let mut new_sidecars = Vec::with_capacity(current.sidecar_indexes.len());

        for (i, id_entry) in current.sstable_ids.iter().enumerate() {
            if !input_id_set.contains(id_entry.0.as_str()) {
                new_sstables.push(Arc::clone(&current.sstables[i]));
                new_ids.push(id_entry.clone());
                if i < current.sidecar_indexes.len() {
                    new_sidecars.push(Arc::clone(&current.sidecar_indexes[i]));
                }
            }
        }

        // Prepend the compacted output.
        new_sstables.insert(0, add);
        new_ids.insert(0, (output_id, output_path));
        new_sidecars.insert(0, Arc::new(output_sidecars));

        let new_view = StoreView {
            active: Arc::clone(&current.active),
            flushing: current.flushing.clone(),
            sstables: Arc::new(new_sstables),
            sstable_ids: Arc::new(new_ids),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::new(new_sidecars),
            vector_indexes: Arc::clone(&current.vector_indexes),
        };
        new_view.check_invariants("swap_compacted");
        self.view.store(Arc::new(new_view));
        Ok(())
    }

    /// Collects sidecar entries from SSTables matching the given `(id, path)` pairs for merging.
    pub fn collect_compaction_sidecar_entries(
        &self,
        input_ids: &[(String, std::path::PathBuf)],
    ) -> HashMap<String, Vec<(IndexKey, RowPosition)>> {
        let guard = self.view.load();
        let mut merged: HashMap<String, Vec<(IndexKey, RowPosition)>> = HashMap::new();
        for (i, id_entry) in guard.sstable_ids.iter().enumerate() {
            if input_ids.contains(id_entry) {
                if let Some(sidecar_map) = guard.sidecar_indexes.get(i) {
                    for (index_name, reader) in sidecar_map.as_ref() {
                        merged
                            .entry(index_name.clone())
                            .or_default()
                            .extend(reader.all_entries());
                    }
                }
            }
        }
        merged
    }

    /// Collect metadata for all current SSTables.
    ///
    /// Used by the compaction strategy to decide which SSTables to merge.
    /// The `table_dir` is the directory where this table's SSTable files
    /// reside (e.g., `{data_dir}/sstables/{table_id}`).
    pub fn sstable_metadata(
        &self,
        table_dir: &std::path::Path,
    ) -> Vec<crate::compaction::metadata::SSTableMetadata> {
        let guard = self.view.load();

        // Invariant: sstables and sstable_ids must have equal length — each
        // in-memory SSTable reader has exactly one registered (id, path).
        // If they desync, the old code silently synthesized fake integer IDs
        // via `format!("{}", i + 1)`, which the compaction executor then
        // tried to read as `{i+1}-Data.db` — always ENOENT, burning cycles
        // and driving the node toward OOM. Fail loud instead: log the
        // invariant violation with full context, drop the desynced tail,
        // and let compaction only plan over the synchronized prefix.
        let n_sst = guard.sstables.len();
        let n_ids = guard.sstable_ids.len();
        if n_sst != n_ids {
            tracing::error!(
                sstables_len = n_sst,
                sstable_ids_len = n_ids,
                table_dir = ?table_dir,
                "INVARIANT VIOLATED: StoreView.sstables and StoreView.sstable_ids \
                 have different lengths. This is a latent bug in view construction. \
                 Dropping desynced tail entries from compaction planning to avoid \
                 phantom SSTable references (e.g. `20-Data.db` for a file that \
                 was never written). Please file a bug with these lengths."
            );
        }
        let synced_len = n_sst.min(n_ids);

        guard
            .sstables
            .iter()
            .take(synced_len)
            .enumerate()
            .map(|(i, sst)| {
                let header = sst.header();

                // WP-001: compute size from SSTable component buffers
                let size_bytes = sst.total_size();

                // WP-002: compute tokens from the smallest/largest raw key
                // bytes stored in the partition index. SSTables are sorted
                // by token, so first key = min token, last key = max token.
                use ferrosa_common::Token;
                let min_token = Token::from_key(sst.smallest_key_bytes()).0;
                let max_token = Token::from_key(sst.largest_key_bytes()).0;

                // Safe to index: i < synced_len <= sstable_ids.len().
                let (id, path) = &guard.sstable_ids[i];
                let sstable_path = if path.as_os_str().is_empty() {
                    table_dir.to_path_buf()
                } else {
                    path.clone()
                };

                crate::compaction::metadata::SSTableMetadata {
                    id: id.clone(),
                    path: sstable_path,
                    size_bytes,
                    min_token,
                    max_token,
                    min_timestamp: header.min_timestamp,
                    max_timestamp: header.max_timestamp,
                    partition_count: sst.key_count(),
                }
            })
            .collect()
    }
}

fn late_partition_needs_replay(
    flushed_by_key: &std::collections::BTreeMap<ferrosa_common::key::DecoratedKey, &Partition>,
    late_partition: &Partition,
) -> bool {
    match flushed_by_key.get(&late_partition.key) {
        None => true,
        Some(flushed_partition) => *flushed_partition != late_partition,
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
            extensions: Default::default(),
        }
    }

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_row(value: &[u8], timestamp: i64) -> Row {
        make_row_with_ck(1, value, timestamp)
    }

    fn make_row_with_ck(ck: i32, value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering: ck.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    fn test_store() -> TableStore<InMemoryFlushTarget> {
        TableStore::new(
            test_schema(),
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        )
    }

    #[test]
    fn range_read_rejects_unbounded_materialization_limit() {
        let store = test_store();

        let err = store
            .read_range(None, None, RANGE_READ_MATERIALIZATION_CAP + 1)
            .expect_err("range reads above the materialization cap must fail closed");

        assert!(
            err.to_string().contains("paged/streaming read path"),
            "error should direct callers away from materializing scans: {err}"
        );
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

    #[test]
    fn late_partition_replay_detects_changes_within_existing_partition() {
        let key = make_key("pk1");
        let flushed = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![make_row_with_ck(1, b"before", 1000)],
        };
        let late_same_key = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![
                make_row_with_ck(1, b"before", 1000),
                make_row_with_ck(2, b"late", 2000),
            ],
        };
        let flushed_by_key = std::collections::BTreeMap::from([(key.clone(), &flushed)]);

        assert!(
            late_partition_needs_replay(&flushed_by_key, &late_same_key),
            "late writes that add rows to an existing partition must be replayed"
        );
    }

    #[test]
    fn late_partition_replay_skips_unchanged_existing_partition() {
        let key = make_key("pk1");
        let flushed = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![make_row_with_ck(1, b"before", 1000)],
        };
        let flushed_by_key = std::collections::BTreeMap::from([(key.clone(), &flushed)]);

        assert!(
            !late_partition_needs_replay(&flushed_by_key, &flushed),
            "unchanged partitions should not be replayed into the new active memtable"
        );
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

    // -------------------------------------------------------------------------
    // Test 11: write to indexed column appears in memtable index
    // -------------------------------------------------------------------------
    #[test]
    fn write_indexed_column_appears_in_memtable_index() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "email".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        // Create store with an index on "email" (regular column index 0)
        let indexed_columns = vec![("email_idx".to_string(), 0_usize)];
        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            indexed_columns,
        );

        let key = make_key("user1");
        let row = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(b"alice@example.com".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };

        store.write(&key, row).unwrap();

        // The memtable index should contain the email value
        let index = store
            .get_memtable_index("email_idx")
            .expect("index must exist");
        let results = index.lookup(&IndexKey(b"alice@example.com".to_vec()));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"user1");
    }

    // -------------------------------------------------------------------------
    // Test 12: multiple writes to indexed column accumulate in index
    // -------------------------------------------------------------------------
    #[test]
    fn multiple_writes_indexed_column_accumulate() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "city".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("city_idx".to_string(), 0_usize)],
        );

        // Two different partition keys with the same indexed value
        let row1 = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(b"NYC".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        let row2 = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(b"NYC".to_vec(), 2000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(2000),
        };

        store.write(&make_key("user1"), row1).unwrap();
        store.write(&make_key("user2"), row2).unwrap();

        let index = store
            .get_memtable_index("city_idx")
            .expect("index must exist");
        let results = index.lookup(&IndexKey(b"NYC".to_vec()));
        assert_eq!(results.len(), 2);

        let pks: Vec<&[u8]> = results.iter().map(|r| r.partition_key.as_slice()).collect();
        assert!(pks.contains(&b"user1".as_slice()));
        assert!(pks.contains(&b"user2".as_slice()));
    }

    // -------------------------------------------------------------------------
    // Test 13: tombstone write does not insert into index
    // -------------------------------------------------------------------------
    #[test]
    fn tombstone_write_skips_index() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
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
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("val_idx".to_string(), 0_usize)],
        );

        // Write a tombstone (cell with no value)
        let row = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::tombstone(1000, 1700000000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };

        store.write(&make_key("user1"), row).unwrap();

        let index = store
            .get_memtable_index("val_idx")
            .expect("index must exist");
        // Tombstones should not appear in the index
        let results = index.lookup(&IndexKey(b"anything".to_vec()));
        assert!(results.is_empty());
    }

    // -------------------------------------------------------------------------
    // Test 14: flush resets the memtable index
    // -------------------------------------------------------------------------
    #[test]
    fn flush_resets_memtable_index() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "email".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("email_idx".to_string(), 0_usize)],
        );

        let row = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(b"alice@example.com".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        store.write(&make_key("user1"), row).unwrap();

        // Verify index has the entry before flush
        let pre_flush_index = store
            .get_memtable_index("email_idx")
            .expect("index must exist");
        assert_eq!(
            pre_flush_index
                .lookup(&IndexKey(b"alice@example.com".to_vec()))
                .len(),
            1
        );

        // Flush — index should be reset
        store.flush().unwrap();

        let post_flush_index = store
            .get_memtable_index("email_idx")
            .expect("index must exist after flush");
        assert!(
            post_flush_index
                .lookup(&IndexKey(b"alice@example.com".to_vec()))
                .is_empty(),
            "index should be empty after flush"
        );
    }

    // -------------------------------------------------------------------------
    // Test 15: no-index store works unchanged (backward compatibility)
    // -------------------------------------------------------------------------
    #[test]
    fn no_index_store_backward_compatible() {
        // The original `new()` constructor should still work identically
        let store = test_store();
        let key = make_key("pk1");
        store.write(&key, make_row(b"hello", 1000)).unwrap();

        let result = store.read(&key).unwrap();
        assert!(result.is_some());

        // get_memtable_index returns None for non-existent indexes
        assert!(store.get_memtable_index("nonexistent").is_none());
    }

    // -------------------------------------------------------------------------
    // Test 16: swap_compacted_sstables atomically replaces inputs with output
    // -------------------------------------------------------------------------
    #[test]
    fn swap_compacted_sstables_replaces_inputs() {
        let store = test_store();

        // Create 3 SSTables via flush.
        for i in 0..3 {
            store
                .write(
                    &make_key(&format!("k{i}")),
                    make_row(format!("v{i}").as_bytes(), i as i64 * 1000),
                )
                .unwrap();
            store.flush().unwrap();
        }
        assert_eq!(store.sstable_count(), 3);

        // Create a new SSTable to be the compaction output (flush a new entry).
        store
            .write(&make_key("compacted"), make_row(b"merged", 9000))
            .unwrap();
        store.flush().unwrap();
        let view = store.view.load();
        let new_sst = Arc::clone(&view.sstables[0]);
        drop(view);

        // Get the actual stored IDs.
        let view = store.view.load();
        let current_id_paths: Vec<(String, std::path::PathBuf)> =
            view.sstable_ids.iter().cloned().collect();
        drop(view);
        // Remove the 2 oldest (last 2 in the list).
        let input_id_paths: Vec<(String, std::path::PathBuf)> =
            current_id_paths.iter().rev().take(2).cloned().collect();

        store
            .swap_compacted_sstables(
                &input_id_paths,
                "compacted".to_string(),
                std::path::PathBuf::new(),
                new_sst,
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(store.sstable_count(), 3); // 4 - 2 + 1 = 3

        // Verify output is present and inputs are gone.
        let view = store.view.load();
        assert!(
            view.sstable_ids.iter().any(|(id, _)| id == "compacted"),
            "compacted output should be present"
        );
        for (id, path) in &input_id_paths {
            assert!(
                !view
                    .sstable_ids
                    .iter()
                    .any(|entry| entry == &(id.clone(), path.clone())),
                "input {id} should be removed"
            );
        }
    }

    /// P0 data loss: two flushes to same partition key, different clustering
    /// keys. The second flush must include rows from both memtables, not
    /// just the latest. The old code skipped prev_flushing rows when the
    /// partition key already existed in the current snapshot.
    #[test]
    fn consecutive_flushes_same_partition_merge_rows() {
        let store = test_store();

        // Batch 1: write row with clustering key "ck1".
        let key = make_key("pk1");
        let row1 = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01], // ck = 1
            cells: vec![(0, ferrosa_common::CellValue::live(b"batch1".to_vec(), 100))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(100),
        };
        store.write(&key, row1).unwrap();
        store.flush().unwrap();

        // Batch 2: write row with DIFFERENT clustering key "ck2" to SAME partition.
        let row2 = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x02], // ck = 2
            cells: vec![(0, ferrosa_common::CellValue::live(b"batch2".to_vec(), 200))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(200),
        };
        store.write(&key, row2).unwrap();
        store.flush().unwrap();

        // Read: BOTH rows must be present.
        let result = store.read(&key).unwrap();
        assert!(result.is_some(), "partition must exist");
        let partition = result.unwrap();
        assert!(
            partition.rows.len() >= 2,
            "BUG: expected 2 rows (ck=1 from batch1, ck=2 from batch2), got {}. \
             Rows from first flush were dropped during second flush.",
            partition.rows.len()
        );
    }

    /// RED TEST: consecutive flushes must produce SSTables with rows in
    /// sorted clustering key order. The prev_flushing merge path (extend)
    /// can produce unsorted rows, which corrupts the SSTable — the reader
    /// misaligns and skips data, causing data loss after compaction.
    #[test]
    fn consecutive_flushes_produce_sorted_rows_in_sstable() {
        let store = test_store();
        let key = make_key("pk1");

        // Batch 1: write rows with clustering keys 1, 3, 5 (odd)
        for ck in [1u32, 3, 5] {
            let row = Row {
                clustering: ck.to_be_bytes().to_vec(),
                cells: vec![(
                    0,
                    ferrosa_common::CellValue::live(format!("batch1_ck{ck}").into_bytes(), 1000),
                )],
                deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
            };
            store.write(&key, row).unwrap();
        }
        store.flush().unwrap();

        // Batch 2: write rows with clustering keys 2, 4, 6 (even)
        // These interleave with batch 1's keys.
        for ck in [2u32, 4, 6] {
            let row = Row {
                clustering: ck.to_be_bytes().to_vec(),
                cells: vec![(
                    0,
                    ferrosa_common::CellValue::live(format!("batch2_ck{ck}").into_bytes(), 2000),
                )],
                deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(2000),
            };
            store.write(&key, row).unwrap();
        }
        store.flush().unwrap();

        // Read back: all 6 rows must be present and in sorted order
        let result = store.read(&key).unwrap().expect("partition must exist");
        assert_eq!(
            result.rows.len(),
            6,
            "expected 6 rows (3 from batch1 + 3 from batch2), got {}",
            result.rows.len()
        );

        // Verify rows are in sorted clustering key order
        let clustering_keys: Vec<u32> = result
            .rows
            .iter()
            .map(|r| u32::from_be_bytes(r.clustering[..4].try_into().unwrap()))
            .collect();
        let mut sorted = clustering_keys.clone();
        sorted.sort();
        assert_eq!(
            clustering_keys, sorted,
            "rows must be in sorted clustering key order after flush merge, \
             got {:?}",
            clustering_keys
        );
    }

    /// Reproduces the P0 data loss bug: flush stores SSTables with empty
    /// PathBuf, but compaction passes the real path. If swap matches on
    /// (id, path), the inputs are never removed — leaving stale references
    /// to files that will be deleted, causing silent data loss.
    #[test]
    fn swap_compacted_sstables_matches_by_id_not_path() {
        let store = test_store();

        // Flush 2 SSTables — they get PathBuf::new() in the view.
        store.write(&make_key("a"), make_row(b"val_a", 1)).unwrap();
        store.flush().unwrap();
        store.write(&make_key("b"), make_row(b"val_b", 2)).unwrap();
        store.flush().unwrap();
        assert_eq!(store.sstable_count(), 2);

        // Get the IDs (they have empty paths from flush).
        let view = store.view.load();
        let ids: Vec<String> = view.sstable_ids.iter().map(|(id, _)| id.clone()).collect();
        drop(view);
        assert_eq!(ids.len(), 2);

        // Simulate what compaction does: pass the IDs with a REAL path
        // (not the empty PathBuf that flush stored).
        let fake_path = std::path::PathBuf::from("/data/sstables/test_ks.test_table");
        let input_ids_with_real_path: Vec<(String, std::path::PathBuf)> = ids
            .iter()
            .map(|id| (id.clone(), fake_path.clone()))
            .collect();

        // Create a compaction output SSTable.
        store
            .write(&make_key("merged"), make_row(b"merged", 3))
            .unwrap();
        store.flush().unwrap();
        let view = store.view.load();
        let output_sst = Arc::clone(&view.sstables[0]);
        drop(view);

        // Swap: this MUST remove the 2 inputs even though their paths
        // don't match the view's empty PathBuf.
        store
            .swap_compacted_sstables(
                &input_ids_with_real_path,
                "output".to_string(),
                fake_path,
                output_sst,
                HashMap::new(),
            )
            .unwrap();

        // Before the fix, this was 4 (2 inputs kept + output + merged).
        // After the fix, inputs are removed: 3 - 2 + 1 = 2.
        assert_eq!(
            store.sstable_count(),
            2,
            "compaction swap must remove inputs by ID regardless of path mismatch"
        );

        // Verify input IDs are gone from the view.
        let view = store.view.load();
        let remaining_ids: Vec<&str> = view.sstable_ids.iter().map(|(id, _)| id.as_str()).collect();
        for id in &ids {
            assert!(
                !remaining_ids.contains(&id.as_str()),
                "input SSTable {id} must be removed after compaction swap"
            );
        }
    }

    // =========================================================================
    // Task 5: read_by_index
    // =========================================================================

    #[test]
    fn read_by_index_returns_matching_rows_from_memtable() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "email".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("email_idx".to_string(), 0_usize)],
        );

        store
            .write(
                &make_key("user1"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"alice@test.com".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();

        store
            .write(
                &make_key("user2"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"bob@test.com".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();

        let results = store
            .read_by_index("email_idx", &IndexKey(b"alice@test.com".to_vec()))
            .unwrap();
        assert_eq!(results.len(), 1, "expected exactly one matching partition");
        assert_eq!(results[0].key.key.as_bytes(), b"user1");
    }

    #[test]
    fn read_by_index_deduplicates_same_partition() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "city".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("city_idx".to_string(), 0_usize)],
        );

        store
            .write(
                &make_key("user1"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"NYC".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();

        store
            .write(
                &make_key("user2"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"NYC".to_vec(), 2000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(2000),
                },
            )
            .unwrap();

        let results = store
            .read_by_index("city_idx", &IndexKey(b"NYC".to_vec()))
            .unwrap();
        assert_eq!(results.len(), 2, "expected both users from index");
        let pks: Vec<&[u8]> = results.iter().map(|p| p.key.key.as_bytes()).collect();
        assert!(pks.contains(&b"user1".as_slice()));
        assert!(pks.contains(&b"user2".as_slice()));
    }

    #[test]
    fn read_by_index_unknown_index_returns_empty() {
        use ferrosa_index::IndexKey;
        let store = test_store();
        store.write(&make_key("k"), make_row(b"v", 1000)).unwrap();
        let results = store
            .read_by_index("nonexistent_idx", &IndexKey(b"anything".to_vec()))
            .unwrap();
        assert!(results.is_empty());
    }

    // =========================================================================
    // Task 6: Result cap (10K RowPositions)
    // =========================================================================

    #[test]
    fn read_by_index_returns_all_rows_under_cap() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "status".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("status_idx".to_string(), 0_usize)],
        );

        for i in 0..100 {
            let key = make_key(&format!("user{i}"));
            store
                .write(
                    &key,
                    Row {
                        clustering: vec![0x00, 0x00, 0x00, i as u8],
                        cells: vec![(0, CellValue::live(b"active".to_vec(), 1000 + i as i64))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(1000 + i as i64),
                    },
                )
                .unwrap();
        }

        let results = store
            .read_by_index("status_idx", &IndexKey(b"active".to_vec()))
            .unwrap();
        assert_eq!(results.len(), 100, "all 100 rows should be returned");
    }

    #[test]
    fn read_by_index_exceeds_cap_returns_error() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "tag".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("tag_idx".to_string(), 0_usize)],
        );

        // Inject >10K entries directly into index to avoid slow row writes
        let idx = store.get_memtable_index("tag_idx").unwrap();
        for i in 0..10_001 {
            idx.insert(
                IndexKey(b"popular".to_vec()),
                RowPosition {
                    partition_key: format!("pk{i}").into_bytes(),
                    clustering_key: vec![],
                },
            );
        }

        let result = store.read_by_index("tag_idx", &IndexKey(b"popular".to_vec()));
        assert!(result.is_err(), "should return error when cap exceeded");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("10000") || err_msg.contains("ALLOW FILTERING"),
            "error should mention cap or ALLOW FILTERING, got: {err_msg}"
        );
    }

    // =========================================================================
    // Task 7: Handle null indexed column values
    // =========================================================================

    #[test]
    fn write_with_null_indexed_column_succeeds() {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "email".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("email_idx".to_string(), 0_usize)],
        );

        // Write a row with a tombstone (null) for the indexed column
        let key = make_key("user_null");
        let row = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::tombstone(1000, 1700000000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        store.write(&key, row).unwrap();

        // Index should be empty (tombstone not indexed)
        let idx = store.get_memtable_index("email_idx").unwrap();
        let all_entries: Vec<_> = idx.iter().collect();
        assert!(all_entries.is_empty(), "null column should not be indexed");

        // Row itself should still be readable via primary key
        let partition = store.read(&key).unwrap();
        assert!(
            partition.is_some(),
            "row should be readable via primary key"
        );
    }

    #[test]
    fn write_with_missing_indexed_column_succeeds() {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "email".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            extensions: Default::default(),
        };

        // Index on "email" (column position 0), but row only has "name" (position 1)
        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("email_idx".to_string(), 0_usize)],
        );

        let key = make_key("user_partial");
        let row = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(1, CellValue::live(b"Alice".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        store.write(&key, row).unwrap();

        // Index should be empty — indexed column was not present
        let idx = store.get_memtable_index("email_idx").unwrap();
        let all_entries: Vec<_> = idx.iter().collect();
        assert!(
            all_entries.is_empty(),
            "missing column should not produce an index entry"
        );
    }

    // =========================================================================
    // Sidecar index integration (flush + read_by_index)
    // =========================================================================

    #[test]
    fn read_by_index_after_flush_queries_sidecar() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "city".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("city_idx".to_string(), 0_usize)],
        );

        // Write a row and flush it to SSTable + sidecar
        store
            .write(
                &make_key("user1"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"NYC".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();
        store.flush().unwrap();

        // The memtable index should be empty after flush
        let idx = store.get_memtable_index("city_idx").unwrap();
        assert!(
            idx.lookup(&IndexKey(b"NYC".to_vec())).is_empty(),
            "memtable index should be reset after flush"
        );

        // But read_by_index should still find it via the sidecar
        let results = store
            .read_by_index("city_idx", &IndexKey(b"NYC".to_vec()))
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "read_by_index should find the flushed row via sidecar"
        );
        assert_eq!(results[0].key.key.as_bytes(), b"user1");
    }

    #[test]
    fn read_by_index_merges_memtable_and_sidecar() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "city".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("city_idx".to_string(), 0_usize)],
        );

        // Write and flush one row
        store
            .write(
                &make_key("user1"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"NYC".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();
        store.flush().unwrap();

        // Write another row with same index value (stays in memtable)
        store
            .write(
                &make_key("user2"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"NYC".to_vec(), 2000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(2000),
                },
            )
            .unwrap();

        // Query should find BOTH: user1 from sidecar, user2 from memtable
        let results = store
            .read_by_index("city_idx", &IndexKey(b"NYC".to_vec()))
            .unwrap();
        assert_eq!(
            results.len(),
            2,
            "should find user1 (sidecar) + user2 (memtable)"
        );
        let pks: Vec<&[u8]> = results.iter().map(|p| p.key.key.as_bytes()).collect();
        assert!(pks.contains(&b"user1".as_slice()));
        assert!(pks.contains(&b"user2".as_slice()));
    }

    // -------------------------------------------------------------------------
    // WP-001: sstable_metadata reports nonzero size after flush
    // -------------------------------------------------------------------------
    #[test]
    fn sstable_metadata_reports_nonzero_size() {
        let store = test_store();
        store.write(&make_key("k1"), make_row(b"v1", 1000)).unwrap();
        store.write(&make_key("k2"), make_row(b"v2", 2000)).unwrap();
        store.flush().unwrap();

        let table_dir = std::path::Path::new("/tmp/test_sstables");
        let metadata = store.sstable_metadata(table_dir);

        assert_eq!(metadata.len(), 1, "expected one SSTable after flush");
        assert!(
            metadata[0].size_bytes > 0,
            "size_bytes must be nonzero; got {}",
            metadata[0].size_bytes
        );
    }

    // -------------------------------------------------------------------------
    // WP-002: sstable_metadata reports correct token range
    // -------------------------------------------------------------------------
    #[test]
    fn sstable_metadata_reports_token_range() {
        let store = test_store();

        // Write multiple partitions with distinct keys to ensure different tokens
        store
            .write(&make_key("alpha"), make_row(b"v1", 1000))
            .unwrap();
        store
            .write(&make_key("beta"), make_row(b"v2", 2000))
            .unwrap();
        store
            .write(&make_key("gamma"), make_row(b"v3", 3000))
            .unwrap();
        store.flush().unwrap();

        let table_dir = std::path::Path::new("/tmp/test_sstables");
        let metadata = store.sstable_metadata(table_dir);

        assert_eq!(metadata.len(), 1);
        let m = &metadata[0];

        // Tokens should not both be zero (the old stub value)
        assert!(
            m.min_token != 0 || m.max_token != 0,
            "at least one token must be nonzero"
        );

        // min_token <= max_token for a multi-partition SSTable stored in
        // token order (SSTables are sorted by token)
        assert!(
            m.min_token <= m.max_token,
            "min_token ({}) must be <= max_token ({})",
            m.min_token,
            m.max_token
        );

        // Cross-check: compute tokens directly and verify they match
        let dk_alpha = make_key("alpha");
        let dk_beta = make_key("beta");
        let dk_gamma = make_key("gamma");
        let mut tokens = [dk_alpha.token.0, dk_beta.token.0, dk_gamma.token.0];
        tokens.sort();
        assert_eq!(
            m.min_token, tokens[0],
            "min_token should match smallest token"
        );
        assert_eq!(
            m.max_token,
            tokens[tokens.len() - 1],
            "max_token should match largest token"
        );
    }

    // -------------------------------------------------------------------------
    // WP-003: sstable_metadata reports correct max_timestamp
    // -------------------------------------------------------------------------
    #[test]
    fn sstable_metadata_reports_max_timestamp() {
        let store = test_store();
        store.write(&make_key("k1"), make_row(b"v1", 5000)).unwrap();
        store.write(&make_key("k2"), make_row(b"v2", 3000)).unwrap();
        store.write(&make_key("k3"), make_row(b"v3", 7000)).unwrap();
        store.flush().unwrap();

        let table_dir = std::path::Path::new("/tmp/test_sstables");
        let metadata = store.sstable_metadata(table_dir);

        assert_eq!(metadata.len(), 1);
        let m = &metadata[0];

        // max_timestamp should be the maximum across all written cells
        assert_eq!(
            m.max_timestamp, 7000,
            "max_timestamp should be 7000 (the highest written timestamp)"
        );
        // min_timestamp should be the minimum
        assert_eq!(
            m.min_timestamp, 3000,
            "min_timestamp should be 3000 (the lowest written timestamp)"
        );
        // max_timestamp must not be the sentinel value
        assert_ne!(
            m.max_timestamp,
            i64::MAX,
            "max_timestamp must not be the sentinel i64::MAX"
        );
    }

    // -------------------------------------------------------------------------
    // Vector sidecar roundtrip: write rows with vector values, flush, verify
    // the HNSW sidecar exists, and verify ann_search returns ordered results.
    // -------------------------------------------------------------------------

    /// Schema with a vector column at position 1 (val column holds raw f32 bytes).
    fn vector_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "vec_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "vec".to_string(),
                type_name: "org.apache.cassandra.db.marshal.VectorType(FloatType,3)".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    /// Build a Row where cell 0 holds a 3-component f32 vector encoded as
    /// little-endian bytes (matching `ferrosa_index::vec_f32_to_bytes`).
    fn make_vector_row(v: &[f32; 3], timestamp: i64) -> Row {
        let bytes = ferrosa_index::vec_f32_to_bytes(v);
        Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(bytes, timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    #[test]
    fn vector_sidecar_roundtrip_ann_search_returns_ordered_results() {
        // Create a store with a vector index on column 0.
        let flush_target = InMemoryFlushTarget::new();
        let mut store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        store.add_vector_index(VectorIndexConfig {
            index_name: "vec_idx".to_string(),
            column_position: 0,
            metric: ferrosa_index::DistanceMetric::L2,
            m: 8,
            ef_construction: 50,
        });

        // Write three vectors: k0 is closest to the query [1,0,0], k2 is farthest.
        //   k0 = [1.0, 0.0, 0.0]   distance 0.0
        //   k1 = [0.9, 0.1, 0.0]   small distance
        //   k2 = [0.0, 1.0, 0.0]   larger distance
        store
            .write(&make_key("k0"), make_vector_row(&[1.0, 0.0, 0.0], 1000))
            .unwrap();
        store
            .write(&make_key("k1"), make_vector_row(&[0.9, 0.1, 0.0], 1001))
            .unwrap();
        store
            .write(&make_key("k2"), make_vector_row(&[0.0, 1.0, 0.0], 1002))
            .unwrap();

        // Flush: this should drain the VectorMemtableIndex and persist a
        // HNSW sidecar via `write_vector_sidecar`.
        store.flush().unwrap();

        assert_eq!(
            store.sstable_count(),
            1,
            "one SSTable should exist after flush"
        );

        // Verify the sidecar was persisted by the flush target.
        let gen = store.last_flush_generation();
        let sidecar_bytes = store
            .flush_target
            .read_vector_sidecar(gen, "vec_idx")
            .expect("vector sidecar must be present after flush");
        assert!(
            !sidecar_bytes.is_empty(),
            "vector sidecar bytes must be non-empty"
        );

        // ann_search should return k=2 results ordered by ascending score
        // (closest first). k0 (all ones aligned with query) should come first.
        let results = store
            .ann_search("vec_idx", &[1.0, 0.0, 0.0], 2, 20)
            .expect("ann_search must not fail");

        assert_eq!(
            results.len(),
            2,
            "ann_search with k=2 must return 2 results"
        );

        // Scores should be in ascending order (closest first).
        assert!(
            results[0].score <= results[1].score,
            "results must be sorted ascending by score: {:?}",
            results
        );

        // The first result should have score ~0.0 (k0 is at distance 0 from query).
        assert!(
            results[0].score < 0.1,
            "first result score should be near 0.0 for exact-match vector, got {}",
            results[0].score
        );
    }

    #[test]
    fn sparse_vector_update_on_existing_row_becomes_visible_to_readback_and_ann() {
        let flush_target = InMemoryFlushTarget::new();
        let mut store: TableStore<InMemoryFlushTarget> = TableStore::new(
            TableSchema {
                keyspace: "agent_memory".to_string(),
                table: "entity_store".to_string(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                clustering_columns: vec![ColumnDefinition {
                    name: "entity_id".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                }],
                static_columns: vec![],
                regular_columns: vec![
                    ColumnDefinition {
                        name: "entity_name".to_string(),
                        type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                    },
                    ColumnDefinition {
                        name: "entity_embedding".to_string(),
                        type_name: "org.apache.cassandra.db.marshal.VectorType(FloatType,3)"
                            .to_string(),
                    },
                ],
                extensions: Default::default(),
            },
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        store.add_vector_index(VectorIndexConfig {
            index_name: "entity_embedding_ann".to_string(),
            column_position: 1,
            metric: ferrosa_index::DistanceMetric::L2,
            m: 8,
            ef_construction: 50,
        });

        let key = make_key("tenant-session");
        let clustering = 7i32.to_be_bytes().to_vec();

        // Given an existing entity row without an embedding.
        store
            .write(
                &key,
                Row {
                    clustering: clustering.clone(),
                    cells: vec![(0, CellValue::live(b"compile-project".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();
        assert!(
            store
                .ann_search("entity_embedding_ann", &[1.0, 0.0, 0.0], 1, 10)
                .unwrap()
                .is_empty(),
            "row without an embedding must not appear in ANN search"
        );

        let embedding = ferrosa_index::vec_f32_to_bytes(&[1.0, 0.0, 0.0]);

        // When a later sparse update adds only the embedding cell.
        store
            .write(
                &key,
                Row {
                    clustering: clustering.clone(),
                    cells: vec![(1, CellValue::live(embedding.clone(), 2000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(2000),
                },
            )
            .unwrap();

        // Then point readback sees the merged row.
        let partition = store.read(&key).unwrap().expect("partition should exist");
        assert_eq!(partition.rows.len(), 1, "expected exactly one logical row");
        let row = &partition.rows[0];
        assert_eq!(
            row.cells.len(),
            2,
            "sparse update should merge into existing row"
        );
        assert_eq!(
            row.cells[1].1.value.as_deref(),
            Some(embedding.as_slice()),
            "merged row should expose the updated embedding bytes"
        );

        // And ANN sees the updated entity immediately from the memtable.
        let memtable_results = store
            .ann_search("entity_embedding_ann", &[1.0, 0.0, 0.0], 1, 10)
            .unwrap();
        assert_eq!(
            memtable_results.len(),
            1,
            "sparse vector update should become visible to ANN before flush"
        );

        // Flush and verify the sidecar path still returns the row.
        store.flush().unwrap();
        let flushed_results = store
            .ann_search("entity_embedding_ann", &[1.0, 0.0, 0.0], 1, 10)
            .unwrap();
        assert_eq!(
            flushed_results.len(),
            1,
            "sparse vector update should remain visible to ANN after flush"
        );
    }
}
