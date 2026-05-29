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
use crate::range_merger::ColumnOrdinalMapping;

/// Maximum number of row positions collected from secondary index before
/// returning an error. Prevents OOM from high-cardinality queries.
const INDEX_RESULT_CAP: usize = 10_000;
const RANGE_READ_MATERIALIZATION_CAP: usize = 10_000;
const QVEC_HNSW_MAGIC: &[u8] = b"FERROSA-QVEC-HNSW-V1\n";

fn build_quantized_vector_artifact(
    cfg: &VectorIndexConfig,
    drained: Vec<(ferrosa_index::vector::RowPosition, Vec<f32>)>,
) -> Result<Vec<u8>> {
    let hnsw_bytes = ferrosa_index::vector::hnsw::build_and_serialize(
        cfg.m,
        cfg.ef_construction,
        cfg.metric,
        drained,
    )
    .map_err(|e| {
        ferrosa_common::Error::InvalidData(format!("quantized artifact build failed: {e}"))
    })?;
    let mut artifact = Vec::with_capacity(QVEC_HNSW_MAGIC.len() + hnsw_bytes.len());
    artifact.extend_from_slice(QVEC_HNSW_MAGIC);
    artifact.extend_from_slice(&hnsw_bytes);
    Ok(artifact)
}

pub(crate) fn search_quantized_vector_artifact(
    bytes: &[u8],
    query: &[f32],
    k: usize,
    ef_search: usize,
) -> Result<Vec<ferrosa_index::vector::IndexResult>> {
    let payload = bytes.strip_prefix(QVEC_HNSW_MAGIC).ok_or_else(|| {
        ferrosa_common::Error::InvalidData("invalid quantized vector artifact header".to_string())
    })?;
    ferrosa_index::vector::hnsw::search_from_bytes(payload, query, k, ef_search).map_err(|e| {
        ferrosa_common::Error::InvalidData(format!("quantized ANN search failed: {e}"))
    })
}

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
/// Vector index artifact/search method.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorIndexMethod {
    /// Existing JSON-serialized HNSW sidecar path (`{gen}-VEC-{index}.db`).
    Hnsw,
    /// Quantized IVFFlat/C-SPANN artifact path (`{gen}-QVEC-{index}.qvec`).
    QuantizedIvf,
}

/// Immutable after registration — parameters control the in-memory and
/// persistent vector artifact built at flush time.
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
    /// and persisted as a method-specific vector artifact.
    vector_index_configs: Vec<VectorIndexConfig>,
    /// Per-index persistent artifact/search method. Missing entries default to
    /// the legacy HNSW sidecar for API compatibility with existing callers.
    vector_index_methods: HashMap<String, VectorIndexMethod>,
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

fn sstable_column_mappings<R: ReadAt + Send + Sync + 'static>(
    schema: &TableSchema,
    sstables: &[Arc<SSTableReader<R>>],
) -> Vec<ColumnOrdinalMapping> {
    sstables
        .iter()
        .map(|sstable| ColumnOrdinalMapping::for_header(schema, sstable.header()))
        .collect()
}

fn next_remapped_clustered_row<R: ReadAt + Send + Sync + 'static>(
    iter: &mut ferrosa_sstable::reader::PartitionIter<'_, R>,
    mapping: &ColumnOrdinalMapping,
) -> Result<Option<Row>> {
    let mut row = iter.next_clustered_row()?;
    if let Some(row) = row.as_mut() {
        mapping.remap_regular_row(row);
    }
    Ok(row)
}

fn partition_with_matching_clustering(
    partition: &Partition,
    clustering: &[u8],
) -> Option<Partition> {
    let rows: Vec<Row> = partition
        .rows
        .iter()
        .filter(|row| row.clustering == clustering)
        .cloned()
        .collect();

    if rows.is_empty() && partition.deletion.is_live() && partition.static_row.is_none() {
        return None;
    }

    Some(Partition {
        key: partition.key.clone(),
        deletion: partition.deletion,
        static_row: partition.static_row.clone(),
        rows,
    })
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
            vector_index_methods: HashMap::new(),
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
            vector_index_methods: HashMap::new(),
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
        self.read_limited_rows(key, 0)
    }

    /// Read a partition by merging all sources while retaining at most
    /// `row_limit` clustered rows from each source when non-zero.
    ///
    /// For single-partition CQL `LIMIT` queries this avoids decoding an
    /// entire wide partition before the router applies the row limit. Each
    /// immutable source is asked for only the needed prefix; the merged
    /// result is trimmed again after last-write-wins reconciliation.
    pub fn read_limited_rows(
        &self,
        key: &DecoratedKey,
        row_limit: usize,
    ) -> Result<Option<Partition>> {
        let guard = self.view.load();
        let schema = self.schema.load();

        let mut sources: Vec<Partition> = Vec::new();

        // Active memtable
        if let Some(p) = guard.active.get(key)? {
            let mut p = (*p).clone();
            if row_limit > 0 {
                p.rows.truncate(row_limit);
            }
            sources.push(p);
        }

        // Flushing memtable
        if let Some(ref flushing) = guard.flushing {
            if let Some(p) = flushing.get(key)? {
                let mut p = (*p).clone();
                if row_limit > 0 {
                    p.rows.truncate(row_limit);
                }
                sources.push(p);
            }
        }

        // SSTables, newest first.
        // Tolerate I/O errors from individual SSTables — a corrupt or
        // format-incompatible SSTable should not prevent reading data
        // that exists in other SSTables or the memtable (FRSA-BUG-026).
        for (i, sstable) in guard.sstables.iter().enumerate() {
            match sstable.get_partition_limited_rows(key, row_limit) {
                Ok(Some(mut p)) => {
                    ColumnOrdinalMapping::for_header(&schema, sstable.header())
                        .remap_partition(&mut p);
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

        let mut merged = merge::merge_partitions(sources);
        if row_limit > 0 {
            merge::apply_deletions(&mut merged);
            merged.rows.truncate(row_limit);
        }
        Ok(Some(merged))
    }

    /// Read exactly one clustered row from a partition by clustering-key
    /// bytes, merging only matching rows across memtable and SSTable sources.
    ///
    /// Full primary-key CQL lookups use this path so equality on every
    /// clustering column does not decode a wide partition before the router
    /// applies its predicates. Reads still use an atomic store view
    /// snapshot and tolerate corrupt SSTables the same way as partition reads.
    pub fn read_clustering_row(
        &self,
        key: &DecoratedKey,
        clustering: &[u8],
    ) -> Result<Option<Partition>> {
        let guard = self.view.load();
        let schema = self.schema.load();
        let mut sources: Vec<Partition> = Vec::new();

        if let Some(p) = guard.active.get(key)? {
            if let Some(filtered) = partition_with_matching_clustering(&p, clustering) {
                sources.push(filtered);
            }
        }

        if let Some(ref flushing) = guard.flushing {
            if let Some(p) = flushing.get(key)? {
                if let Some(filtered) = partition_with_matching_clustering(&p, clustering) {
                    sources.push(filtered);
                }
            }
        }

        for (i, sstable) in guard.sstables.iter().enumerate() {
            match sstable.get_clustering_row(key, clustering) {
                Ok(Some(mut p)) => {
                    ColumnOrdinalMapping::for_header(&schema, sstable.header())
                        .remap_partition(&mut p);
                    sources.push(p);
                }
                Ok(None) => {}
                Err(e) => {
                    let id_info = guard
                        .sstable_ids
                        .get(i)
                        .map(|(id, path)| format!("id={id} path={path:?}"))
                        .unwrap_or_else(|| format!("index={i}"));
                    tracing::error!(
                        %e,
                        %id_info,
                        key = ?key.key.as_bytes(),
                        clustering = ?clustering,
                        "SSTable exact clustering read error: skipping corrupt source — data may be incomplete"
                    );
                    self.sstable_read_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        if sources.is_empty() {
            return Ok(None);
        }

        let mut merged = merge::merge_partitions(sources);
        merged.rows.retain(|row| row.clustering == clustering);
        if merged.rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(merged))
        }
    }

    /// Visit rows for one partition and timestamp window without returning an
    /// owned [`Partition`] or result vector to the caller.
    ///
    /// This is the storage cursor used by RRD late-window recomputation. It is
    /// keyed to one partition and invokes `cb` for each row whose 8-byte
    /// big-endian clustering timestamp falls in `[window_start_ts,
    /// window_end_ts)`. Missing partitions, empty windows, and rows with
    /// non time-series clustering shapes visit zero rows.
    ///
    /// SSTable rows are decoded through `PartitionIter::next_clustered_row`,
    /// keeping only one row per source in memory while preserving cell-level
    /// last-write-wins across overlapping memtable/SSTable sources.
    pub fn visit_time_series_window_rows<Cb>(
        &self,
        key: &DecoratedKey,
        window_start_ts: i64,
        window_end_ts: i64,
        timestamp_unit: crate::timeseries::TimeSeriesTimestampUnit,
        mut cb: Cb,
    ) -> Result<usize>
    where
        Cb: FnMut(&Row) -> Result<()>,
    {
        if window_start_ts >= window_end_ts {
            return Ok(0);
        }

        let guard = self.view.load();
        let schema = self.schema.load();
        let mut partition_delete_at = ferrosa_sstable::types::DeletionTime::LIVE;
        let mut mem_row_iters: Vec<std::vec::IntoIter<Row>> = Vec::new();

        if let Some(partition) = guard.active.get(key)? {
            if partition.deletion.marked_for_delete_at > partition_delete_at.marked_for_delete_at {
                partition_delete_at = partition.deletion;
            }
            let mut rows = partition.rows.clone();
            rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
            mem_row_iters.push(rows.into_iter());
        }
        if let Some(flushing) = guard.flushing.as_ref() {
            if let Some(partition) = flushing.get(key)? {
                if partition.deletion.marked_for_delete_at
                    > partition_delete_at.marked_for_delete_at
                {
                    partition_delete_at = partition.deletion;
                }
                let mut rows = partition.rows.clone();
                rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
                mem_row_iters.push(rows.into_iter());
            }
        }

        let mut sst_sources: Vec<(
            ferrosa_sstable::reader::PartitionIter<'_, F::Reader>,
            ColumnOrdinalMapping,
        )> = Vec::new();
        for sstable in guard.sstables.iter() {
            let mut iter = match sstable.partitions_iter() {
                Ok(iter) => iter,
                Err(e) => {
                    tracing::warn!(%e, key = ?key.key.as_bytes(), "time-series cursor: skipping unreadable SSTable iterator");
                    continue;
                }
            };
            iter.seek_to_token(key.token.0)?;
            while let Some(peeked) = iter.peek_partition_key()? {
                if peeked == *key {
                    let Some((_, deletion, _static_row)) = iter.next_partition_header_only()?
                    else {
                        break;
                    };
                    if deletion.marked_for_delete_at > partition_delete_at.marked_for_delete_at {
                        partition_delete_at = deletion;
                    }
                    sst_sources.push((
                        iter,
                        ColumnOrdinalMapping::for_header(&schema, sstable.header()),
                    ));
                    break;
                }
                if peeked.token != key.token || peeked > *key {
                    break;
                }
                iter.skip_to_next_partition()?;
            }
        }

        if mem_row_iters.is_empty() && sst_sources.is_empty() {
            return Ok(0);
        }

        let mut mem_heads: Vec<Option<Row>> =
            mem_row_iters.iter_mut().map(|iter| iter.next()).collect();
        let mut sst_heads: Vec<Option<Row>> = Vec::with_capacity(sst_sources.len());
        for (iter, mapping) in sst_sources.iter_mut() {
            sst_heads.push(next_remapped_clustered_row(iter, mapping)?);
        }

        let mut visited = 0;
        loop {
            let mut smallest: Option<Vec<u8>> = None;
            for row in mem_heads.iter().chain(sst_heads.iter()).flatten() {
                if smallest
                    .as_ref()
                    .map(|clustering| row.clustering < *clustering)
                    .unwrap_or(true)
                {
                    smallest = Some(row.clustering.clone());
                }
            }
            let Some(clustering) = smallest else {
                break;
            };

            let mut merged_row: Option<Row> = None;
            for (idx, head) in mem_heads.iter_mut().enumerate() {
                if head
                    .as_ref()
                    .map(|row| row.clustering == clustering)
                    .unwrap_or(false)
                {
                    let row = head.take().expect("checked as present");
                    merged_row = match merged_row.take() {
                        Some(prev) => Some(crate::merge::merge_rows(prev, row)),
                        None => Some(row),
                    };
                    *head = mem_row_iters[idx].next();
                }
            }
            for (idx, head) in sst_heads.iter_mut().enumerate() {
                if head
                    .as_ref()
                    .map(|row| row.clustering == clustering)
                    .unwrap_or(false)
                {
                    let row = head.take().expect("checked as present");
                    merged_row = match merged_row.take() {
                        Some(prev) => Some(crate::merge::merge_rows(prev, row)),
                        None => Some(row),
                    };
                    let (iter, mapping) = &mut sst_sources[idx];
                    *head = next_remapped_clustered_row(iter, mapping)?;
                }
            }

            let mut row = merged_row.expect("at least one source matched clustering");
            if !partition_delete_at.is_live()
                && row.primary_key_liveness.timestamp < partition_delete_at.marked_for_delete_at
            {
                continue;
            }
            if !row.deletion.is_live() {
                let row_delete_at = row.deletion.marked_for_delete_at;
                row.cells
                    .retain(|(_column, cell)| cell.timestamp >= row_delete_at);
                if row.cells.is_empty() {
                    continue;
                }
            }

            if let Some(ts) = time_series_row_timestamp(&row, timestamp_unit) {
                if ts >= window_start_ts && ts < window_end_ts {
                    cb(&row)?;
                    visited += 1;
                }
            }
        }

        Ok(visited)
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
                    // Same partition key: merge rows from both sources using
                    // the normal read-path semantics. A raw append preserves
                    // data but can leave clustering rows out of order
                    // (current flush rows followed by previous flushing rows),
                    // which corrupts wide-row row-index construction.
                    partitions[idx] = merge::merge_partitions(vec![partitions[idx].clone(), p]);
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

                match self.vector_index_method(&cfg.index_name) {
                    VectorIndexMethod::Hnsw => {
                        // Build HNSW graph and serialize via the public API.
                        match ferrosa_index::vector::hnsw::build_and_serialize(
                            cfg.m,
                            cfg.ef_construction,
                            cfg.metric,
                            drained,
                        ) {
                            Ok(vec_bytes) => {
                                if let Err(e) = self.flush_target.write_vector_sidecar(
                                    gen,
                                    &cfg.index_name,
                                    &vec_bytes,
                                ) {
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
                    VectorIndexMethod::QuantizedIvf => {
                        match build_quantized_vector_artifact(cfg, drained) {
                            Ok(qvec_bytes) => {
                                if let Err(e) = self.flush_target.write_quantized_vector_sidecar(
                                    gen,
                                    &cfg.index_name,
                                    &qvec_bytes,
                                ) {
                                    tracing::error!(%e, index_name = %cfg.index_name, gen,
                                    "store: quantized vector artifact persist failed");
                                    #[cfg(debug_assertions)]
                                    panic!("quantized vector artifact persist failed: {e}");
                                } else {
                                    tracing::debug!(index_name = %cfg.index_name, gen,
                                    "flush: quantized vector artifact written");
                                }
                            }
                            Err(e) => {
                                tracing::error!(%e, index_name = %cfg.index_name, gen,
                                "store: quantized vector artifact serialization failed");
                                #[cfg(debug_assertions)]
                                panic!("quantized vector artifact serialize failed: {e}");
                            }
                        }
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
        let schema = self.schema.load_full();
        let column_mappings = sstable_column_mappings(&schema, &view.sstables);
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

            let mut merger = match crate::range_merger::merger_for_projected_sources_with_mappings(
                active_iter,
                flushing_iter,
                sstables_slice,
                &column_mappings,
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
        let schema = self.schema.load_full();
        let column_mappings = sstable_column_mappings(&schema, &view.sstables);
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

            let mut merger = match crate::range_merger::merger_for_sources_with_mappings(
                active_iter,
                flushing_iter,
                sstables_slice,
                &column_mappings,
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
        let schema = self.schema.load();
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
            let mapping = ColumnOrdinalMapping::for_header(&schema, sstable.header());
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
                    Ok(Some(mut p)) => {
                        let t = p.key.token.0;
                        if t >= end_token {
                            break; // SSTable is token-sorted — done with this source.
                        }
                        if t >= start_token {
                            mapping.remap_partition(&mut p);
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

    /// Streaming token-bounded walk: invoke `cb` for every partition
    /// in `[start_token, end_token)`, one at a time, dropping each
    /// before the next is decoded.
    ///
    /// The materialising `read_token_range` collects up to `limit`
    /// partitions into a `Vec` before returning. Repair's Merkle
    /// build only needs the hash of each partition — never the
    /// collection — so a callback that consumes each partition by
    /// reference lets the iterator's per-partition allocation be
    /// freed on the next loop. Peak working-set is **one** decoded
    /// partition per active walker, regardless of table size,
    /// partition density, or per-partition row count.
    ///
    /// This is the only path that bounds memory for a Merkle build
    /// on a table with multi-MB partitions inside the fmem 2 GiB
    /// cgroup. The Vec-returning `read_token_range`, even with
    /// `limit = 16`, still materialised dozens of MB of decoded
    /// content per page; concurrent pages stacked past the cap.
    ///
    /// Dedup across (memtable + flushing-memtable + sstables) for
    /// the same partition key is preserved: the callback receives
    /// the *cell-merged* partition (cross-source dedup happens via
    /// an O(1) "carry" — at most one held partition is kept in
    /// flight, merged with later occurrences of the same key, then
    /// emitted when a strictly-greater key arrives).
    /// Walk partitions in `[start_token, end_token)` for the
    /// anti-entropy repair digest path.
    ///
    /// For each unique partition key the callback is invoked with
    /// the key, deletion, optional static row, and an `emit_rows`
    /// continuation. The continuation accepts a `&mut dyn
    /// FnMut(&Row) -> Result<()>` and walks the partition's
    /// clustered rows, invoking it once per row.
    ///
    /// **Hot path** (key is in exactly one SSTable source, neither
    /// memtable has it): rows are streamed via the SSTable
    /// reader's 2-phase API — `next_partition_header_only` then
    /// `stream_clustered_rows`. No `Partition` is materialised;
    /// peak working set during the partition is one row.
    ///
    /// **Multi-source fallback** (memtable + SSTable, or
    /// overlapping LSM levels): every contributing source's full
    /// partition is decoded, `merge_partitions` + `apply_deletions`
    /// produce the cell-merged content, and `emit_rows` iterates
    /// the merged row vector. Same cost as the legacy materialised
    /// path; only triggers for keys with cross-source content
    /// (active writes / pre-compaction state) — settled replicas
    /// stay on the hot path.
    pub fn walk_token_range_for_digest<Cb>(
        &self,
        start_token: i64,
        end_token: i64,
        mut cb: Cb,
    ) -> Result<()>
    where
        Cb: FnMut(
            &DecoratedKey,
            ferrosa_sstable::types::DeletionTime,
            Option<&ferrosa_sstable::types::Row>,
            &mut dyn FnMut(&mut dyn FnMut(&ferrosa_sstable::types::Row) -> Result<()>) -> Result<()>,
        ) -> Result<()>,
    {
        if start_token >= end_token {
            return Ok(());
        }
        let guard = self.view.load();
        let schema = self.schema.load();

        // Memtable bootstrap (same shape as walk_token_range).
        let mut mem_active: Vec<Partition> = guard
            .active
            .range_iter(None, None)
            .filter(|p: &Partition| p.key.token.0 >= start_token && p.key.token.0 < end_token)
            .collect();
        mem_active.sort_by(|a, b| a.key.cmp(&b.key));
        let mut mem_active_iter = mem_active.into_iter().peekable();

        let mut mem_flushing_vec: Vec<Partition> = match guard.flushing {
            Some(ref f) => f
                .range_iter(None, None)
                .filter(|p: &Partition| p.key.token.0 >= start_token && p.key.token.0 < end_token)
                .collect(),
            None => Vec::new(),
        };
        mem_flushing_vec.sort_by(|a, b| a.key.cmp(&b.key));
        let mut mem_flushing_iter = mem_flushing_vec.into_iter().peekable();

        let mut sst_iters: Vec<ferrosa_sstable::reader::PartitionIter<'_, _>> =
            Vec::with_capacity(guard.sstables.len());
        let mut sst_mappings: Vec<ColumnOrdinalMapping> = Vec::with_capacity(guard.sstables.len());
        for sstable in guard.sstables.iter() {
            let mut iter = match sstable.partitions_iter() {
                Ok(it) => it,
                Err(_) => continue,
            };
            let _ = iter.seek_to_token(start_token);
            while let Ok(Some(k)) = iter.peek_partition_key() {
                if k.token.0 < start_token {
                    if iter.skip_to_next_partition().is_err() {
                        break;
                    }
                    continue;
                }
                break;
            }
            sst_iters.push(iter);
            sst_mappings.push(ColumnOrdinalMapping::for_header(&schema, sstable.header()));
        }

        loop {
            // Pick the smallest key across all sources via peek.
            let mut smallest_key: Option<DecoratedKey> = None;
            let pick = |cur: &Option<DecoratedKey>, candidate: &DecoratedKey| -> bool {
                cur.as_ref().map(|k| candidate < k).unwrap_or(true)
            };
            if let Some(p) = mem_active_iter.peek() {
                if pick(&smallest_key, &p.key) {
                    smallest_key = Some(p.key.clone());
                }
            }
            if let Some(p) = mem_flushing_iter.peek() {
                if pick(&smallest_key, &p.key) {
                    smallest_key = Some(p.key.clone());
                }
            }
            for iter in sst_iters.iter_mut() {
                if let Ok(Some(k)) = iter.peek_partition_key() {
                    if k.token.0 >= end_token {
                        continue;
                    }
                    if pick(&smallest_key, &k) {
                        smallest_key = Some(k);
                    }
                }
            }
            let Some(key) = smallest_key else {
                break;
            };

            // Count how many sources hold `key`.
            let mem_active_has = mem_active_iter.peek().map(|p| p.key == key) == Some(true);
            let mem_flushing_has = mem_flushing_iter.peek().map(|p| p.key == key) == Some(true);
            let sst_match_indices: Vec<usize> = sst_iters
                .iter_mut()
                .enumerate()
                .filter_map(|(i, iter)| match iter.peek_partition_key() {
                    Ok(Some(k)) if k == key => Some(i),
                    _ => None,
                })
                .collect();
            let total_sources =
                (mem_active_has as usize) + (mem_flushing_has as usize) + sst_match_indices.len();

            if total_sources == 1 && sst_match_indices.len() == 1 {
                // Hot path: single SSTable source. Use the 2-phase
                // SSTable API so no `Partition` ever materialises.
                let sst_idx = sst_match_indices[0];
                let header = sst_iters[sst_idx]
                    .next_partition_header_only()?
                    .expect("source had key; header must yield");
                let mapping = &sst_mappings[sst_idx];
                let (decoded_key, deletion, mut static_row) = header;
                if let Some(static_row) = static_row.as_mut() {
                    mapping.remap_static_row(static_row);
                }
                debug_assert_eq!(decoded_key, key);
                let iter_ref = &mut sst_iters[sst_idx];
                let mut emit_rows = |on_row: &mut dyn FnMut(
                    &ferrosa_sstable::types::Row,
                ) -> Result<()>|
                 -> Result<()> {
                    if mapping.is_identity() {
                        iter_ref.stream_clustered_rows(|row| on_row(row))
                    } else {
                        iter_ref.stream_clustered_rows(|row| {
                            let mut row = row.clone();
                            mapping.remap_regular_row(&mut row);
                            on_row(&row)
                        })
                    }
                };
                cb(&decoded_key, deletion, static_row.as_ref(), &mut emit_rows)?;
            } else {
                // Multi-source streaming merge. The header (deletion,
                // static row) is small (zero-or-one static row × N
                // sources) so we merge it eagerly. The clustered
                // rows are k-way-merged BY CLUSTERING KEY across all
                // sources, one row at a time — we never hold the
                // full multi-source partition in memory at any point.
                //
                // Memtable sources contribute a pre-sorted `Vec<Row>`
                // iterator (the active memtable's rows are pulled
                // into a Vec just for this partition, then iterated).
                // SSTable sources contribute their `PartitionIter`,
                // walked via `next_clustered_row` so each source's
                // in-flight footprint is exactly one decoded row.

                // Per-source memtable rows (sorted by clustering)
                // and per-source SSTable iter indices.
                let mut mem_row_iters: Vec<std::vec::IntoIter<ferrosa_sstable::types::Row>> =
                    Vec::new();
                let mut headers: Vec<(
                    DecoratedKey,
                    ferrosa_sstable::types::DeletionTime,
                    Option<ferrosa_sstable::types::Row>,
                )> = Vec::with_capacity(total_sources);

                if mem_active_has {
                    let p = mem_active_iter.next().expect("peeked");
                    let key_p = p.key.clone();
                    let deletion = p.deletion;
                    let static_row = p.static_row;
                    let mut rows = p.rows;
                    rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
                    headers.push((key_p, deletion, static_row));
                    mem_row_iters.push(rows.into_iter());
                }
                if mem_flushing_has {
                    let p = mem_flushing_iter.next().expect("peeked");
                    let key_p = p.key.clone();
                    let deletion = p.deletion;
                    let static_row = p.static_row;
                    let mut rows = p.rows;
                    rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
                    headers.push((key_p, deletion, static_row));
                    mem_row_iters.push(rows.into_iter());
                }
                for i in &sst_match_indices {
                    if let Some((k, d, mut sr)) = sst_iters[*i].next_partition_header_only()? {
                        if let Some(static_row) = sr.as_mut() {
                            sst_mappings[*i].remap_static_row(static_row);
                        }
                        headers.push((k, d, sr));
                    }
                }

                // Merge header: max-timestamp deletion, cell-merged
                // static row.
                let mut merged_deletion = ferrosa_sstable::types::DeletionTime::LIVE;
                let mut merged_static: Option<ferrosa_sstable::types::Row> = None;
                for (_, d, sr) in &headers {
                    if d.marked_for_delete_at > merged_deletion.marked_for_delete_at {
                        merged_deletion = *d;
                    }
                    if let Some(s) = sr {
                        merged_static = match merged_static.take() {
                            Some(prev) => Some(crate::merge::merge_rows(prev, s.clone())),
                            None => Some(s.clone()),
                        };
                    }
                }
                let merged_key = headers[0].0.clone();

                // Streaming row merge. Heads from each source —
                // memtable rows arrive from `mem_row_iters`,
                // SSTable rows from `sst_iters[idx].next_clustered_row()`.
                let mem_indices: Vec<usize> = (0..mem_row_iters.len()).collect();
                let sst_local_indices = sst_match_indices.clone();
                let mut emit_rows = |on_row: &mut dyn FnMut(
                    &ferrosa_sstable::types::Row,
                ) -> Result<()>|
                 -> Result<()> {
                    let mut mem_heads: Vec<Option<ferrosa_sstable::types::Row>> =
                        mem_row_iters.iter_mut().map(|it| it.next()).collect();
                    let mut sst_heads: Vec<Option<ferrosa_sstable::types::Row>> =
                        Vec::with_capacity(sst_local_indices.len());
                    for &si in &sst_local_indices {
                        sst_heads.push(
                            next_remapped_clustered_row(&mut sst_iters[si], &sst_mappings[si])
                                .map_err(|e| {
                                    ferrosa_common::Error::InvalidData(format!(
                                        "sst.next_clustered_row: {e}"
                                    ))
                                })?,
                        );
                    }
                    loop {
                        // Pick the smallest clustering key
                        // across all live heads.
                        let mut smallest: Option<Vec<u8>> = None;
                        for r in mem_heads.iter().flatten() {
                            if smallest.as_ref().map(|c| r.clustering < *c).unwrap_or(true) {
                                smallest = Some(r.clustering.clone());
                            }
                        }
                        for r in sst_heads.iter().flatten() {
                            if smallest.as_ref().map(|c| r.clustering < *c).unwrap_or(true) {
                                smallest = Some(r.clustering.clone());
                            }
                        }
                        let Some(ck) = smallest else { break };

                        // Collect every source's row at that
                        // clustering, merging cells one pair at
                        // a time. Peak in-flight: two rows.
                        let mut merged_row: Option<ferrosa_sstable::types::Row> = None;
                        for (i_local, _i_global) in mem_indices.iter().enumerate() {
                            if mem_heads[i_local]
                                .as_ref()
                                .map(|r| r.clustering == ck)
                                .unwrap_or(false)
                            {
                                let row = mem_heads[i_local].take().unwrap();
                                merged_row = match merged_row.take() {
                                    Some(prev) => Some(crate::merge::merge_rows(prev, row)),
                                    None => Some(row),
                                };
                                mem_heads[i_local] = mem_row_iters[i_local].next();
                            }
                        }
                        for (h_idx, &si) in sst_local_indices.iter().enumerate() {
                            if sst_heads[h_idx]
                                .as_ref()
                                .map(|r| r.clustering == ck)
                                .unwrap_or(false)
                            {
                                let row = sst_heads[h_idx].take().unwrap();
                                merged_row = match merged_row.take() {
                                    Some(prev) => Some(crate::merge::merge_rows(prev, row)),
                                    None => Some(row),
                                };
                                sst_heads[h_idx] = next_remapped_clustered_row(
                                    &mut sst_iters[si],
                                    &sst_mappings[si],
                                )
                                .map_err(|e| {
                                    ferrosa_common::Error::InvalidData(format!(
                                        "sst.next_clustered_row: {e}"
                                    ))
                                })?;
                            }
                        }
                        let row = merged_row.expect("at least one source matched");
                        on_row(&row)?;
                        drop(row);
                    }
                    Ok(())
                };
                cb(
                    &merged_key,
                    merged_deletion,
                    merged_static.as_ref(),
                    &mut emit_rows,
                )?;
            }
        }
        Ok(())
    }

    pub fn walk_token_range<Cb>(&self, start_token: i64, end_token: i64, mut cb: Cb) -> Result<()>
    where
        Cb: FnMut(&Partition) -> Result<()>,
    {
        if start_token >= end_token {
            return Ok(());
        }
        let guard = self.view.load();
        let schema = self.schema.load();

        // K-way merge across sources (memtables + every SSTable)
        // by key. Each source advertises its current key via a
        // cheap **peek** (DecoratedKey only — no row bodies); the
        // partition body is decoded ONLY for the source(s) whose
        // peek matches the smallest key in the current cycle.
        // That keeps peak in-flight memory at `O(#decoded_in_cycle)`
        // — typically 1-3 partitions — regardless of how many
        // SSTables exist or how big each partition is. The earlier
        // version held `#sources × decoded_partition` simultaneously
        // and OOM'd the 2 GiB cgroup at ~1.8 GiB on a 235-SSTable
        // replica with fat partitions.

        // Memtable matches are cheap to collect (memtables are
        // already in memory by design) and small. Sort by key so
        // the merge can rely on per-source order.
        let mut mem_active: Vec<Partition> = guard
            .active
            .range_iter(None, None)
            .filter(|p: &Partition| p.key.token.0 >= start_token && p.key.token.0 < end_token)
            .collect();
        mem_active.sort_by(|a, b| a.key.cmp(&b.key));
        let mut mem_active_iter = mem_active.into_iter().peekable();

        let mut mem_flushing_vec: Vec<Partition> = match guard.flushing {
            Some(ref f) => f
                .range_iter(None, None)
                .filter(|p: &Partition| p.key.token.0 >= start_token && p.key.token.0 < end_token)
                .collect(),
            None => Vec::new(),
        };
        mem_flushing_vec.sort_by(|a, b| a.key.cmp(&b.key));
        let mut mem_flushing_iter = mem_flushing_vec.into_iter().peekable();

        // For each SSTable: an iter parked at the first in-range
        // partition. We do NOT decode the body — we keep only the
        // peeked DecoratedKey (small).
        let mut sst_iters: Vec<ferrosa_sstable::reader::PartitionIter<'_, _>> =
            Vec::with_capacity(guard.sstables.len());
        let mut sst_mappings: Vec<ColumnOrdinalMapping> = Vec::with_capacity(guard.sstables.len());
        for sstable in guard.sstables.iter() {
            let mut iter = match sstable.partitions_iter() {
                Ok(it) => it,
                Err(_) => continue,
            };
            // seek_to_token can leave us BEFORE start_token (cache
            // build failed → no-op) — the per-cycle key compare
            // handles that, but skip any partition with token <
            // start_token here to keep the peek-key honest.
            let _ = iter.seek_to_token(start_token);
            // Advance past any pre-range partitions left over from
            // a cache-build-failure fallback.
            while let Ok(Some(k)) = iter.peek_partition_key() {
                if k.token.0 < start_token {
                    // skip past this partition
                    if iter.skip_to_next_partition().is_err() {
                        break;
                    }
                    continue;
                }
                break;
            }
            sst_iters.push(iter);
            sst_mappings.push(ColumnOrdinalMapping::for_header(&schema, sstable.header()));
        }

        loop {
            // Pick the smallest key across all sources, using
            // peek for SSTables (no body decode).
            let mut smallest_key: Option<DecoratedKey> = None;
            let pick = |cur: &Option<DecoratedKey>, candidate: &DecoratedKey| -> bool {
                cur.as_ref().map(|k| candidate < k).unwrap_or(true)
            };
            if let Some(p) = mem_active_iter.peek() {
                if pick(&smallest_key, &p.key) {
                    smallest_key = Some(p.key.clone());
                }
            }
            if let Some(p) = mem_flushing_iter.peek() {
                if pick(&smallest_key, &p.key) {
                    smallest_key = Some(p.key.clone());
                }
            }
            for iter in sst_iters.iter_mut() {
                if let Ok(Some(k)) = iter.peek_partition_key() {
                    if k.token.0 >= end_token {
                        // sstable is past the range; treat as exhausted
                        continue;
                    }
                    if pick(&smallest_key, &k) {
                        smallest_key = Some(k);
                    }
                }
            }
            let Some(key) = smallest_key else {
                break; // every source exhausted
            };

            // Decode the body ONLY from sources whose peek matches
            // the smallest key — typically 1 source, occasionally
            // a handful when the same key landed in both memtable
            // and an SSTable (or got split across compactions).
            let mut group: Vec<Partition> = Vec::new();
            if mem_active_iter.peek().map(|p| p.key == key) == Some(true) {
                group.push(mem_active_iter.next().expect("peeked"));
            }
            if mem_flushing_iter.peek().map(|p| p.key == key) == Some(true) {
                group.push(mem_flushing_iter.next().expect("peeked"));
            }
            for (idx, iter) in sst_iters.iter_mut().enumerate() {
                let matches = matches!(iter.peek_partition_key(), Ok(Some(k)) if k == key);
                if !matches {
                    continue;
                }
                match iter.next_partition() {
                    Ok(Some(mut p)) => {
                        sst_mappings[idx].remap_partition(&mut p);
                        group.push(p);
                    }
                    Ok(None) => {}
                    Err(_) => {}
                }
            }

            let merged = if group.len() == 1 {
                group.into_iter().next().expect("len 1")
            } else {
                let mut m = crate::merge::merge_partitions(group);
                crate::merge::apply_deletions(&mut m);
                m
            };
            cb(&merged)?;
            drop(merged);
        }
        Ok(())
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
        let schema = self.schema.load();

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
                Ok(mut parts) => {
                    let mapping = ColumnOrdinalMapping::for_header(&schema, sstable.header());
                    for partition in &mut parts {
                        mapping.remap_partition(partition);
                    }
                    all_partitions.extend(parts);
                }
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
        self.add_vector_index_with_method(config, VectorIndexMethod::Hnsw);
    }

    /// Register a quantized IVFFlat/C-SPANN vector index for this table.
    ///
    /// Keeps `add_vector_index` as the legacy HNSW path so existing callers and
    /// sidecar artifacts remain compatible.
    pub fn add_quantized_vector_index(&mut self, config: VectorIndexConfig) {
        self.add_vector_index_with_method(config, VectorIndexMethod::QuantizedIvf);
    }

    fn vector_index_method(&self, index_name: &str) -> VectorIndexMethod {
        self.vector_index_methods
            .get(index_name)
            .copied()
            .unwrap_or(VectorIndexMethod::Hnsw)
    }

    fn add_vector_index_with_method(
        &mut self,
        config: VectorIndexConfig,
        method: VectorIndexMethod,
    ) {
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
        self.vector_index_methods
            .insert(config.index_name.clone(), method);
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

        let guard = self.view.load();
        let method = self.vector_index_method(index_name);
        let mut all: Vec<IndexResult> = Vec::new();

        // 1. Search active memtable.
        if let Some(vi) = guard.vector_indexes.get(index_name) {
            let results = vi.search(query, k, ef_search).map_err(|e| {
                ferrosa_common::Error::InvalidData(format!("ann_search memtable failed: {e}"))
            })?;
            all.extend(results);
        }

        // 2. Search flushing memtable is handled by the existing vector_indexes
        // snapshot: the flushing memtable's VectorMemtableIndex is drained at
        // flush start, so active is the only in-flight index we need to query.

        // 3. Search each SSTable's persisted vector artifact.
        for (gen_str, _dir) in guard.sstable_ids.iter() {
            if let Ok(gen) = gen_str.parse::<u64>() {
                match method {
                    VectorIndexMethod::Hnsw => {
                        if let Some(vec_bytes) =
                            self.flush_target.read_vector_sidecar(gen, index_name)
                        {
                            match ferrosa_index::vector::hnsw::search_from_bytes(
                                &vec_bytes, query, k, ef_search,
                            ) {
                                Ok(results) => {
                                    all.extend(results);
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
                    VectorIndexMethod::QuantizedIvf => {
                        match self
                            .flush_target
                            .search_quantized_vector_sidecar(gen, index_name, query, k, ef_search)
                        {
                            Ok(Some(results)) => {
                                all.extend(results);
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::error!(
                                    %e, index_name, gen,
                                    "ann_search: quantized vector artifact search failed"
                                );
                            }
                        }
                    }
                }
            }
        }

        // 4. Merge per-source top-k candidates, sort ascending by score, truncate to k.
        // Row offsets are only unique within a flushed artifact or active memtable;
        // active offsets restart after flush, so offset-keyed dedupe drops valid hits.
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
            .filter_map(|(i, sst)| {
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

                if !path.as_os_str().is_empty()
                    && !sstable_has_required_compaction_components(&sstable_path, id)
                {
                    tracing::warn!(
                        sstable_id = %id,
                        table_dir = ?sstable_path,
                        "compaction planning: skipping SSTable because required on-disk \
                         component files are missing or empty; live readers remain loaded, \
                         but this SSTable cannot be used as a compaction input"
                    );
                    return None;
                }
                if !sstable_data_stream_is_token_monotonic(sst.as_ref()) {
                    tracing::warn!(
                        sstable_id = %id,
                        table_dir = ?sstable_path,
                        "compaction planning: skipping SSTable because sequential Data.db \
                         partition order is not token-monotonic; live readers remain loaded, \
                         but this SSTable requires explicit repair or quarantine before it can \
                         be compacted"
                    );
                    return None;
                }

                Some(crate::compaction::metadata::SSTableMetadata {
                    id: id.clone(),
                    path: sstable_path,
                    size_bytes,
                    min_token,
                    max_token,
                    min_timestamp: header.min_timestamp,
                    max_timestamp: header.max_timestamp,
                    partition_count: sst.key_count(),
                })
            })
            .collect()
    }
}

fn sstable_has_required_compaction_components(table_dir: &std::path::Path, id: &str) -> bool {
    for suffix in [
        "Data.db",
        "Partitions.db",
        "Rows.db",
        "Filter.db",
        "Statistics.db",
        "TOC.txt",
    ] {
        let path = table_dir.join(format!("{id}-{suffix}"));
        let Ok(meta) = std::fs::metadata(&path) else {
            return false;
        };
        if matches!(suffix, "Data.db" | "Partitions.db" | "Statistics.db") && meta.len() == 0 {
            return false;
        }
    }
    true
}

fn sstable_data_stream_is_token_monotonic<R: ReadAt + Send + Sync + 'static>(
    sstable: &SSTableReader<R>,
) -> bool {
    let mut iter = match sstable.partitions_iter() {
        Ok(iter) => iter,
        Err(_) => return false,
    };
    let mut previous: Option<DecoratedKey> = None;
    loop {
        match iter.next_partition_metadata() {
            Ok(Some(partition)) => {
                if previous.as_ref().is_some_and(|prev| partition.key < *prev) {
                    return false;
                }
                previous = Some(partition.key);
            }
            Ok(None) => return true,
            Err(_) => return false,
        }
    }
}

fn time_series_row_timestamp(
    row: &Row,
    timestamp_unit: crate::timeseries::TimeSeriesTimestampUnit,
) -> Option<i64> {
    let bytes: [u8; 8] = row.clustering.as_slice().try_into().ok()?;
    Some(timestamp_unit.raw_to_micros(i64::from_be_bytes(bytes)))
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
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition};

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

    fn make_partition(key: &str, value: &[u8], timestamp: i64) -> Partition {
        Partition {
            key: make_key(key),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![make_row(value, timestamp)],
        }
    }

    fn data_bytes_for_single_partition(
        schema: &TableSchema,
        header_partitions: &[Partition],
        partition: &Partition,
    ) -> Vec<u8> {
        let header = crate::flush::build_serialization_header(schema, header_partitions);
        let mut writer = ferrosa_sstable::writer::SSTableWriter::new(
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            header,
        );
        writer.add_partition(partition).unwrap();
        writer.finish().unwrap().data
    }

    fn sstable_reader_from_partitions(
        schema: &TableSchema,
        partitions: &[Partition],
        truncate_data_to: Option<usize>,
    ) -> ferrosa_sstable::reader::SSTableReader<Vec<u8>> {
        let header = crate::flush::build_serialization_header(schema, partitions);
        let mut writer = ferrosa_sstable::writer::SSTableWriter::new(
            WriteOptions {
                compression: None,
                verify_output: false,
                ..WriteOptions::default()
            },
            header,
        );
        for partition in partitions {
            writer.add_partition(partition).unwrap();
        }
        let mut output = writer.finish().unwrap();
        if let Some(len) = truncate_data_to {
            output.data.truncate(len);
        }
        ferrosa_sstable::reader::SSTableReader::open(ferrosa_sstable::reader::SSTableComponents {
            data: output.data,
            partitions: output.partitions,
            rows: output.rows,
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        })
        .unwrap()
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

    fn file_backed_test_store(dir: &std::path::Path) -> TableStore<crate::flush::FileFlushTarget> {
        TableStore::new(
            test_schema(),
            crate::flush::FileFlushTarget::new_starting_at(dir.to_path_buf()).unwrap(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        )
    }

    fn two_column_schema(first: &str, second: &str) -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "column_order".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: first.to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: second.to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            extensions: Default::default(),
        }
    }

    fn two_column_time_series_schema(first: &str, second: &str) -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "column_order".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ts".to_string(),
                type_name: "org.apache.cassandra.db.marshal.LongType".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: first.to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: second.to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            extensions: Default::default(),
        }
    }

    fn make_two_column_row(first_value: &[u8], second_value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering: 1i32.to_be_bytes().to_vec(),
            cells: vec![
                (0, CellValue::live(first_value.to_vec(), timestamp)),
                (1, CellValue::live(second_value.to_vec(), timestamp)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    fn make_two_column_time_series_row(
        clustering_ts: i64,
        first_value: &[u8],
        second_value: &[u8],
        timestamp: i64,
    ) -> Row {
        Row {
            clustering: clustering_ts.to_be_bytes().to_vec(),
            cells: vec![
                (0, CellValue::live(first_value.to_vec(), timestamp)),
                (1, CellValue::live(second_value.to_vec(), timestamp)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    fn store_with_legacy_order_sstable(
        legacy_schema: TableSchema,
        current_schema: TableSchema,
        key: &DecoratedKey,
        row: Row,
    ) -> TableStore<InMemoryFlushTarget> {
        let legacy = TableStore::new(
            legacy_schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        legacy.write(key, row).unwrap();
        legacy.flush().unwrap();

        let legacy_view = legacy.view.load();
        let initial_sstables = legacy_view.sstables.iter().cloned().collect();
        let initial_ids = vec![("1".to_string(), std::path::PathBuf::new())];

        TableStore::new_with_sstables(
            current_schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            initial_sstables,
            vec![],
            initial_ids,
        )
    }

    #[test]
    fn read_remaps_legacy_sstable_column_order_to_current_schema() {
        // Given an SSTable written when storage order was [b, a].
        let key = make_key("pk-column-order");
        let store = store_with_legacy_order_sstable(
            two_column_schema("b", "a"),
            two_column_schema("a", "b"),
            &key,
            make_two_column_row(b"bee", b"aye", 1000),
        );

        // When the same table is read with current storage order [a, b].
        let partition = store.read(&key).unwrap().expect("partition should exist");

        // Then cells are exposed using current ordinals: 0 => a, 1 => b.
        let row = &partition.rows[0];
        assert_eq!(row.cells[0].0, 0);
        assert_eq!(row.cells[0].1.value.as_deref(), Some(b"aye".as_slice()));
        assert_eq!(row.cells[1].0, 1);
        assert_eq!(row.cells[1].1.value.as_deref(), Some(b"bee".as_slice()));
    }

    #[tokio::test]
    async fn projected_range_translates_current_ordinals_for_legacy_sstable_order() {
        // Given an SSTable written when storage order was [b, a].
        let key = make_key("pk-projected-column-order");
        let store = store_with_legacy_order_sstable(
            two_column_schema("b", "a"),
            two_column_schema("a", "b"),
            &key,
            make_two_column_row(b"bee", b"aye", 1000),
        );

        // When current-schema ordinal 0 (column a) is projected.
        let mut stream = store.range_iter_projected(vec![0], None, None, None);
        let partition = futures::StreamExt::next(&mut stream)
            .await
            .expect("one partition")
            .unwrap();

        // Then the reader decodes legacy physical ordinal 1 and exposes it as ordinal 0.
        let row = &partition.rows[0];
        assert_eq!(row.cells.len(), 1);
        assert_eq!(row.cells[0].0, 0);
        assert_eq!(row.cells[0].1.value.as_deref(), Some(b"aye".as_slice()));
        assert!(
            futures::StreamExt::next(&mut stream).await.is_none(),
            "expected exactly one partition"
        );
    }

    #[test]
    fn token_range_remaps_legacy_sstable_column_order_to_current_schema() {
        let key = make_key("pk-token-column-order");
        let store = store_with_legacy_order_sstable(
            two_column_schema("b", "a"),
            two_column_schema("a", "b"),
            &key,
            make_two_column_row(b"bee", b"aye", 1000),
        );

        let partitions = store.read_token_range(i64::MIN, i64::MAX, 10).unwrap();

        assert_eq!(partitions.len(), 1);
        let row = &partitions[0].rows[0];
        assert_eq!(row.cells[0].0, 0);
        assert_eq!(row.cells[0].1.value.as_deref(), Some(b"aye".as_slice()));
        assert_eq!(row.cells[1].0, 1);
        assert_eq!(row.cells[1].1.value.as_deref(), Some(b"bee".as_slice()));
    }

    #[test]
    fn streaming_token_walk_remaps_legacy_sstable_column_order_to_current_schema() {
        let key = make_key("pk-walk-column-order");
        let store = store_with_legacy_order_sstable(
            two_column_schema("b", "a"),
            two_column_schema("a", "b"),
            &key,
            make_two_column_row(b"bee", b"aye", 1000),
        );

        let mut cells = Vec::new();
        store
            .walk_token_range(i64::MIN, i64::MAX, |partition| {
                cells.push(partition.rows[0].cells.clone());
                Ok(())
            })
            .unwrap();

        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0][0].0, 0);
        assert_eq!(cells[0][0].1.value.as_deref(), Some(b"aye".as_slice()));
        assert_eq!(cells[0][1].0, 1);
        assert_eq!(cells[0][1].1.value.as_deref(), Some(b"bee".as_slice()));
    }

    #[test]
    fn digest_stream_remaps_legacy_sstable_column_order_to_current_schema() {
        let key = make_key("pk-digest-column-order");
        let store = store_with_legacy_order_sstable(
            two_column_schema("b", "a"),
            two_column_schema("a", "b"),
            &key,
            make_two_column_row(b"bee", b"aye", 1000),
        );

        let mut cells = Vec::new();
        store
            .walk_token_range_for_digest(i64::MIN, i64::MAX, |_key, _deletion, _static, emit| {
                emit(&mut |row| {
                    cells.push(row.cells.clone());
                    Ok(())
                })
            })
            .unwrap();

        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0][0].0, 0);
        assert_eq!(cells[0][0].1.value.as_deref(), Some(b"aye".as_slice()));
        assert_eq!(cells[0][1].0, 1);
        assert_eq!(cells[0][1].1.value.as_deref(), Some(b"bee".as_slice()));
    }

    #[test]
    fn time_series_window_cursor_remaps_legacy_sstable_column_order_to_current_schema() {
        let key = make_key("pk-timeseries-column-order");
        let store = store_with_legacy_order_sstable(
            two_column_time_series_schema("b", "a"),
            two_column_time_series_schema("a", "b"),
            &key,
            make_two_column_time_series_row(123, b"bee", b"aye", 1000),
        );

        let mut cells = Vec::new();
        let visited = store
            .visit_time_series_window_rows(
                &key,
                100,
                200,
                crate::timeseries::TimeSeriesTimestampUnit::Micros,
                |row| {
                    cells.push(row.cells.clone());
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(visited, 1);
        assert_eq!(cells[0][0].0, 0);
        assert_eq!(cells[0][0].1.value.as_deref(), Some(b"aye".as_slice()));
        assert_eq!(cells[0][1].0, 1);
        assert_eq!(cells[0][1].1.value.as_deref(), Some(b"bee".as_slice()));
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

    #[test]
    fn count_range_propagates_truncated_sstable_error() {
        // Given one readable SSTable and one legacy/truncated SSTable loaded
        // into the store view, matching a restart over already-corrupt files.
        let store = test_store();
        let schema = test_schema();
        let good = Arc::new(sstable_reader_from_partitions(
            &schema,
            &[make_partition("good", b"good", 2000)],
            None,
        ));
        let corrupt = Arc::new(sstable_reader_from_partitions(
            &schema,
            &[make_partition("corrupt", b"bad", 1000)],
            Some(7),
        ));
        let current = store.view.load_full();
        store.view.store(Arc::new(StoreView {
            active: new_memtable(),
            flushing: None,
            sstables: Arc::new(vec![good, corrupt]),
            sstable_ids: Arc::new(vec![
                ("good".to_string(), std::path::PathBuf::new()),
                ("corrupt".to_string(), std::path::PathBuf::new()),
            ]),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::new(vec![Arc::new(HashMap::new()), Arc::new(HashMap::new())]),
            vector_indexes: Arc::clone(&current.vector_indexes),
        }));

        // When COUNT(*) uses the metadata-only streaming path, the query must
        // fail closed instead of returning a lower count that looks exact.
        let err = store
            .count_range(None, None)
            .expect_err("corrupt SSTable must make COUNT(*) fail closed");

        assert!(
            err.to_string().contains("read_exact_at")
                || err.to_string().contains("unexpected EOF")
                || err.to_string().contains("UnexpectedEof"),
            "error should identify the SSTable read failure, got: {err}"
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

    /// Wide-row version of the previous regression: once a partition has
    /// enough clustered rows to build a Rows.db trie, an append-merge of the
    /// previous flushing memtable makes `SSTableWriter` reject the second flush
    /// with "keys must be added in sorted order".
    #[test]
    fn consecutive_flushes_with_wide_partition_preserve_row_index_order() {
        let store = test_store();
        let key = make_key("wide-pk");

        for ck in 0..100i32 {
            store
                .write(
                    &key,
                    make_row_with_ck(ck, format!("batch1-{ck}").as_bytes(), 1000 + ck as i64),
                )
                .unwrap();
        }
        store.flush().unwrap();

        for ck in 100..150i32 {
            store
                .write(
                    &key,
                    make_row_with_ck(ck, format!("batch2-{ck}").as_bytes(), 2000 + ck as i64),
                )
                .unwrap();
        }
        store
            .flush()
            .expect("wide-row second flush must keep clustering rows sorted for row-index build");

        let result = store.read(&key).unwrap().expect("partition must exist");
        assert_eq!(result.rows.len(), 150);
        assert!(result
            .rows
            .windows(2)
            .all(|pair| pair[0].clustering < pair[1].clustering));
    }

    #[test]
    fn exact_clustering_row_read_returns_only_matching_row_across_sources() {
        let store = test_store();
        let key = make_key("wide");

        for ck in 0..100i32 {
            store
                .write(
                    &key,
                    make_row_with_ck(ck, format!("sst-{ck}").as_bytes(), 1000),
                )
                .unwrap();
        }
        store.flush().unwrap();

        store
            .write(&key, make_row_with_ck(42, b"mem-newer", 2000))
            .unwrap();

        let partition = store
            .read_clustering_row(&key, &42i32.to_be_bytes())
            .unwrap()
            .expect("matching clustering row should be found");

        assert_eq!(
            partition.rows.len(),
            1,
            "exact clustering read must not return or materialize the rest of a wide partition"
        );
        assert_eq!(partition.rows[0].clustering, 42i32.to_be_bytes());
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"mem-newer".as_slice()),
            "newer memtable data must merge over the SSTable row for the same clustering key"
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

    #[test]
    fn sstable_metadata_skips_entries_missing_required_component_files() {
        let tmp = tempfile::tempdir().unwrap();
        let store = file_backed_test_store(tmp.path());

        store
            .write(
                &make_key("still-readable-through-open-fd"),
                make_row(b"v1", 1000),
            )
            .unwrap();
        store.flush().unwrap();

        let gen = store.last_flush_generation();
        let data_path = tmp.path().join(format!("{gen}-Data.db"));
        std::fs::remove_file(&data_path).unwrap();

        let metadata = store.sstable_metadata(tmp.path());

        assert!(
            metadata.is_empty(),
            "compaction planning must not select SSTable {gen} after {:?} is missing",
            data_path
        );
    }

    #[test]
    fn sstable_metadata_skips_entries_with_out_of_order_data_streams() {
        let tmp = tempfile::tempdir().unwrap();
        let store = file_backed_test_store(tmp.path());
        let schema = test_schema();

        let first = make_partition("decision", b"first", 1000);
        let second = make_partition("org", b"second", 1000);
        assert!(
            first.key > second.key,
            "test keys must be descending by decorated token to simulate a legacy unsorted Data.db"
        );

        store.write(&first.key, first.rows[0].clone()).unwrap();
        store.write(&second.key, second.rows[0].clone()).unwrap();
        store.flush().unwrap();

        let gen = store.last_flush_generation();
        let data_path = tmp.path().join(format!("{gen}-Data.db"));
        let header_partitions = vec![second.clone(), first.clone()];
        let mut unsorted_data =
            data_bytes_for_single_partition(&schema, &header_partitions, &first);
        unsorted_data.extend(data_bytes_for_single_partition(
            &schema,
            &header_partitions,
            &second,
        ));
        std::fs::write(&data_path, unsorted_data).unwrap();

        let metadata = store.sstable_metadata(tmp.path());

        assert!(
            metadata.is_empty(),
            "compaction planning must not select SSTable {gen} when sequential Data.db order is not token-monotonic"
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
    fn quantized_ann_dispatch_uses_qvec_artifact_without_legacy_sidecar() {
        let flush_target = InMemoryFlushTarget::new();
        let mut store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        store.add_quantized_vector_index(VectorIndexConfig {
            index_name: "vec_idx".to_string(),
            column_position: 0,
            metric: ferrosa_index::DistanceMetric::L2,
            m: 8,
            ef_construction: 50,
        });

        store
            .write(&make_key("k0"), make_vector_row(&[1.0, 0.0, 0.0], 1000))
            .unwrap();
        store
            .write(&make_key("k1"), make_vector_row(&[0.9, 0.1, 0.0], 1001))
            .unwrap();
        store
            .write(&make_key("k2"), make_vector_row(&[0.0, 1.0, 0.0], 1002))
            .unwrap();
        store.flush().unwrap();

        let gen = store.last_flush_generation();
        assert!(
            store
                .flush_target
                .read_vector_sidecar(gen, "vec_idx")
                .is_none(),
            "quantized method must not write the legacy HNSW/VEC sidecar"
        );
        assert!(
            store
                .flush_target
                .has_quantized_vector_sidecar(gen, "vec_idx"),
            "quantized method must persist a .qvec sidecar"
        );

        let results = store
            .ann_search("vec_idx", &[1.0, 0.0, 0.0], 2, 20)
            .expect("quantized ann_search must not fail");

        assert_eq!(
            results.len(),
            2,
            "quantized ann_search should search flushed .qvec results"
        );
        assert!(
            results[0].score <= results[1].score,
            "results must remain top-k sorted: {:?}",
            results
        );
    }

    #[test]
    fn quantized_ann_search_merges_active_memtable_with_flushed_qvec_even_when_offsets_overlap() {
        let flush_target = InMemoryFlushTarget::new();
        let mut store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        store.add_quantized_vector_index(VectorIndexConfig {
            index_name: "vec_idx".to_string(),
            column_position: 0,
            metric: ferrosa_index::DistanceMetric::L2,
            m: 8,
            ef_construction: 50,
        });

        store
            .write(
                &make_key("flushed-0"),
                make_vector_row(&[0.0, 1.0, 0.0], 1000),
            )
            .unwrap();
        store
            .write(
                &make_key("flushed-1"),
                make_vector_row(&[0.0, 0.0, 1.0], 1001),
            )
            .unwrap();
        store.flush().unwrap();

        // The active memtable starts row offsets at 0 again after flush. The
        // merge must not key only by row offset, or this exact active hit is
        // overwritten by the flushed .qvec result at offset 0.
        store
            .write(
                &make_key("active-exact"),
                make_vector_row(&[1.0, 0.0, 0.0], 1002),
            )
            .unwrap();

        let results = store
            .ann_search("vec_idx", &[1.0, 0.0, 0.0], 2, 20)
            .expect("quantized ann_search must merge active and flushed results");

        assert_eq!(results.len(), 2, "active + flushed sources should merge");
        assert!(
            results[0].score < 0.01,
            "exact active memtable hit must survive qvec merge, got {:?}",
            results
        );
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
