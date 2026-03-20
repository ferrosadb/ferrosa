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

use crate::flush::{self, FlushTarget};
use crate::index::sidecar::SidecarReader;
use crate::memtable::index::MemtableIndex;
#[cfg(not(feature = "skiplist-memtable"))]
use crate::memtable::sharded::ShardedBTreeMemtable;
#[cfg(feature = "skiplist-memtable")]
use crate::memtable::skiplist::SkipListMemtable;
use crate::memtable::Memtable;
use crate::merge;

/// Maximum number of row positions collected from secondary index before
/// returning an error. Prevents OOM from high-cardinality queries.
const INDEX_RESULT_CAP: usize = 10_000;

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
    /// Per-index MemtableIndex companions for the active memtable, keyed by
    /// index name. Swapped atomically alongside the active memtable during flush.
    indexes: Arc<HashMap<String, Arc<MemtableIndex>>>,
    /// Per-SSTable sidecar index readers, parallel to `sstables`.
    /// Each entry maps index_name -> SidecarReader for that SSTable.
    sidecar_indexes: Arc<Vec<Arc<HashMap<String, SidecarReader>>>>,
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
    /// Secondary index declarations: `(index_name, column_position)` pairs.
    /// Column position is the index into `Row.cells` by column ordinal
    /// (matching the `u16` tag in each cell tuple).
    indexed_columns: Vec<(String, usize)>,
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
            indexes,
            sidecar_indexes: Arc::new(vec![]),
        };
        Self {
            schema,
            view: ArcSwap::from_pointee(initial_view),
            flush_guard: Mutex::new(()),
            flush_target,
            options,
            indexed_columns,
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
    ) -> Self {
        Self::new_with_sstables_and_indexes(
            schema,
            flush_target,
            options,
            initial_sstables,
            initial_sidecars,
            vec![],
        )
    }

    /// Like [`Self::new_with_sstables`] but also registers secondary index declarations
    /// so that new writes populate the memtable index.
    pub fn new_with_sstables_and_indexes(
        schema: TableSchema,
        flush_target: F,
        options: WriteOptions,
        initial_sstables: Vec<Arc<SSTableReader<F::Reader>>>,
        initial_sidecars: Vec<Arc<HashMap<String, SidecarReader>>>,
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

        let initial_view = StoreView {
            active,
            flushing: None,
            sstables: Arc::new(initial_sstables),
            indexes,
            sidecar_indexes: Arc::new(sidecars),
        };
        Self {
            schema,
            view: ArcSwap::from_pointee(initial_view),
            flush_guard: Mutex::new(()),
            flush_target,
            options,
            indexed_columns,
        }
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
        // Also swap in fresh indexes for the new active memtable.
        let new_active: Arc<dyn Memtable> = new_memtable();
        let fresh_indexes = new_indexes(&self.indexed_columns);
        let old_view = self.view.load();
        let old_active = Arc::clone(&old_view.active);
        let old_indexes = Arc::clone(&old_view.indexes);
        let current_sstables = Arc::clone(&old_view.sstables);
        let current_sidecars = Arc::clone(&old_view.sidecar_indexes);
        // Drop the guard before storing (ArcSwap does not require it, but
        // dropping early avoids holding a pinned epoch longer than needed).
        drop(old_view);

        self.view.store(Arc::new(StoreView {
            active: new_active,
            flushing: Some(Arc::clone(&old_active)),
            sstables: Arc::clone(&current_sstables),
            indexes: fresh_indexes,
            sidecar_indexes: Arc::clone(&current_sidecars),
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
                indexes: Arc::clone(&live.indexes),
                sidecar_indexes: Arc::clone(&live.sidecar_indexes),
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

        // Step 5b: Build sidecar readers from the old memtable indexes and
        // persist them to disk so they survive process restarts.
        let gen = self.flush_target.last_generation();
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
            eprintln!("[store] sidecar persist failed for gen {gen}: {e}");
        }

        // Step 6: Prepend new SSTable and sidecar, clear flushing.
        let current_view = self.view.load();
        let mut new_sstables = vec![new_reader];
        new_sstables.extend(current_view.sstables.iter().cloned());

        let mut new_sidecars = vec![Arc::new(sidecar_map)];
        new_sidecars.extend(current_view.sidecar_indexes.iter().cloned());

        self.view.store(Arc::new(StoreView {
            active: Arc::clone(&current_view.active),
            flushing: None,
            sstables: Arc::new(new_sstables),
            indexes: Arc::clone(&current_view.indexes),
            sidecar_indexes: Arc::new(new_sidecars),
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

        // 1. Query memtable index
        if let Some(idx) = guard.indexes.get(index_name) {
            positions.extend(idx.lookup(key));
        }

        // 2. Query SSTable sidecar indexes
        for sidecar in guard.sidecar_indexes.iter() {
            if let Some(reader) = sidecar.get(index_name) {
                if let Ok(results) = reader.lookup(key) {
                    positions.extend(results);
                }
            }
        }

        // 3. Enforce result cap before dedup to bound memory usage
        if positions.len() > INDEX_RESULT_CAP {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "secondary index query exceeded {} row limit; \
                 use ALLOW FILTERING for unbounded scans",
                INDEX_RESULT_CAP
            )));
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
        self.view.store(Arc::new(StoreView {
            active: Arc::clone(&current.active),
            flushing: current.flushing.clone(),
            sstables: Arc::clone(&current.sstables),
            indexes: Arc::new(new_indexes),
            sidecar_indexes: Arc::clone(&current.sidecar_indexes),
        }));
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

    /// Truncate (clear) all data: replaces the memtable with a fresh one
    /// and drops all SSTable references.
    ///
    /// Existing readers holding `Arc` references to the old memtable or
    /// SSTables will complete normally; the data is freed once those
    /// references drop. On-disk SSTable files remain until GC.
    pub fn truncate(&self) {
        let _guard = self.flush_guard.lock();
        self.view.store(Arc::new(StoreView {
            active: new_memtable(),
            flushing: None,
            sstables: Arc::new(vec![]),
            indexes: new_indexes(&self.indexed_columns),
            sidecar_indexes: Arc::new(vec![]),
        }));
    }

    /// Atomically replace input SSTables with a compacted output SSTable.
    ///
    /// Removes the `input_count` oldest SSTables (from the end of the
    /// "newest first" list) and inserts the compacted output at position 0.
    pub fn swap_compacted_sstables(
        &self,
        input_count: usize,
        add: Arc<SSTableReader<F::Reader>>,
        output_sidecars: HashMap<String, SidecarReader>,
    ) -> Result<()> {
        let _guard = self.flush_guard.lock();
        let current = self.view.load();
        let len = current.sstables.len();
        let mut new_sstables: Vec<Arc<SSTableReader<F::Reader>>> = current
            .sstables
            .iter()
            .take(len.saturating_sub(input_count))
            .cloned()
            .collect();
        new_sstables.insert(0, add);

        // Mirror the sidecar list: drop the oldest `input_count`, prepend merged output sidecar.
        let slen = current.sidecar_indexes.len();
        let mut new_sidecars: Vec<Arc<HashMap<String, SidecarReader>>> = current
            .sidecar_indexes
            .iter()
            .take(slen.saturating_sub(input_count))
            .cloned()
            .collect();
        new_sidecars.insert(0, Arc::new(output_sidecars));

        self.view.store(Arc::new(StoreView {
            active: Arc::clone(&current.active),
            flushing: current.flushing.clone(),
            sstables: Arc::new(new_sstables),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::new(new_sidecars),
        }));
        Ok(())
    }

    /// Collects sidecar entries from the oldest `input_count` SSTables for merging.
    pub fn collect_compaction_sidecar_entries(
        &self,
        input_count: usize,
    ) -> HashMap<String, Vec<(IndexKey, RowPosition)>> {
        let guard = self.view.load();
        let slen = guard.sidecar_indexes.len();
        let start = slen.saturating_sub(input_count);
        let mut merged: HashMap<String, Vec<(IndexKey, RowPosition)>> = HashMap::new();
        for sidecar_map in &guard.sidecar_indexes[start..] {
            for (index_name, reader) in sidecar_map.as_ref() {
                merged
                    .entry(index_name.clone())
                    .or_default()
                    .extend(reader.all_entries());
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
        guard
            .sstables
            .iter()
            .enumerate()
            .map(|(i, sst)| {
                let header = sst.header();
                crate::compaction::metadata::SSTableMetadata {
                    id: format!("{}", i + 1),
                    path: table_dir.to_path_buf(),
                    size_bytes: 0, // Approximate; exact tracking is a future optimization
                    min_token: 0,
                    max_token: 0,
                    min_timestamp: header.min_timestamp,
                    max_timestamp: i64::MAX, // SerializationHeader only has min; sentinel until full stats tracking
                    partition_count: sst.key_count(),
                }
            })
            .collect()
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
        };

        // Create store with an index on "email" (regular column index 0)
        let indexed_columns = vec![("email_idx".to_string(), 0_usize)];
        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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

        // Perform swap: remove 2 oldest, add 1 → should go from 4 to 3.
        store
            .swap_compacted_sstables(2, new_sst, HashMap::new())
            .unwrap();
        assert_eq!(store.sstable_count(), 3); // 4 - 2 + 1 = 3
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
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
        };

        // Index on "email" (column position 0), but row only has "name" (position 1)
        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
        };

        let store = TableStore::new_with_indexes(
            schema,
            InMemoryFlushTarget,
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
}
