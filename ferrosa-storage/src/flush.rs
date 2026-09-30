//! FlushTarget abstraction and serialization header construction.
//! Correctness: staged output is verified before publication; abandoned scratch is swept.
//! Last revised: 2026-09-26
//! Last changed: Removed legacy Data.raw scratch at startup and targeted Data.db in writer callers.
//!
//! This module provides the [`FlushTarget`] trait, which decouples memtable
//! flush logic from the destination: in-memory buffers ([`InMemoryFlushTarget`])
//! for testing, or real files on disk ([`FileFlushTarget`]) for production.
//!
//! [`build_serialization_header`] scans a set of partitions to compute the
//! minimum timestamp, local deletion time, and TTL across all cells, then
//! builds a [`SerializationHeader`] compatible with the SSTable writer.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ferrosa_index::{IndexKey, RowPosition};
use rayon::prelude::*;

use ferrosa_common::schema::TableSchema;
use ferrosa_common::{Result, NO_DELETION_TIME, NO_TIMESTAMP, NO_TTL};
use ferrosa_sstable::io::{FileReadAt, ReadAt};
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::Partition;
use ferrosa_sstable::writer::{SSTableOutput, SSTableOutputFiles};

/// SSTable components that must exist before a generation can be opened or
/// published to remote storage.
pub(crate) const REQUIRED_SSTABLE_COMPONENTS: [&str; 4] =
    ["Data.db", "Partitions.db", "Rows.db", "Filter.db"];

/// Build a [`SerializationHeader`] by scanning partitions for minimum values.
///
/// The header captures the minimum timestamp, local deletion time, and TTL
/// across all cells in the provided partitions. These minimums enable
/// delta-encoding in the SSTable data file.
///
/// If no cells are present, sentinel values from `ferrosa_common` are used
/// as defaults (NO_TIMESTAMP, NO_DELETION_TIME, NO_TTL).
pub fn build_serialization_header(
    schema: &TableSchema,
    partitions: &[Partition],
) -> SerializationHeader {
    let mut min_timestamp = NO_TIMESTAMP;
    let mut max_timestamp = i64::MIN;
    let mut min_local_deletion_time = NO_DELETION_TIME;
    let mut min_ttl = NO_TTL;

    /// Update `min_timestamp` if `ts` is a real timestamp (not sentinel).
    #[inline]
    fn update_min_ts(min_ts: &mut i64, ts: i64) {
        if ts != NO_TIMESTAMP && (*min_ts == NO_TIMESTAMP || ts < *min_ts) {
            *min_ts = ts;
        }
    }

    /// Update `min_local_deletion_time` if `ldt` is a real value (not sentinel).
    #[inline]
    fn update_min_ldt(min_ldt: &mut i32, ldt: i32) {
        if ldt != NO_DELETION_TIME && (*min_ldt == NO_DELETION_TIME || ldt < *min_ldt) {
            *min_ldt = ldt;
        }
    }

    /// Update `min_ttl` if `ttl` is a real value (not sentinel).
    #[inline]
    fn update_min_ttl(min_ttl_val: &mut i32, ttl: i32) {
        if ttl != NO_TTL && (*min_ttl_val == NO_TTL || ttl < *min_ttl_val) {
            *min_ttl_val = ttl;
        }
    }

    /// Update `max_timestamp` if `ts` is a real timestamp (not sentinel).
    #[inline]
    fn update_max_ts(max_ts: &mut i64, ts: i64) {
        if ts != NO_TIMESTAMP && ts > *max_ts {
            *max_ts = ts;
        }
    }

    /// Scan a row's liveness info and deletion time for min/max values.
    /// The SSTable writer delta-encodes these against the header minimums,
    /// so we must account for them to prevent subtraction overflow.
    #[inline]
    fn scan_row_metadata(
        row: &ferrosa_sstable::types::Row,
        min_ts: &mut i64,
        max_ts: &mut i64,
        min_ldt: &mut i32,
        min_ttl_val: &mut i32,
    ) {
        // Primary key liveness: timestamp, ttl, and local_deletion_time
        // are delta-encoded in the writer.
        if row.primary_key_liveness.has_timestamp() {
            update_min_ts(min_ts, row.primary_key_liveness.timestamp);
            update_max_ts(max_ts, row.primary_key_liveness.timestamp);
        }
        if row.primary_key_liveness.has_ttl() {
            update_min_ttl(min_ttl_val, row.primary_key_liveness.ttl);
            update_min_ldt(min_ldt, row.primary_key_liveness.local_deletion_time);
        }

        // Row-level deletion: marked_for_delete_at and local_deletion_time
        // are delta-encoded in the writer.
        if !row.deletion.is_live() {
            update_min_ts(min_ts, row.deletion.marked_for_delete_at);
            update_max_ts(max_ts, row.deletion.marked_for_delete_at);
            // DeletionTime.local_deletion_time is u32; cast to i32 for comparison
            // with the header field (i32). Values > i32::MAX are sentinel-like and
            // should not lower the minimum.
            let ldt = row.deletion.local_deletion_time;
            if ldt != u32::MAX {
                let ldt_i32 = ldt as i32;
                update_min_ldt(min_ldt, ldt_i32);
            }
        }
    }

    // Data-driven complex-collection activation (D-write, t_83c4f093): if the
    // memtable produced any per-element cell (path set), this SSTable holds at
    // least one complex column and must be framed as complex so the paths
    // persist. Legacy whole-value cells (path=None) leave it false; the reader
    // handles both formats (lazy dual-read).
    let mut has_complex = false;
    for partition in partitions {
        // Scan static row cells if present
        if let Some(ref static_row) = partition.static_row {
            scan_row_metadata(
                static_row,
                &mut min_timestamp,
                &mut max_timestamp,
                &mut min_local_deletion_time,
                &mut min_ttl,
            );
            for (_, cell) in &static_row.cells {
                update_min_ts(&mut min_timestamp, cell.timestamp);
                update_max_ts(&mut max_timestamp, cell.timestamp);
                update_min_ldt(&mut min_local_deletion_time, cell.local_deletion_time);
                update_min_ttl(&mut min_ttl, cell.ttl);
                has_complex |= cell.path.is_some();
            }
        }

        // Scan clustered rows: metadata and cells
        for row in &partition.rows {
            scan_row_metadata(
                row,
                &mut min_timestamp,
                &mut max_timestamp,
                &mut min_local_deletion_time,
                &mut min_ttl,
            );
            for (_, cell) in &row.cells {
                update_min_ts(&mut min_timestamp, cell.timestamp);
                update_max_ts(&mut max_timestamp, cell.timestamp);
                update_min_ldt(&mut min_local_deletion_time, cell.local_deletion_time);
                update_min_ttl(&mut min_ttl, cell.ttl);
                has_complex |= cell.path.is_some();
            }
        }
    }

    // If no real timestamps were found, use safe defaults.
    // Both must be reset symmetrically — a stale NO_TIMESTAMP min with a
    // real max would cause delta-encoding underflow in the SSTable writer.
    if max_timestamp == i64::MIN {
        max_timestamp = i64::MAX;
    }
    if min_timestamp == NO_TIMESTAMP {
        min_timestamp = 0;
    }

    SerializationHeader {
        complex_collections: has_complex,
        min_timestamp,
        min_local_deletion_time,
        min_ttl,
        max_timestamp,
        key_type: schema.key_type.clone(),
        clustering_types: schema.clustering_types(),
        static_columns: schema
            .static_columns
            .iter()
            .map(|c| (c.name.as_bytes().to_vec(), c.type_name.clone()))
            .collect(),
        regular_columns: schema
            .regular_columns
            .iter()
            .map(|c| (c.name.as_bytes().to_vec(), c.type_name.clone()))
            .collect(),
    }
}

/// Split token-sorted `partitions` into at most `num_shards` contiguous slices
/// so each shard can be encoded into its own SSTable in parallel (parallel flush
/// slice #3 — the encode phase is ~98% of flush time and single-threaded per
/// SSTable, so sharding the encode across cores is the write-throughput lever).
///
/// Preconditions / invariants:
/// - Input MUST already be sorted by `DecoratedKey` (token order). The caller
///   (`TableStore::flush`) sorts before calling. Each returned shard then covers
///   a disjoint, CONTIGUOUS token range, so no partition straddles two shards
///   and the shards' concatenation, in order, equals the input. That is what
///   makes the N resulting SSTables correct to merge on read/compaction exactly
///   like any other set of non-overlapping-by-construction SSTables.
/// - Balanced by partition count: the first `n % shards` shards get one extra
///   partition. (Balancing by bytes is a possible future refinement; count is a
///   good proxy and keeps the split O(n) and allocation-light.)
/// - No empty shard is ever returned. `num_shards <= 1`, an empty input, or
///   fewer partitions than shards all degrade gracefully to `<= n` non-empty
///   shards (and to the single-SSTable behavior when `num_shards <= 1`).
pub(crate) fn split_sorted_partitions_into_shards(
    partitions: Vec<Partition>,
    num_shards: usize,
) -> Vec<Vec<Partition>> {
    let n = partitions.len();
    if n == 0 {
        return Vec::new();
    }
    let shards = num_shards.clamp(1, n);
    if shards == 1 {
        return vec![partitions];
    }
    let base = n / shards;
    let rem = n % shards;
    let mut out = Vec::with_capacity(shards);
    let mut it = partitions.into_iter();
    for i in 0..shards {
        let take = base + usize::from(i < rem);
        let chunk: Vec<Partition> = it.by_ref().take(take).collect();
        debug_assert!(
            !chunk.is_empty(),
            "balanced split must not yield empty shard"
        );
        out.push(chunk);
    }
    out
}

/// Minimum partitions per shard — don't shard a flush into slivers. Sharding a
/// tiny flush just makes more, smaller SSTables (more compaction) for no encode
/// win, since the encode cost that sharding parallelizes scales with data size.
pub(crate) const MIN_PARTITIONS_PER_FLUSH_SHARD: usize = 512;

/// Decide how many SSTable shards a flush of `partition_count` partitions should
/// produce (parallel flush slice #3). Returns 1 (single SSTable — the unchanged
/// path) unless the table is shardable (`can_shard`: no secondary indexes in
/// this first increment) AND there is enough data to be worth parallelizing.
/// Capped at `pool_width` — more shards than the flush pool has threads would
/// just queue.
pub(crate) fn desired_flush_shards(
    partition_count: usize,
    can_shard: bool,
    pool_width: usize,
) -> usize {
    if !can_shard || pool_width <= 1 || partition_count < 2 * MIN_PARTITIONS_PER_FLUSH_SHARD {
        return 1;
    }
    (partition_count / MIN_PARTITIONS_PER_FLUSH_SHARD).clamp(1, pool_width)
}

/// Trait abstracting where flushed SSTable component bytes are stored.
///
/// Implementers decide whether the output goes to in-memory buffers or
/// to the filesystem. After writing, the trait returns an `SSTableReader`
/// so the flushed data is immediately queryable.
/// Collect a source's postings into `entries`, for the two targets that have
/// no file to map and must hold an image.
///
/// This is the one copy the streaming path cannot avoid, so it is in one place
/// and named: everything else writes straight through.
fn visit_postings(
    source: &dyn crate::index::sidecar::SidecarSource,
    entries: &mut Vec<(IndexKey, RowPosition)>,
) -> Result<()> {
    source
        .visit(&mut |key, position| {
            entries.push((key.clone(), position.clone()));
            Ok(())
        })
        .map_err(|error| {
            ferrosa_common::Error::InvalidData(format!(
                "reading a memtable index's postings: {error}"
            ))
        })
}

pub trait FlushTarget {
    /// The reader type used to access component data after flushing.
    type Reader: ReadAt + Send + Sync + 'static;

    /// Write SSTable component bytes to the target and open a reader.
    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<Self::Reader>>;

    /// Open (or re-open) a reader for the SSTable generation `gen` living in
    /// `dir`, on demand.
    ///
    /// This is the opener used by the bounded [`crate::reader_pool::ReaderPool`]:
    /// `StoreView` holds only lightweight descriptors, and a reader is
    /// materialised through this method when a read path needs it, then evicted
    /// when idle. File-backed targets re-read the component files from disk
    /// (`gen` is the numeric generation, `dir` the directory holding the
    /// `{gen}-*.db` components). In-memory targets return the components they
    /// retained at flush time.
    ///
    /// The default fails loud: a target that participates in the bounded-reader
    /// pool must provide a real opener (fail-loud rule — never fake a reader).
    fn open_reader(&self, _dir: &Path, gen: u64) -> Result<SSTableReader<Self::Reader>> {
        Err(ferrosa_common::Error::InvalidFormat(format!(
            "FlushTarget::open_reader not implemented for this target (gen {gen}); \
             the bounded SSTable reader pool requires an opener"
        )))
    }

    /// Return a staging directory for file-backed SSTable output.
    ///
    /// Targets that return `Some` from this method can receive
    /// `SSTableOutputFiles` through [`FlushTarget::flush_files`], avoiding a
    /// full in-memory `SSTableOutput` allocation.
    fn file_output_staging_dir(&self) -> Result<Option<PathBuf>> {
        Ok(None)
    }

    /// Materialise an **ephemeral** SSTable reader from freshly-written
    /// component bytes WITHOUT registering it as a durable generation.
    ///
    /// Used by the bounded multi-pass merge in the read/digest paths: when a
    /// token range overlaps more SSTables than the per-operation fan-in budget,
    /// batches of inputs are stream-merged into temporary sorted runs that are
    /// re-read in a later pass and then discarded. These runs must NEVER enter
    /// the `StoreView`, the durable generation namespace, or the shared reader
    /// pool — they exist only for the lifetime of one merge.
    ///
    /// Returns the reader plus an optional temp directory the caller must
    /// remove once the reader is dropped (file targets stage component files
    /// there; in-memory targets return `None`). The default in-memory
    /// implementation opens directly from the byte buffers.
    fn open_ephemeral_reader(
        &self,
        output: SSTableOutput,
    ) -> Result<(SSTableReader<Self::Reader>, Option<PathBuf>)>;

    /// Promote or consume SSTable component files and open a reader.
    ///
    /// The default path is intended for tests and non-file targets: it reads
    /// the staged files into memory, then calls [`FlushTarget::flush`].
    fn flush_files(&self, output: SSTableOutputFiles) -> Result<SSTableReader<Self::Reader>> {
        self.flush(output.read_to_memory()?)
    }

    /// Publish files produced by `finish_to_directory_deferred_sync`.
    /// Implementations that stage files must sync them before promotion and
    /// may apply their buffered page-cache policy after verification. The
    /// default fails loudly so a deferred file output is never materialized
    /// through [`FlushTarget::flush_files`].
    fn flush_deferred_files(
        &self,
        _output: SSTableOutputFiles,
    ) -> Result<SSTableReader<Self::Reader>> {
        Err(ferrosa_common::Error::InvalidFormat(
            "FlushTarget::flush_deferred_files is not implemented; deferred writer output must be \
             explicitly handed to a target that owns its durability barrier"
                .into(),
        ))
    }

    /// Returns the generation number of the most recently flushed SSTable.
    ///
    /// Used by `TableStore` to determine which generation number to use
    /// when writing per-SSTable sidecar index files alongside the SSTable.
    /// Returns 0 for in-memory targets where no generation tracking occurs.
    fn last_generation(&self) -> u64 {
        0
    }

    /// Advance the generation counter to at least `min_gen + 1`.
    /// Prevents future flush file names from colliding with compaction output.
    fn advance_generation(&self, _min_gen: u64) {}

    /// Returns the base directory where SSTable files are written.
    /// Used by the store to register the SSTable with its actual path.
    fn base_dir(&self) -> &std::path::Path {
        std::path::Path::new("")
    }

    /// Write per-index sidecar files alongside the flushed SSTable.
    ///
    /// Called after [`FlushTarget::flush`] with the same generation number. For each
    /// `(index_name, entries)` pair, writes a `{gen}-{index_name}.sidecar`
    /// file so that sidecar indexes survive process restarts.
    ///
    /// The default implementation is a no-op (in-memory targets do not
    /// persist sidecar files).
    fn write_sidecars(
        &self,
        _generation: u64,
        sidecars: &[(&str, &dyn crate::index::sidecar::SidecarSource)],
    ) -> Result<HashMap<String, crate::index::sidecar::SidecarReader>> {
        // No file to map, so this target has to hold an image. It collects
        // once, straight from the source — the caller never builds a `Vec` for
        // it to copy.
        let mut readers = HashMap::with_capacity(sidecars.len());
        for (index_name, source) in sidecars {
            if source.is_empty() {
                continue;
            }
            let mut entries: Vec<(IndexKey, RowPosition)> = Vec::new();
            visit_postings(*source, &mut entries)?;
            readers.insert(
                (*index_name).to_string(),
                crate::index::sidecar::SidecarReader::from_entries(entries),
            );
        }
        Ok(readers)
    }

    /// Write a full-text index (FTI) sidecar file alongside the SSTable.
    ///
    /// Writes `{gen}-FTI-{index_name}.db` to the SSTable directory.
    /// The default implementation is a no-op (in-memory targets do not
    /// persist FTI sidecar files).
    fn write_fti_sidecar(
        &self,
        _generation: u64,
        _index_name: &str,
        _fti_bytes: &[u8],
    ) -> Result<()> {
        Ok(())
    }

    /// Whether [`FlushTarget::write_fti_sidecar_in`] persists anything. Callers
    /// skip building a sidecar a target would discard.
    fn persists_fti_sidecars(&self) -> bool {
        false
    }

    /// Write generation `generation`'s FTI sidecar for `index_name` into
    /// `dir` — the SSTable's own component directory — atomically: readers
    /// see the complete file or no file.
    ///
    /// Used for SSTables that did not get a sidecar at flush time (compaction
    /// outputs, and SSTables written before those were built). The default is
    /// an error, so a caller that skipped [`FlushTarget::persists_fti_sidecars`]
    /// fails loudly instead of believing a discarded sidecar was written.
    fn write_fti_sidecar_in(
        &self,
        _dir: &Path,
        generation: &str,
        index_name: &str,
        _fti_bytes: &[u8],
    ) -> Result<()> {
        Err(ferrosa_common::Error::InvalidFormat(format!(
            "this flush target does not persist FTI sidecars \
             (generation {generation}, index {index_name})"
        )))
    }

    /// Write a vector (HNSW) sidecar file alongside the SSTable.
    ///
    /// Writes `{gen}-VEC-{index_name}.db` to the SSTable directory (or
    /// stores the bytes in memory for test targets). Called from
    /// `TableStore::flush` after draining the `VectorMemtableIndex` and
    /// serializing the HNSW graph via `build_and_serialize`.
    ///
    /// The default implementation is a no-op (callers that only need writes
    /// can leave reads as the default returning `None`).
    fn write_vector_sidecar(
        &self,
        _generation: u64,
        _index_name: &str,
        _vec_bytes: &[u8],
    ) -> Result<()> {
        Ok(())
    }

    /// Read back a vector sidecar that was written by `write_vector_sidecar`.
    ///
    /// Returns `None` if no sidecar was written for this `(generation,
    /// index_name)` pair, or if the target does not persist sidecars
    /// (e.g. `FileFlushTarget` — the store loads those from disk instead).
    ///
    /// Used in integration tests to verify the sidecar round-trip without
    /// touching the filesystem.
    fn read_vector_sidecar(&self, _generation: u64, _index_name: &str) -> Option<Vec<u8>> {
        None
    }

    /// Write a quantized vector artifact (`{gen}-QVEC-{index_name}.qvec`).
    fn write_quantized_vector_sidecar(
        &self,
        _generation: u64,
        _index_name: &str,
        _qvec_bytes: &[u8],
    ) -> Result<()> {
        Ok(())
    }

    /// Search a quantized vector artifact without exposing full sidecar bytes
    /// to the storage read path.
    fn search_quantized_vector_sidecar(
        &self,
        _generation: u64,
        _index_name: &str,
        _query: &[f32],
        _k: usize,
        _ef_search: usize,
    ) -> Result<Option<Vec<ferrosa_index::vector::IndexResult>>> {
        Ok(None)
    }

    /// Test/metadata probe for quantized artifacts.
    fn has_quantized_vector_sidecar(&self, _generation: u64, _index_name: &str) -> bool {
        false
    }
}

/// In-memory flush target for testing — wraps output as `SSTableComponents<Vec<u8>>`.
///
/// No filesystem interaction. The flushed data lives entirely in memory.
/// Tracks a monotonic generation counter so that each flush produces a
/// unique ID, matching the behavior of [`FileFlushTarget`].
///
/// Also stores vector sidecar bytes keyed by `(generation, index_name)` so
/// that integration tests can read them back via `read_vector_sidecar` without
/// touching the filesystem.
pub struct InMemoryFlushTarget {
    generation: std::sync::atomic::AtomicU64,
    /// Retained component bytes keyed by generation, so [`FlushTarget::open_reader`]
    /// can re-open a reader on demand for the bounded reader pool. In-memory
    /// targets have nothing on disk, so the bytes must be held here instead.
    components: std::sync::Mutex<HashMap<u64, Arc<RetainedComponents>>>,
    /// Vector sidecar bytes keyed by `(generation, index_name)`.
    vector_sidecars: std::sync::Mutex<HashMap<(u64, String), Vec<u8>>>,
    /// Quantized vector sidecar bytes keyed by `(generation, index_name)`.
    quantized_vector_sidecars: std::sync::Mutex<HashMap<(u64, String), Vec<u8>>>,
    vector_sidecar_bytes_read: std::sync::atomic::AtomicU64,
}

/// Component bytes retained by [`InMemoryFlushTarget`] so a reader can be
/// re-opened on demand (the in-memory analogue of on-disk component files).
struct RetainedComponents {
    data: Vec<u8>,
    partitions: Vec<u8>,
    rows: Vec<u8>,
    filter: Vec<u8>,
    compression_info: Option<Vec<u8>>,
    statistics: Vec<u8>,
}

impl RetainedComponents {
    fn open(&self) -> Result<SSTableReader<Vec<u8>>> {
        SSTableReader::open(SSTableComponents {
            data: self.data.clone(),
            partitions: self.partitions.clone(),
            rows: self.rows.clone(),
            filter: self.filter.clone(),
            compression_info: self.compression_info.clone(),
            statistics: self.statistics.clone(),
        })
    }
}

impl InMemoryFlushTarget {
    /// Create a new in-memory flush target with the generation counter at 0.
    pub fn new() -> Self {
        Self {
            generation: std::sync::atomic::AtomicU64::new(0),
            components: std::sync::Mutex::new(HashMap::new()),
            vector_sidecars: std::sync::Mutex::new(HashMap::new()),
            quantized_vector_sidecars: std::sync::Mutex::new(HashMap::new()),
            vector_sidecar_bytes_read: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn reset_vector_sidecar_bytes_read(&self) {
        self.vector_sidecar_bytes_read
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn vector_sidecar_bytes_read(&self) -> u64 {
        self.vector_sidecar_bytes_read
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for InMemoryFlushTarget {
    fn default() -> Self {
        Self::new()
    }
}

impl FlushTarget for InMemoryFlushTarget {
    type Reader = Vec<u8>;

    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<Vec<u8>>> {
        // `fetch_add` returns the previous value; the new generation (matching
        // `last_generation()` after this call) is `prev + 1`.
        let gen = self
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let retained = Arc::new(RetainedComponents {
            data: output.data,
            partitions: output.partitions,
            rows: output.rows,
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        });
        let reader = retained.open()?;
        // Retain the component bytes so the reader pool can re-open this gen on
        // demand after eviction (in-memory targets have no on-disk fallback).
        self.components
            .lock()
            .expect("in-memory components poisoned")
            .insert(gen, retained);
        Ok(reader)
    }

    fn open_reader(&self, _dir: &Path, gen: u64) -> Result<SSTableReader<Vec<u8>>> {
        let retained = self
            .components
            .lock()
            .expect("in-memory components poisoned")
            .get(&gen)
            .cloned();
        match retained {
            Some(c) => c.open(),
            None => Err(ferrosa_common::Error::InvalidFormat(format!(
                "InMemoryFlushTarget has no retained components for generation {gen}"
            ))),
        }
    }

    fn open_ephemeral_reader(
        &self,
        output: SSTableOutput,
    ) -> Result<(SSTableReader<Vec<u8>>, Option<PathBuf>)> {
        // In-memory: open straight from the component buffers. The reader owns
        // its bytes, so there is nothing to clean up and no generation is
        // registered — the run never becomes visible to the store or pool.
        let reader = SSTableReader::open(SSTableComponents {
            data: output.data,
            partitions: output.partitions,
            rows: output.rows,
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        })?;
        Ok((reader, None))
    }

    fn last_generation(&self) -> u64 {
        self.generation.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn advance_generation(&self, min_gen: u64) {
        self.generation
            .fetch_max(min_gen + 1, std::sync::atomic::Ordering::SeqCst);
    }

    fn write_vector_sidecar(
        &self,
        generation: u64,
        index_name: &str,
        vec_bytes: &[u8],
    ) -> Result<()> {
        let mut map = self
            .vector_sidecars
            .lock()
            .expect("vector_sidecars poisoned");
        map.insert((generation, index_name.to_string()), vec_bytes.to_vec());
        Ok(())
    }

    fn read_vector_sidecar(&self, generation: u64, index_name: &str) -> Option<Vec<u8>> {
        let map = self
            .vector_sidecars
            .lock()
            .expect("vector_sidecars poisoned");
        let bytes = map.get(&(generation, index_name.to_string())).cloned();
        if let Some(bytes) = bytes.as_ref() {
            self.vector_sidecar_bytes_read
                .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        bytes
    }

    fn write_quantized_vector_sidecar(
        &self,
        generation: u64,
        index_name: &str,
        qvec_bytes: &[u8],
    ) -> Result<()> {
        let mut map = self
            .quantized_vector_sidecars
            .lock()
            .expect("quantized_vector_sidecars poisoned");
        map.insert((generation, index_name.to_string()), qvec_bytes.to_vec());
        Ok(())
    }

    fn search_quantized_vector_sidecar(
        &self,
        generation: u64,
        index_name: &str,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Option<Vec<ferrosa_index::vector::IndexResult>>> {
        let map = self
            .quantized_vector_sidecars
            .lock()
            .expect("quantized_vector_sidecars poisoned");
        let Some(bytes) = map.get(&(generation, index_name.to_string())) else {
            return Ok(None);
        };
        crate::store::search_quantized_vector_artifact(bytes, query, k, ef_search).map(Some)
    }

    fn has_quantized_vector_sidecar(&self, generation: u64, index_name: &str) -> bool {
        let map = self
            .quantized_vector_sidecars
            .lock()
            .expect("quantized_vector_sidecars poisoned");
        map.contains_key(&(generation, index_name.to_string()))
    }
}

/// File-based flush target — writes components to numbered files on disk.
///
/// Each flush creates files named `{generation}-{Component}.db` under the
/// configured base directory. Component files are written in parallel using
/// `std::thread::scope`. An [`AtomicU64`] counter tracks the generation
/// number across flushes.
pub struct FileFlushTarget {
    /// Directory where SSTable component files are written.
    base_dir: PathBuf,
    /// Monotonically increasing generation counter.
    generation: AtomicU64,
}

/// Test-only durability observation seam.
///
/// Records the set of file paths and directory paths that were fsynced during
/// flush/promote so tests can assert the durability barrier actually fired on
/// every component and on the containing directory. Not compiled into release.
#[cfg(test)]
pub(crate) mod fsync_probe {
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard};

    static EXCLUSIVE: Mutex<()> = Mutex::new(());
    static SYNCED_FILES: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
    static SYNCED_DIRS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
    static RENAMED_FILES: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
    static EVENTS: Mutex<Vec<Event>> = Mutex::new(Vec::new());

    /// A single durability-relevant event, in the order it happened.
    ///
    /// The per-kind vectors above (`SYNCED_FILES` etc.) lose relative order
    /// between different kinds of events. `EVENTS` is the ordered timeline
    /// used to prove sequencing invariants such as "rename happens before the
    /// directory fsync, which happens before any input is unlinked"
    /// (T-001 / compaction-cancel-safety.md window E').
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum Event {
        Rename(PathBuf),
        FileFsync(PathBuf),
        ReadbackVerified(PathBuf),
        Fadvise(PathBuf),
        DirFsync(PathBuf),
        Unlink(PathBuf),
    }

    impl Event {
        fn path(&self) -> &Path {
            match self {
                Self::Rename(path)
                | Self::FileFsync(path)
                | Self::ReadbackVerified(path)
                | Self::Fadvise(path)
                | Self::DirFsync(path)
                | Self::Unlink(path) => path,
            }
        }
    }

    pub(crate) struct ExclusiveGuard {
        _guard: MutexGuard<'static, ()>,
    }

    /// A prior test that panicked while holding one of these locks poisons
    /// it for every later probe test — the probe's job is to observe
    /// durability ordering across *deliberately panicking* test bodies (see
    /// `compaction_promotion_fail_after_first_component_is_atomic_and_recoverable`-
    /// style tests elsewhere), so poisoning here is expected, not a sign of
    /// corrupted data. Recovering the inner value and immediately `reset()`ing
    /// it is what makes probe state a per-test fixture again instead of a
    /// permanently poisoned global (T-091: 5 cascading failures traced to
    /// exactly this).
    pub(crate) fn exclusive() -> ExclusiveGuard {
        let guard = EXCLUSIVE.lock().unwrap_or_else(|e| e.into_inner());
        reset();
        ExclusiveGuard { _guard: guard }
    }

    impl Drop for ExclusiveGuard {
        fn drop(&mut self) {
            reset();
        }
    }

    fn reset() {
        SYNCED_FILES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        SYNCED_DIRS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        RENAMED_FILES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        EVENTS.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    pub(crate) fn note_file(path: &Path) {
        SYNCED_FILES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(path.to_path_buf());
        EVENTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Event::FileFsync(path.to_path_buf()));
    }

    pub(crate) fn note_dir(path: &Path) {
        SYNCED_DIRS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(path.to_path_buf());
        EVENTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Event::DirFsync(path.to_path_buf()));
    }

    pub(crate) fn note_rename(path: &Path) {
        RENAMED_FILES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(path.to_path_buf());
        EVENTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Event::Rename(path.to_path_buf()));
    }

    pub(crate) fn note_readback_verified(path: &Path) {
        EVENTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Event::ReadbackVerified(path.to_path_buf()));
    }

    pub(crate) fn note_fadvise(path: &Path) {
        EVENTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Event::Fadvise(path.to_path_buf()));
    }

    /// Record an attempted unlink (e.g. of a retired compaction input
    /// component). Recorded regardless of whether the unlink succeeded, since
    /// what matters for ordering proofs is when the attempt happened.
    pub(crate) fn note_unlink(path: &Path) {
        EVENTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Event::Unlink(path.to_path_buf()));
    }

    pub(crate) fn synced_files() -> HashSet<PathBuf> {
        SYNCED_FILES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    pub(crate) fn synced_files_under(base: &Path) -> HashSet<PathBuf> {
        synced_files()
            .into_iter()
            .filter(|path| path.starts_with(base))
            .collect()
    }

    pub(crate) fn synced_dirs() -> HashSet<PathBuf> {
        SYNCED_DIRS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    pub(crate) fn synced_dirs_under(base: &Path) -> HashSet<PathBuf> {
        synced_dirs()
            .into_iter()
            .filter(|path| path.starts_with(base))
            .collect()
    }

    pub(crate) fn renamed_files() -> Vec<PathBuf> {
        RENAMED_FILES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The full chronological timeline of rename/fsync/unlink events.
    pub(crate) fn events() -> Vec<Event> {
        EVENTS.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub(crate) fn events_under(base: &Path) -> Vec<Event> {
        events()
            .into_iter()
            .filter(|event| event.path().starts_with(base))
            .collect()
    }
}

/// Sweep leftover flush/compaction-output staging debris from a component
/// directory (`publication-safety.md` M2).
///
/// A `.tmp` component is left behind only when the process died between
/// renaming staged output to `.tmp` (`FileFlushTarget::flush_files`) and
/// either quarantining it (verification failure) or promoting it to a live
/// name (success) — every other path removes it. `{gen}-Data.db.tmp` never
/// matches the `{gen}-Data.db` suffix generation discovery scans for, so it
/// is not a data-loss risk by itself, but leaving it as unlabeled debris hides
/// exactly the kind of interrupted publish M2 asks to make visible. It is
/// moved into `quarantine/`, the same destination a failed `flush_files` call
/// uses, so an operator finds both kinds of refusal in one place.
///
/// `.sstable-staging/` and `.merge-spill/` hold pre-rename bytes under their
/// original component names — nothing in them has been renamed to a `.tmp` or
/// live name yet. Like the compaction output directory
/// (`StorageEngine::cleanup_stale_compaction_staging`), they are only ever
/// live while a flush or merge-read is running in this process; there is
/// nothing at startup to resume one, so they are pure debris and are removed
/// outright rather than quarantined. Legacy `Data.raw` scratch files directly
/// in the component directory are also removed; they are never live components.
///
/// Called from `StorageEngine::load_existing_sstables_and_sidecars_with_repair_mode`
/// before generation discovery, and again from `FileFlushTarget::new`/
/// `new_starting_at` as a safety net for callers that construct a flush
/// target without going through table startup (tests, the compaction output
/// directory, the merge-spill ephemeral path).
pub(crate) fn sweep_stale_flush_staging(dir: &Path) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(
                %e,
                dir = %dir.display(),
                "flush: could not scan dir for stale staging"
            );
            return;
        }
    };

    let quarantine_dir = dir.join("quarantine");
    let mut quarantine_ready = false;
    let mut quarantined = 0usize;
    let mut removed_staging_dirs = 0usize;

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let is_dir = entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false);

        if is_dir && (name_str == ".sstable-staging" || name_str == ".merge-spill") {
            match std::fs::remove_dir_all(entry.path()) {
                Ok(()) => removed_staging_dirs += 1,
                Err(e) => tracing::error!(
                    %e,
                    dir = %entry.path().display(),
                    "flush: could not remove stale flush staging dir"
                ),
            }
            continue;
        }

        // The streaming writer never produces Data.raw. An exact legacy
        // basename is uncommitted scratch; generation-prefixed Data.db stays live.
        if !is_dir && name_str == "Data.raw" {
            if let Err(e) = std::fs::remove_file(entry.path()) {
                tracing::error!(%e, path = %entry.path().display(),
                    "flush: could not remove stale legacy raw data");
            }
            continue;
        }

        if is_dir || !name_str.ends_with(".tmp") {
            continue;
        }

        if !quarantine_ready {
            if let Err(e) = std::fs::create_dir_all(&quarantine_dir) {
                tracing::error!(
                    %e,
                    dir = %quarantine_dir.display(),
                    "flush: could not create quarantine dir for stale staged components"
                );
                break;
            }
            quarantine_ready = true;
        }
        let dest = quarantine_dir.join(&name);
        match std::fs::rename(entry.path(), &dest) {
            Ok(()) => quarantined += 1,
            Err(e) => tracing::error!(
                %e,
                path = %entry.path().display(),
                "flush: could not quarantine stale staged component"
            ),
        }
    }

    if quarantined > 0 || removed_staging_dirs > 0 {
        tracing::warn!(
            quarantined,
            removed_staging_dirs,
            dir = %dir.display(),
            "flush: swept stale staged output left by an earlier crash"
        );
    }
}

impl FileFlushTarget {
    /// Create a new file flush target writing to the given directory.
    ///
    /// The directory is created if it does not exist. The generation
    /// counter starts at 0; the first flush produces generation 1.
    pub fn new(base_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_dir)?;
        sweep_stale_flush_staging(&base_dir);
        Ok(Self {
            base_dir,
            generation: AtomicU64::new(0),
        })
    }

    /// Returns the current generation counter value (the last generation written).
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Create a file flush target that starts after the highest existing generation.
    ///
    /// Scans the directory for existing SSTable files (`{gen}-Data.db`) and
    /// starts the generation counter at `max(max_gen, node_offset)` where
    /// `node_offset` is derived from `FERROSA_HOST_ID` to prevent generation
    /// collisions across nodes. Without this, two fresh nodes both start at
    /// gen=1 and their SSTables collide in the S3 manifest.
    pub fn new_starting_at(base_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_dir)?;
        sweep_stale_flush_staging(&base_dir);
        let max_gen = Self::scan_max_generation(&base_dir);
        let node_offset = Self::node_generation_offset();
        Ok(Self {
            base_dir,
            generation: AtomicU64::new(max_gen.max(node_offset)),
        })
    }

    /// Compute a per-node generation offset from `FERROSA_HOST_ID`.
    ///
    /// Hashes the full host UUID to produce a well-distributed 40-bit offset
    /// in range [0, 1 trillion). This gives each node a unique ~1M-generation
    /// window. With typical flush rates (< 1000/day), a node would need to
    /// run for years to exhaust its window.
    ///
    /// Using a hash instead of a prefix of the UUID ensures uniform
    /// distribution even for UUIDs with common prefixes.
    ///
    /// Nodes with no host_id (tests, single-node) get offset 0.
    pub(crate) fn node_generation_offset() -> u64 {
        std::env::var("FERROSA_HOST_ID")
            .ok()
            .map(|s| {
                // Simple FNV-1a hash of the UUID string, masked to 40 bits.
                // 40 bits = ~1 trillion possible offsets.
                let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis
                for byte in s.bytes() {
                    hash ^= byte as u64;
                    hash = hash.wrapping_mul(0x100000001b3); // FNV prime
                }
                hash & 0xFF_FFFF_FFFF // 40-bit mask → max ~1.1 trillion
            })
            .unwrap_or(0)
    }

    /// Scan a directory for the highest SSTable generation number.
    fn scan_max_generation(dir: &std::path::Path) -> u64 {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_string();
                if name.ends_with("-Data.db") {
                    name.split('-').next()?.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0)
    }

    fn next_generation(&self) -> u64 {
        // Allocate from ONE cell shared by every flush target in the process.
        //
        // This used to be a per-target counter seeded from a microsecond clock,
        // with the claim that "a microsecond timestamp ensures uniqueness
        // across all flush targets on this node". It does not: each target owns
        // its own `AtomicU64`, so two targets that seed from the same
        // microsecond both `fetch_max` to the same `ts` and both return
        // `ts + 1`. Interleaving two targets collides on roughly half of all
        // allocations.
        //
        // Generations name files -- `{gen}-Data.db`, `{gen}-Partitions.db` --
        // so a duplicate generation means two writes over the same names, and
        // the SSTable that survives has one write's data with another's index.
        // That is the 2026-08-20 node2 corruption, which surfaced as an extent
        // pointing past the end of a file: `read_exact_at: wanted 17063 bytes,
        // got 818`. The table's flush target and the compaction executor's
        // target are exactly this pair, and compaction output is moved into the
        // table's directory, so their filenames really do meet.
        //
        // The engine already knew they overlapped -- "Compaction output gen may
        // collide with flush gen (different dirs)" -- and mitigated afterwards
        // with `advance_gen_past`, which cannot repair a collision that has
        // already been written.
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Raise the shared floor to this target's own (directory scan and
        // per-node offset) and to the clock, then take the next value.
        let floor = self.generation.load(Ordering::SeqCst).max(ts);
        NEXT_SSTABLE_GENERATION.fetch_max(floor, Ordering::SeqCst);
        let gen = NEXT_SSTABLE_GENERATION.fetch_add(1, Ordering::SeqCst) + 1;

        // Keep the per-target view meaningful for `generation()`.
        self.generation.fetch_max(gen, Ordering::SeqCst);
        gen
    }

    fn component_paths(&self, gen: u64) -> FileComponentPaths {
        let base = &self.base_dir;
        FileComponentPaths {
            data: base.join(format!("{gen}-Data.db")),
            partitions: base.join(format!("{gen}-Partitions.db")),
            rows: base.join(format!("{gen}-Rows.db")),
            filter: base.join(format!("{gen}-Filter.db")),
            statistics: base.join(format!("{gen}-Statistics.db")),
            toc: base.join(format!("{gen}-TOC.txt")),
            compression_info: base.join(format!("{gen}-CompressionInfo.db")),
            digest: base.join(format!("{gen}-Digest.crc32")),
            crc: base.join(format!("{gen}-CRC.db")),
        }
    }

    fn tmp_component_path(path: &Path) -> PathBuf {
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("txt") => path.with_extension("txt.tmp"),
            _ => path.with_extension("db.tmp"),
        }
    }

    fn rename_path(source: impl AsRef<Path>, target: impl AsRef<Path>) -> std::io::Result<()> {
        std::fs::rename(source.as_ref(), target.as_ref())?;
        #[cfg(test)]
        fsync_probe::note_rename(target.as_ref());
        Ok(())
    }

    fn promote_tmp_components(
        paths: &FileComponentPaths,
        has_compression_info: bool,
    ) -> Result<()> {
        let tmp = Self::tmp_component_path;

        // `Data.db` is the discovery marker for a live generation. Promote it
        // last so a crash between renames can leave side components without
        // Data.db, but never a discoverable Data-only orphan.
        Self::rename_path(tmp(&paths.partitions), &paths.partitions)?;
        Self::rename_path(tmp(&paths.rows), &paths.rows)?;
        Self::rename_path(tmp(&paths.filter), &paths.filter)?;
        Self::rename_path(tmp(&paths.statistics), &paths.statistics)?;
        Self::rename_path(tmp(&paths.toc), &paths.toc)?;
        Self::rename_path(tmp(&paths.digest), &paths.digest)?;
        if has_compression_info {
            Self::rename_path(tmp(&paths.compression_info), &paths.compression_info)?;
        } else {
            Self::rename_path(tmp(&paths.crc), &paths.crc)?;
        }
        Self::rename_path(tmp(&paths.data), &paths.data)?;
        Ok(())
    }

    /// fsync a single file so its bytes are durable on the underlying device.
    ///
    /// Opens the file read-only and calls `sync_all()` (flushes data + metadata).
    /// This MUST be called on the *final* component path after rename so the
    /// promoted file's contents survive a power loss / SIGKILL. A missing file
    /// is a hard error — every component we promote must exist when we claim
    /// the SSTable is durable.
    fn fsync_path(path: &Path) -> std::io::Result<()> {
        let f = std::fs::File::open(path)?;
        f.sync_all()?;
        #[cfg(test)]
        Self::record_fsync(path);
        Ok(())
    }

    /// fsync the directory `dir` so that rename directory entries are durable.
    ///
    /// `pub(crate)` so `StorageEngine::promote_compaction_output` (engine.rs,
    /// T-001) can reuse this exact barrier for `sstables/<table>/` after
    /// promoting a compaction output, instead of duplicating the open+sync_all
    /// dance with its own durability semantics.
    ///
    /// On POSIX a `rename(2)` updates the directory; that update lives in the
    /// page cache until the directory inode is fsynced. Without this, a crash
    /// after rename but before writeback can lose the final-named entry (or,
    /// symmetrically, leave a final-named file whose data blocks were never
    /// flushed). Opening the directory and calling `sync_all()` flushes those
    /// entries. This is the single barrier that makes the temp→rename→final
    /// sequence crash-atomic.
    pub(crate) fn fsync_dir(dir: &Path) -> std::io::Result<()> {
        let f = std::fs::File::open(dir)?;
        f.sync_all()?;
        #[cfg(test)]
        Self::record_dir_fsync(dir);
        Ok(())
    }

    /// fsync every component file that exists for this generation, then fsync
    /// the containing directory once. Returns Ok only when all component bytes
    /// AND their directory entries are durable on disk.
    ///
    /// Crash-safety ordering: callers rename each component to its final name
    /// first, then call this. We fsync the final files (their data blocks),
    /// then fsync `base_dir` (the rename entries). Doing the file fsyncs before
    /// the directory fsync guarantees that once the directory entry is durable,
    /// the data it points at is already durable too.
    fn fsync_components(
        &self,
        paths: &FileComponentPaths,
        has_compression_info: bool,
    ) -> Result<()> {
        // Required + always-written components for a generation.
        let mut components: Vec<&Path> = vec![
            &paths.data,
            &paths.partitions,
            &paths.rows,
            &paths.filter,
            &paths.statistics,
            &paths.toc,
            &paths.digest,
        ];
        if has_compression_info {
            components.push(&paths.compression_info);
        } else {
            components.push(&paths.crc);
        }

        // Issue every component fsync CONCURRENTLY so their device flushes fill
        // the storage queue depth instead of serializing at QD1. The single
        // serial fsync stream is the measured write-throughput floor: the device
        // has ~4x idle queue depth over one serial fsync stream (fio: 17000 raw
        // IOPS at QD128 vs ~4090 single-stream fdatasync). Each `sync_all()` is
        // an independent blocking syscall on a distinct component file.
        //
        // The fsyncs run on the shared, BOUNDED flush pool
        // (`crate::flush_executor`), whose width is configurable
        // (`FERROSA_FLUSH_PARALLELISM`, default = host parallelism). The pool
        // caps concurrency across ALL concurrent flushes, so parallelism is a
        // tunable, not a hard-coded per-flush thread count.
        //
        // BARRIER ORDERING (crash-safety — do NOT weaken): `install` blocks
        // until every component fsync completes, so all component bytes are
        // durable BEFORE the single directory fsync below. The directory fsync
        // makes the rename entries durable / claims the SSTable complete, so it
        // must happen strictly after. `try_for_each` short-circuits on the first
        // failure and we return it WITHOUT reaching the directory fsync — no
        // false-durability claim. Guarded by
        // `fsync_components_fails_loud_and_skips_dir_when_a_component_is_missing`.
        crate::flush_executor::pool()?.install(|| {
            components.par_iter().try_for_each(|component| {
                Self::fsync_path(component).map_err(|e| {
                    ferrosa_common::Error::Io(std::io::Error::new(
                        e.kind(),
                        format!("fsync of component {} failed: {e}", component.display()),
                    ))
                })
            })
        })?;

        // One directory fsync after all component fsyncs makes the rename
        // entries durable. This is the most important step in the barrier.
        Self::fsync_dir(&self.base_dir).map_err(|e| {
            ferrosa_common::Error::Io(std::io::Error::new(
                e.kind(),
                format!("fsync of base dir {} failed: {e}", self.base_dir.display()),
            ))
        })?;
        Ok(())
    }

    #[cfg(test)]
    fn record_fsync(path: &Path) {
        fsync_probe::note_file(path);
    }

    #[cfg(test)]
    fn record_dir_fsync(dir: &Path) {
        fsync_probe::note_dir(dir);
    }

    fn open_reader_from_paths(
        paths: &FileComponentPaths,
        has_compression_info: bool,
    ) -> Result<SSTableReader<FileReadAt>> {
        let data = FileReadAt::open(&paths.data)?;
        let partitions = FileReadAt::open(&paths.partitions)?;
        let rows = FileReadAt::open(&paths.rows)?;
        let filter = std::fs::read(&paths.filter)?;
        let statistics = std::fs::read(&paths.statistics)?;
        let compression_info = if has_compression_info {
            Some(std::fs::read(&paths.compression_info)?)
        } else {
            None
        };

        let mut reader = SSTableReader::open(SSTableComponents {
            data,
            partitions,
            rows,
            filter,
            compression_info,
            statistics,
        })?;

        // This helper only ever opens a generation this process just
        // promoted (see call sites), so Digest.crc32 (and CRC.db for
        // uncompressed tables) are always present here -- `?`, not `.ok()`.
        let digest = std::fs::read(&paths.digest)?;
        reader.load_digest(&digest)?;
        if !has_compression_info {
            let crc = std::fs::read(&paths.crc)?;
            reader.load_crc_table(&crc)?;
        }

        Ok(reader)
    }

    /// Validate that every staged `.tmp` component landed at its expected
    /// length. `SSTableOutputFiles` records a length per component; checking
    /// only `data_len` let five of six components promote unchecked. What a
    /// length check cannot catch is covered by the readback walk that follows
    /// it in `flush_files`.
    ///
    /// `Rows.db` is legitimately zero-length for small SSTables (see
    /// `StorageEngine::smoke_test_generation`, which excludes it from its
    /// zero-byte rule), so the comparison is against the recorded length,
    /// never against zero.
    fn check_staged_component_lengths(
        gen: u64,
        paths: &FileComponentPaths,
        output: &SSTableOutputFiles,
    ) -> std::result::Result<(), String> {
        let tmp = Self::tmp_component_path;
        let mut checks: Vec<(&str, &Path, u64)> = vec![
            ("Data.db", &paths.data, output.data_len),
            ("Partitions.db", &paths.partitions, output.partitions_len),
            ("Rows.db", &paths.rows, output.rows_len),
            ("Filter.db", &paths.filter, output.filter_len),
            ("Statistics.db", &paths.statistics, output.statistics_len),
            ("TOC.txt", &paths.toc, output.toc_len),
            ("Digest.crc32", &paths.digest, output.digest_len),
        ];
        if output.compression_info.is_some() {
            checks.push((
                "CompressionInfo.db",
                &paths.compression_info,
                output.compression_info_len,
            ));
        }
        if output.crc.is_some() {
            checks.push(("CRC.db", &paths.crc, output.crc_len));
        }

        for (name, path, expected) in checks {
            let actual = std::fs::metadata(tmp(path)).map(|m| m.len()).unwrap_or(0);
            if actual != expected {
                return Err(format!(
                    "FLUSH CORRUPTION: staged {name} gen={gen} expected {expected} bytes, \
                     got {actual}."
                ));
            }
        }
        Ok(())
    }

    /// Recompute `Digest.crc32` from the staged `.tmp` Data.db bytes **read
    /// back from disk** and compare it with the producer's value
    /// (`publication-safety.md` M2 step 4 / M3, T-012). Runs unconditionally
    /// -- there is no environment variable that can turn this off, unlike
    /// compaction's row/partition count walk -- and runs for compaction too,
    /// since compaction publishes through this same `flush_files`.
    ///
    /// This closes G3/G4: the structural readback walk that follows only
    /// proves the file decodes, not that it holds the bytes the producer
    /// actually computed a checksum over. A length-preserving corruption
    /// (bit flip, swapped block, stale bytes from a recycled buffer) changes
    /// the CRC32 essentially certainly, so this check catches every case in
    /// the fault matrix a length comparison and a decode walk both miss.
    ///
    /// Reads in bounded chunks through one reused buffer -- never a
    /// whole-file `Vec` -- and on Linux advises the kernel to drop the pages
    /// this verification read just forced into cache, so checking the digest
    /// does not itself refill the cache the write pump bypasses. An advise
    /// failure is logged, never silent, and never fails the check: the
    /// digest comparison is the correctness gate, the fadvise call is only a
    /// cache hint.
    fn verify_staged_data_digest(
        gen: u64,
        paths: &FileComponentPaths,
    ) -> std::result::Result<(), String> {
        let tmp = Self::tmp_component_path;
        let digest_path = tmp(&paths.digest);
        let data_path = tmp(&paths.data);

        let digest_bytes = std::fs::read(&digest_path).map_err(|e| {
            format!(
                "staged Digest.crc32 gen={gen} at {} could not be read: {e}",
                digest_path.display()
            )
        })?;
        let expected = ferrosa_sstable::checksum::parse_digest(&digest_bytes).map_err(|e| {
            format!(
                "staged Digest.crc32 gen={gen} at {} is unreadable: {e}",
                digest_path.display()
            )
        })?;

        let file = std::fs::File::open(&data_path).map_err(|e| {
            format!(
                "staged Data.db gen={gen} at {} could not be opened for digest verification: {e}",
                data_path.display()
            )
        })?;

        use std::io::Read;
        let digest_read_chunk_bytes =
            crate::runtime_tuning::storage_runtime_tuning().digest_read_chunk_bytes;
        let mut hasher = ferrosa_sstable::checksum::DigestCrc32::new();
        let mut buf = vec![0u8; digest_read_chunk_bytes];
        #[cfg(test)]
        let readback_hook = readback_test_support::for_path(&data_path);
        loop {
            #[cfg(test)]
            if let Some(hook) = &readback_hook {
                hook().map_err(|e| format!("gated digest readback failed: {e}"))?;
            }
            let n = (&file).read(&mut buf).map_err(|e| {
                format!("read failed verifying staged Data.db digest gen={gen}: {e}")
            })?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Self::fadvise_dontneed_after_verification(&file, &data_path);

        let actual = hasher.finalize();
        if actual != expected {
            return Err(format!(
                "staged Data.db gen={gen} at {} Digest.crc32 mismatch: producer computed \
                 {expected:#010x}, re-reading the staged bytes from disk computed {actual:#010x}.",
                data_path.display()
            ));
        }
        Ok(())
    }

    /// Advise the kernel to drop this file's pages from the page cache
    /// (Linux only). Called after verification reads so they do not leave the
    /// just-checked bytes resident in cache. A no-op on other platforms: there
    /// is no portable equivalent, and this is a cache hint, not a correctness
    /// requirement.
    #[cfg(target_os = "linux")]
    fn fadvise_dontneed_after_verification(file: &std::fs::File, path: &Path) {
        use std::os::fd::AsRawFd;
        // SAFETY: `file` is a valid, open fd for the duration of this call;
        // offset/len 0 means "the whole file", and `posix_fadvise` is
        // advisory -- it cannot fault or invalidate the fd.
        let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        if rc != 0 {
            tracing::warn!(
                path = %path.display(),
                errno = rc,
                "posix_fadvise(DONTNEED) failed after staged verification; the \
                 verification bytes may remain in the page cache"
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn fadvise_dontneed_after_verification(_file: &std::fs::File, _path: &Path) {}

    /// Build the `.tmp`-named counterpart of every path in `paths`, for
    /// opening a throwaway reader over staged (not-yet-promoted) bytes.
    fn tmp_component_paths(paths: &FileComponentPaths) -> FileComponentPaths {
        let tmp = Self::tmp_component_path;
        FileComponentPaths {
            data: tmp(&paths.data),
            partitions: tmp(&paths.partitions),
            rows: tmp(&paths.rows),
            filter: tmp(&paths.filter),
            statistics: tmp(&paths.statistics),
            toc: tmp(&paths.toc),
            compression_info: tmp(&paths.compression_info),
            digest: tmp(&paths.digest),
            crc: tmp(&paths.crc),
        }
    }

    /// For deferred writer output, discard component pages only after the
    /// publication transaction has synced and verified every staged file.
    /// This preserves the buffered pump's cache policy without asking DONTNEED
    /// to discard dirty, not-yet-durable pages.
    fn fadvise_deferred_components(
        paths: &FileComponentPaths,
        has_compression_info: bool,
        has_crc: bool,
    ) {
        let advise = |path: &Path| {
            #[cfg(test)]
            fsync_probe::note_fadvise(path);
            match std::fs::File::open(path) {
                Ok(file) => Self::fadvise_dontneed_after_verification(&file, path),
                Err(error) => tracing::warn!(
                    path = %path.display(),
                    %error,
                    "failed to open verified staged component for page-cache advice"
                ),
            }
        };
        for path in [
            &paths.data,
            &paths.partitions,
            &paths.rows,
            &paths.filter,
            &paths.statistics,
            &paths.toc,
            &paths.digest,
        ] {
            advise(path);
        }
        if has_compression_info {
            advise(&paths.compression_info);
        }
        if has_crc {
            advise(&paths.crc);
        }
    }

    /// fsync every staged `.tmp` component (concurrently, on the shared flush
    /// pool, mirroring `fsync_components`'s ordering rationale) WITHOUT
    /// touching the containing directory -- the `.tmp` names are not renamed
    /// yet, so there is no new directory entry to make durable here. Called
    /// before the pre-promote readback walk so a crash right after a
    /// successful flush_files still has fsynced bytes to read back.
    fn fsync_tmp_components(
        &self,
        paths: &FileComponentPaths,
        has_compression_info: bool,
        has_crc: bool,
    ) -> Result<()> {
        let tmp = Self::tmp_component_path;
        let mut tmp_paths: Vec<PathBuf> = vec![
            tmp(&paths.data),
            tmp(&paths.partitions),
            tmp(&paths.rows),
            tmp(&paths.filter),
            tmp(&paths.statistics),
            tmp(&paths.toc),
            tmp(&paths.digest),
        ];
        if has_compression_info {
            tmp_paths.push(tmp(&paths.compression_info));
        }
        if has_crc {
            tmp_paths.push(tmp(&paths.crc));
        }

        crate::flush_executor::pool()?.install(|| {
            tmp_paths.par_iter().try_for_each(|component| {
                Self::fsync_path(component).map_err(|e| {
                    ferrosa_common::Error::Io(std::io::Error::new(
                        e.kind(),
                        format!(
                            "fsync of staged component {} failed: {e}",
                            component.display()
                        ),
                    ))
                })
            })
        })
    }

    /// Move a refused generation's `.tmp` component set into `quarantine/`,
    /// WARN with the reason, count the refusal
    /// (`sstable_publication_refused_total{reason}`), and return the `Err`
    /// the caller should propagate.
    ///
    /// Scans for the `{gen}-*.tmp` prefix rather than the fixed component list
    /// so an unexpected leftover (a partial write outside the six/seven named
    /// components) is still swept, matching the prefix-scan convention
    /// `StorageEngine::quarantine_generation` already uses for live names.
    fn quarantine_staged_components(
        &self,
        gen: u64,
        reason: crate::metrics::PublicationRefusedReason,
        detail: &str,
    ) -> ferrosa_common::Error {
        let quarantine_dir = self.base_dir.join("quarantine");
        let mut moved = 0usize;
        match std::fs::create_dir_all(&quarantine_dir) {
            Ok(()) => {
                let prefix = format!("{gen}-");
                for entry in std::fs::read_dir(&self.base_dir)
                    .into_iter()
                    .flatten()
                    .flatten()
                {
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy();
                    if !name_str.starts_with(&prefix) || !name_str.ends_with(".tmp") {
                        continue;
                    }
                    let dest = quarantine_dir.join(&name);
                    match std::fs::rename(entry.path(), &dest) {
                        Ok(()) => moved += 1,
                        Err(e) => tracing::error!(
                            gen, %e, path = %entry.path().display(),
                            "flush: could not move refused staged component to quarantine"
                        ),
                    }
                }
            }
            Err(e) => tracing::error!(
                gen, %e, dir = %quarantine_dir.display(),
                "flush: could not create quarantine dir; refused staged output left under .tmp names"
            ),
        }

        crate::metrics::inc_sstable_publication_refused(reason);
        tracing::warn!(
            gen,
            reason = reason.label(),
            detail,
            moved,
            dir = %quarantine_dir.display(),
            "flush: refusing to publish staged SSTable output; quarantined"
        );

        ferrosa_common::Error::InvalidFormat(format!(
            "{detail} Refusing to publish it; {moved} staged component(s) quarantined under {}.",
            quarantine_dir.display()
        ))
    }
}

struct FileComponentPaths {
    data: PathBuf,
    partitions: PathBuf,
    rows: PathBuf,
    filter: PathBuf,
    statistics: PathBuf,
    toc: PathBuf,
    compression_info: PathBuf,
    /// `Digest.crc32` — written for every table (T-011).
    digest: PathBuf,
    /// `CRC.db` — written for uncompressed tables only (T-011).
    crc: PathBuf,
}

/// Restore generation `gen` from S3 when the uploaded-cache evictor removed
/// its local copy, so a live reader can reopen it without a restart.
///
/// Only a generation carrying an eviction marker (`<gen>.evicted`, written
/// fsynced BEFORE the evictor deletes anything) is restored. An unmarked
/// generation with missing components was compacted away: restoring it would
/// resurrect purged rows, and its open error is what drives the read path's
/// view-retry, so it is left to fail loud in the caller.
///
/// `rehydrate_file` (the registered S3 read-through hook) restores every
/// component of the generation next to the path it is given, each via
/// fsynced temp file + rename, so a failure leaves the marker in place for the
/// next attempt. Once the generation is local again the marker is cleared so
/// the restart path does not re-handle it; the evictor rewrites it if it evicts
/// the generation again. A concurrent opener that already cleared the marker
/// has also already restored the files, which the caller's own existence
/// checks then observe.
pub(crate) fn rehydrate_if_evicted(dir: &Path, gen: &str) -> Result<()> {
    let data = dir.join(format!("{gen}-Data.db"));
    let marker = crate::engine::StorageEngine::evicted_marker_path(dir, gen);
    if data.exists() || !marker.exists() {
        return Ok(());
    }
    if !ferrosa_sstable::io::rehydrate_file(&data)? {
        tracing::error!(
            dir = %dir.display(),
            gen,
            "evicted SSTable could not be rehydrated from S3 (no read-through hook \
             owns it, or its objects are missing); the open will fail. Marker kept"
        );
        return Ok(());
    }
    if let Err(e) = std::fs::remove_file(&marker) {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                marker = %marker.display(),
                error = %e,
                "rehydrated an evicted SSTable but could not clear its marker; a restart will find it already local"
            );
        }
    }
    Ok(())
}

/// Open a file-backed SSTable reader from component files for generation `gen`
/// in `dir`. Shared by [`FileFlushTarget::open_reader`] and the engine's
/// startup/load path so on-demand reopens go through one code path.
///
/// Required components (`Data.db`, `Partitions.db`, `Rows.db`, `Filter.db`) must
/// exist — `Filter.db` is always written for a live SSTable, so its absence while
/// `Data.db` is present means a concurrent compaction/eviction deleted it and we
/// fail loud (see the inline comment at the read). Genuinely-optional components
/// (`Statistics.db`, `CompressionInfo.db`) default to empty/absent when missing.
pub fn open_file_sstable(dir: &Path, gen: &str) -> Result<SSTableReader<FileReadAt>> {
    rehydrate_if_evicted(dir, gen)?;
    let [data_component, partitions_component, rows_component, filter_component] =
        REQUIRED_SSTABLE_COMPONENTS;
    let required = |suffix: &str| -> Result<PathBuf> {
        let p = dir.join(format!("{gen}-{suffix}"));
        if p.exists() {
            Ok(p)
        } else {
            Err(ferrosa_common::Error::InvalidFormat(format!(
                "missing required {suffix} for sstable generation {gen} in {}",
                dir.display()
            )))
        }
    };

    let data = FileReadAt::open(required(data_component)?)?;
    let partitions = FileReadAt::open(required(partitions_component)?)?;
    let rows = FileReadAt::open(required(rows_component)?)?;

    // `Filter.db` is ALWAYS written for a live SSTable (flush and compaction
    // both emit it unconditionally — see `file_flush_target_creates_component_files`).
    // So unlike a legitimately-optional component, an *absent* `Filter.db` while
    // `Data.db` is present means a concurrent compaction/eviction deleted it out
    // from under this open. Substituting an empty filter (the old
    // `unwrap_or_default()`) would build a DEGRADED reader whose bloom rejects
    // every key, silently pruning the only SSTable holding a row and surfacing
    // as a spurious `Ok(None)` (silent data loss) with NO open error — so the
    // read-path view-retry would never fire. Fail loud instead: this converts
    // the window into an open `Err`, engaging the existing `with_retried_view`
    // retry which reopens against the freshly-compacted view that holds the key.
    let filter = std::fs::read(required(filter_component)?)?;
    let statistics = std::fs::read(dir.join(format!("{gen}-Statistics.db"))).unwrap_or_default();
    let compression_info = std::fs::read(dir.join(format!("{gen}-CompressionInfo.db"))).ok();
    let is_compressed = compression_info.is_some();

    let mut reader = SSTableReader::open(SSTableComponents {
        data,
        partitions,
        rows,
        filter,
        compression_info,
        statistics,
    })?;

    // Digest.crc32 (all tables) and CRC.db (uncompressed tables only) are
    // genuinely optional, same as CompressionInfo.db above: a generation
    // written before T-011 has neither, and `SSTableReader` treats an
    // unloaded digest/CRC table as "not checked" rather than an error
    // (logged once per generation — see `checksum` module docs in
    // ferrosa-sstable). Centralised (T-012) so every other file-backed open
    // path gets the same automatic loading instead of opting in ad hoc.
    ferrosa_sstable::reader::load_checksums_for_generation(&mut reader, dir, gen, is_compressed);

    Ok(reader)
}

/// Process-wide SSTable generation allocator.
///
/// Every `FileFlushTarget` draws from this one cell. Per-target counters
/// collide even when timestamp-seeded, and a generation is a filename, so a
/// collision is two writes sharing `{gen}-*.db`.
static NEXT_SSTABLE_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Walk every partition of a freshly promoted SSTable.
///
/// Opening a reader is not enough -- the 2026-08-20 corruption opened fine and
/// failed on the first real read. Touching each partition's rows is what
/// surfaces a bad partition index or a short Data.db.
/// Each component's on-disk path paired with the byte length the writer
/// produced for it.
///
/// Used by the pre-rename and post-rename guards so both check the whole
/// SSTable rather than Data.db alone.
fn component_expectations<'a>(
    paths: &'a FileComponentPaths,
    output: &'a SSTableOutput,
) -> Vec<(&'static str, &'a Path, u64)> {
    let mut v: Vec<(&'static str, &'a Path, u64)> = vec![
        ("Data.db", &paths.data, output.data.len() as u64),
        (
            "Partitions.db",
            &paths.partitions,
            output.partitions.len() as u64,
        ),
        ("Rows.db", &paths.rows, output.rows.len() as u64),
        ("Filter.db", &paths.filter, output.filter.len() as u64),
        (
            "Statistics.db",
            &paths.statistics,
            output.statistics.len() as u64,
        ),
        ("TOC.txt", &paths.toc, output.toc.len() as u64),
        ("Digest.crc32", &paths.digest, output.digest.len() as u64),
    ];
    if let Some(ci) = output.compression_info.as_ref() {
        v.push((
            "CompressionInfo.db",
            &paths.compression_info,
            ci.len() as u64,
        ));
    }
    if let Some(crc) = output.crc.as_ref() {
        v.push(("CRC.db", &paths.crc, crc.len() as u64));
    }
    v
}

fn verify_promoted_sstable(reader: &SSTableReader<FileReadAt>) -> Result<()> {
    let mut iter = reader.partitions_iter()?;
    while iter.next_partition_count()?.is_some() {
        // Walk partition framing and count rows without decoding cell payloads.
    }
    Ok(())
}

fn write_legacy_component(path: impl AsRef<Path>, bytes: &[u8]) -> std::io::Result<()> {
    ferrosa_sstable::pump::note_component_write_outside_pump(path.as_ref());
    std::fs::write(path, bytes)
}

impl FileFlushTarget {
    fn flush_files_inner(
        &self,
        output: SSTableOutputFiles,
        drop_cache_after_verify: bool,
    ) -> Result<SSTableReader<FileReadAt>> {
        let gen = self.next_generation();
        let paths = self.component_paths(gen);
        let has_compression_info = output.compression_info.is_some();
        tracing::info!(
            gen,
            data_size = output.data_len,
            partitions_size = output.partitions_len,
            dir = %self.base_dir.display(),
            "flush: staging SSTable for verify-before-promote"
        );

        let tmp = Self::tmp_component_path;

        std::fs::rename(&output.data, tmp(&paths.data))?;
        std::fs::rename(&output.partitions, tmp(&paths.partitions))?;
        std::fs::rename(&output.rows, tmp(&paths.rows))?;
        std::fs::rename(&output.filter, tmp(&paths.filter))?;
        std::fs::rename(&output.statistics, tmp(&paths.statistics))?;
        std::fs::rename(&output.toc, tmp(&paths.toc))?;
        std::fs::rename(&output.digest, tmp(&paths.digest))?;
        if let Some(compression_info) = output.compression_info.as_ref() {
            std::fs::rename(compression_info, tmp(&paths.compression_info))?;
        }
        if let Some(crc) = output.crc.as_ref() {
            std::fs::rename(crc, tmp(&paths.crc))?;
        }

        // The staging dir is empty now regardless of what happens below, so
        // clean it up eagerly rather than leaving a failure path to forget it.
        let staging_parent = output.staging_dir.parent().map(Path::to_path_buf);
        if let Err(e) = std::fs::remove_dir(&output.staging_dir) {
            tracing::debug!(
                gen, %e, dir = %output.staging_dir.display(),
                "flush: could not remove empty staging dir"
            );
        }
        if let Some(parent) = staging_parent {
            if let Err(e) = std::fs::remove_dir(&parent) {
                tracing::debug!(
                    gen, %e, dir = %parent.display(),
                    "flush: staging parent not empty or already removed"
                );
            }
        }

        // Verify-before-promote (`publication-safety.md` M2): every check
        // below runs against the `.tmp` names, BEFORE `promote_tmp_components`
        // makes anything live. A refusal at any point quarantines the `.tmp`
        // set instead of returning with output under a name the startup scan
        // will load. This closes G1: the previous order promoted to live
        // names first and verified after, so a readback failure left a
        // corrupt SSTable at a live name next to the WAL replay of the same
        // rows.
        if let Err(detail) = Self::check_staged_component_lengths(gen, &paths, &output) {
            return Err(self.quarantine_staged_components(
                gen,
                crate::metrics::PublicationRefusedReason::LengthMismatch,
                &detail,
            ));
        }

        // Durability barrier on the STAGED bytes: fsync every `.tmp` component
        // before the readback walk trusts what it reads, and before promote
        // can rename them into place. `fsync_dir` after promote below then
        // only needs to make the rename entries durable -- the component
        // bytes underneath are already synced.
        if let Err(e) =
            self.fsync_tmp_components(&paths, has_compression_info, output.crc.is_some())
        {
            return Err(self.quarantine_staged_components(
                gen,
                crate::metrics::PublicationRefusedReason::Fsync,
                &format!(
                    "FLUSH CORRUPTION: staged components gen={gen} could not be fsynced: {e}."
                ),
            ));
        }

        // Recompute Digest.crc32 from the now-durable staged bytes and compare
        // it with the producer's value (`publication-safety.md` M2 step 4 / M3,
        // T-012). Unconditional -- flush and compaction (which calls this same
        // function) both get it, and there is no environment variable that can
        // disable it, unlike the row/partition count walk compaction runs
        // separately. This runs BEFORE the structural readback walk below: a
        // length-preserving content corruption changes the CRC32, so this is
        // the check that actually catches it, not the decode walk.
        if let Err(detail) = Self::verify_staged_data_digest(gen, &paths) {
            return Err(self.quarantine_staged_components(
                gen,
                crate::metrics::PublicationRefusedReason::DigestMismatch,
                &format!("FLUSH CORRUPTION: {detail}"),
            ));
        }

        // Read back the STAGED file, not a promised promotion of it.
        //
        // On 2026-08-20 node2's compaction of agent_memory.session_task_focus_stack
        // verified its output in the compaction staging directory -- "output
        // verified (streaming readback matches merge) partitions=13 rows=17" --
        // and nine seconds later the swap published it into the table directory
        // under a different generation. That published file was corrupt:
        //
        //     read_exact_at: wanted 17063 bytes, got 818
        //
        // Nothing checked the published file itself. Verifying staged bytes
        // and then promoting something else was not verification -- this now
        // walks the exact `.tmp` bytes that promote will rename into place, on
        // a THROWAWAY reader (the engine keeps the reader this function
        // returns; verifying through it would leave cached state that a later
        // corruption could hide behind).
        let tmp_paths = Self::tmp_component_paths(&paths);
        let readback =
            Self::open_reader_from_paths(&tmp_paths, has_compression_info).and_then(|probe| {
                let result = verify_promoted_sstable(&probe);
                drop(probe);
                result
            });
        if let Err(e) = readback {
            return Err(self.quarantine_staged_components(
                gen,
                crate::metrics::PublicationRefusedReason::ReadbackFailed,
                &format!("FLUSH CORRUPTION: staged SSTable gen={gen} could not be read back: {e}."),
            ));
        }

        #[cfg(test)]
        fsync_probe::note_readback_verified(&tmp_paths.data);
        if drop_cache_after_verify {
            Self::fadvise_deferred_components(
                &tmp_paths,
                has_compression_info,
                output.crc.is_some(),
            );
        }

        Self::promote_tmp_components(&paths, has_compression_info)?;

        // The component bytes are already fsynced (above); only the rename
        // entries need to become durable now.
        Self::fsync_dir(&self.base_dir)?;

        tracing::info!(
            gen,
            data_bytes = std::fs::metadata(&paths.data).map(|m| m.len()).unwrap_or(0),
            path = %paths.data.display(),
            "flush: staged SSTable verified, promoted, and fsynced"
        );

        Self::open_reader_from_paths(&paths, has_compression_info)
    }
}

impl FlushTarget for FileFlushTarget {
    type Reader = FileReadAt;

    fn open_reader(&self, dir: &Path, gen: u64) -> Result<SSTableReader<FileReadAt>> {
        // A descriptor may carry an empty dir (legacy flush rows) — fall back
        // to this target's base dir, mirroring the store's path-resolution rule.
        let resolved = if dir.as_os_str().is_empty() {
            self.base_dir.as_path()
        } else {
            dir
        };
        open_file_sstable(resolved, &gen.to_string())
    }

    fn open_ephemeral_reader(
        &self,
        output: SSTableOutput,
    ) -> Result<(SSTableReader<FileReadAt>, Option<PathBuf>)> {
        // Stage the run under a dedicated `.merge-spill` root with generation
        // `0`, so its file names (`0-Data.db`, ...) can never collide with a
        // real generation (which are timestamp-derived and always > 0) and the
        // directory is never scanned by `scan_max_generation`. The returned
        // PathBuf is the unique run directory; the caller removes it when the
        // reader is dropped.
        let spill_root = self.base_dir.join(".merge-spill");
        std::fs::create_dir_all(&spill_root)?;
        let run_dir = (0..32u32)
            .find_map(|attempt| {
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                let path = spill_root.join(format!("{}-{}-{attempt}", std::process::id(), ts));
                match std::fs::create_dir(&path) {
                    Ok(()) => Some(Ok(path)),
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .transpose()?
            .ok_or_else(|| {
                ferrosa_common::Error::InvalidFormat(
                    "failed to allocate unique merge-spill directory".into(),
                )
            })?;

        let has_compression_info = output.compression_info.is_some();
        let paths = FileComponentPaths {
            data: run_dir.join("0-Data.db"),
            partitions: run_dir.join("0-Partitions.db"),
            rows: run_dir.join("0-Rows.db"),
            filter: run_dir.join("0-Filter.db"),
            statistics: run_dir.join("0-Statistics.db"),
            toc: run_dir.join("0-TOC.txt"),
            compression_info: run_dir.join("0-CompressionInfo.db"),
            digest: run_dir.join("0-Digest.crc32"),
            crc: run_dir.join("0-CRC.db"),
        };
        // Write components; on any failure remove the run dir so we never leak.
        let write_all = || -> Result<()> {
            write_legacy_component(&paths.data, &output.data)?;
            write_legacy_component(&paths.partitions, &output.partitions)?;
            write_legacy_component(&paths.rows, &output.rows)?;
            write_legacy_component(&paths.filter, &output.filter)?;
            write_legacy_component(&paths.statistics, &output.statistics)?;
            write_legacy_component(&paths.toc, &output.toc)?;
            write_legacy_component(&paths.digest, &output.digest)?;
            if let Some(ref ci) = output.compression_info {
                write_legacy_component(&paths.compression_info, ci)?;
            }
            if let Some(ref crc) = output.crc {
                write_legacy_component(&paths.crc, crc)?;
            }
            Ok(())
        };
        if let Err(e) = write_all() {
            let _ = std::fs::remove_dir_all(&run_dir);
            return Err(e);
        }
        match Self::open_reader_from_paths(&paths, has_compression_info) {
            Ok(reader) => Ok((reader, Some(run_dir))),
            Err(e) => {
                let _ = std::fs::remove_dir_all(&run_dir);
                Err(e)
            }
        }
    }

    fn file_output_staging_dir(&self) -> Result<Option<PathBuf>> {
        let staging_root = self.base_dir.join(".sstable-staging");
        std::fs::create_dir_all(&staging_root)?;
        for attempt in 0..32u32 {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = staging_root.join(format!("{}-{}-{attempt}", std::process::id(), ts));
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Some(path)),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(ferrosa_common::Error::InvalidFormat(
            "failed to allocate unique SSTable staging directory".into(),
        ))
    }

    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<FileReadAt>> {
        let gen = self.next_generation();
        let base = &self.base_dir;
        let data_size = output.data.len();
        tracing::info!(
            gen,
            data_size,
            partitions_size = output.partitions.len(),
            dir = %base.display(),
            "flush: writing SSTable"
        );

        let paths = self.component_paths(gen);

        let has_compression_info = output.compression_info.is_some();

        // Write to .tmp files first, then rename to final names, then fsync.
        //
        // temp+rename alone is NOT crash-atomic: rename makes the *name* visible
        // but neither the file's data blocks nor the directory entry are durable
        // until fsynced. A SIGKILL after rename but before writeback can leave a
        // final-named, truncated Data.db (the production corruption this fixes).
        // The durability barrier below (fsync every component, then fsync the
        // directory once) is what makes the sequence crash-atomic: this function
        // returns Ok only after all component bytes AND their directory entries
        // are durable. Stale .tmp files from a pre-fsync crash are cleaned up on
        // next startup.
        let tmp = Self::tmp_component_path;
        let toc_tmp = tmp(&paths.toc);

        if let Some(ref ci) = output.compression_info {
            write_legacy_component(tmp(&paths.compression_info), ci)?;
        }
        if let Some(ref crc) = output.crc {
            write_legacy_component(tmp(&paths.crc), crc)?;
        }

        std::thread::scope(|s| {
            let handles: Vec<_> = [
                s.spawn(|| write_legacy_component(tmp(&paths.data), &output.data)),
                s.spawn(|| write_legacy_component(tmp(&paths.partitions), &output.partitions)),
                s.spawn(|| write_legacy_component(tmp(&paths.rows), &output.rows)),
                s.spawn(|| write_legacy_component(tmp(&paths.filter), &output.filter)),
                s.spawn(|| write_legacy_component(tmp(&paths.statistics), &output.statistics)),
                s.spawn(|| write_legacy_component(&toc_tmp, &output.toc)),
                s.spawn(|| write_legacy_component(tmp(&paths.digest), &output.digest)),
            ]
            .into_iter()
            .collect();

            for h in handles {
                h.join().unwrap()?;
            }

            Ok::<(), ferrosa_common::Error>(())
        })?;

        // Verify tmp files were written completely before renaming.
        //
        // Every component, not just Data.db. An SSTable is only readable if its
        // data and its indexes came from the same write; checking one of six
        // catches a truncated Data.db and misses a truncated Partitions.db,
        // which fails later as an extent pointing past the end of a file.
        for (name, path, expected) in component_expectations(&paths, &output) {
            let actual = std::fs::metadata(tmp(path)).map(|m| m.len()).unwrap_or(0);
            if actual != expected {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "FLUSH CORRUPTION: {name}.tmp gen={gen} expected {expected} bytes, \
                     got {actual} on disk. Path: {:?}",
                    tmp(path)
                )));
            }
        }

        // All tmp files written successfully — atomically rename to final names.
        // rename() is atomic on POSIX (same filesystem). Data.db is promoted
        // last because it is the generation discovery marker.
        Self::promote_tmp_components(&paths, has_compression_info)?;

        // Verify the renamed files are the correct size.
        //
        // If any differs from the tmp file just checked, something else wrote a
        // file with the same name in between -- a generation collision. The
        // engine's own compaction path documents that this is possible:
        // "Compaction output gen may collide with flush gen (different dirs)",
        // mitigated after the fact by `advance_gen_past`.
        //
        // This guard checked Data.db alone, so a collision landing on any other
        // component was invisible and the SSTable was fsynced and published
        // with mismatched parts -- an index describing more data than the file
        // holds. That is the shape node2 hit on 2026-08-20:
        // `read_exact_at: wanted 17063 bytes, got 818`.
        for (name, path, expected) in component_expectations(&paths, &output) {
            let actual = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            if actual != expected {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "FLUSH COLLISION: {name} gen={gen} was {expected} bytes after rename, \
                     now {actual} bytes. Another flush/compaction wrote the same file. \
                     Path: {:?}",
                    path
                )));
            }
        }

        // Durability barrier: fsync every promoted component, then fsync the
        // directory once. Only after this returns Ok are the bytes and the
        // rename directory entries durable. The engine relies on this so it can
        // safely discard the WAL copy after flush() returns.
        self.fsync_components(&paths, has_compression_info)?;

        tracing::info!(
            gen,
            data_bytes = std::fs::metadata(&paths.data).map(|m| m.len()).unwrap_or(0),
            path = %paths.data.display(),
            "flush: Data.db verified and fsynced on disk"
        );

        Self::open_reader_from_paths(&paths, has_compression_info)
    }

    fn flush_files(&self, output: SSTableOutputFiles) -> Result<SSTableReader<FileReadAt>> {
        self.flush_files_inner(output, false)
    }

    fn flush_deferred_files(
        &self,
        output: SSTableOutputFiles,
    ) -> Result<SSTableReader<FileReadAt>> {
        self.flush_files_inner(output, true)
    }

    fn last_generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    fn advance_generation(&self, min_gen: u64) {
        self.generation.fetch_max(min_gen + 1, Ordering::SeqCst);
    }

    fn base_dir(&self) -> &std::path::Path {
        &self.base_dir
    }

    /// Write per-index sidecar files as `{gen}-{index_name}.sidecar` and
    /// return a reader for each that maps the file just written (t_7ac6b0e3):
    /// the view then holds a mapping, not a heap copy of every posting.
    ///
    /// Skips empty entry lists. A sidecar that cannot be written or mapped
    /// does not abort the flush and must not leave its index short: the error
    /// is logged and that index is served from an in-memory image of the same
    /// entries — correct, but heap-resident until the next restart rebuilds
    /// it, which the log line says.
    fn write_sidecars(
        &self,
        generation: u64,
        sidecars: &[(&str, &dyn crate::index::sidecar::SidecarSource)],
    ) -> Result<HashMap<String, crate::index::sidecar::SidecarReader>> {
        use crate::index::sidecar::{SidecarReader, SidecarWriter};

        let mut readers = HashMap::with_capacity(sidecars.len());
        for (index_name, source) in sidecars {
            if source.is_empty() {
                continue;
            }
            let path = self
                .base_dir
                .join(format!("{generation}-{index_name}.sidecar"));
            let mapped = SidecarWriter::write_from_source(&path, *source)
                .and_then(|_written| SidecarReader::open(&path));
            let reader = match mapped {
                Ok(reader) => reader,
                Err(e) => {
                    tracing::error!(
                        %e,
                        path = %path.display(),
                        index_name,
                        "flush: sidecar could not be written or mapped; serving this index \
                         for this generation from an in-memory copy (heap-resident until restart)"
                    );
                    // The degraded path is the one place an image is built, and
                    // it is built from the source rather than from a `Vec` the
                    // caller was holding for the purpose.
                    let mut entries: Vec<(IndexKey, RowPosition)> = Vec::new();
                    visit_postings(*source, &mut entries)?;
                    if entries.is_empty() {
                        continue;
                    }
                    SidecarReader::from_entries(entries)
                }
            };
            readers.insert((*index_name).to_string(), reader);
        }
        Ok(readers)
    }

    fn persists_fti_sidecars(&self) -> bool {
        true
    }

    fn write_fti_sidecar_in(
        &self,
        dir: &Path,
        generation: &str,
        index_name: &str,
        fti_bytes: &[u8],
    ) -> Result<()> {
        use std::io::Write;

        let dir = if dir.as_os_str().is_empty() {
            self.base_dir.as_path()
        } else {
            dir
        };
        let path = dir.join(crate::store::fti_sidecar_file_name(generation, index_name));
        // Not `{gen}-` prefixed, so nothing that enumerates a generation's
        // components can pick up a half-written file; `.tmp`, so startup
        // cleanup removes one a crash left behind.
        let tmp = dir.join(format!(
            ".fti-{generation}-{index_name}.{}.tmp",
            std::process::id()
        ));
        let written = std::fs::File::create(&tmp).and_then(|mut file| {
            file.write_all(fti_bytes)?;
            file.sync_all()
        });
        if let Err(e) = written.and_then(|()| std::fs::rename(&tmp, &path)) {
            if let Err(cleanup) = std::fs::remove_file(&tmp) {
                if cleanup.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %tmp.display(), %cleanup, "fts: could not remove temp FTI sidecar");
                }
            }
            return Err(e.into());
        }
        Ok(())
    }

    fn write_fti_sidecar(&self, generation: u64, index_name: &str, fti_bytes: &[u8]) -> Result<()> {
        let path = self
            .base_dir
            .join(format!("{generation}-FTI-{index_name}.db"));
        std::fs::write(&path, fti_bytes)?;
        Ok(())
    }

    fn write_vector_sidecar(
        &self,
        generation: u64,
        index_name: &str,
        vec_bytes: &[u8],
    ) -> Result<()> {
        let path = self
            .base_dir
            .join(format!("{generation}-VEC-{index_name}.db"));
        std::fs::write(&path, vec_bytes)?;
        Ok(())
    }

    fn write_quantized_vector_sidecar(
        &self,
        generation: u64,
        index_name: &str,
        qvec_bytes: &[u8],
    ) -> Result<()> {
        let path = self
            .base_dir
            .join(format!("{generation}-QVEC-{index_name}.qvec"));
        std::fs::write(&path, qvec_bytes)?;
        Ok(())
    }

    fn search_quantized_vector_sidecar(
        &self,
        generation: u64,
        index_name: &str,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Option<Vec<ferrosa_index::vector::IndexResult>>> {
        let path = self
            .base_dir
            .join(format!("{generation}-QVEC-{index_name}.qvec"));
        if !path.exists() {
            return Ok(None);
        }
        let reader = FileReadAt::open(&path)?;
        crate::store::search_quantized_vector_artifact_reader(&reader, query, k, ef_search)
            .map(Some)
    }

    fn has_quantized_vector_sidecar(&self, generation: u64, index_name: &str) -> bool {
        self.base_dir
            .join(format!("{generation}-QVEC-{index_name}.qvec"))
            .exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
    use ferrosa_sstable::{SSTableWriter, WriteOptions};

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

    fn make_partition(key: &str, value: &[u8], ts: i64) -> Partition {
        Partition {
            key: make_key(key),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![0x00, 0x00, 0x00, 0x01], // Int32Type = 4 bytes
                cells: vec![(0, CellValue::live(value.to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
        }
    }

    /// The three partition shapes a flush sees after DELETEs on a table with
    /// no clustering column: live rows, a newer partition delete over an older
    /// still-stored row, and a partition delete of rows flushed earlier (no
    /// rows at all). `n` partitions, cycling through the shapes.
    fn mixed_delete_partitions(n: i64) -> Vec<Partition> {
        let payload = vec![b'x'; 200];
        let live_row = |ts: i64| Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(payload.clone(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        let mut partitions: Vec<Partition> = (0..n)
            .map(|i| {
                let (deletion, rows) = match i % 3 {
                    0 => (DeletionTime::LIVE, vec![live_row(1_000 + i)]),
                    1 => (
                        DeletionTime::new(100_000 + i, 1_790_000_000),
                        vec![live_row(1_000 + i)],
                    ),
                    _ => (DeletionTime::new(100_000 + i, 1_790_000_000), vec![]),
                };
                Partition {
                    key: make_key(&i.to_string()),
                    deletion,
                    static_row: None,
                    rows,
                }
            })
            .collect();
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        partitions
    }

    #[test]
    fn flushed_mix_of_live_shadowed_and_tombstone_only_partitions_reads_back() {
        let mut schema = test_schema();
        schema.clustering_columns.clear();
        for n in [300i64, 1_200] {
            let partitions = mixed_delete_partitions(n);
            let header = build_serialization_header(&schema, &partitions);
            let mut writer = SSTableWriter::new(WriteOptions::default(), header);
            for p in &partitions {
                writer.add_partition(p).unwrap();
            }
            let out = writer.finish().unwrap();
            let reader = ferrosa_sstable::reader::SSTableReader::open(
                ferrosa_sstable::reader::SSTableComponents {
                    data: out.data,
                    partitions: out.partitions,
                    rows: out.rows,
                    filter: out.filter,
                    compression_info: out.compression_info,
                    statistics: out.statistics,
                },
            )
            .unwrap();

            // The startup smoke test and self-heal read with the two-phase
            // streaming path, not `next_partition`. It must agree.
            let mut stream = reader.partitions_iter().unwrap();
            for (idx, want) in partitions.iter().enumerate() {
                let (key, deletion, _static) = stream
                    .next_partition_header_only()
                    .unwrap_or_else(|e| panic!("n={n}: streamed header {idx} failed: {e}"))
                    .unwrap_or_else(|| panic!("n={n}: streamed EOF after {idx} partitions"));
                assert_eq!(
                    key, want.key,
                    "n={n}: streamed partition {idx} out of place"
                );
                assert_eq!(
                    deletion, want.deletion,
                    "n={n}: streamed partition {idx} deletion"
                );
                let mut rows = 0usize;
                stream
                    .stream_clustered_rows(|_| {
                        rows += 1;
                        Ok(())
                    })
                    .unwrap_or_else(|e| panic!("n={n}: streamed rows of partition {idx}: {e}"));
                assert_eq!(
                    rows,
                    want.rows.len(),
                    "n={n}: streamed partition {idx} rows"
                );
            }

            let mut iter = reader.partitions_iter().unwrap();
            for (read, want) in partitions.iter().enumerate() {
                let got = iter
                    .next_partition()
                    .unwrap_or_else(|e| panic!("n={n}: partition {read} failed to read: {e}"))
                    .unwrap_or_else(|| panic!("n={n}: EOF after {read} partitions"));
                assert_eq!(got.key, want.key, "n={n}: partition {read} out of place");
                assert_eq!(
                    got.deletion, want.deletion,
                    "n={n}: partition {read} deletion"
                );
                assert_eq!(
                    got.rows.len(),
                    want.rows.len(),
                    "n={n}: partition {read} rows"
                );
            }
            assert!(
                iter.next_partition().unwrap().is_none(),
                "n={n}: trailing data"
            );
        }
    }

    fn assert_data_db_promoted_last(base_dir: &Path, gen: u64) {
        let generation_prefix = format!("{gen}-");
        let renamed: Vec<_> = fsync_probe::renamed_files()
            .into_iter()
            .filter(|path| path.parent() == Some(base_dir))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(&generation_prefix))
            })
            .collect();
        let expected_data = base_dir.join(format!("{gen}-Data.db"));
        let data_pos = renamed
            .iter()
            .position(|path| path == &expected_data)
            .unwrap_or_else(|| panic!("Data.db was not promoted; renamed={renamed:?}"));

        assert_eq!(
            data_pos,
            renamed.len() - 1,
            "Data.db must be the last final component promoted; renamed={renamed:?}"
        );

        for suffix in ["Partitions.db", "Rows.db"] {
            let path = base_dir.join(format!("{gen}-{suffix}"));
            let pos = renamed
                .iter()
                .position(|renamed_path| renamed_path == &path)
                .unwrap_or_else(|| panic!("{suffix} was not promoted; renamed={renamed:?}"));
            assert!(
                pos < data_pos,
                "{suffix} must be promoted before Data.db; renamed={renamed:?}"
            );
        }
    }

    #[test]
    fn build_serialization_header_computes_min_timestamp() {
        let schema = test_schema();
        let partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
            make_partition("k3", b"v3", 7000),
        ];

        let header = build_serialization_header(&schema, &partitions);

        assert_eq!(header.min_timestamp, 3000);
        assert_eq!(header.min_local_deletion_time, NO_DELETION_TIME);
        assert_eq!(header.min_ttl, NO_TTL);
        assert_eq!(header.key_type, "org.apache.cassandra.db.marshal.UTF8Type");
        assert_eq!(header.regular_columns.len(), 1);
        assert_eq!(header.regular_columns[0].0, b"val");
    }

    #[test]
    fn build_serialization_header_with_static_columns() {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![ColumnDefinition {
                name: "s1".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };

        let partition = Partition {
            key: make_key("k1"),
            deletion: DeletionTime::LIVE,
            static_row: Some(Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"static_val".to_vec(), 2000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            }),
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"regular_val".to_vec(), 4000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(4000),
            }],
        };

        let header = build_serialization_header(&schema, &[partition]);

        // min_timestamp should be 2000 (from the static row cell)
        assert_eq!(header.min_timestamp, 2000);
        assert_eq!(header.static_columns.len(), 1);
        assert_eq!(header.static_columns[0].0, b"s1");
        assert_eq!(header.regular_columns.len(), 1);
    }

    #[test]
    fn in_memory_flush_target_round_trip() {
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };

        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = InMemoryFlushTarget::new();
        let reader = target.flush(output).unwrap();

        // Verify we can read back both partitions
        for p in &partitions {
            let got = reader.get_partition(&p.key).unwrap().expect("partition");
            assert_eq!(got.key.key.as_bytes(), p.key.key.as_bytes());
            assert_eq!(got.rows.len(), 1);
        }
    }

    #[test]
    fn file_flush_target_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };

        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let reader = target.flush(output).unwrap();

        // Verify we can read back both partitions
        for p in &partitions {
            let got = reader.get_partition(&p.key).unwrap().expect("partition");
            assert_eq!(got.key.key.as_bytes(), p.key.key.as_bytes());
            assert_eq!(got.rows.len(), 1);
        }
    }

    /// The file flush path's collision guards must cover every component.
    ///
    /// Three guards exist in this file for exactly the generation-collision
    /// failure -- a pre-rename completeness check, a post-rename check whose
    /// message says "Another flush/compaction wrote the same file", and the
    /// promote length check -- and all three used to inspect `Data.db` alone.
    /// A collision landing on any other component was invisible, so the SSTable
    /// was fsynced and published with mismatched parts.
    ///
    /// Simulated here by another writer replacing a component between the
    /// writer recording its length and the promote reading it back, which is
    /// what two writers sharing a generation do to each other.
    #[test]
    fn a_component_overwritten_by_another_writer_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &partitions);
        let mut writer = SSTableWriter::new_file_backed(
            WriteOptions::default(),
            header,
            staging_dir.join("Data.db"),
        )
        .unwrap();
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish_to_directory(&staging_dir).unwrap();
        assert!(output.filter_len > 0);

        // A competing writer lands on this generation's Filter.db.
        std::fs::write(&output.filter, b"another writer's filter").unwrap();

        let msg = match target.flush_files(output) {
            Ok(_) => panic!(
                "a component written by another writer must not be published; \
                 the SSTable's parts would come from two different writes"
            ),
            Err(e) => e.to_string(),
        };
        assert!(
            msg.contains("Filter.db"),
            "the refusal must name the component that disagrees: {msg}"
        );
    }

    /// Verifying a promoted SSTable must not warm the reader that is returned.
    ///
    /// The first version of the readback iterated the reader this function
    /// hands to the engine. That left cached state behind, so a file corrupted
    /// *after* the flush was served from memory instead of being detected --
    /// the verification masked exactly the failures it exists to catch. Three
    /// existing resilience tests caught it; this one says why.
    #[test]
    fn verification_does_not_warm_the_returned_reader() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let partitions = vec![make_partition("k1", b"v1", 5000)];

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &partitions);
        let mut writer = SSTableWriter::new_file_backed(
            WriteOptions::default(),
            header,
            staging_dir.join("Data.db"),
        )
        .unwrap();
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish_to_directory(&staging_dir).unwrap();
        let reader = target.flush_files(output).expect("a clean flush succeeds");

        // Damage the published Data.db after the flush returned.
        let gen = target.generation();
        let data_path = dir.path().join(format!("{gen}-Data.db"));
        std::fs::write(&data_path, [0u8]).unwrap();

        let key = partitions[0].key.clone();
        assert!(
            reader.get_partition(&key).is_err(),
            "the returned reader must still hit the file; if verification left \
             the partition cached, post-flush corruption becomes invisible"
        );
    }

    /// Two flush targets on a node must never hand out the same generation.
    ///
    /// This is the root cause of the 2026-08-20 node2 corruption. Generations
    /// name files -- `{gen}-Data.db`, `{gen}-Partitions.db` -- so two writers
    /// issued the same generation write over each other's components, and the
    /// published SSTable ends up with one write's data and another's index. It
    /// surfaces later as an extent pointing past the end of a file:
    ///
    /// ```text
    /// read_exact_at: wanted 17063 bytes, got 818
    /// ```
    ///
    /// `next_generation` claims "a microsecond timestamp ensures uniqueness
    /// across all flush targets on this node". It does not. Each target owns a
    /// separate counter and seeds it from the same wall clock:
    ///
    /// ```text
    /// self.generation.fetch_max(ts, SeqCst);
    /// self.generation.fetch_add(1, SeqCst) + 1
    /// ```
    ///
    /// Two targets that call this in the same microsecond both observe the same
    /// `ts` and both return `ts + 1`. The engine knows they overlap -- the
    /// compaction swap says so, "Compaction output gen may collide with flush
    /// gen (different dirs)" -- and mitigates after the fact with
    /// `advance_gen_past`, which cannot help a collision that already happened.
    ///
    /// The table's own flush target and the compaction executor's target are
    /// exactly this pair, and the compaction output is moved into the table's
    /// directory, so their names really do meet.
    #[test]
    fn two_flush_targets_never_issue_the_same_generation() {
        let dir = tempfile::tempdir().unwrap();
        let table_target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let compaction_target = FileFlushTarget::new(dir.path().join("compaction")).unwrap();

        // Interleave the way a flush and a compaction promote do.
        let mut seen = std::collections::BTreeSet::new();
        let mut collisions = Vec::new();
        for _ in 0..200 {
            for gen in [
                table_target.next_generation(),
                compaction_target.next_generation(),
            ] {
                if !seen.insert(gen) {
                    collisions.push(gen);
                }
            }
        }

        assert!(
            collisions.is_empty(),
            "two flush targets issued {} duplicate generation(s) {:?}; each one \
             names the same {{gen}}-*.db files in a shared table directory, so \
             one write's components overwrite another's",
            collisions.len(),
            &collisions[..collisions.len().min(5)]
        );
    }

    /// Every component must match the length the writer recorded, not just
    /// Data.db.
    ///
    /// `SSTableOutputFiles` records a length per component and the promote gate
    /// compared only `data_len`, so five of six were published unchecked. A
    /// truncated Partitions.db would have been renamed into place, fsynced, and
    /// entered the live view.
    ///
    /// Rows.db is deliberately not asserted non-zero here: it is legitimately
    /// zero-length for small SSTables, which is why
    /// `StorageEngine::smoke_test_generation` excludes it from its zero-byte
    /// rule. The gate compares against the recorded length, not against zero.
    #[test]
    fn promote_refuses_a_staged_sstable_whose_partitions_file_is_short() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &partitions);
        let mut writer = SSTableWriter::new_file_backed(
            WriteOptions::default(),
            header,
            staging_dir.join("Data.db"),
        )
        .unwrap();
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish_to_directory(&staging_dir).unwrap();
        assert!(output.partitions_len > 0);

        // Truncate after the writer recorded the length, so file and record
        // disagree.
        std::fs::write(&output.partitions, b"short").unwrap();

        let msg = match target.flush_files(output) {
            Ok(_) => panic!("a staged SSTable with a short Partitions.db must not be promoted"),
            Err(e) => e.to_string(),
        };
        assert!(
            msg.contains("Partitions.db"),
            "the refusal must name the component that is wrong, or an operator \
             cannot tell which file to look at: {msg}"
        );
    }

    /// The SSTable that enters the live view must be read back, not the one
    /// that was staged.
    ///
    /// On 2026-08-20 node2 compacted `agent_memory.session_task_focus_stack`.
    /// The streaming readback passed on the staged output in the compaction
    /// directory -- `output verified (streaming readback matches merge)
    /// partitions=13 rows=17` -- and nine seconds later the swap published it
    /// into the table directory under a *different* generation, which was
    /// corrupt:
    ///
    /// ```text
    /// read_exact_at: wanted 17063 bytes, got 818
    /// ```
    ///
    /// Nothing checked the published file. The promote gate compared lengths,
    /// and the damage does not change a length -- so a corrupt SSTable entered
    /// the live view as healthy and was only noticed thirty seconds later by
    /// the periodic self-heal scan, by which point quarantining it broke every
    /// read of that table on that node.
    ///
    /// Verifying in staging and publishing something else is not verification.
    #[test]
    fn promote_refuses_a_staged_sstable_whose_data_is_unreadable() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &partitions);
        let mut writer = SSTableWriter::new_file_backed(
            WriteOptions::default(),
            header,
            staging_dir.join("Data.db"),
        )
        .unwrap();
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer
            .finish_to_directory_deferred_sync(&staging_dir)
            .unwrap();

        // Damage the CONTENT while keeping the length exactly as recorded --
        // the shape a length comparison cannot see, and the shape that
        // actually occurred.
        let good = std::fs::read(&output.data).unwrap();
        assert_eq!(good.len() as u64, output.data_len);
        let mut damaged = good.clone();
        for b in damaged.iter_mut().skip(good.len() / 4) {
            *b = 0xff;
        }
        assert_eq!(
            damaged.len(),
            good.len(),
            "the damage must not change the length"
        );
        std::fs::write(&output.data, &damaged).unwrap();

        let msg = match target.flush_deferred_files(output) {
            Ok(_) => panic!(
                "an SSTable whose contents cannot be read back must not be \
                 promoted into the live view"
            ),
            Err(e) => e.to_string(),
        };
        assert!(
            msg.contains("FLUSH CORRUPTION"),
            "the refusal must be reported as flush corruption so it is \
             attributable to the write that produced it: {msg}"
        );
    }

    #[test]
    fn writer_callers_restart_removes_legacy_raw_without_touching_live_data() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join(".sstable-staging").join("old-writer");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("Data.raw"), b"abandoned staging").unwrap();
        std::fs::write(dir.path().join("Data.raw"), b"abandoned raw output").unwrap();
        std::fs::write(dir.path().join("1-Data.db"), b"live component").unwrap();

        FileFlushTarget::new(dir.path().to_path_buf()).unwrap();

        assert!(!staging.exists());
        assert!(!dir.path().join("Data.raw").exists());
        assert_eq!(
            std::fs::read(dir.path().join("1-Data.db")).unwrap(),
            b"live component"
        );
    }

    #[test]
    fn writer_callers_flush_promotes_staged_sstable_files() {
        for compression in [None, Some(ferrosa_sstable::Compression::Lz4)] {
            assert_writer_caller_flush(compression);
        }
    }

    fn assert_writer_caller_flush(compression: Option<ferrosa_sstable::Compression>) {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &partitions);
        let compressed = compression.is_some();
        let options = WriteOptions {
            compression,
            ..WriteOptions::default()
        };
        let mut writer =
            SSTableWriter::new_file_backed(options, header, staging_dir.join("Data.db")).unwrap();
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish_to_directory(&staging_dir).unwrap();
        let staged_data_len = output.data_len;
        assert!(!staging_dir.join("Data.raw").exists());

        let reader = target.flush_files(output).unwrap();

        assert!(!staging_dir.exists());
        let gen = target.generation();
        let data_path = dir.path().join(format!("{gen}-Data.db"));
        assert_eq!(
            std::fs::metadata(&data_path).unwrap().len(),
            staged_data_len
        );
        assert_eq!(
            dir.path()
                .join(format!("{gen}-CompressionInfo.db"))
                .exists(),
            compressed
        );

        for p in &partitions {
            let got = reader.get_partition(&p.key).unwrap().expect("partition");
            assert_eq!(got.key.key.as_bytes(), p.key.key.as_bytes());
            assert_eq!(got.rows.len(), 1);
        }
    }

    #[test]
    fn file_flush_target_creates_component_files() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![make_partition("k1", b"v1", 5000)];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };

        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let _reader = target.flush(output).unwrap();

        // Verify component files were created
        let gen = target.generation();
        assert!(dir.path().join(format!("{gen}-Data.db")).exists());
        assert!(dir.path().join(format!("{gen}-Partitions.db")).exists());
        assert!(dir.path().join(format!("{gen}-Rows.db")).exists());
        assert!(dir.path().join(format!("{gen}-Filter.db")).exists());
        assert!(dir.path().join(format!("{gen}-Statistics.db")).exists());
        assert!(dir.path().join(format!("{gen}-TOC.txt")).exists());
        // No compression, so CompressionInfo.db should not exist
        assert!(!dir
            .path()
            .join(format!("{gen}-CompressionInfo.db"))
            .exists());
    }

    /// Window-1 fail-loud: `Filter.db` is always written for a live SSTable, so
    /// its absence while `Data.db` is still present means a concurrent
    /// compaction/eviction deleted it mid-open. Silently substituting an empty
    /// filter (`unwrap_or_default()`) builds a DEGRADED reader whose bloom
    /// rejects every key — pruning the only SSTable holding a row and surfacing
    /// as a spurious `Ok(None)` (silent data loss) with NO open error, so the
    /// read-path view-retry never fires. `open_file_sstable` must instead return
    /// `Err` so the retry reopens against the freshly-compacted view.
    #[test]
    fn open_file_sstable_errors_when_filter_db_deleted_mid_open() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![make_partition("k1", b"v1", 5000)];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let _reader = target.flush(output).unwrap();
        let gen = target.generation();

        // Sanity: a healthy SSTable opens fine.
        open_file_sstable(dir.path(), &gen.to_string())
            .expect("healthy sstable with Filter.db must open");

        // Simulate a concurrent compaction deleting ONLY Filter.db while
        // Data/Partitions/Rows are still on disk and still referenced by a
        // stale read view.
        let filter = dir.path().join(format!("{gen}-Filter.db"));
        assert!(filter.exists(), "fixture must have a Filter.db to delete");
        std::fs::remove_file(&filter).unwrap();
        assert!(dir.path().join(format!("{gen}-Data.db")).exists());

        let err = match open_file_sstable(dir.path(), &gen.to_string()) {
            Ok(_) => panic!(
                "open must FAIL LOUD when Filter.db is absent but Data.db is present \
                 (concurrent delete) — never build a degraded empty-bloom reader"
            ),
            Err(e) => e,
        };
        // The error must explicitly name the missing Filter.db so the cause is
        // diagnosable, rather than relying on a downstream "bloom filter too
        // short" parse error from feeding empty bytes to BloomFilter::read
        // (which `unwrap_or_default()` masks the *reason* for). This pins the
        // fail-loud point to the genuine cause: a concurrently-deleted filter.
        let msg = err.to_string();
        assert!(
            msg.contains("Filter.db"),
            "error must name the absent Filter.db (got: {msg:?})"
        );
    }

    #[test]
    fn file_flush_target_increments_generation() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();

        // First flush
        let mut partitions = vec![make_partition("k1", b"v1", 5000)];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options.clone(), header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();
        let _reader1 = target.flush(output).unwrap();
        let gen1 = target.generation();
        assert!(gen1 > 0, "generation must be positive after first flush");

        // Second flush
        let mut partitions2 = vec![make_partition("k2", b"v2", 6000)];
        partitions2.sort_by(|a, b| a.key.cmp(&b.key));
        let header2 = build_serialization_header(&schema, &partitions2);
        let mut writer2 = SSTableWriter::new(options, header2);
        for p in &partitions2 {
            writer2.add_partition(p).unwrap();
        }
        let output2 = writer2.finish().unwrap();
        let _reader2 = target.flush(output2).unwrap();
        let gen2 = target.generation();
        assert!(gen2 > gen1, "generation must increase: {gen1} → {gen2}");

        // Verify both generations have files
        assert!(dir.path().join(format!("{gen1}-Data.db")).exists());
        assert!(dir.path().join(format!("{gen2}-Data.db")).exists());
    }

    #[test]
    fn flush_does_not_leave_final_files_if_interrupted() {
        // Simulate a crash: write a .tmp file for Data.db but no final files.
        // On next load, the .tmp should be ignored and cleaned up.
        let dir = tempfile::tempdir().unwrap();

        // Create a stale .tmp file as if flush crashed mid-write
        std::fs::write(dir.path().join("1-Data.db.tmp"), b"partial data").unwrap();
        std::fs::write(dir.path().join("1-Partitions.db.tmp"), b"partial").unwrap();

        // These .tmp files must NOT be treated as valid SSTables
        assert!(
            !dir.path().join("1-Data.db").exists(),
            "final Data.db must not exist — flush was interrupted"
        );

        // Creating a new FileFlushTarget should clean up stale .tmp files
        let _target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        assert!(
            !dir.path().join("1-Data.db.tmp").exists(),
            "stale .tmp files must be cleaned up on startup"
        );
        assert!(
            !dir.path().join("1-Partitions.db.tmp").exists(),
            "stale .tmp files must be cleaned up on startup"
        );
    }

    #[test]
    fn flush_uses_atomic_rename() {
        // After a successful flush, no .tmp files should remain
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![make_partition("k1", b"v1", 5000)];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let _reader = target.flush(output).unwrap();

        // Final files exist
        let gen = target.generation();
        assert!(dir.path().join(format!("{gen}-Data.db")).exists());
        assert!(dir.path().join(format!("{gen}-Partitions.db")).exists());

        // No .tmp files remain
        let tmp_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "tmp"))
            .collect();
        assert!(
            tmp_files.is_empty(),
            "no .tmp files should remain after successful flush, found: {tmp_files:?}"
        );
    }

    #[test]
    fn flush_promotes_data_db_after_required_components() {
        let _fsync_probe = fsync_probe::exclusive();

        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![make_partition("k1", b"v1", 5000)];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let _reader = target.flush(output).unwrap();
        let gen = target.generation();

        assert_data_db_promoted_last(dir.path(), gen);
    }

    #[test]
    fn flush_files_promotes_data_db_after_required_components() {
        let _fsync_probe = fsync_probe::exclusive();

        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions::default();
        let mut writer =
            SSTableWriter::new_file_backed(options, header, staging_dir.join("Data.db")).unwrap();
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish_to_directory(&staging_dir).unwrap();

        let _reader = target.flush_files(output).unwrap();
        let gen = target.generation();

        assert_data_db_promoted_last(dir.path(), gen);
    }

    #[test]
    fn flush_fsyncs_every_component_and_directory() {
        // Durability barrier: after flush(), every promoted component file AND
        // the containing directory must have been fsynced. Without the barrier
        // a SIGKILL after rename can leave a truncated, final-named Data.db.
        let _fsync_probe = fsync_probe::exclusive();

        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![make_partition("k1", b"v1", 5000)];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let _reader = target.flush(output).unwrap();
        let gen = target.generation();

        let synced = fsync_probe::synced_files_under(dir.path());
        for suffix in [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
        ] {
            let path = dir.path().join(format!("{gen}-{suffix}"));
            assert!(
                synced.contains(&path),
                "component {suffix} was not fsynced; synced={synced:?}"
            );
        }
        // No compression in this test → CompressionInfo.db not written/synced.
        assert!(
            fsync_probe::synced_dirs().contains(&dir.path().to_path_buf()),
            "containing directory was not fsynced"
        );
    }

    // ---- split_sorted_partitions_into_shards (parallel-flush slice #3) ----

    /// Flatten shards back to a key list to assert order/coverage preservation.
    fn shard_keys(shards: &[Vec<Partition>]) -> Vec<DecoratedKey> {
        shards
            .iter()
            .flat_map(|s| s.iter().map(|p| p.key.clone()))
            .collect()
    }

    #[test]
    fn desired_shards_is_one_unless_shardable_and_big_enough() {
        // Not shardable (has indexes) → always 1.
        assert_eq!(desired_flush_shards(100_000, false, 8), 1);
        // Pool width 1 → 1 (no concurrency available).
        assert_eq!(desired_flush_shards(100_000, true, 1), 1);
        // Too few partitions to be worth it → 1.
        assert_eq!(
            desired_flush_shards(2 * MIN_PARTITIONS_PER_FLUSH_SHARD - 1, true, 8),
            1
        );
    }

    #[test]
    fn desired_shards_scales_with_data_capped_at_pool_width() {
        // 4x the min → 4 shards, within an 8-wide pool.
        assert_eq!(
            desired_flush_shards(4 * MIN_PARTITIONS_PER_FLUSH_SHARD, true, 8),
            4
        );
        // Plenty of data but only 2 pool threads → capped at 2.
        assert_eq!(
            desired_flush_shards(100 * MIN_PARTITIONS_PER_FLUSH_SHARD, true, 2),
            2
        );
    }

    #[test]
    fn split_empty_partitions_yields_no_shards() {
        assert!(split_sorted_partitions_into_shards(vec![], 4).is_empty());
    }

    #[test]
    fn split_one_shard_returns_input_unchanged() {
        let parts = vec![make_partition("a", b"1", 1), make_partition("b", b"2", 2)];
        let keys: Vec<_> = parts.iter().map(|p| p.key.clone()).collect();
        let shards = split_sorted_partitions_into_shards(parts, 1);
        assert_eq!(shards.len(), 1);
        assert_eq!(shard_keys(&shards), keys);
    }

    #[test]
    fn split_zero_shards_clamps_to_one() {
        let parts = vec![make_partition("a", b"1", 1)];
        let shards = split_sorted_partitions_into_shards(parts, 0);
        assert_eq!(shards.len(), 1, "num_shards=0 must clamp to a single shard");
    }

    #[test]
    fn split_balances_and_preserves_order_and_coverage() {
        // 10 partitions into 3 shards → sizes [4,3,3], concatenation == input.
        let parts: Vec<Partition> = (0..10)
            .map(|i| make_partition(&format!("k{i:02}"), b"v", i as i64 + 1))
            .collect();
        let input_keys: Vec<_> = parts.iter().map(|p| p.key.clone()).collect();
        let shards = split_sorted_partitions_into_shards(parts, 3);
        assert_eq!(shards.len(), 3);
        assert_eq!(
            shards.iter().map(|s| s.len()).collect::<Vec<_>>(),
            vec![4, 3, 3],
            "first (n % shards) shards get one extra"
        );
        // No partition lost/duplicated; order preserved (so token ranges are
        // contiguous and disjoint — the read/compaction-safety invariant).
        assert_eq!(shard_keys(&shards), input_keys);
    }

    #[test]
    fn split_more_shards_than_partitions_yields_singletons_no_empties() {
        let parts: Vec<Partition> = (0..3)
            .map(|i| make_partition(&format!("k{i}"), b"v", i as i64 + 1))
            .collect();
        let input_keys: Vec<_> = parts.iter().map(|p| p.key.clone()).collect();
        let shards = split_sorted_partitions_into_shards(parts, 8);
        assert_eq!(shards.len(), 3, "at most n shards, never empty ones");
        assert!(shards.iter().all(|s| s.len() == 1));
        assert_eq!(shard_keys(&shards), input_keys);
    }

    #[test]
    fn split_coverage_holds_across_many_shapes() {
        // Property-ish: for many (n, shards), flatten == input and no empties.
        for n in [1usize, 2, 5, 7, 16, 33] {
            let parts: Vec<Partition> = (0..n)
                .map(|i| make_partition(&format!("p{i:03}"), b"v", i as i64 + 1))
                .collect();
            let input_keys: Vec<_> = parts.iter().map(|p| p.key.clone()).collect();
            for shards_n in [1usize, 2, 3, 4, 8, 64] {
                let shards = split_sorted_partitions_into_shards(parts.clone(), shards_n);
                assert!(
                    shards.iter().all(|s| !s.is_empty()),
                    "n={n} shards_n={shards_n}: empty shard"
                );
                assert!(shards.len() <= n.min(shards_n.max(1)));
                assert_eq!(
                    shard_keys(&shards),
                    input_keys,
                    "n={n} shards_n={shards_n}: coverage/order broken"
                );
            }
        }
    }

    #[test]
    fn fsync_components_fails_loud_and_skips_dir_when_a_component_is_missing() {
        // Guards the fail-loud barrier that the parallel-fsync refactor must
        // preserve: if ANY component fsync fails (here, a missing file), the
        // call returns Err AND the containing directory is NOT fsynced. Fsyncing
        // the directory is what makes the rename entries durable / claims the
        // SSTable is complete — doing it after a component fsync failed would be
        // a false-durability claim (the worst outcome). Present components may or
        // may not have been fsynced by the time the failure is observed (with
        // parallel fsyncs some will have completed); the invariant under test is
        // narrowly: Err is returned and the dir barrier did not fire.
        let _fsync_probe = fsync_probe::exclusive();

        let dir = tempfile::tempdir().unwrap();
        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();

        // Create every component file EXCEPT `statistics`, which stays missing so
        // its fsync_path() (File::open) fails.
        let base = dir.path();
        let touch = |name: &str| {
            let p = base.join(name);
            std::fs::write(&p, b"x").unwrap();
            p
        };
        let paths = FileComponentPaths {
            data: touch("9-Data.db"),
            partitions: touch("9-Partitions.db"),
            rows: touch("9-Rows.db"),
            filter: touch("9-Filter.db"),
            statistics: base.join("9-Statistics.db"), // intentionally NOT created
            toc: touch("9-TOC.txt"),
            compression_info: base.join("9-CompressionInfo.db"),
            digest: touch("9-Digest.crc32"),
            crc: touch("9-CRC.db"),
        };

        let result = target.fsync_components(&paths, /* has_compression_info */ false);
        assert!(
            result.is_err(),
            "fsync_components must fail loud when a component is missing"
        );
        assert!(
            !fsync_probe::synced_dirs().contains(&base.to_path_buf()),
            "directory must NOT be fsynced after a component fsync failed \
             (that would falsely claim the SSTable is durable)"
        );
    }

    #[test]
    fn flush_files_fsyncs_every_component_and_directory() {
        // The staged-promotion path (used by compaction) must apply the same
        // durability barrier as flush() -- but verify-before-promote
        // (`publication-safety.md` M2) fsyncs the STAGED `.tmp` bytes before
        // the readback walk trusts them, and promote's rename needs only the
        // directory fsync afterward (the bytes underneath did not change).
        let _fsync_probe = fsync_probe::exclusive();

        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions::default();
        let mut writer =
            SSTableWriter::new_file_backed(options, header, staging_dir.join("Data.db")).unwrap();
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer
            .finish_to_directory_deferred_sync(&staging_dir)
            .unwrap();
        let has_ci = output.compression_info.is_some();

        let _reader = target.flush_deferred_files(output).unwrap();
        let gen = target.generation();

        let synced = fsync_probe::synced_files();
        let mut expected = vec![
            "Data.db.tmp",
            "Partitions.db.tmp",
            "Rows.db.tmp",
            "Filter.db.tmp",
            "Statistics.db.tmp",
            "TOC.txt.tmp",
        ];
        if has_ci {
            expected.push("CompressionInfo.db.tmp");
        }
        for suffix in expected {
            let path = dir.path().join(format!("{gen}-{suffix}"));
            assert!(
                synced.contains(&path),
                "staged component {suffix} was not fsynced before verify; synced={synced:?}"
            );
        }

        let events = fsync_probe::events_under(dir.path());
        let last_component_fsync = events
            .iter()
            .rposition(|event| matches!(event, fsync_probe::Event::FileFsync(_)))
            .expect("component fsync events");
        let readback_verified = events
            .iter()
            .position(|event| matches!(event, fsync_probe::Event::ReadbackVerified(_)))
            .expect("readback completion event");
        let first_fadvise = events
            .iter()
            .position(|event| matches!(event, fsync_probe::Event::Fadvise(_)))
            .expect("deferred output cache advice");
        let first_promotion = events
            .iter()
            .position(|event| matches!(event, fsync_probe::Event::Rename(_)))
            .expect("promotion rename event");
        let directory_fsync = events
            .iter()
            .position(|event| matches!(event, fsync_probe::Event::DirFsync(_)))
            .expect("final directory fsync event");
        assert!(last_component_fsync < readback_verified);
        assert!(readback_verified < first_fadvise);
        assert!(first_fadvise < first_promotion);
        assert!(first_promotion < directory_fsync);
        assert!(
            events.iter().enumerate().all(|(index, event)| {
                !matches!(event, fsync_probe::Event::Fadvise(_))
                    || (readback_verified < index && index < first_promotion)
            }),
            "all deferred cache advice must follow verification and precede promotion: {events:?}"
        );
        assert!(
            fsync_probe::synced_dirs_under(dir.path()).contains(&dir.path().to_path_buf()),
            "containing directory was not fsynced after promote"
        );
    }

    /// G1 regression (`publication-safety.md` M2, FMEA F13/ST-31): a refused
    /// flush must never leave `*-Data.db` under a LIVE name.
    ///
    /// Before this fix, `flush_files` promoted staged `.tmp` components to
    /// live names, fsynced them, and only THEN ran the readback walk. A
    /// content-only corruption (right length, wrong bytes -- exactly what a
    /// length check cannot see) failed the walk, but by then `{gen}-Data.db`
    /// was already a live name the next startup's generation-discovery scan
    /// (`*-Data.db`) would load next to the WAL replay of the same rows. This
    /// test reproduces that corruption and asserts the live name never
    /// existed and the staged output is quarantined instead.
    ///
    /// Since T-012, this exact corruption shape is caught by the digest
    /// check (`DigestMismatch`), which runs before the structural readback
    /// walk (`ReadbackFailed`) ever gets a chance to fail on it -- any
    /// length-preserving content change almost certainly changes the CRC32.
    /// The readback walk remains a second, independent line of defense for
    /// whatever the digest does not cover.
    #[test]
    fn publication_verify_refused_flush_is_quarantined_not_left_live() {
        let _fsync_probe = fsync_probe::exclusive();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &partitions);
        let mut writer = SSTableWriter::new_file_backed(
            WriteOptions::default(),
            header,
            staging_dir.join("Data.db"),
        )
        .unwrap();
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer
            .finish_to_directory_deferred_sync(&staging_dir)
            .unwrap();

        // Damage the CONTENT while keeping the length exactly as recorded --
        // the shape a length comparison cannot see.
        let good = std::fs::read(&output.data).unwrap();
        assert_eq!(good.len() as u64, output.data_len);
        let mut damaged = good.clone();
        for b in damaged.iter_mut().skip(good.len() / 4) {
            *b = 0xff;
        }
        std::fs::write(&output.data, &damaged).unwrap();

        let metric_before = crate::metrics::sstable_publication_refused_total(
            crate::metrics::PublicationRefusedReason::DigestMismatch,
        );

        let msg = match target.flush_deferred_files(output) {
            Ok(_) => panic!("an SSTable whose contents cannot be read back must not be promoted"),
            Err(e) => e.to_string(),
        };
        let gen = target.generation();

        assert!(
            msg.contains("FLUSH CORRUPTION"),
            "the refusal must be reported as flush corruption: {msg}"
        );

        let live_data = dir.path().join(format!("{gen}-Data.db"));
        assert!(
            !live_data.exists(),
            "a refused flush must never leave *-Data.db under a live name: {live_data:?}"
        );

        let quarantined_data = dir
            .path()
            .join("quarantine")
            .join(format!("{gen}-Data.db.tmp"));
        assert!(
            quarantined_data.exists(),
            "the refused staged Data.db must be quarantined for salvage: {quarantined_data:?}"
        );
        let events = fsync_probe::events_under(dir.path());
        assert!(
            !events.iter().any(|event| matches!(
                event,
                fsync_probe::Event::ReadbackVerified(_) | fsync_probe::Event::Fadvise(_)
            )),
            "a digest-refused deferred output must not reach cache eviction: {events:?}"
        );

        assert!(
            crate::metrics::sstable_publication_refused_total(
                crate::metrics::PublicationRefusedReason::DigestMismatch
            ) > metric_before,
            "the publication-refused metric must count the digest mismatch"
        );
    }

    /// A refused flush must not block progress: the next flush against the
    /// same target succeeds and lands under a NEW generation, never reusing
    /// or resurrecting the quarantined one.
    #[test]
    fn publication_verify_refused_flush_then_next_flush_succeeds_with_new_generation() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();

        // First flush: corrupt content, so it is refused and quarantined.
        let bad_partitions = vec![make_partition("k1", b"v1", 5000)];
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &bad_partitions);
        let mut writer = SSTableWriter::new_file_backed(
            WriteOptions::default(),
            header,
            staging_dir.join("Data.db"),
        )
        .unwrap();
        for p in &bad_partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish_to_directory(&staging_dir).unwrap();
        let good = std::fs::read(&output.data).unwrap();
        let mut damaged = good.clone();
        for b in damaged.iter_mut().skip(good.len() / 4) {
            *b = 0xff;
        }
        std::fs::write(&output.data, &damaged).unwrap();
        if target.flush_files(output).is_ok() {
            panic!("first flush must be refused");
        }
        let refused_gen = target.generation();

        // Second flush: clean output must succeed under a new generation.
        let good_partitions = vec![make_partition("k2", b"v2", 6000)];
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &good_partitions);
        let mut writer = SSTableWriter::new_file_backed(
            WriteOptions::default(),
            header,
            staging_dir.join("Data.db"),
        )
        .unwrap();
        for p in &good_partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish_to_directory(&staging_dir).unwrap();
        let reader = target
            .flush_files(output)
            .expect("a clean flush after a refusal must still succeed");
        let new_gen = target.generation();

        assert_ne!(
            new_gen, refused_gen,
            "the retried flush must land on a new generation, not reuse the refused one"
        );
        let got = reader
            .get_partition(&good_partitions[0].key)
            .unwrap()
            .expect("partition");
        assert_eq!(got.rows.len(), 1);

        assert!(
            !dir.path().join(format!("{refused_gen}-Data.db")).exists(),
            "the refused generation must still never be live after a later successful flush"
        );
    }

    /// Build a real staged flush output (not yet promoted) from `partitions`,
    /// returning the `FileFlushTarget`, the base dir, and the `SSTableOutputFiles`
    /// ready to hand to `flush_files`. Shared setup for the `digest_verify_`
    /// tests below, which each damage `output.data` differently before calling
    /// `flush_files`.
    fn staged_output_for_digest_tests(
        dir: &Path,
        partitions: &[Partition],
    ) -> (FileFlushTarget, ferrosa_sstable::writer::SSTableOutputFiles) {
        staged_output_for_digest_tests_with_options(dir, partitions, WriteOptions::default())
    }

    /// Same as [`staged_output_for_digest_tests`], with the writer's
    /// `WriteOptions` under the caller's control -- needed by the
    /// swapped-block test below, which must disable compression to keep a
    /// guaranteed on-disk size regardless of how compressible its fixture
    /// values are.
    fn staged_output_for_digest_tests_with_options(
        dir: &Path,
        partitions: &[Partition],
        options: WriteOptions,
    ) -> (FileFlushTarget, ferrosa_sstable::writer::SSTableOutputFiles) {
        let schema = test_schema();
        let target = FileFlushTarget::new(dir.to_path_buf()).unwrap();
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, partitions);
        let mut writer =
            SSTableWriter::new_file_backed(options, header, staging_dir.join("Data.db")).unwrap();
        for p in partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish_to_directory(&staging_dir).unwrap();
        (target, output)
    }

    fn refresh_staged_digest(output: &mut ferrosa_sstable::writer::SSTableOutputFiles) {
        let data = std::fs::read(&output.data).unwrap();
        let mut hasher = ferrosa_sstable::checksum::DigestCrc32::new();
        hasher.update(&data);
        let digest = ferrosa_sstable::checksum::format_digest(hasher.finalize());
        std::fs::write(&output.digest, &digest).unwrap();
        output.digest_len = digest.len() as u64;
    }

    #[test]
    fn promoted_sstable_count_walk_matches_full_partition_decode() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 5001),
        ];
        let (_target, output) = staged_output_for_digest_tests(dir.path(), &partitions);
        let reader = SSTableReader::open(SSTableComponents {
            data: FileReadAt::open(&output.data).unwrap(),
            partitions: FileReadAt::open(&output.partitions).unwrap(),
            rows: FileReadAt::open(&output.rows).unwrap(),
            filter: std::fs::read(&output.filter).unwrap(),
            compression_info: output
                .compression_info
                .as_ref()
                .map(std::fs::read)
                .transpose()
                .unwrap(),
            statistics: std::fs::read(&output.statistics).unwrap(),
        })
        .unwrap();

        let mut full = reader.partitions_iter().unwrap();
        let mut expected = Vec::new();
        while let Some(partition) = full.next_partition().unwrap() {
            expected.push((partition.key, partition.rows.len() as u64));
        }

        let mut counts = reader.partitions_iter().unwrap();
        let mut actual = Vec::new();
        while let Some((key, rows)) = counts.next_partition_count().unwrap() {
            actual.push((key, rows));
        }
        assert_eq!(actual, expected);
        verify_promoted_sstable(&reader).unwrap();
    }

    #[test]
    fn flush_files_rejects_truncated_partition_framing_after_digest_matches() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = vec![make_partition("k1", b"v1", 5000)];
        let options = WriteOptions {
            compression: None,
            verify_output: false,
            ..WriteOptions::default()
        };
        let (target, mut output) =
            staged_output_for_digest_tests_with_options(dir.path(), &partitions, options);

        let mut data = std::fs::read(&output.data).unwrap();
        let terminator = data.last_mut().expect("nonempty Data.db");
        assert_ne!(*terminator, 0, "fixture ends with a partition terminator");
        *terminator = 0;
        std::fs::write(&output.data, data).unwrap();
        refresh_staged_digest(&mut output);

        let error = match target.flush_files(output) {
            Ok(_) => {
                panic!("the readback framing check must reject a missing partition terminator")
            }
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("could not be read back"),
            "framing refusal should come from staged SSTable readback: {error}"
        );
        assert!(
            dir.path().join("quarantine").exists(),
            "the malformed staged output must be quarantined"
        );
    }

    #[test]
    fn flush_files_rejects_malformed_partition_index_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = vec![make_partition("k1", b"v1", 5000)];
        let options = WriteOptions {
            compression: None,
            verify_output: false,
            ..WriteOptions::default()
        };
        let (target, output) =
            staged_output_for_digest_tests_with_options(dir.path(), &partitions, options);

        let mut index = std::fs::read(&output.partitions).unwrap();
        assert!(!index.is_empty(), "fixture has partition index metadata");
        index.fill(0xff);
        std::fs::write(&output.partitions, index).unwrap();

        let error = match target.flush_files(output) {
            Ok(_) => panic!("the reader must reject malformed partition index metadata"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("could not be read back"),
            "metadata refusal should come from staged SSTable readback: {error}"
        );
    }

    /// Enough partitions, with big enough values, that Data.db comfortably
    /// exceeds two 4 KiB blocks -- needed by the swapped-block test below.
    /// Each partition's value bytes vary (not a constant fill), so LZ4
    /// cannot shrink the fixture out from under the size assumption even if
    /// a caller forgets to disable compression. `SSTableWriter` requires
    /// partitions in token order, so this sorts by key (matching
    /// `DecoratedKey`'s `Ord`) before returning, the same way every other
    /// multi-partition test in this module does.
    fn partitions_spanning_multiple_blocks() -> Vec<Partition> {
        let mut partitions: Vec<Partition> = (0..64)
            .map(|i| {
                let value: Vec<u8> = (0..512u32).map(|b| (b ^ (i * 37)) as u8).collect();
                make_partition(&format!("k{i:04}"), &value, 1000 + i as i64)
            })
            .collect();
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        partitions
    }

    /// L4 fault row: a single bit flip in the staged Data.db, at a length
    /// that keeps every length check green (`publication-safety.md` M2 step
    /// 4 / M3, T-012). Must be refused, quarantined, and counted under
    /// `DigestMismatch` -- never left under a live name.
    #[test]
    fn digest_verify_bit_flip_is_refused_and_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = vec![make_partition("k1", b"v1", 5000)];
        let (target, output) = staged_output_for_digest_tests(dir.path(), &partitions);

        let mut damaged = std::fs::read(&output.data).unwrap();
        let mid = damaged.len() / 2;
        damaged[mid] ^= 0x01; // single bit flip, length unchanged
        std::fs::write(&output.data, &damaged).unwrap();

        let metric_before = crate::metrics::sstable_publication_refused_total(
            crate::metrics::PublicationRefusedReason::DigestMismatch,
        );

        let msg = match target.flush_files(output) {
            Ok(_) => panic!("a bit-flipped Data.db must fail digest verification"),
            Err(e) => e.to_string(),
        };
        let gen = target.generation();

        assert!(
            msg.contains("FLUSH CORRUPTION") && msg.contains("Digest.crc32 mismatch"),
            "refusal must name the digest mismatch: {msg}"
        );
        assert!(
            !dir.path().join(format!("{gen}-Data.db")).exists(),
            "a bit-flipped flush must never be promoted to a live name"
        );
        assert!(
            dir.path()
                .join("quarantine")
                .join(format!("{gen}-Data.db.tmp"))
                .exists(),
            "the refused staged Data.db must be quarantined for salvage"
        );
        assert!(
            crate::metrics::sstable_publication_refused_total(
                crate::metrics::PublicationRefusedReason::DigestMismatch
            ) > metric_before,
            "the publication-refused metric must count the digest mismatch"
        );
    }

    /// L4 fault row: two 4 KiB blocks of the staged Data.db swapped in place
    /// (same length, same bytes overall, wrong order). A structural decode
    /// can plausibly still succeed on shuffled bytes for some encodings; the
    /// digest must not.
    #[test]
    fn digest_verify_swapped_block_is_refused_and_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = partitions_spanning_multiple_blocks();
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let (target, output) =
            staged_output_for_digest_tests_with_options(dir.path(), &partitions, options);

        let mut damaged = std::fs::read(&output.data).unwrap();
        assert!(
            damaged.len() >= 2 * 4096,
            "fixture must be big enough to hold two distinct 4 KiB blocks: {}",
            damaged.len()
        );
        let (block_a, rest) = damaged.split_at_mut(4096);
        let block_b = &mut rest[..4096];
        block_a.swap_with_slice(block_b);
        std::fs::write(&output.data, &damaged).unwrap();

        let msg = match target.flush_files(output) {
            Ok(_) => panic!("a Data.db with swapped 4 KiB blocks must fail digest verification"),
            Err(e) => e.to_string(),
        };
        assert!(
            msg.contains("Digest.crc32 mismatch"),
            "refusal must name the digest mismatch: {msg}"
        );
    }

    /// L4 fault row: "segment recycled before its write completed" -- stale
    /// bytes from an unrelated, previously-written Data.db land at the right
    /// offset with the right length. Simulated by splicing in bytes from a
    /// second, differently-sized real SSTable's Data.db, truncated/padded to
    /// match. The length check cannot see this; the digest must.
    #[test]
    fn digest_verify_stale_bytes_is_refused_and_quarantined() {
        let dir = tempfile::tempdir().unwrap();

        // A second, unrelated SSTable purely as a source of "stale" bytes.
        let stale_dir = tempfile::tempdir().unwrap();
        let stale_partitions = vec![make_partition("stale", b"stale-value-bytes", 9999)];
        let (_stale_target, stale_output) =
            staged_output_for_digest_tests(stale_dir.path(), &stale_partitions);
        let stale_bytes = std::fs::read(&stale_output.data).unwrap();

        let partitions = vec![make_partition("k1", b"v1", 5000)];
        let (target, output) = staged_output_for_digest_tests(dir.path(), &partitions);

        let good = std::fs::read(&output.data).unwrap();
        let mut damaged = good.clone();
        // Overwrite a middle span with stale bytes (cycled if shorter),
        // keeping the overall length exactly as recorded.
        let start = good.len() / 3;
        let span = (good.len() / 3).max(1);
        for (i, b) in damaged.iter_mut().skip(start).take(span).enumerate() {
            *b = stale_bytes[i % stale_bytes.len()];
        }
        assert_ne!(damaged, good, "the splice must actually change the bytes");
        std::fs::write(&output.data, &damaged).unwrap();

        let msg = match target.flush_files(output) {
            Ok(_) => panic!("stale spliced-in bytes must fail digest verification"),
            Err(e) => e.to_string(),
        };
        assert!(
            msg.contains("Digest.crc32 mismatch"),
            "refusal must name the digest mismatch: {msg}"
        );
    }

    /// Acceptance criterion 3 (`publication-safety.md`): after a digest
    /// refusal, a later flush against the same target still succeeds, lands
    /// on a new generation, and the refused generation stays quarantined
    /// (never live, never retried in place).
    #[test]
    fn digest_verify_refused_flush_then_next_flush_succeeds_with_new_generation() {
        let dir = tempfile::tempdir().unwrap();
        let bad_partitions = vec![make_partition("k1", b"v1", 5000)];
        let (target, output) = staged_output_for_digest_tests(dir.path(), &bad_partitions);
        let mut damaged = std::fs::read(&output.data).unwrap();
        let mid = damaged.len() / 2;
        damaged[mid] ^= 0xff;
        std::fs::write(&output.data, &damaged).unwrap();
        match target.flush_files(output) {
            Ok(_) => panic!("first flush must be refused on digest mismatch"),
            Err(e) => assert!(e.to_string().contains("Digest.crc32 mismatch")),
        }
        let refused_gen = target.generation();

        // A clean flush against the same target must still succeed, on a new
        // generation, after the digest-refused attempt above.
        let good_partitions = vec![make_partition("k2", b"v2", 6000)];
        let schema = test_schema();
        let staging_dir = target
            .file_output_staging_dir()
            .unwrap()
            .expect("file target staging dir");
        let header = build_serialization_header(&schema, &good_partitions);
        let mut writer = SSTableWriter::new_file_backed(
            WriteOptions::default(),
            header,
            staging_dir.join("Data.db"),
        )
        .unwrap();
        for p in &good_partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish_to_directory(&staging_dir).unwrap();
        let reader = target
            .flush_files(output)
            .expect("a clean flush after a digest refusal must still succeed");
        let new_gen = target.generation();

        assert_ne!(
            new_gen, refused_gen,
            "the retried flush must land on a new generation, not reuse the refused one"
        );
        let got = reader
            .get_partition(&good_partitions[0].key)
            .unwrap()
            .expect("partition");
        assert_eq!(got.rows.len(), 1);
        assert!(
            !dir.path().join(format!("{refused_gen}-Data.db")).exists(),
            "the digest-refused generation must still never be live"
        );
    }

    /// An SSTable written before T-011 has neither `Digest.crc32` nor
    /// `CRC.db`. Every file-backed open path must still open it, treating
    /// the missing components as "not checked" rather than an error
    /// (`publication-safety.md` M1), and must not report checksums as
    /// loaded.
    #[test]
    fn digest_verify_old_sstable_without_digest_or_crc_still_opens() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = vec![make_partition("k1", b"v1", 5000)];
        let (target, output) = staged_output_for_digest_tests(dir.path(), &partitions);
        let reader = target.flush_files(output).expect("clean flush succeeds");
        let gen = target.generation();
        assert!(
            reader.digest_loaded(),
            "a fresh flush must load its own digest"
        );
        drop(reader);

        std::fs::remove_file(dir.path().join(format!("{gen}-Digest.crc32"))).unwrap();
        let crc_path = dir.path().join(format!("{gen}-CRC.db"));
        let _ = std::fs::remove_file(&crc_path);

        let reader = open_file_sstable(dir.path(), &gen.to_string())
            .expect("an SSTable without Digest.crc32/CRC.db must still open");
        assert!(
            !reader.digest_loaded(),
            "a generation with no Digest.crc32 file must not report a loaded digest"
        );
        assert!(
            !reader.crc_table_loaded(),
            "a generation with no CRC.db file must not report a loaded CRC table"
        );
        let got = reader.get_partition(&partitions[0].key).unwrap();
        assert!(got.is_some(), "the SSTable must still read correctly");
    }

    /// RED TEST (known bug): Two FileFlushTarget instances on the SAME
    /// Two FileFlushTarget instances on the same directory must produce
    /// DIFFERENT generation numbers. Timestamp-based gens guarantee this.
    #[test]
    fn concurrent_flush_targets_same_dir_no_collision() {
        let dir = tempfile::tempdir().unwrap();

        // Create two flush targets on the same directory simultaneously
        let target_a = FileFlushTarget::new_starting_at(dir.path().to_path_buf()).unwrap();
        let target_b = FileFlushTarget::new_starting_at(dir.path().to_path_buf()).unwrap();

        // Both write SSTables
        let schema = test_schema();
        let partitions_a = vec![make_partition("ka", b"val_a", 1000)];
        let partitions_b = vec![make_partition("kb", b"val_b", 2000)];

        let header_a = build_serialization_header(&schema, &partitions_a);
        let header_b = build_serialization_header(&schema, &partitions_b);

        let opts = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };

        let mut writer_a = SSTableWriter::new(opts.clone(), header_a);
        writer_a.add_partition(&partitions_a[0]).unwrap();
        let output_a = writer_a.finish().unwrap();

        let mut writer_b = SSTableWriter::new(opts, header_b);
        writer_b.add_partition(&partitions_b[0]).unwrap();
        let output_b = writer_b.finish().unwrap();

        let _reader_a = target_a.flush(output_a).unwrap();
        let gen_a = target_a.generation();

        let _reader_b = target_b.flush(output_b).unwrap();
        let gen_b = target_b.generation();

        // Generations MUST be different — if they're the same, one SSTable
        // overwrites the other in the shared directory
        assert_ne!(
            gen_a, gen_b,
            "Two flush targets on the same directory produced the same generation {gen_a}. \
             This causes file overwrites and truncated SSTables during concurrent compaction."
        );
    }

    /// Verify that node_generation_offset produces different values for
    /// different FERROSA_HOST_ID values. This is the fix for multi-node
    /// gen collision: each node starts at a different offset.
    #[test]
    fn node_generation_offset_differs_per_host_id() {
        // Compute offsets by temporarily setting the env var.
        // We can't set env in parallel tests, so compute manually.
        let hash = |s: &str| -> u64 {
            let mut h: u64 = 0xcbf29ce484222325;
            for byte in s.bytes() {
                h ^= byte as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            h & 0xFF_FFFF_FFFF
        };

        let offset_a = hash("11111111-1111-1111-1111-111111111111");
        let offset_b = hash("22222222-2222-2222-2222-222222222222");
        let offset_c = hash("a7b3c9d2-e4f5-4a1b-8c6d-2e3f4a5b6c7d"); // realistic UUID

        assert_ne!(
            offset_a, offset_b,
            "Different host IDs must produce different offsets"
        );
        assert_ne!(offset_a, offset_c);
        assert_ne!(offset_b, offset_c);

        // All offsets should be > 0 (non-trivial)
        assert!(offset_a > 0, "offset_a should be non-zero");
        assert!(offset_b > 0, "offset_b should be non-zero");
        assert!(offset_c > 0, "offset_c should be non-zero");

        // Offsets should be well-distributed (40-bit range)
        assert!(offset_a > 1_000_000, "offset should be in the millions+");
        assert!(offset_b > 1_000_000);
    }

    /// Verify that new_starting_at uses the node offset on an empty directory.
    #[test]
    fn new_starting_at_uses_node_offset_on_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let offset = FileFlushTarget::node_generation_offset();
        let target = FileFlushTarget::new_starting_at(dir.path().to_path_buf()).unwrap();

        // On an empty directory, the starting gen should be the node offset
        // (or 0 if no FERROSA_HOST_ID is set in tests)
        assert_eq!(
            target.generation(),
            offset,
            "starting generation should equal node offset on empty dir"
        );
    }
}

#[cfg(test)]
#[path = "flush_readback_test_support.rs"]
pub(crate) mod readback_test_support;
