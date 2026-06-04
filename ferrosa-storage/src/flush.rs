//! FlushTarget abstraction and serialization header construction.
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

use ferrosa_common::schema::TableSchema;
use ferrosa_common::{Result, NO_DELETION_TIME, NO_TIMESTAMP, NO_TTL};
use ferrosa_sstable::io::{FileReadAt, ReadAt};
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::Partition;
use ferrosa_sstable::writer::{SSTableOutput, SSTableOutputFiles};

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

/// Trait abstracting where flushed SSTable component bytes are stored.
///
/// Implementers decide whether the output goes to in-memory buffers or
/// to the filesystem. After writing, the trait returns an `SSTableReader`
/// so the flushed data is immediately queryable.
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
        _sidecars: &HashMap<String, Vec<(IndexKey, RowPosition)>>,
    ) -> Result<()> {
        Ok(())
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

impl FileFlushTarget {
    /// Create a new file flush target writing to the given directory.
    ///
    /// The directory is created if it does not exist. The generation
    /// counter starts at 0; the first flush produces generation 1.
    pub fn new(base_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_dir)?;
        Self::cleanup_stale_tmp_files(&base_dir);
        Ok(Self {
            base_dir,
            generation: AtomicU64::new(0),
        })
    }

    /// Remove any stale `.tmp` files left behind by a crash during flush.
    fn cleanup_stale_tmp_files(dir: &Path) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "tmp") {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
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
        Self::cleanup_stale_tmp_files(&base_dir);
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
        // Use a timestamp-based generation to guarantee global uniqueness.
        // The sequential counter can collide between the table's flush target
        // and the compaction executor's flush target (different instances with
        // overlapping gen ranges). A microsecond timestamp ensures uniqueness
        // across all flush targets on this node. The fetch_max + add ensures
        // monotonicity even if two flushes happen in the same microsecond.
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        self.generation.fetch_max(ts, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst) + 1
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
        }
    }

    fn tmp_component_path(path: &Path) -> PathBuf {
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("txt") => path.with_extension("txt.tmp"),
            _ => path.with_extension("db.tmp"),
        }
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

        SSTableReader::open(SSTableComponents {
            data,
            partitions,
            rows,
            filter,
            compression_info,
            statistics,
        })
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
}

/// Open a file-backed SSTable reader from component files for generation `gen`
/// in `dir`. Shared by [`FileFlushTarget::open_reader`] and the engine's
/// startup/load path so on-demand reopens go through one code path.
///
/// Required components (`Data.db`, `Partitions.db`, `Rows.db`) must exist;
/// optional components (`Filter.db`, `Statistics.db`, `CompressionInfo.db`)
/// default to empty/absent when missing, matching the historical engine loader.
pub fn open_file_sstable(dir: &Path, gen: &str) -> Result<SSTableReader<FileReadAt>> {
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

    let data = FileReadAt::open(required("Data.db")?)?;
    let partitions = FileReadAt::open(required("Partitions.db")?)?;
    let rows = FileReadAt::open(required("Rows.db")?)?;

    let filter = std::fs::read(dir.join(format!("{gen}-Filter.db"))).unwrap_or_default();
    let statistics = std::fs::read(dir.join(format!("{gen}-Statistics.db"))).unwrap_or_default();
    let compression_info = std::fs::read(dir.join(format!("{gen}-CompressionInfo.db"))).ok();

    SSTableReader::open(SSTableComponents {
        data,
        partitions,
        rows,
        filter,
        compression_info,
        statistics,
    })
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
        };
        // Write components; on any failure remove the run dir so we never leak.
        let write_all = || -> Result<()> {
            std::fs::write(&paths.data, &output.data)?;
            std::fs::write(&paths.partitions, &output.partitions)?;
            std::fs::write(&paths.rows, &output.rows)?;
            std::fs::write(&paths.filter, &output.filter)?;
            std::fs::write(&paths.statistics, &output.statistics)?;
            std::fs::write(&paths.toc, &output.toc)?;
            if let Some(ref ci) = output.compression_info {
                std::fs::write(&paths.compression_info, ci)?;
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

        // Write to .tmp files first, then atomically rename.
        // If the process crashes mid-flush, only .tmp files exist — no corrupt
        // final SSTables. Stale .tmp files are cleaned up on next startup.
        let tmp = Self::tmp_component_path;
        let toc_tmp = tmp(&paths.toc);

        if let Some(ref ci) = output.compression_info {
            std::fs::write(tmp(&paths.compression_info), ci)?;
        }

        std::thread::scope(|s| {
            let handles: Vec<_> = [
                s.spawn(|| std::fs::write(tmp(&paths.data), &output.data)),
                s.spawn(|| std::fs::write(tmp(&paths.partitions), &output.partitions)),
                s.spawn(|| std::fs::write(tmp(&paths.rows), &output.rows)),
                s.spawn(|| std::fs::write(tmp(&paths.filter), &output.filter)),
                s.spawn(|| std::fs::write(tmp(&paths.statistics), &output.statistics)),
                s.spawn(|| std::fs::write(&toc_tmp, &output.toc)),
            ]
            .into_iter()
            .collect();

            for h in handles {
                h.join().unwrap()?;
            }

            Ok::<(), ferrosa_common::Error>(())
        })?;

        // Verify tmp files were written completely before renaming.
        // This catches the truncation bug: if the file on disk is shorter
        // than the in-memory buffer, something overwrote or truncated it.
        {
            let expected_data_size = output.data.len() as u64;
            let actual_data_size = std::fs::metadata(tmp(&paths.data))
                .map(|m| m.len())
                .unwrap_or(0);
            if actual_data_size != expected_data_size {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "FLUSH CORRUPTION: Data.db.tmp gen={gen} expected {expected_data_size} bytes, \
                     got {actual_data_size} on disk. Buffer was {expected_data_size} bytes. \
                     Path: {:?}",
                    tmp(&paths.data)
                )));
            }
        }

        // All tmp files written successfully — atomically rename to final names.
        // rename() is atomic on POSIX (same filesystem).
        std::fs::rename(tmp(&paths.data), &paths.data)?;
        std::fs::rename(tmp(&paths.partitions), &paths.partitions)?;
        std::fs::rename(tmp(&paths.rows), &paths.rows)?;
        std::fs::rename(tmp(&paths.filter), &paths.filter)?;
        std::fs::rename(tmp(&paths.statistics), &paths.statistics)?;
        std::fs::rename(&toc_tmp, &paths.toc)?;
        if has_compression_info {
            std::fs::rename(tmp(&paths.compression_info), &paths.compression_info)?;
        }

        // Verify the renamed Data.db file is the correct size.
        // If it differs from the tmp file we just checked, something else
        // wrote a file with the same name in between (gen collision).
        {
            let expected = output.data.len() as u64;
            let actual = std::fs::metadata(&paths.data).map(|m| m.len()).unwrap_or(0);
            if actual != expected {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "FLUSH COLLISION: Data.db gen={gen} was {expected} bytes after rename, \
                     now {actual} bytes. Another flush/compaction wrote the same file. \
                     Path: {:?}",
                    paths.data
                )));
            }
        }

        tracing::info!(
            gen,
            data_bytes = std::fs::metadata(&paths.data).map(|m| m.len()).unwrap_or(0),
            path = %paths.data.display(),
            "flush: Data.db verified on disk"
        );

        Self::open_reader_from_paths(&paths, has_compression_info)
    }

    fn flush_files(&self, output: SSTableOutputFiles) -> Result<SSTableReader<FileReadAt>> {
        let gen = self.next_generation();
        let paths = self.component_paths(gen);
        let has_compression_info = output.compression_info.is_some();
        tracing::info!(
            gen,
            data_size = output.data_len,
            partitions_size = output.partitions_len,
            dir = %self.base_dir.display(),
            "flush: promoting staged SSTable"
        );

        let tmp = Self::tmp_component_path;

        std::fs::rename(&output.data, tmp(&paths.data))?;
        std::fs::rename(&output.partitions, tmp(&paths.partitions))?;
        std::fs::rename(&output.rows, tmp(&paths.rows))?;
        std::fs::rename(&output.filter, tmp(&paths.filter))?;
        std::fs::rename(&output.statistics, tmp(&paths.statistics))?;
        std::fs::rename(&output.toc, tmp(&paths.toc))?;
        if let Some(compression_info) = output.compression_info.as_ref() {
            std::fs::rename(compression_info, tmp(&paths.compression_info))?;
        }

        let actual_data_size = std::fs::metadata(tmp(&paths.data))
            .map(|m| m.len())
            .unwrap_or(0);
        if actual_data_size != output.data_len {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "FLUSH CORRUPTION: staged Data.db gen={gen} expected {} bytes, got {actual_data_size}",
                output.data_len
            )));
        }

        std::fs::rename(tmp(&paths.data), &paths.data)?;
        std::fs::rename(tmp(&paths.partitions), &paths.partitions)?;
        std::fs::rename(tmp(&paths.rows), &paths.rows)?;
        std::fs::rename(tmp(&paths.filter), &paths.filter)?;
        std::fs::rename(tmp(&paths.statistics), &paths.statistics)?;
        std::fs::rename(tmp(&paths.toc), &paths.toc)?;
        if has_compression_info {
            std::fs::rename(tmp(&paths.compression_info), &paths.compression_info)?;
        }

        let staging_parent = output.staging_dir.parent().map(Path::to_path_buf);
        let _ = std::fs::remove_dir(&output.staging_dir);
        if let Some(parent) = staging_parent {
            let _ = std::fs::remove_dir(parent);
        }

        tracing::info!(
            gen,
            data_bytes = std::fs::metadata(&paths.data).map(|m| m.len()).unwrap_or(0),
            path = %paths.data.display(),
            "flush: staged Data.db promoted"
        );

        Self::open_reader_from_paths(&paths, has_compression_info)
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

    /// Write per-index sidecar files as `{gen}-{index_name}.sidecar`.
    ///
    /// Skips empty entry lists (no-ops for indexes with no data).
    /// Files that fail to write are logged but do not abort the flush —
    /// a missing sidecar degrades to a full-scan on that index, which is
    /// recoverable. This matches the `load_existing_sstables` "skip corrupt"
    /// policy.
    fn write_sidecars(
        &self,
        generation: u64,
        sidecars: &HashMap<String, Vec<(IndexKey, RowPosition)>>,
    ) -> Result<()> {
        use crate::index::sidecar::SidecarWriter;

        for (index_name, entries) in sidecars {
            if entries.is_empty() {
                continue;
            }
            let path = self
                .base_dir
                .join(format!("{generation}-{index_name}.sidecar"));
            if let Err(e) = SidecarWriter::write(&path, entries) {
                tracing::error!(%e, path = %path.display(), "flush: failed to write sidecar");
            }
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

    #[test]
    fn file_flush_target_promotes_staged_sstable_files() {
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
            SSTableWriter::new_file_backed(options, header, staging_dir.join("Data.raw")).unwrap();
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
        assert!(dir
            .path()
            .join(format!("{gen}-CompressionInfo.db"))
            .exists());

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
