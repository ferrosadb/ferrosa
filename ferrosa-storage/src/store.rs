//! Module: Compose lock-free memtable, flush, SSTable read, and metadata views.
//! Correctness: Correct when every ArcSwap view is internally aligned and read
//! and compaction planning preserve key bounds without resident-reader fanout.
//! Last revised: 2026-10-03
//! Last changed: Removed the per-table locks (write gate, view CAS, rotation
//! queue, ArcSwap sets), t_d938e6ae.
//!
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
//! Memtable rotations (flush, index DDL, ALTER, TRUNCATE) run one at a time
//! through a lock-free queue (`TableStore::rotate`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// A partition read touching more immutable files than this is degraded enough
/// to page operators. The read still completes through the bounded reader pool
/// while background compaction drains the backlog.
const READ_FANOUT_ALERT_SSTABLES: usize = 32;
const READ_FANOUT_ERROR_INTERVAL_SECS: u64 = 60;
const MAX_COMPACTION_CANDIDATES: usize = 64;
static LAST_READ_FANOUT_ERROR_UNIX_SECS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

use arc_swap::ArcSwap;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::TableSchema;
use ferrosa_common::Result;
use ferrosa_index::{FilterPredicate, IndexKey, IndexType, RowPosition};
use ferrosa_sstable::io::ReadAt;
use ferrosa_sstable::reader::SSTableReader;
use ferrosa_sstable::types::{DeletionTime, Partition, Row};
use ferrosa_sstable::writer::{SSTableOutput, SSTableOutputFiles, SSTableWriter};
use ferrosa_sstable::WriteOptions;
use rayon::prelude::*;

use ferrosa_index::DistanceMetric;

use crate::flush::{self, FlushTarget};
use crate::index::sidecar::{RowPositionRef, SidecarReader};
use crate::memtable::index::MemtableIndex;
#[cfg(any(not(feature = "skiplist-memtable"), miri))]
use crate::memtable::sharded::ShardedBTreeMemtable;
#[cfg(all(feature = "skiplist-memtable", not(miri)))]
use crate::memtable::skiplist::SkipListMemtable;
use crate::memtable::vector_index::VectorMemtableIndex;
use crate::memtable::Memtable;
use crate::merge;
use crate::range_merger::ColumnOrdinalMapping;

/// What a flush did to the table's SSTable set.
///
/// The flush path has two early exits that write nothing; they must stay
/// distinguishable from a real publish because everything downstream of a
/// flush (eager index builds, pin accounting) acts on "the SSTable this flush
/// just wrote".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a flush that published an SSTable needs the engine's post-flush bookkeeping"]
pub enum FlushOutcome {
    /// No SSTable was written: the memtable was empty, or every row was
    /// quarantined.
    NothingToFlush,
    /// At least one new SSTable was installed in the live view.
    Published,
}

/// Outcome of resolving an SSTable's column-ordinal mapping.
///
/// `SstableGone` is benign — compaction replaced the generation and its rows
/// live in a successor SSTable. `Unavailable` means the header could not be
/// read, so the physical layout is UNKNOWN: callers must propagate the error
/// (fail closed) rather than guess a layout.
enum SstableMappingOutcome {
    Mapped(ColumnOrdinalMapping),
    SstableGone,
    Unavailable(ferrosa_common::Error),
}

/// Engine-wide bounded reader pool keyed by `(table_id, gen)`, shared by every
/// `TableStore` so resident reader memory is `O(reader_cap)` across all tables.
pub(crate) type SharedReaderPool<R> =
    Arc<crate::reader_pool::ReaderPool<(String, u64), SSTableReader<R>>>;

/// Test-only instrumentation that makes the large-range data-bound contract
/// observable: it tracks how many `Partition` bodies the streaming read paths
/// hold materialised *simultaneously* and records the high-water mark.
///
/// The OOM regression this guards against (see
/// `specs/proposed/p0-bounded-sstable-reader-fmea.md`) was tier
/// materialisation: `stage_sstable_tiers` collected every in-range partition of
/// each tier into a `Vec<Partition>` *up front*, so a full-range digest build
/// over a table whose SSTables span the whole range held `O(total partitions)`
/// resident at once. The streaming k-way merge instead holds only the
/// partition(s) for the *current* key in flight (`O(open sources)`), so peak
/// in-flight is bounded regardless of table size. This gauge proves the
/// difference: it is `O(total)` on the old code and `O(sources)` on the new.
#[cfg(test)]
pub(crate) mod inflight {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);

    /// Reset both counters; call at the start of a measured region.
    pub(crate) fn reset() {
        LIVE.store(0, Ordering::SeqCst);
        PEAK.store(0, Ordering::SeqCst);
    }

    /// High-water mark of simultaneously-materialised partitions since `reset`.
    pub(crate) fn peak() -> usize {
        PEAK.load(Ordering::SeqCst)
    }

    fn add(n: usize) {
        let live = LIVE.fetch_add(n, Ordering::SeqCst) + n;
        PEAK.fetch_max(live, Ordering::SeqCst);
    }

    fn sub(n: usize) {
        LIVE.fetch_sub(n, Ordering::SeqCst);
    }

    /// RAII token: `count` partitions are live for as long as it is held.
    pub(crate) struct Guard(usize);
    impl Guard {
        pub(crate) fn new(count: usize) -> Self {
            add(count);
            Guard(count)
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            sub(self.0);
        }
    }
}

/// A `Vec<Partition>` source whose entire resident length counts toward the
/// test-only in-flight gauge for its whole lifetime, draining by one as each
/// partition is yielded. This is what exposes tier materialisation: a staged
/// tier registers `O(tier_size)` live partitions the instant it is built and
/// keeps them live until fully drained, whereas a memtable source registers
/// only its (range-filtered) match count. In production builds the gauge
/// compiles away and this is a plain peekable iterator.
struct PartitionSource {
    inner: std::iter::Peekable<std::vec::IntoIter<Arc<Partition>>>,
    #[cfg(test)]
    _guard: inflight::Guard,
}

impl PartitionSource {
    fn new(partitions: Vec<Arc<Partition>>) -> Self {
        #[cfg(test)]
        let _guard = inflight::Guard::new(partitions.len());
        Self {
            inner: partitions.into_iter().peekable(),
            #[cfg(test)]
            _guard,
        }
    }

    fn peek(&mut self) -> Option<&Arc<Partition>> {
        self.inner.peek()
    }

    fn next(&mut self) -> Option<Arc<Partition>> {
        self.inner.next()
    }
}

/// Maximum number of row positions retained by partition-scoped and geo index
/// reads. Global index reads are consumed by the CQL layer, which owns the
/// inbound LIMIT/paging contract and must not turn a large, valid edge lookup
/// into an empty result.
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

pub(crate) fn search_quantized_vector_artifact_reader<R: ReadAt>(
    reader: &R,
    query: &[f32],
    k: usize,
    ef_search: usize,
) -> Result<Vec<ferrosa_index::vector::IndexResult>> {
    let total_len = reader.len()?;
    let header_len = QVEC_HNSW_MAGIC.len() as u64;
    if total_len < header_len {
        return Err(ferrosa_common::Error::InvalidData(
            "invalid quantized vector artifact header".to_string(),
        ));
    }

    let mut header = vec![0; QVEC_HNSW_MAGIC.len()];
    reader.read_exact_at(&mut header, 0)?;
    if header != QVEC_HNSW_MAGIC {
        return Err(ferrosa_common::Error::InvalidData(
            "invalid quantized vector artifact header".to_string(),
        ));
    }

    let payload_len = (total_len - header_len).try_into().map_err(|_| {
        ferrosa_common::Error::InvalidData("quantized vector artifact too large".to_string())
    })?;
    let mut payload = vec![0; payload_len];
    reader.read_exact_at(&mut payload, header_len)?;
    ferrosa_index::vector::hnsw::search_from_bytes(&payload, query, k, ef_search).map_err(|e| {
        ferrosa_common::Error::InvalidData(format!("quantized ANN search failed: {e}"))
    })
}

/// Lightweight, always-resident identity + pruning metadata for one SSTable.
///
/// This replaces the previously-resident `Arc<SSTableReader>` as the
/// `StoreView` source of truth. Holding one reader per SSTable made resident
/// memory scale with SSTable count and OOM-killed bloated nodes
/// (`specs/todo/p0-unbounded-sstable-reader-memory-oom.md`). A descriptor is
/// cheap to clone and carries no file handles, bloom filter, or index — the
/// actual reader is opened on demand through the engine-wide
/// [`crate::reader_pool::ReaderPool`] and evicted when idle.
///
/// Key/token bounds are captured from the SSTable's index footer at the moment
/// the reader exists (flush / compaction-swap / startup load) and are never
/// approximated (FMEA #2: wrong bounds silently drop matching rows on read).
#[derive(Clone, Debug)]
pub(crate) struct SstableDescriptor {
    /// Stable generation ID (used for file names, swap matching, and the pool key).
    pub gen: String,
    /// Directory containing the SSTable component files. May be empty for legacy
    /// in-memory rows, in which case the flush target's base dir is used.
    pub dir: std::path::PathBuf,
    /// Smallest decorated-key bytes in this SSTable (byte-comparable order).
    pub min_key: Vec<u8>,
    /// Largest decorated-key bytes in this SSTable (byte-comparable order).
    pub max_key: Vec<u8>,
    /// Smallest partition token covered by this SSTable.
    pub min_token: i64,
    /// Largest partition token covered by this SSTable.
    pub max_token: i64,
    /// Total component bytes captured from the verified reader.
    pub size_bytes: u64,
    /// Minimum cell timestamp from the serialization header.
    pub min_timestamp: i64,
    /// Maximum cell timestamp from the serialization header.
    pub max_timestamp: i64,
    /// Number of partitions in the SSTable.
    pub partition_count: u64,
    /// Whether either persisted key bound uses the legacy non-byte-comparable
    /// encoding and therefore needs a sorting rewrite.
    pub legacy_format: bool,
}

/// Normalise every last-write-wins timestamp `row` carries (cells, primary-key
/// liveness, row deletion) from legacy nanoseconds to microseconds, and count
/// them under `legacy_ns_timestamps_normalised_total{source="memtable_write"}`.
/// TTL and local deletion times are seconds and untouched (t_cf637b6e).
fn normalise_legacy_ns_row(row: &mut Row) {
    use ferrosa_common::{is_legacy_ns, normalize_cell_ts};
    let mut count = 0u64;
    let mut normalise = |ts: &mut i64| {
        if is_legacy_ns(*ts) {
            count += 1;
            *ts = normalize_cell_ts(*ts);
        }
    };
    normalise(&mut row.primary_key_liveness.timestamp);
    normalise(&mut row.deletion.marked_for_delete_at);
    for (_, cell) in &mut row.cells {
        normalise(&mut cell.timestamp);
    }
    if count > 0 {
        ferrosa_common::cell_ts::record_legacy_ns_normalised(
            ferrosa_common::cell_ts::LegacyNsSource::MemtableWrite,
            count,
        );
        if !LEGACY_NS_WRITE_SEEN.swap(true, std::sync::atomic::Ordering::Relaxed) {
            tracing::warn!(
                normalised = count,
                "a write carried legacy nanosecond timestamps; stored as microseconds \
                 (t_cf637b6e). Further ones are counted in \
                 legacy_ns_timestamps_normalised_total{{source=\"memtable_write\"}}, not logged"
            );
        }
    }
}

/// Set once the first legacy-stamped write of this process has been logged.
static LEGACY_NS_WRITE_SEEN: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

impl SstableDescriptor {
    /// Build a descriptor from a live reader, capturing key/token bounds from
    /// the index footer. The bounds use the same decode precedent as compaction
    /// metadata: byte-comparable decode of the smallest/largest key bytes, with
    /// a raw-key token fallback for older fixtures.
    pub(crate) fn from_reader<R: ReadAt + Send + Sync + 'static>(
        gen: String,
        dir: std::path::PathBuf,
        reader: &SSTableReader<R>,
    ) -> Self {
        use ferrosa_common::Token;
        let min_key = reader.smallest_key_bytes().to_vec();
        let max_key = reader.largest_key_bytes().to_vec();
        let decoded_min = ferrosa_sstable::byte_comparable::decode(&min_key);
        let decoded_max = ferrosa_sstable::byte_comparable::decode(&max_key);
        let legacy_format = decoded_min.is_err() || decoded_max.is_err();
        let min_token = decoded_min
            .map(|key| key.token.0)
            .unwrap_or_else(|_| Token::from_key(&min_key).0);
        let max_token = decoded_max
            .map(|key| key.token.0)
            .unwrap_or_else(|_| Token::from_key(&max_key).0);
        let header = reader.header();
        if reader.may_hold_legacy_ns_timestamps() {
            // Once per SSTable, not per cell: the cells are normalised at decode
            // and the count lands in legacy_ns_timestamps_normalised_total.
            let stored = reader.stored_header();
            tracing::warn!(
                generation = %gen,
                dir = %dir.display(),
                stored_min_timestamp = stored.min_timestamp,
                stored_max_timestamp = stored.max_timestamp,
                min_timestamp = header.min_timestamp,
                max_timestamp = header.max_timestamp,
                "SSTable holds legacy nanosecond cell timestamps; they are read as microseconds \
                 and compaction rewrites them (t_cf637b6e)"
            );
        }
        Self {
            gen,
            dir,
            min_key,
            max_key,
            min_token,
            max_token,
            size_bytes: reader.total_size(),
            min_timestamp: header.min_timestamp,
            max_timestamp: header.max_timestamp,
            partition_count: reader.key_count(),
            legacy_format,
        }
    }

    fn compaction_metadata(
        &self,
        fallback_table_dir: &std::path::Path,
    ) -> crate::compaction::metadata::SSTableMetadata {
        crate::compaction::metadata::SSTableMetadata {
            id: self.gen.clone(),
            path: if self.dir.as_os_str().is_empty() {
                fallback_table_dir.to_path_buf()
            } else {
                self.dir.clone()
            },
            size_bytes: self.size_bytes,
            min_token: self.min_token,
            max_token: self.max_token,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            partition_count: self.partition_count,
            legacy_format: self.legacy_format,
        }
    }

    fn validated_compaction_metadata(
        &self,
        fallback_table_dir: &std::path::Path,
    ) -> Option<crate::compaction::metadata::SSTableMetadata> {
        let mut metadata = self.compaction_metadata(fallback_table_dir);
        // In-memory flush targets deliberately have no component directory.
        // Their descriptor was built from an already-verified reader, so its
        // cached metadata is the only available source of truth. Production
        // descriptors always carry a directory and retain the fail-loud
        // component validation below.
        if self.dir.as_os_str().is_empty() {
            return Some(metadata);
        }
        match sstable_compaction_component_size(&metadata.path, &metadata.id) {
            Some(size_bytes) => metadata.size_bytes = size_bytes,
            None if sstable_compaction_remote_component_available(&metadata.path, &metadata.id) => {
                tracing::warn!(
                    sstable_id = %metadata.id,
                    table_dir = ?metadata.path,
                    size_bytes = metadata.size_bytes,
                    "compaction planning: SSTable local components are missing or empty; \
                     using cached verified-reader size and allowing execution to rehydrate inputs"
                );
            }
            None => {
                tracing::warn!(
                    sstable_id = %metadata.id,
                    table_dir = ?metadata.path,
                    "compaction planning: skipping SSTable because required components are missing or empty and no remote length is registered"
                );
                return None;
            }
        }
        Some(metadata)
    }

    /// Numeric generation parsed from the stable ID, used as the pool key gen.
    /// Non-numeric IDs (e.g. the `"compacted"` test fixture) hash to a stable
    /// synthetic value so they still pool correctly.
    fn gen_num(&self) -> u64 {
        Self::gen_num_for(&self.gen)
    }

    /// Compute the pool-key generation for a raw gen string. Shared by
    /// [`Self::gen_num`] and the engine startup loop so the transient
    /// startup open and the live read path key the pool identically — a
    /// mismatch would silently reopen readers or, worse, miss the cache and
    /// serve from a different key (FMEA #2/#4 keying correctness).
    pub(crate) fn gen_num_for(gen: &str) -> u64 {
        gen.parse::<u64>().unwrap_or_else(|_| {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            gen.hash(&mut h);
            // Reserve the high bit so synthetic gens never collide with real
            // numeric gens in the pool key space.
            h.finish() | (1u64 << 63)
        })
    }

    /// Can this SSTable contain a partition whose token is in `[start, end)`?
    /// Used to prune descriptors before opening their readers on range reads.
    fn overlaps_token_range(&self, start: i64, end: i64) -> bool {
        // Half-open [start, end): a descriptor covering [min_token, max_token]
        // overlaps when min_token < end && max_token >= start.
        self.min_token < end && self.max_token >= start
    }
}

/// Identity of an SSTable that a read could not consult after exhausting the
/// view-retry bound — i.e. genuinely corrupt/missing rather than a transient
/// compaction window. Carried up the read path so the caller can (a) name the
/// file in a fail-loud error and (b) quarantine it and target anti-entropy
/// repair at its covered token range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptSstableId {
    /// Stable generation ID of the corrupt SSTable (file-name / pool key).
    pub gen: String,
    /// Directory holding the SSTable's component files (empty for in-memory
    /// fixtures); paired with `gen` it locates the file for forensics/repair.
    pub dir: std::path::PathBuf,
    /// Smallest partition token the corrupt SSTable covered — the lower bound
    /// of the range anti-entropy repair must refill from a healthy replica.
    pub min_token: i64,
    /// Largest partition token the corrupt SSTable covered.
    pub max_token: i64,
}

impl CorruptSstableId {
    fn from_descriptor(desc: &SstableDescriptor) -> Self {
        Self {
            gen: desc.gen.clone(),
            dir: desc.dir.clone(),
            min_token: desc.min_token,
            max_token: desc.max_token,
        }
    }
}

impl std::fmt::Display for CorruptSstableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "gen={} dir={:?} tokens=[{},{}]",
            self.gen, self.dir, self.min_token, self.max_token
        )
    }
}

/// A memtable swapped out of the active slot, with everything a reader and
/// its flush need: the catalog and schema its rows were written under, and
/// the index postings and vector indexes readers consult until its SSTable
/// and sidecars are installed.
pub(crate) struct SealedMemtable {
    memtable: Arc<dyn Memtable>,
    /// The catalog, schema, scalar postings and (sealed) write gate the
    /// memtable was bound to while it took writes.
    bound: Arc<MemtableIndexes>,
    /// The memtable's own vector indexes.
    vector_indexes: Arc<HashMap<String, Arc<VectorMemtableIndex>>>,
    /// The scalar postings readers consult: the memtable's own at first,
    /// then, once its flush has built them, the postings for the catalog the
    /// flush writes sidecars for (an index a rotating DDL added included).
    postings: ArcSwap<HashMap<String, Arc<MemtableIndex>>>,
    /// The vector indexes readers consult, published the same way.
    vectors: ArcSwap<HashMap<String, Arc<VectorMemtableIndex>>>,
}

impl SealedMemtable {
    fn new(
        memtable: Arc<dyn Memtable>,
        bound: Arc<MemtableIndexes>,
        vector_indexes: Arc<HashMap<String, Arc<VectorMemtableIndex>>>,
    ) -> Arc<Self> {
        let postings = ArcSwap::from_pointee(bound.by_name.clone());
        let vectors = ArcSwap::new(Arc::clone(&vector_indexes));
        Arc::new(Self {
            memtable,
            bound,
            vector_indexes,
            postings,
            vectors,
        })
    }
}

/// Atomic snapshot of the storage engine's current state.
///
/// Held inside an [`ArcSwap`] so any thread can load a consistent view
/// without locking. The `Arc` fields inside ensure the data structures
/// remain alive as long as any reader holds a guard.
struct StoreView {
    /// The active memtable: accepts all current writes.
    active: Arc<dyn Memtable>,
    /// Memtables swapped out of `active` and not yet flushed, newest first.
    /// Each stays readable (rows and index postings) until the flush that
    /// writes it installs its SSTable. A flush that fails or panics leaves
    /// its memtable here; the next rotation flushes it to its own SSTable,
    /// exactly once (t_7681b32b). Overlapping SSTables are normal in the LSM;
    /// reads merge them and compaction folds them together.
    flushing: Arc<Vec<Arc<SealedMemtable>>>,
    /// Completed SSTables, newest first. Lightweight descriptors only — the
    /// readers are opened on demand through the engine-wide reader pool so
    /// resident memory is `O(reader_cap)` rather than `O(sstable_count)`.
    sstables: Arc<Vec<SstableDescriptor>>,
    /// Stable generation IDs and file directories for each SSTable, parallel to `sstables`.
    /// The String is the gen (used for file names and swap matching).
    /// The PathBuf is the directory containing the SSTable files.
    ///
    /// Retained for now to preserve the parallel-length invariant and the many
    /// existing call sites; the same `(gen, dir)` pair is also stored on each
    /// descriptor.
    sstable_ids: Arc<Vec<(String, std::path::PathBuf)>>,
    /// Per-index MemtableIndex companions for the active memtable, keyed by
    /// index name, and the index catalog they were built from. Swapped
    /// atomically alongside the active memtable during flush; the catalog
    /// published here is the table's current one.
    indexes: Arc<MemtableIndexes>,
    /// Per-SSTable sidecar index readers, parallel to `sstables`.
    /// Each entry maps index_name -> SidecarReader for that SSTable.
    sidecar_indexes: Arc<Vec<Arc<HashMap<String, SidecarReader>>>>,
    /// In-memory vector indexes for the active memtable, keyed by index name.
    /// Each holds the accumulated vectors for one vector column.
    /// Drained at flush time and used to build persistent HNSW sidecar files.
    vector_indexes: Arc<HashMap<String, Arc<VectorMemtableIndex>>>,
}

impl StoreView {
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
    /// Shared so a streamed scan can re-read it after compaction retires one
    /// of its inputs (see `RangeScan::resume_after_retired_input`).
    view: Arc<ArcSwap<StoreView>>,
    /// Engine-wide bounded reader pool, shared across all tables. Opens
    /// `SSTableReader<F::Reader>` on demand keyed by `(table, gen)`.
    reader_pool: SharedReaderPool<F::Reader>,
    /// Stable identifier for this table, used as the high-order half of the
    /// pool key so generations from different tables never collide.
    pool_table_key: String,
    /// Memtable rotations (flushes, index DDL, ALTER, TRUNCATE, retire)
    /// waiting to run; see [`TableStore::rotate`].
    rotation_tx: crossbeam_channel::Sender<RotationRequest>,
    rotation_rx: crossbeam_channel::Receiver<RotationRequest>,
    /// Set while one thread runs queued rotations for everyone (the
    /// combiner). Claimed by compare-and-swap; nothing ever waits on it.
    rotating: std::sync::atomic::AtomicBool,
    /// Memtable swaps performed, for coalescing checks and diagnostics.
    rotations_started: std::sync::atomic::AtomicU64,
    /// The indexes the most recent flush wrote a sidecar for, published for
    /// the engine's post-flush bookkeeping. Shared by pointer, so reading it
    /// copies no names: the engine only asks whether a name is in the set.
    last_flush_indexes: ArcSwap<Vec<String>>,
    /// Counter of SSTable read errors during get_partition.
    pub sstable_read_errors: std::sync::atomic::AtomicU64,
    /// Partitions a flush found changed in its frozen memtable after the
    /// snapshot. The sealed write gate makes this impossible; non-zero means
    /// admission is broken (see `TableStore::write`).
    late_writes_after_seal: std::sync::atomic::AtomicU64,
    /// Counter of reads that exhausted the store-view retry bound while an
    /// SSTable was still failing to open. Non-zero in steady state means a
    /// genuinely corrupt/missing file (not a transient compaction swap) and a
    /// read may have returned an incomplete result — alert on it.
    pub view_retry_exhausted: std::sync::atomic::AtomicU64,
    /// Wrapped in `Arc` so a scan producer can capture a cheap clone and open
    /// its SSTable readers *after* admission (t_6d0553ee): `F` itself is neither
    /// `Clone` nor `'static`-movable, but `Arc<F>` is.
    pub(crate) flush_target: Arc<F>,
    options: WriteOptions,
    /// Set once the table is dropped (see [`TableStore::retire`]): no flush
    /// of this store writes anything after it is set.
    retired: std::sync::atomic::AtomicBool,
    /// Monotonic generation counter for stable SSTable IDs.
    /// Incremented on each flush. Used by compaction swap to identify
    /// exactly which SSTables to remove.
    next_gen: std::sync::atomic::AtomicU64,
    /// Partition-key scopes observed per vector index, accumulated at flush.
    ///
    /// Persisted vector sidecars store only a placeholder `RowPosition::offset`
    /// and do **not** record the partition key, so a flushed global-sidecar
    /// `IndexResult` cannot be mapped back to a base-table row on its own. The
    /// scoped per-prefix sidecars written at flush time *are* keyed by scope,
    /// so remembering which scopes were flushed lets the index-consult path in
    /// [`ann_search_partitions`] enumerate those scoped sidecars and recover the
    /// actual partition keys without a full table scan.
    vector_index_scopes: ArcSwap<VectorIndexScopes>,
    /// SSTable generations the read path has quarantined because a read
    /// exhausted the view-retry bound against them (genuine corruption, not a
    /// transient compaction window). Subsequent reads SKIP these generations so
    /// they neither re-fail nor re-incur the full retry storm, and anti-entropy
    /// repair targets their covered token range to refill from a healthy
    /// replica. Keyed by `gen`; the covered range is recovered from the live
    /// descriptor (still in the view until repair swaps it out).
    quarantined_sstables: crate::lockfree::SharedSet,
    /// Generations whose secondary-index sidecars could not all be opened.
    /// Their data is intact and still read; only index consults refuse them.
    index_unavailable_sstables: crate::lockfree::SharedSet,
    /// Generations whose object recently failed to open: they fail fast for a
    /// short TTL instead of costing a reopen per retry per read.
    missing_sstables: MissingSstableCache,
    /// `gen/index` keys of the FTI sidecars being built for this table, so a
    /// burst of concurrent queries that all find the same SSTable uncovered
    /// builds its sidecar once instead of tokenizing it once per query at the
    /// same time. A query that finds a build in flight does not wait for it.
    fulltext_sidecars_in_flight: Arc<crate::lockfree::SharedSet>,
    /// `gen/index` keys of the generations whose scoped vector sidecars are
    /// complete (a manifest on disk whose sidecars add up). ANN over a live
    /// generation that is not in this set refuses with a retryable error
    /// rather than answer without its rows; the vector repair fills it in.
    vector_ready: crate::lockfree::SharedSet,
    /// `gen/index` keys of the vector sidecar builds running now:
    /// single-flight per generation and index, with no lock to wait on.
    vector_sidecars_in_flight: Arc<crate::lockfree::SharedSet>,
    /// `gen/index` keys of complete generations whose sidecars have been
    /// decoded and checked against their manifest and the index declaration
    /// (see [`Self::verify_vector_index`]). Generations are immutable, so
    /// each is checked once.
    vector_verified: crate::lockfree::SharedSet,
    /// Why each incomplete `gen/index` was found invalid, for the repair's
    /// `reason` label. A generation with no entry was simply missing.
    vector_invalid_reasons: ArcSwap<HashMap<String, VectorInvalidReason>>,
    /// The declared dimension of each vector index, from its column type.
    vector_dimensions: ArcSwap<HashMap<String, usize>>,
}

/// The index declarations of one table, as one immutable value.
///
/// A [`TableStore`] publishes its catalog through an `ArcSwap`: the write
/// and read paths `load()` a snapshot and never block, and DDL clones the
/// current catalog, edits the clone and stores it. These fields used to be
/// plain members mutated through `&mut TableStore`, which only worked
/// because the engine-wide table map was a `RwLock` whose write guard made
/// DDL exclusive. That lock is gone (t_d938e6ae): a reader parked holding it
/// deadlocked a node.
#[derive(Clone, Debug, Default)]
pub(crate) struct IndexCatalog {
    /// Secondary index declarations: `(index_name, column_position)` pairs.
    /// Column position is the index into `Row.cells` by column ordinal
    /// (matching the `u16` tag in each cell tuple).
    indexed_columns: Vec<(String, usize)>,
    /// Secondary indexes on CLUSTERING columns: `(index_name,
    /// clustering_component)` pairs (t_430c4188). A clustering column's value
    /// is not a cell — the write path extracts it from the row's composite
    /// clustering-key bytes at the given component index.
    indexed_clustering_columns: Vec<(String, usize)>,
    /// Partition-key secondary index declarations:
    /// `(index_name, partition_key_component)`.
    ///
    /// A partition-key value is encoded in the key, not stored as a cell, so
    /// the cell-based `indexed_columns` path cannot see it — the same reason
    /// `indexed_clustering_columns` exists for the other half of the primary
    /// key.
    indexed_partition_key_columns: Vec<(String, usize)>,
    /// Per-index type, keyed by index name. Threaded from the schema so eager /
    /// backfill / compaction index-build jobs carry the correct `IndexType`
    /// instead of a hardcoded `BTree`. Missing entries default to `BTree`.
    index_types: HashMap<String, IndexType>,
    /// Partial-index predicates, keyed by index name. Present only for
    /// [`IndexType::Filtered`] indexes. The memtable write path consults this so
    /// a live write is added to the filtered memtable index ONLY when its
    /// filter-column cell satisfies the predicate — matching exactly the rows
    /// the SSTable sidecar build keeps, so memtable and sidecar agree.
    index_filter_predicates: HashMap<String, FilterPredicate>,
    /// Full-text index declarations: `(index_name, column_position)` pairs.
    /// Built as FTI sidecar files during flush.
    fulltext_indexes: Vec<(String, usize)>,
    /// Vector index configurations. At flush time each declared vector index
    /// is drained from the memtable and persisted as a method-specific
    /// vector artifact.
    vector_index_configs: Vec<VectorIndexConfig>,
    /// Per-index persistent artifact/search method. Missing entries default to
    /// the legacy HNSW sidecar for API compatibility with existing callers.
    vector_index_methods: HashMap<String, VectorIndexMethod>,
}

impl IndexCatalog {
    /// A catalog declaring only the given cell-column indexes, each BTree.
    fn with_indexed_columns(indexed_columns: Vec<(String, usize)>) -> Self {
        Self {
            index_types: default_index_types(&indexed_columns),
            indexed_columns,
            ..Self::default()
        }
    }

    /// The declared type of `index_name`; BTree when undeclared.
    fn index_type_for(&self, index_name: &str) -> IndexType {
        self.index_types
            .get(index_name)
            .copied()
            .unwrap_or(IndexType::BTree)
    }

    /// The artifact/search method of vector index `index_name`; HNSW when
    /// undeclared or registered through the legacy path.
    fn vector_index_method(&self, index_name: &str) -> VectorIndexMethod {
        self.vector_index_methods
            .get(index_name)
            .copied()
            .unwrap_or(VectorIndexMethod::Hnsw)
    }

    /// This catalog with every column ordinal remapped from `old_schema`'s
    /// regular-column layout onto `new_schema`'s.
    ///
    /// Regular columns are ordered by Cassandra's column-name comparator, so
    /// `ALTER TABLE ADD` of a column that sorts before an indexed column
    /// shifts the indexed column's ordinal (the `u16` cell tag). Index
    /// declarations store ordinals, not names — left stale, every subsequent
    /// write extracts the indexed value from the WRONG cell, and a
    /// backfilled-Current index then serves false empty results (memory-suite
    /// regression `fixed_phonetic_match`). So every positional declaration is
    /// remapped through the old schema's column name.
    fn remapped_onto(&self, old_schema: &TableSchema, new_schema: &TableSchema) -> Self {
        let mut catalog = self.clone();
        let remap = |index_name: &str, pos: &mut usize| {
            let Some(old_col) = old_schema.regular_columns.get(*pos) else {
                tracing::error!(
                    index = index_name,
                    position = *pos,
                    "index ordinal points past the pre-ALTER regular column \
                     set — leaving it unchanged; index writes may be wrong"
                );
                return;
            };
            match new_schema
                .regular_columns
                .iter()
                .position(|c| c.name == old_col.name)
            {
                Some(new_pos) => {
                    if new_pos != *pos {
                        tracing::info!(
                            index = index_name,
                            column = %old_col.name,
                            old_position = *pos,
                            new_position = new_pos,
                            "remapped index column ordinal after schema update"
                        );
                        *pos = new_pos;
                    }
                }
                None => tracing::error!(
                    index = index_name,
                    column = %old_col.name,
                    "indexed column absent from post-ALTER schema — leaving \
                     ordinal unchanged; index writes may be wrong"
                ),
            }
        };
        for (name, pos) in &mut catalog.indexed_columns {
            remap(name, pos);
        }
        for (name, pos) in &mut catalog.fulltext_indexes {
            remap(name, pos);
        }
        for cfg in &mut catalog.vector_index_configs {
            remap(&cfg.index_name.clone(), &mut cfg.column_position);
        }
        for (name, pred) in &mut catalog.index_filter_predicates {
            for clause in &mut pred.clauses {
                remap(name, &mut clause.column_position);
            }
        }
        catalog
    }

    /// Whether this catalog declares any index at all.
    fn declares_any(&self) -> bool {
        !(self.indexed_columns.is_empty()
            && self.indexed_clustering_columns.is_empty()
            && self.indexed_partition_key_columns.is_empty()
            && self.fulltext_indexes.is_empty()
            && self.vector_index_configs.is_empty())
    }

    /// Every scalar (memtable-indexed) index this catalog declares: cell,
    /// clustering and partition-key indexes, by name.
    fn scalar_index_names(&self) -> impl Iterator<Item = &String> {
        self.indexed_columns
            .iter()
            .chain(self.indexed_clustering_columns.iter())
            .chain(self.indexed_partition_key_columns.iter())
            .map(|(name, _)| name)
    }

    /// Drop every declaration named `index_name`; whether there was one.
    fn remove(&mut self, index_name: &str) -> bool {
        let before = (
            self.indexed_columns.len(),
            self.indexed_clustering_columns.len(),
            self.indexed_partition_key_columns.len(),
            self.vector_index_configs.len(),
        );
        self.indexed_columns.retain(|(name, _)| name != index_name);
        self.indexed_clustering_columns
            .retain(|(name, _)| name != index_name);
        self.indexed_partition_key_columns
            .retain(|(name, _)| name != index_name);
        // Full-text declarations are NOT dropped here: `remove_index` never
        // has, and DROP INDEX of a full-text index is out of this change.
        self.vector_index_configs
            .retain(|cfg| cfg.index_name != index_name);
        let after = (
            self.indexed_columns.len(),
            self.indexed_clustering_columns.len(),
            self.indexed_partition_key_columns.len(),
            self.vector_index_configs.len(),
        );
        let mut removed = before != after;
        removed |= self.index_types.remove(index_name).is_some();
        removed |= self.index_filter_predicates.remove(index_name).is_some();
        removed |= self.vector_index_methods.remove(index_name).is_some();
        removed
    }

    /// Everything that determines the postings scalar index `index_name`
    /// holds: which key family it reads, the position there, its type and its
    /// partial-index predicate. Two catalogs agreeing on this give the same
    /// postings for the same rows.
    fn scalar_definition(
        &self,
        index_name: &str,
    ) -> Option<(u8, usize, IndexType, Option<&FilterPredicate>)> {
        let find = |list: &[(String, usize)]| {
            list.iter()
                .find(|(name, _)| name == index_name)
                .map(|(_, pos)| *pos)
        };
        let (family, position) = find(&self.indexed_columns)
            .map(|pos| (0, pos))
            .or_else(|| find(&self.indexed_clustering_columns).map(|pos| (1, pos)))
            .or_else(|| find(&self.indexed_partition_key_columns).map(|pos| (2, pos)))?;
        Some((
            family,
            position,
            self.index_type_for(index_name),
            self.index_filter_predicates.get(index_name),
        ))
    }

    /// Whether this catalog declares an index named `index_name`, of any kind.
    fn declares(&self, index_name: &str) -> bool {
        let named = |list: &[(String, usize)]| list.iter().any(|(n, _)| n == index_name);
        named(&self.indexed_columns)
            || named(&self.indexed_clustering_columns)
            || named(&self.indexed_partition_key_columns)
            || named(&self.fulltext_indexes)
            || self
                .vector_index_configs
                .iter()
                .any(|cfg| cfg.index_name == index_name)
    }
}

/// A read-only list out of one `IndexCatalog` snapshot.
///
/// Dereferences to a slice, so callers iterate it like the `&[T]` the
/// accessors used to return; it keeps its snapshot alive, so a DDL that
/// swaps the catalog meanwhile cannot change what the holder sees.
pub struct CatalogSlice<T: 'static> {
    catalog: Arc<IndexCatalog>,
    project: fn(&IndexCatalog) -> &[T],
}

impl<T> std::ops::Deref for CatalogSlice<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        (self.project)(&self.catalog)
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for CatalogSlice<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// File name of generation `gen`'s FTI sidecar for `index_name`.
/// Append a term index to the legacy FTI sidecar at `path`, atomically.
/// `Ok(false)`: it already had one.
fn upgrade_fti_sidecar(path: &std::path::Path, gen: &str, index_name: &str) -> Result<bool> {
    use std::io::Write;

    let Some(tail) = ferrosa_index::fulltext::stream::term_index_tail_for_path(path)
        .map_err(|e| ferrosa_common::Error::InvalidFormat(format!("FTI term index: {e}")))?
    else {
        return Ok(false);
    };
    let dir = path.parent().ok_or_else(|| {
        ferrosa_common::Error::InvalidFormat(format!(
            "FTI sidecar {} has no directory",
            path.display()
        ))
    })?;
    // `.tmp`, not `{gen}-` prefixed: startup cleanup removes one a crash left,
    // and nothing enumerating a generation's components picks it up.
    let tmp = dir.join(format!(
        ".fti-{gen}-{index_name}.{}.upgrade.tmp",
        std::process::id()
    ));
    let written = std::fs::copy(path, &tmp).and_then(|_| {
        let mut file = std::fs::OpenOptions::new().append(true).open(&tmp)?;
        file.write_all(&tail)?;
        file.sync_all()
    });
    if let Err(e) = written.and_then(|()| std::fs::rename(&tmp, path)) {
        if let Err(cleanup) = std::fs::remove_file(&tmp) {
            if cleanup.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %tmp.display(), %cleanup, "fts: could not remove temp FTI sidecar");
            }
        }
        return Err(e.into());
    }
    Ok(true)
}

pub(crate) fn fti_sidecar_file_name(gen: &str, index_name: &str) -> String {
    format!("{gen}-FTI-{index_name}.db")
}

/// Build the full-text index of one SSTable's `column_position` column: one
/// document per row, keyed by the row's full primary key (t_da51e20c).
///
/// Errors on any open, iterator or decode failure rather than returning the
/// rows read so far: a partial index persisted as a sidecar would hide the
/// remaining rows from every later query.
fn build_sstable_fti<R: ReadAt>(
    sstable: &SSTableReader<R>,
    schema: &TableSchema,
    column_position: usize,
) -> Result<ferrosa_index::fulltext::builder::FullTextIndex> {
    let mapping = ColumnOrdinalMapping::for_header(schema, sstable.header());
    let mut iter = sstable.partitions_iter()?;
    let mut builder = ferrosa_index::fulltext::builder::FullTextIndexBuilder::new();
    while let Some(mut partition) = iter.next_partition()? {
        mapping.remap_partition(&mut partition);
        let pk_bytes = partition.key.key.as_bytes();
        for row in &partition.rows {
            let mut text = String::new();
            for (col_idx, cell) in &row.cells {
                if *col_idx as usize != column_position {
                    continue;
                }
                if let Some(s) = cell
                    .value
                    .as_deref()
                    .and_then(|v| std::str::from_utf8(v).ok())
                {
                    text.push_str(s);
                    text.push(' ');
                }
            }
            if !text.is_empty() {
                let doc_key =
                    ferrosa_index::fulltext::keys::encode_doc_key(pk_bytes, &row.clustering);
                builder.add_document(doc_key, text.trim());
            }
        }
    }
    Ok(builder.build())
}

/// One FTI sidecar to build: generation `gen`'s `index_name` sidecar, written
/// beside the SSTable's component files in `dir`.
struct FulltextSidecarJob<R: ReadAt> {
    gen: String,
    dir: std::path::PathBuf,
    index_name: String,
    column_position: usize,
    /// The same pooled reader the read path uses, so the build opens the
    /// SSTable exactly as a query would.
    reader: Arc<SSTableReader<R>>,
}

/// What a [`FulltextSidecarBuild`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FulltextSidecarOutcome {
    /// Sidecars written.
    pub built: usize,
    /// Sidecars a concurrent build had already written by the time this one
    /// reached them.
    pub already_present: usize,
    /// Sidecars another build was writing when this one reached them. This
    /// build did not wait for it: the query that ran it scans those SSTables
    /// in full this once instead.
    pub in_flight_elsewhere: usize,
    /// Sidecars that could not be built or written. Each is logged at ERROR;
    /// queries keep scanning that SSTable in full until one is built.
    pub failed: usize,
}

/// Why a generation's vector sidecars for an index are not trusted, which is
/// what sends it to the repair. Each has a detector in
/// [`TableStore::verify_vector_index`] (or registration, for `Missing`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VectorInvalidReason {
    /// (a) No manifest, or scoped sidecars that do not add up to it: never
    /// built, compacted before compaction wrote vector sidecars, or a build
    /// that crashed.
    Missing,
    /// (b) A sidecar or the manifest that cannot be read or decoded.
    Corrupt,
    /// (c) The sidecars hold a different number of vectors or scopes than
    /// the manifest built from the generation's rows recorded.
    CountMismatch,
    /// (d) A sidecar's vectors differ in dimension from the index's column.
    DimensionMismatch,
    /// (e) A scope with a sidecar on disk was missing from the in-memory
    /// scope set, so ANN never probed it.
    ScopeSet,
}

impl VectorInvalidReason {
    /// The `reason` label on `ferrosa_index_repairs_total`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing_sidecar",
            Self::Corrupt => "corrupt_sidecar",
            Self::CountMismatch => "count_mismatch",
            Self::DimensionMismatch => "dimension_mismatch",
            Self::ScopeSet => "scope_set",
        }
    }
}

/// What one [`TableStore::verify_vector_index`] pass found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorVerifyOutcome {
    /// Complete generations decoded and checked this pass.
    pub verified: usize,
    /// Generations found invalid this pass, now incomplete and queued for
    /// the repair.
    pub invalidated: Vec<(String, VectorInvalidReason)>,
    /// Scopes on disk that were missing from the scope set and were put back.
    pub scopes_restored: usize,
    /// Live generations incomplete after this pass (ANN refuses while > 0).
    pub pending: usize,
}

/// What a vector sidecar repair or compaction-output build did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorRepairOutcome {
    /// Generations whose scoped sidecars were built and marked complete.
    pub repaired: usize,
    /// Vectors indexed by those builds.
    pub vectors: u64,
    /// Generations another build was working on; not waited for.
    pub in_flight_elsewhere: usize,
    /// Generations that could not be built. Each is logged at ERROR, and ANN
    /// over the index refuses until a later repair succeeds.
    pub failed: usize,
}

/// FTI sidecars planned from one view of a table and built outside it.
///
/// Holds only shared handles (flush target, schema snapshot, readers, the
/// table's set of sidecars being built), so tokenizing a large SSTable holds
/// nothing any read, write or DDL statement waits on.
#[must_use = "a planned sidecar build does nothing until it is run"]
pub struct FulltextSidecarBuild<F: FlushTarget> {
    flush_target: Arc<F>,
    schema: Arc<TableSchema>,
    /// `gen/index` keys of the sidecars being built right now, by any build
    /// of this table: single-flight per sidecar, with no lock to wait on.
    in_flight: Arc<crate::lockfree::SharedSet>,
    jobs: Vec<FulltextSidecarJob<F::Reader>>,
}

/// One sidecar build's claim on its `gen/index` key in the table's in-flight
/// set; dropping it releases the claim, whether the build succeeded, failed
/// or panicked.
struct SidecarClaim<'a> {
    in_flight: &'a crate::lockfree::SharedSet,
    key: String,
}

impl<'a> SidecarClaim<'a> {
    /// Claim `key`, or `None` if another build holds it. A claim that cannot
    /// be recorded is refused too, loudly: building without it could
    /// tokenize the same SSTable twice at once.
    fn take(in_flight: &'a crate::lockfree::SharedSet, key: String) -> Option<Self> {
        match in_flight.insert(&key) {
            Ok(true) => Some(Self { in_flight, key }),
            Ok(false) => None,
            Err(e) => {
                tracing::error!(%e, sidecar = %key, "fts: could not claim a sidecar build; skipped");
                None
            }
        }
    }
}

impl Drop for SidecarClaim<'_> {
    fn drop(&mut self) {
        match self.in_flight.remove(&self.key) {
            Ok(true) => {}
            Ok(false) => tracing::error!(
                sidecar = %self.key,
                "fts: a sidecar build claim was already released; the in-flight set is inconsistent"
            ),
            // Left claimed, this sidecar is never built again by this
            // process; every query keeps scanning its SSTable in full.
            Err(e) => tracing::error!(
                %e,
                sidecar = %self.key,
                "fts: a sidecar build claim could not be released; this sidecar will not be built \
                 again until restart"
            ),
        }
    }
}

impl<F: FlushTarget> FulltextSidecarBuild<F> {
    /// Whether there is anything to build.
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// Build and persist every planned sidecar, one at a time.
    pub fn run(self) -> FulltextSidecarOutcome {
        let mut outcome = FulltextSidecarOutcome::default();
        if self.jobs.is_empty() {
            return outcome;
        }
        for job in &self.jobs {
            let Some(_claim) =
                SidecarClaim::take(&self.in_flight, format!("{}/{}", job.gen, job.index_name))
            else {
                outcome.in_flight_elsewhere += 1;
                continue;
            };
            // Checked under the claim: a build that finished between this
            // plan and the claim has left its file.
            if job
                .dir
                .join(fti_sidecar_file_name(&job.gen, &job.index_name))
                .is_file()
            {
                outcome.already_present += 1;
                continue;
            }
            let start = Instant::now();
            match self.build_one(job) {
                Ok(doc_count) => {
                    outcome.built += 1;
                    tracing::info!(
                        gen = %job.gen,
                        index = %job.index_name,
                        doc_count,
                        elapsed_ms = start.elapsed().as_millis() as u64,
                        "fts: built FTI sidecar for a live SSTable that had none; \
                         queries no longer tokenize it in full"
                    );
                }
                Err(e) => {
                    outcome.failed += 1;
                    tracing::error!(
                        %e,
                        gen = %job.gen,
                        index = %job.index_name,
                        "fts: FTI sidecar build failed; every query keeps tokenizing \
                         this SSTable in full until one is built"
                    );
                }
            }
        }
        outcome
    }

    fn build_one(&self, job: &FulltextSidecarJob<F::Reader>) -> Result<u32> {
        #[cfg(test)]
        crate::engine::FTS_SSTABLE_FULL_SCANS.with(|c| c.set(c.get() + 1));
        let fti = build_sstable_fti(&job.reader, &self.schema, job.column_position)?;
        let doc_count = fti.doc_count;
        let bytes = ferrosa_index::fulltext::builder::serialize_fti(&fti).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("serialize FTI sidecar: {e}"))
        })?;
        drop(fti);
        self.flush_target
            .write_fti_sidecar_in(&job.dir, &job.gen, &job.index_name, &bytes)?;
        Ok(doc_count)
    }
}

/// Under Miri the sharded BTree memtable is used whatever the features say:
/// crossbeam-skiplist 0.1.3 / crossbeam-epoch 0.9.18 trip Miri's aliasing
/// models inside their own code (upstream, crossbeam#545), which would stop
/// Miri before it reaches ferrosa's own unsafe (`OwnedMerger`). Production
/// builds never set `miri`.
fn new_memtable() -> Arc<dyn Memtable> {
    #[cfg(all(feature = "skiplist-memtable", not(miri)))]
    {
        Arc::new(SkipListMemtable::new())
    }
    #[cfg(any(not(feature = "skiplist-memtable"), miri))]
    {
        Arc::new(ShardedBTreeMemtable::with_default_shards())
    }
}

/// Partition-key scopes flushed per vector index (see
/// `TableStore::vector_index_scopes`). Each index's set is its own `Arc`, so
/// recording scopes for one index copies only that index's set.
type VectorIndexScopes = HashMap<String, Arc<std::collections::HashSet<Vec<u8>>>>;

/// An index DDL's change to a catalog: the next catalog, or `None` for none.
type CatalogEdit = Box<dyn FnOnce(&IndexCatalog) -> Option<IndexCatalog> + Send>;

/// What [`TableStore::swap_compacted_sstables`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "an output that was not swapped in must be deleted by the caller"]
pub enum CompactionSwap {
    /// The output replaced its inputs in the view.
    Swapped,
    /// At least one input had already left the view (a TRUNCATE, or another
    /// compaction): installing the output would bring back rows the view no
    /// longer holds, so nothing changed and the output must be discarded.
    InputsGone,
}

/// What one queued memtable rotation asks for (see `TableStore::rotate`).
enum RotationKind {
    /// Flush the active memtable; `on_release` runs right after the swap.
    Flush {
        on_release: Box<dyn FnOnce() + Send>,
    },
    /// Rotate onto a catalog derived from the current one (index DDL);
    /// `None` from the edit leaves the catalog as it is.
    Edit(CatalogEdit),
    /// Rotate onto a new schema (`ALTER TABLE`).
    Schema {
        schema: TableSchema,
        on_release: Box<dyn FnOnce() + Send>,
    },
    /// Discard every row and SSTable (`TRUNCATE`).
    Truncate,
    /// Change nothing; answered once every rotation queued before it is done.
    Barrier,
}

/// A catalog or schema change one rotation applies, in queue order.
enum RotationChange {
    Edit(CatalogEdit),
    Schema(TableSchema),
}

/// The catalog and schema a rotation's new memtable is bound to: `changes`
/// applied in queue order to the frozen memtable's. A schema change remaps
/// the catalog's column ordinals onto the new layout.
fn apply_rotation_changes(
    changes: Vec<RotationChange>,
    catalog: &Arc<IndexCatalog>,
    schema: &Arc<TableSchema>,
) -> (Arc<IndexCatalog>, Arc<TableSchema>) {
    changes.into_iter().fold(
        (Arc::clone(catalog), Arc::clone(schema)),
        |(catalog, schema), change| match change {
            RotationChange::Edit(edit) => match edit(&catalog) {
                Some(edited) => (Arc::new(edited), schema),
                None => (catalog, schema),
            },
            RotationChange::Schema(next) => {
                let remapped = catalog.remapped_onto(&schema, &next);
                (Arc::new(remapped), Arc::new(next))
            }
        },
    )
}

/// One queued rotation and where to send its result.
struct RotationRequest {
    kind: RotationKind,
    enqueued_at: Instant,
    reply: crossbeam_channel::Sender<Result<FlushOutcome>>,
}

/// How often a thread waiting for its rotation re-checks whether it should
/// run the queue itself (the combiner may have stopped between rounds).
const ROTATION_POLL: std::time::Duration = std::time::Duration::from_millis(10);

/// How long a thread waits for its rotation before failing loud. A rotation
/// is a flush, so this is far beyond any healthy one.
const ROTATION_WAIT_LIMIT: std::time::Duration = std::time::Duration::from_secs(3600);

/// How many queue drains one combiner runs before handing the queue back,
/// so a busy table cannot keep one caller combining for everyone forever.
const MAX_COMBINE_ROUNDS: usize = 8;

/// Clears `TableStore::rotating` when the combiner stops, panics included.
struct CombinerSlot<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for CombinerSlot<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// How many sealed memtables one write may meet before it is refused. A
/// flush publishes the next memtable before sealing the old one, so a retry
/// normally finds an open gate at once; this many in a row means rotations
/// are not completing.
const MAX_SEALED_MEMTABLE_RETRIES: usize = 10_000;

/// How long a flush waits for the writers inside a memtable it sealed. Those
/// writes are in-memory puts; still inside after this, one is stuck and the
/// flush fails loud rather than snapshot an incomplete memtable.
const MEMTABLE_SEAL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// The memtable indexes of ONE active memtable, together with the catalog
/// they were built from.
///
/// The pair is created when its memtable is, and replaced only when the
/// memtable is (a flush swap), so the catalog a write posts under is always
/// the catalog its memtable's flush writes sidecars for: index postings equal
/// the flushed sidecar set by construction. Index DDL never edits a live
/// memtable's indexes; it rotates the memtable (see
/// [`TableStore::flush_applying_catalog_edit`]). Dereferences to the
/// name→index map, which is what reads use.
pub(crate) struct MemtableIndexes {
    catalog: Arc<IndexCatalog>,
    /// The schema the memtable's rows are put under: the one current when
    /// the memtable was created, so an `ALTER` publishing a new schema at a
    /// rotation never reaches a write still inside the old memtable.
    schema: Arc<TableSchema>,
    by_name: HashMap<String, Arc<MemtableIndex>>,
    /// Admission to the memtable. The flush that freezes it seals the gate
    /// and waits for the writers inside, so its snapshot holds every row
    /// written to it and no later write lands in it (see `TableStore::write`).
    gate: crate::lockfree::WriteGate,
}

impl std::ops::Deref for MemtableIndexes {
    type Target = HashMap<String, Arc<MemtableIndex>>;

    fn deref(&self) -> &Self::Target {
        &self.by_name
    }
}

/// Post every row of `partitions` to `index_name`, as the write path would
/// have had the index existed when they were written. Used only for a frozen
/// memtable — immutable rows no write can reach — whose rotating DDL added
/// the index (see `TableStore::flush_sidecar_indexes`).
fn scalar_index_from_rows(
    catalog: &IndexCatalog,
    index_name: &str,
    partitions: &[Partition],
    key_component_counts: (usize, usize),
) -> Arc<MemtableIndex> {
    let (pk_total, ck_total) = key_component_counts;
    let index = Arc::new(MemtableIndex::new());
    let index_type = catalog.index_type_for(index_name);
    let find = |list: &[(String, usize)]| {
        list.iter()
            .find(|(name, _)| name == index_name)
            .map(|(_, pos)| *pos)
    };
    let post_key_component =
        |value: &[u8], row_pos: RowPosition| match crate::index::scheduler::encode_index_key(
            index_type, value,
        ) {
            Ok(Some(index_key)) => index.insert(index_key, row_pos),
            Ok(None) => {}
            Err(e) => tracing::warn!(
                index_name,
                %e,
                "store: skipping key-component index entry built at flush; key encoding failed"
            ),
        };
    for partition in partitions {
        let pk_bytes = partition.key.key.as_bytes();
        if let Some(position) = find(&catalog.indexed_columns) {
            let predicate = catalog.index_filter_predicates.get(index_name);
            for row in partition.static_row.iter().chain(partition.rows.iter()) {
                insert_scalar_memtable_index_row(
                    &index, index_name, position, index_type, predicate, pk_bytes, row,
                );
            }
        } else if let Some(component) = find(&catalog.indexed_partition_key_columns) {
            let components = ferrosa_row_bridge::decode_pk(&partition.key, pk_total);
            if let Some(value) = components.get(component) {
                post_key_component(
                    value,
                    RowPosition {
                        partition_key: pk_bytes.to_vec(),
                        clustering_key: Vec::new(),
                    },
                );
            }
        } else if let Some(component) = find(&catalog.indexed_clustering_columns) {
            for row in partition
                .rows
                .iter()
                .filter(|row| !row.clustering.is_empty())
            {
                let components = ferrosa_row_bridge::decode_clustering(&row.clustering, ck_total);
                if let Some(value) = components.get(component) {
                    post_key_component(
                        value,
                        RowPosition {
                            partition_key: pk_bytes.to_vec(),
                            clustering_key: row.clustering.clone(),
                        },
                    );
                }
            }
        }
    }
    index
}

/// Build vector index `cfg` over the rows of `partitions` (see
/// [`scalar_index_from_rows`] for when).
fn vector_index_from_rows(
    cfg: &VectorIndexConfig,
    partitions: &[Partition],
) -> Arc<VectorMemtableIndex> {
    let vector_index = Arc::new(VectorMemtableIndex::new(
        cfg.metric,
        cfg.m,
        cfg.ef_construction,
    ));
    for partition in partitions {
        for row in partition.static_row.iter().chain(partition.rows.iter()) {
            let Some(value) = row
                .cells
                .iter()
                .find(|(idx, _)| *idx as usize == cfg.column_position)
                .and_then(|(_, cell)| cell.value.as_ref())
            else {
                continue;
            };
            let Ok(vector) = ferrosa_index::bytes_to_vec_f32(value) else {
                continue;
            };
            let position = ferrosa_index::vector::RowPosition::new(vector_index.len() as u64);
            vector_index.insert_with_scope(
                position,
                vector,
                Some(partition.key.key.as_bytes().to_vec()),
            );
        }
    }
    vector_index
}

/// Build a fresh empty `MemtableIndex` per secondary index `catalog`
/// declares — cell, clustering (t_430c4188) and partition-key indexes alike.
fn new_indexes(catalog: Arc<IndexCatalog>, schema: Arc<TableSchema>) -> Arc<MemtableIndexes> {
    // EVERY declared index must appear here. A flush installs this map
    // wholesale, so a family of indexes omitted from it exists in the
    // declaration and nowhere else: `guard.indexes.get(name)` then returns
    // None for the rest of the table's life and each write silently skips it.
    let by_name: HashMap<String, Arc<MemtableIndex>> = catalog
        .indexed_columns
        .iter()
        .chain(catalog.indexed_clustering_columns.iter())
        .chain(catalog.indexed_partition_key_columns.iter())
        .map(|(name, _)| (name.clone(), Arc::new(MemtableIndex::new())))
        .collect();
    Arc::new(MemtableIndexes {
        catalog,
        schema,
        by_name,
        gate: crate::lockfree::WriteGate::default(),
    })
}

/// Default per-index types for a freshly-declared set of secondary indexes.
/// Defaults every declaration to `BTree`; the true type is supplied later via
/// [`TableStore::add_index`] (or by reload from persisted schema metadata).
fn default_index_types(indexed_columns: &[(String, usize)]) -> HashMap<String, IndexType> {
    indexed_columns
        .iter()
        .map(|(name, _)| (name.clone(), IndexType::BTree))
        .collect()
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

fn hex_scope(scope: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(scope.len() * 2);
    for byte in scope {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn scoped_vector_sidecar_name(index_name: &str, scope: &[u8]) -> String {
    format!("{index_name}__scope_{}", hex_scope(scope))
}

/// The scope of `sidecar_name` if it is one of `index_name`'s scoped
/// sidecars (the inverse of [`scoped_vector_sidecar_name`]); `None` if it
/// belongs to another index or is the global sidecar.
///
/// # Errors
///
/// The name carries `index_name`'s scope prefix but not a hex scope. That
/// sidecar's scope cannot be recovered, and skipping it would silently drop
/// its rows from ANN.
fn scope_of_vector_sidecar(index_name: &str, sidecar_name: &str) -> Option<Result<Vec<u8>>> {
    let hex = sidecar_name
        .strip_prefix(index_name)?
        .strip_prefix("__scope_")?;
    let malformed = || {
        ferrosa_common::Error::InvalidData(format!(
            "vector sidecar {sidecar_name} of index {index_name} has a malformed scope"
        ))
    };
    // Exactly what `hex_scope` writes, so the scope maps back to this file.
    let lower_hex = |b: u8| matches!(b, b'0'..=b'9' | b'a'..=b'f');
    if hex.len() % 2 != 0 || !hex.bytes().all(lower_hex) {
        return Some(Err(malformed()));
    }
    let scope = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()
        .ok_or_else(malformed);
    Some(scope)
}

/// Sidecar name of the manifest that marks `index_name`'s scoped sidecars
/// of a generation complete. It is a `-VEC-` file, so it lives and dies with
/// the generation's other vector sidecars.
fn vector_manifest_name(index_name: &str) -> String {
    format!("{index_name}__manifest")
}

/// The record that a generation's scoped vector sidecars for one index are
/// complete. Written last, after every scoped sidecar: a generation with no
/// manifest, or whose scoped sidecars on disk do not add up to `bytes`, is
/// rebuilt from its rows before ANN will answer over it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorSidecarManifest {
    /// Vectors indexed across the generation's scoped sidecars.
    pub vectors: u64,
    /// Scoped sidecars written, one per partition with a vector.
    pub scopes: u64,
    /// Total bytes of those scoped sidecars.
    pub bytes: u64,
}

impl VectorSidecarManifest {
    const MAGIC: &'static [u8; 4] = b"FVM1";
    const ENCODED_LEN: usize = 4 + 3 * 8;

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::ENCODED_LEN);
        out.extend_from_slice(Self::MAGIC);
        for field in [self.vectors, self.scopes, self.bytes] {
            out.extend_from_slice(&field.to_le_bytes());
        }
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN || &bytes[..4] != Self::MAGIC {
            return None;
        }
        let field = |i: usize| {
            let start = 4 + i * 8;
            bytes
                .get(start..start + 8)
                .and_then(|b| b.try_into().ok())
                .map(u64::from_le_bytes)
        };
        Some(Self {
            vectors: field(0)?,
            scopes: field(1)?,
            bytes: field(2)?,
        })
    }

    /// Count one scoped sidecar of `vectors` vectors and `bytes` bytes.
    fn add_scope(&mut self, vectors: usize, bytes: usize) {
        self.vectors = self.vectors.saturating_add(vectors as u64);
        self.scopes = self.scopes.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes as u64);
    }
}

/// Key of `(gen, index_name)` in a store's set of vector-ready generations.
fn vector_ready_key(gen: &str, index_name: &str) -> String {
    format!("{gen}/{index_name}")
}

/// Write one scoped sidecar per partition of `reader` that holds a `cfg`
/// vector, then the manifest that marks them complete. Calls `on_scope` for
/// each scope written.
///
/// Streams the SSTable a partition at a time: memory is bounded by the
/// largest partition's vectors (one scope's HNSW graph), never by the
/// generation. Any read, decode or write failure is returned before the
/// manifest is written, so a partial build is never taken for a complete one.
fn build_generation_vector_sidecars<F: FlushTarget>(
    flush_target: &F,
    reader: &SSTableReader<F::Reader>,
    schema: &TableSchema,
    gen: u64,
    cfg: &VectorIndexConfig,
    mut on_scope: impl FnMut(&[u8]),
) -> Result<VectorSidecarManifest> {
    let mapping = ColumnOrdinalMapping::for_header(schema, reader.header());
    let mut iter = reader.partitions_iter()?;
    let mut manifest = VectorSidecarManifest::default();
    while let Some(mut partition) = iter.next_partition()? {
        mapping.remap_partition(&mut partition);
        let entries = partition_vectors(&partition, cfg.column_position)?;
        if entries.is_empty() {
            continue;
        }
        let vectors = entries.len();
        let scope = partition.key.key.as_bytes();
        let bytes = ferrosa_index::vector::hnsw::build_and_serialize(
            cfg.m,
            cfg.ef_construction,
            cfg.metric,
            entries,
        )
        .map_err(|e| {
            ferrosa_common::Error::InvalidData(format!(
                "vector index {}: building generation {gen}'s sidecar for one scope: {e}",
                cfg.index_name
            ))
        })?;
        flush_target.write_vector_sidecar(
            gen,
            &scoped_vector_sidecar_name(&cfg.index_name, scope),
            &bytes,
        )?;
        manifest.add_scope(vectors, bytes.len());
        on_scope(scope);
    }
    flush_target.write_vector_sidecar(
        gen,
        &vector_manifest_name(&cfg.index_name),
        &manifest.encode(),
    )?;
    Ok(manifest)
}

/// The live vectors of `partition`'s `column_position` column, positioned in
/// row order. A deleted row or cell carries none; a value that is not a
/// float vector is an error, not a silently unindexed row.
fn partition_vectors(
    partition: &Partition,
    column_position: usize,
) -> Result<Vec<(ferrosa_index::vector::RowPosition, Vec<f32>)>> {
    let mut entries = Vec::new();
    for row in partition.static_row.iter().chain(partition.rows.iter()) {
        if !row.deletion.is_live() {
            continue;
        }
        let Some(value) = row
            .cells
            .iter()
            .find(|(idx, _)| *idx as usize == column_position)
            .and_then(|(_, cell)| cell.value.as_deref())
        else {
            continue;
        };
        let vector = ferrosa_index::bytes_to_vec_f32(value).map_err(|e| {
            ferrosa_common::Error::InvalidData(format!(
                "a vector-indexed cell does not hold a float vector: {e}"
            ))
        })?;
        let position = ferrosa_index::vector::RowPosition::new(entries.len() as u64);
        entries.push((position, vector));
    }
    Ok(entries)
}

/// How one delivery attempt by a pausable producer ended.
enum Delivery {
    Sent,
    /// The consumer dropped the stream.
    Gone,
    /// The channel was full. The item comes back so the scan can pause
    /// holding it.
    Stalled(Result<Arc<Partition>>),
}

/// Hand `item` to the consumer. When the channel is full, give up the pool
/// slot and I/O permit for good and return the item ([`Delivery::Stalled`]),
/// so the producer pauses and returns its thread; the async supervisor waits
/// for room.
///
/// There is deliberately no wait here, not even a short one. The producer
/// runs on a blocking thread, and its consumer often needs a blocking thread
/// from the same bounded pool to make room (the PG executor pulls rows with
/// `blocking_recv` on `spawn_blocking`). A producer that waits for room on
/// its thread holds what its consumer needs: an earlier 10 ms grace wait put
/// all four threads of the PG listener's blocking pool into such waits beside
/// eight slow readers, every wait ran out, and another session's `SELECT`
/// took 9 s instead of 0.1 s (20 s+ on a 4-vCPU CI runner). A resume costs
/// one re-admission (the merger keeps its position), so pausing at once is
/// cheap.
fn deliver_or_pause(
    slot: &mut ferrosa_sched::ScanSlot,
    tx: &tokio::sync::mpsc::Sender<Result<Arc<Partition>>>,
    item: Result<Arc<Partition>>,
) -> Delivery {
    use tokio::sync::mpsc::error::TrySendError;
    match tx.try_send(item) {
        Ok(()) => Delivery::Sent,
        Err(TrySendError::Closed(_)) => Delivery::Gone,
        Err(TrySendError::Full(item)) => {
            // The slot (and I/O permit) go back now; the producer must return
            // without producing more, and its ticket admits the next run.
            slot.release();
            Delivery::Stalled(item)
        }
    }
}

/// A producer [`spawn_resumable_range_scan`] can run, stop and resume: one
/// run produces until it finishes or must stop, and never waits.
trait PausableScan: Send + 'static {
    fn run(
        &mut self,
        slot: &mut ferrosa_sched::ScanSlot,
        tx: &tokio::sync::mpsc::Sender<Result<Arc<Partition>>>,
    ) -> ScanRun;
}

/// How one run of a pausable range-scan producer ended.
enum ScanRun {
    /// The scan ended: the range is exhausted, an error was delivered, or the
    /// consumer went away.
    Finished,
    /// The consumer stopped reading. The item it would not take is handed to
    /// the supervisor, which delivers it when there is room and then resumes.
    Paused(Result<Arc<Partition>>),
    /// A more deserving scan waited at a budget boundary; the slot is given
    /// up and the supervisor re-admits the scan at once.
    Yielded,
}

/// A [`crate::range_merger::RangeMerger`] that owns everything it borrows: the
/// store view whose memtables it iterates, the open SSTable readers, the
/// column mappings and the projection. A paused scan keeps one of these, so
/// it resumes at its exact position, on any thread, at no cost — no re-open,
/// no re-seek, no skip, and exactly the output of a scan that never paused.
struct OwnedMerger<R: ReadAt + Send + Sync + 'static> {
    // Declared FIRST so it is dropped first (fields drop in declaration
    // order): it borrows from every field below.
    merger: crate::range_merger::RangeMerger<'static, R>,
    _mappings: Box<[ColumnOrdinalMapping]>,
    _wanted: Option<Box<[u16]>>,
    _readers: Box<[Arc<SSTableReader<R>>]>,
    _view: Arc<StoreView>,
}

impl<R: ReadAt + Send + Sync + 'static> OwnedMerger<R> {
    fn open(
        view: Arc<StoreView>,
        readers: Vec<Arc<SSTableReader<R>>>,
        schema: &TableSchema,
        wanted: Option<Vec<u16>>,
        start: Option<DecoratedKey>,
        end: Option<DecoratedKey>,
    ) -> Result<Self> {
        let readers: Box<[Arc<SSTableReader<R>>]> = readers.into_boxed_slice();
        let mappings: Box<[ColumnOrdinalMapping]> =
            sstable_column_mappings(schema, &readers).into_boxed_slice();
        let wanted: Option<Box<[u16]>> = wanted.map(Vec::into_boxed_slice);
        // SAFETY: each reference below points into a heap allocation — the
        // `StoreView` behind an `Arc`, or a boxed slice — that this struct
        // owns, never mutates, and frees only after `merger` is dropped
        // (`merger` is the first field). Moving the struct moves the `Arc` and
        // `Box` pointers, not the allocations, so the references stay valid
        // for as long as `merger` can use them; nothing hands them out beyond
        // the struct. This is the stable-allocation pattern the merger itself
        // uses for its SSTable runs (`runs_arena`). On unwind the struct drops
        // like any other value, merger first. Checked under Miri (Tree
        // Borrows): `owned_merger_*` tests, which a projection freed early
        // turns into a reported use-after-free.
        let view_ref: &'static StoreView = unsafe { &*Arc::as_ptr(&view) };
        let readers_ref: &'static [Arc<SSTableReader<R>>] =
            unsafe { &*(&*readers as *const [Arc<SSTableReader<R>>]) };
        let mappings_ref: &'static [ColumnOrdinalMapping] =
            unsafe { &*(&*mappings as *const [ColumnOrdinalMapping]) };
        let wanted_ref: Option<&'static [u16]> = wanted
            .as_deref()
            .map(|wanted| unsafe { &*(wanted as *const [u16]) });

        let active_iter = view_ref.active.range_iter(start.as_ref(), end.as_ref());
        // Every sealed memtable still being flushed is a source, as in the
        // other range producers.
        let flushing_iters: Vec<_> = view_ref
            .flushing
            .iter()
            .map(|sealed| sealed.memtable.range_iter(start.as_ref(), end.as_ref()))
            .collect();
        let merger = match wanted_ref {
            Some(wanted) => crate::range_merger::merger_for_projected_sources_with_mappings(
                active_iter,
                flushing_iters,
                readers_ref,
                mappings_ref,
                wanted,
                start,
                end,
            )?,
            None => crate::range_merger::merger_for_sources_with_mappings(
                active_iter,
                flushing_iters,
                readers_ref,
                mappings_ref,
                start,
                end,
            )?,
        };
        Ok(Self {
            merger,
            _mappings: mappings,
            _wanted: wanted,
            _readers: readers,
            _view: view,
        })
    }
}

/// A range scan that can stop mid-range and resume where it stopped. Between
/// runs it holds its merger (see [`OwnedMerger`]) and costs no pool slot and
/// no thread. It yields whole partitions, or `<= K`-row fragments of them
/// (`fragment_rows`) for the intra-partition streaming variants.
///
/// A resumed run continues the same merger, so the scan yields exactly what an
/// unpaused scan would. A storage range scan is not a snapshot: a write made
/// during the scan, ahead of its position, may or may not be seen, paused or
/// not; a key is never yielded twice, and keys come out in order.
struct RangeScan<F: FlushTarget> {
    view: Arc<StoreView>,
    schema: Arc<TableSchema>,
    reader_pool: SharedReaderPool<F::Reader>,
    pool_table_key: String,
    flush_target: Arc<F>,
    /// The cell ordinals to decode, for the projected variant.
    wanted: Option<Vec<u16>>,
    partition_limit: Option<usize>,
    /// `Some(K)`: yield `<= K`-row fragments (`next_fragment`) instead of whole
    /// partitions. A fragment scan has no partition limit.
    fragment_rows: Option<usize>,
    start: Option<DecoratedKey>,
    end: Option<DecoratedKey>,
    /// Opened by the first run, after admission (t_6d0553ee), and kept across
    /// pauses: the scan's position, and its SSTable readers, so compaction
    /// cannot pull an input out from under a paused scan.
    merger: Option<OwnedMerger<F::Reader>>,
    emitted: usize,
    /// The store's live view, read again when compaction retires an input
    /// under the scan (`resume_after_retired_input`).
    live_view: Arc<ArcSwap<StoreView>>,
    /// How far the scan has delivered, so a resumed merger skips it.
    delivered: Option<Delivered>,
    /// Set while a resumed merger re-reads the partition it stopped in.
    skip: Option<Delivered>,
    /// Resumes so far; bounded by [`RANGE_SCAN_MAX_RESUMES`].
    resumes: u32,
    /// The table-level tombstone resolved against the scan's initial view.
    /// Folded into every partition the scan yields so a TRUNCATE hides rows on the
    /// streaming scan path too. A scan is not a snapshot, so a truncate committed
    /// after the scan started may not be seen by that scan (documented behaviour).
    table_delete: DeletionTime,
}

/// The last partition a scan delivered (some or all of), for resuming it.
#[derive(Clone, Debug)]
struct Delivered {
    key: DecoratedKey,
    /// The last row delivered from `key`, by clustering bytes (the merger's
    /// row order). `None`: only its header, or no rows, went out.
    last_clustering: Option<Vec<u8>>,
    /// Every fragment of `key` went out.
    complete: bool,
}

/// Resumes a single scan may make before its error is delivered. Each is a
/// compaction retiring an input under it; more than this in one scan means
/// something other than compaction is removing files.
const RANGE_SCAN_MAX_RESUMES: u32 = 8;

static RANGE_SCAN_COMPACTION_RESUMES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Times a streamed range scan re-opened on a fresh view because compaction
/// retired one of its inputs mid-scan.
pub fn range_scan_compaction_resumes_total() -> u64 {
    RANGE_SCAN_COMPACTION_RESUMES_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// A read that failed because a file is gone: the error a retired input gives.
fn is_missing_file(error: &ferrosa_common::Error) -> bool {
    matches!(error, ferrosa_common::Error::Io(e) if e.kind() == std::io::ErrorKind::NotFound)
}

/// Whether `later` no longer has some SSTable `earlier` had: compaction (or
/// eviction) took an input away.
fn view_dropped_an_sstable(earlier: &StoreView, later: &StoreView) -> bool {
    let live: std::collections::HashSet<&str> =
        later.sstables.iter().map(|d| d.gen.as_str()).collect();
    earlier
        .sstables
        .iter()
        .any(|d| !live.contains(d.gen.as_str()))
}

impl Delivered {
    /// What of `partition` has not been delivered yet. A key already passed
    /// whole is dropped; the rest of a key stopped mid-way keeps only rows
    /// past `last_clustering`, without the header the first fragment carried.
    fn remainder(&self, partition: Arc<Partition>) -> Option<Arc<Partition>> {
        if partition.key != self.key {
            return Some(partition);
        }
        if self.complete {
            return None;
        }
        // Copy-on-write: only the resume path (rare — a compaction retired an
        // input mid-scan) materialises an owned partition to trim rows.
        let mut owned = Arc::unwrap_or_clone(partition);
        if let Some(last) = &self.last_clustering {
            owned
                .rows
                .retain(|row| row.clustering.as_slice() > last.as_slice());
        }
        owned.deletion = ferrosa_sstable::types::DeletionTime::LIVE;
        owned.static_row = None;
        (!owned.rows.is_empty()).then_some(Arc::new(owned))
    }

    /// Record that `partition` went out; `complete` when its key is done.
    fn advance(previous: Option<Delivered>, partition: &Partition, complete: bool) -> Delivered {
        let carried = previous
            .filter(|p| p.key == partition.key)
            .and_then(|p| p.last_clustering);
        Delivered {
            key: partition.key.clone(),
            last_clustering: partition
                .rows
                .last()
                .map(|row| row.clustering.clone())
                .or(carried),
            complete,
        }
    }
}

impl<F: FlushTarget> RangeScan<F>
where
    F: Send + Sync + 'static,
{
    /// Open the readers and the merger: on the first run only, now that a
    /// slot is held.
    fn open_merger(&self) -> Result<OwnedMerger<F::Reader>> {
        let readers = open_pooled_readers(
            &self.reader_pool,
            &self.pool_table_key,
            &*self.flush_target,
            &self.view.sstables,
            self.start.as_ref(),
            self.end.as_ref(),
        )?;
        OwnedMerger::open(
            Arc::clone(&self.view),
            readers,
            &self.schema,
            self.wanted.clone(),
            self.start.clone(),
            self.end.clone(),
        )
    }

    /// After a mid-scan read failed on a missing file: if compaction has
    /// since retired one of the scan's inputs, re-open the merger on the
    /// current view, starting at the partition the scan stopped in, and
    /// return `true`. The rows already delivered are skipped as they come
    /// back (`Delivered::remainder`), so no key is yielded twice and none is
    /// missed. `false`: the file is missing for some other reason, or the
    /// scan has resumed too often; the caller delivers the error.
    fn resume_after_retired_input(&mut self, error: &ferrosa_common::Error) -> bool {
        if !is_missing_file(error) || self.resumes >= RANGE_SCAN_MAX_RESUMES {
            return false;
        }
        let current = self.live_view.load_full();
        if !view_dropped_an_sstable(&self.view, &current) {
            return false;
        }
        self.resumes += 1;
        RANGE_SCAN_COMPACTION_RESUMES_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(
            %error,
            resume = self.resumes,
            from = ?self.delivered.as_ref().map(|d| &d.key),
            "range scan: an input was retired under the scan (compaction); resuming on the \
             current view after the last partition delivered"
        );
        self.view = current;
        self.merger = None;
        if let Some(delivered) = &self.delivered {
            self.start = Some(delivered.key.clone());
        }
        self.skip = self.delivered.clone();
        true
    }
}

impl<F: FlushTarget> PausableScan for RangeScan<F>
where
    F: Send + Sync + 'static,
{
    /// Produce until the range ends, the consumer goes away, the channel is
    /// full, or the scan is told to yield its slot. Never waits.
    fn run(
        &mut self,
        slot: &mut ferrosa_sched::ScanSlot,
        tx: &tokio::sync::mpsc::Sender<Result<Arc<Partition>>>,
    ) -> ScanRun {
        let cap = self.partition_limit.unwrap_or(usize::MAX);
        loop {
            if self.merger.is_none() {
                match self.open_merger() {
                    Ok(merger) => self.merger = Some(merger),
                    Err(e) if self.resume_after_retired_input(&e) => continue,
                    Err(e) => return deliver_failure(slot, tx, e),
                }
            }
            if self.emitted >= cap {
                return ScanRun::Finished;
            }
            let Some(owned) = self.merger.as_mut() else {
                unreachable!("the merger is opened above");
            };
            let next = match self.fragment_rows {
                Some(k) => owned.merger.next_fragment(k).map(|fragment| {
                    fragment.map(|f| {
                        let complete = f.last;
                        (Arc::new(f.into_partition()), complete)
                    })
                }),
                None => owned
                    .merger
                    .next_merged_partition()
                    .map(|partition| partition.map(|p| (p, true))),
            };
            let (partition, complete) = match next {
                Ok(Some(item)) => item,
                Ok(None) => return ScanRun::Finished,
                Err(e) if self.resume_after_retired_input(&e) => continue,
                Err(e) => return deliver_failure(slot, tx, e),
            };
            // The reserved table-tombstone partition is bookkeeping, not data:
            // never surface it to a scan's consumer, and fold its watermark into
            // every real partition so a TRUNCATE hides earlier rows table-wide.
            if crate::table_tombstone::is_table_tombstone_key(&partition.key) {
                continue;
            }
            let partition = if !self.table_delete.is_live()
                && self.table_delete.marked_for_delete_at > partition.deletion.marked_for_delete_at
            {
                let mut owned = Arc::unwrap_or_clone(partition);
                merge::apply_table_deletion(&mut owned, self.table_delete);
                Arc::new(owned)
            } else {
                partition
            };
            let partition = match &self.skip {
                Some(skip) if skip.key == partition.key => match skip.remainder(partition) {
                    Some(rest) => rest,
                    None => continue,
                },
                Some(_) => {
                    self.skip = None;
                    partition
                }
                None => partition,
            };
            self.delivered = Some(Delivered::advance(
                self.delivered.take(),
                &partition,
                complete,
            ));
            match deliver_or_pause(slot, tx, Ok(partition)) {
                Delivery::Sent => {
                    if self.fragment_rows.is_none() {
                        self.emitted += 1;
                    }
                    // B1 T1.2: account the chunk; every budget chunks a more
                    // deserving scan may take the slot, and then this run ends.
                    if slot.tick() == ferrosa_sched::Tick::Yield {
                        return ScanRun::Yielded;
                    }
                }
                Delivery::Gone => return ScanRun::Finished,
                Delivery::Stalled(item) => {
                    // Handed out by the supervisor once there is room.
                    if self.fragment_rows.is_none() {
                        self.emitted += 1;
                    }
                    return ScanRun::Paused(item);
                }
            }
        }
    }
}

/// Deliver a scan failure from a pausable producer. If the consumer is not
/// reading, the error is held while paused and delivered by the supervisor,
/// never dropped. A consumer that has gone has nobody to tell, so that case
/// is logged (designed fallback).
fn deliver_failure(
    slot: &mut ferrosa_sched::ScanSlot,
    tx: &tokio::sync::mpsc::Sender<Result<Arc<Partition>>>,
    error: ferrosa_common::Error,
) -> ScanRun {
    let message = error.to_string();
    match deliver_or_pause(slot, tx, Err(error)) {
        Delivery::Sent => ScanRun::Finished,
        Delivery::Gone => {
            tracing::debug!(error = %message, "range scan failed after its consumer went away");
            ScanRun::Finished
        }
        Delivery::Stalled(item) => ScanRun::Paused(item),
    }
}

static RANGE_SCAN_RESUMES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Times a paused range scan resumed after its consumer made room. Each costs
/// one scheduler admission; the merger keeps its position.
pub fn range_scan_resumes_total() -> u64 {
    RANGE_SCAN_RESUMES_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// Run a [`RangeScan`] through the bounded scheduler pool, pausing it while its
/// consumer is not reading and resuming it when the consumer makes room.
///
/// The supervisor is an async task: while the scan is paused, or queued after
/// yielding its slot, it holds no pool slot and no thread — the producer never
/// waits on a thread for anything. The first run is a fresh admission and
/// every later one is re-admitted with the scan's ticket (its place in the
/// fair queue). Every admission keeps two properties: cancellation while
/// queued (the consumer dropped the stream) and fail-loud overload. A producer
/// panic is delivered as an error, never as a short stream.
fn spawn_resumable_range_scan<S: PausableScan>(
    tx: tokio::sync::mpsc::Sender<Result<Arc<Partition>>>,
    scan: S,
) {
    tokio::spawn(async move {
        let mut scan = scan;
        let mut ticket: Option<ferrosa_sched::ScanTicket> = None;
        loop {
            let cancel_probe = tx.clone();
            let cancel = async move { cancel_probe.closed().await };
            let producer_tx = tx.clone();
            let produce = move |slot: &mut ferrosa_sched::ScanSlot| {
                let outcome = scan.run(slot, &producer_tx);
                match outcome {
                    // Release the readers and memtable views here, on the
                    // producer's thread, as the scan ends — not whenever this
                    // supervisor task is next polled.
                    ScanRun::Finished => None,
                    stopped => Some((scan, stopped, slot.take_ticket())),
                }
            };
            let pool = ferrosa_sched::global_pool();
            let handle = match ticket.take() {
                None => pool.submit_scan(
                    ferrosa_sched::SchedClass::Bulk,
                    ferrosa_sched::DEFAULT_SCAN_CHUNK_BUDGET,
                    cancel,
                    produce,
                ),
                Some(next) => pool.resubmit_scan(next, cancel, produce),
            };
            let (back, stopped, next_ticket) = match handle.await {
                Ok(ferrosa_sched::ScanOutcome::Ran(Some(stopped))) => stopped,
                Ok(ferrosa_sched::ScanOutcome::Ran(None))
                | Ok(ferrosa_sched::ScanOutcome::Cancelled) => return,
                Ok(ferrosa_sched::ScanOutcome::Overloaded) => {
                    fail_range_scan(
                        &tx,
                        "overloaded: scan admission queue full — retry".to_string(),
                    )
                    .await;
                    return;
                }
                Err(join_error) => {
                    fail_range_scan(&tx, format!("range scan producer failed: {join_error}")).await;
                    return;
                }
            };
            scan = back;
            let Some(next_ticket) = next_ticket else {
                // A run stops early only by giving its slot up, which always
                // leaves a ticket. Without one the scan cannot continue; fail
                // it rather than end the stream as if it were complete.
                fail_range_scan(
                    &tx,
                    "range scan stopped without a resume ticket; the scan is incomplete"
                        .to_string(),
                )
                .await;
                return;
            };
            ticket = Some(next_ticket);
            let pending = match stopped {
                ScanRun::Yielded => continue,
                ScanRun::Paused(pending) => pending,
                ScanRun::Finished => unreachable!("a finished run returns None"),
            };
            // Paused: no slot, no thread. Wait for room, hand over the item the
            // consumer would not take, then resume after it.
            let ends_scan = pending.is_err();
            match tx.reserve().await {
                Ok(permit) => permit.send(pending),
                Err(_) => return, // the consumer dropped the stream
            }
            if ends_scan {
                return;
            }
            // Resume once the consumer has drained half the channel, not at
            // its first read, so a slow consumer costs one resume per several
            // partitions rather than one per partition.
            let room = (tx.max_capacity() / 2).max(1);
            match tx.reserve_many(room).await {
                Ok(permits) => drop(permits),
                Err(_) => return,
            }
            RANGE_SCAN_RESUMES_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    });
}

/// End a range scan's stream with an error, so its consumer sees a failure
/// rather than a short result. A consumer that has gone is logged.
async fn fail_range_scan(tx: &tokio::sync::mpsc::Sender<Result<Arc<Partition>>>, message: String) {
    tracing::warn!(error = %message, "range scan ended with an error");
    if tx
        .send(Err(ferrosa_common::Error::InvalidData(message)))
        .await
        .is_err()
    {
        tracing::debug!("range scan error had no consumer left to receive it");
    }
}

/// Open (pooled) the readers for every descriptor whose byte-comparable key
/// range overlaps `[start, end]`, newest-first. Same logic as
/// [`TableStore::open_readers_for_key_range`] but over cloned, `'static`-owned
/// pool inputs (`Arc<ReaderPool>`, the table key, `Arc<F>`), so a scan producer
/// can open its readers *after* admission — a cancelled or overloaded scan then
/// never opens readers (t_6d0553ee). Pruning stays conservative (FMEA #2): a
/// descriptor is skipped only when its key range is provably disjoint.
fn open_pooled_readers<T: FlushTarget>(
    reader_pool: &SharedReaderPool<T::Reader>,
    pool_table_key: &str,
    flush_target: &T,
    descriptors: &[SstableDescriptor],
    start: Option<&DecoratedKey>,
    end: Option<&DecoratedKey>,
) -> Result<Vec<Arc<SSTableReader<T::Reader>>>> {
    open_pooled_readers_with(
        reader_pool,
        pool_table_key,
        flush_target,
        descriptors,
        start,
        end,
        None,
    )
}

/// [`open_pooled_readers`] with an optional negative cache. Callers that sit
/// under the fresh-view retry loop pass the store's, so a known-missing
/// generation fails fast instead of being reopened on every retry.
fn open_pooled_readers_with<T: FlushTarget>(
    reader_pool: &SharedReaderPool<T::Reader>,
    pool_table_key: &str,
    flush_target: &T,
    descriptors: &[SstableDescriptor],
    start: Option<&DecoratedKey>,
    end: Option<&DecoratedKey>,
    missing: Option<&MissingSstableCache>,
) -> Result<Vec<Arc<SSTableReader<T::Reader>>>> {
    let start_bytes = start.map(ferrosa_sstable::byte_comparable::encode);
    let end_bytes = end.map(ferrosa_sstable::byte_comparable::encode);
    let mut readers = Vec::new();
    for desc in descriptors.iter() {
        if let Some(ref sb) = start_bytes {
            if sb.as_slice() > desc.max_key.as_slice() {
                continue;
            }
        }
        if let Some(ref eb) = end_bytes {
            if eb.as_slice() < desc.min_key.as_slice() {
                continue;
            }
        }
        let dir = desc.dir.clone();
        let gen = desc.gen_num();
        let key = (pool_table_key.to_string(), gen);
        let open = || reader_pool.get_or_open(key, || flush_target.open_reader(&dir, gen));
        readers.push(match missing {
            Some(cache) => cache.open_through(&desc.gen, open)?,
            None => open()?,
        });
    }
    Ok(readers)
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

/// [`ferrosa_sstable::reader::PartitionIter::stream_clustered_rows`] where a
/// failure of the SSTable's own decode is attributed through `attribute`, but
/// a failure raised by `on_row` (the caller's sink, not the SSTable) passes
/// through unchanged: a full hash sink must not quarantine a healthy file.
fn stream_rows_attributed<R: ReadAt + Send + Sync + 'static>(
    iter: &mut ferrosa_sstable::reader::PartitionIter<'_, R>,
    mut on_row: impl FnMut(&Row) -> Result<()>,
    attribute: impl FnOnce(ferrosa_common::Error) -> ferrosa_common::Error,
) -> Result<()> {
    let mut sink_failed = false;
    let streamed =
        iter.stream_clustered_rows(|row| on_row(row).inspect_err(|_| sink_failed = true));
    streamed.map_err(|e| if sink_failed { e } else { attribute(e) })
}

/// How long a generation whose object could not be opened keeps failing fast.
const MISSING_SSTABLE_TTL: std::time::Duration = std::time::Duration::from_secs(5);
/// Most generations the negative cache remembers at once.
const MISSING_SSTABLE_CAP: usize = 256;

/// Short-lived negative cache of SSTable generations whose object could not be
/// opened (missing from local disk and from the object store, typically an
/// evicted SSTable whose rehydrate failed).
///
/// Without it every range read re-opens the same missing object on each of the
/// eight fresh-view retries and again on the next read, until repair
/// intervenes. With it the FIRST failed open is real; for the next `ttl` the
/// same generation fails immediately with an error that still reaches the
/// caller as the typed `CorruptSstable` — it is a fast failure, never a short
/// `Ok`. Entries are dropped when the generation is seeded (restored or
/// freshly written), when quarantine is resolved, when an open of it succeeds,
/// when they expire, and — to bound memory — oldest first once `cap` is hit.
///
/// Observable through [`TableStore::missing_sstable_open_failures`] /
/// [`TableStore::missing_sstable_fast_fails`] and a WARN when a generation
/// first enters the cache.
pub(crate) struct MissingSstableCache {
    ttl: std::time::Duration,
    cap: usize,
    /// `gen -> when its open last failed`.
    entries: parking_lot::Mutex<HashMap<String, Instant>>,
    /// Mirror of `entries.len()` so the all-healthy hot path skips the lock.
    count: std::sync::atomic::AtomicUsize,
    open_failures: std::sync::atomic::AtomicU64,
    fast_fails: std::sync::atomic::AtomicU64,
}

impl Default for MissingSstableCache {
    fn default() -> Self {
        Self::new(MISSING_SSTABLE_TTL, MISSING_SSTABLE_CAP)
    }
}

impl MissingSstableCache {
    pub(crate) fn new(ttl: std::time::Duration, cap: usize) -> Self {
        assert!(
            cap > 0,
            "a zero-capacity negative cache cannot record anything"
        );
        Self {
            ttl,
            cap,
            entries: parking_lot::Mutex::new(HashMap::new()),
            count: std::sync::atomic::AtomicUsize::new(0),
            open_failures: std::sync::atomic::AtomicU64::new(0),
            fast_fails: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.count.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn sync_len(&self, entries: &HashMap<String, Instant>) {
        self.count
            .store(entries.len(), std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether `gen` failed to open within the last `ttl`. An expired entry is
    /// dropped on the way past.
    pub(crate) fn is_known_missing_at(&self, gen: &str, now: Instant) -> bool {
        if self.len() == 0 {
            return false;
        }
        let mut entries = self.entries.lock();
        match entries.get(gen).copied() {
            Some(at) if now.saturating_duration_since(at) < self.ttl => true,
            Some(_) => {
                entries.remove(gen);
                self.sync_len(&entries);
                false
            }
            None => false,
        }
    }

    /// Remember that `gen` failed to open at `now`. Returns whether it is a
    /// new entry. At capacity, expired entries go first, then the oldest.
    pub(crate) fn record_at(&self, gen: &str, now: Instant) -> bool {
        let mut entries = self.entries.lock();
        if !entries.contains_key(gen) && entries.len() >= self.cap {
            let ttl = self.ttl;
            entries.retain(|_, at| now.saturating_duration_since(*at) < ttl);
            if entries.len() >= self.cap {
                let oldest = entries
                    .iter()
                    .min_by_key(|(_, at)| **at)
                    .map(|(g, _)| g.clone());
                if let Some(oldest) = oldest {
                    entries.remove(&oldest);
                }
            }
        }
        let inserted = entries.insert(gen.to_string(), now).is_none();
        self.sync_len(&entries);
        inserted
    }

    /// Drop `gen`'s entry (restored, rewritten, or quarantine resolved).
    pub(crate) fn forget(&self, gen: &str) {
        if self.len() == 0 {
            return;
        }
        let mut entries = self.entries.lock();
        entries.remove(gen);
        self.sync_len(&entries);
    }

    /// Run `open` for `gen` unless it recently failed to open, in which case
    /// fail at once without touching storage.
    pub(crate) fn open_through<T>(&self, gen: &str, open: impl FnOnce() -> Result<T>) -> Result<T> {
        if self.is_known_missing_at(gen, Instant::now()) {
            self.fast_fails
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(ferrosa_common::Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "SSTable generation {gen} recently failed to open; failing fast for up to \
                     {:?} (restore or repair clears this)",
                    self.ttl
                ),
            )));
        }
        match open() {
            Ok(v) => {
                self.forget(gen);
                Ok(v)
            }
            Err(e) => {
                self.open_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if self.record_at(gen, Instant::now()) {
                    tracing::warn!(
                        gen,
                        ttl = ?self.ttl,
                        cause = %e,
                        "SSTable open failed; further reads fail fast until the TTL lapses or \
                         the generation is restored"
                    );
                }
                Err(e)
            }
        }
    }
}

/// One SSTable reader participating in a bounded token-range merge.
///
/// Either a **pooled** reader (opened through the shared
/// [`crate::reader_pool::ReaderPool`], evicted when idle) or an **ephemeral
/// spill run** produced by an earlier merge pass. Spill runs live in their own
/// temp directory outside the durable generation namespace and the shared
/// pool; the directory is removed when this `MergeReader` is dropped, so a
/// merge that errors out mid-pass never leaks files (fail-loud cleanup).
struct MergeReader<F: FlushTarget> {
    reader: Arc<SSTableReader<F::Reader>>,
    /// `Some` for an ephemeral spill run — the temp dir to remove on drop.
    /// `None` for a pooled reader (the pool owns its lifecycle).
    cleanup_dir: Option<std::path::PathBuf>,
    /// The durable SSTable this reader serves, so a failure reading it can
    /// name it in a typed error. `None` for an ephemeral spill run, which is
    /// this process's own scratch file, not a table SSTable.
    source: Option<SstableDescriptor>,
}

/// One input to a cascade level: either an unopened durable SSTable
/// (`Descriptor` — opened just-in-time, in batches of `<= budget`, so the pool
/// is never asked to hold more than the budget) or an already-open ephemeral
/// spill `Run` produced by a previous level. Keeping descriptors unopened until
/// their batch is processed is what bounds peak open readers to `budget` even
/// when thousands of SSTables overlap the range.
enum MergeInput<F: FlushTarget> {
    Descriptor(SstableDescriptor),
    Run(MergeReader<F>),
}

impl<F: FlushTarget> Drop for MergeReader<F> {
    fn drop(&mut self) {
        if let Some(dir) = self.cleanup_dir.take() {
            // Best-effort cleanup of the spill run's component files. Logged on
            // failure (fail-loud: a leaked spill dir is observable, not silent).
            if let Err(e) = std::fs::remove_dir_all(&dir) {
                tracing::warn!(
                    spill_dir = %dir.display(),
                    "merge spill cleanup failed: {e}"
                );
            }
        }
    }
}

/// Streaming k-way merge over a set of already-positioned SSTable partition
/// iterators (no memtable sources), invoking `emit` once per distinct key with
/// the **cell-merged** partition for that key. At most one merged `Partition`
/// is materialised at a time, so peak in-flight data is `O(1)` regardless of
/// how many sources participate or how wide a partition is. This is the engine
/// of the bounded multi-pass merge: each pass merges `<= budget` sources into
/// one sorted run, so it must hold at most `budget` readers open.
///
/// Keys past `end_token` are ignored. The merge is order-independent over
/// sources for a given key (`merge_partitions` is associative LWW), so the
/// emitted partition is identical no matter how inputs are batched across
/// passes — the property the multi-pass cascade relies on for digest
/// equivalence.
///
/// `fail(i, cause)` attributes a read error from `iters[i]` to the SSTable it
/// serves (typed `CorruptSstable` for a table SSTable). Errors from `emit` are
/// the caller's own and pass through untouched.
fn merge_sstable_iters<R, Emit, Fail>(
    iters: &mut [ferrosa_sstable::reader::PartitionIter<'_, R>],
    mappings: &[ColumnOrdinalMapping],
    end_token: i64,
    fail: Fail,
    mut emit: Emit,
) -> Result<()>
where
    R: ReadAt + Send + Sync + 'static,
    Emit: FnMut(Partition) -> Result<()>,
    Fail: Fn(usize, ferrosa_common::Error) -> ferrosa_common::Error,
{
    loop {
        let pick = |cur: &Option<DecoratedKey>, candidate: &DecoratedKey| -> bool {
            cur.as_ref().map(|k| candidate < k).unwrap_or(true)
        };
        let mut smallest_key: Option<DecoratedKey> = None;
        for (idx, iter) in iters.iter_mut().enumerate() {
            // A peek error is an unreadable SSTable: propagate it. Treating it
            // as "exhausted" would silently drop every later partition of
            // that source from the merge.
            if let Some(k) = iter.peek_partition_key().map_err(|e| fail(idx, e))? {
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

        let mut group: Vec<Partition> = Vec::new();
        for (idx, iter) in iters.iter_mut().enumerate() {
            let peeked = iter.peek_partition_key().map_err(|e| fail(idx, e))?;
            if matches!(peeked, Some(k) if k == key) {
                if let Some(mut p) = iter.next_partition().map_err(|e| fail(idx, e))? {
                    mappings[idx].remap_partition(&mut p);
                    group.push(p);
                }
            }
        }
        let merged = if group.len() == 1 {
            group.into_iter().next().expect("len 1")
        } else {
            let mut m = merge::merge_partitions(group);
            merge::apply_deletions(&mut m);
            m
        };
        emit(merged)?;
    }
    Ok(())
}

/// Park `iter` at the first partition with `token >= start_token`.
///
/// `seek_to_token` is an optimisation (O(log N) via the token-offset cache):
/// if it fails the iterator is still at the start, and the linear skip below
/// reaches the same position, so that failure is a logged fallback. A failure
/// of the skip or the peek is an unreadable SSTable and is returned — the
/// caller must not treat the source as exhausted.
fn position_iter_at_token<R: ReadAt + Send + Sync + 'static>(
    iter: &mut ferrosa_sstable::reader::PartitionIter<'_, R>,
    start_token: i64,
) -> Result<()> {
    if let Err(e) = iter.seek_to_token(start_token) {
        tracing::warn!(
            %e,
            start_token,
            "seek_to_token failed; falling back to a linear skip to the first in-range partition"
        );
    }
    while let Some(k) = iter.peek_partition_key()? {
        if k.token.0 >= start_token {
            break;
        }
        iter.skip_to_next_partition()?;
    }
    Ok(())
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

fn clone_partition_limited(
    partition: &Partition,
    start_clustering: Option<&[u8]>,
    row_limit: usize,
) -> Partition {
    if row_limit == 0 {
        return partition.clone();
    }

    Partition {
        key: partition.key.clone(),
        deletion: partition.deletion,
        static_row: partition.static_row.clone(),
        rows: partition
            .rows
            .iter()
            .filter(|row| start_clustering.is_none_or(|start| row.clustering.as_slice() > start))
            .take(row_limit)
            .cloned()
            .collect(),
    }
}

/// Add one live row to a scalar memtable index using the same predicate and
/// key-encoding semantics for write-time maintenance and CREATE INDEX backfill.
fn insert_scalar_memtable_index_row(
    index: &MemtableIndex,
    index_name: &str,
    column_position: usize,
    index_type: IndexType,
    filter_predicate: Option<&FilterPredicate>,
    partition_key: &[u8],
    row: &Row,
) {
    if let Some(predicate) = filter_predicate {
        let matches = ferrosa_index::evaluate_predicate_row(predicate, |predicate_col_pos| {
            row.cells
                .iter()
                .find(|(idx, _)| *idx as usize == predicate_col_pos)
                .and_then(|(_, cell)| cell.value.as_deref())
        });
        if !matches {
            return;
        }
    }

    let Some(value) = row
        .cells
        .iter()
        .find(|(idx, _)| *idx as usize == column_position)
        .and_then(|(_, cell)| cell.value.as_deref())
    else {
        return;
    };
    match crate::index::scheduler::encode_index_key(index_type, value) {
        Ok(Some(index_key)) => index.insert(
            index_key,
            RowPosition {
                partition_key: partition_key.to_vec(),
                clustering_key: row.clustering.clone(),
            },
        ),
        Ok(None) => {}
        Err(e) => tracing::warn!(
            %e,
            %index_name,
            "store: skipping memtable index entry; key encoding failed"
        ),
    }
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

/// Test-only deterministic hook for the read-vs-compaction race. A read, right
/// after it snapshots the store view, pauses at an armed barrier so a test can
/// interpose a full compaction (view swap + input-file deletion) before the read
/// opens its SSTables — reproducing the stale-view condition every time instead
/// of relying on timing.
/// Test-only fault injection for [`TableStore::flush_with_swap_callback`]:
/// a panic armed on the calling thread fires at a named point of the flush,
/// so a test can reproduce "the flush thread died mid-flush" (2026-10-02,
/// node2: `writer.rs:1965` panicked after the memtable swap, t_7681b32b)
/// without depending on a particular encoder bug still existing.
#[cfg(test)]
pub(crate) mod flush_fault_test_hook {
    use std::cell::Cell;

    /// Where in the flush an armed panic fires.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum FlushFault {
        /// After the active memtable moved to `flushing`, before encoding.
        AfterSwap,
    }

    thread_local! {
        static ARMED: Cell<Option<FlushFault>> = const { Cell::new(None) };
    }

    /// Arm one panic at `at` for the next flush on this thread.
    pub(crate) fn arm(at: FlushFault) {
        ARMED.with(|armed| armed.set(Some(at)));
    }

    /// Fire (and disarm) the panic if one is armed for `at` on this thread.
    pub(crate) fn fire(at: FlushFault) {
        if ARMED.with(|armed| armed.get()) == Some(at) {
            ARMED.with(|armed| armed.set(None));
            panic!("injected flush panic at {at:?}");
        }
    }
}

#[cfg(test)]
pub(crate) mod read_race_test_hook {
    use std::sync::{Arc, Condvar, Mutex};

    /// One-shot rendezvous between a paused read and the test driving it.
    #[derive(Default)]
    pub struct ReadViewBarrier {
        /// `(reached, released)`.
        state: Mutex<(bool, bool)>,
        cv: Condvar,
    }

    impl ReadViewBarrier {
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        /// Reader side: signal that the view is snapshotted, then block until
        /// the test releases.
        pub(crate) fn reach_and_wait(&self) {
            let mut g = self.state.lock().unwrap();
            g.0 = true;
            self.cv.notify_all();
            while !g.1 {
                g = self.cv.wait(g).unwrap();
            }
        }

        /// Test side: block until the read has snapshotted its view.
        pub fn wait_reached(&self) {
            let mut g = self.state.lock().unwrap();
            while !g.0 {
                g = self.cv.wait(g).unwrap();
            }
        }

        /// Test side: let the paused read proceed.
        pub fn release(&self) {
            let mut g = self.state.lock().unwrap();
            g.1 = true;
            self.cv.notify_all();
        }
    }

    thread_local! {
        /// When set on a thread, that thread's next `read_limited_rows` pauses
        /// once at the barrier right after snapshotting the view.
        pub static ARMED: std::cell::RefCell<Option<Arc<ReadViewBarrier>>> =
            const { std::cell::RefCell::new(None) };

        /// When set on a thread, runs between a failed range-scan attempt and
        /// its fresh-view retry — where a compaction swap would land.
        pub static ON_SCAN_RETRY: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
            const { std::cell::RefCell::new(None) };
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only: rows an index read fetched by single-row point read.
    static INDEX_POINT_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Test-only: bounded chunks an index read streamed whole partitions in.
    static INDEX_PARTITION_CHUNKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// One row-ordered source of index postings: the memtable's pinned list or
/// an SSTable sidecar's run for the key, borrowed in place (a sidecar's from
/// its memory map).
type PostingSource<'a> = Box<dyn Iterator<Item = RowPositionRef<'a>> + 'a>;

/// An index key matching at least `1 / UNSELECTIVE_INDEX_SHARE_DENOMINATOR`
/// of a table's partitions is served by scan rather than point reads, when
/// the query licenses a scan (see [`TableStore::index_key_is_unselective`]).
/// A point read probes every SSTable; a scan streams each once, so the scan
/// wins long before the key names the whole table.
pub const UNSELECTIVE_INDEX_SHARE_DENOMINATOR: u64 = 10;

/// Below this many matches an index key is always served by the index: on a
/// small table the point reads are cheap and the scan saves nothing.
pub const UNSELECTIVE_INDEX_MIN_MATCHES: usize = 50;

/// The row-ordered posting sources for one index key, each positioned at
/// `from` (inclusive): the memtables' posting lists (see
/// [`memtable_posting_lists`]), then every SSTable sidecar that holds the
/// index.
fn index_posting_sources<'a>(
    view: &'a StoreView,
    memtables: &'a [crate::memtable::index::PostingList],
    index_name: &str,
    key: &'a IndexKey,
    from: Option<&RowPosition>,
) -> Vec<PostingSource<'a>> {
    let mut sources: Vec<PostingSource<'a>> =
        Vec::with_capacity(view.sidecar_indexes.len() + memtables.len());
    for list in memtables {
        let postings = list.as_slice();
        let start = postings.partition_point(|p| from.is_some_and(|start| p < start));
        sources.push(Box::new(postings[start..].iter().map(RowPositionRef::from)));
    }
    for sidecar in view.sidecar_indexes.iter() {
        if let Some(reader) = sidecar.get(index_name) {
            sources.push(Box::new(reader.postings_from(key, from)));
        }
    }
    sources
}

/// The posting lists for `key` in `index_name` of the active memtable and of
/// every sealed memtable not yet flushed (T1): until a sealed memtable's
/// sidecar is installed, its postings are the only record of its rows.
fn memtable_posting_lists(
    view: &StoreView,
    index_name: &str,
    key: &IndexKey,
) -> Vec<crate::memtable::index::PostingList> {
    memtable_indexes_named(view, index_name)
        .iter()
        .filter_map(|index| index.posting_list(key))
        .collect()
}

/// `flushing` without the sealed memtable holding `memtable`.
fn without_sealed(
    flushing: &[Arc<SealedMemtable>],
    memtable: &Arc<dyn Memtable>,
) -> Arc<Vec<Arc<SealedMemtable>>> {
    let target = Arc::as_ptr(memtable).cast::<()>();
    Arc::new(
        flushing
            .iter()
            .filter(|sealed| Arc::as_ptr(&sealed.memtable).cast::<()>() != target)
            .cloned()
            .collect(),
    )
}

/// The generation `ann_search` files a sealed memtable's results under: one
/// per sealed memtable counting down from here, so placeholder positions
/// cannot collide with the active memtable's or a real SSTable's (flush
/// generations never reach this range).
const FLUSHING_MEMTABLE_GENERATION: u64 = u64::MAX;

/// Every sealed memtable's vector index named `index_name`, newest first.
fn flushing_vector_indexes(view: &StoreView, index_name: &str) -> Vec<Arc<VectorMemtableIndex>> {
    view.flushing
        .iter()
        .filter_map(|sealed| sealed.vectors.load().get(index_name).cloned())
        .collect()
}

/// The active memtable's index named `index_name` and every sealed one's.
fn memtable_indexes_named(view: &StoreView, index_name: &str) -> Vec<Arc<MemtableIndex>> {
    view.indexes
        .get(index_name)
        .cloned()
        .into_iter()
        .chain(
            view.flushing
                .iter()
                .filter_map(|sealed| sealed.postings.load().get(index_name).cloned()),
        )
        .collect()
}

/// K-way merge of whole sidecars in `(key, row)` order, yielding each entry
/// once: one head per sidecar, duplicates dropped by comparison with the
/// previous entry.
/// One sidecar entry, borrowed: `(index key, row position)`.
type SidecarEntryRef<'a> = (&'a [u8], RowPositionRef<'a>);

/// One whole sidecar's entries, in `(key, row)` order.
type SidecarEntrySource<'a> = Box<dyn Iterator<Item = SidecarEntryRef<'a>> + 'a>;

struct MergedSidecarEntries<'a> {
    sources: Vec<SidecarEntrySource<'a>>,
    heads: std::collections::BinaryHeap<std::cmp::Reverse<(SidecarEntryRef<'a>, usize)>>,
    previous: Option<SidecarEntryRef<'a>>,
}

impl<'a> MergedSidecarEntries<'a> {
    fn new(readers: &[&'a SidecarReader]) -> Self {
        let mut sources: Vec<SidecarEntrySource<'a>> = readers
            .iter()
            .map(|reader| Box::new(reader.entries_in_order()) as SidecarEntrySource<'a>)
            .collect();
        let mut heads = std::collections::BinaryHeap::with_capacity(sources.len());
        for (index, source) in sources.iter_mut().enumerate() {
            if let Some(first) = source.next() {
                heads.push(std::cmp::Reverse((first, index)));
            }
        }
        Self {
            sources,
            heads,
            previous: None,
        }
    }
}

impl<'a> Iterator for MergedSidecarEntries<'a> {
    type Item = SidecarEntryRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let std::cmp::Reverse((entry, index)) = self.heads.pop()?;
            if let Some(following) = self.sources[index].next() {
                self.heads.push(std::cmp::Reverse((following, index)));
            }
            if self.previous == Some(entry) {
                continue;
            }
            self.previous = Some(entry);
            return Some(entry);
        }
    }
}

/// K-way merge of row-ordered posting sources, yielding each row once.
///
/// Holds one head per source and the previous row: a row present in two
/// sources surfaces from both consecutively and the second is dropped by
/// comparison with the previous one, so no set of seen rows is kept. Each
/// `next` consumes at least one posting, so the walk is bounded by the
/// postings present.
struct OrderedPostings<'a> {
    sources: Vec<PostingSource<'a>>,
    heads: std::collections::BinaryHeap<std::cmp::Reverse<(RowPositionRef<'a>, usize)>>,
    previous: Option<RowPositionRef<'a>>,
}

impl<'a> OrderedPostings<'a> {
    fn new(mut sources: Vec<PostingSource<'a>>) -> Self {
        let mut heads = std::collections::BinaryHeap::with_capacity(sources.len());
        for (index, source) in sources.iter_mut().enumerate() {
            if let Some(first) = source.next() {
                heads.push(std::cmp::Reverse((first, index)));
            }
        }
        Self {
            sources,
            heads,
            previous: None,
        }
    }
}

impl<'a> Iterator for OrderedPostings<'a> {
    type Item = RowPositionRef<'a>;

    fn next(&mut self) -> Option<RowPositionRef<'a>> {
        loop {
            let std::cmp::Reverse((position, index)) = self.heads.pop()?;
            if let Some(following) = self.sources[index].next() {
                debug_assert!(
                    following >= position,
                    "index posting source out of row order"
                );
                self.heads.push(std::cmp::Reverse((following, index)));
            }
            if self.previous == Some(position) {
                continue;
            }
            self.previous = Some(position);
            return Some(position);
        }
    }
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
        let schema = Arc::new(schema);
        let (rotation_tx, rotation_rx) = crossbeam_channel::unbounded();
        let indexes = new_indexes(
            Arc::new(IndexCatalog::with_indexed_columns(indexed_columns)),
            Arc::clone(&schema),
        );
        let initial_view = StoreView {
            active,
            flushing: Arc::new(Vec::new()),
            sstables: Arc::new(vec![]),
            sstable_ids: Arc::new(vec![]),
            indexes,
            sidecar_indexes: Arc::new(vec![]),
            vector_indexes: Arc::new(HashMap::new()),
        };
        initial_view.check_invariants("new:empty");
        Self {
            schema: ArcSwap::new(schema),
            view: Arc::new(ArcSwap::from_pointee(initial_view)),
            rotation_tx,
            rotation_rx,
            rotating: std::sync::atomic::AtomicBool::new(false),
            rotations_started: std::sync::atomic::AtomicU64::new(0),
            last_flush_indexes: ArcSwap::from_pointee(Vec::new()),
            flush_target: Arc::new(flush_target),
            options,
            next_gen: std::sync::atomic::AtomicU64::new(1),
            retired: std::sync::atomic::AtomicBool::new(false),
            vector_index_scopes: ArcSwap::from_pointee(VectorIndexScopes::new()),
            quarantined_sstables: crate::lockfree::SharedSet::new("quarantined SSTables"),
            index_unavailable_sstables: crate::lockfree::SharedSet::new(
                "index-unavailable SSTables",
            ),
            missing_sstables: MissingSstableCache::default(),
            fulltext_sidecars_in_flight: Arc::new(crate::lockfree::SharedSet::new(
                "FTI sidecar builds in flight",
            )),
            vector_ready: crate::lockfree::SharedSet::new("vector-ready generations"),
            vector_verified: crate::lockfree::SharedSet::new("vector-verified generations"),
            vector_invalid_reasons: ArcSwap::from_pointee(HashMap::new()),
            vector_dimensions: ArcSwap::from_pointee(HashMap::new()),
            vector_sidecars_in_flight: Arc::new(crate::lockfree::SharedSet::new(
                "vector sidecar builds in flight",
            )),
            sstable_read_errors: std::sync::atomic::AtomicU64::new(0),
            late_writes_after_seal: std::sync::atomic::AtomicU64::new(0),
            view_retry_exhausted: std::sync::atomic::AtomicU64::new(0),
            reader_pool: Arc::new(crate::reader_pool::ReaderPool::new(
                crate::reader_pool::configured_reader_cache_cap(),
            )),
            pool_table_key: String::new(),
        }
    }

    /// Replace this store's reader pool with a shared engine-wide pool and set
    /// the table key used to namespace generations in that pool.
    ///
    /// Called by the engine right after constructing the store so all tables
    /// share one global resident-reader budget (FMEA #8 — a per-table pool
    /// would bound only `N_tables × cap`).
    pub fn attach_reader_pool(&mut self, pool: SharedReaderPool<F::Reader>, table_key: String) {
        self.reader_pool = pool;
        self.pool_table_key = table_key;
    }

    /// Current number of resident open readers attributable across the shared
    /// pool. Bounded by the pool cap (soft cap when readers are in use).
    pub fn resident_reader_count(&self) -> usize {
        self.reader_pool.resident()
    }

    /// High-water mark of resident readers in the pool (test/metrics gauge).
    pub fn peak_resident_readers(&self) -> usize {
        self.reader_pool.peak_resident()
    }

    /// Pool key for a generation belonging to this table.
    fn pool_key(&self, desc: &SstableDescriptor) -> (String, u64) {
        (self.pool_table_key.clone(), desc.gen_num())
    }

    /// Open (or fetch from the pool) the reader for `desc`. The returned `Arc`
    /// keeps the reader resident for the caller's lifetime; once dropped it
    /// becomes evictable.
    fn open_reader(&self, desc: &SstableDescriptor) -> Result<Arc<SSTableReader<F::Reader>>> {
        let dir = desc.dir.clone();
        let gen = desc.gen_num();
        let key = self.pool_key(desc);
        let flush_target = &self.flush_target;
        self.missing_sstables.open_through(&desc.gen, || {
            self.reader_pool
                .get_or_open(key, || flush_target.open_reader(&dir, gen))
        })
    }

    /// Open (pooled) the readers for every descriptor whose key range overlaps
    /// `[start, end]`, newest-first, returning the `Arc`s so the caller can hand
    /// a `&[Arc<SSTableReader>]` slice to the range merger and keep them alive
    /// for the merge's lifetime.
    ///
    /// Pruning is conservative — a descriptor is skipped only when its
    /// byte-comparable key range is provably disjoint from the requested window
    /// (FMEA #2: never prune away an SSTable that might hold a matching row).
    fn open_readers_for_key_range(
        &self,
        descriptors: &[SstableDescriptor],
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> Result<Vec<Arc<SSTableReader<F::Reader>>>> {
        open_pooled_readers_with(
            &self.reader_pool,
            &self.pool_table_key,
            &*self.flush_target,
            descriptors,
            start,
            end,
            Some(&self.missing_sstables),
        )
    }

    /// Seed the pool with an already-open reader for `desc` (e.g. just-flushed
    /// or just-compacted), so the immediately-following read is a cache hit and
    /// does not reopen freshly-written component files.
    fn seed_reader(&self, desc: &SstableDescriptor, reader: Arc<SSTableReader<F::Reader>>) {
        self.reader_pool.insert_arc(self.pool_key(desc), reader);
        // The generation's object exists now: stop failing it fast.
        self.missing_sstables.forget(&desc.gen);
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
        let schema = Arc::new(schema);
        let (rotation_tx, rotation_rx) = crossbeam_channel::unbounded();
        let indexes = new_indexes(
            Arc::new(IndexCatalog::with_indexed_columns(indexed_columns)),
            Arc::clone(&schema),
        );
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

        // Build the engine-local pool first so we can seed the provided readers.
        let reader_pool: SharedReaderPool<F::Reader> = Arc::new(
            crate::reader_pool::ReaderPool::new(crate::reader_pool::configured_reader_cache_cap()),
        );
        let pool_table_key = String::new();

        // Convert each provided reader into a lightweight descriptor and seed
        // the pool so the immediately-following reads hit the cache instead of
        // reopening. The pool's cap still bounds how many stay resident.
        let descriptors: Vec<SstableDescriptor> = initial_sstables
            .iter()
            .zip(initial_ids.iter())
            .map(|(reader, (gen, dir))| {
                SstableDescriptor::from_reader(gen.clone(), dir.clone(), reader)
            })
            .collect();
        for (reader, desc) in initial_sstables.into_iter().zip(descriptors.iter()) {
            let key = (pool_table_key.clone(), desc.gen_num());
            reader_pool.insert_arc(key, reader);
        }

        let initial_view = StoreView {
            active,
            flushing: Arc::new(Vec::new()),
            sstables: Arc::new(descriptors),
            sstable_ids: Arc::new(initial_ids),
            indexes,
            sidecar_indexes: Arc::new(sidecars),
            vector_indexes: Arc::new(HashMap::new()),
        };
        initial_view.check_invariants("new_with_sstables");
        Self {
            schema: ArcSwap::new(schema),
            view: Arc::new(ArcSwap::from_pointee(initial_view)),
            rotation_tx,
            rotation_rx,
            rotating: std::sync::atomic::AtomicBool::new(false),
            rotations_started: std::sync::atomic::AtomicU64::new(0),
            last_flush_indexes: ArcSwap::from_pointee(Vec::new()),
            flush_target: Arc::new(flush_target),
            options,
            next_gen: std::sync::atomic::AtomicU64::new(1),
            retired: std::sync::atomic::AtomicBool::new(false),
            vector_index_scopes: ArcSwap::from_pointee(VectorIndexScopes::new()),
            quarantined_sstables: crate::lockfree::SharedSet::new("quarantined SSTables"),
            index_unavailable_sstables: crate::lockfree::SharedSet::new(
                "index-unavailable SSTables",
            ),
            missing_sstables: MissingSstableCache::default(),
            fulltext_sidecars_in_flight: Arc::new(crate::lockfree::SharedSet::new(
                "FTI sidecar builds in flight",
            )),
            vector_ready: crate::lockfree::SharedSet::new("vector-ready generations"),
            vector_verified: crate::lockfree::SharedSet::new("vector-verified generations"),
            vector_invalid_reasons: ArcSwap::from_pointee(HashMap::new()),
            vector_dimensions: ArcSwap::from_pointee(HashMap::new()),
            vector_sidecars_in_flight: Arc::new(crate::lockfree::SharedSet::new(
                "vector sidecar builds in flight",
            )),
            sstable_read_errors: std::sync::atomic::AtomicU64::new(0),
            late_writes_after_seal: std::sync::atomic::AtomicU64::new(0),
            view_retry_exhausted: std::sync::atomic::AtomicU64::new(0),
            reader_pool,
            pool_table_key,
        }
    }

    /// Build a `TableStore` from lightweight SSTable *descriptors* rather than
    /// live readers (Phase 5, FMEA #1).
    ///
    /// The startup load path validates each SSTable transiently — open → smoke
    /// test → capture descriptor → drop — so it never materializes O(count) live
    /// readers at once (the observed startup OOM). It hands this constructor only
    /// the resulting descriptors; the engine-wide reader pool reopens readers on
    /// demand afterward, bounded by its cap. Unlike
    /// [`Self::new_with_sstables_and_indexes`], no readers are seeded here: there
    /// are no live readers to seed, which is the whole point.
    pub(crate) fn new_with_descriptors_and_indexes(
        schema: TableSchema,
        flush_target: F,
        options: WriteOptions,
        descriptors: Vec<SstableDescriptor>,
        initial_sidecars: Vec<Arc<HashMap<String, SidecarReader>>>,
        initial_ids: Vec<(String, std::path::PathBuf)>,
        indexed_columns: Vec<(String, usize)>,
    ) -> Self {
        let active: Arc<dyn Memtable> = new_memtable();
        let schema = Arc::new(schema);
        let (rotation_tx, rotation_rx) = crossbeam_channel::unbounded();
        let indexes = new_indexes(
            Arc::new(IndexCatalog::with_indexed_columns(indexed_columns)),
            Arc::clone(&schema),
        );
        let sstable_count = descriptors.len();

        // Pad sidecar list with empty maps if shorter than the SSTable list.
        let mut sidecars: Vec<Arc<HashMap<String, SidecarReader>>> = initial_sidecars;
        while sidecars.len() < sstable_count {
            sidecars.push(Arc::new(HashMap::new()));
        }

        // Fail loud if caller didn't provide a matching IDs vec, mirroring
        // `new_with_sstables_and_indexes` — the parallel-length invariant
        // (`check_invariants`) must hold one (gen_str, dir) per descriptor.
        assert_eq!(
            descriptors.len(),
            initial_ids.len(),
            "new_with_descriptors_and_indexes: descriptors ({}) and initial_ids ({}) \
             must have equal length — one (gen_str, dir) per SSTable descriptor",
            descriptors.len(),
            initial_ids.len()
        );

        let reader_pool: SharedReaderPool<F::Reader> = Arc::new(
            crate::reader_pool::ReaderPool::new(crate::reader_pool::configured_reader_cache_cap()),
        );

        let initial_view = StoreView {
            active,
            flushing: Arc::new(Vec::new()),
            sstables: Arc::new(descriptors),
            sstable_ids: Arc::new(initial_ids),
            indexes,
            sidecar_indexes: Arc::new(sidecars),
            vector_indexes: Arc::new(HashMap::new()),
        };
        initial_view.check_invariants("new_with_descriptors");
        Self {
            schema: ArcSwap::new(schema),
            view: Arc::new(ArcSwap::from_pointee(initial_view)),
            rotation_tx,
            rotation_rx,
            rotating: std::sync::atomic::AtomicBool::new(false),
            rotations_started: std::sync::atomic::AtomicU64::new(0),
            last_flush_indexes: ArcSwap::from_pointee(Vec::new()),
            flush_target: Arc::new(flush_target),
            options,
            next_gen: std::sync::atomic::AtomicU64::new(1),
            retired: std::sync::atomic::AtomicBool::new(false),
            vector_index_scopes: ArcSwap::from_pointee(VectorIndexScopes::new()),
            quarantined_sstables: crate::lockfree::SharedSet::new("quarantined SSTables"),
            index_unavailable_sstables: crate::lockfree::SharedSet::new(
                "index-unavailable SSTables",
            ),
            missing_sstables: MissingSstableCache::default(),
            fulltext_sidecars_in_flight: Arc::new(crate::lockfree::SharedSet::new(
                "FTI sidecar builds in flight",
            )),
            vector_ready: crate::lockfree::SharedSet::new("vector-ready generations"),
            vector_verified: crate::lockfree::SharedSet::new("vector-verified generations"),
            vector_invalid_reasons: ArcSwap::from_pointee(HashMap::new()),
            vector_dimensions: ArcSwap::from_pointee(HashMap::new()),
            vector_sidecars_in_flight: Arc::new(crate::lockfree::SharedSet::new(
                "vector sidecar builds in flight",
            )),
            sstable_read_errors: std::sync::atomic::AtomicU64::new(0),
            late_writes_after_seal: std::sync::atomic::AtomicU64::new(0),
            view_retry_exhausted: std::sync::atomic::AtomicU64::new(0),
            reader_pool,
            pool_table_key: String::new(),
        }
    }

    /// Atomically swap in a new schema. Called when `ALTER TABLE` mutates
    /// the set of columns so that subsequent flushes build the
    /// `SerializationHeader` with up-to-date `num_columns`, avoiding the
    /// writer's out-of-range-col_idx panic
    /// (see bug-sstable-writer-produces-zero-byte-rows-db.md).
    ///
    /// Rows already in the memtable were written under the old schema, so
    /// this is a flush that publishes the new schema at its memtable swap:
    /// see `TableStore::flush_and_update_schema`.
    pub fn update_schema(&self, new_schema: TableSchema) -> Result<()> {
        self.flush_and_update_schema(new_schema, || {})
            .map(|_outcome| ())
    }

    /// The table's current index catalog: the one the active memtable was
    /// created with, and which every write to it posts under.
    fn catalog(&self) -> Arc<IndexCatalog> {
        Arc::clone(&self.view.load().indexes.catalog)
    }

    /// Return a guard over the current schema. Holding the guard keeps the
    /// schema `Arc` alive; `ALTER TABLE` can still swap in a new schema
    /// concurrently.
    pub fn schema(&self) -> arc_swap::Guard<Arc<TableSchema>> {
        self.schema.load()
    }

    /// The current schema as an owned `Arc`, for holders that outlive a
    /// guard or cross threads.
    pub fn schema_arc(&self) -> Arc<TableSchema> {
        self.schema.load_full()
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
    ///
    /// The whole write — index postings and memtable put — runs against ONE
    /// loaded view, admitted through that view's memtable gate, and posts
    /// under the catalog and schema bound to that memtable. A flush publishes
    /// the next memtable before it seals this one, and waits for the writers
    /// inside before it snapshots, so a write never posts into one memtable's
    /// indexes and puts its row into another, never lands in a frozen
    /// memtable after its snapshot, and never posts under a catalog its
    /// memtable's flush will not write sidecars for. A writer that meets a
    /// sealed gate reloads the view and writes to the new memtable; no writer
    /// ever waits for a flush.
    ///
    /// Legacy nanosecond timestamps are normalised to microseconds before the
    /// row reaches the indexes or the memtable (t_cf637b6e), so no producer can
    /// put one where a comparison would see it.
    pub fn write(&self, key: &DecoratedKey, mut row: Row) -> Result<()> {
        normalise_legacy_ns_row(&mut row);
        for _ in 0..MAX_SEALED_MEMTABLE_RETRIES {
            let guard = self.view.load();
            if let Some(_admitted) = guard.indexes.gate.try_enter() {
                return self.write_admitted(&guard, key, row);
            }
            // Sealed: a flush already published the next memtable.
            std::thread::yield_now();
        }
        tracing::error!(
            attempts = MAX_SEALED_MEMTABLE_RETRIES,
            "store: every memtable this write loaded was already sealed; the write is refused"
        );
        Err(ferrosa_common::Error::InvalidData(format!(
            "write refused: met a sealed memtable {MAX_SEALED_MEMTABLE_RETRIES} times in a row"
        )))
    }

    /// [`Self::write`] once it holds an admission to `guard`'s memtable.
    ///
    /// A whole-value (path-less, live) cell on a non-frozen collection column
    /// must parse as a collection, or the write is refused before anything
    /// sees it: a complex-framed flush or compaction later expands that cell
    /// into elements, and a value that cannot be expanded would fail every
    /// flush of the table. Every write producer (CQL, mixed-version forwards,
    /// hints, batchlog, graph writes, commit-log replay) passes through here.
    /// It is checked against the schema bound to the admitted memtable, the
    /// same schema its flush writes under, so a racing `ALTER` cannot pass a
    /// cell under one schema and flush it under another.
    fn write_admitted(&self, guard: &StoreView, key: &DecoratedKey, row: Row) -> Result<()> {
        crate::memtable::validate_legacy_collection_blobs(&row, &guard.indexes.schema)?;
        let catalog = Arc::clone(&guard.indexes.catalog);

        // Secondary index maintenance: extract indexed column values and insert
        // before the memtable put (which consumes the row reference via move).
        if !catalog.indexed_columns.is_empty() {
            for (index_name, col_pos) in &catalog.indexed_columns {
                if let Some(index) = guard.indexes.get(index_name) {
                    insert_scalar_memtable_index_row(
                        index,
                        index_name,
                        *col_pos,
                        catalog.index_type_for(index_name),
                        catalog.index_filter_predicates.get(index_name),
                        key.key.as_bytes(),
                        &row,
                    );
                }
            }
        }

        // Partition-key index maintenance: a partition-key column's value is
        // not a cell either, so it is extracted from the row's composite
        // partition-key bytes at the declared component.
        if !catalog.indexed_partition_key_columns.is_empty() {
            let total = self.partition_key_column_count();
            let components = ferrosa_row_bridge::decode_pk(key, total);
            for (index_name, component) in &catalog.indexed_partition_key_columns {
                let Some(value) = components.get(*component) else {
                    tracing::warn!(
                        index_name,
                        component,
                        total,
                        "store: partition-key index component missing from key bytes"
                    );
                    continue;
                };
                let index_type = catalog.index_type_for(index_name);
                match crate::index::scheduler::encode_index_key(index_type, value) {
                    Ok(Some(index_key)) => {
                        // One posting per PARTITION (t_c5bccc65): every row of
                        // the partition shares this value, so the posting names
                        // the partition and the read streams its rows. A later
                        // write to the same partition posts the same position,
                        // which the index ignores.
                        let row_pos = RowPosition {
                            partition_key: key.key.as_bytes().to_vec(),
                            clustering_key: Vec::new(),
                        };
                        match guard.indexes.get(index_name) {
                            Some(idx) => idx.insert(index_key, row_pos),
                            // Declared but absent from the live map: the write
                            // is not indexed and a later read of this index
                            // would report a short answer as a complete one.
                            None => tracing::error!(
                                index_name,
                                "store: declared partition-key index is missing from the live index \
                                 map; this write is NOT indexed and reads of this index \
                                 will be incomplete"
                            ),
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            index_name,
                            %e,
                            "store: skipping partition-key index entry; key encoding failed"
                        );
                    }
                }
            }
        }

        // Clustering-column index maintenance (t_430c4188): a clustering
        // column's value is not a cell, so it is extracted from the row's
        // composite clustering-key bytes at the declared component.
        if !catalog.indexed_clustering_columns.is_empty() && !row.clustering.is_empty() {
            let total = self.clustering_column_count();
            let components = ferrosa_row_bridge::decode_clustering(&row.clustering, total);
            for (index_name, component) in &catalog.indexed_clustering_columns {
                let Some(value) = components.get(*component) else {
                    tracing::warn!(
                        index_name,
                        component,
                        total,
                        "store: clustering index component missing from row clustering bytes"
                    );
                    continue;
                };
                let index_type = catalog.index_type_for(index_name);
                match crate::index::scheduler::encode_index_key(index_type, value) {
                    Ok(Some(index_key)) => {
                        let row_pos = RowPosition {
                            partition_key: key.key.as_bytes().to_vec(),
                            clustering_key: row.clustering.clone(),
                        };
                        match guard.indexes.get(index_name) {
                            Some(idx) => idx.insert(index_key, row_pos),
                            // Declared but absent from the live map: the write
                            // is not indexed and a later read of this index
                            // would report a short answer as a complete one.
                            None => tracing::error!(
                                index_name,
                                "store: declared clustering index is missing from the live index \
                                 map; this write is NOT indexed and reads of this index \
                                 will be incomplete"
                            ),
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            index_name,
                            %e,
                            "store: skipping clustering memtable index entry; key encoding failed"
                        );
                    }
                }
            }
        }

        // Vector index maintenance: extract vector column values and insert into
        // the in-memory HNSW/brute-force index for the active memtable.
        // Row byte offset is unknown until the SSTable is written; we use 0 as a
        // placeholder. The drain→HNSW build at flush time re-inserts with
        // the final on-disk offset. For now, the memtable search is by position
        // within the memtable (ordering only), not absolute file offset.
        if !catalog.vector_index_configs.is_empty() {
            for cfg in &catalog.vector_index_configs {
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
                                vi.insert_with_scope(
                                    pos,
                                    vector,
                                    Some(key.key.as_bytes().to_vec()),
                                );
                            }
                        }
                        // If bytes_to_vec_f32 fails, the cell contains non-vector
                        // data — skip silently (the schema enforces the type).
                    }
                }
            }
        }

        // The caller holds an admission to this memtable's gate, so the flush
        // that freezes it has not snapshotted it yet: the row lands in the
        // memtable its postings were added for, under that memtable's schema.
        guard.active.put(key, row, &guard.indexes.schema)
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
        self.with_retried_view("read_limited_rows", |view| {
            self.read_with_view(view, key, row_limit, None)
        })
    }

    /// Read a bounded clustering suffix from one partition. Every source drops
    /// rows at or before `start_clustering` before retaining at most
    /// `row_limit`, so the merge never holds the delivered prefix or tail.
    pub fn read_limited_rows_from(
        &self,
        key: &DecoratedKey,
        start_clustering: &[u8],
        row_limit: usize,
    ) -> Result<Option<Partition>> {
        self.with_retried_view("read_limited_rows_from", |view| {
            self.read_with_view(view, key, row_limit, Some(start_clustering))
        })
    }

    /// Run a single-snapshot read `attempt` under the store-view retry policy.
    ///
    /// A read takes an atomic snapshot of the store view. If an SSTable in that
    /// snapshot is concurrently compacted away (its local file deleted, or a
    /// component such as `Filter.db` removed) before we open it, the data has
    /// already moved into a new SSTable in a newer view — so reload and retry
    /// instead of returning a result that silently drops the key (a fail-loud
    /// violation). `attempt` returns `(result, corrupt)` where `corrupt` is
    /// `Some(id)` when a snapshotted SSTable could not be fully consulted; while
    /// it is set we reload the view and retry.
    ///
    /// Retry is **not** gated on the view pointer changing: a deletion that
    /// leaves the live view referencing the same generation (e.g. S3
    /// local-cache eviction) must still be retried so the reopen re-resolves /
    /// re-fetches the file. Bounded by `MAX_VIEW_RETRIES`. If a fresh reopen
    /// against the current view still fails after the bound, the file is
    /// genuinely corrupt (a transient compaction window resolves within the
    /// bound): the SSTable is **quarantined** (so later reads fail fast without
    /// re-opening it and anti-entropy repair can target its range) and the read
    /// **fails loud** with a typed error naming it — never a silent `Ok(None)`
    /// and never a short `Ok(Some)`. A quarantined generation is not skipped by
    /// later reads: each read whose token lies in its range keeps failing until
    /// the generation leaves the view (FMEA ST-56).
    fn with_retried_view<R>(
        &self,
        op: &'static str,
        mut attempt: impl FnMut(&StoreView) -> Result<(Option<R>, Option<CorruptSstableId>)>,
    ) -> Result<Option<R>> {
        const MAX_VIEW_RETRIES: usize = 8;
        let mut previous: Option<Arc<StoreView>> = None;
        for n in 0..=MAX_VIEW_RETRIES {
            let view = self.view.load_full();

            #[cfg(test)]
            if let Some(barrier) = read_race_test_hook::ARMED.with(|c| c.borrow_mut().take()) {
                barrier.reach_and_wait();
            }

            let (result, corrupt) = attempt(&view)?;
            let Some(corrupt) = corrupt else {
                // No snapshotted SSTable failed to be consulted — clean attempt.
                return Ok(result);
            };
            // Another attempt can only help if the view can change: a
            // generation that is already quarantined and still sits in the
            // very view we just read will fail identically, so fail now.
            let pointless = self.is_sstable_quarantined(&corrupt.gen)
                && previous.as_ref().is_some_and(|p| Arc::ptr_eq(p, &view));
            if n < MAX_VIEW_RETRIES && !pointless {
                // A snapshotted SSTable could not be consulted. In the common
                // case this is the transient compaction window: reload and retry
                // against a fresh view. ONLY exhaustion (below) is treated as
                // genuine corruption — a transient window resolves within the
                // bound and never reaches the quarantine/fail-loud path.
                previous = Some(view);
                continue;
            }

            // Still failing: genuine corruption, or the generation's objects
            // are gone. Quarantine it (no retry storm, and anti-entropy repair
            // can target its covered token range) — but NEVER skip it: this
            // read's token lies in the generation's range, so its result could
            // be missing rows or cells that live only there. A key resolved
            // from the memtable or another SSTable is not proof of
            // completeness, so there is no "healthy source" escape: the read
            // fails with the typed error and the coordinator fails over to a
            // replica. See FMEA ST-56.
            self.view_retry_exhausted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let newly_quarantined = self.quarantine_sstable(&corrupt);
            if newly_quarantined {
                tracing::error!(
                    op,
                    corrupt = %corrupt,
                    retries = n,
                    resolved_locally = result.is_some(),
                    "read overlaps an unreadable SSTable — failing loud rather than \
                     returning a possibly incomplete result (quarantined; reads over \
                     its token range keep failing until repair, a restore or a \
                     compaction removes it from the view)"
                );
            } else {
                // Already reported when it was quarantined; this fires once
                // per read, so it stays below ERROR (edges, not events).
                tracing::debug!(
                    op,
                    corrupt = %corrupt,
                    "read refused: it overlaps an already-quarantined SSTable"
                );
            }
            // Typed signal (never string-matched): carries the corrupt
            // SSTable's generation and covered token range so the read
            // coordinator can fail over to a replica and target anti-entropy
            // repair at exactly that range.
            return Err(ferrosa_common::Error::corrupt_sstable(
                corrupt.gen.clone(),
                corrupt.min_token,
                corrupt.max_token,
            ));
        }
        unreachable!("read retry loop returns within MAX_VIEW_RETRIES")
    }

    /// Run a range/scan `attempt` under the fresh-view retry policy.
    ///
    /// Unlike a point read, a range read has no "healthy source resolved it"
    /// escape: a missing SSTable means missing rows, which reads as the rows
    /// not existing. So an SSTable that cannot be opened or read
    /// ([`Self::unreadable_sstable`], surfaced as a typed
    /// [`ferrosa_common::Error::CorruptSstable`]) is retried against a freshly
    /// loaded view — the transient compaction window retires the input and the
    /// merged output serves the rows — and once the bound is exhausted the
    /// SSTable is quarantined for anti-entropy repair and the typed error is
    /// returned. Never a partial `Ok`.
    ///
    /// `attempt` must load its own view (so each retry sees a fresh one) and
    /// must not deliver results to its caller before it can fail with this
    /// error: every SSTable is opened before any row is produced.
    fn with_retried_scan<R>(
        &self,
        op: &'static str,
        attempt: impl FnMut() -> Result<R>,
    ) -> Result<R> {
        self.with_retried_scan_guarded(op, &std::cell::Cell::new(false), attempt)
    }

    /// [`Self::with_retried_scan`] for streaming walks that hand rows to a
    /// callback. Once `delivered` is set the attempt has already produced
    /// output, so a retry would deliver rows twice: a failure from then on is
    /// final (quarantine and error immediately).
    fn with_retried_scan_guarded<R>(
        &self,
        op: &'static str,
        delivered: &std::cell::Cell<bool>,
        mut attempt: impl FnMut() -> Result<R>,
    ) -> Result<R> {
        const MAX_VIEW_RETRIES: usize = 8;
        let mut retries = 0;
        loop {
            let err = match attempt() {
                Ok(v) => return Ok(v),
                Err(e) => e,
            };
            let ferrosa_common::Error::CorruptSstable {
                ref gen,
                min_token,
                max_token,
            } = err
            else {
                return Err(err);
            };
            if retries < MAX_VIEW_RETRIES && !delivered.get() {
                retries += 1;
                #[cfg(test)]
                read_race_test_hook::ON_SCAN_RETRY.with(|c| {
                    if let Some(hook) = c.borrow_mut().as_mut() {
                        hook();
                    }
                });
                continue;
            }
            self.view_retry_exhausted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let corrupt = CorruptSstableId {
                gen: gen.clone(),
                dir: std::path::PathBuf::new(),
                min_token,
                max_token,
            };
            self.quarantine_sstable(&corrupt);
            tracing::error!(
                op,
                corrupt = %corrupt,
                retries = MAX_VIEW_RETRIES,
                "range read exhausted view retries with an unreadable SSTable — failing \
                 loud rather than returning a partial result (quarantined for repair)"
            );
            return Err(err);
        }
    }

    /// Failure reading a [`MergeReader`]: typed (retriable, quarantinable) when
    /// it serves a table SSTable, the raw error for a spill run (our own
    /// scratch file — not retriable against a fresh view).
    fn merge_reader_failure(
        &self,
        op: &'static str,
        stage: &'static str,
        mr: &MergeReader<F>,
        cause: ferrosa_common::Error,
    ) -> ferrosa_common::Error {
        match mr.source.as_ref() {
            Some(desc) => self.unreadable_sstable(op, stage, desc, &cause),
            None => cause,
        }
    }

    /// Build the typed error for an SSTable that belongs to a range read's
    /// view but could not be opened or read. Logs and meters the failure;
    /// the caller returns the error, and [`Self::with_retried_scan`] decides
    /// whether to retry against a fresh view or give up.
    fn unreadable_sstable(
        &self,
        op: &'static str,
        stage: &'static str,
        desc: &SstableDescriptor,
        cause: &ferrosa_common::Error,
    ) -> ferrosa_common::Error {
        tracing::warn!(
            op,
            stage,
            gen = %desc.gen,
            %cause,
            "SSTable in the read's view could not be consulted; retrying against a fresh \
             view, then failing the read (a range read never returns a partial result)"
        );
        self.sstable_read_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ferrosa_common::Error::corrupt_sstable(desc.gen.clone(), desc.min_token, desc.max_token)
    }

    /// Record `corrupt`'s generation in the quarantine set. Idempotent: a
    /// generation already present is left as-is. Returns whether it was newly
    /// inserted, so callers report the edge once rather than per read.
    ///
    /// Quarantine does NOT hide the generation from reads: every read whose
    /// token range overlaps it fails with the typed error until the
    /// generation leaves the view (FMEA ST-56). What it buys is that those
    /// reads fail fast instead of re-opening the file on each attempt.
    fn quarantine_sstable(&self, corrupt: &CorruptSstableId) -> bool {
        let inserted = match self.quarantined_sstables.insert(&corrupt.gen) {
            Ok(inserted) => inserted,
            Err(e) => {
                // The read that found it still fails with the typed error;
                // only the fast-fail on later reads is lost.
                tracing::error!(corrupt = %corrupt, %e, "could not record an SSTable quarantine");
                false
            }
        };
        if inserted {
            tracing::warn!(
                corrupt = %corrupt,
                "quarantined unreadable SSTable: reads over its token range fail with a typed \
                 error until anti-entropy repair, a restore or a compaction removes it from \
                 the view"
            );
        }
        inserted
    }

    /// Record that `gen`'s sidecars could not all be opened at load. Index
    /// consults over a view holding it fail with the typed corrupt-SSTable
    /// error; data reads are unaffected (FMEA ST-59).
    pub(crate) fn mark_index_unavailable(&self, gen: &str) {
        let inserted = match self.index_unavailable_sstables.insert(gen) {
            Ok(inserted) => inserted,
            Err(e) => {
                tracing::error!(
                    gen,
                    %e,
                    "could not record that this SSTable's index sidecars are unavailable; \
                     index consults over it are NOT refused"
                );
                false
            }
        };
        if inserted {
            tracing::error!(
                gen,
                "secondary-index sidecars of this SSTable could not be opened; index consults \
                 over it fail until it is rebuilt, repaired or compacted away"
            );
        }
    }

    /// Mark `gen`'s quarantine resolved (repair refilled or restored it): it
    /// leaves the quarantine set and the negative open cache, so the next read
    /// probes it for real. Returns whether it was quarantined.
    pub fn resolve_sstable_quarantine(&self, gen: &str) -> bool {
        self.missing_sstables.forget(gen);
        match self.quarantined_sstables.remove(gen) {
            Ok(removed) => removed,
            Err(e) => {
                tracing::error!(gen, %e, "could not clear an SSTable quarantine; it stays quarantined");
                false
            }
        }
    }

    /// Open attempts that failed for real (reached storage), not fast-failed.
    pub fn missing_sstable_open_failures(&self) -> u64 {
        self.missing_sstables
            .open_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Opens refused immediately because the generation recently failed to
    /// open. Each still surfaced to its caller as an error.
    pub fn missing_sstable_fast_fails(&self) -> u64 {
        self.missing_sstables
            .fast_fails
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether `gen` is currently quarantined (reads overlapping it fail fast).
    /// Used by the repair path to target the SSTable's range and by tests.
    pub fn is_sstable_quarantined(&self, gen: &str) -> bool {
        self.quarantined_sstables.contains(gen)
    }

    /// Snapshot of all currently-quarantined SSTable generations. The
    /// anti-entropy repair scheduler drains this to learn which ranges to
    /// refill from a healthy replica.
    pub fn quarantined_sstable_gens(&self) -> Vec<String> {
        self.quarantined_sstables.to_vec()
    }

    /// Refuse an index consult while a generation overlapping it is
    /// quarantined. `token` narrows the check to one partition's token; `None`
    /// means the consult spans every token, so ANY quarantined generation in
    /// `view` counts.
    ///
    /// An index's postings come from sidecars, which are not the SSTables the
    /// rows live in. A generation whose objects are lost can leave its
    /// sidecar readable (or absent) and the postings pointing at rows the
    /// store can no longer read: the consult then reports zero or fewer rows
    /// as success. Refuse it the way a stale index is refused (FMEA ST-56).
    fn refuse_index_read_over_quarantine(
        &self,
        view: &StoreView,
        token: Option<i64>,
        op: &'static str,
    ) -> Result<()> {
        if self.quarantined_sstables.is_empty() && self.index_unavailable_sstables.is_empty() {
            return Ok(());
        }
        let overlapping = view.sstables.iter().find(|desc| {
            (self.is_sstable_quarantined(&desc.gen)
                || self.index_unavailable_sstables.contains(&desc.gen))
                && token.is_none_or(|t| t >= desc.min_token && t <= desc.max_token)
        });
        let Some(desc) = overlapping else {
            return Ok(());
        };
        tracing::debug!(
            op,
            gen = %desc.gen,
            "index read refused: a quarantined SSTable overlaps it, so its result could be short"
        );
        Err(ferrosa_common::Error::corrupt_sstable(
            desc.gen.clone(),
            desc.min_token,
            desc.max_token,
        ))
    }

    /// The table-level tombstone currently visible in `guard`, or
    /// [`DeletionTime::LIVE`] if the table has not been truncated — together with
    /// the identity of the first SSTable that could not be consulted while
    /// resolving it.
    ///
    /// A TRUNCATE is stored as one reserved partition
    /// ([`crate::table_tombstone::table_tombstone_key`]) whose `deletion` is the
    /// table tombstone. This folds that partition's deletion across the active
    /// memtable, every flushing memtable and every SSTable whose token range covers
    /// the reserved key, returning the newest.
    ///
    /// An overlapping SSTable that cannot be opened or read is NOT a hard error
    /// here. A missing tombstone would resurrect truncated data, so no caller may
    /// proceed on a partial answer — but the ordinary cause is the transient
    /// compaction window (the input was retired and its merged output lives in a
    /// newer view), which is exactly what the callers' fresh-view retry exists to
    /// absorb. So the unconsultable descriptor is returned as a
    /// [`CorruptSstableId`] for the caller to fold into the retry signal it already
    /// uses for its own sources; on retry exhaustion that same path quarantines the
    /// generation and fails the read loud. `Err` is reserved for a failure no fresh
    /// view can fix (a memtable fault).
    ///
    /// Only SSTables whose `[min_token, max_token]` covers the reserved key are
    /// opened, and a Bloom check (`may_contain_key`) skips the rest, so this is a
    /// bounded, single-key probe on a table that has ever been truncated.
    fn table_deletion(
        &self,
        guard: &StoreView,
    ) -> Result<(DeletionTime, Option<CorruptSstableId>)> {
        let key = crate::table_tombstone::table_tombstone_key();
        let mut best = DeletionTime::LIVE;
        let mut corrupt: Option<CorruptSstableId> = None;
        if let Some(p) = guard.active.get(&key)? {
            best = crate::table_tombstone::newest_deletion(best, p.deletion);
        }
        for sealed in guard.flushing.iter() {
            if let Some(p) = sealed.memtable.get(&key)? {
                best = crate::table_tombstone::newest_deletion(best, p.deletion);
            }
        }
        let token = key.token.0;
        for desc in guard.sstables.iter() {
            if token < desc.min_token || token > desc.max_token {
                continue;
            }
            // A quarantined generation is never skipped: its tombstone, if any,
            // cannot be assumed absent. Report it without re-opening the file and
            // let the caller retry onto a view that no longer holds it, or fail.
            if self.is_sstable_quarantined(&desc.gen) {
                corrupt.get_or_insert_with(|| CorruptSstableId::from_descriptor(desc));
                continue;
            }
            let reader = match self.open_reader(desc) {
                Ok(r) => r,
                Err(e) => {
                    self.unreadable_sstable("table_tombstone", "open", desc, &e);
                    corrupt.get_or_insert_with(|| CorruptSstableId::from_descriptor(desc));
                    continue;
                }
            };
            if !reader.may_contain_key(&key) {
                continue;
            }
            match reader.get_partition(&key) {
                Ok(Some(p)) => best = crate::table_tombstone::newest_deletion(best, p.deletion),
                Ok(None) => {}
                Err(e) => {
                    self.unreadable_sstable("table_tombstone", "read", desc, &e);
                    corrupt.get_or_insert_with(|| CorruptSstableId::from_descriptor(desc));
                }
            }
        }
        Ok((best, corrupt))
    }

    /// The table tombstone as of the store's current view, or
    /// [`DeletionTime::LIVE`]. The store-internal entry point for callers outside
    /// this module (compaction, which must reclaim rows a truncate covers).
    ///
    /// Unlike the read paths, a caller here must not reclaim against a tombstone it
    /// could not fully resolve: dropping rows on a partial answer risks deleting
    /// live data, so an unconsultable overlapping SSTable stays a hard error.
    pub(crate) fn table_tombstone(&self) -> Result<DeletionTime> {
        match self.table_deletion(&self.view.load())? {
            (delete, None) => Ok(delete),
            (_, Some(c)) => Err(ferrosa_common::Error::corrupt_sstable(
                c.gen,
                c.min_token,
                c.max_token,
            )),
        }
    }

    /// One attempt of [`read_limited_rows`] against a fixed `view` snapshot.
    /// Returns the merged partition (if any) plus the identity of any
    /// snapshotted SSTable that could not be consulted — the signal the caller
    /// uses to retry against a fresh view (transient compaction swap) and, on
    /// retry exhaustion, to quarantine the corrupt file and target repair.
    fn read_with_view(
        &self,
        guard: &StoreView,
        key: &DecoratedKey,
        row_limit: usize,
        start_clustering: Option<&[u8]>,
    ) -> Result<(Option<Partition>, Option<CorruptSstableId>)> {
        let started = Instant::now();
        let schema = self.schema.load();
        // Identity of a snapshotted SSTable that could not be consulted this
        // attempt. `None` = clean; `Some` drives the retry, then (on exhaustion)
        // quarantine + fail-loud in `with_retried_view`.
        let mut corrupt: Option<CorruptSstableId> = None;

        let mut sources: Vec<Partition> = Vec::new();
        let mut memtable_hits = 0u64;
        let mut flushing_hits = 0u64;
        let mut sstable_pruned = 0u64;
        let mut sstable_probes = 0u64;
        let mut sstable_hits = 0u64;
        let mut sstable_errors = 0u64;

        let sstable_fanout = guard.sstables.len();
        let high_fanout = sstable_fanout > READ_FANOUT_ALERT_SSTABLES;
        crate::metrics::observe_read_sstable_fanout(sstable_fanout, high_fanout);
        if high_fanout {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or(0);
            let last = LAST_READ_FANOUT_ERROR_UNIX_SECS.load(std::sync::atomic::Ordering::Relaxed);
            if now.saturating_sub(last) >= READ_FANOUT_ERROR_INTERVAL_SECS
                && LAST_READ_FANOUT_ERROR_UNIX_SECS
                    .compare_exchange(
                        last,
                        now,
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .is_ok()
            {
                tracing::error!(
                    table = %self.pool_table_key,
                    sstable_fanout,
                    alert_threshold = READ_FANOUT_ALERT_SSTABLES,
                    reader_pool_capacity = self.reader_pool.capacity(),
                    "partition read exceeded the SSTable fanout operational bound; \
                     continuing through the bounded reader pool while compaction drains backlog"
                );
            }
        }

        // Active memtable
        if let Some(p) = guard.active.get(key)? {
            memtable_hits += 1;
            sources.push(clone_partition_limited(&p, start_clustering, row_limit));
        }

        // Flushing memtable
        for flushing in guard.flushing.iter().map(|sealed| &sealed.memtable) {
            if let Some(p) = flushing.get(key)? {
                flushing_hits += 1;
                sources.push(clone_partition_limited(&p, start_clustering, row_limit));
            }
        }

        // SSTables, newest first.
        // Tolerate I/O errors from individual SSTables — a corrupt or
        // format-incompatible SSTable should not prevent reading data
        // that exists in other SSTables or the memtable (FRSA-BUG-026).
        for (i, desc) in guard.sstables.iter().enumerate() {
            // A quarantined generation is never skipped (FMEA ST-56): when this
            // key's token lies in its range the read cannot be known complete,
            // so report it without re-opening the file and let
            // `with_retried_view` fail the read (or retry onto a view that no
            // longer holds it).
            if self.is_sstable_quarantined(&desc.gen)
                && key.token.0 >= desc.min_token
                && key.token.0 <= desc.max_token
            {
                corrupt = Some(CorruptSstableId::from_descriptor(desc));
                continue;
            }
            // Token-prune by descriptor bounds first (no reader open). The
            // partition's token must lie within the SSTable's covered range or
            // it cannot hold the key.
            let token = key.token.0;
            if token < desc.min_token || token > desc.max_token {
                sstable_pruned += 1;
                continue;
            }
            // In range — open the reader (pooled) and bloom-check precisely.
            let sstable = match self.open_reader(desc) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(%e, gen = %desc.gen, "point read: failed to open SSTable reader");
                    self.sstable_read_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    sstable_errors += 1;
                    corrupt = Some(CorruptSstableId::from_descriptor(desc));
                    continue;
                }
            };
            if !sstable.may_contain_key(key) {
                sstable_pruned += 1;
                continue;
            }
            sstable_probes += 1;
            let source_read = match start_clustering {
                Some(start) => sstable.get_partition_limited_rows_from(key, start, row_limit),
                None => sstable.get_partition_limited_rows(key, row_limit),
            };
            match source_read {
                Ok(Some(mut p)) => {
                    sstable_hits += 1;
                    ColumnOrdinalMapping::for_header(&schema, sstable.header())
                        .remap_partition(&mut p);
                    sources.push(p);
                }
                Ok(None) => {}
                Err(e) => {
                    // A mid-read error after a SUCCESSFUL open is the residual
                    // read-vs-compaction window: the descriptor came from a view
                    // snapshot that a concurrent compaction has since swapped out,
                    // and `evict_local_input_sstable_files` deleted this input's
                    // `Data.db` *between* our open (cached reader) and our seek —
                    // so the index/bloom said "present" but the partition fetch
                    // hits `ENOENT`. The merged output in the NEW view holds the
                    // row, so this must drive the same view-retry as an open
                    // failure rather than silently dropping the key (`Ok(None)` =
                    // data loss). Recording the failing descriptor engages
                    // `with_retried_view`: a transient compaction window resolves
                    // on retry against the fresh view; a genuinely corrupt SSTable
                    // re-errors across all retries and is quarantined + surfaced
                    // loudly (never silently masked).
                    corrupt = Some(CorruptSstableId::from_descriptor(desc));
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
                        "SSTable read error after successful open: retrying against a fresh \
                         view (likely compaction deleted this input mid-read); data may be \
                         incomplete only if the error persists across all retries"
                    );
                    self.sstable_read_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    sstable_errors += 1;
                }
            }
        }

        if sources.is_empty() {
            crate::metrics::observe_read_limited_rows(
                started.elapsed(),
                false,
                memtable_hits,
                flushing_hits,
                sstable_pruned,
                sstable_probes,
                sstable_hits,
                sstable_errors,
            );
            return Ok((None, corrupt));
        }

        let mut merged = merge::merge_partitions(sources);
        // Resolving the reserved-key tombstone can meet an SSTable the sources
        // above never opened; an unconsultable one drives the same fresh-view
        // retry rather than failing the read on a transient compaction window.
        let (table_delete, probe_corrupt) = self.table_deletion(guard)?;
        corrupt = corrupt.or(probe_corrupt);
        merge::apply_table_deletion(&mut merged, table_delete);
        if row_limit > 0 {
            merge::apply_deletions(&mut merged);
            merged.rows.truncate(row_limit);
        }
        crate::metrics::observe_read_limited_rows(
            started.elapsed(),
            true,
            memtable_hits,
            flushing_hits,
            sstable_pruned,
            sstable_probes,
            sstable_hits,
            sstable_errors,
        );
        Ok((Some(merged), corrupt))
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
        // Route through the shared store-view retry policy so a stale-view
        // SSTable open failure (compaction swap or local-cache eviction) retries
        // against a fresh view instead of silently returning `Ok(None)` — the
        // same data-loss class as a partition read. This path is a production
        // hot path (full primary-key CQL point reads).
        self.with_retried_view("read_clustering_row", |view| {
            self.read_clustering_row_with_view(view, key, clustering)
        })
    }

    /// One attempt of [`read_clustering_row`] against a fixed `view` snapshot.
    /// Returns the matching row (if any) plus the identity of any snapshotted
    /// SSTable that could not be consulted — the signal the caller uses to retry
    /// against a fresh view and, on exhaustion, to quarantine + repair.
    fn read_clustering_row_with_view(
        &self,
        guard: &StoreView,
        key: &DecoratedKey,
        clustering: &[u8],
    ) -> Result<(Option<Partition>, Option<CorruptSstableId>)> {
        let schema = self.schema.load();
        let mut sources: Vec<Partition> = Vec::new();
        let mut corrupt: Option<CorruptSstableId> = None;

        if let Some(p) = guard.active.get(key)? {
            if let Some(filtered) = partition_with_matching_clustering(&p, clustering) {
                sources.push(filtered);
            }
        }

        for flushing in guard.flushing.iter().map(|sealed| &sealed.memtable) {
            if let Some(p) = flushing.get(key)? {
                if let Some(filtered) = partition_with_matching_clustering(&p, clustering) {
                    sources.push(filtered);
                }
            }
        }

        for (i, desc) in guard.sstables.iter().enumerate() {
            // A quarantined generation is never skipped (see `read_with_view`).
            if self.is_sstable_quarantined(&desc.gen)
                && key.token.0 >= desc.min_token
                && key.token.0 <= desc.max_token
            {
                corrupt = Some(CorruptSstableId::from_descriptor(desc));
                continue;
            }
            let token = key.token.0;
            if token < desc.min_token || token > desc.max_token {
                continue;
            }
            let sstable = match self.open_reader(desc) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(%e, gen = %desc.gen, "clustering read: failed to open SSTable reader");
                    self.sstable_read_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    corrupt = Some(CorruptSstableId::from_descriptor(desc));
                    continue;
                }
            };
            if !sstable.may_contain_key(key) {
                continue;
            }
            match sstable.get_clustering_row(key, clustering) {
                Ok(Some(mut p)) => {
                    ColumnOrdinalMapping::for_header(&schema, sstable.header())
                        .remap_partition(&mut p);
                    sources.push(p);
                }
                Ok(None) => {}
                Err(e) => {
                    // Mid-read error after a successful open: same residual
                    // read-vs-compaction window as the partition-read path —
                    // a concurrent compaction deleted this input's `Data.db`
                    // between our (cached) open and the row seek, so the row
                    // lives in the merged SSTable of the NEW view. Drive the
                    // view-retry instead of silently dropping the row.
                    corrupt = Some(CorruptSstableId::from_descriptor(desc));
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
                        "SSTable exact clustering read error after successful open: retrying \
                         against a fresh view (likely compaction deleted this input mid-read)"
                    );
                    self.sstable_read_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        if sources.is_empty() {
            return Ok((None, corrupt));
        }

        let mut merged = merge::merge_partitions(sources);
        let (table_delete, probe_corrupt) = self.table_deletion(guard)?;
        corrupt = corrupt.or(probe_corrupt);
        merge::apply_table_deletion(&mut merged, table_delete);
        merged.rows.retain(|row| row.clustering == clustering);
        if merged.rows.is_empty() {
            Ok((None, corrupt))
        } else {
            Ok((Some(merged), corrupt))
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
    ///
    /// An SSTable covering the partition that cannot be opened or read fails
    /// the visit (typed [`ferrosa_common::Error::CorruptSstable`]); a window
    /// recomputed from a subset of its sources would be silently wrong.
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
        let delivered = std::cell::Cell::new(false);
        self.with_retried_scan_guarded("visit_time_series_window_rows", &delivered, || {
            self.visit_time_series_window_rows_once(
                key,
                window_start_ts,
                window_end_ts,
                timestamp_unit,
                &delivered,
                &mut cb,
            )
        })
    }

    fn visit_time_series_window_rows_once<Cb>(
        &self,
        key: &DecoratedKey,
        window_start_ts: i64,
        window_end_ts: i64,
        timestamp_unit: crate::timeseries::TimeSeriesTimestampUnit,
        delivered: &std::cell::Cell<bool>,
        mut cb: Cb,
    ) -> Result<usize>
    where
        Cb: FnMut(&Row) -> Result<()>,
    {
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
        for flushing in guard.flushing.iter().map(|sealed| &sealed.memtable) {
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

        // Open readers (pooled) for descriptors whose token range covers the
        // key. Hold the `Arc`s for the lifetime of the borrowed iterators so
        // the pool cannot evict a reader mid-scan (FMEA #5/#10).
        let token = key.token.0;
        let mut sst_readers: Vec<(Arc<SSTableReader<F::Reader>>, &SstableDescriptor)> = Vec::new();
        for desc in guard.sstables.iter() {
            if token < desc.min_token || token > desc.max_token {
                continue;
            }
            let reader = self.open_reader(desc).map_err(|e| {
                self.unreadable_sstable("visit_time_series_window_rows", "open", desc, &e)
            })?;
            sst_readers.push((reader, desc));
        }

        let mut sst_sources: Vec<(
            ferrosa_sstable::reader::PartitionIter<'_, F::Reader>,
            ColumnOrdinalMapping,
        )> = Vec::new();
        for (sstable, desc) in sst_readers.iter() {
            let mut iter = sstable.partitions_iter().map_err(|e| {
                self.unreadable_sstable("visit_time_series_window_rows", "iter", desc, &e)
            })?;
            // Walk partition metadata until the exact key instead of using
            // `seek_to_token`; this cursor only needs one partition and must
            // not depend on the SSTable token-offset cache hot path.
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
                if peeked.token > key.token || (peeked.token == key.token && peeked > *key) {
                    break;
                }
                let _ = iter.next_partition_metadata()?;
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
                    delivered.set(true);
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
    /// 1. Queue the rotation; one thread runs the queue (see `TableStore::rotate`).
    /// 2. Install a fresh active memtable; move the old one to `flushing`.
    /// 3. Snapshot the flushing memtable.
    /// 4. If the snapshot is empty, clear `flushing` and return (no-op).
    /// 5. Build the SSTable via [`SSTableWriter`] and [`FlushTarget::flush`].
    /// 6. Prepend the new reader to the SSTable list and clear `flushing`.
    pub fn flush(&self) -> Result<()> {
        self.flush_with_swap_callback(|| {}).map(|_outcome| ())
    }

    /// Flushes while notifying the owner immediately after the active
    /// memtable has been swapped for a fresh one. The completion result still
    /// carries the durability outcome; this callback only releases
    /// backpressure waiters that depend on active-memtable capacity.
    ///
    /// Returns [`FlushOutcome::Published`] only when this call installed a new
    /// SSTable. A flush with nothing to write (empty memtable, or every row
    /// quarantined) returns [`FlushOutcome::NothingToFlush`]: no SSTable
    /// exists for it, and [`Self::last_flush_generation`] still names an
    /// EARLIER flush (or 0 if this process has never flushed the table), so a
    /// caller must not treat that generation as freshly written.
    pub(crate) fn flush_with_swap_callback(
        &self,
        on_memtable_release: impl FnOnce() + Send + 'static,
    ) -> Result<FlushOutcome> {
        self.rotate(RotationKind::Flush {
            on_release: Box::new(on_memtable_release),
        })
    }

    /// Flush every row written under the current schema, then publish
    /// `new_schema`, with no write admitted in between (`ALTER TABLE`).
    ///
    /// The memtable swap and the schema swap are one step: the frozen memtable
    /// is serialized with the schema and catalog it was written under, and the
    /// new memtable starts empty under the new ones. So every row in the
    /// flushed SSTable carries the pre-ALTER layout its header describes, and
    /// every later row enters a memtable that will flush under the new one.
    /// This used to depend on the engine holding its table-map write lock
    /// across `flush` and `update_schema`; that lock is gone (t_d938e6ae).
    pub(crate) fn flush_and_update_schema(
        &self,
        new_schema: TableSchema,
        on_memtable_release: impl FnOnce() + Send + 'static,
    ) -> Result<FlushOutcome> {
        self.rotate(RotationKind::Schema {
            schema: new_schema,
            on_release: Box::new(on_memtable_release),
        })
    }

    /// Change the table's index catalog by rotating the memtable: `edit`
    /// derives the new catalog from the current one (or returns `None` to
    /// change nothing), the active memtable is frozen and flushed, and a new
    /// empty one bound to the new catalog takes writes.
    ///
    /// This is how index DDL stays lock-free and exact. A live memtable's
    /// indexes are never edited, so no write can post under a catalog its
    /// memtable does not carry, and every index of a memtable is complete for
    /// it — the new memtable starts empty, and the frozen one gets sidecars for
    /// the NEW catalog built from its own postings, or from its now-immutable
    /// rows for an index the edit added (see `flush_rotating`). `edit` runs at
    /// the swap against the latest catalog, and rotations run one at a time
    /// in queue order, so concurrent DDLs cannot lose one another's changes.
    ///
    /// Returns what the rotation's flush did. A published generation's
    /// sidecars already cover every index of the new catalog (see
    /// [`Self::indexes_written_by_last_flush`]), so it needs no backfill build
    /// for an index the edit added. The caller owns the engine-side
    /// bookkeeping every flush needs.
    pub(crate) fn flush_applying_catalog_edit(
        &self,
        edit: impl FnOnce(&IndexCatalog) -> Option<IndexCatalog> + Send + 'static,
    ) -> Result<FlushOutcome> {
        self.rotate(RotationKind::Edit(Box::new(edit)))
    }

    /// Run one memtable rotation, with no lock (t_d938e6ae).
    ///
    /// Rotations of one table must happen one at a time: each swaps the
    /// active memtable, and a catalog edit derives from the catalog the
    /// previous rotation left. That used to be a per-table `Mutex` every
    /// flush, DDL and truncate queued on. Now the request goes onto a queue,
    /// and whichever caller claims the `rotating` flag by compare-and-swap
    /// runs everything queued, in order, for everyone (flat combining):
    /// flushes, edits and schema changes queued together are ONE rotation.
    /// The others wait for their own answer, never for a lock, and take over
    /// the queue if the combiner stops.
    ///
    /// # Errors
    ///
    /// The rotation's own error, or a loud error if no answer arrives within
    /// [`ROTATION_WAIT_LIMIT`].
    fn rotate(&self, kind: RotationKind) -> Result<FlushOutcome> {
        let (reply, answer) = crossbeam_channel::bounded(1);
        let request = RotationRequest {
            kind,
            enqueued_at: Instant::now(),
            reply,
        };
        if self.rotation_tx.send(request).is_err() {
            // The store holds the receiver, so this cannot happen while
            // `self` is alive.
            return Err(ferrosa_common::Error::InvalidData(
                "rotation queue closed".to_string(),
            ));
        }
        let start = Instant::now();
        while start.elapsed() < ROTATION_WAIT_LIMIT {
            self.combine_rotations();
            match answer.recv_timeout(ROTATION_POLL) {
                Ok(result) => return result,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    tracing::error!("store: a rotation was dropped without an answer");
                    return Err(ferrosa_common::Error::InvalidData(
                        "the rotation was dropped without an answer (its combiner panicked)"
                            .to_string(),
                    ));
                }
            }
        }
        tracing::error!(
            waited_s = start.elapsed().as_secs(),
            "store: no answer for a queued rotation; it may still run later"
        );
        Err(ferrosa_common::Error::InvalidData(format!(
            "no answer for a queued memtable rotation after {ROTATION_WAIT_LIMIT:?}"
        )))
    }

    /// If no other thread is running the rotation queue, run it.
    fn combine_rotations(&self) {
        for _ in 0..MAX_COMBINE_ROUNDS {
            if self
                .rotating
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                )
                .is_err()
            {
                return;
            }
            let slot = CombinerSlot(&self.rotating);
            let batch: Vec<RotationRequest> = self.rotation_rx.try_iter().collect();
            self.run_rotation_batch(batch);
            drop(slot);
            // A request queued after the drain but before the flag cleared
            // found the flag set; pick it up rather than strand it.
            if self.rotation_rx.is_empty() {
                return;
            }
        }
    }

    /// Run one drained batch in queue order: consecutive flushes, edits and
    /// schema changes as one rotation; a truncate or barrier on its own.
    fn run_rotation_batch(&self, batch: Vec<RotationRequest>) {
        let mut group = Vec::new();
        for request in batch {
            match request.kind {
                RotationKind::Truncate | RotationKind::Barrier => {
                    if !group.is_empty() {
                        self.run_rotation_group(std::mem::take(&mut group));
                    }
                    let result = match request.kind {
                        RotationKind::Truncate => self.truncate_now(),
                        _ => Ok(FlushOutcome::NothingToFlush),
                    };
                    Self::answer(&request.reply, result);
                }
                _ => group.push(request),
            }
        }
        if !group.is_empty() {
            self.run_rotation_group(group);
        }
    }

    /// Send `result` to one waiting caller. The caller may have given up
    /// (see [`ROTATION_WAIT_LIMIT`]); its answer is then only logged.
    fn answer(
        reply: &crossbeam_channel::Sender<Result<FlushOutcome>>,
        result: Result<FlushOutcome>,
    ) {
        if let Err(unsent) = reply.send(result) {
            tracing::warn!(
                result = ?unsent.0,
                "store: a rotation finished after its caller stopped waiting"
            );
        }
    }

    /// Run queued flushes, edits and schema changes as ONE memtable rotation.
    fn run_rotation_group(&self, group: Vec<RotationRequest>) {
        let oldest = group
            .iter()
            .map(|request| request.enqueued_at)
            .min()
            .unwrap_or_else(Instant::now);
        crate::metrics::observe_flush_phase(crate::metrics::FlushPhase::LockWait, oldest.elapsed());
        let mut replies = Vec::with_capacity(group.len());
        let mut changes = Vec::with_capacity(group.len());
        let mut releases: Vec<Box<dyn FnOnce() + Send>> = Vec::new();
        for request in group {
            replies.push(request.reply);
            match request.kind {
                RotationKind::Flush { on_release } => releases.push(on_release),
                RotationKind::Edit(edit) => changes.push(RotationChange::Edit(edit)),
                RotationKind::Schema { schema, on_release } => {
                    changes.push(RotationChange::Schema(schema));
                    releases.push(on_release);
                }
                RotationKind::Truncate | RotationKind::Barrier => {
                    unreachable!("run_rotation_batch never groups a truncate or barrier")
                }
            }
        }
        let result = if self.retired.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::info!(
                dir = %self.flush_target.base_dir().display(),
                "flush skipped: the table was dropped while this flush was pending"
            );
            Ok(FlushOutcome::NothingToFlush)
        } else {
            self.flush_rotating(
                |catalog, schema| apply_rotation_changes(changes, catalog, schema),
                || releases.into_iter().for_each(|release| release()),
            )
        };
        match result {
            Ok(outcome) => replies
                .iter()
                .for_each(|reply| Self::answer(reply, Ok(outcome))),
            Err(e) => {
                // The first caller gets the error itself; the rest its text.
                let message = e.to_string();
                let mut replies = replies.iter();
                if let Some(first) = replies.next() {
                    Self::answer(first, Err(e));
                }
                replies.for_each(|reply| {
                    Self::answer(
                        reply,
                        Err(ferrosa_common::Error::InvalidData(format!(
                            "the memtable rotation this request joined failed: {message}"
                        ))),
                    );
                });
            }
        }
    }

    /// The memtable indexes a flush writes sidecars from: one per scalar index
    /// the `target` catalog declares. An index the frozen memtable carried
    /// gives its own postings, complete because every write to that memtable
    /// posted to it; an index the rotating DDL added is posted now from the
    /// frozen rows. An index the DDL dropped is left out.
    fn flush_sidecar_indexes(
        &self,
        frozen: &MemtableIndexes,
        target: &IndexCatalog,
        partitions: &[Partition],
    ) -> Vec<(String, Arc<MemtableIndex>)> {
        let key_component_counts = (
            self.partition_key_column_count(),
            self.clustering_column_count(),
        );
        target
            .scalar_index_names()
            .map(|name| {
                // The frozen postings serve only when they were built to the
                // same definition: a re-declaration that changed the type,
                // position or predicate needs its sidecar built from the rows.
                let same_definition =
                    frozen.catalog.scalar_definition(name) == target.scalar_definition(name);
                let index = match frozen.get(name) {
                    Some(index) if same_definition => Arc::clone(index),
                    _ => scalar_index_from_rows(target, name, partitions, key_component_counts),
                };
                (name.clone(), index)
            })
            .collect()
    }

    /// Mark this store dropped: wait for a rotation already running to
    /// finish, then make every later one a no-op.
    ///
    /// DROP TABLE removes the table from the engine and deletes its directory
    /// so a re-CREATE starts empty. Readers that still hold the table get
    /// I/O errors for files that are gone, which is loud and correct for a
    /// dropped table. A flush still running on it is different: it would
    /// write an SSTable into the deleted directory for the re-created table
    /// to load. So the flag is set first, and then a barrier goes through the
    /// rotation queue: it is answered only after every rotation queued before
    /// it has finished, and every rotation after it sees the flag.
    pub fn retire(&self) {
        self.retired
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Err(e) = self.rotate(RotationKind::Barrier) {
            tracing::error!(
                %e,
                dir = %self.flush_target.base_dir().display(),
                "retire: could not confirm that no flush of the dropped table is still writing"
            );
        }
    }

    /// The partition for `key` in SSTable generation `gen` alone.
    #[cfg(test)]
    pub(crate) fn read_from_generation_for_test(
        &self,
        gen: &str,
        key: &DecoratedKey,
    ) -> Result<Option<Partition>> {
        let view = self.view.load();
        let desc = view
            .sstables
            .iter()
            .find(|desc| desc.gen == gen)
            .ok_or_else(|| ferrosa_common::Error::InvalidData(format!("no generation {gen}")))?;
        self.open_reader(desc)?.get_partition_limited_rows(key, 0)
    }

    /// Sealed memtables not yet flushed.
    #[cfg(test)]
    pub(crate) fn sealed_memtable_count_for_test(&self) -> usize {
        self.view.load().flushing.len()
    }

    /// Memtable swaps this store has performed (see [`Self::rotate`]).
    #[cfg(test)]
    pub(crate) fn rotations_started(&self) -> u64 {
        self.rotations_started
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// One memtable rotation and the flush of the frozen memtable. Runs only
    /// on the rotation combiner (see [`Self::rotate`]), so never twice at once
    /// for one table. `plan` gives the catalog and schema the new memtable is
    /// bound to, from the frozen memtable's.
    fn flush_rotating(
        &self,
        plan: impl FnOnce(
            &Arc<IndexCatalog>,
            &Arc<TableSchema>,
        ) -> (Arc<IndexCatalog>, Arc<TableSchema>),
        on_memtable_release: impl FnOnce(),
    ) -> Result<FlushOutcome> {
        let total_start = Instant::now();
        self.rotations_started
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Step 1: Swap in a fresh active memtable and seal the old one: push it
        // onto the front of `flushing`, then seal its write gate and wait for
        // the writers already inside. After that every new write goes to the
        // new active memtable and the sealed one holds a complete, final
        // snapshot. Writers never wait here: one that meets the sealed gate
        // reloads the view.
        let new_active: Arc<dyn Memtable> = new_memtable();
        let phase_start = Instant::now();
        let target_catalog = {
            let old_view = self.view.load_full();
            let flush_schema = Arc::clone(&old_view.indexes.schema);
            let flush_catalog = Arc::clone(&old_view.indexes.catalog);
            // The catalog and schema the new memtable is bound to; every
            // sealed memtable this rotation flushes gets sidecars for this
            // catalog.
            let (target_catalog, target_schema) = plan(&flush_catalog, &flush_schema);
            if !Arc::ptr_eq(&target_schema, &flush_schema) {
                self.schema.store(Arc::clone(&target_schema));
            }
            let fresh_indexes = new_indexes(Arc::clone(&target_catalog), target_schema);
            let fresh_vector_indexes = new_vector_indexes(&target_catalog.vector_index_configs);
            let frozen_active = Arc::clone(&old_view.active);
            let old_indexes = Arc::clone(&old_view.indexes);
            debug_assert!(
                !old_indexes.gate.is_sealed(),
                "the active memtable's gate is sealed before its flush"
            );
            let sealed = SealedMemtable::new(
                Arc::clone(&frozen_active),
                Arc::clone(&old_indexes),
                Arc::clone(&old_view.vector_indexes),
            );
            drop(old_view);

            // Only a rotation replaces the active memtable, and rotations run
            // one at a time, so the view this derives from still holds
            // `frozen_active`; compaction swaps and sidecar installs that
            // landed since are kept. A memtable a failed flush left in
            // `flushing` stays there, behind this one (t_7681b32b).
            let swapped = self.update_view("flush:swap_active", |current| {
                if !Arc::ptr_eq(&current.active, &frozen_active) {
                    return (None, false);
                }
                let mut flushing = Vec::with_capacity(current.flushing.len() + 1);
                flushing.push(Arc::clone(&sealed));
                flushing.extend(current.flushing.iter().cloned());
                let next = StoreView {
                    active: Arc::clone(&new_active),
                    flushing: Arc::new(flushing),
                    sstables: Arc::clone(&current.sstables),
                    sstable_ids: Arc::clone(&current.sstable_ids),
                    indexes: Arc::clone(&fresh_indexes),
                    sidecar_indexes: Arc::clone(&current.sidecar_indexes),
                    vector_indexes: Arc::clone(&fresh_vector_indexes),
                };
                (Some(next), true)
            })?;
            if !swapped {
                tracing::error!("flush: the active memtable changed under a rotation");
                return Err(ferrosa_common::Error::InvalidData(
                    "flush: the active memtable was replaced while this rotation held it"
                        .to_string(),
                ));
            }
            // Writers that load the view from here on write to the new
            // memtable; seal the old one and wait out the writes inside it.
            old_indexes.gate.seal_and_drain(MEMTABLE_SEAL_DEADLINE)?;
            target_catalog
        };
        crate::metrics::observe_flush_phase(
            crate::metrics::FlushPhase::SwapMemtable,
            phase_start.elapsed(),
        );
        on_memtable_release();
        #[cfg(test)]
        flush_fault_test_hook::fire(flush_fault_test_hook::FlushFault::AfterSwap);

        // Step 2: Flush every sealed memtable, oldest first, each to its own
        // SSTable. More than one means earlier flushes failed after their
        // swap; each memtable leaves `flushing` only in the same view change
        // that installs its SSTable, so it is flushed exactly once. A failure
        // stops here and leaves the rest for the next rotation.
        let sealed: Vec<Arc<SealedMemtable>> =
            self.view.load().flushing.iter().rev().cloned().collect();
        if sealed.len() > 1 {
            tracing::warn!(
                sealed = sealed.len(),
                "flush: writing memtables left by earlier failed flushes, one SSTable each"
            );
        }
        let mut outcome = FlushOutcome::NothingToFlush;
        for memtable in &sealed {
            if self.flush_sealed(memtable, &target_catalog, total_start)? == FlushOutcome::Published
            {
                outcome = FlushOutcome::Published;
            }
        }
        Ok(outcome)
    }

    /// Flush one sealed memtable to its own SSTable, with sidecars for
    /// `target_catalog`, and take it out of `flushing` in the view change
    /// that installs that SSTable.
    fn flush_sealed(
        &self,
        sealed: &Arc<SealedMemtable>,
        target_catalog: &Arc<IndexCatalog>,
        total_start: Instant,
    ) -> Result<FlushOutcome> {
        let old_active = Arc::clone(&sealed.memtable);
        let old_indexes = Arc::clone(&sealed.bound);
        let old_vector_indexes = Arc::clone(&sealed.vector_indexes);
        // The schema and catalog the memtable's rows were written under. The
        // flush serializes with these even if an ALTER has replaced them: the
        // rows still carry the old ordinals.
        let flush_schema = Arc::clone(&sealed.bound.schema);
        let flush_catalog = Arc::clone(&sealed.bound.catalog);

        // Snapshot the sealed memtable.
        let phase_start = Instant::now();
        // Read the write epoch in the SAME critical step as the snapshot, so the
        // late-writer drain can tell whether anything landed afterwards (I-1).
        // Read BEFORE the snapshot: the drain then treats "epoch changed" as
        // "maybe a late write" — conservative in the safe direction, since a
        // write that lands during the snapshot also bumps it and forces the walk.
        let snapshot_epoch = old_active.write_epoch();
        let mut partitions = old_active.snapshot();
        crate::metrics::observe_flush_phase(
            crate::metrics::FlushPhase::SnapshotMemtable,
            phase_start.elapsed(),
        );

        let total_rows: usize = partitions.iter().map(|p| p.rows.len()).sum();
        tracing::debug!(
            partitions = partitions.len(),
            total_rows,
            "flush: memtable snapshot captured"
        );

        // Step 3: No-op if the memtable was empty.
        if partitions.is_empty() {
            // Derived from the live view, so a compaction swap or sidecar
            // install since the swap above is kept.
            self.update_view("flush:clear_flushing", |live| {
                let next = StoreView {
                    active: Arc::clone(&live.active),
                    flushing: without_sealed(&live.flushing, &old_active),
                    sstables: Arc::clone(&live.sstables),
                    sstable_ids: Arc::clone(&live.sstable_ids),
                    indexes: Arc::clone(&live.indexes),
                    sidecar_indexes: Arc::clone(&live.sidecar_indexes),
                    vector_indexes: Arc::clone(&live.vector_indexes),
                };
                (Some(next), ())
            })?;
            return Ok(FlushOutcome::NothingToFlush);
        }

        // Step 4: Sort partitions by key (required by SSTableWriter).
        let phase_start = Instant::now();
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        crate::metrics::observe_flush_phase(
            crate::metrics::FlushPhase::SortPartitions,
            phase_start.elapsed(),
        );

        // Step 5: Build the SSTable, under the schema captured at the swap.
        let options = self.options.clone();

        let schema = Arc::clone(&flush_schema);

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
        let phase_start = Instant::now();
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
        // Drop partitions that lost all their rows to quarantine. A partition
        // holding only a partition-level deletion (a `DELETE` of rows already
        // flushed) has no rows and no static row by construction; it must be
        // flushed, or the delete is lost and the older SSTable's rows read back
        // as live.
        partitions
            .retain(|p| !p.rows.is_empty() || p.static_row.is_some() || !p.deletion.is_live());
        crate::metrics::observe_flush_phase(
            crate::metrics::FlushPhase::ValidateRows,
            phase_start.elapsed(),
        );

        if total_quarantined > 0 {
            tracing::error!(
                keyspace = %schema.keyspace,
                table = %schema.table,
                quarantined_rows = total_quarantined,
                quarantine_file = ?quarantine_writer.as_ref().map(|w| w.path().display().to_string()),
                "flush: quarantined malformed rows — see quarantine file for forensic record"
            );
        }

        // Step 5a': the memtable speaks flat ordinals (statics first, static
        // cells inside clustered rows); an SSTable numbers statics and regulars
        // separately and holds statics in the static row. Convert here, in
        // place, before any header is built (t_65661473).
        for p in partitions.iter_mut() {
            crate::ordinal_space::flat_into_sstable_space(p, schema.static_columns.len())?;
        }

        if partitions.is_empty() {
            tracing::warn!(
                keyspace = %schema.keyspace,
                table = %schema.table,
                quarantined_rows = total_quarantined,
                "flush: all rows were quarantined; skipping empty SSTable publish"
            );
            self.update_view("flush:clear_all_quarantined", |live| {
                let next = StoreView {
                    active: Arc::clone(&live.active),
                    flushing: without_sealed(&live.flushing, &old_active),
                    sstables: Arc::clone(&live.sstables),
                    sstable_ids: Arc::clone(&live.sstable_ids),
                    indexes: Arc::clone(&live.indexes),
                    sidecar_indexes: Arc::clone(&live.sidecar_indexes),
                    vector_indexes: Arc::clone(&live.vector_indexes),
                };
                (Some(next), ())
            })?;
            return Ok(FlushOutcome::NothingToFlush);
        }

        // Parallel sharded flush (slice #3): when the table has NO secondary
        // indexes, split the token-sorted partitions into shards and encode each
        // into its own SSTable in parallel. Encode is ~98% of flush time and is
        // single-threaded per SSTable, so this parallelizes the write-throughput
        // floor. Indexed tables fall through to the single-SSTable path below
        // (per-shard index splitting is a later increment); their sidecar/
        // fulltext/vector steps stay exactly as-is.
        // Sidecars are written for the target catalog (see Step 5b), so a
        // flush whose rotation added an index must take the indexed path.
        let can_shard = !flush_catalog.declares_any() && !target_catalog.declares_any();
        let num_shards = flush::desired_flush_shards(
            partitions.len(),
            can_shard,
            crate::flush_executor::width(),
        );
        if num_shards > 1 {
            return self
                .flush_sharded(
                    partitions,
                    num_shards,
                    total_rows,
                    snapshot_epoch,
                    &old_active,
                    flush_schema,
                )
                .map(|()| FlushOutcome::Published);
        }

        // Everything a sidecar is built from the rows is built HERE, before
        // the rows are encoded: encoding expands legacy whole-value collection
        // cells in place, and the sidecars must describe the rows as the
        // memtable held and indexed them, the same as the memtable postings.
        //
        // The sidecar set is exactly the indexes the TARGET catalog declares —
        // the one the next memtable is bound to. An index DDL rotated the
        // memtable to get here: an index it dropped gets no sidecar, and an
        // index it added is posted now from the frozen rows, which no write
        // can reach any more.
        let sidecar_indexes = self.flush_sidecar_indexes(&old_indexes, target_catalog, &partitions);
        // Serve exactly these postings for the flushing memtable until the
        // sidecar is installed: an index the rotating DDL added now reads the
        // frozen rows too, and one it dropped no longer answers from them.
        let published: Arc<HashMap<String, Arc<MemtableIndex>>> =
            Arc::new(sidecar_indexes.iter().cloned().collect());
        // The vector indexes the target catalog declares, for the frozen rows:
        // its own index, or one built from the rows for an index the rotating
        // DDL added. The same map feeds the vector sidecars below.
        let flush_vector_indexes: Arc<HashMap<String, Arc<VectorMemtableIndex>>> = Arc::new(
            target_catalog
                .vector_index_configs
                .iter()
                .map(|cfg| {
                    let index = match old_vector_indexes.get(&cfg.index_name) {
                        Some(index) => Arc::clone(index),
                        None => vector_index_from_rows(cfg, &partitions),
                    };
                    (cfg.index_name.clone(), index)
                })
                .collect(),
        );
        sealed.postings.store(Arc::clone(&published));
        sealed.vectors.store(Arc::clone(&flush_vector_indexes));
        // FTI documents for the target catalog's full-text indexes, read at the
        // ordinal the frozen rows were written with (the frozen catalog's,
        // when it declares the index). Built before encoding, from the rows as
        // the memtable holds them; written once the generation is known.
        let mut fti_sidecars: Vec<(String, std::result::Result<Vec<u8>, String>)> = Vec::new();
        for (index_name, target_pos) in &target_catalog.fulltext_indexes {
            let col_pos = flush_catalog
                .fulltext_indexes
                .iter()
                .find(|(name, _)| name == index_name)
                .map_or(target_pos, |(_, frozen_pos)| frozen_pos);
            let mut fti_builder = ferrosa_index::fulltext::builder::FullTextIndexBuilder::new();
            for partition in &partitions {
                let pk_bytes = partition.key.key.as_bytes();
                // Index ONE document PER ROW, keyed by the full primary key
                // (partition + clustering). Indexing per-partition with the text
                // of all rows concatenated made a hit identify only the partition,
                // leaking non-matching clustering rows (t_da51e20c).
                for row in &partition.rows {
                    let mut text = String::new();
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
                    if !text.is_empty() {
                        let doc_key = ferrosa_index::fulltext::keys::encode_doc_key(
                            pk_bytes,
                            &row.clustering,
                        );
                        fti_builder.add_document(doc_key, text.trim());
                    }
                }
            }
            fti_sidecars.push((
                index_name.clone(),
                fti_builder.finish().map_err(|e| e.to_string()),
            ));
        }

        let header = flush::header_for_flush(&schema, &partitions);
        let staged_output = self.flush_target.file_output_staging_dir()?;
        let mut writer = if let Some(staging_dir) = staged_output.as_ref() {
            SSTableWriter::new_file_backed(options, header.clone(), staging_dir.join("Data.db"))?
        } else {
            SSTableWriter::new(options, header.clone())
        };
        let phase_start = Instant::now();
        let table_label = format!("{}.{}", schema.keyspace, schema.table);
        // The partitions are owned, so a legacy whole-value collection cell is
        // expanded in place. The late-writer check below knows which ones.
        let mut expanded_keys = std::collections::BTreeSet::new();
        for p in partitions.iter_mut() {
            if crate::memtable::expand_collection_blobs_in_place(p, &header, &table_label)? {
                expanded_keys.insert(p.key.clone());
            }
            writer.add_partition(p)?;
        }
        let (reader, output_bytes) = if let Some(staging_dir) = staged_output {
            let output = writer.finish_to_directory_deferred_sync(staging_dir)?;
            let output_bytes = output.total_size_bytes();
            crate::metrics::observe_flush_phase(
                crate::metrics::FlushPhase::EncodeSstable,
                phase_start.elapsed(),
            );
            let phase_start = Instant::now();
            let reader = self.flush_target.flush_deferred_files(output)?;
            crate::metrics::observe_flush_phase(
                crate::metrics::FlushPhase::LocalWriteSstable,
                phase_start.elapsed(),
            );
            (reader, output_bytes)
        } else {
            let output = writer.finish()?;
            let output_bytes = output.data.len()
                + output.partitions.len()
                + output.rows.len()
                + output.filter.len()
                + output.statistics.len()
                + output.toc.len()
                + output
                    .compression_info
                    .as_ref()
                    .map(|ci| ci.len())
                    .unwrap_or(0);
            crate::metrics::observe_flush_phase(
                crate::metrics::FlushPhase::EncodeSstable,
                phase_start.elapsed(),
            );
            let phase_start = Instant::now();
            let reader = self.flush_target.flush(output)?;
            crate::metrics::observe_flush_phase(
                crate::metrics::FlushPhase::LocalWriteSstable,
                phase_start.elapsed(),
            );
            (reader, output_bytes as u64)
        };
        crate::metrics::observe_flush_phase(
            crate::metrics::FlushPhase::Total,
            total_start.elapsed(),
        );
        crate::metrics::observe_flush_output(
            output_bytes,
            total_rows as u64,
            partitions.len() as u64,
        );
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
        // Hand the memtable indexes to the flush target as SOURCES, not as a
        // materialised posting set: a tree is already in `(key, row)` order,
        // so the target streams it to disk one borrowed entry at a time. This
        // used to copy every posting three times before a byte was written —
        // the traversal built a `Vec`, the flatten cloned the key once per
        // posting, and the writer took its own `to_vec` of that to sort it —
        // which made the flush, not the query, the memory peak on a node with
        // a large index (see tests/sidecar_memory_bound.rs).
        //
        // The pin is taken HERE, beside the memtable that was just swapped out,
        // so the sidecar describes the rows this SSTable holds however long the
        // write itself takes. Pinning a persistent tree copies nothing: it is
        // one `Arc` per index.
        //
        // The sidecar set is exactly the indexes the TARGET catalog declares —
        // the one the next memtable is bound to. An index DDL rotated the
        // memtable to get here: an index it dropped gets no sidecar, and an
        // index it added is posted now from the frozen rows, which no write
        // can reach any more.
        let pinned_indexes: Vec<(&str, crate::memtable::index::IndexSnapshot)> = sidecar_indexes
            .iter()
            .map(|(index_name, memtable_idx)| (index_name.as_str(), memtable_idx.pin()))
            .collect();
        // Publish what this flush covers before writing it: these indexes take
        // their postings for this generation from the pins above, so the engine
        // must not queue a rebuild for them (see `flush_index_action`).
        self.last_flush_indexes.store(Arc::new(
            pinned_indexes
                .iter()
                .map(|(index_name, _)| (*index_name).to_string())
                .collect::<Vec<_>>(),
        ));
        let sidecar_sources: Vec<(&str, &dyn crate::index::sidecar::SidecarSource)> =
            pinned_indexes
                .iter()
                .map(|(index_name, pinned)| {
                    (
                        *index_name,
                        pinned as &dyn crate::index::sidecar::SidecarSource,
                    )
                })
                .collect();
        let sidecar_map: HashMap<String, SidecarReader> =
            match self.flush_target.write_sidecars(gen, &sidecar_sources) {
                Ok(readers) => readers,
                Err(e) => {
                    // Fail loud and degrade visibly: this generation's indexes
                    // are served from images so no read answers short, and the
                    // next restart rebuilds them from the files.
                    tracing::error!(
                        %e,
                        gen,
                        "store: sidecar persist failed; serving this generation's indexes from \
                         in-memory copies (heap-resident until restart)"
                    );
                    sidecar_sources
                        .iter()
                        .filter(|(_, source)| !source.is_empty())
                        .filter_map(|(index_name, source)| {
                            let mut entries: Vec<(IndexKey, RowPosition)> = Vec::new();
                            let visited = source.visit(&mut |key, position| {
                                entries.push((key.clone(), position.clone()));
                                Ok(())
                            });
                            if let Err(error) = visited {
                                tracing::error!(
                                    %error, index_name,
                                    "store: could not read a memtable index for its fallback image"
                                );
                                return None;
                            }
                            (!entries.is_empty()).then(|| {
                                (
                                    (*index_name).to_string(),
                                    SidecarReader::from_entries(entries),
                                )
                            })
                        })
                        .collect()
                }
            };
        drop(sidecar_sources);

        // Step 5c: Write the FTI sidecars built before the rows were encoded.
        for (index_name, built) in fti_sidecars {
            match built {
                Ok(fti_bytes) => {
                    if let Err(e) =
                        self.flush_target
                            .write_fti_sidecar(gen, &index_name, &fti_bytes)
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
        for cfg in &target_catalog.vector_index_configs {
            // Read, not drained: readers keep searching this index through
            // `flushing_vector_indexes` until the sidecar is installed.
            let Some(vi) = flush_vector_indexes.get(&cfg.index_name) else {
                // No vectors reached this memtable: the generation has none
                // to index, and its (empty) manifest says so.
                self.complete_flushed_vector_sidecars(gen, cfg, VectorSidecarManifest::default());
                continue;
            };
            {
                let drained_with_scopes = vi.entries_with_scopes();
                if drained_with_scopes.is_empty() {
                    self.complete_flushed_vector_sidecars(
                        gen,
                        cfg,
                        VectorSidecarManifest::default(),
                    );
                    continue;
                }

                let drained: Vec<_> = drained_with_scopes
                    .iter()
                    .map(|(_, pos, vector)| (*pos, vector.clone()))
                    .collect();

                match target_catalog.vector_index_method(&cfg.index_name) {
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

                let mut by_scope: HashMap<
                    Vec<u8>,
                    Vec<(ferrosa_index::vector::RowPosition, Vec<f32>)>,
                > = HashMap::new();
                // A vector with no scope is in no scoped sidecar, so the
                // generation is left without a manifest and rebuilt from its
                // rows rather than marked complete without that row.
                let mut complete = true;
                for (scope, pos, vector) in drained_with_scopes {
                    match scope {
                        Some(scope) => by_scope.entry(scope).or_default().push((pos, vector)),
                        None => complete = false,
                    }
                }
                let mut manifest = VectorSidecarManifest::default();

                // Remember these scopes so `ann_search_partitions` can later
                // enumerate the scoped sidecars and recover partition keys
                // (the global sidecar drops the scope on write).
                self.record_vector_scopes(&cfg.index_name, by_scope.keys());
                for (scope, scoped_entries) in by_scope {
                    let scoped_index_name = scoped_vector_sidecar_name(&cfg.index_name, &scope);
                    let vectors = scoped_entries.len();
                    match ferrosa_index::vector::hnsw::build_and_serialize(
                        cfg.m,
                        cfg.ef_construction,
                        cfg.metric,
                        scoped_entries,
                    ) {
                        Ok(vec_bytes) => {
                            if let Err(e) = self.flush_target.write_vector_sidecar(
                                gen,
                                &scoped_index_name,
                                &vec_bytes,
                            ) {
                                complete = false;
                                tracing::error!(%e, index_name = %scoped_index_name, gen,
                                    "store: scoped vector sidecar persist failed; the generation \
                                     is rebuilt from its rows before ANN answers over it");
                                if cfg!(debug_assertions) {
                                    panic!("scoped vector sidecar persist failed: {e}");
                                }
                            } else {
                                manifest.add_scope(vectors, vec_bytes.len());
                            }
                        }
                        Err(e) => {
                            complete = false;
                            tracing::error!(%e, index_name = %scoped_index_name, gen,
                                "store: scoped vector sidecar serialization failed; the \
                                 generation is rebuilt from its rows before ANN answers over it");
                            if cfg!(debug_assertions) {
                                panic!("scoped vector sidecar serialize failed: {e}");
                            }
                        }
                    }
                }
                if complete {
                    self.complete_flushed_vector_sidecars(gen, cfg, manifest);
                }
            }
        }

        // Step 5d: Drain late writers. Any writer that loaded the view before
        // step 1 may have written to old_active AFTER our snapshot. Those writes
        // would be lost when we clear `flushing`. Re-scan the old memtable and
        // replay any entries not in the original flush to the new active.
        //
        // Two rules this scan must obey:
        //
        // 1. It is READ-ONLY, so it uses `for_each_partition` — the BORROWED
        //    scan — never `range_iter`. `range_iter` yields an owned
        //    `Arc<Partition>`; a consumer holding one raises the partition's
        //    strong count, and the next `put` on that partition then finds
        //    `Arc::make_mut` with refcount > 1 and deep-clones the whole
        //    partition before merging. That is O(rows) per write, the O(N^2)
        //    fill pathology `e440b60f` removed, and it is what regressed t512.
        // 2. Almost always there is nothing to replay (the sealed gate makes a
        //    late write an admission bug). `write_epoch` answers "did anything
        //    land after the snapshot?" in O(1), so the full walk is skipped
        //    entirely on a healthy cluster.
        let flushed_by_key: std::collections::BTreeMap<_, _> =
            partitions.iter().map(|p| (p.key.clone(), p)).collect();
        // Loaded once, and only if something actually needs replaying.
        let mut replay_ctx = None;
        if late_writer_drain_needed(old_active.as_ref(), snapshot_epoch) {
            old_active.for_each_partition(None, None, &mut |p| {
                if !late_partition_needs_replay(&flushed_by_key, &expanded_keys, p) {
                    return true;
                }
                // The sealed gate makes this impossible: a write after the
                // snapshot means admission is broken. Keep the rows (replay them
                // into the new memtable), and say so loudly — their index
                // postings are NOT carried over.
                let (current_view, schema) =
                    replay_ctx.get_or_insert_with(|| (self.view.load(), self.schema.load()));
                self.late_writes_after_seal
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    partition = ?p.key,
                    "flush: a row reached a sealed memtable after its snapshot; replaying \
                     it without its index postings"
                );
                for row in &p.rows {
                    if let Err(e) = current_view.active.put(&p.key, row.clone(), schema) {
                        tracing::error!(%e, "flush: late-writer replay put failed");
                    }
                }
                true
            });
        }

        // Step 6: Prepend new SSTable and sidecar, clear flushing.
        tracing::debug!(
            gen,
            prior_sstable_count = self.sstable_count(),
            "flush: SSTable written, updating view"
        );
        // Use the actual base directory from the flush target, not empty PathBuf.
        // An empty path causes ID collisions with compaction output:
        // swap_compacted_sstables matches by ID only, so a flush SSTable with
        // the same gen as a compaction input gets incorrectly removed during swap.
        let flush_dir = self.flush_target.base_dir().to_path_buf();

        // Build the lightweight descriptor from the freshly-flushed reader
        // (capturing its key/token bounds), seed the pool so the next read is a
        // cache hit, then store only the descriptor in the view.
        let new_desc =
            SstableDescriptor::from_reader(format!("{gen}"), flush_dir.clone(), &new_reader);
        self.seed_reader(&new_desc, new_reader);
        let new_sidecar_map = Arc::new(sidecar_map);

        // Once the SSTable reader is installed, the flushed memtable must leave
        // the live view. Writers cannot be racing against `old_active`: its
        // gate was sealed and drained before the snapshot. Keeping
        // `old_active` in `flushing` after a successful flush makes subsequent
        // flushes re-ingest already-flushed rows and can cascade wide-partition
        // snapshots under aggressive concurrent flush loops.
        self.update_view("flush:install_new_sstable", |current_view| {
            let mut new_sstables = vec![new_desc.clone()];
            new_sstables.extend(current_view.sstables.iter().cloned());
            let mut new_ids = vec![(format!("{gen}"), flush_dir.clone())];
            new_ids.extend(current_view.sstable_ids.iter().cloned());
            let mut new_sidecars = vec![Arc::clone(&new_sidecar_map)];
            new_sidecars.extend(current_view.sidecar_indexes.iter().cloned());
            let next = StoreView {
                active: Arc::clone(&current_view.active),
                flushing: without_sealed(&current_view.flushing, &old_active),
                sstables: Arc::new(new_sstables),
                sstable_ids: Arc::new(new_ids),
                indexes: Arc::clone(&current_view.indexes),
                sidecar_indexes: Arc::new(new_sidecars),
                vector_indexes: Arc::clone(&current_view.vector_indexes),
            };
            (Some(next), ())
        })?;

        Ok(FlushOutcome::Published)
    }

    /// Parallel sharded flush (slice #3). Split the token-sorted `partitions`
    /// into `num_shards` contiguous token-range shards, ENCODE each into its own
    /// SSTable in parallel on the flush pool (encode is ~98% of flush time and
    /// single-threaded per SSTable — this parallelizes the write-throughput
    /// floor), then publish all shard SSTables into the view together.
    ///
    /// Only called by `flush()` for tables with NO secondary indexes (its
    /// `can_shard` gate), so there are no sidecar / fulltext / vector artifacts
    /// to split per shard. The shards partition the token space disjointly and
    /// contiguously, so the resulting SSTables are non-overlapping-by-
    /// construction and read/compact exactly like any other SSTable set.
    ///
    /// Preconditions: `partitions` is sorted by `DecoratedKey` and non-empty;
    /// the active/flushing view swap (`flush()` step 1) already happened, and
    /// `old_active` is the flushing memtable (for late-writer replay).
    fn flush_sharded(
        &self,
        partitions: Vec<Partition>,
        num_shards: usize,
        total_rows: usize,
        // The sealed memtable's write epoch, read by the caller in the same step
        // as the snapshot it also took. It must be captured THERE, not here:
        // reading it inside this function would be after the snapshot, so a
        // write landing in between would be in neither the snapshot nor flagged
        // as late — data loss (I-1).
        snapshot_epoch: u64,
        old_active: &Arc<dyn Memtable>,
        schema: Arc<TableSchema>,
    ) -> Result<()> {
        // Only used for this phase's elapsed-time metric; the sharded path is
        // entered directly from `flush()`, so it measures from here.
        let total_start = Instant::now();
        let options = self.options.clone();
        // `schema` is the one captured at the memtable swap (owned, Send+Sync),
        // so the encode closures share it across the rayon pool and a
        // concurrent ALTER cannot change the layout mid-flush.
        let mut shards = flush::split_sorted_partitions_into_shards(partitions, num_shards);

        // Stage directories before entering Rayon: the target need not be Sync.
        // Guards outlive all parallel writers and remove incomplete or unpublished
        // files on every error. Each worker retains bounded pump buffers plus a
        // component manifest, never a complete encoded SSTable byte image.
        struct Staging(Option<std::path::PathBuf>);
        impl Drop for Staging {
            fn drop(&mut self) {
                if let Some(path) = &self.0 {
                    if let Err(error) = std::fs::remove_dir_all(path) {
                        if error.kind() != std::io::ErrorKind::NotFound {
                            tracing::error!(?path, %error, "sharded flush staging cleanup failed");
                        }
                    }
                }
            }
        }
        enum ShardOutput {
            Files(SSTableOutputFiles),
            Memory(SSTableOutput),
        }
        let staging: Vec<Staging> = (0..shards.len())
            .map(|_| self.flush_target.file_output_staging_dir().map(Staging))
            .collect::<Result<_>>()?;
        let phase_start = Instant::now();
        let table_label = format!("{}.{}", schema.keyspace, schema.table);
        // Each shard is owned, so a legacy whole-value collection cell is
        // expanded in place; the keys that were expanded go to the late-writer
        // check below.
        let encoded: Vec<(ShardOutput, Vec<DecoratedKey>)> = crate::flush_executor::pool()?
            .install(|| {
                shards
                    .par_iter_mut()
                    .zip(staging.par_iter())
                    .map(|(shard, stage)| {
                        let header = flush::header_for_flush(&schema, shard);
                        let mut writer = match &stage.0 {
                            Some(dir) => SSTableWriter::new_file_backed(
                                options.clone(),
                                header.clone(),
                                dir.join("Data.db"),
                            )?,
                            None => SSTableWriter::new(options.clone(), header.clone()),
                        };
                        let mut expanded = Vec::new();
                        for partition in shard.iter_mut() {
                            if crate::memtable::expand_collection_blobs_in_place(
                                partition,
                                &header,
                                &table_label,
                            )? {
                                expanded.push(partition.key.clone());
                            }
                            writer.add_partition(partition)?;
                        }
                        let output = match &stage.0 {
                            Some(dir) => writer
                                .finish_to_directory_deferred_sync(dir)
                                .map(ShardOutput::Files),
                            None => writer.finish().map(ShardOutput::Memory),
                        }?;
                        Ok((output, expanded))
                    })
                    .collect::<Result<_>>()
            })?;
        let mut expanded_keys = std::collections::BTreeSet::new();
        let mut outputs: Vec<ShardOutput> = Vec::with_capacity(encoded.len());
        for (output, expanded) in encoded {
            outputs.push(output);
            expanded_keys.extend(expanded);
        }
        crate::metrics::observe_flush_phase(
            crate::metrics::FlushPhase::EncodeSstable,
            phase_start.elapsed(),
        );

        // Publish each finished shard under its own generation; install all
        // readers together only after every shard has been published.
        let phase_start = Instant::now();
        let flush_dir = self.flush_target.base_dir().to_path_buf();
        let mut published: Vec<(u64, Arc<SSTableReader<F::Reader>>)> =
            Vec::with_capacity(outputs.len());
        let mut total_output_bytes = 0u64;
        for output in outputs {
            let reader = match output {
                ShardOutput::Files(output) => {
                    total_output_bytes += output.total_size_bytes();
                    self.flush_target.flush_deferred_files(output)?
                }
                ShardOutput::Memory(output) => {
                    total_output_bytes += (output.data.len()
                        + output.partitions.len()
                        + output.rows.len()
                        + output.filter.len()
                        + output.statistics.len()
                        + output.toc.len()
                        + output.compression_info.as_ref().map_or(0, Vec::len))
                        as u64;
                    self.flush_target.flush(output)?
                }
            };
            let gen = self.flush_target.last_generation();
            self.next_gen
                .fetch_max(gen + 1, std::sync::atomic::Ordering::SeqCst);
            published.push((gen, Arc::new(reader)));
        }
        crate::metrics::observe_flush_phase(
            crate::metrics::FlushPhase::LocalWriteSstable,
            phase_start.elapsed(),
        );
        crate::metrics::observe_flush_phase(
            crate::metrics::FlushPhase::Total,
            total_start.elapsed(),
        );
        let total_partitions: usize = shards.iter().map(|s| s.len()).sum();
        crate::metrics::observe_flush_output(
            total_output_bytes,
            total_rows as u64,
            total_partitions as u64,
        );

        // Step 5d (late-writer replay) — identical semantics to the single-
        // SSTable path: writes that landed in `old_active` after our snapshot
        // must be replayed into the new active memtable or they are lost when
        // `flushing` is cleared. `shards` is still owned here, so we can compare
        // late writes against the full flushed partitions.
        let flushed_by_key: std::collections::BTreeMap<
            ferrosa_common::key::DecoratedKey,
            &Partition,
        > = shards
            .iter()
            .flatten()
            .map(|p| (p.key.clone(), p))
            .collect();
        // STREAMING: walk the BORROWED scan one partition at a time; never collect
        // the memtable into a `Vec`, and never hand out an owned
        // `Arc<Partition>` — see the single-SSTable path for why that would make
        // concurrent writes copy-on-write the whole partition. The O(1) epoch
        // check skips this walk entirely when nothing landed after the snapshot.
        let mut replay_ctx = None;
        if late_writer_drain_needed(old_active.as_ref(), snapshot_epoch) {
            old_active.for_each_partition(None, None, &mut |p| {
                if !late_partition_needs_replay(&flushed_by_key, &expanded_keys, p) {
                    return true;
                }
                // Impossible behind the sealed gate; see the unsharded path.
                let (current_view, schema) =
                    replay_ctx.get_or_insert_with(|| (self.view.load(), self.schema.load()));
                self.late_writes_after_seal
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    partition = ?p.key,
                    "flush: a row reached a sealed memtable after its snapshot; replaying it"
                );
                for row in &p.rows {
                    if let Err(e) = current_view.active.put(&p.key, row.clone(), schema) {
                        tracing::error!(%e, "flush(sharded): late-writer replay put failed");
                    }
                }
                true
            });
        }

        // Step 6 (publish) — prepend ALL shard SSTables to the view, clear
        // flushing. Each shard carries an empty sidecar map (no secondary
        // indexes on a shardable table).
        let mut shard_sstables = Vec::with_capacity(published.len());
        let mut shard_ids = Vec::with_capacity(published.len());
        for (gen, reader) in &published {
            let desc = SstableDescriptor::from_reader(format!("{gen}"), flush_dir.clone(), reader);
            self.seed_reader(&desc, Arc::clone(reader));
            shard_sstables.push(desc);
            shard_ids.push((format!("{gen}"), flush_dir.clone()));
        }
        // Derived from the live view, so a compaction swap or sidecar install
        // since the memtable swap is kept.
        self.update_view("flush:install_sharded_sstables", |current_view| {
            let mut new_sstables = shard_sstables.clone();
            new_sstables.extend(current_view.sstables.iter().cloned());
            let mut new_ids = shard_ids.clone();
            new_ids.extend(current_view.sstable_ids.iter().cloned());
            let mut new_sidecars: Vec<Arc<HashMap<String, SidecarReader>>> =
                shard_ids.iter().map(|_| Arc::new(HashMap::new())).collect();
            new_sidecars.extend(current_view.sidecar_indexes.iter().cloned());
            let next = StoreView {
                active: Arc::clone(&current_view.active),
                flushing: without_sealed(&current_view.flushing, old_active),
                sstables: Arc::new(new_sstables),
                sstable_ids: Arc::new(new_ids),
                indexes: Arc::clone(&current_view.indexes),
                sidecar_indexes: Arc::new(new_sidecars),
                vector_indexes: Arc::clone(&current_view.vector_indexes),
            };
            (Some(next), ())
        })
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
        self.count_range_matching(start, end, &|_| true)
    }

    /// `count_range`, counting only partitions whose key satisfies `matches`.
    ///
    /// The predicate is applied to the merged partition KEY, inside the same
    /// metadata-only pass — no cell payload is decoded for a partition that is
    /// counted, and none for one that is skipped either.
    ///
    /// This exists so `COUNT(*) WHERE <partition-key component> = ?` can stay
    /// on the ADR-020 fast path. Without it any predicate drops the query onto
    /// the secondary index and a lookup per posting: measured on a live
    /// cluster at 24s against the unfiltered count's 0.32s over the same
    /// 103,664 rows, and four times slower than shipping every row to the
    /// client.
    ///
    /// It has to be a predicate rather than a key range. `DecoratedKey` orders
    /// by token first, so partitions sharing a partition-key component are
    /// scattered across the ring; a prefix range would silently count a
    /// different set.
    pub fn count_range_matching(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        matches: &dyn Fn(&DecoratedKey) -> bool,
    ) -> Result<u64> {
        let view = self.view.load_full();
        let start_owned = start.cloned();
        let end_owned = end.cloned();
        let active_iter = view
            .active
            .range_iter(start_owned.as_ref(), end_owned.as_ref());
        let flushing_iters: Vec<_> = view
            .flushing
            .iter()
            .map(|sealed| {
                sealed
                    .memtable
                    .range_iter(start_owned.as_ref(), end_owned.as_ref())
            })
            .collect();
        // Open readers for descriptors overlapping the key window (pooled).
        // Held for the merger's lifetime so they cannot be evicted mid-merge.
        let sst_readers = self.open_readers_for_key_range(
            &view.sstables,
            start_owned.as_ref(),
            end_owned.as_ref(),
        )?;
        let sstables_slice = &sst_readers[..];

        let mut merger = crate::range_merger::merger_for_metadata_sources(
            active_iter,
            flushing_iters,
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
        // The reserved table-tombstone partition itself holds no rows, but its
        // watermark still suppresses rows table-wide; fold it in before counting.
        let (table_delete, probe_corrupt) = self.table_deletion(&view)?;
        if let Some(c) = probe_corrupt {
            // This path has no fresh-view retry of its own (a merger error is
            // already final here), so surface an unconsultable overlapping
            // SSTable exactly as a source failure: typed, never a partial count.
            return Err(ferrosa_common::Error::corrupt_sstable(
                c.gen,
                c.min_token,
                c.max_token,
            ));
        }
        while let Some(p) = merger.next_merged_partition()? {
            if !matches(&p.key) || crate::table_tombstone::is_table_tombstone_key(&p.key) {
                continue;
            }
            let p = if !table_delete.is_live()
                && table_delete.marked_for_delete_at > p.deletion.marked_for_delete_at
            {
                let mut owned = Arc::unwrap_or_clone(p);
                merge::apply_table_deletion(&mut owned, table_delete);
                owned
            } else {
                Arc::unwrap_or_clone(p)
            };
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
    /// Takes `wanted` by value so the scan can own it; the returned stream
    /// has no borrow.
    pub fn range_iter_projected(
        &self,
        wanted: Vec<u16>,
        partition_limit: Option<usize>,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Arc<Partition>>> + Send>>
    where
        F: Send + Sync + 'static,
    {
        self.whole_partition_range_scan(Some(wanted), partition_limit, start, end)
    }

    /// The producer behind [`Self::range_iter`] and
    /// [`Self::range_iter_projected`]: a [`RangeScan`] on the bounded pool
    /// that pauses — no slot, no thread — while its consumer is not reading.
    fn whole_partition_range_scan(
        &self,
        wanted: Option<Vec<u16>>,
        partition_limit: Option<usize>,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Arc<Partition>>> + Send>>
    where
        F: Send + Sync + 'static,
    {
        self.range_scan_stream(wanted, partition_limit, None, start, end)
    }

    /// Every range-scan producer: a [`RangeScan`] on the bounded pool, whole
    /// partitions or (`fragment_rows`) `<= K`-row fragments, that pauses — no
    /// slot, no thread — while its consumer is not reading.
    fn range_scan_stream(
        &self,
        wanted: Option<Vec<u16>>,
        partition_limit: Option<usize>,
        fragment_rows: Option<usize>,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Arc<Partition>>> + Send>>
    where
        F: Send + Sync + 'static,
    {
        // Buffer is intentionally small. Per-partition body decode on cold
        // cache is the dominant cost (wide rows + embedding cells + dedup
        // across multiple SSTable runs sharing a key). A larger buffer turns a
        // `LIMIT N` scan into a `LIMIT N + buffer` scan because the producer
        // races ahead before the consumer can drop the stream; we measured
        // ~32 s cold-cache walls on a 1.7 GB table for `LIMIT 5` with
        // buffer=64. With buffer=4 *and* `partition_limit` pushed into the
        // producer loop, the producer stops cleanly after N emissions.
        const STREAM_BUFFER: usize = 4;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Arc<Partition>>>(STREAM_BUFFER);

        // The scan opens its SSTable readers only once admitted (t_6d0553ee):
        // a cancelled or overloaded scan never opens readers. Everything it
        // needs is an owned `Arc`/clone, so it can pause between runs.
        //
        // t_88223ad0: the producer runs on the bounded scheduler pool (cores -
        // reserved), not the unbounded blocking pool, so a broad scan cannot
        // oversubscribe the cores and starve raft heartbeats. Every such scan
        // is `ScanPlan::FullScan`-class work, weighted Bulk.
        // Resolve the table tombstone once for the scan. A failure to read an
        // overlapping SSTable is surfaced as a stream error, never silently
        // dropped (a missing tombstone would resurrect truncated data).
        let table_delete = match self.table_deletion(&self.view.load()) {
            Ok((d, None)) => d,
            Ok((_, Some(c))) => {
                // No scan has started yet, so there is nothing to retire; surface
                // the unconsultable overlapping SSTable as the same typed error the
                // resumable walk raises for an unreadable source.
                return Box::pin(futures::stream::once(async move {
                    Err(ferrosa_common::Error::corrupt_sstable(
                        c.gen,
                        c.min_token,
                        c.max_token,
                    ))
                }));
            }
            Err(e) => return Box::pin(futures::stream::once(async move { Err(e) })),
        };
        let scan = RangeScan {
            view: self.view.load_full(),
            schema: self.schema.load_full(),
            reader_pool: self.reader_pool.clone(),
            pool_table_key: self.pool_table_key.clone(),
            flush_target: self.flush_target.clone(),
            wanted,
            partition_limit,
            fragment_rows,
            start: start.cloned(),
            end: end.cloned(),
            merger: None,
            emitted: 0,
            live_view: Arc::clone(&self.view),
            delivered: None,
            skip: None,
            resumes: 0,
            table_delete,
        };
        spawn_resumable_range_scan(tx, scan);

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
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Arc<Partition>>> + Send>>
    where
        F: Send + Sync + 'static,
    {
        self.whole_partition_range_scan(None, None, start, end)
    }

    /// Intra-partition streaming variant of [`Self::range_iter`]. A single
    /// (possibly multi-million-row) partition is delivered as a SEQUENCE of
    /// `Partition` items each holding `<= K` clustered rows (K =
    /// [`crate::range_merger::rows_per_fragment`]), so the producer's
    /// resident memory is `O(num_sources + K)` rows regardless of how wide
    /// the partition is.
    ///
    /// Reassembly contract: every item for one partition key shares the
    /// same `key`; the FIRST item carries the real `deletion` + `static_row`
    /// and later items carry `LIVE` / `None`. A consumer that flattens
    /// `Partition.rows` independently (the CQL row bridge, the cluster
    /// stream handler) therefore reproduces the merged partition exactly —
    /// the row sequence is byte-identical to [`Self::range_iter`]'s
    /// whole-partition output. This is the OOM fix for full-table
    /// `SELECT *` over inverted-index-shaped tables.
    pub fn range_iter_fragmented(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Arc<Partition>>> + Send>>
    where
        F: Send + Sync + 'static,
    {
        self.range_scan_stream(
            None,
            None,
            Some(crate::range_merger::rows_per_fragment()),
            start,
            end,
        )
    }

    /// Projection-aware intra-partition streaming variant of
    /// [`Self::range_iter_projected`]. Same fragment reassembly contract as
    /// [`Self::range_iter_fragmented`]; SSTable cells outside `wanted` are
    /// byte-skipped per row.
    pub fn range_iter_projected_fragmented(
        &self,
        wanted: Vec<u16>,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Arc<Partition>>> + Send>>
    where
        F: Send + Sync + 'static,
    {
        self.range_scan_stream(
            Some(wanted),
            None,
            Some(crate::range_merger::rows_per_fragment()),
            start,
            end,
        )
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
        self.with_retried_scan("read_token_range", || {
            self.read_token_range_once(start_token, end_token, limit)
        })
    }

    fn read_token_range_once(
        &self,
        start_token: i64,
        end_token: i64,
        limit: usize,
    ) -> Result<Vec<Partition>> {
        let guard = self.view.load();
        let schema = self.schema.load();
        let in_range = |t: i64| t >= start_token && t < end_token;
        let mut matched: Vec<Partition> = Vec::new();

        // Active memtable: BORROWED scan. `range_iter` would share the memtable's
        // `Arc`, so a concurrent `put` on any in-range partition would find
        // `Arc::make_mut` at refcount > 1 and copy-on-write the whole partition
        // while this scan holds it. Borrowing keeps the memtable sole owner; the
        // clone we take is the one this function already needs to return.
        guard
            .active
            .for_each_partition(None, None, &mut |p: &Partition| {
                if matched.len() >= limit {
                    return false;
                }
                if in_range(p.key.token.0) {
                    matched.push(p.clone());
                }
                true
            });

        // Flushing memtable (if any).
        if matched.len() < limit {
            for flushing in guard.flushing.iter().map(|sealed| &sealed.memtable) {
                for p in flushing.range_iter(None, None) {
                    if matched.len() >= limit {
                        break;
                    }
                    if in_range(p.key.token.0) {
                        matched.push(Arc::unwrap_or_clone(p));
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
        // Staged fan-in: process each SSTable's iterator to completion (for the
        // window) and drop its reader before opening the next, so at most ONE
        // SSTable reader is resident at any instant in this loop — well within
        // `fanin_cap`. Token-prune by descriptor bounds before opening.
        for (i, desc) in guard.sstables.iter().enumerate() {
            if matched.len() >= limit {
                break;
            }
            if !desc.overlaps_token_range(start_token, end_token) {
                continue;
            }
            let sstable = self
                .open_reader(desc)
                .map_err(|e| self.unreadable_sstable("read_token_range", "open", desc, &e))?;
            let mapping = ColumnOrdinalMapping::for_header(&schema, sstable.header());
            let mut iter = sstable
                .partitions_iter()
                .map_err(|e| self.unreadable_sstable("read_token_range", "iter", desc, &e))?;
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
                        return Err(self.unreadable_sstable(
                            "read_token_range",
                            "decode",
                            desc,
                            &e,
                        ));
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
        let (table_delete, probe_corrupt) = self.table_deletion(&guard)?;
        if let Some(c) = probe_corrupt {
            // `with_retried_scan` retries this typed error against a fresh view:
            // the retired input's merged output holds the rows.
            return Err(ferrosa_common::Error::corrupt_sstable(
                c.gen,
                c.min_token,
                c.max_token,
            ));
        }
        for p in &mut merged {
            merge::apply_table_deletion(p, table_delete);
        }
        Ok(merged
            .into_iter()
            .filter(|p| !crate::table_tombstone::is_table_tombstone_key(&p.key))
            .take(limit)
            .collect())
    }

    /// Approximate heap footprint of a materialised partition, used to bound
    /// the working set of [`Self::read_token_range_bounded`] by bytes. Sums
    /// key, clustering, and cell-value bytes plus a small per-cell overhead;
    /// an estimate is sufficient for a memory budget.
    fn partition_heap_bytes(p: &Partition) -> usize {
        fn row_bytes(r: &Row) -> usize {
            r.clustering.len()
                + r.cells
                    .iter()
                    .map(|(_, c)| c.value.as_ref().map_or(0, |v| v.len()) + 16)
                    .sum::<usize>()
        }
        p.key.key.as_bytes().len()
            + p.static_row.as_ref().map_or(0, row_bytes)
            + p.rows.iter().map(row_bytes).sum::<usize>()
    }

    /// Token-ordered, budget-bounded chunked read for anti-entropy repair.
    ///
    /// Walks `[start_token, end_token)` in strict token order via a k-way
    /// merge across the active memtable, the flushing memtable, and every
    /// active SSTable (corrupt SSTables are already excluded from
    /// `guard.sstables` at startup, so this never touches them). One streaming
    /// reader is opened per overlapping SSTable through the engine-wide pool and
    /// merged one partition at a time — SSTables are NOT staged into
    /// `Vec<Partition>` tiers, because a tier materialised every in-range
    /// partition at once and OOM-killed the node on a table whose SSTables span
    /// the full range. It materialises cell-merged partitions into the returned
    /// `Vec` until **either** `max_partitions` have been collected **or**
    /// `max_bytes` of estimated partition content has accumulated, then stops
    /// and returns the token of the next partition that would have been emitted
    /// as the resume cursor (`None` once the range is exhausted). Because the
    /// budget is checked before each partition is merged, peak working set is
    /// `max_bytes` plus one in-flight partition regardless of overlap count.
    ///
    /// Unlike [`Self::read_token_range`] — which collects up to `limit`
    /// partitions in *source* order before sorting, making it both unbounded
    /// in bytes and unable to resume a partially-read window when more than
    /// `limit` partitions fall in the span — this emits a true token-ordered
    /// prefix. Peak working set is therefore bounded by `max_bytes` plus at
    /// most one in-flight partition, and a chunked caller can resume
    /// deterministically from the returned cursor.
    ///
    /// At least one partition is always emitted when the range is non-empty
    /// (even if it alone exceeds `max_bytes`) so a chunked caller always makes
    /// forward progress. A decode error from any source is propagated (fail
    /// loud) rather than silently truncating the chunk.
    pub fn read_token_range_bounded(
        &self,
        start_token: i64,
        end_token: i64,
        max_partitions: usize,
        max_bytes: usize,
    ) -> Result<(Vec<Arc<Partition>>, Option<i64>)> {
        if start_token >= end_token || max_partitions == 0 {
            return Ok((Vec::new(), None));
        }
        self.with_retried_scan("read_token_range_bounded", || {
            self.read_token_range_bounded_once(start_token, end_token, max_partitions, max_bytes)
        })
    }

    fn read_token_range_bounded_once(
        &self,
        start_token: i64,
        end_token: i64,
        max_partitions: usize,
        max_bytes: usize,
    ) -> Result<(Vec<Arc<Partition>>, Option<i64>)> {
        let guard = self.view.load();
        let schema = self.schema.load();
        let in_range = |t: i64| t >= start_token && t < end_token;
        // The table tombstone, resolved once against this view and folded into
        // every merged partition below (a TRUNCATE must hide rows table-wide).
        // An unconsultable overlapping SSTable is a typed, retriable error here:
        // `with_retried_scan` retries it against a fresh view.
        let (table_delete, probe_corrupt) = self.table_deletion(&guard)?;
        if let Some(c) = probe_corrupt {
            return Err(ferrosa_common::Error::corrupt_sstable(
                c.gen,
                c.min_token,
                c.max_token,
            ));
        }

        // Token-ordered, peekable source streams. Memtable sources are staged
        // into sorted vecs (already range-filtered, so bounded by matches);
        // SSTable sources are streamed directly through one open reader each and
        // k-way-merged one partition at a time below. We do NOT materialise
        // SSTables into `Vec<Partition>` tiers: a tier collected every in-range
        // partition into memory at once, so over a full range on a table whose
        // SSTables each span it, peak was O(table) and OOM-killed the node. The
        // streaming merge holds one decoded partition per source in flight, so
        // peak DATA is O(open sources) — and crucially the budget check happens
        // BEFORE the next partition is merged, so peak working set never exceeds
        // `max_bytes` plus one in-flight partition regardless of how many
        // SSTables overlap. Reader structs are small and the pool + compaction
        // bound the overlap count, so the open-reader COUNT is acceptable; strict
        // reader-count bounding under full overlap (external multi-pass) is out
        // of scope. The token-ordered prefix and resume cursor are preserved.
        let mut vec_sources: Vec<PartitionSource> = Vec::new();

        // Active memtable: BORROWED scan, one partition at a time. `range_iter`
        // would hand out an `Arc` shared with the memtable, so every concurrent
        // `put` on an in-range partition would find `Arc::make_mut` with a
        // refcount > 1 and copy-on-write the whole partition for as long as this
        // scan holds it. Borrowing keeps the memtable the sole owner, and the one
        // clone we do take is wrapped in a FRESH `Arc` (refcount 1, not shared
        // with the memtable) for `PartitionSource`. Only the active tier matters
        // here: sealed/flushing memtables take no writes, so their `Arc`s are
        // free and stay as they are below.
        let mut mem_active: Vec<Arc<Partition>> = Vec::new();
        guard
            .active
            .for_each_partition(None, None, &mut |p: &Partition| {
                if in_range(p.key.token.0) {
                    mem_active.push(Arc::new(p.clone()));
                }
                true
            });
        mem_active.sort_by(|a, b| a.key.cmp(&b.key));
        vec_sources.push(PartitionSource::new(mem_active));

        // One source per sealed memtable: two of them can hold the same key.
        for sealed in guard.flushing.iter() {
            let mut mem_flushing: Vec<Arc<Partition>> = sealed
                .memtable
                .range_iter(None, None)
                .filter(|p: &Arc<Partition>| in_range(p.key.token.0))
                .collect();
            mem_flushing.sort_by(|a, b| a.key.cmp(&b.key));
            vec_sources.push(PartitionSource::new(mem_flushing));
        }

        // Open one streaming reader per overlapping SSTable. Hold the opened
        // `Arc`s for the lifetime of the borrowed iterators so the pool cannot
        // evict mid-scan. (No tier materialisation — see the source-stream note
        // above and `walk_token_range_for_digest`.)
        let overlapping: Vec<&SstableDescriptor> = guard
            .sstables
            .iter()
            .filter(|d| d.overlaps_token_range(start_token, end_token))
            .collect();

        let mut sst_readers: Vec<Arc<SSTableReader<F::Reader>>> = Vec::new();
        for desc in overlapping.iter() {
            sst_readers.push(self.open_reader(desc).map_err(|e| {
                self.unreadable_sstable("read_token_range_bounded", "open", desc, &e)
            })?);
        }

        let mut sst_iters: Vec<ferrosa_sstable::reader::PartitionIter<'_, _>> =
            Vec::with_capacity(sst_readers.len());
        let mut sst_mappings: Vec<ColumnOrdinalMapping> = Vec::with_capacity(sst_readers.len());
        // Parallel to `sst_iters`: names the SSTable behind each iterator so a
        // mid-merge failure is a typed, retriable error rather than a raw one.
        let mut sst_descs: Vec<&SstableDescriptor> = Vec::with_capacity(sst_readers.len());
        for (sstable, desc) in sst_readers.iter().zip(overlapping.iter()) {
            let mut iter = sstable.partitions_iter().map_err(|e| {
                self.unreadable_sstable("read_token_range_bounded", "iter", desc, &e)
            })?;
            position_iter_at_token(&mut iter, start_token).map_err(|e| {
                self.unreadable_sstable("read_token_range_bounded", "position", desc, &e)
            })?;
            sst_iters.push(iter);
            sst_descs.push(*desc);
            sst_mappings.push(ColumnOrdinalMapping::for_header(&schema, sstable.header()));
        }

        let mut out: Vec<Arc<Partition>> = Vec::new();
        let mut out_bytes: usize = 0;
        let pick = |cur: &Option<DecoratedKey>, candidate: &DecoratedKey| -> bool {
            cur.as_ref().map(|k| candidate < k).unwrap_or(true)
        };
        let next_cursor = loop {
            // Smallest key across all sources (SSTable keys past the range end
            // are ignored; tiers and memtables are pre-filtered to the range).
            let mut smallest_key: Option<DecoratedKey> = None;
            for src in vec_sources.iter_mut() {
                if let Some(p) = src.peek() {
                    if pick(&smallest_key, &p.key) {
                        smallest_key = Some(p.key.clone());
                    }
                }
            }
            for (iter, desc) in sst_iters.iter_mut().zip(sst_descs.iter()) {
                let peeked = iter.peek_partition_key().map_err(|e| {
                    self.unreadable_sstable("read_token_range_bounded", "peek", desc, &e)
                })?;
                if let Some(k) = peeked {
                    if k.token.0 >= end_token {
                        continue;
                    }
                    if pick(&smallest_key, &k) {
                        smallest_key = Some(k);
                    }
                }
            }
            let Some(key) = smallest_key else {
                break None;
            };

            // Stop once the budget is hit — but only after at least one
            // partition, so a single oversized partition can't stall progress.
            if !out.is_empty() && (out.len() >= max_partitions || out_bytes >= max_bytes) {
                break Some(key.token.0);
            }

            // Gather every source that holds this key, cell-merge, dedup.
            // Memtable sources hand out `Arc<Partition>` — no deep clone on the
            // read path; only a genuine multi-source group materialises.
            let mut sources: Vec<Arc<Partition>> = Vec::new();
            for src in vec_sources.iter_mut() {
                if src.peek().map(|p| p.key == key) == Some(true) {
                    sources.push(src.next().expect("peeked key must exist"));
                }
            }
            for (i, iter) in sst_iters.iter_mut().enumerate() {
                let desc = sst_descs[i];
                let peeked = iter.peek_partition_key().map_err(|e| {
                    self.unreadable_sstable("read_token_range_bounded", "peek", desc, &e)
                })?;
                if matches!(peeked, Some(k) if k == key) {
                    let next = iter.next_partition().map_err(|e| {
                        self.unreadable_sstable("read_token_range_bounded", "decode", desc, &e)
                    })?;
                    if let Some(mut p) = next {
                        sst_mappings[i].remap_partition(&mut p);
                        sources.push(Arc::new(p));
                    }
                }
            }
            let is_multi = sources.len() > 1;
            let needs_table_suppression = !table_delete.is_live();
            let merged = if !is_multi {
                // Single source: serve the `Arc` straight through unless a
                // tombstone (the partition's own, or the table's) means deletion
                // suppression would rewrite it.
                let only = sources.pop().expect("len checked");
                if needs_table_suppression
                    || crate::range_merger::partition_needs_deletion_suppression(&only)
                {
                    let mut owned = Arc::unwrap_or_clone(only);
                    merge::apply_table_deletion(&mut owned, table_delete);
                    Arc::new(owned)
                } else {
                    only
                }
            } else {
                let owned: Vec<Partition> = sources.into_iter().map(Arc::unwrap_or_clone).collect();
                let mut merged = merge::merge_partitions(owned);
                merge::apply_table_deletion(&mut merged, table_delete);
                Arc::new(merged)
            };
            out_bytes += Self::partition_heap_bytes(&merged);
            // The reserved table-tombstone partition is consumed (so the merge
            // advances past it) but never surfaced to the caller.
            if !crate::table_tombstone::is_table_tombstone_key(&merged.key) {
                out.push(merged);
            }
        };
        Ok((out, next_cursor))
    }

    /// Per-operation open-reader budget for a token-range merge: at most this
    /// many SSTable readers may be held open at any instant. Bounded by BOTH
    /// the configured fan-in (`FERROSA_READ_MERGE_FANIN`) and the shared reader
    /// pool's capacity — opening more than the pool can hold forces a soft-cap
    /// breach (an in-use reader cannot be evicted), the exact OOM vector the
    /// repair Merkle build hit under full overlap.
    fn merge_reader_budget(&self) -> usize {
        crate::reader_pool::configured_read_merge_fanin()
            .min(self.reader_pool.capacity())
            .max(1)
    }

    /// Open the overlapping SSTable readers for `[start_token, end_token)` while
    /// holding at most [`Self::merge_reader_budget`] readers open at once.
    ///
    /// When overlap `<= budget` (the healthy, common case) this just opens each
    /// overlapping descriptor through the pool — no spill, identical to the old
    /// single-pass behaviour. When overlap `> budget` (the full-overlap bloat
    /// case that OOM-killed the node — `entity_store`/`typed_edges`), it falls
    /// back to a **bounded multi-pass cascade**: inputs are split into batches
    /// of `<= budget`, each batch is stream-merged into one ephemeral sorted-run
    /// SSTable (holding only one merged partition in flight), the batch's
    /// readers are dropped, and the runs are recursively merged the same way
    /// until `<= budget` of them remain. Every pass therefore opens at most
    /// `budget` readers, and peak materialised data stays `O(1)` per pass.
    ///
    /// The returned runs are byte-equivalent inputs to a final merge: because
    /// `merge_partitions` is associative LWW, multi-pass merging yields exactly
    /// the same per-key result as a single pass, so the digest XOR is identical.
    fn bounded_overlap_readers(
        &self,
        start_token: i64,
        end_token: i64,
    ) -> Result<Vec<MergeReader<F>>> {
        // Snapshot the overlapping descriptors WITHOUT opening any reader. The
        // descriptors are cheap (no file handles), so this never pins memory.
        let guard = self.view.load();
        let mut inputs: Vec<MergeInput<F>> = guard
            .sstables
            .iter()
            .filter(|d| d.overlaps_token_range(start_token, end_token))
            .cloned()
            .map(MergeInput::Descriptor)
            .collect();
        drop(guard);

        let budget = self.merge_reader_budget();
        // Bound the number of cascade levels: each level shrinks the input count
        // by a factor of `budget` (>= 2 once spilling engages), so this caps at
        // ceil(log_budget(initial)). The guard makes the loop provably
        // terminating (safety rule 2) and turns a logic bug into a loud error
        // instead of an unbounded spill storm.
        let mut levels_remaining: usize = 64;
        while inputs.len() > budget {
            if levels_remaining == 0 {
                return Err(ferrosa_common::Error::InvalidData(format!(
                    "bounded merge cascade exceeded depth cap with {} runs (budget {budget}) — \
                     refusing to spill further",
                    inputs.len()
                )));
            }
            levels_remaining -= 1;
            inputs = self.spill_one_level(inputs, start_token, end_token, budget)?;
        }

        // Final level: `<= budget` inputs. Open each (pooled descriptor or
        // already-open ephemeral run) so the caller can iterate them. Opening
        // `<= budget` readers respects the pool cap.
        let mut readers: Vec<MergeReader<F>> = Vec::with_capacity(inputs.len());
        for input in inputs.into_iter() {
            match input {
                MergeInput::Run(mr) => readers.push(mr),
                MergeInput::Descriptor(desc) => {
                    let reader = self
                        .open_reader(&desc)
                        .map_err(|e| self.unreadable_sstable("bounded_merge", "open", &desc, &e))?;
                    readers.push(MergeReader {
                        reader,
                        cleanup_dir: None,
                        source: Some(desc),
                    });
                }
            }
        }
        Ok(readers)
    }

    /// One cascade level: consume `inputs` in batches of `<= budget`, merging
    /// each batch into a single ephemeral sorted-run SSTable, and return the
    /// runs. Each batch's readers are opened just-in-time and dropped before the
    /// next batch opens, so peak open readers within this level is `budget`. A
    /// batch of one is passed through unchanged (no pointless rewrite).
    fn spill_one_level(
        &self,
        inputs: Vec<MergeInput<F>>,
        start_token: i64,
        end_token: i64,
        budget: usize,
    ) -> Result<Vec<MergeInput<F>>> {
        let schema = self.schema.load();
        let mut runs: Vec<MergeInput<F>> = Vec::new();
        let mut batch: Vec<MergeInput<F>> = Vec::with_capacity(budget);
        for input in inputs.into_iter() {
            batch.push(input);
            if batch.len() == budget {
                self.spill_batch(
                    std::mem::take(&mut batch),
                    start_token,
                    end_token,
                    &schema,
                    &mut runs,
                )?;
            }
        }
        if !batch.is_empty() {
            self.spill_batch(batch, start_token, end_token, &schema, &mut runs)?;
        }
        Ok(runs)
    }

    /// Open the `<= budget` inputs of one batch (pooled descriptors are opened
    /// just-in-time; ephemeral runs are already open), stream-merge them into a
    /// single ephemeral sorted-run SSTable, and push it onto `runs`. A singleton
    /// batch is moved through unchanged. All of the batch's readers are dropped
    /// on return (the `batch` vec is consumed), letting the pool reclaim their
    /// slots before the next batch opens — so peak open readers is `budget`.
    fn spill_batch(
        &self,
        batch: Vec<MergeInput<F>>,
        start_token: i64,
        end_token: i64,
        schema: &TableSchema,
        runs: &mut Vec<MergeInput<F>>,
    ) -> Result<()> {
        if batch.len() == 1 {
            runs.extend(batch);
            return Ok(());
        }

        // Open every input in the batch (just-in-time for pooled descriptors).
        let mut batch_readers: Vec<MergeReader<F>> = Vec::with_capacity(batch.len());
        for input in batch.into_iter() {
            match input {
                MergeInput::Run(mr) => batch_readers.push(mr),
                MergeInput::Descriptor(desc) => {
                    let reader = self.open_reader(&desc).map_err(|e| {
                        self.unreadable_sstable("bounded_merge", "open_batch", &desc, &e)
                    })?;
                    batch_readers.push(MergeReader {
                        reader,
                        cleanup_dir: None,
                        source: Some(desc),
                    });
                }
            }
        }

        // Position one streaming iterator per batch reader at the first in-range
        // partition. Iterators borrow the readers, so the readers must outlive
        // this scope — they do (owned by `batch_readers`).
        let mut iters: Vec<ferrosa_sstable::reader::PartitionIter<'_, F::Reader>> =
            Vec::with_capacity(batch_readers.len());
        let mut mappings: Vec<ColumnOrdinalMapping> = Vec::with_capacity(batch_readers.len());
        for mr in batch_readers.iter() {
            let mut iter = mr
                .reader
                .partitions_iter()
                .map_err(|e| self.merge_reader_failure("bounded_merge", "iter", mr, e))?;
            position_iter_at_token(&mut iter, start_token)
                .map_err(|e| self.merge_reader_failure("bounded_merge", "position", mr, e))?;
            iters.push(iter);
            // A spill run is an SSTable read back later through `for_header`,
            // so it is written in SSTable ordinal space (crate::ordinal_space).
            mappings.push(ColumnOrdinalMapping::for_rewrite(
                schema,
                mr.reader.header(),
            ));
        }

        // Conservative streaming header: equal to `build_serialization_header`'s
        // no-partition fallback (`min_timestamp = 0`). Real timestamps are
        // micros-since-epoch (>= 0), so the writer's `>= min_timestamp` delta
        // guard always holds and every value round-trips byte-identically on
        // read (the reader reconstructs `min + delta`). This lets us stream the
        // merge without a pre-scan pass over all partitions.
        let header = flush::build_serialization_header(schema, &[]);
        let mut writer = SSTableWriter::new(WriteOptions::default(), header);
        let mut wrote = 0u64;
        merge_sstable_iters(
            &mut iters,
            &mappings,
            end_token,
            |idx, cause| {
                self.merge_reader_failure("bounded_merge", "merge", &batch_readers[idx], cause)
            },
            |partition| {
                writer.add_partition(&partition)?;
                wrote += 1;
                Ok(())
            },
        )?;
        // Iterators borrow `batch_readers`; drop them so the readers (and their
        // pool slots) are released before we open the spill run.
        drop(iters);
        drop(batch_readers);

        let output = writer.finish()?;
        let (reader, cleanup_dir) = self.flush_target.open_ephemeral_reader(output)?;
        tracing::debug!(
            partitions = wrote,
            spilled = cleanup_dir.is_some(),
            "bounded merge: spilled sorted run"
        );
        runs.push(MergeInput::Run(MergeReader {
            reader: Arc::new(reader),
            cleanup_dir,
            source: None,
        }));
        Ok(())
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
    ///
    /// An SSTable in the view that cannot be opened or read fails the walk
    /// (typed [`ferrosa_common::Error::CorruptSstable`]) rather than hashing a
    /// partial range. Open-stage failures retry against a fresh view before
    /// any callback has run; once `cb` has been invoked a failure is final,
    /// since a retry would deliver partitions twice.
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
        let delivered = std::cell::Cell::new(false);
        self.with_retried_scan_guarded("walk_token_range_for_digest", &delivered, || {
            self.walk_token_range_for_digest_once(start_token, end_token, &delivered, &mut cb)
        })
    }

    fn walk_token_range_for_digest_once<Cb>(
        &self,
        start_token: i64,
        end_token: i64,
        delivered: &std::cell::Cell<bool>,
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
        let guard = self.view.load();
        let schema = self.schema.load();

        // Vec-style partition sources, each pre-sorted by key: the active and
        // flushing memtables only (range-filtered, so bounded by matches). The
        // SSTables are NOT staged here — they stream through `sst_iters` below,
        // one partition per source in flight. Each is a peekable `Vec<Partition>`
        // source whose resident length feeds the in-flight gauge (test builds).
        let mut vec_sources: Vec<PartitionSource> = Vec::new();

        // Active memtable: BORROWED scan (see the token-range producer's note) —
        // `range_iter` shares the memtable's `Arc`, so every concurrent `put` on
        // an in-range partition copy-on-writes. The one clone goes into a fresh,
        // unshared `Arc`. Sealed memtables below take no writes, so their shared
        // `Arc`s are free.
        let mut mem_active: Vec<Arc<Partition>> = Vec::new();
        guard
            .active
            .for_each_partition(None, None, &mut |p: &Partition| {
                if p.key.token.0 >= start_token && p.key.token.0 < end_token {
                    mem_active.push(Arc::new(p.clone()));
                }
                true
            });
        mem_active.sort_by(|a, b| a.key.cmp(&b.key));
        vec_sources.push(PartitionSource::new(mem_active));

        // One source per sealed memtable: two of them can hold the same key.
        for sealed in guard.flushing.iter() {
            let mut mem_flushing_vec: Vec<Arc<Partition>> = sealed
                .memtable
                .range_iter(None, None)
                .filter(|p: &Arc<Partition>| {
                    p.key.token.0 >= start_token && p.key.token.0 < end_token
                })
                .collect();
            mem_flushing_vec.sort_by(|a, b| a.key.cmp(&b.key));
            vec_sources.push(PartitionSource::new(mem_flushing_vec));
        }

        // Acquire the overlapping SSTable inputs through the BOUNDED multi-pass
        // merge: at most `merge_reader_budget()` readers are ever open at once.
        // Under healthy overlap (`<= budget`) this is a direct open of each
        // overlapping descriptor — identical to the old single pass. Under full
        // overlap with `> budget` SSTables (the `entity_store`/`typed_edges`
        // bloat that OOM-killed the node), inputs are cascaded into ephemeral
        // sorted runs so neither the open-reader COUNT nor the materialised-
        // partition DATA scales with table size. `merge_partitions` is
        // associative LWW, so the multi-pass result — and therefore the digest
        // XOR — is byte-identical to a single pass. The `MergeReader`s own their
        // readers (and any spill temp dirs) for the lifetime of the borrowed
        // iterators below, so the pool cannot evict mid-scan and spill files are
        // cleaned up on drop.
        let merge_readers = self.bounded_overlap_readers(start_token, end_token)?;
        // Attribute a mid-stream read error to the SSTable that raised it, so
        // it is the same typed, retried, quarantined error as an open failure.
        let fail = |idx: usize, stage: &'static str, cause: ferrosa_common::Error| {
            self.merge_reader_failure(
                "walk_token_range_for_digest",
                stage,
                &merge_readers[idx],
                cause,
            )
        };

        let mut sst_iters: Vec<ferrosa_sstable::reader::PartitionIter<'_, _>> =
            Vec::with_capacity(merge_readers.len());
        let mut sst_mappings: Vec<ColumnOrdinalMapping> = Vec::with_capacity(merge_readers.len());
        for mr in merge_readers.iter() {
            let mut iter = mr.reader.partitions_iter().map_err(|e| {
                self.merge_reader_failure("walk_token_range_for_digest", "iter", mr, e)
            })?;
            position_iter_at_token(&mut iter, start_token).map_err(|e| {
                self.merge_reader_failure("walk_token_range_for_digest", "position", mr, e)
            })?;
            sst_iters.push(iter);
            sst_mappings.push(ColumnOrdinalMapping::for_header(
                &schema,
                mr.reader.header(),
            ));
        }

        loop {
            // Pick the smallest key across all sources via peek.
            let mut smallest_key: Option<DecoratedKey> = None;
            let pick = |cur: &Option<DecoratedKey>, candidate: &DecoratedKey| -> bool {
                cur.as_ref().map(|k| candidate < k).unwrap_or(true)
            };
            for src in vec_sources.iter_mut() {
                if let Some(p) = src.peek() {
                    if pick(&smallest_key, &p.key) {
                        smallest_key = Some(p.key.clone());
                    }
                }
            }
            for (idx, iter) in sst_iters.iter_mut().enumerate() {
                if let Some(k) = iter
                    .peek_partition_key()
                    .map_err(|e| fail(idx, "peek", e))?
                {
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

            // Which vec sources hold `key`?
            let vec_match_indices: Vec<usize> = vec_sources
                .iter_mut()
                .enumerate()
                .filter_map(|(i, src)| match src.peek() {
                    Some(p) if p.key == key => Some(i),
                    _ => None,
                })
                .collect();
            let mut sst_match_indices: Vec<usize> = Vec::new();
            for (i, iter) in sst_iters.iter_mut().enumerate() {
                let peeked = iter.peek_partition_key().map_err(|e| fail(i, "peek", e))?;
                if matches!(peeked, Some(k) if k == key) {
                    sst_match_indices.push(i);
                }
            }
            let total_sources = vec_match_indices.len() + sst_match_indices.len();

            if total_sources == 1 && sst_match_indices.len() == 1 {
                // Hot path: single SSTable source. Use the 2-phase
                // SSTable API so no `Partition` ever materialises.
                let sst_idx = sst_match_indices[0];
                let header = sst_iters[sst_idx]
                    .next_partition_header_only()
                    .map_err(|e| fail(sst_idx, "header", e))?
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
                    let attribute = |e| fail(sst_idx, "rows", e);
                    if mapping.is_identity() {
                        stream_rows_attributed(iter_ref, |row| on_row(row), attribute)
                    } else {
                        stream_rows_attributed(
                            iter_ref,
                            |row| {
                                let mut row = row.clone();
                                mapping.remap_regular_row(&mut row);
                                on_row(&row)
                            },
                            attribute,
                        )
                    }
                };
                delivered.set(true);
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

                for &vi in &vec_match_indices {
                    // Fragment/streaming path materialises owned rows anyway
                    // (it sorts and consumes them), so unwrap the memtable's
                    // `Arc` here rather than widening the merge signature.
                    let p = Arc::unwrap_or_clone(vec_sources[vi].next().expect("peeked"));
                    let key_p = p.key.clone();
                    let deletion = p.deletion;
                    let static_row = p.static_row;
                    let mut rows = p.rows;
                    rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
                    headers.push((key_p, deletion, static_row));
                    mem_row_iters.push(rows.into_iter());
                }
                for i in &sst_match_indices {
                    let header = sst_iters[*i]
                        .next_partition_header_only()
                        .map_err(|e| fail(*i, "header", e))?;
                    if let Some((k, d, mut sr)) = header {
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
                                .map_err(|e| fail(si, "row", e))?,
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
                                .map_err(|e| fail(si, "row", e))?;
                            }
                        }
                        let row = merged_row.expect("at least one source matched");
                        on_row(&row)?;
                        drop(row);
                    }
                    Ok(())
                };
                delivered.set(true);
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

    /// Walk merged partitions in `[start_token, end_token)`. Same fail-loud
    /// contract as [`Self::walk_token_range_for_digest`]: an unreadable SSTable
    /// fails the walk; it is never skipped.
    pub fn walk_token_range<Cb>(&self, start_token: i64, end_token: i64, mut cb: Cb) -> Result<()>
    where
        Cb: FnMut(&Partition) -> Result<()>,
    {
        if start_token >= end_token {
            return Ok(());
        }
        let delivered = std::cell::Cell::new(false);
        self.with_retried_scan_guarded("walk_token_range", &delivered, || {
            self.walk_token_range_once(start_token, end_token, &delivered, &mut cb)
        })
    }

    fn walk_token_range_once<Cb>(
        &self,
        start_token: i64,
        end_token: i64,
        delivered: &std::cell::Cell<bool>,
        mut cb: Cb,
    ) -> Result<()>
    where
        Cb: FnMut(&Partition) -> Result<()>,
    {
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

        // Vec-style partition sources: active + flushing memtables only. SSTables
        // stream through `sst_iters` (one partition per source in flight) — no
        // tier materialisation. See `walk_token_range_for_digest` for the OOM
        // rationale.
        let mut vec_sources: Vec<PartitionSource> = Vec::new();

        // Active memtable: BORROWED scan (see the token-range producer's note) —
        // `range_iter` shares the memtable's `Arc`, so every concurrent `put` on
        // an in-range partition copy-on-writes. The one clone goes into a fresh,
        // unshared `Arc`. Sealed memtables below take no writes, so their shared
        // `Arc`s are free.
        let mut mem_active: Vec<Arc<Partition>> = Vec::new();
        guard
            .active
            .for_each_partition(None, None, &mut |p: &Partition| {
                if p.key.token.0 >= start_token && p.key.token.0 < end_token {
                    mem_active.push(Arc::new(p.clone()));
                }
                true
            });
        mem_active.sort_by(|a, b| a.key.cmp(&b.key));
        vec_sources.push(PartitionSource::new(mem_active));

        // One source per sealed memtable: two of them can hold the same key.
        for sealed in guard.flushing.iter() {
            let mut mem_flushing_vec: Vec<Arc<Partition>> = sealed
                .memtable
                .range_iter(None, None)
                .filter(|p: &Arc<Partition>| {
                    p.key.token.0 >= start_token && p.key.token.0 < end_token
                })
                .collect();
            mem_flushing_vec.sort_by(|a, b| a.key.cmp(&b.key));
            vec_sources.push(PartitionSource::new(mem_flushing_vec));
        }

        // Acquire the overlapping SSTable inputs through the BOUNDED multi-pass
        // merge (see `bounded_overlap_readers`): at most `merge_reader_budget()`
        // readers open at once. Direct open under healthy overlap; cascaded into
        // ephemeral sorted runs under `> budget` full overlap. `merge_partitions`
        // is associative LWW, so the multi-pass result is byte-identical to a
        // single pass — `streaming_token_range_read_is_byte_identical_to_single_pass`
        // covers this. The `MergeReader`s own their readers and any spill temp
        // dirs for the iterators' lifetime (cleaned up on drop).
        let merge_readers = self.bounded_overlap_readers(start_token, end_token)?;
        // Attribute a mid-stream read error to the SSTable that raised it, so
        // it is the same typed, retried, quarantined error as an open failure.
        let fail = |idx: usize, stage: &'static str, cause: ferrosa_common::Error| {
            self.merge_reader_failure("walk_token_range", stage, &merge_readers[idx], cause)
        };

        // For each input reader: an iter parked at the first in-range partition.
        // We do NOT decode the body — we keep only the peeked DecoratedKey.
        let mut sst_iters: Vec<ferrosa_sstable::reader::PartitionIter<'_, _>> =
            Vec::with_capacity(merge_readers.len());
        let mut sst_mappings: Vec<ColumnOrdinalMapping> = Vec::with_capacity(merge_readers.len());
        for mr in merge_readers.iter() {
            let mut iter = mr
                .reader
                .partitions_iter()
                .map_err(|e| self.merge_reader_failure("walk_token_range", "iter", mr, e))?;
            // seek_to_token can leave us BEFORE start_token (cache build
            // failed); the helper skips any pre-range partition so the
            // peeked key stays honest.
            position_iter_at_token(&mut iter, start_token)
                .map_err(|e| self.merge_reader_failure("walk_token_range", "position", mr, e))?;
            sst_iters.push(iter);
            sst_mappings.push(ColumnOrdinalMapping::for_header(
                &schema,
                mr.reader.header(),
            ));
        }

        loop {
            // Pick the smallest key across all sources, using
            // peek for SSTables (no body decode).
            let mut smallest_key: Option<DecoratedKey> = None;
            let pick = |cur: &Option<DecoratedKey>, candidate: &DecoratedKey| -> bool {
                cur.as_ref().map(|k| candidate < k).unwrap_or(true)
            };
            for src in vec_sources.iter_mut() {
                if let Some(p) = src.peek() {
                    if pick(&smallest_key, &p.key) {
                        smallest_key = Some(p.key.clone());
                    }
                }
            }
            for (idx, iter) in sst_iters.iter_mut().enumerate() {
                if let Some(k) = iter
                    .peek_partition_key()
                    .map_err(|e| fail(idx, "peek", e))?
                {
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
            for src in vec_sources.iter_mut() {
                if src.peek().map(|p| p.key == key) == Some(true) {
                    group.push(Arc::unwrap_or_clone(src.next().expect("peeked")));
                }
            }
            for (idx, iter) in sst_iters.iter_mut().enumerate() {
                let peeked = iter
                    .peek_partition_key()
                    .map_err(|e| fail(idx, "peek", e))?;
                let matches = matches!(peeked, Some(k) if k == key);
                if !matches {
                    continue;
                }
                // A decode error here is an unreadable SSTable: propagate it.
                // Dropping this source's copy of the key would hand the
                // callback a stale or empty merge as if it were complete.
                if let Some(mut p) = iter.next_partition().map_err(|e| fail(idx, "decode", e))? {
                    sst_mappings[idx].remap_partition(&mut p);
                    group.push(p);
                }
            }

            let merged = if group.len() == 1 {
                group.into_iter().next().expect("len 1")
            } else {
                let mut m = crate::merge::merge_partitions(group);
                crate::merge::apply_deletions(&mut m);
                m
            };
            delivered.set(true);
            cb(&merged)?;
            drop(merged);
        }
        Ok(())
    }

    /// Materializing range read. An SSTable in the view that cannot be opened
    /// or read fails the read (typed [`ferrosa_common::Error::CorruptSstable`])
    /// after the fresh-view retry bound; it never returns fewer rows as `Ok`.
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
        self.with_retried_scan("read_range_limited_rows", || {
            self.read_range_limited_rows_once(start, end, limit, row_limit)
        })
    }

    fn read_range_limited_rows_once(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
        row_limit: usize,
    ) -> Result<Vec<Partition>> {
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
            for flushing in guard.flushing.iter().map(|sealed| &sealed.memtable) {
                let remaining = limit.saturating_sub(all_partitions.len());
                let mut flushing_parts = flushing.snapshot_range_limited(start, end, remaining);
                trim_rows(&mut flushing_parts);
                all_partitions.extend(flushing_parts);
            }
        }

        // SSTables — read only the remaining budget from each, and when a row
        // cap is requested skip unretained rows while decoding instead of
        // materializing full wide partitions and truncating afterwards.
        for desc in guard.sstables.iter() {
            let remaining = limit.saturating_sub(all_partitions.len());
            if remaining == 0 {
                break;
            }
            // One reader open at a time — opened, drained, dropped per loop.
            let sstable = self
                .open_reader(desc)
                .map_err(|e| self.unreadable_sstable("read_range", "open", desc, &e))?;
            let mut parts = sstable
                .read_partitions_limited_rows(remaining, row_limit)
                .map_err(|e| self.unreadable_sstable("read_range", "read", desc, &e))?;
            let mapping = ColumnOrdinalMapping::for_header(&schema, sstable.header());
            for partition in &mut parts {
                mapping.remap_partition(partition);
            }
            all_partitions.extend(parts);
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
        let (table_delete, probe_corrupt) = self.table_deletion(&guard)?;
        if let Some(c) = probe_corrupt {
            // `with_retried_scan` retries this typed error against a fresh view:
            // the retired input's merged output holds the rows.
            return Err(ferrosa_common::Error::corrupt_sstable(
                c.gen,
                c.min_token,
                c.max_token,
            ));
        }
        for p in &mut merged {
            merge::apply_table_deletion(p, table_delete);
        }

        // Apply range filter and limit
        let filtered: Vec<Partition> = merged
            .into_iter()
            .filter(|p| {
                if crate::table_tombstone::is_table_tombstone_key(&p.key) {
                    return false;
                }
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

    /// Visit every row matching a secondary-index key, in row order. See
    /// [`Self::read_by_index_each_after`].
    pub fn read_by_index_each(
        &self,
        index_name: &str,
        key: &IndexKey,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> Result<()> {
        self.read_by_index_each_after(index_name, key, None, visitor)
    }

    /// Visit the rows matching a secondary-index key in row order —
    /// `(partition key, clustering)` bytes — strictly after `after` when
    /// given. The visitor owns back-pressure: [`std::ops::ControlFlow::Break`]
    /// ends the walk once a page is full, and the caller resumes it with the
    /// last row it kept as `after`.
    ///
    /// Memory is O(posting sources), never O(result): every source (the
    /// active memtable index and each SSTable sidecar) holds its postings in
    /// row order, so one `OrderedPostings` merge yields them in order, and a
    /// row held by two sources is dropped by comparing it with the previous
    /// row rather than by remembering every row seen (t_50c8bc7d). A row
    /// posting is point-read as it is reached; a partition posting (pk, []),
    /// which a partition-key index writes, streams the partition's rows in
    /// bounded chunks instead of one point read per row (t_c5bccc65).
    pub fn read_by_index_each_after(
        &self,
        index_name: &str,
        key: &IndexKey,
        after: Option<&RowPosition>,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> Result<()> {
        // An index this node does not declare cannot answer, and must not
        // answer "nothing". The planner selects indexes from the CQL schema,
        // so a consult arrives here only when schema and engine disagree — a
        // restart that failed to re-register the index, or a DROP racing the
        // read. An empty answer from here is unioned by the coordinator into
        // a short result that reads as the rows not existing (t_50c8bc7d:
        // 101,848 entities read as an empty database). Refusing is still
        // never stale: a dropped index's sidecars are not consulted.
        if !self.secondary_index_declared(index_name) {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "secondary index '{index_name}' is not declared on this node's \
                 table, so it cannot answer; refusing to report its absence as \
                 zero matching rows"
            )));
        }
        let Some(lookup_key) = self.encode_index_lookup_key(index_name, key)? else {
            return Ok(());
        };

        let guard = self.view.load();
        self.refuse_index_read_over_quarantine(&guard, None, "read_by_index_each_after")?;
        let memtables = memtable_posting_lists(&guard, index_name, &lookup_key);
        // Seek to the cursor's PARTITION, inclusive: a partition posting
        // (pk, []) sorts before every row of pk, and a page that stopped
        // inside pk must reopen it. Rows at or before the cursor are dropped
        // as they are reached.
        let from = after.map(|cursor| RowPosition {
            partition_key: cursor.partition_key.clone(),
            clustering_key: Vec::new(),
        });
        let sources =
            index_posting_sources(&guard, &memtables, index_name, &lookup_key, from.as_ref());

        // The partition last streamed whole: a row posting for it (a sidecar
        // written before partition postings) names a row already delivered.
        let mut streamed_partition: Option<&[u8]> = None;
        for position in OrderedPostings::new(sources) {
            if streamed_partition == Some(position.partition_key) {
                continue;
            }
            let resume = after
                .filter(|cursor| cursor.partition_key.as_slice() == position.partition_key)
                .map(|cursor| cursor.clustering_key.as_slice());
            let flow = if position.clustering_key.is_empty() {
                streamed_partition = Some(position.partition_key);
                self.stream_partition_rows(position.partition_key, resume, visitor)?
            } else if resume.is_some_and(|cursor| position.clustering_key <= cursor) {
                continue;
            } else {
                self.visit_indexed_row(position, visitor)?
            };
            if flow.is_break() {
                break;
            }
        }
        Ok(())
    }

    /// Whether an index key names so large a share of the table that one
    /// sequential scan beats a point read per match.
    ///
    /// Serving an index key point-reads every row it names, and each point
    /// read probes every SSTable. A tenant index over a single-tenant table
    /// names the whole table: ferrosa-memory's edge count point-read 193,181
    /// rows through `co_occurs_with`'s tenant index in 87 s (2026-09-29),
    /// where a scan takes well under a second. The postings are counted only
    /// up to the threshold, so a selective key costs a handful of posting
    /// reads, never a walk of the whole index.
    pub fn index_key_is_unselective(&self, index_name: &str, key: &IndexKey) -> Result<bool> {
        if !self.secondary_index_declared(index_name) {
            // The read path refuses an undeclared index loudly; not here.
            return Ok(false);
        }
        let Some(lookup_key) = self.encode_index_lookup_key(index_name, key)? else {
            return Ok(false);
        };
        let guard = self.view.load();
        let partitions = guard
            .sstables
            .iter()
            .map(|sstable| sstable.partition_count)
            .sum::<u64>()
            .saturating_add(guard.active.partition_count() as u64)
            .saturating_add(
                guard
                    .flushing
                    .iter()
                    .map(|sealed| sealed.memtable.partition_count() as u64)
                    .sum::<u64>(),
            );
        let threshold = usize::try_from(partitions / UNSELECTIVE_INDEX_SHARE_DENOMINATOR)
            .unwrap_or(usize::MAX)
            .max(UNSELECTIVE_INDEX_MIN_MATCHES);
        let memtables = memtable_posting_lists(&guard, index_name, &lookup_key);
        let sources = index_posting_sources(&guard, &memtables, index_name, &lookup_key, None);
        Ok(OrderedPostings::new(sources).take(threshold).count() >= threshold)
    }

    /// Point-read the one row a row posting names and hand it to `visitor`.
    /// A posting that outlived its row (deleted since it was indexed) yields
    /// nothing.
    fn visit_indexed_row(
        &self,
        position: RowPositionRef<'_>,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> Result<std::ops::ControlFlow<()>> {
        #[cfg(test)]
        INDEX_POINT_READS.with(|count| count.set(count.get() + 1));
        let decorated = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
            position.partition_key.to_vec(),
        ));
        let row = self.read_clustering_row(&decorated, position.clustering_key)?;
        Ok(match row {
            Some(partition) => visitor(partition),
            None => std::ops::ControlFlow::Continue(()),
        })
    }

    /// Stream one partition's rows to `visitor` — a partition posting means
    /// every row of the partition matches (t_c5bccc65). Rows go out one
    /// single-row `Partition` at a time, the same shape a row posting yields,
    /// strictly after `resume` when given. They are read in chunks of
    /// `rows_per_fragment` under the store-view retry policy, so memory is one
    /// chunk however wide the partition. Each full chunk advances past at
    /// least one row, and a short chunk ends the partition, so the loop is
    /// bounded by the partition's rows.
    fn stream_partition_rows(
        &self,
        partition_key: &[u8],
        resume: Option<&[u8]>,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> Result<std::ops::ControlFlow<()>> {
        let key = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
            partition_key.to_vec(),
        ));
        let chunk_rows = crate::range_merger::rows_per_fragment().max(1);
        let mut start: Option<Vec<u8>> = resume.map(<[u8]>::to_vec);
        loop {
            #[cfg(test)]
            INDEX_PARTITION_CHUNKS.with(|count| count.set(count.get() + 1));
            let chunk = match &start {
                Some(after) => self.read_limited_rows_from(&key, after, chunk_rows)?,
                None => self.read_limited_rows(&key, chunk_rows)?,
            };
            let Some(Partition {
                key: chunk_key,
                deletion,
                static_row,
                rows,
            }) = chunk
            else {
                return Ok(std::ops::ControlFlow::Continue(()));
            };
            if rows.is_empty() && start.is_none() && static_row.is_some() {
                // A partition holding only static cells still matches.
                return Ok(visitor(Partition {
                    key: chunk_key,
                    deletion,
                    static_row,
                    rows,
                }));
            }
            let more = rows.len() >= chunk_rows;
            let last = rows.last().map(|row| row.clustering.clone());
            for row in rows {
                let single = Partition {
                    key: chunk_key.clone(),
                    deletion,
                    static_row: static_row.clone(),
                    rows: vec![row],
                };
                if visitor(single).is_break() {
                    return Ok(std::ops::ControlFlow::Break(()));
                }
            }
            match (more, last) {
                (true, Some(last)) => start = Some(last),
                _ => return Ok(std::ops::ControlFlow::Continue(())),
            }
        }
    }

    /// Encode a raw query term into the index's native key space. A phonetic
    /// index is keyed by the phonetic *code* of the term (both the memtable
    /// index and the flushed sidecar store codes), so `WHERE name = 'Jon'`
    /// must look up Jon's code, not its bytes; BTree/Hash/Composite are
    /// identity-encoded. `None` for a term that encodes to nothing (e.g.
    /// empty or non-UTF-8 text on a phonetic index): it cannot match any
    /// stored entry.
    fn encode_index_lookup_key(
        &self,
        index_name: &str,
        key: &IndexKey,
    ) -> Result<Option<IndexKey>> {
        let index_type = self.index_type_for(index_name);
        crate::index::scheduler::encode_index_key(index_type, &key.0).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "secondary index '{index_name}' read key encoding failed: {e}"
            ))
        })
    }

    /// Query by secondary index restricted to ONE partition (t_430c4188):
    /// looks up the index key in the memtable index and all SSTable sidecar
    /// indexes, keeps only the postings whose `RowPosition.partition_key`
    /// equals `partition_key`, and point-reads exactly those rows.
    ///
    /// This serves the fully-keyed CQL shape `WHERE <full partition key> AND
    /// <indexed_col> = ?`: work and memory are O(rows matching the value in
    /// the partition), never O(partition rows). Postings from other partitions
    /// are dropped per batch BEFORE retention, so a value that is hot globally
    /// cannot blow the `INDEX_RESULT_CAP` bound (or memory) for a keyed query
    /// that matches only a few rows in its partition. Postings retained after
    /// keying are still capped by `INDEX_RESULT_CAP` — the same fail-loud
    /// bound as the geo candidate consult, never a silent truncation.
    ///
    /// The secondary index is keyed globally by value (not by `(partition,
    /// value)`), so this consult still walks the per-node postings list for
    /// the value; only the retained set is partition-scoped. Staleness
    /// semantics match the global streaming consult — both read the same
    /// memtable index + sidecar layers.
    pub fn read_by_index_in_partition(
        &self,
        index_name: &str,
        key: &IndexKey,
        partition_key: &[u8],
    ) -> Result<Vec<Partition>> {
        if !self.secondary_index_declared(index_name) {
            return Ok(Vec::new());
        }

        let guard = self.view.load();
        let partition_token = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
            partition_key.to_vec(),
        ))
        .token
        .0;
        self.refuse_index_read_over_quarantine(
            &guard,
            Some(partition_token),
            "read_by_index_in_partition",
        )?;

        // Same per-type key encoding as `read_by_index` (see there for why a
        // phonetic index must be probed by code, not raw bytes).
        let index_type = self.index_type_for(index_name);
        let lookup_key = match crate::index::scheduler::encode_index_key(index_type, &key.0) {
            Ok(Some(encoded)) => encoded,
            Ok(None) => return Ok(Vec::new()),
            Err(e) => {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "secondary index '{index_name}' read key encoding failed: {e}"
                )));
            }
        };
        let key = &lookup_key;

        let mut positions: Vec<RowPosition> = Vec::new();
        let mut append_in_partition = |batch: Vec<RowPosition>| -> Result<()> {
            for pos in batch {
                if pos.partition_key != partition_key {
                    continue;
                }
                if positions.len() >= INDEX_RESULT_CAP {
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "secondary index query exceeded {} row limit; \
                         use ALLOW FILTERING for unbounded scans",
                        INDEX_RESULT_CAP
                    )));
                }
                positions.push(pos);
            }
            Ok(())
        };

        // 1. Memtable indexes: the active memtable's and the flushing one's.
        for idx in memtable_indexes_named(&guard, index_name) {
            append_in_partition(idx.lookup(key))?;
        }

        // 2. SSTable sidecar indexes (same lenient per-sidecar error handling
        // as `read_by_index`: a missing/failed sidecar contributes nothing).
        for sidecar in guard.sidecar_indexes.iter() {
            if let Some(reader) = sidecar.get(index_name) {
                let results = reader.lookup(key).map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "secondary index '{index_name}' read failed: {e}"
                    ))
                })?;
                append_in_partition(results)?;
            }
        }

        // 3. Deduplicate by clustering key (the partition key is fixed).
        let mut seen = std::collections::HashSet::new();
        positions.retain(|p| seen.insert(p.clustering_key.clone()));

        // 4. Point-read exactly the matching rows — never the whole partition.
        let dk = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
            partition_key.to_vec(),
        ));
        let mut partitions = Vec::new();
        for pos in &positions {
            let read = if pos.clustering_key.is_empty() {
                self.read(&dk)
            } else {
                self.read_clustering_row(&dk, &pos.clustering_key)
            };
            let partition = read?;
            if let Some(partition) = partition {
                partitions.push(partition);
            }
        }

        Ok(partitions)
    }

    /// Query a geo (cell-id) secondary index by a set of `[start, end]` cell-id
    /// ranges, returning the matching base-table partitions.
    ///
    /// Unlike a keyed secondary-index lookup — which does a point lookup on an
    /// exact key — a geo index is keyed by an 8-byte big-endian
    /// space-filling-curve cell id, so a spatial query maps to a small bounded
    /// set of contiguous cell-id ranges (produced by `ferrosa_index::geo::cover_*`).
    /// Each `(start, end)` is an **inclusive** range of `u64` cell ids; this
    /// scans the ordered memtable index and every SSTable sidecar for entries
    /// whose big-endian key falls inside any range, deduplicates by
    /// `(partition_key, clustering_key)`, and fetches the rows.
    ///
    /// The same fail-loud `INDEX_RESULT_CAP` bound as the keyed consult applies:
    /// the candidate set is never silently truncated — exceeding the cap returns
    /// an error suggesting `ALLOW FILTERING`. The geo cover ranges are already
    /// bounded (<= a few thousand cells), so the candidate count is bounded by
    /// the data that actually lives in those cells. Refinement with exact
    /// distance / containment is the caller's responsibility (the cover is an
    /// over-approximation).
    pub fn read_by_index_cell_ranges(
        &self,
        index_name: &str,
        ranges: &[(u64, u64)],
    ) -> Result<Vec<Partition>> {
        if !self.secondary_index_declared(index_name) {
            return Ok(Vec::new());
        }

        let guard = self.view.load();
        self.refuse_index_read_over_quarantine(&guard, None, "read_by_index_cell_ranges")?;

        let mut positions: Vec<RowPosition> = Vec::new();
        let mut append_positions = |batch: Vec<RowPosition>| -> Result<()> {
            if positions.len().saturating_add(batch.len()) > INDEX_RESULT_CAP {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "geo index query exceeded {} row limit; \
                     use ALLOW FILTERING for unbounded scans",
                    INDEX_RESULT_CAP
                )));
            }
            positions.extend(batch);
            Ok(())
        };

        // Encode each (start, end) cell id to its big-endian key bounds once.
        let key_ranges: Vec<(IndexKey, IndexKey)> = ranges
            .iter()
            .map(|(start, end)| {
                (
                    IndexKey(start.to_be_bytes().to_vec()),
                    IndexKey(end.to_be_bytes().to_vec()),
                )
            })
            .collect();

        // 1. Memtable indexes (active and flushing): ordered range scan per range.
        for idx in memtable_indexes_named(&guard, index_name) {
            for (start_key, end_key) in &key_ranges {
                append_positions(idx.range(start_key, end_key))?;
            }
        }

        // 2. SSTable sidecar indexes: ordered range scan per range.
        //
        // A failing range read is REPORTED, not skipped. This used to be
        // `if let Ok(results) = ...` with no else, so an unreadable sidecar —
        // a short read, a truncated file, a decode failure — contributed no
        // positions and the query returned the rows it could find as though
        // they were all of them. A caller cannot tell that from a genuinely
        // smaller result, which is how unreadable data gets reported as absent
        // data.
        for sidecar in guard.sidecar_indexes.iter() {
            if let Some(reader) = sidecar.get(index_name) {
                for (start_key, end_key) in &key_ranges {
                    let results = reader.range(start_key, end_key).map_err(|e| {
                        ferrosa_common::Error::InvalidFormat(format!(
                            "geo index '{index_name}' range read failed: {e}"
                        ))
                    })?;
                    append_positions(results)?;
                }
            }
        }

        // 3. Deduplicate by (partition_key, clustering_key). A point may appear
        //    in overlapping ranges and across memtable + sidecars.
        let mut seen = std::collections::HashSet::new();
        positions.retain(|p| seen.insert((p.partition_key.clone(), p.clustering_key.clone())));

        // 4. Fetch base-table rows, carrying the clustering key so wide clustered
        //    tables do not materialize the whole partition per hit.
        let mut partitions = Vec::new();
        for pos in &positions {
            let dk = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
                pos.partition_key.clone(),
            ));
            let read = if pos.clustering_key.is_empty() {
                self.read(&dk)
            } else {
                self.read_clustering_row(&dk, &pos.clustering_key)
            };
            let partition = read?;
            if let Some(partition) = partition {
                partitions.push(partition);
            }
        }

        Ok(partitions)
    }

    /// Replace the view with one derived from the current view, by
    /// compare-and-swap: if another writer (a flush, a compaction swap, a
    /// sidecar install) replaced the view meanwhile, `derive` runs again on
    /// the newer one, so no change is lost. `derive` must be a pure function
    /// of the view it is given; it returns the next view (or `None` to leave
    /// the view alone) and a result for the caller.
    fn update_view<R>(
        &self,
        what: &'static str,
        mut derive: impl FnMut(&StoreView) -> (Option<StoreView>, R),
    ) -> Result<R> {
        crate::lockfree::update(&self.view, what, |current| {
            let (next, result) = derive(current);
            if let Some(next) = next.as_ref() {
                next.check_invariants(what);
            }
            (next, result)
        })
    }

    /// Add `scopes` to the scopes recorded for vector index `index_name`.
    ///
    /// A failure is logged, not returned: the flush that calls this has
    /// written its sidecars, and a scope left unrecorded makes
    /// `ann_search_partitions` miss that scope's sidecar until the next
    /// flush records it again.
    fn record_vector_scopes<'a>(
        &self,
        index_name: &str,
        scopes: impl Iterator<Item = &'a Vec<u8>>,
    ) {
        if let Err(e) = self.try_record_vector_scopes(index_name, scopes) {
            tracing::error!(
                index_name,
                %e,
                "vector index scopes not recorded; scoped ANN reads miss them until the next flush"
            );
        }
    }

    /// Add `scopes` to the scopes recorded for vector index `index_name`, or
    /// say why not.
    fn try_record_vector_scopes<'a>(
        &self,
        index_name: &str,
        scopes: impl Iterator<Item = &'a Vec<u8>>,
    ) -> Result<()> {
        let scopes: Vec<&Vec<u8>> = scopes.collect();
        crate::lockfree::update(&self.vector_index_scopes, "vector index scopes", |map| {
            let known = map.get(index_name);
            if scopes
                .iter()
                .all(|scope| known.is_some_and(|set| set.contains(*scope)))
            {
                return (None, ());
            }
            let mut set = known.map(|set| (**set).clone()).unwrap_or_default();
            set.extend(scopes.iter().map(|scope| (*scope).clone()));
            let mut next = map.clone();
            next.insert(index_name.to_string(), Arc::new(set));
            (Some(next), ())
        })
    }

    /// The live generations of this table, by number.
    fn live_generations(&self) -> std::collections::HashSet<u64> {
        self.view
            .load()
            .sstable_ids
            .iter()
            .filter_map(|(gen, _dir)| gen.parse().ok())
            .collect()
    }

    /// Rebuild, from the sidecar files themselves, the scopes that have a
    /// scoped sidecar for `index_name` in a live SSTable and the generations
    /// whose scoped sidecars are complete.
    ///
    /// Both live in memory and were otherwise filled only by flushes, so
    /// after a restart the scope set was empty: `ann_search_partitions`
    /// probed no flushed sidecar and ANN answered from the memtable alone.
    /// The files are the durable record, so this runs whenever the index is
    /// registered, which a restart does from `system_schema.indexes`.
    ///
    /// A generation is complete when it has a manifest whose byte count
    /// matches its scoped sidecars on disk. Any other live generation (no
    /// manifest: compacted before compaction wrote vector sidecars, flushed
    /// before manifests existed, or a build that crashed) is left for
    /// [`Self::plan_vector_sidecar_repair`]; ANN refuses over it until then.
    ///
    /// # Errors
    ///
    /// The sidecars cannot be listed, or one carries a scope that cannot be
    /// decoded. Registration fails rather than leave the index answering
    /// without those rows.
    fn recover_vector_scopes(&self, index_name: &str) -> Result<()> {
        let live = self.live_generations();
        let manifest_name = vector_manifest_name(index_name);
        let mut scoped_bytes: HashMap<u64, u64> = HashMap::new();
        let mut with_manifest = Vec::new();
        let mut scopes = std::collections::HashSet::new();
        for file in self.flush_target.list_vector_sidecars()? {
            if !live.contains(&file.generation) {
                continue;
            }
            if file.name == manifest_name {
                with_manifest.push(file.generation);
            } else if let Some(scope) = scope_of_vector_sidecar(index_name, &file.name) {
                scopes.insert(scope?);
                let total = scoped_bytes.entry(file.generation).or_default();
                *total = total.saturating_add(file.len);
            }
        }
        let mut ready = 0usize;
        for gen in with_manifest {
            let on_disk = scoped_bytes.get(&gen).copied().unwrap_or(0);
            match self.read_vector_manifest(gen, index_name) {
                Ok(Some(manifest)) if manifest.bytes == on_disk => {
                    self.mark_vector_ready(&gen.to_string(), index_name);
                    ready += 1;
                }
                other => tracing::warn!(
                    index_name,
                    gen,
                    manifest = ?other,
                    scoped_bytes_on_disk = on_disk,
                    "vector sidecars of a generation do not match their manifest; \
                     rebuilding them from its rows"
                ),
            }
        }
        tracing::info!(
            index_name,
            scopes = scopes.len(),
            ready_generations = ready,
            pending_generations = live.len().saturating_sub(ready),
            "recovered vector sidecars from disk"
        );
        self.record_vector_scopes(index_name, scopes.iter());
        Ok(())
    }

    /// Generation `gen`'s manifest for `index_name`: `Ok(None)` if it has
    /// none; an `Err` if it exists but is unreadable or malformed.
    fn read_vector_manifest(
        &self,
        gen: u64,
        index_name: &str,
    ) -> Result<Option<VectorSidecarManifest>> {
        let Some(bytes) = self
            .flush_target
            .read_vector_sidecar(gen, &vector_manifest_name(index_name))?
        else {
            return Ok(None);
        };
        VectorSidecarManifest::decode(&bytes)
            .map(Some)
            .ok_or_else(|| {
                ferrosa_common::Error::InvalidData(format!(
                    "vector manifest of index {index_name} in generation {gen} is malformed"
                ))
            })
    }

    fn mark_vector_ready(&self, gen: &str, index_name: &str) {
        if let Err(e) = self.vector_ready.insert(&vector_ready_key(gen, index_name)) {
            // Left unmarked, ANN refuses over this generation and the repair
            // rebuilds it: slower, never wrong.
            tracing::error!(%e, gen, index_name, "could not mark a generation's vector sidecars complete");
        }
    }

    fn unmark_vector_ready(&self, gen: &str, index_name: &str) {
        if let Err(e) = self.vector_ready.remove(&vector_ready_key(gen, index_name)) {
            tracing::error!(%e, gen, index_name, "could not unmark a generation's vector sidecars");
        }
    }

    /// Record a flush's scoped sidecars for `cfg` complete: write the
    /// manifest, then mark the generation ready. Runs before the flush
    /// installs the generation, so ANN never sees it unmarked. A manifest
    /// that cannot be written leaves the generation to the repair.
    fn complete_flushed_vector_sidecars(
        &self,
        gen: u64,
        cfg: &VectorIndexConfig,
        manifest: VectorSidecarManifest,
    ) {
        match self.flush_target.write_vector_sidecar(
            gen,
            &vector_manifest_name(&cfg.index_name),
            &manifest.encode(),
        ) {
            Ok(()) => self.mark_vector_ready(&gen.to_string(), &cfg.index_name),
            Err(e) => tracing::error!(
                %e,
                index_name = %cfg.index_name,
                gen,
                "store: vector manifest persist failed; the generation is rebuilt from its \
                 rows before ANN answers over it"
            ),
        }
    }

    /// Live generations whose scoped sidecars for `index_name` are not known
    /// complete. ANN over the index refuses while any exist.
    pub fn vector_generations_pending(&self, index_name: &str) -> Vec<String> {
        let declared = self
            .catalog()
            .vector_index_configs
            .iter()
            .any(|cfg| cfg.index_name == index_name);
        if !declared {
            return Vec::new();
        }
        self.view
            .load()
            .sstable_ids
            .iter()
            .filter(|(gen, _)| !self.is_sstable_quarantined(gen))
            .filter(|(gen, _)| {
                !self
                    .vector_ready
                    .contains(&vector_ready_key(gen, index_name))
            })
            .map(|(gen, _)| gen.clone())
            .collect()
    }

    /// Refuse an ANN query over `index_name` while a live generation's vector
    /// sidecars are incomplete: answering would silently leave out its rows.
    /// The error is backpressure (retryable); the repair clears it.
    fn require_vector_generations_ready(&self, index_name: &str) -> Result<()> {
        let pending = self.vector_generations_pending(index_name);
        if pending.is_empty() {
            return Ok(());
        }
        let schema = self.schema.load();
        Err(ferrosa_common::Error::Overloaded {
            reason: format!(
                "vector index {index_name} is being rebuilt for {} generation(s) (e.g. {}); \
                 ANN would miss their rows, retry shortly",
                pending.len(),
                pending[0]
            ),
            table: format!("{}.{}", schema.keyspace, schema.table),
        })
    }

    /// Delete every vector sidecar of `index_name` (global, scoped and
    /// manifests) and forget which generations were complete.
    ///
    /// Registration rebuilds the scope set from these files, so a dropped
    /// index's sidecars must not outlive it: a later index created under the
    /// same name, possibly on another column, would otherwise answer ANN from
    /// the dropped index's vectors.
    fn remove_vector_sidecars(&self, index_name: &str) -> Result<()> {
        let manifest_name = vector_manifest_name(index_name);
        for file in self.flush_target.list_vector_sidecars()? {
            if file.name == index_name
                || file.name == manifest_name
                || scope_of_vector_sidecar(index_name, &file.name).is_some()
            {
                self.flush_target
                    .remove_vector_sidecar(file.generation, &file.name)?;
            }
        }
        let suffix = format!("/{index_name}");
        for set in [&self.vector_ready, &self.vector_verified] {
            for key in set.to_vec() {
                if key.ends_with(&suffix) {
                    set.remove(&key)?;
                }
            }
        }
        Ok(())
    }

    /// Delete generation `gen`'s scoped sidecars and manifest for
    /// `index_name` and mark it incomplete, before it is rebuilt. Its global
    /// sidecar, if a flush wrote one, is kept for [`Self::ann_search`].
    fn clear_generation_vector_sidecars(&self, gen: u64, index_name: &str) -> Result<()> {
        self.unmark_vector_ready(&gen.to_string(), index_name);
        let manifest_name = vector_manifest_name(index_name);
        for file in self.flush_target.list_vector_sidecars()? {
            if file.generation == gen
                && (file.name == manifest_name
                    || scope_of_vector_sidecar(index_name, &file.name).is_some())
            {
                self.flush_target.remove_vector_sidecar(gen, &file.name)?;
            }
        }
        Ok(())
    }

    /// Build generation `desc`'s scoped vector sidecars for `cfg` from its
    /// rows, record their scopes, and only then mark it complete.
    fn rebuild_generation_vector_sidecars(
        &self,
        desc: &SstableDescriptor,
        cfg: &VectorIndexConfig,
        schema: &TableSchema,
    ) -> Result<VectorSidecarManifest> {
        let gen: u64 = desc.gen.parse().map_err(|_| {
            ferrosa_common::Error::InvalidData(format!(
                "generation id {} is not numeric; its vector sidecars cannot be named",
                desc.gen
            ))
        })?;
        let reader = self.open_reader(desc)?;
        self.clear_generation_vector_sidecars(gen, &cfg.index_name)?;
        self.build_vector_sidecars_from(&reader, gen, cfg, schema)
    }

    /// Write `reader`'s scoped sidecars and manifest for `cfg` as generation
    /// `gen` and record the scopes. The caller marks the generation complete
    /// ([`Self::complete_vector_build`]) once it has counted the build, so
    /// anyone who sees it complete also sees it counted.
    fn build_vector_sidecars_from(
        &self,
        reader: &SSTableReader<F::Reader>,
        gen: u64,
        cfg: &VectorIndexConfig,
        schema: &TableSchema,
    ) -> Result<VectorSidecarManifest> {
        // Keys only, one per partition with a vector: the scope set this
        // feeds is held in memory for the whole index anyway.
        let mut scopes = std::collections::HashSet::new();
        let manifest =
            build_generation_vector_sidecars(&*self.flush_target, reader, schema, gen, cfg, |s| {
                scopes.insert(s.to_vec());
            })?;
        self.try_record_vector_scopes(&cfg.index_name, scopes.iter())?;
        Ok(manifest)
    }

    /// Mark generation `gen`'s freshly built sidecars for `index_name`
    /// verified and complete: ANN answers over it from here on.
    fn complete_vector_build(&self, gen: &str, index_name: &str) {
        if let Err(e) = self
            .vector_verified
            .insert(&vector_ready_key(gen, index_name))
        {
            tracing::error!(%e, gen, index_name, "could not mark freshly built vector sidecars verified; they are decoded once more");
        }
        self.mark_vector_ready(gen, index_name);
    }

    /// Rebuild, from their rows, the scoped vector sidecars of every live
    /// generation whose sidecars for `index_name` are not known complete:
    /// SSTables compacted before compaction wrote vector sidecars, flushed
    /// before manifests existed, written while the index did not exist, or
    /// left behind by a build that crashed.
    ///
    /// One generation at a time, each streamed a partition at a time, so
    /// memory is bounded by one partition's vectors. Resumable: a generation
    /// counts as done only once its manifest is written, so an interrupted
    /// run redoes just the generation it was in. Single-flight per
    /// generation and index; a generation another run is building is
    /// skipped, not waited for.
    pub fn repair_vector_sidecars(&self, index_name: &str) -> VectorRepairOutcome {
        let mut outcome = VectorRepairOutcome::default();
        let Some(cfg) = self
            .catalog()
            .vector_index_configs
            .iter()
            .find(|cfg| cfg.index_name == index_name)
            .cloned()
        else {
            return outcome;
        };
        let schema = self.schema.load_full();
        let pending = self.vector_generations_pending(index_name);
        crate::metrics::add_vector_generations_pending(pending.len() as i64);
        for gen in pending {
            crate::metrics::add_vector_generations_pending(-1);
            let key = vector_ready_key(&gen, index_name);
            let Some(_claim) = SidecarClaim::take(&self.vector_sidecars_in_flight, key.clone())
            else {
                outcome.in_flight_elsewhere += 1;
                continue;
            };
            if self.vector_ready.contains(&key) {
                continue;
            }
            // Compacted away since the plan: its successor is its own job.
            let Some(desc) = self
                .view
                .load()
                .sstables
                .iter()
                .find(|desc| desc.gen == gen)
                .cloned()
            else {
                continue;
            };
            let start = Instant::now();
            match self.rebuild_generation_vector_sidecars(&desc, &cfg, &schema) {
                Ok(manifest) => {
                    outcome.repaired += 1;
                    outcome.vectors += manifest.vectors;
                    crate::metrics::vector_generation_repaired();
                    crate::metrics::index_repaired(
                        index_name,
                        self.take_vector_invalid_reason(&key).as_str(),
                    );
                    self.complete_vector_build(&gen, index_name);
                    tracing::info!(
                        index_name,
                        gen = %gen,
                        vectors = manifest.vectors,
                        scopes = manifest.scopes,
                        elapsed_ms = start.elapsed().as_millis() as u64,
                        "vector repair: rebuilt a generation's sidecars from its rows"
                    );
                }
                Err(e) => {
                    outcome.failed += 1;
                    crate::metrics::vector_generation_repair_failed();
                    tracing::error!(
                        %e,
                        index_name,
                        gen = %gen,
                        "vector repair: could not rebuild a generation's sidecars; ANN over \
                         the index keeps refusing until it is rebuilt"
                    );
                }
            }
        }
        outcome
    }

    /// The names of this table's vector indexes.
    pub fn vector_index_names(&self) -> Vec<String> {
        self.catalog()
            .vector_index_configs
            .iter()
            .map(|cfg| cfg.index_name.clone())
            .collect()
    }

    /// Record the dimension `index_name`'s column declares, which
    /// [`Self::verify_vector_index`] holds every sidecar to.
    pub fn set_vector_index_dimension(&self, index_name: &str, dimension: usize) {
        let recorded =
            crate::lockfree::update(&self.vector_dimensions, "vector index dimensions", |map| {
                if map.get(index_name) == Some(&dimension) {
                    return (None, ());
                }
                let mut next = map.clone();
                next.insert(index_name.to_string(), dimension);
                (Some(next), ())
            });
        if let Err(e) = recorded {
            tracing::error!(%e, index_name, dimension, "vector index dimension not recorded; its sidecars are not checked against it");
        }
    }

    /// Stop trusting generation `gen`'s sidecars for `index_name`: ANN over
    /// the index refuses until the repair rebuilds them, and the repair is
    /// labelled with `reason`.
    pub(crate) fn invalidate_vector_generation(
        &self,
        gen: &str,
        index_name: &str,
        reason: VectorInvalidReason,
    ) {
        let key = vector_ready_key(gen, index_name);
        let noted = crate::lockfree::update(
            &self.vector_invalid_reasons,
            "vector invalid reasons",
            |map| {
                let mut next = map.clone();
                next.insert(key.clone(), reason);
                (Some(next), ())
            },
        );
        if let Err(e) = noted {
            tracing::error!(%e, gen, index_name, "vector invalidation reason not recorded; the repair is labelled missing_sidecar");
        }
        if let Err(e) = self.vector_verified.remove(&key) {
            tracing::error!(%e, gen, index_name, "could not clear a generation's verified mark");
        }
        self.unmark_vector_ready(gen, index_name);
    }

    /// The recorded reason `gen/index` is incomplete, cleared as it is read.
    fn take_vector_invalid_reason(&self, key: &str) -> VectorInvalidReason {
        let mut taken = None;
        let cleared = crate::lockfree::update(
            &self.vector_invalid_reasons,
            "vector invalid reasons",
            |map| {
                taken = map.get(key).copied();
                if taken.is_none() {
                    return (None, ());
                }
                let mut next = map.clone();
                next.remove(key);
                (Some(next), ())
            },
        );
        if let Err(e) = cleared {
            tracing::error!(%e, key, "vector invalidation reason not cleared");
        }
        taken.unwrap_or(VectorInvalidReason::Missing)
    }

    /// Check `index_name`'s complete generations against the files on disk
    /// and the index declaration, invalidating any that fail so the repair
    /// rebuilds them. Each tick of the self-heal controller runs this.
    ///
    /// Every pass: a complete generation must still have its manifest and
    /// scoped sidecars adding up to it (a), and every scope with a sidecar
    /// must be in the scope set (e; restored in place, no rebuild needed).
    /// Once per generation, for at most `decode_budget` not yet verified:
    /// each sidecar must decode (b), the sidecars must hold the manifest's
    /// vector and scope counts (c), and every vector must have the declared
    /// dimension (d). Memory is one sidecar at a time.
    ///
    /// # Errors
    ///
    /// The sidecars cannot be listed.
    pub fn verify_vector_index(
        &self,
        index_name: &str,
        decode_budget: usize,
    ) -> Result<VectorVerifyOutcome> {
        let mut outcome = VectorVerifyOutcome::default();
        let declared = self
            .catalog()
            .vector_index_configs
            .iter()
            .any(|cfg| cfg.index_name == index_name);
        if !declared {
            return Ok(outcome);
        }
        let manifest_name = vector_manifest_name(index_name);
        // Per generation: scoped sidecars (scope, name, bytes) and whether a
        // manifest is present. Names only, never sidecar contents.
        let mut scoped: HashMap<u64, Vec<(Vec<u8>, String, u64)>> = HashMap::new();
        let mut with_manifest = std::collections::HashSet::new();
        for file in self.flush_target.list_vector_sidecars()? {
            if file.name == manifest_name {
                with_manifest.insert(file.generation);
            } else if let Some(scope) = scope_of_vector_sidecar(index_name, &file.name) {
                scoped
                    .entry(file.generation)
                    .or_default()
                    .push((scope?, file.name, file.len));
            }
        }
        let dimension = self.vector_dimensions.load().get(index_name).copied();
        let recorded_scopes = self.vector_index_scopes.load().get(index_name).cloned();
        let mut missing_scopes = Vec::new();
        for (gen_str, _) in self.view.load().sstable_ids.iter() {
            let key = vector_ready_key(gen_str, index_name);
            let Ok(gen) = gen_str.parse::<u64>() else {
                continue;
            };
            if !self.vector_ready.contains(&key) {
                continue;
            }
            let files = scoped.get(&gen).map(Vec::as_slice).unwrap_or(&[]);
            missing_scopes.extend(
                files
                    .iter()
                    .filter(|(scope, _, _)| {
                        !recorded_scopes
                            .as_ref()
                            .is_some_and(|set| set.contains(scope))
                    })
                    .map(|(scope, _, _)| scope.clone()),
            );
            let invalid = if !with_manifest.contains(&gen) {
                Some(VectorInvalidReason::Missing)
            } else if self.vector_verified.contains(&key) {
                self.check_manifest_sizes(gen, index_name, files).err()
            } else if outcome.verified < decode_budget {
                outcome.verified += 1;
                self.check_generation_sidecars(gen, index_name, files, dimension)
                    .err()
            } else {
                None
            };
            if let Some(reason) = invalid {
                tracing::warn!(
                    index_name,
                    gen,
                    reason = reason.as_str(),
                    "vector index: a generation's sidecars are invalid; ANN over the index \
                     refuses until the repair rebuilds them from its rows"
                );
                self.invalidate_vector_generation(gen_str, index_name, reason);
                outcome.invalidated.push((gen_str.clone(), reason));
            }
        }
        if !missing_scopes.is_empty() {
            outcome.scopes_restored = missing_scopes.len();
            tracing::warn!(
                index_name,
                scopes = missing_scopes.len(),
                "vector index: scopes with sidecars on disk were missing from the scope set; \
                 ANN never probed them. Restored."
            );
            self.try_record_vector_scopes(index_name, missing_scopes.iter())?;
            crate::metrics::index_repaired(index_name, VectorInvalidReason::ScopeSet.as_str());
        }
        outcome.pending = self.vector_generations_pending(index_name).len();
        Ok(outcome)
    }

    /// A verified generation's manifest still matches its scoped sidecars'
    /// count and size on disk. Metadata only.
    fn check_manifest_sizes(
        &self,
        gen: u64,
        index_name: &str,
        files: &[(Vec<u8>, String, u64)],
    ) -> std::result::Result<(), VectorInvalidReason> {
        let manifest = self
            .read_vector_manifest(gen, index_name)
            .map_err(|_| VectorInvalidReason::Corrupt)?
            .ok_or(VectorInvalidReason::Missing)?;
        let bytes: u64 = files.iter().map(|(_, _, len)| *len).sum();
        if manifest.scopes != files.len() as u64 {
            return Err(VectorInvalidReason::CountMismatch);
        }
        if manifest.bytes != bytes {
            // A sidecar changed size since it was written: truncated or
            // overwritten.
            return Err(VectorInvalidReason::Corrupt);
        }
        Ok(())
    }

    /// Decode every scoped sidecar of `gen` for `index_name` and hold it to
    /// the manifest and the declared dimension; mark it verified if it
    /// passes.
    fn check_generation_sidecars(
        &self,
        gen: u64,
        index_name: &str,
        files: &[(Vec<u8>, String, u64)],
        dimension: Option<usize>,
    ) -> std::result::Result<(), VectorInvalidReason> {
        self.check_manifest_sizes(gen, index_name, files)?;
        let manifest = self
            .read_vector_manifest(gen, index_name)
            .map_err(|_| VectorInvalidReason::Corrupt)?
            .ok_or(VectorInvalidReason::Missing)?;
        let mut vectors = 0u64;
        for (_, name, _) in files {
            let bytes = self
                .flush_target
                .read_vector_sidecar(gen, name)
                .map_err(|_| VectorInvalidReason::Corrupt)?
                .ok_or(VectorInvalidReason::Missing)?;
            let stats = ferrosa_index::vector::hnsw::inspect_bytes(&bytes)
                .map_err(|_| VectorInvalidReason::Corrupt)?;
            if let (Some(declared), Some(found)) = (dimension, stats.dimension) {
                if declared != found {
                    return Err(VectorInvalidReason::DimensionMismatch);
                }
            }
            vectors = vectors.saturating_add(stats.vectors as u64);
        }
        if vectors != manifest.vectors {
            return Err(VectorInvalidReason::CountMismatch);
        }
        if let Err(e) = self
            .vector_verified
            .insert(&vector_ready_key(&gen.to_string(), index_name))
        {
            // Unmarked, it is decoded again next pass: costlier, never wrong.
            tracing::error!(%e, gen, index_name, "could not mark a generation's vector sidecars verified");
        }
        Ok(())
    }

    /// Whether a [`Self::run_vector_repair`] for `index_name` is running.
    pub fn vector_repair_running(&self, index_name: &str) -> bool {
        self.vector_sidecars_in_flight
            .contains(&format!("run/{index_name}"))
    }

    /// Repair `index_name`'s incomplete generations (see
    /// [`Self::repair_vector_sidecars`]) unless another run is already doing
    /// so, logging the edges: a WARN when it starts with work to do, and one
    /// line when ANN can answer again or why it still cannot. `None` if
    /// another run holds the index.
    pub fn run_vector_repair(&self, index_name: &str) -> Option<VectorRepairOutcome> {
        let _run =
            SidecarClaim::take(&self.vector_sidecars_in_flight, format!("run/{index_name}"))?;
        let pending = self.vector_generations_pending(index_name).len();
        if pending == 0 {
            return Some(VectorRepairOutcome::default());
        }
        let schema = self.schema.load();
        let table = format!("{}.{}", schema.keyspace, schema.table);
        crate::metrics::set_index_invalid(&table, index_name, pending as u64);
        tracing::warn!(
            %table,
            index_name,
            pending,
            "vector repair: generations without complete vector sidecars; ANN over this \
             index refuses (retryable) until they are rebuilt from their rows"
        );
        let start = Instant::now();
        let outcome = self.repair_vector_sidecars(index_name);
        let still_pending = self.vector_generations_pending(index_name).len();
        crate::metrics::set_index_invalid(&table, index_name, still_pending as u64);
        if still_pending == 0 {
            tracing::warn!(
                %table,
                index_name,
                repaired = outcome.repaired,
                vectors = outcome.vectors,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "vector repair: complete; ANN over this index answers again"
            );
        } else {
            tracing::error!(
                %table,
                index_name,
                ?outcome,
                still_pending,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "vector repair: generations remain incomplete; ANN over this index keeps \
                 refusing until a later repair succeeds"
            );
        }
        Some(outcome)
    }

    /// Build a compaction output's scoped vector sidecars for every vector
    /// index, BEFORE the swap that makes it live, and mark it complete, so
    /// ANN finds the compacted rows from the instant their inputs leave the
    /// view. A failure is logged; the output then swaps in incomplete, ANN
    /// refuses over the index, and the repair rebuilds it.
    pub fn build_vector_sidecars_for_output(
        &self,
        output_gen: &str,
        reader: &SSTableReader<F::Reader>,
    ) -> VectorRepairOutcome {
        let mut outcome = VectorRepairOutcome::default();
        let catalog = self.catalog();
        if catalog.vector_index_configs.is_empty() {
            return outcome;
        }
        let Ok(gen) = output_gen.parse::<u64>() else {
            outcome.failed = catalog.vector_index_configs.len();
            tracing::error!(
                output_gen,
                "compaction: output generation id is not numeric; its vector sidecars cannot \
                 be named, ANN refuses over the table's vector indexes"
            );
            return outcome;
        };
        let schema = self.schema.load_full();
        for cfg in catalog.vector_index_configs.iter() {
            match self.build_vector_sidecars_from(reader, gen, cfg, &schema) {
                Ok(manifest) => {
                    outcome.repaired += 1;
                    outcome.vectors += manifest.vectors;
                    self.complete_vector_build(output_gen, &cfg.index_name);
                }
                Err(e) => {
                    outcome.failed += 1;
                    tracing::error!(
                        %e,
                        index_name = %cfg.index_name,
                        output_gen,
                        "compaction: output vector sidecars could not be built; ANN refuses \
                         over the index until the repair rebuilds them"
                    );
                }
            }
        }
        outcome
    }

    /// Forget that generation `gen`'s vector sidecars were complete, for every
    /// index: it left the view. Its files go with the SSTable's.
    fn forget_generation_vector_ready(&self, gen: &str) {
        for cfg in self.catalog().vector_index_configs.iter() {
            self.unmark_vector_ready(gen, &cfg.index_name);
            let key = vector_ready_key(gen, &cfg.index_name);
            if let Err(e) = self.vector_verified.remove(&key) {
                tracing::error!(%e, gen, "could not clear a retired generation's verified mark");
            }
            self.take_vector_invalid_reason(&key);
        }
    }

    /// Delete a compaction output's vector sidecars of every index and forget
    /// it was complete: the output was discarded and never entered the view.
    pub fn discard_generation_vector_sidecars(&self, gen: &str) -> Result<()> {
        self.forget_generation_vector_ready(gen);
        let Ok(gen) = gen.parse::<u64>() else {
            return Ok(());
        };
        for file in self.flush_target.list_vector_sidecars()? {
            if file.generation == gen {
                self.flush_target.remove_vector_sidecar(gen, &file.name)?;
            }
        }
        Ok(())
    }

    /// Forget every scope recorded for `index_name`; whether there were any.
    fn forget_vector_scopes(&self, index_name: &str) -> Result<bool> {
        crate::lockfree::update(&self.vector_index_scopes, "vector index scopes", |map| {
            if !map.contains_key(index_name) {
                return (None, false);
            }
            let mut next = map.clone();
            next.remove(index_name);
            (Some(next), true)
        })
    }

    /// Run one index DDL: rotate the memtable onto the catalog `edit` derives
    /// from the current one. See [`Self::flush_applying_catalog_edit`] for why
    /// a rotation, and not an edit of the live memtable's indexes, is what
    /// keeps index postings equal to the flushed sidecars without a lock.
    ///
    /// `edit` returns `false` to leave the catalog unchanged (an idempotent
    /// re-declaration); the memtable still rotates, which is harmless.
    /// Returns what the rotation's flush did (see
    /// [`Self::flush_applying_catalog_edit`]).
    fn rotate_catalog(
        &self,
        edit: impl FnOnce(&mut IndexCatalog) -> bool + Send + 'static,
    ) -> Result<FlushOutcome> {
        self.flush_applying_catalog_edit(|current| {
            let mut next = current.clone();
            edit(&mut next).then_some(next)
        })
    }

    /// Retrieve a named memtable-level secondary index.
    ///
    /// Dynamically adds a secondary index. Rows already written are flushed by
    /// the memtable rotation that publishes it, and their sidecar for the new
    /// index is built from those rows; future writes are indexed.
    pub fn add_index(
        &self,
        index_name: String,
        column_position: usize,
        index_type: IndexType,
    ) -> Result<FlushOutcome> {
        self.add_index_with_predicate(index_name, column_position, index_type, None)
    }

    /// Dynamically adds a secondary index carrying an optional partial-index
    /// [`FilterPredicate`]. Rows already written are flushed by the rotation
    /// that publishes the index, with a sidecar built from them under the same
    /// predicate; future writes update it. Re-declaring an index with the same
    /// definition changes nothing; a different definition replaces the old one
    /// (the engine refuses that case before it gets here).
    pub fn add_index_with_predicate(
        &self,
        index_name: String,
        column_position: usize,
        index_type: IndexType,
        filter_predicate: Option<FilterPredicate>,
    ) -> Result<FlushOutcome> {
        self.rotate_catalog(move |catalog| {
            let wanted = Some((0, column_position, index_type, filter_predicate.as_ref()));
            if catalog.scalar_definition(&index_name) == wanted {
                return false;
            }
            catalog.remove(&index_name);
            catalog
                .indexed_columns
                .push((index_name.clone(), column_position));
            catalog.index_types.insert(index_name.clone(), index_type);
            if let Some(pred) = filter_predicate {
                catalog.index_filter_predicates.insert(index_name, pred);
            }
            true
        })
    }

    /// Removes a declared secondary index from every live store layer.
    ///
    /// This is the storage-side inverse of [`add_index`](Self::add_index) /
    /// [`add_clustering_index`](Self::add_clustering_index): future writes no
    /// longer update a memtable index, reads no longer consult active or
    /// sidecar state for the name, and vector-index metadata is unwired too.
    /// Persisted scalar and full-text sidecar files are left as inert orphan
    /// artifacts; the declaration metadata and read guards stop naming them
    /// immediately. Vector sidecars are deleted, because registering a vector
    /// index rebuilds its scopes from whatever sidecars carry its name.
    /// Returns whether anything named `index_name` was declared, and what the
    /// rotation's flush did.
    pub fn remove_index(&self, index_name: &str) -> Result<(bool, FlushOutcome)> {
        let declared = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let found = Arc::clone(&declared);
        let name = index_name.to_string();
        let outcome = self.rotate_catalog(move |catalog| {
            let removed = catalog.remove(&name);
            found.store(removed, std::sync::atomic::Ordering::Release);
            removed
        })?;
        let mut removed = declared.load(std::sync::atomic::Ordering::Acquire);
        removed |= self.forget_vector_scopes(index_name)?;
        self.remove_vector_sidecars(index_name)?;
        Ok((removed, outcome))
    }

    /// Dynamically adds a secondary index on a CLUSTERING column
    /// (t_430c4188). Future writes extract the indexed value from the row's
    /// composite clustering-key bytes at `clustering_component` — a
    /// clustering column's value is not a cell, so the cell-based
    /// [`add_index`](Self::add_index) path cannot see it at all. Rows already
    /// written are flushed by the rotation and indexed from their keys.
    pub fn add_clustering_index(
        &self,
        index_name: String,
        clustering_component: usize,
        index_type: IndexType,
    ) -> Result<FlushOutcome> {
        self.rotate_catalog(move |catalog| {
            if catalog.declares(&index_name) {
                return false;
            }
            catalog
                .indexed_clustering_columns
                .push((index_name.clone(), clustering_component));
            catalog.index_types.insert(index_name, index_type);
            true
        })
    }

    /// Clustering-column secondary index declarations:
    /// `(index_name, clustering_component)` pairs.
    pub fn indexed_clustering_columns(&self) -> CatalogSlice<(String, usize)> {
        CatalogSlice {
            catalog: self.catalog(),
            project: |catalog| &catalog.indexed_clustering_columns,
        }
    }

    /// Dynamically adds a secondary index on a PARTITION-KEY column.
    ///
    /// Future writes extract the indexed value from the row's composite
    /// partition-key bytes at `partition_key_component`. A partition-key
    /// value is not a cell, so the cell-based [`add_index`](Self::add_index)
    /// path cannot see it at all — the same reason
    /// [`add_clustering_index`](Self::add_clustering_index) exists.
    ///
    /// Without this, `CREATE INDEX ... (tenant_id)` on
    /// `PRIMARY KEY ((tenant_id, session_id), entity_id)` was accepted and
    /// built nothing, and every read through it returned no rows over a table
    /// full of data.
    pub fn add_partition_key_index(
        &self,
        index_name: String,
        partition_key_component: usize,
        index_type: IndexType,
    ) -> Result<FlushOutcome> {
        self.rotate_catalog(move |catalog| {
            if catalog.declares(&index_name) {
                return false;
            }
            catalog
                .indexed_partition_key_columns
                .push((index_name.clone(), partition_key_component));
            catalog.index_types.insert(index_name, index_type);
            true
        })
    }

    /// Partition-key secondary index declarations:
    /// `(index_name, partition_key_component)` pairs.
    /// The definition of one partition-key index: which key component it
    /// covers and what kind it is.
    ///
    /// `None` means this store has no such index — which is the answer a
    /// rebuild needs, so it can refuse rather than build nothing and report
    /// success.
    pub fn partition_key_index_def(&self, index_name: &str) -> Option<(usize, IndexType)> {
        let catalog = self.catalog();
        let component = catalog
            .indexed_partition_key_columns
            .iter()
            .find(|(n, _)| n == index_name)
            .map(|(_, c)| *c)?;
        Some((component, catalog.index_type_for(index_name)))
    }

    pub fn indexed_partition_key_columns(&self) -> CatalogSlice<(String, usize)> {
        CatalogSlice {
            catalog: self.catalog(),
            project: |catalog| &catalog.indexed_partition_key_columns,
        }
    }

    /// Number of partition-key columns in this table's schema — the component
    /// count needed to split composite partition-key bytes.
    pub fn partition_key_column_count(&self) -> usize {
        let schema = self.schema.load();
        let kt = &schema.key_type;
        if !kt.contains("CompositeType") {
            return 1;
        }
        // CompositeType(A,B,...) — count the comma-separated inner types.
        kt.split_once('(')
            .map(|(_, rest)| rest.trim_end_matches(')').split(',').count())
            .unwrap_or(1)
    }

    /// Number of clustering columns in this table's schema (the component
    /// count needed to split composite clustering-key bytes).
    pub fn clustering_column_count(&self) -> usize {
        self.schema.load().clustering_columns.len()
    }

    /// Retrieve a named memtable-level secondary index.
    ///
    /// Returns `None` if no index with the given name was declared at
    /// construction time. The returned `Arc` is a snapshot — it remains
    /// valid even after a flush swaps in fresh indexes.
    /// Test-only: for each SSTable in the view that has a sidecar for
    /// `index_name`, whether that sidecar is read through a memory map.
    #[cfg(test)]
    pub(crate) fn sidecar_backings_for_test(&self, index_name: &str) -> Vec<bool> {
        let guard = self.view.load();
        guard
            .sidecar_indexes
            .iter()
            .filter_map(|sidecars| sidecars.get(index_name).map(SidecarReader::is_mapped))
            .collect()
    }

    pub fn get_memtable_index(&self, name: &str) -> Option<Arc<MemtableIndex>> {
        let guard = self.view.load();
        guard.indexes.get(name).cloned()
    }

    /// Whether this table declares the secondary index `index_name`. A global
    /// read of an undeclared index is refused by [`Self::read_by_index_each`].
    pub fn secondary_index_declared(&self, index_name: &str) -> bool {
        let catalog = self.catalog();
        catalog.index_types.contains_key(index_name)
            || catalog
                .indexed_columns
                .iter()
                .any(|(name, _)| name == index_name)
            || catalog
                .indexed_clustering_columns
                .iter()
                .any(|(name, _)| name == index_name)
    }

    /// Returns the current secondary index declarations, as one snapshot.
    pub fn indexed_columns(&self) -> CatalogSlice<(String, usize)> {
        CatalogSlice {
            catalog: self.catalog(),
            project: |catalog| &catalog.indexed_columns,
        }
    }

    /// The declared `IndexType` for a named secondary index, defaulting to
    /// `BTree` when unknown. Used by the eager / backfill / compaction index
    /// build paths so a job carries the index's real type.
    pub fn index_type_for(&self, index_name: &str) -> IndexType {
        self.catalog().index_type_for(index_name)
    }

    /// The partial-index [`FilterPredicate`] for a named index, if any. `Some`
    /// only for [`IndexType::Filtered`] indexes that were registered with a
    /// predicate (and thus survives reload). Owned: the catalog it comes from
    /// can be replaced by DDL at any time.
    pub fn filter_predicate_for(&self, index_name: &str) -> Option<FilterPredicate> {
        self.catalog()
            .index_filter_predicates
            .get(index_name)
            .cloned()
    }

    /// Returns the current full-text index declarations, as one snapshot.
    pub fn fulltext_indexes(&self) -> CatalogSlice<(String, usize)> {
        CatalogSlice {
            catalog: self.catalog(),
            project: |catalog| &catalog.fulltext_indexes,
        }
    }

    /// Search active/flushing memtables for a declared full-text index.
    ///
    /// Persisted FTI sidecars are produced only on flush, so read-after-write
    /// queries must also consult the live memtable tiers. This builds a small
    /// transient FTI over the current memtable snapshots and runs the same query
    /// evaluator used by sidecar readers so fresh rows and flushed rows follow
    /// the same matching semantics.
    ///
    /// `limit` is the query-derived `LIMIT k` bound: when `Some(k)` only the k
    /// best-scoring hits are retained (bounded top-k, t_ee98faa0 layer 2). The
    /// transient index is queried in place — it is never serialized and
    /// re-deserialized (that round trip used to hold 3× the indexed text).
    pub fn fulltext_memtable_search(
        &self,
        index_name: &str,
        query: &str,
        limit: Option<usize>,
    ) -> ferrosa_common::Result<Vec<(Vec<u8>, f64)>> {
        use ferrosa_index::fulltext::builder::FullTextIndexBuilder;
        use ferrosa_index::fulltext::query::parse_fts_query;
        use ferrosa_index::fulltext::reader::FullTextIndexReader;

        let catalog = self.catalog();
        let Some((_, col_pos)) = catalog
            .fulltext_indexes
            .iter()
            .find(|(name, _)| name == index_name)
        else {
            return Ok(vec![]);
        };

        let parsed_query = parse_fts_query(query).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("fts_match query error: {e}"))
        })?;

        let guard = self.view.load();
        // Reserve the term map up front: it is the largest allocation on this
        // path and otherwise rehashes as terms accumulate.
        let estimated_terms = guard
            .active
            .partition_count()
            .saturating_mul(8)
            .clamp(64, 1 << 16);
        let mut builder = FullTextIndexBuilder::with_capacity(estimated_terms);
        // Reusable per-row scratch, hoisted OUT of the row loop: the text buffer,
        // the token buffer and the term-frequency map are cleared and reused, so
        // a scan of N rows does not allocate N of each.
        let mut scratch = crate::fulltext_scratch::RowScratch::new();
        let mut add_partition = |partition: &Partition| {
            let pk_bytes = partition.key.key.as_bytes();
            // Per-row document keyed by the full primary key (t_da51e20c).
            for row in &partition.rows {
                scratch.reset();
                for (col_idx, cell) in &row.cells {
                    if *col_idx as usize == *col_pos {
                        if let Some(ref val) = cell.value {
                            if let Ok(s) = std::str::from_utf8(val) {
                                scratch.push_text(s);
                            }
                        }
                    }
                }
                if scratch.has_text() {
                    let doc_key =
                        ferrosa_index::fulltext::keys::encode_doc_key(pk_bytes, &row.clustering);
                    // Move the buffers out so the builder owns them while it
                    // runs; they are returned to the scratch afterwards and
                    // reused for the next row.
                    let mut tf = std::mem::take(&mut scratch.tf);
                    let mut tokens = std::mem::take(&mut scratch.tokens);
                    builder.add_document_with_tf(
                        doc_key,
                        scratch.text_trimmed(),
                        &mut tf,
                        &mut tokens,
                    );
                    scratch.tf = tf;
                    scratch.tokens = tokens;
                }
            }
        };
        // BOUNDED-HOLD scan, not `for_each_partition`: the fulltext build does
        // real work per row (analyze + term-frequency fold), so holding the
        // read guard across it would stall a concurrent writer to that partition
        // for the whole partition. `for_each_partition_cloned` clones under the
        // guard and releases it BEFORE the callback, so the writer waits for a
        // memcpy (I-5). The clone does not inflate the memtable's refcount, so
        // writes still merge in place (I-2); only one partition is live at a
        // time, so the table is not materialized (I-4).
        guard
            .active
            .for_each_partition_cloned(None, None, &mut |partition| add_partition(partition));
        for flushing in guard.flushing.iter().map(|sealed| &sealed.memtable) {
            flushing
                .for_each_partition_cloned(None, None, &mut |partition| add_partition(partition));
        }
        drop(guard);

        let fti = builder.build();
        if fti.doc_count == 0 {
            return Ok(vec![]);
        }

        let reader = FullTextIndexReader::from_index(fti);
        let hits = match limit {
            Some(k) => reader.search_top_k(&parsed_query, k),
            None => reader.search(&parsed_query),
        };
        Ok(hits
            .into_iter()
            .map(|hit| (hit.partition_key, hit.score))
            .collect())
    }

    /// Full-text search over LIVE SSTables that have NO on-disk FTI sidecar.
    ///
    /// `covered_gens` are the SSTable generations that already have a
    /// `{gen}-FTI-{index}.db` sidecar on disk (searched directly by the engine).
    /// For every other live SSTable the sidecar may be missing transiently — e.g.
    /// after compaction swaps in the merged SSTable but before its sidecar is
    /// (re)built (the eager index build is async in production), or after a
    /// sidecar write failure. Without this fallback those rows are invisible to
    /// `fts_match`, which on a multi-node cluster made every replica's
    /// scatter-gather union empty at once → non-deterministic 0 rows
    /// (BUG-F-007 / t_0455c0a1). We build a transient FTI over each such
    /// SSTable's indexed column and run the same query, so a stable row is never
    /// transiently dropped from full-text search.
    ///
    /// Memory bounds (t_ee98faa0 layer 2): one transient FTI is built PER
    /// sidecar-less SSTable (not one across all of them) and queried in place
    /// (never serialized + re-deserialized — that combination used to hold 3×
    /// the total indexed text of every uncovered SSTable at once). Peak is
    /// O(one SSTable's indexed column); with a query-derived `limit` the
    /// retained hits are additionally bounded top-k per SSTable.
    pub fn fulltext_sstable_scan_missing_sidecar(
        &self,
        index_name: &str,
        query: &str,
        covered_gens: &std::collections::HashSet<String>,
        limit: Option<usize>,
    ) -> ferrosa_common::Result<Vec<(Vec<u8>, f64)>> {
        self.with_retried_scan("fulltext_sstable_scan_missing_sidecar", || {
            self.fulltext_sstable_scan_missing_sidecar_once(index_name, query, covered_gens, limit)
        })
    }

    /// One attempt of [`Self::fulltext_sstable_scan_missing_sidecar`]. A
    /// sidecar-less SSTable that cannot be opened or indexed fails the scan
    /// (typed `CorruptSstable`): its rows would otherwise be missing from the
    /// search with nothing telling the caller. Quarantined SSTables are NOT
    /// skipped either — they remain in the view, so their rows remain missing
    /// until repair swaps them out, and the search must say so.
    fn fulltext_sstable_scan_missing_sidecar_once(
        &self,
        index_name: &str,
        query: &str,
        covered_gens: &std::collections::HashSet<String>,
        limit: Option<usize>,
    ) -> ferrosa_common::Result<Vec<(Vec<u8>, f64)>> {
        use ferrosa_index::fulltext::query::parse_fts_query;
        use ferrosa_index::fulltext::reader::FullTextIndexReader;

        let catalog = self.catalog();
        let Some((_, col_pos)) = catalog
            .fulltext_indexes
            .iter()
            .find(|(name, _)| name == index_name)
        else {
            return Ok(vec![]);
        };
        let col_pos = *col_pos;

        let parsed_query = parse_fts_query(query).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("fts_match query error: {e}"))
        })?;

        let schema = self.schema.load();
        let guard = self.view.load();
        let mut out: Vec<(Vec<u8>, f64)> = Vec::new();
        for desc in guard.sstables.iter() {
            // SSTables with a sidecar are already covered by the engine's direct
            // sidecar search; only scan the ones that are (transiently) missing.
            if covered_gens.contains(&desc.gen) {
                continue;
            }
            #[cfg(test)]
            crate::engine::FTS_SSTABLE_FULL_SCANS.with(|c| c.set(c.get() + 1));
            let sstable = self
                .open_reader(desc)
                .map_err(|e| self.unreadable_sstable("fts_sidecarless_scan", "open", desc, &e))?;
            // One transient FTI per SSTable, dropped before the next one is
            // scanned — peak stays O(this SSTable's indexed column), not
            // O(every uncovered SSTable at once).
            let fti = build_sstable_fti(&sstable, &schema, col_pos)
                .map_err(|e| self.unreadable_sstable("fts_sidecarless_scan", "index", desc, &e))?;
            if fti.doc_count == 0 {
                continue;
            }
            // Query the transient index in place — no serialize/deserialize
            // round trip (that used to hold 3× the indexed text at once).
            let reader = FullTextIndexReader::from_index(fti);
            let hits = match limit {
                Some(k) => reader.search_top_k(&parsed_query, k),
                None => reader.search(&parsed_query),
            };
            out.extend(hits.into_iter().map(|hit| (hit.partition_key, hit.score)));
        }
        drop(guard);
        Ok(out)
    }

    /// The directory holding `desc`'s component files.
    fn descriptor_dir(&self, desc: &SstableDescriptor) -> std::path::PathBuf {
        if desc.dir.as_os_str().is_empty() {
            self.flush_target.base_dir().to_path_buf()
        } else {
            desc.dir.clone()
        }
    }

    /// `desc`'s on-disk FTI sidecar for `index_name`, if it has one: beside its
    /// own component files (a compaction output's generation directory), or in
    /// the flat table directory where flush writes them.
    fn existing_fti_sidecar(
        &self,
        desc: &SstableDescriptor,
        index_name: &str,
    ) -> Option<std::path::PathBuf> {
        let name = fti_sidecar_file_name(&desc.gen, index_name);
        let own = self.descriptor_dir(desc).join(&name);
        if own.is_file() {
            return Some(own);
        }
        let flat = self.flush_target.base_dir().join(&name);
        (flat != own && flat.is_file()).then_some(flat)
    }

    /// The FTI sidecars a query against `index_name` must read: one per LIVE,
    /// unquarantined SSTable that has one, as `(generation, path)`.
    ///
    /// Derived from the store view, never from a directory listing. Compaction
    /// leaves its inputs' index artifacts on disk, so a listing of
    /// `*-FTI-{index}.db` grows with every generation the table has EVER had —
    /// 5,122 files against 11 live SSTables on the cluster where this was found
    /// — and each of those sidecars returns keys for rows as they were before
    /// they were rewritten or compacted away.
    pub fn fulltext_live_sidecars(&self, index_name: &str) -> Vec<(String, std::path::PathBuf)> {
        let guard = self.view.load();
        guard
            .sstables
            .iter()
            .filter(|desc| !self.is_sstable_quarantined(&desc.gen))
            .filter_map(|desc| {
                self.existing_fti_sidecar(desc, index_name)
                    .map(|path| (desc.gen.clone(), path))
            })
            .collect()
    }

    /// Plan FTI sidecars for every live SSTable that has none for
    /// `index_name`. Run the plan with the engine's table lock RELEASED.
    ///
    /// Without a sidecar, each query decodes and tokenizes the SSTable in full
    /// (`fulltext_sstable_scan_missing_sidecar`). That fallback was written for
    /// a transient window, but nothing ever closed the window: compaction wrote
    /// no sidecar, so every compacted SSTable was re-tokenized on every query
    /// for the rest of its life. Building the sidecar once turns that per-query
    /// cost into a one-time one.
    ///
    /// Empty for targets that do not persist sidecars (in-memory), where the
    /// fallback scan remains the only way to search SSTable rows.
    pub fn plan_missing_fulltext_sidecars(&self, index_name: &str) -> FulltextSidecarBuild<F> {
        let mut plan = self.empty_fulltext_sidecar_build();
        if !self.flush_target.persists_fti_sidecars() {
            return plan;
        }
        let catalog = self.catalog();
        let Some((_, column_position)) = catalog
            .fulltext_indexes
            .iter()
            .find(|(name, _)| name == index_name)
        else {
            return plan;
        };
        let guard = self.view.load();
        for desc in guard.sstables.iter() {
            if self.is_sstable_quarantined(&desc.gen)
                || self.existing_fti_sidecar(desc, index_name).is_some()
            {
                continue;
            }
            // An SSTable that cannot be opened is left to the fallback scan,
            // which now fails every full-text query that needs it (typed
            // CorruptSstable) — so this skip only defers the sidecar build,
            // it hides nothing. Logged so the cause is visible on its own.
            let reader = match self.open_reader(desc) {
                Ok(reader) => reader,
                Err(e) => {
                    tracing::warn!(
                        %e,
                        gen = %desc.gen,
                        index_name,
                        "fts sidecar plan: SSTable could not be opened; sidecar not built \
                         (full-text queries over it will fail until it is readable)"
                    );
                    continue;
                }
            };
            plan.jobs.push(FulltextSidecarJob {
                gen: desc.gen.clone(),
                dir: self.descriptor_dir(desc),
                index_name: index_name.to_string(),
                column_position: *column_position,
                reader,
            });
        }
        plan
    }

    /// Plan FTI sidecars, for every full-text index, for a compaction output
    /// that is about to be swapped in. Built before the swap, a query never
    /// sees the output without its sidecar.
    pub fn plan_fulltext_sidecars_for_output(
        &self,
        gen: &str,
        dir: &std::path::Path,
        reader: &Arc<SSTableReader<F::Reader>>,
    ) -> FulltextSidecarBuild<F> {
        let mut plan = self.empty_fulltext_sidecar_build();
        if !self.flush_target.persists_fti_sidecars() {
            return plan;
        }
        plan.jobs = self
            .catalog()
            .fulltext_indexes
            .iter()
            .map(|(index_name, column_position)| FulltextSidecarJob {
                gen: gen.to_string(),
                dir: dir.to_path_buf(),
                index_name: index_name.clone(),
                column_position: *column_position,
                reader: Arc::clone(reader),
            })
            .collect();
        plan
    }

    /// Give up to `budget` live FTI sidecars written before the term index one,
    /// in place; returns how many were upgraded. Run with the engine's table
    /// lock RELEASED.
    ///
    /// Until upgraded, every lookup in such a sidecar walks its whole term
    /// dictionary (`legacy_sidecar_walks_total`). The upgrade appends to a copy
    /// (a clone on APFS), fsyncs it, and renames it over the original, so a
    /// query sees the old file or the new one, never a partial one; an open
    /// reader keeps the inode it opened. It holds the same per-sidecar claim
    /// as the backfill build. A sidecar already uploaded keeps its legacy copy
    /// in the object store, which is valid and is upgraded again if restored.
    /// Failures are logged per sidecar and leave it as it was.
    pub fn upgrade_legacy_fulltext_sidecars(&self, budget: usize) -> usize {
        if budget == 0 || !self.flush_target.persists_fti_sidecars() {
            return 0;
        }
        let mut upgraded = 0;
        let indexes: Vec<String> = self
            .catalog()
            .fulltext_indexes
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        for index_name in &indexes {
            for (gen, path) in self.fulltext_live_sidecars(index_name) {
                if upgraded >= budget {
                    return upgraded;
                }
                let Some(_claim) = SidecarClaim::take(
                    &self.fulltext_sidecars_in_flight,
                    format!("{gen}/{index_name}"),
                ) else {
                    continue; // being built or upgraded now; next pass.
                };
                match upgrade_fti_sidecar(&path, &gen, index_name) {
                    Ok(true) => {
                        upgraded += 1;
                        tracing::info!(
                            %gen,
                            index = %index_name,
                            path = %path.display(),
                            "fts: gave a legacy FTI sidecar a term index; lookups in it seek \
                             instead of walking its dictionary"
                        );
                    }
                    Ok(false) => {}
                    Err(e) => tracing::error!(
                        %e,
                        %gen,
                        index = %index_name,
                        path = %path.display(),
                        "fts: legacy FTI sidecar upgrade failed; lookups in it keep walking \
                         its whole dictionary"
                    ),
                }
            }
        }
        upgraded
    }

    fn empty_fulltext_sidecar_build(&self) -> FulltextSidecarBuild<F> {
        FulltextSidecarBuild {
            flush_target: Arc::clone(&self.flush_target),
            schema: self.schema.load_full(),
            in_flight: Arc::clone(&self.fulltext_sidecars_in_flight),
            jobs: Vec::new(),
        }
    }

    /// Register a full-text index for this table. Rows already written are
    /// flushed by the memtable rotation that publishes it, with an FTI sidecar
    /// built from them. Re-registering an existing name changes nothing.
    pub fn add_fulltext_index(
        &self,
        index_name: String,
        column_position: usize,
    ) -> Result<FlushOutcome> {
        self.rotate_catalog(move |catalog| {
            if catalog
                .fulltext_indexes
                .iter()
                .any(|(n, _)| n == &index_name)
            {
                return false;
            }
            catalog.fulltext_indexes.push((index_name, column_position));
            true
        })
    }

    /// Register a vector index for this table.
    ///
    /// Idempotent: calling twice with the same `index_name` changes nothing.
    /// The memtable rotation that publishes it flushes the rows already
    /// written, with a vector sidecar built from them, and every later write
    /// populates the in-memory vector index.
    pub fn add_vector_index(&self, config: VectorIndexConfig) -> Result<FlushOutcome> {
        self.add_vector_index_with_method(config, VectorIndexMethod::Hnsw)
    }

    /// Register a quantized IVFFlat/C-SPANN vector index for this table.
    ///
    /// Keeps `add_vector_index` as the legacy HNSW path so existing callers and
    /// sidecar artifacts remain compatible.
    pub fn add_quantized_vector_index(&self, config: VectorIndexConfig) -> Result<FlushOutcome> {
        self.add_vector_index_with_method(config, VectorIndexMethod::QuantizedIvf)
    }

    /// Report the artifact/search method registered for `index_name`.
    ///
    /// Defaults to [`VectorIndexMethod::Hnsw`] when the index is unknown or was
    /// registered through the legacy path, matching `add_vector_index`.
    pub fn vector_index_method(&self, index_name: &str) -> VectorIndexMethod {
        self.catalog().vector_index_method(index_name)
    }

    fn add_vector_index_with_method(
        &self,
        config: VectorIndexConfig,
        method: VectorIndexMethod,
    ) -> Result<FlushOutcome> {
        // Publishing an empty index over existing rows would make the planner
        // select it and turn a correct brute-force ANN query into an empty
        // result. The rotation avoids that: the rows already written leave
        // with the frozen memtable, whose flush builds their vector sidecar.
        let index_name = config.index_name.clone();
        let outcome = self.rotate_catalog(move |catalog| {
            if catalog
                .vector_index_configs
                .iter()
                .any(|c| c.index_name == config.index_name)
            {
                return false; // already registered
            }
            catalog
                .vector_index_methods
                .insert(config.index_name.clone(), method);
            catalog.vector_index_configs.push(config);
            true
        })?;
        self.recover_vector_scopes(&index_name)?;
        Ok(outcome)
    }

    /// Perform an approximate nearest-neighbor search across memtable and
    /// all flushed SSTable vector sidecars.
    ///
    /// Searches the active (and optionally flushing) memtable via brute-force,
    /// then queries each SSTable's persisted HNSW sidecar via the flush target.
    /// Results from all sources are merged, deduplicated by generation-aware row
    /// identity, sorted ascending by score, and truncated to `k`.
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
        use ferrosa_index::vector::{IndexResult, VectorRowRef};
        use std::collections::HashMap as StdHashMap;

        self.require_vector_generations_ready(index_name)?;
        let guard = self.view.load();
        let method = self.vector_index_method(index_name);
        let mut merged: StdHashMap<VectorRowRef, IndexResult> = StdHashMap::new();

        if let Some(vi) = guard.vector_indexes.get(index_name) {
            let results = vi.search(query, k, ef_search).map_err(|e| {
                ferrosa_common::Error::InvalidData(format!("ann_search memtable failed: {e}"))
            })?;
            for result in results {
                merged.insert(VectorRowRef::memtable(result.position), result);
            }
        }
        for (nth, vi) in flushing_vector_indexes(&guard, index_name)
            .iter()
            .enumerate()
        {
            let results = vi.search(query, k, ef_search).map_err(|e| {
                ferrosa_common::Error::InvalidData(format!(
                    "ann_search flushing memtable failed: {e}"
                ))
            })?;
            for result in results {
                merged.insert(
                    VectorRowRef::sstable(
                        FLUSHING_MEMTABLE_GENERATION - nth as u64,
                        result.position,
                    ),
                    result,
                );
            }
        }

        for (gen_str, _dir) in guard.sstable_ids.iter() {
            if let Ok(gen) = gen_str.parse::<u64>() {
                match method {
                    VectorIndexMethod::Hnsw => {
                        if let Some(vec_bytes) =
                            self.flush_target.read_vector_sidecar(gen, index_name)?
                        {
                            let results = ferrosa_index::vector::hnsw::search_from_bytes(
                                &vec_bytes, query, k, ef_search,
                            )
                            .map_err(|e| {
                                ferrosa_common::Error::InvalidData(format!(
                                    "vector sidecar {index_name} of generation {gen} \
                                     cannot be searched: {e}"
                                ))
                            })?;
                            for result in results {
                                merged.insert(VectorRowRef::sstable(gen, result.position), result);
                            }
                        } else if self
                            .read_vector_manifest(gen, index_name)?
                            .is_some_and(|manifest| manifest.vectors > 0)
                        {
                            // Compacted or repaired: its vectors are in scoped
                            // sidecars only, which this unscoped search cannot
                            // read. Refuse rather than leave its rows out.
                            return Err(ferrosa_common::Error::InvalidData(format!(
                                "ann_search: generation {gen} of vector index {index_name} has \
                                 only partition-scoped sidecars; use ann_search_partitions"
                            )));
                        }
                    }
                    VectorIndexMethod::QuantizedIvf => {
                        let results = self
                            .flush_target
                            .search_quantized_vector_sidecar(gen, index_name, query, k, ef_search)
                            .map_err(|e| {
                                ferrosa_common::Error::InvalidData(format!(
                                    "quantized vector artifact {index_name} of generation \
                                     {gen} cannot be searched: {e}"
                                ))
                            })?;
                        for result in results.into_iter().flatten() {
                            merged.insert(VectorRowRef::sstable(gen, result.position), result);
                        }
                    }
                }
            }
        }

        let mut all: Vec<(VectorRowRef, IndexResult)> = merged.into_iter().collect();
        all.sort_by(|a, b| {
            a.1.score
                .partial_cmp(&b.1.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        all.truncate(k);
        Ok(all.into_iter().map(|(_, result)| result).collect())
    }

    /// Perform ANN search restricted to one partition/prefix scope.
    ///
    /// The scope bytes are the serialized partition key for the v1 routing seam
    /// (tenant_id + session_id in the blueprint's schema). The active memtable
    /// filters entries by scope; flushed SSTables use per-scope vector sidecars
    /// so scoped queries avoid probing unrelated prefixes.
    pub fn ann_search_in_partition_scope(
        &self,
        index_name: &str,
        partition_scope: &[u8],
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<ferrosa_index::vector::IndexResult>> {
        use ferrosa_index::vector::{IndexResult, VectorRowRef};
        use std::collections::HashMap as StdHashMap;

        self.require_vector_generations_ready(index_name)?;
        let guard = self.view.load();
        let mut merged: StdHashMap<VectorRowRef, IndexResult> = StdHashMap::new();

        if let Some(vi) = guard.vector_indexes.get(index_name) {
            let results = vi
                .search_with_scope(query, k, ef_search, partition_scope)
                .map_err(|e| {
                    ferrosa_common::Error::InvalidData(format!(
                        "ann_search scoped memtable failed: {e}"
                    ))
                })?;
            for result in results {
                merged.insert(VectorRowRef::memtable(result.position), result);
            }
        }
        for (nth, vi) in flushing_vector_indexes(&guard, index_name)
            .iter()
            .enumerate()
        {
            let results = vi
                .search_with_scope(query, k, ef_search, partition_scope)
                .map_err(|e| {
                    ferrosa_common::Error::InvalidData(format!(
                        "ann_search scoped flushing memtable failed: {e}"
                    ))
                })?;
            for result in results {
                merged.insert(
                    VectorRowRef::sstable(
                        FLUSHING_MEMTABLE_GENERATION - nth as u64,
                        result.position,
                    ),
                    result,
                );
            }
        }

        let scoped_index_name = scoped_vector_sidecar_name(index_name, partition_scope);
        for (gen_str, _dir) in guard.sstable_ids.iter() {
            if let Ok(gen) = gen_str.parse::<u64>() {
                if let Some(vec_bytes) = self
                    .flush_target
                    .read_vector_sidecar(gen, &scoped_index_name)?
                {
                    // A sidecar that cannot be searched fails the query:
                    // skipping it would answer without this scope's rows.
                    let results = ferrosa_index::vector::hnsw::search_from_bytes(
                        &vec_bytes, query, k, ef_search,
                    )
                    .map_err(|e| {
                        // Fail this query, and queue the generation for the
                        // repair so the next one is not failed the same way.
                        self.invalidate_vector_generation(
                            gen_str,
                            index_name,
                            VectorInvalidReason::Corrupt,
                        );
                        ferrosa_common::Error::InvalidData(format!(
                            "scoped vector sidecar {scoped_index_name} of generation {gen} \
                             cannot be searched: {e}"
                        ))
                    })?;
                    for result in results {
                        merged.insert(VectorRowRef::sstable(gen, result.position), result);
                    }
                }
            }
        }

        let mut all: Vec<(VectorRowRef, IndexResult)> = merged.into_iter().collect();
        all.sort_by(|a, b| {
            a.1.score
                .partial_cmp(&b.1.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        all.truncate(k);
        Ok(all.into_iter().map(|(_, result)| result).collect())
    }

    /// Consult the vector index for the `k` nearest rows and return their
    /// **base-table partitions** in ascending-score (nearest-first) order.
    ///
    /// Unlike `ann_search`, which returns placeholder `IndexResult`s whose
    /// `RowPosition::offset` cannot address a row, this method recovers the
    /// partition-key scope captured at insert time and fetches each row via the
    /// normal read path — so the router can serve `ORDER BY col ANN OF [...]
    /// LIMIT k` straight from the index without a full table scan.
    ///
    /// Sources and scope availability (Fail Loud, Never Fake):
    /// - **Active/flushing memtable**: the partition key is carried inline by
    ///   [`VectorMemtableIndex::search_with_scopes`]; rows are recovered exactly.
    /// - **Flushed scoped sidecars**: written per partition-key prefix at flush
    ///   time and remembered in `vector_index_scopes` (rebuilt from the files
    ///   when the index is registered, so it survives a restart), so each is
    ///   probed under its own scope and the partition key is recovered from
    ///   that scope.
    /// - **Flushed *global* HNSW sidecar / quantized `.qvec`**: these persist
    ///   only a placeholder offset and therefore carry **no** partition key. A
    ///   result that arrives only from such a scope-less source cannot be mapped
    ///   to a row; rather than fabricate one, this method **logs a loud warning
    ///   and skips it**. (In practice every flushed vector also lands in a
    ///   scoped sidecar, so scope recovery succeeds; the warning fires only for
    ///   legacy/external artifacts that predate scoped sidecars.)
    ///
    /// The result is bounded by `k`: at most `k` partitions are ever fetched.
    pub fn ann_search_partitions(
        &self,
        index_name: &str,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<Partition>> {
        if k == 0 {
            return Ok(Vec::new());
        }
        self.require_vector_generations_ready(index_name)?;

        // Best (lowest) score per partition-key scope. Deduping by scope keeps
        // one entry per partition and lets us bound the fetch by k.
        let mut best_by_scope: HashMap<Vec<u8>, f32> = HashMap::new();
        let mut consider = |scope: Vec<u8>, score: f32| {
            best_by_scope
                .entry(scope)
                .and_modify(|existing| {
                    if score < *existing {
                        *existing = score;
                    }
                })
                .or_insert(score);
        };

        // Source (a): active memtable — scope carried inline.
        let guard = self.view.load();
        let mut scopeless_memtable_hits = 0usize;
        let memtable_vector_indexes = guard
            .vector_indexes
            .get(index_name)
            .cloned()
            .into_iter()
            .chain(flushing_vector_indexes(&guard, index_name));
        for vi in memtable_vector_indexes {
            let results = vi.search_with_scopes(query, k, ef_search).map_err(|e| {
                ferrosa_common::Error::InvalidData(format!(
                    "ann_search_partitions memtable failed: {e}"
                ))
            })?;
            for (result, scope) in results {
                match scope {
                    Some(scope) => consider(scope, result.score),
                    None => scopeless_memtable_hits += 1,
                }
            }
        }
        if scopeless_memtable_hits > 0 {
            tracing::warn!(
                index_name,
                scopeless_memtable_hits,
                "ann_search_partitions: skipping memtable vector results without a \
                 partition-key scope — cannot recover the base row, falling back loudly \
                 (these rows will not appear in index-consult results)"
            );
        }

        // Source (b): flushed scoped sidecars — probe each remembered scope.
        let scopes: Vec<Vec<u8>> = self
            .vector_index_scopes
            .load()
            .get(index_name)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        for scope in scopes {
            let scoped =
                self.ann_search_in_partition_scope(index_name, &scope, query, k, ef_search)?;
            for result in scoped {
                consider(scope.clone(), result.score);
            }
        }

        // Rank scopes by best score, take the k nearest, then fetch their rows.
        let mut ranked: Vec<(Vec<u8>, f32)> = best_by_scope.into_iter().collect();
        ranked.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        ranked.truncate(k);

        let mut partitions = Vec::with_capacity(ranked.len());
        for (scope, _score) in ranked {
            let dk = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(scope.clone()));
            match self.read(&dk) {
                Ok(Some(partition)) => partitions.push(partition),
                Ok(None) => {
                    tracing::warn!(
                        index_name,
                        "ann_search_partitions: vector index referenced a partition that no \
                         longer exists in the base table; skipping (index/base drift)"
                    );
                }
                Err(e) => return Err(e),
            }
        }

        Ok(partitions)
    }

    /// Returns the generation number of the most recently flushed SSTable.
    pub fn last_flush_generation(&self) -> u64 {
        self.flush_target.last_generation()
    }

    /// The indexes the most recent flush wrote a sidecar for.
    ///
    /// Those generations are indexed the moment `flush` returns — the
    /// postings came from the memtable index that produced the SSTable, and
    /// the sidecar is on disk and in the view before it returns. The engine
    /// uses this to avoid queueing a rebuild for work already done, which
    /// would leave the tracker reporting a complete index as pending.
    pub fn indexes_written_by_last_flush(&self) -> Arc<Vec<String>> {
        self.last_flush_indexes.load_full()
    }

    /// Returns generation IDs for all SSTables currently in the store.
    ///
    /// Used by `add_index` to submit backfill jobs for existing SSTables.
    /// Returns IDs based on the flush target's generation counter: the most
    /// recent flush is `last_generation`, and prior ones count down from there.
    /// The generation id of every SSTable this store currently holds.
    ///
    /// Read from the live view, because that is the only thing that knows. This
    /// used to synthesise a contiguous range — `last_generation()` from the
    /// flush target, minus the live count, and every integer between — on the
    /// assumption that generations are dense and end at `last_gen`.
    ///
    /// They are not. Ids come from a separate counter (`next_sstable_id`),
    /// `advance_gen_past` jumps it on recovery and around compaction output,
    /// and compaction retires arbitrary generations. The two drift apart
    /// immediately and the range is then mostly fiction.
    ///
    /// What that cost, measured on a live cluster: a rebuild of
    /// `idx_entity_by_tenant` was handed 21 ids, found no data file for 20 of
    /// them, classified all 20 as "compacted away — nothing to index", indexed
    /// the 1 that happened to be real, and reported the index complete. The
    /// node held 8 SSTables with data and none with orphaned metadata, so the
    /// 20 were never files and 7 real ones were never scanned. Reads through
    /// that index then returned 32,632 rows where the table held 102,840 —
    /// a wrong answer with no error anywhere, which is the outcome the whole
    /// index-currency machinery exists to prevent.
    ///
    /// Six call sites in `engine.rs` take their SSTable list from here, so
    /// every index backfill inherited it, not only `ferrosa-ctl index rebuild`.
    pub fn sstable_generation_ids(&self) -> Vec<String> {
        self.view
            .load()
            .sstables
            .iter()
            .map(|descriptor| descriptor.gen.clone())
            .collect()
    }

    /// Translate a current-schema regular-column ordinal to the physical
    /// ordinal used by one SSTable's SerializationHeader.
    ///
    /// Backfill jobs read raw SSTable rows, so their `column_position` must be
    /// in the source SSTable's ordinal space. Returns `None` when the current
    /// column did not exist in that SSTable; callers should treat that
    /// generation as an empty backfill rather than probing a different old
    /// column with the same ordinal.
    pub fn source_regular_ordinal_for_sstable(
        &self,
        sstable_id: &str,
        current_ordinal: usize,
    ) -> ferrosa_common::Result<Option<usize>> {
        let Ok(current) = u16::try_from(current_ordinal) else {
            return Ok(None);
        };
        let mapping = match self.column_mapping_for_sstable(sstable_id) {
            SstableMappingOutcome::Mapped(mapping) => mapping,
            // Vanished under compaction: nothing left to build from this
            // generation; the successor SSTable carries the rows.
            SstableMappingOutcome::SstableGone => return Ok(None),
            SstableMappingOutcome::Unavailable(e) => return Err(e),
        };
        let source = mapping.source_regular_ordinals_for_projection(&[current]);
        match source.as_slice() {
            [only] => Ok(Some(*only as usize)),
            _ => Ok(None),
        }
    }

    /// Remap a filtered-index predicate from current-schema ordinals to one
    /// SSTable's physical ordinals.
    ///
    /// If any predicate column is absent from that SSTable, the correct
    /// backfill result is empty because the conjunction cannot be satisfied.
    /// An empty conjunction evaluates false in `ferrosa-index`, giving the
    /// local backend a normal no-entry build path.
    pub fn source_filter_predicate_for_sstable(
        &self,
        sstable_id: &str,
        predicate: &FilterPredicate,
    ) -> ferrosa_common::Result<FilterPredicate> {
        let mapping = match self.column_mapping_for_sstable(sstable_id) {
            SstableMappingOutcome::Mapped(mapping) => mapping,
            // Vanished: an unsatisfiable conjunction gives the normal
            // no-entry build path for a generation that no longer exists.
            SstableMappingOutcome::SstableGone => {
                return Ok(FilterPredicate::conjunction(Vec::new()));
            }
            SstableMappingOutcome::Unavailable(e) => return Err(e),
        };

        let mut clauses = Vec::with_capacity(predicate.clauses.len());
        for clause in &predicate.clauses {
            let Some(current) = u16::try_from(clause.column_position).ok() else {
                return Ok(FilterPredicate::conjunction(Vec::new()));
            };
            let source = mapping.source_regular_ordinals_for_projection(&[current]);
            let [source_ordinal] = source.as_slice() else {
                return Ok(FilterPredicate::conjunction(Vec::new()));
            };
            let mut remapped = clause.clone();
            remapped.column_position = *source_ordinal as usize;
            clauses.push(remapped);
        }
        Ok(FilterPredicate::conjunction(clauses))
    }

    // Outcome of resolving an SSTable's column-ordinal mapping. `SstableGone`
    // is benign (compaction replaced the generation); `Unavailable` must FAIL
    // the caller — guessing a layout fabricates ordinal correctness.

    /// Resolve one SSTable's column-ordinal mapping, with a three-state
    /// outcome so callers can distinguish the benign vanished-under-compaction
    /// case from an unreadable header (which MUST fail the caller — see
    /// `SstableMappingOutcome`).
    fn column_mapping_for_sstable(&self, sstable_id: &str) -> SstableMappingOutcome {
        let schema = self.schema.load();
        let view = self.view.load();
        let Some(idx) = view
            .sstable_ids
            .iter()
            .position(|(id, _)| id == sstable_id)
            .or_else(|| view.sstables.iter().position(|desc| desc.gen == sstable_id))
        else {
            return SstableMappingOutcome::SstableGone;
        };
        let Some(desc) = view.sstables.get(idx) else {
            return SstableMappingOutcome::SstableGone;
        };
        match self.open_reader(desc) {
            Ok(reader) => SstableMappingOutcome::Mapped(ColumnOrdinalMapping::for_header(
                &schema,
                reader.header(),
            )),
            // FAIL CLOSED: an unreadable header means the physical ordinal
            // layout is UNKNOWN. Guessing (identity fallback) would reintroduce
            // the wrong-ordinal probe this mapping exists to prevent, silently.
            // The caller must surface the error (e.g. fail the CREATE INDEX
            // DDL) so the operation retries against a readable file.
            Err(e) => {
                SstableMappingOutcome::Unavailable(ferrosa_common::Error::InvalidFormat(format!(
                    "ordinal remap: cannot open SSTable reader for {sstable_id}: {e}; \
                     refusing to guess the physical column layout"
                )))
            }
        }
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

    /// The smallest timestamp of any write not yet in an SSTable: the minimum over
    /// the active memtable and, during a flush, the memtable being flushed.
    /// `i64::MAX` when both are empty.
    ///
    /// A compaction reads this BEFORE it lists the table's SSTables: data moves
    /// memtable → SSTable during a flush, so the other order could miss it in both.
    pub fn unflushed_min_timestamp(&self) -> i64 {
        let view = self.view.load();
        let flushing = view
            .flushing
            .iter()
            .map(|sealed| sealed.memtable.min_timestamp())
            .min()
            .unwrap_or(i64::MAX);
        view.active.min_timestamp().min(flushing)
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
    ///
    /// Runs as a rotation (see `TableStore::rotate`), so it never interleaves
    /// with a flush or an index DDL of this table.
    pub fn truncate(&self) -> Result<()> {
        self.rotate(RotationKind::Truncate).map(|_outcome| ())
    }

    /// [`Self::truncate`], on the rotation combiner.
    fn truncate_now(&self) -> Result<FlushOutcome> {
        // Rotations run one at a time, so this catalog cannot change before
        // the view below is published.
        let catalog = self.catalog();
        let new_view = StoreView {
            active: new_memtable(),
            flushing: Arc::new(Vec::new()),
            sstables: Arc::new(vec![]),
            sstable_ids: Arc::new(vec![]),
            indexes: new_indexes(Arc::clone(&catalog), self.schema.load_full()),
            sidecar_indexes: Arc::new(vec![]),
            vector_indexes: new_vector_indexes(&catalog.vector_index_configs),
        };
        new_view.check_invariants("truncate");
        let old_view = self.view.swap(Arc::new(new_view));
        // A writer that loaded the old view must not land in the discarded
        // memtable after this returns: seal it, so such a writer reloads.
        if let Err(e) = old_view.indexes.gate.seal_and_drain(MEMTABLE_SEAL_DEADLINE) {
            tracing::error!(%e, "truncate: writes still inside the discarded memtable");
        }
        Ok(FlushOutcome::NothingToFlush)
    }

    /// Atomically replace input SSTables with a compacted output SSTable.
    ///
    /// Identifies input SSTables by their `(id, path)` pair — not just by ID —
    /// because different directories (flush vs compaction) can produce the same
    /// generation number. Matching on both fields prevents accidental removal
    /// of an SSTable that happens to share a gen with an input in a different dir.
    /// Build the compaction output's sidecars by merging the inputs' (t_7ac6b0e3).
    ///
    /// A posting names a row by `(partition key, clustering)` — a key, not a
    /// file offset — so every input posting stays valid for the merged
    /// output. Per index, the inputs' sidecars are k-way merged in `(key, row)`
    /// order, adjacent duplicates dropped, streamed through the atomic writer
    /// to `{output_gen}-{index}.sidecar` in `output_dir`, and mapped: memory is
    /// one head per input, and no SSTable is rescanned. A posting whose row
    /// the compaction dropped is harmless — the read finds no row, or a row the
    /// query's predicate rejects.
    ///
    /// Returns an error rather than a partial map: a compaction swapped in
    /// without an input's postings would leave that index short.
    pub(crate) fn merge_sidecars_for_compaction(
        &self,
        input_ids: &[(String, std::path::PathBuf)],
        output_dir: &std::path::Path,
        output_gen: &str,
    ) -> Result<HashMap<String, SidecarReader>> {
        let guard = self.view.load();
        let mut inputs_by_index: HashMap<&str, Vec<&SidecarReader>> = HashMap::new();
        for (position, (id, _)) in guard.sstable_ids.iter().enumerate() {
            if !input_ids.iter().any(|(input_id, _)| input_id == id) {
                continue;
            }
            let Some(sidecars) = guard.sidecar_indexes.get(position) else {
                continue;
            };
            for (index_name, reader) in sidecars.iter() {
                inputs_by_index
                    .entry(index_name.as_str())
                    .or_default()
                    .push(reader);
            }
        }
        let mut merged = HashMap::with_capacity(inputs_by_index.len());
        for (index_name, readers) in inputs_by_index {
            let path = output_dir.join(format!("{output_gen}-{index_name}.sidecar"));
            let written = crate::index::sidecar::SidecarWriter::write_sorted(
                &path,
                MergedSidecarEntries::new(&readers).map(|(key, position)| {
                    Ok((IndexKey(key.to_vec()), position.to_owned_position()))
                }),
            )
            .and_then(|_| SidecarReader::open(&path))
            .map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "compaction: could not build the output sidecar for index '{index_name}' \
                     at {}: {e}",
                    path.display()
                ))
            })?;
            merged.insert(index_name.to_string(), written);
        }
        Ok(merged)
    }

    pub fn swap_compacted_sstables(
        &self,
        input_ids: &[(String, std::path::PathBuf)],
        output_id: String,
        output_path: std::path::PathBuf,
        add: Arc<SSTableReader<F::Reader>>,
        output_sidecars: HashMap<String, SidecarReader>,
    ) -> Result<CompactionSwap> {
        // Build the descriptor for the compacted output and seed its reader
        // into the pool once; the view swap below may derive more than once.
        let out_desc = SstableDescriptor::from_reader(output_id.clone(), output_path.clone(), &add);
        self.seed_reader(&out_desc, add);
        let output_sidecars = Arc::new(output_sidecars);
        let input_id_set: std::collections::HashSet<&str> =
            input_ids.iter().map(|(id, _)| id.as_str()).collect();

        // Derived from the live view by compare-and-swap: a flush installing
        // its SSTable meanwhile is kept, never overwritten (this used to be
        // excluded by the table's flush lock).
        let removed = self.update_view("swap_compacted", |current| {
            Self::view_with_compaction_output(
                current,
                &input_id_set,
                &out_desc,
                (&output_id, &output_path),
                &output_sidecars,
            )
        })?;
        let Some(removed) = removed else {
            // An input already left the view (a TRUNCATE or another
            // compaction got there first). The output holds rows the view no
            // longer has; installing it would resurrect them.
            self.reader_pool.remove(&self.pool_key(&out_desc));
            if let Err(e) = self.discard_generation_vector_sidecars(&output_id) {
                tracing::error!(%e, output = %output_id, "compaction swap refused: the discarded output's vector sidecars could not be removed");
            }
            tracing::warn!(
                output = %output_id,
                inputs = ?input_id_set,
                "compaction swap refused: an input is no longer in the view; the output must be discarded"
            );
            return Ok(CompactionSwap::InputsGone);
        };

        // Evict every removed input generation from the pool so a stale reader
        // can never be served or reopened after its files are deleted (FMEA #4).
        for desc in &removed {
            self.reader_pool.remove(&self.pool_key(desc));
            self.forget_generation_vector_ready(&desc.gen);
        }
        Ok(CompactionSwap::Swapped)
    }

    /// `current` with the compaction inputs removed and the output prepended,
    /// plus the input descriptors it removed; `(None, None)` when an input is
    /// no longer in `current`.
    fn view_with_compaction_output(
        current: &StoreView,
        input_id_set: &std::collections::HashSet<&str>,
        out_desc: &SstableDescriptor,
        (output_id, output_path): (&String, &std::path::PathBuf),
        output_sidecars: &Arc<HashMap<String, SidecarReader>>,
    ) -> (Option<StoreView>, Option<Vec<SstableDescriptor>>) {
        let all_present = input_id_set.iter().all(|input| {
            current
                .sstable_ids
                .iter()
                .any(|(id, _)| id.as_str() == *input)
        });
        if !all_present {
            return (None, None);
        }
        // Keep SSTables whose ID is NOT in the compaction input set.
        // Match on ID only — the path in the view may be empty (from flush)
        // while the compaction task resolves it to the table directory. Matching
        // on (id, path) caused inputs to never be removed, leaving stale
        // references to deleted files that silently lost data on reads.
        let mut new_sstables = Vec::with_capacity(current.sstables.len());
        let mut new_ids = Vec::with_capacity(current.sstable_ids.len());
        let mut new_sidecars = Vec::with_capacity(current.sidecar_indexes.len());

        for (i, id_entry) in current.sstable_ids.iter().enumerate() {
            if !input_id_set.contains(id_entry.0.as_str()) {
                if let Some(desc) = current.sstables.get(i) {
                    new_sstables.push(desc.clone());
                }
                new_ids.push(id_entry.clone());
                if i < current.sidecar_indexes.len() {
                    new_sidecars.push(Arc::clone(&current.sidecar_indexes[i]));
                }
            }
        }

        let removed: Vec<SstableDescriptor> = current
            .sstables
            .iter()
            .filter(|desc| input_id_set.contains(desc.gen.as_str()))
            .cloned()
            .collect();

        // Prepend the compacted output.
        new_sstables.insert(0, out_desc.clone());
        new_ids.insert(0, (output_id.clone(), output_path.clone()));
        new_sidecars.insert(0, Arc::clone(output_sidecars));

        let next = StoreView {
            active: Arc::clone(&current.active),
            flushing: current.flushing.clone(),
            sstables: Arc::new(new_sstables),
            sstable_ids: Arc::new(new_ids),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::new(new_sidecars),
            vector_indexes: Arc::clone(&current.vector_indexes),
        };
        (Some(next), Some(removed))
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

    /// Install a freshly-built sidecar index for an SSTable already in the
    /// view, making it visible to reads in THIS process immediately.
    ///
    /// The index build scheduler writes sidecar files to disk and marks the
    /// SSTable indexed, but nothing installs the result into the live view —
    /// so before this existed, a backfill became visible only after a restart
    /// reloaded the directory. A read in between consulted an index that was
    /// complete on disk and empty in memory, and reported the empty answer as
    /// the whole one.
    ///
    /// Returns whether the generation was found. A `false` means the SSTable
    /// was compacted away while the build ran, which is benign — the compacted
    /// output gets its own build — but the caller must not treat it as
    /// "installed".
    pub fn install_sidecar(
        &self,
        generation_id: &str,
        index_name: &str,
        reader: crate::index::sidecar::SidecarReader,
    ) -> bool {
        // By compare-and-swap: this used to load and store the view with no
        // exclusion at all, so a flush or compaction swap landing in between
        // was overwritten — an SSTable dropped from the view, or this
        // sidecar lost.
        let installed = self.update_view("install_sidecar", |guard| {
            let Some(position) = guard
                .sstable_ids
                .iter()
                .position(|(gen, _)| gen == generation_id)
            else {
                return (None, false);
            };
            let mut sidecars: Vec<Arc<HashMap<String, SidecarReader>>> =
                Vec::with_capacity(guard.sidecar_indexes.len());
            for (i, existing) in guard.sidecar_indexes.iter().enumerate() {
                if i == position {
                    let mut map: HashMap<String, SidecarReader> = existing.as_ref().clone();
                    map.insert(index_name.to_string(), reader.clone());
                    sidecars.push(Arc::new(map));
                } else {
                    sidecars.push(Arc::clone(existing));
                }
            }
            let next = StoreView {
                active: Arc::clone(&guard.active),
                flushing: guard.flushing.clone(),
                sstables: Arc::clone(&guard.sstables),
                sstable_ids: Arc::clone(&guard.sstable_ids),
                indexes: Arc::clone(&guard.indexes),
                sidecar_indexes: Arc::new(sidecars),
                vector_indexes: Arc::clone(&guard.vector_indexes),
            };
            (Some(next), true)
        });
        match installed {
            Ok(installed) => installed,
            Err(e) => {
                tracing::error!(
                    %e,
                    generation_id,
                    index_name,
                    "install_sidecar: the view stayed contended; sidecar NOT installed"
                );
                false
            }
        }
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
            .filter_map(|descriptor| descriptor.validated_compaction_metadata(table_dir))
            .collect()
    }

    /// Select the smallest compaction inputs with memory bounded by
    /// `max_sstables`, regardless of the table's total SSTable count.
    ///
    /// Descriptor metadata was captured when each reader was already open, so
    /// this scan performs no reader opens, decompression, or component reads.
    /// The candidate vector never grows beyond `max_sstables`.
    pub fn smallest_sstable_metadata_batch(
        &self,
        table_dir: &std::path::Path,
        max_sstables: usize,
        max_input_bytes: u64,
    ) -> (usize, Vec<crate::compaction::metadata::SSTableMetadata>) {
        let max_sstables = max_sstables.min(MAX_COMPACTION_CANDIDATES);
        if max_sstables == 0 || max_input_bytes == 0 {
            return (self.sstable_count(), Vec::new());
        }

        let guard = self.view.load();
        guard.check_invariants("smallest_sstable_metadata_batch");
        let available = guard.sstables.len().min(guard.sstable_ids.len());
        let mut selected = Vec::with_capacity(max_sstables);

        for descriptor in guard.sstables.iter().take(available) {
            let candidate = descriptor.compaction_metadata(table_dir);
            if candidate.size_bytes > max_input_bytes {
                continue;
            }
            if selected.len() < max_sstables {
                selected.push(candidate);
                continue;
            }

            let largest = selected
                .iter()
                .enumerate()
                .max_by(|(_, left), (_, right)| {
                    left.size_bytes
                        .cmp(&right.size_bytes)
                        .then_with(|| left.id.cmp(&right.id))
                })
                .map(|(index, _)| index)
                .expect("bounded candidate set is non-empty");
            let replace = candidate.size_bytes < selected[largest].size_bytes
                || (candidate.size_bytes == selected[largest].size_bytes
                    && candidate.id < selected[largest].id);
            if replace {
                selected[largest] = candidate;
            }
        }

        selected.sort_by(|left, right| {
            left.size_bytes
                .cmp(&right.size_bytes)
                .then_with(|| left.id.cmp(&right.id))
        });
        let mut selected_bytes = 0_u64;
        let mut keep = 0_usize;
        for candidate in &selected {
            let next = selected_bytes.saturating_add(candidate.size_bytes);
            if next > max_input_bytes {
                break;
            }
            selected_bytes = next;
            keep += 1;
        }
        selected.truncate(keep);
        (available, selected)
    }
}

fn sstable_compaction_component_size(table_dir: &std::path::Path, id: &str) -> Option<u64> {
    let mut total = 0u64;
    for suffix in [
        "Data.db",
        "Partitions.db",
        "Rows.db",
        "Filter.db",
        "Statistics.db",
        "TOC.txt",
    ] {
        let path = table_dir.join(format!("{id}-{suffix}"));
        let meta = std::fs::metadata(&path).ok()?;
        if matches!(suffix, "Data.db" | "Partitions.db" | "Statistics.db") && meta.len() == 0 {
            return None;
        }
        total = total.saturating_add(meta.len());
    }
    let compression_info = table_dir.join(format!("{id}-CompressionInfo.db"));
    if let Ok(meta) = std::fs::metadata(compression_info) {
        total = total.saturating_add(meta.len());
    }
    for suffix in ["Digest.crc32", "CRC.db"] {
        let path = table_dir.join(format!("{id}-{suffix}"));
        if let Ok(meta) = std::fs::metadata(path) {
            total = total.saturating_add(meta.len());
        }
    }
    Some(total)
}

fn sstable_compaction_remote_component_available(table_dir: &std::path::Path, id: &str) -> bool {
    let data_path = table_dir.join(format!("{id}-Data.db"));
    matches!(ferrosa_sstable::io::remote_file_len(data_path), Ok(Some(len)) if len > 0)
}

fn time_series_row_timestamp(
    row: &Row,
    timestamp_unit: crate::timeseries::TimeSeriesTimestampUnit,
) -> Option<i64> {
    let bytes: [u8; 8] = row.clustering.as_slice().try_into().ok()?;
    Some(timestamp_unit.raw_to_micros(i64::from_be_bytes(bytes)))
}

/// Whether the flush's late-writer drain must walk the sealed memtable at all.
///
/// The drain exists to catch writes that landed after the snapshot but before
/// `flushing` is cleared. Behind the sealed gate that is an admission bug, so on
/// a healthy cluster it finds nothing — but it still costs a full memtable walk
/// per flush. `write_epoch` makes the empty case O(1).
///
/// Conservative by construction (I-1): if the backing does not track writes, or
/// the epoch moved for any reason, the walk runs. It is only skipped when the
/// epoch is tracked and provably unchanged, so a real late write can never be
/// missed — the failure mode is a wasted walk, never a dropped row.
fn late_writer_drain_needed(memtable: &dyn super::memtable::Memtable, snapshot_epoch: u64) -> bool {
    snapshot_epoch == super::memtable::UNTRACKED_WRITE_EPOCH
        || memtable.write_epoch() != snapshot_epoch
}

fn late_partition_needs_replay(
    flushed_by_key: &std::collections::BTreeMap<ferrosa_common::key::DecoratedKey, &Partition>,
    expanded: &std::collections::BTreeSet<ferrosa_common::key::DecoratedKey>,
    late_partition: &Partition,
) -> bool {
    match flushed_by_key.get(&late_partition.key) {
        None => true,
        // The flush expanded this partition's whole-value collection cells in
        // place, so its cells differ from the memtable's by design. A late
        // write would still add a row or a newer timestamp: compare those.
        Some(flushed_partition) if expanded.contains(&late_partition.key) => {
            !same_rows_and_timestamps(flushed_partition, late_partition)
        }
        Some(flushed_partition) => *flushed_partition != late_partition,
    }
}

/// Whether two images of one partition hold the same rows with the same
/// deletions and newest timestamps, whatever their collection cells' layout.
/// Expanding a whole-value collection keeps each row's clustering, deletion,
/// liveness and newest cell timestamp (the sentinel it adds is one older).
fn same_rows_and_timestamps(a: &Partition, b: &Partition) -> bool {
    fn newest(row: &Row) -> Option<i64> {
        row.cells.iter().map(|(_, cell)| cell.timestamp).max()
    }
    fn same_row(a: &Row, b: &Row) -> bool {
        a.clustering == b.clustering
            && a.deletion == b.deletion
            && a.primary_key_liveness == b.primary_key_liveness
            && newest(a) == newest(b)
    }
    a.deletion == b.deletion
        && a.rows.len() == b.rows.len()
        && a.rows.iter().zip(&b.rows).all(|(x, y)| same_row(x, y))
        && match (&a.static_row, &b.static_row) {
            (Some(x), Some(y)) => same_row(x, y),
            (None, None) => true,
            _ => false,
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

    /// Encode a composite partition key the way `decode_pk` reads it:
    /// `[2-byte len][value][0x00]` per component.
    fn make_composite_key(parts: &[&str]) -> DecoratedKey {
        let mut bytes = Vec::new();
        for part in parts {
            let b = part.as_bytes();
            bytes.extend_from_slice(&(b.len() as u16).to_be_bytes());
            bytes.extend_from_slice(b);
            bytes.push(0x00);
        }
        DecoratedKey::new(ferrosa_common::key::PartitionKey::new(bytes))
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

    fn collect_index_results(
        store: &TableStore<InMemoryFlushTarget>,
        index_name: &str,
        key: &IndexKey,
    ) -> Result<Vec<Partition>> {
        let mut results = Vec::new();
        store.read_by_index_each(index_name, key, &mut |partition| {
            results.push(partition);
            std::ops::ControlFlow::Continue(())
        })?;
        Ok(results)
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

    /// Build an SSTable reader whose Partitions.db smallest/largest key
    /// bounds are overwritten with `smallest`/`largest` raw bytes,
    /// leaving the trie + footer intact (bounds are rewritten in place,
    /// same total length required so the footer's `key_bounds_offset`
    /// stays valid). Used to reproduce SSTables whose partition-index
    /// key bounds are NOT in byte-comparable form — the case that made
    /// `partition_into_disjoint_runs` fuse overlapping SSTables and
    /// defeat cross-SSTable dedup (COUNT(*) over-count).
    ///
    /// The Partitions.db footer (last 24 bytes) is 3 big-endian i64s:
    /// `key_bounds_offset`, `key_count`, `root_pos`. The bounds section
    /// at `key_bounds_offset` is two short-length-prefixed byte strings.
    fn sstable_reader_with_raw_bounds(
        schema: &TableSchema,
        partitions: &[Partition],
        smallest: &[u8],
        largest: &[u8],
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
        let output = writer.finish().unwrap();

        // Rewrite the bounds section in the Partitions.db blob.
        let mut pidx = output.partitions.clone();
        let footer_off = pidx.len() - 24;
        let key_bounds_offset =
            i64::from_be_bytes(pidx[footer_off..footer_off + 8].try_into().unwrap()) as usize;
        // Reconstruct the tail (bounds + footer) with new bounds; the
        // trie prefix [0, key_bounds_offset) is untouched.
        let mut tail = Vec::new();
        tail.extend_from_slice(&(smallest.len() as u16).to_be_bytes());
        tail.extend_from_slice(smallest);
        tail.extend_from_slice(&(largest.len() as u16).to_be_bytes());
        tail.extend_from_slice(largest);
        // Footer with the SAME key_bounds_offset (bounds still start
        // where they did), preserved key_count + root_pos.
        tail.extend_from_slice(&(key_bounds_offset as i64).to_be_bytes());
        tail.extend_from_slice(&pidx[footer_off + 8..footer_off + 16]); // key_count
        tail.extend_from_slice(&pidx[footer_off + 16..footer_off + 24]); // root_pos
        pidx.truncate(key_bounds_offset);
        pidx.extend_from_slice(&tail);

        ferrosa_sstable::reader::SSTableReader::open(ferrosa_sstable::reader::SSTableComponents {
            data: output.data,
            partitions: pidx,
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
    // ---------------------------------------------------------------------
    // Table-level tombstone (TRUNCATE as a normal replicated write).
    // ---------------------------------------------------------------------

    /// Write a table tombstone into `store` at `marked_for_delete_at` micros.
    fn write_table_tombstone(store: &TableStore<InMemoryFlushTarget>, marked_for_delete_at: i64) {
        store
            .write(
                &crate::table_tombstone::table_tombstone_key(),
                crate::table_tombstone::table_tombstone_row(marked_for_delete_at, 1_700_000_000),
            )
            .expect("write the table tombstone");
    }

    fn visible_rows(store: &TableStore<InMemoryFlushTarget>, key: &DecoratedKey) -> Vec<Row> {
        store
            .read_limited_rows(key, 0)
            .expect("read")
            .map(|partition| partition.rows)
            .unwrap_or_default()
    }

    /// I. IMMEDIATE LOGICAL EFFECT: a row written before the truncate is invisible
    ///    to a read immediately after the tombstone write lands (property 4's twin).
    #[test]
    fn table_tombstone_hides_rows_written_before_it_immediately() {
        let store = test_store();
        store
            .write(&make_key("a"), make_row(b"before", 100))
            .unwrap();
        store
            .write(&make_key("b"), make_row(b"before", 100))
            .unwrap();
        assert_eq!(visible_rows(&store, &make_key("a")).len(), 1);

        write_table_tombstone(&store, 200);
        assert!(
            visible_rows(&store, &make_key("a")).is_empty(),
            "a row older than the table tombstone must be invisible at once"
        );
        assert!(visible_rows(&store, &make_key("b")).is_empty());
    }

    /// IV. NEWER DATA SURVIVES: a row written AFTER the truncate (newer timestamp)
    ///     must not be deleted by it.
    #[test]
    fn a_row_written_after_the_table_tombstone_survives() {
        let store = test_store();
        store
            .write(&make_key("a"), make_row(b"before", 100))
            .unwrap();
        write_table_tombstone(&store, 200);
        store
            .write(&make_key("a"), make_row(b"after", 300))
            .unwrap();

        let rows = visible_rows(&store, &make_key("a"));
        assert_eq!(rows.len(), 1, "the post-truncate row must survive");
        assert_eq!(rows[0].cells[0].1.value.as_deref(), Some(b"after".as_ref()));

        // Boundary: the tombstone suppresses strictly older timestamps only, so a
        // row at exactly the tombstone timestamp survives (>= predicate).
        let store2 = test_store();
        store2
            .write(&make_key("e"), make_row(b"edge", 200))
            .unwrap();
        write_table_tombstone(&store2, 200);
        assert_eq!(
            visible_rows(&store2, &make_key("e")).len(),
            1,
            "a row at exactly the tombstone timestamp is newer-or-equal and survives"
        );
    }

    /// III. NO RESURRECTION (the critical one): once the truncate has landed, a
    ///      stale replica that still holds the pre-truncate row cannot bring it
    ///      back — a copy re-merged at its ORIGINAL, older timestamp stays
    ///      suppressed. The table tombstone may only be dropped once every replica
    ///      has purged the data it covers; until then it must dominate any stale
    ///      copy.
    #[test]
    fn a_stale_older_copy_cannot_resurrect_a_truncated_row() {
        let store = test_store();
        store
            .write(&make_key("a"), make_row(b"before", 100))
            .unwrap();
        write_table_tombstone(&store, 200);

        // A stale replica's copy of the pre-truncate row, replayed at its original
        // older timestamp (what repair / read-repair would write back).
        store
            .write(&make_key("a"), make_row(b"before", 100))
            .unwrap();

        assert!(
            visible_rows(&store, &make_key("a")).is_empty(),
            "a stale older copy must not resurrect a truncated row"
        );
    }

    /// VI. NEGATIVE CONTROL: with no table tombstone the earlier row stays visible,
    ///     so the suppression above is caused by the tombstone and nothing else.
    #[test]
    fn without_a_table_tombstone_an_earlier_row_stays_visible() {
        let store = test_store();
        store
            .write(&make_key("a"), make_row(b"before", 100))
            .unwrap();
        assert_eq!(
            visible_rows(&store, &make_key("a")).len(),
            1,
            "control: no tombstone means no suppression"
        );
    }

    /// The tombstone suppresses on scans and COUNT(*) too, and the reserved marker
    /// partition is never surfaced to a scan.
    #[test]
    fn table_tombstone_hides_rows_from_scans_and_counts() {
        let store = test_store();
        store
            .write(&make_key("a"), make_row(b"before", 100))
            .unwrap();
        store
            .write(&make_key("b"), make_row(b"before", 100))
            .unwrap();
        write_table_tombstone(&store, 200);

        let all = store.read_range_limited_rows(None, None, 100, 0).unwrap();
        assert!(
            all.iter().all(|p| p.rows.is_empty()),
            "no rows may be visible in a scan after a truncate"
        );
        assert!(
            all.iter()
                .all(|p| !crate::table_tombstone::is_table_tombstone_key(&p.key)),
            "the reserved table-tombstone partition is bookkeeping, never surfaced"
        );
        assert_eq!(store.count_range(None, None).unwrap(), 0);
    }

    /// Phase 0 (index-type threading): `add_index` records the declared
    /// `IndexType` so eager/backfill/compaction build jobs carry the real type
    /// instead of a hardcoded `BTree`. Unknown indexes default to `BTree`.
    #[test]
    fn add_index_threads_declared_index_type() {
        let store = test_store();
        assert_eq!(
            store.index_type_for("missing"),
            IndexType::BTree,
            "unknown index defaults to BTree"
        );
        let _rotation: FlushOutcome = store
            .add_index("name_idx".to_string(), 0, IndexType::Phonetic)
            .unwrap();
        let _rotation: FlushOutcome = store
            .add_index("emb_idx".to_string(), 1, IndexType::Vector)
            .unwrap();
        assert_eq!(store.index_type_for("name_idx"), IndexType::Phonetic);
        assert_eq!(
            store.index_type_for("emb_idx"),
            IndexType::Vector,
            "a vector index is no longer mis-stamped as BTree"
        );
    }

    /// CREATE INDEX must cover rows that are still in the active memtable.
    /// Otherwise an index created after writes is registered but remains empty
    /// until those rows are flushed and rebuilt into a sidecar.
    #[test]
    fn add_index_backfills_existing_active_memtable_rows() {
        let store = test_store();
        store
            .write(&make_key("before-index"), make_row(b"match", 1000))
            .unwrap();

        let _rotation: FlushOutcome = store
            .add_index("val_idx".to_string(), 0, IndexType::BTree)
            .unwrap();

        let rows = collect_index_results(&store, "val_idx", &IndexKey(b"match".to_vec())).unwrap();
        assert_eq!(rows.len(), 1, "pre-existing memtable row must be indexed");
    }

    /// Index DDL is lock-free (t_d938e6ae): it rotates the memtable instead of
    /// editing the live one's indexes under a table-map write lock. The
    /// invariant that rotation must keep is "a memtable's postings are exactly
    /// the flushed sidecars of the catalog it is bound to". So race writers,
    /// index DDL and flushes, then check every SSTable: each sidecar holds one
    /// posting per row of its SSTable (none lost, none from another
    /// memtable), and no SSTable flushed after an index was dropped has a
    /// sidecar for it.
    #[test]
    fn index_postings_equal_flushed_sidecars_under_racing_ddl_and_flush() {
        const WRITERS: usize = 2;
        const ROWS_PER_WRITER: usize = 400;
        const DDL_ROUNDS: usize = 25;

        let store = Arc::new(test_store());
        let _rotation: FlushOutcome = store
            .add_index("idx_main".to_string(), 0, IndexType::BTree)
            .unwrap();
        let writers_done = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let writers: Vec<_> = (0..WRITERS)
            .map(|w| {
                let store = Arc::clone(&store);
                let done = Arc::clone(&writers_done);
                std::thread::spawn(move || {
                    for i in 0..ROWS_PER_WRITER {
                        store
                            .write(&make_key(&format!("w{w}-{i:04}")), make_row(b"v", 1000))
                            .unwrap();
                    }
                    done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
            })
            .collect();
        let ddl = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for _ in 0..DDL_ROUNDS {
                    let _added: FlushOutcome = store
                        .add_index("idx_tmp".to_string(), 0, IndexType::BTree)
                        .unwrap();
                    let (removed, _flushed) = store.remove_index("idx_tmp").unwrap();
                    assert!(removed, "idx_tmp was declared and must be removed");
                }
                let _added: FlushOutcome = store
                    .add_index("idx_tmp".to_string(), 0, IndexType::BTree)
                    .unwrap();
                let _added: FlushOutcome = store
                    .add_index("idx_gone".to_string(), 0, IndexType::BTree)
                    .unwrap();
                let (removed, _flushed) = store.remove_index("idx_gone").unwrap();
                assert!(removed, "idx_gone was declared and must be removed");
            })
        };
        let flusher = {
            let store = Arc::clone(&store);
            let done = Arc::clone(&writers_done);
            std::thread::spawn(move || {
                // Bounded by the writers finishing; each round is one flush.
                for _ in 0..100_000 {
                    if done.load(std::sync::atomic::Ordering::SeqCst) == WRITERS {
                        return;
                    }
                    store.flush().unwrap();
                }
                panic!("writers did not finish within 100,000 flushes");
            })
        };
        for writer in writers {
            writer.join().unwrap();
        }
        ddl.join().unwrap();
        flusher.join().unwrap();
        // One more row, so the final flush publishes an SSTable whose memtable
        // was created after every DDL above.
        store
            .write(&make_key("zz-last"), make_row(b"v", 1000))
            .unwrap();
        store.flush().unwrap();

        let total = WRITERS * ROWS_PER_WRITER + 1;
        let main = collect_index_results(&store, "idx_main", &IndexKey(b"v".to_vec())).unwrap();
        assert_eq!(main.len(), total, "a posting of idx_main was lost");

        let view = store.view.load();
        assert_eq!(view.sstables.len(), view.sidecar_indexes.len());
        for (sstable, sidecars) in view.sstables.iter().zip(view.sidecar_indexes.iter()) {
            assert!(
                sidecars.contains_key("idx_main"),
                "generation {} has no idx_main sidecar",
                sstable.gen
            );
            for (name, reader) in sidecars.iter() {
                assert!(
                    ["idx_main", "idx_tmp", "idx_gone"].contains(&name.as_str()),
                    "generation {} has a sidecar for unknown index {name}",
                    sstable.gen
                );
                assert_eq!(
                    reader.entries_in_order().count() as u64,
                    sstable.partition_count,
                    "generation {}'s {name} sidecar must hold one posting per row",
                    sstable.gen
                );
            }
        }
        // The view lists the newest SSTable first.
        let last = view
            .sidecar_indexes
            .first()
            .expect("the final flush published");
        let mut last_names: Vec<&str> = last.keys().map(String::as_str).collect();
        last_names.sort_unstable();
        assert_eq!(
            last_names,
            ["idx_main", "idx_tmp"],
            "the last flush's sidecars must be exactly the declared indexes"
        );
        let mut live: Vec<&str> = view.indexes.keys().map(String::as_str).collect();
        live.sort_unstable();
        assert_eq!(live, ["idx_main", "idx_tmp"]);
    }

    /// `write_barrier` (an `RwLock` every write took shared and every flush
    /// took exclusive) is gone: a flush seals the frozen memtable's gate
    /// instead, and writers that meet it move to the new memtable. The
    /// invariant the barrier protected: no write lands in a memtable after
    /// its flush snapshot. Race writers against back-to-back flushes; the
    /// flush counts any partition that changed after its snapshot, and every
    /// row must be readable through the index afterwards (t_d938e6ae).
    #[test]
    fn no_write_lands_in_a_sealed_memtable() {
        const WRITERS: usize = 4;
        const ROWS_PER_WRITER: usize = 1_500;
        let store = Arc::new(test_store());
        let _rotation: FlushOutcome = store
            .add_index("idx_main".to_string(), 0, IndexType::BTree)
            .unwrap();
        let writers_done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let writers: Vec<_> = (0..WRITERS)
            .map(|w| {
                let store = Arc::clone(&store);
                let done = Arc::clone(&writers_done);
                std::thread::spawn(move || {
                    (0..ROWS_PER_WRITER).for_each(|i| {
                        store
                            .write(&make_key(&format!("w{w}-{i:05}")), make_row(b"v", 1000))
                            .unwrap();
                    });
                    done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
            })
            .collect();
        let flushes = {
            let store = Arc::clone(&store);
            let done = Arc::clone(&writers_done);
            std::thread::spawn(move || {
                let mut flushes = 0_usize;
                while done.load(std::sync::atomic::Ordering::SeqCst) < WRITERS {
                    store.flush().unwrap();
                    flushes += 1;
                    assert!(flushes < 1_000_000, "writers never finished");
                }
                flushes
            })
        };
        writers
            .into_iter()
            .for_each(|writer| writer.join().unwrap());
        let flushes = flushes.join().unwrap();
        store.flush().unwrap();

        assert!(
            flushes > 1,
            "the test must race flushes against the writers"
        );
        assert_eq!(
            store
                .late_writes_after_seal
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a write landed in a memtable after its flush snapshot"
        );
        let rows = collect_index_results(&store, "idx_main", &IndexKey(b"v".to_vec())).unwrap();
        assert_eq!(rows.len(), WRITERS * ROWS_PER_WRITER, "a write was lost");
    }

    /// `flush_guard` (a per-table `Mutex` every flush, DDL and truncate
    /// queued on) is gone: rotations go onto a queue that one caller runs
    /// for everyone (t_d938e6ae). Two flushes requested while a third is
    /// running must become ONE rotation, not two more, and lose no row.
    #[test]
    fn flushes_queued_behind_a_running_flush_coalesce_into_one_rotation() {
        let store = Arc::new(test_store());
        store.write(&make_key("a"), make_row(b"v", 1000)).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let first = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                store.flush_with_swap_callback(move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
            })
        };
        // The first flush is inside its rotation, holding the combiner.
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert_eq!(store.rotations_started(), 1);

        store.write(&make_key("b"), make_row(b"v", 1000)).unwrap();
        store.write(&make_key("c"), make_row(b"v", 1000)).unwrap();
        let queued: Vec<_> = (0..2)
            .map(|_| {
                let store = Arc::clone(&store);
                std::thread::spawn(move || store.flush())
            })
            .collect();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while store.rotation_rx.len() < 2 {
            assert!(Instant::now() < deadline, "the two flushes never queued");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        release_tx.send(()).unwrap();

        assert_eq!(first.join().unwrap().unwrap(), FlushOutcome::Published);
        queued
            .into_iter()
            .for_each(|flush| flush.join().unwrap().unwrap());
        assert_eq!(
            store.rotations_started(),
            2,
            "the two queued flushes must run as one rotation"
        );
        assert_eq!(store.sstable_count(), 2);
        assert_eq!(store.memtable_size(), 0);
        ["a", "b", "c"].iter().for_each(|key| {
            assert!(
                store.read(&make_key(key)).unwrap().is_some(),
                "row {key} was lost"
            );
        });
    }

    /// Every view change is a compare-and-swap derived from the current view
    /// (t_d938e6ae). `install_sidecar` used to load and store the view with
    /// no exclusion, so a flush installing its SSTable in between was
    /// overwritten (the SSTable vanished from reads) or overwrote the
    /// sidecar. Race the two and check neither loses the other's change.
    #[test]
    fn sidecar_installs_and_flushes_lose_no_view_change() {
        const ROUNDS: usize = 200;
        let store = Arc::new(test_store());
        let _rotation: FlushOutcome = store
            .add_index("idx_main".to_string(), 0, IndexType::BTree)
            .unwrap();
        store
            .write(&make_key("first"), make_row(b"v", 1000))
            .unwrap();
        store.flush().unwrap();
        let first_gen = store.view.load().sstable_ids[0].0.clone();
        let reader = store.view.load().sidecar_indexes[0]["idx_main"].clone();

        let flusher = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                (0..ROUNDS).for_each(|i| {
                    store
                        .write(&make_key(&format!("k{i:04}")), make_row(b"v", 1000))
                        .unwrap();
                    store.flush().unwrap();
                });
            })
        };
        let installer = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                (0..ROUNDS).for_each(|i| {
                    assert!(store.install_sidecar(
                        &first_gen,
                        &format!("extra_{i}"),
                        reader.clone()
                    ));
                });
            })
        };
        flusher.join().unwrap();
        installer.join().unwrap();

        let view = store.view.load();
        assert_eq!(
            view.sstables.len(),
            ROUNDS + 1,
            "an SSTable a flush installed was dropped from the view"
        );
        let first = view
            .sidecar_indexes
            .last()
            .expect("the first flush's SSTable is the oldest");
        (0..ROUNDS).for_each(|i| {
            assert!(
                first.contains_key(&format!("extra_{i}")),
                "sidecar extra_{i} was installed and then lost"
            );
        });
    }

    /// A scoped sidecar name maps back to the scope it was written for, and
    /// nothing else is mistaken for one of the index's scoped sidecars.
    #[test]
    fn scoped_vector_sidecar_names_round_trip() {
        for scope in [b"k0".to_vec(), vec![], vec![0x00, 0xff, 0x10]] {
            let name = scoped_vector_sidecar_name("vec_idx", &scope);
            assert_eq!(
                scope_of_vector_sidecar("vec_idx", &name).map(Result::unwrap),
                Some(scope)
            );
        }
        assert!(scope_of_vector_sidecar("vec_idx", "vec_idx").is_none());
        assert!(scope_of_vector_sidecar("vec_idx", "other__scope_6b30").is_none());
        for malformed in [
            "vec_idx__scope_6b3",
            "vec_idx__scope_zz",
            "vec_idx__scope_6B30",
            "vec_idx__scope_+f",
        ] {
            assert!(
                matches!(scope_of_vector_sidecar("vec_idx", malformed), Some(Err(_))),
                "{malformed} must be refused, not skipped"
            );
        }
    }

    /// `vector_index_scopes` was a `Mutex<HashMap>`; recording scopes from
    /// concurrent callers must lose none of them, and forgetting an index
    /// must drop only that index's scopes (t_d938e6ae).
    #[test]
    fn concurrent_vector_scope_records_lose_none() {
        const THREADS: usize = 8;
        const SCOPES: usize = 50;
        let store = Arc::new(test_store());
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let store = Arc::clone(&store);
                std::thread::spawn(move || {
                    let index = format!("vec_{}", t % 2);
                    (0..SCOPES).for_each(|i| {
                        let scope = format!("{t}-{i}").into_bytes();
                        store.record_vector_scopes(&index, std::iter::once(&scope));
                    });
                })
            })
            .collect();
        handles
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
        let scopes = store.vector_index_scopes.load();
        assert_eq!(scopes.len(), 2);
        assert_eq!(scopes["vec_0"].len(), THREADS / 2 * SCOPES);
        assert_eq!(scopes["vec_1"].len(), THREADS / 2 * SCOPES);
        assert!(store.forget_vector_scopes("vec_0").unwrap());
        assert!(!store.forget_vector_scopes("vec_0").unwrap());
        assert_eq!(
            store.vector_index_scopes.load()["vec_1"].len(),
            THREADS / 2 * SCOPES
        );
    }

    #[test]
    fn remove_index_unwires_future_index_reads() {
        let store = test_store();
        let _rotation: FlushOutcome = store
            .add_index("val_idx".to_string(), 0, IndexType::BTree)
            .unwrap();
        store.write(&make_key("k"), make_row(b"v", 1000)).unwrap();

        let before_drop =
            collect_index_results(&store, "val_idx", &IndexKey(b"v".to_vec())).unwrap();
        assert_eq!(before_drop.len(), 1);

        assert!(
            store.remove_index("val_idx").unwrap().0,
            "declared index state should be removed"
        );
        assert!(store.indexed_columns().is_empty());

        // Not consulted, and not answered as "no rows" either: the read is
        // refused, naming the index (t_50c8bc7d).
        let after_drop = collect_index_results(&store, "val_idx", &IndexKey(b"v".to_vec()))
            .expect_err("dropped index must not consult stale memtable index state");
        assert!(
            after_drop.to_string().contains("val_idx"),
            "the refusal must name the index: {after_drop}"
        );
        assert!(
            !store.remove_index("val_idx").unwrap().0,
            "second removal is idempotent and reports no state removed"
        );
    }

    /// A node's index read is one ordered stream over every posting source —
    /// the memtable index and each SSTable sidecar — in row order, each row
    /// once even when two sources hold it, resumable strictly after a row.
    /// That is what lets a tenant-wide read page through a node holding only
    /// a cursor per source, never the result (t_50c8bc7d).
    /// The index rebuild asks this for the SSTables to scan, so it must name
    /// the ones the store ACTUALLY holds — not a guess.
    ///
    /// It used to synthesise a contiguous range: take `last_generation()` from
    /// the flush target, subtract the live SSTable count, and emit every integer
    /// between. That is only right while generations are dense and end at
    /// `last_gen`. Generations are allocated from a SEPARATE counter
    /// (`next_sstable_id` / `next_gen`) and compaction retires arbitrary ones,
    /// so in practice the two diverge and the range is mostly fiction.
    ///
    /// Live cluster, 2026-09-14: a rebuild of `idx_entity_by_tenant` on
    /// agent_memory.entity_store reported `sstables_total=21`, then logged 20 of
    /// them as "no data file — compacted away" and indexed 1. On disk that node
    /// held 8 generations WITH a Data.db and ZERO with a TOC but no Data.db —
    /// so the 20 were not orphaned metadata, they were ids that never existed,
    /// and 7 real SSTables were never enumerated at all. The index was then
    /// marked current while covering an eighth of the table, and
    /// `SELECT COUNT(*) WHERE tenant_id = …` returned 32,632 against a true
    /// 102,840.
    ///
    /// Six call sites in engine.rs take their SSTable list from here, so every
    /// index backfill inherited the same fiction — not only `index rebuild`.
    #[test]
    fn generation_ids_name_the_sstables_the_store_holds() {
        let store = test_store();
        store.write(&make_key("k1"), make_row(b"v", 1000)).unwrap();
        store.flush().unwrap();
        // Generations do not stay dense. `advance_gen_past` is called on
        // recovery and around compaction output so new files cannot collide
        // with existing ones, and it jumps BOTH counters. After it, the live
        // set is {1, 1001} while the arithmetic guess covers {1000, 1001}:
        // one id that never existed, one real SSTable never named.
        store.advance_gen_past(1000);
        store.write(&make_key("k2"), make_row(b"v", 2000)).unwrap();
        store.flush().unwrap();

        assert!(
            store.sstable_count() >= 2,
            "fixture must actually hold SSTables or this test proves nothing; held {}",
            store.sstable_count()
        );

        let ids = store.sstable_generation_ids();
        assert_eq!(
            ids.len(),
            store.sstable_count(),
            "the store holds {} SSTables and the enumeration named {}: {ids:?}",
            store.sstable_count(),
            ids.len()
        );

        // And they must be the REAL generations, not fabricated ones. Anything
        // named here that the store does not hold sends a backfill looking for
        // a file that was never written, which it then classifies as
        // "compacted away" and discounts from coverage.
        let live: Vec<String> = store
            .view
            .load()
            .sstables
            .iter()
            .map(|d| d.gen.clone())
            .collect();
        let mut got = ids.clone();
        let mut want = live.clone();
        got.sort();
        want.sort();
        assert_eq!(
            got, want,
            "enumeration must match the live SSTable set exactly; \
             named {got:?} but the store holds {want:?}"
        );
    }

    #[test]
    fn index_reads_merge_sources_in_row_order_and_resume_after_a_row() {
        let store = TableStore::new_with_indexes(
            test_schema(),
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("val_idx".to_string(), 0_usize)],
        );
        // Sidecar: k3, k1. Memtable afterwards: k4, k2, and k1 again.
        store.write(&make_key("k3"), make_row(b"v", 1000)).unwrap();
        store.write(&make_key("k1"), make_row(b"v", 1000)).unwrap();
        store.flush().unwrap();
        store.write(&make_key("k4"), make_row(b"v", 2000)).unwrap();
        store.write(&make_key("k2"), make_row(b"v", 2000)).unwrap();
        store.write(&make_key("k1"), make_row(b"v", 2000)).unwrap();
        store
            .write(&make_key("x9"), make_row(b"other", 2000))
            .unwrap();

        let key = IndexKey(b"v".to_vec());
        // Each delivered row's own position — the cursor a pager would keep.
        let read_after = |after: Option<&RowPosition>| -> Vec<RowPosition> {
            let mut delivered = Vec::new();
            store
                .read_by_index_each_after("val_idx", &key, after, &mut |partition| {
                    delivered.extend(partition.rows.iter().map(|row| RowPosition {
                        partition_key: partition.key.key.as_bytes().to_vec(),
                        clustering_key: row.clustering.clone(),
                    }));
                    std::ops::ControlFlow::Continue(())
                })
                .unwrap();
            delivered
        };
        let pk = |s: &str| make_key(s).key.as_bytes().to_vec();
        let keys = |rows: &[RowPosition]| -> Vec<Vec<u8>> {
            rows.iter().map(|row| row.partition_key.clone()).collect()
        };

        let all = read_after(None);
        assert_eq!(
            keys(&all),
            vec![pk("k1"), pk("k2"), pk("k3"), pk("k4")],
            "every match in row order, k1 once although both sources hold it"
        );
        assert_eq!(
            keys(&read_after(Some(&all[1]))),
            vec![pk("k3"), pk("k4")],
            "a read resumed after the second row yields exactly the rows after it"
        );
    }

    /// Tripwire: the node-level index walk may hold a cursor per source and
    /// the previous row, nothing that grows with the result.
    #[test]
    fn the_index_walk_holds_no_result_sized_collection() {
        let source = include_str!("store.rs");
        let body = source
            .split("pub fn read_by_index_each_after(")
            .nth(1)
            // `\x7d` is a closing brace, spelled as an escape so brace-counting
            // tools (scripts/check-unbounded-reads.py) do not see the test
            // module end here.
            .and_then(|rest| rest.split("\n    \x7d\n").next())
            .expect("read_by_index_each_after must exist");
        for forbidden in ["HashSet", "HashMap", ".collect::<Vec", "BTreeSet"] {
            assert!(
                !body.contains(forbidden),
                "read_by_index_each_after must not hold a result-sized `{forbidden}`"
            );
        }
    }

    #[test]
    fn remove_index_unwires_flushed_sidecar_reads() {
        let store = TableStore::new_with_indexes(
            test_schema(),
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("val_idx".to_string(), 0_usize)],
        );
        store.write(&make_key("k"), make_row(b"v", 1000)).unwrap();
        store.flush().unwrap();

        assert_eq!(
            collect_index_results(&store, "val_idx", &IndexKey(b"v".to_vec()))
                .unwrap()
                .len(),
            1,
            "sanity check: flushed sidecar serves the declared index"
        );

        assert!(store.remove_index("val_idx").unwrap().0);
        assert!(
            collect_index_results(&store, "val_idx", &IndexKey(b"v".to_vec())).is_err(),
            "dropped index must not consult stale sidecar readers, and must not \
             report its absence as zero rows"
        );
        assert!(
            !store.remove_index("val_idx").unwrap().0,
            "orphan sidecar readers must not make removal non-idempotent"
        );
    }

    #[test]
    fn remove_index_unwires_clustering_and_vector_metadata() {
        let store = test_store();
        let _rotation: FlushOutcome = store
            .add_clustering_index("ck_idx".to_string(), 0, IndexType::BTree)
            .unwrap();
        let _rotation: FlushOutcome = store
            .add_quantized_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                m: 8,
                ef_construction: 16,
                metric: DistanceMetric::L2,
            })
            .unwrap();

        assert_eq!(store.indexed_clustering_columns().len(), 1);
        assert_eq!(
            store.vector_index_method("vec_idx"),
            VectorIndexMethod::QuantizedIvf
        );

        assert!(store.remove_index("ck_idx").unwrap().0);
        assert!(store.indexed_clustering_columns().is_empty());

        assert!(store.remove_index("vec_idx").unwrap().0);
        assert_eq!(
            store.vector_index_method("vec_idx"),
            VectorIndexMethod::Hnsw,
            "dropped vector index falls back to the default method"
        );
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

    struct SnapshotMemtable {
        partitions: Vec<Partition>,
    }

    impl Memtable for SnapshotMemtable {
        fn put(&self, _key: &DecoratedKey, _row: Row, _schema: &TableSchema) -> Result<()> {
            panic!("SnapshotMemtable is read-only and only used as a legacy flush snapshot")
        }

        fn get(&self, key: &DecoratedKey) -> Result<Option<Arc<Partition>>> {
            Ok(self
                .partitions
                .iter()
                .find(|partition| &partition.key == key)
                .cloned()
                .map(Arc::new))
        }

        fn snapshot(&self) -> Vec<Partition> {
            self.partitions.clone()
        }

        fn size_bytes(&self) -> usize {
            0
        }

        fn partition_count(&self) -> usize {
            self.partitions.len()
        }
    }

    fn install_snapshot_memtable<F: FlushTarget>(
        store: &TableStore<F>,
        partitions: Vec<Partition>,
    ) {
        let current = store.view.load();
        let replacement = StoreView {
            active: Arc::new(SnapshotMemtable { partitions }),
            flushing: Arc::new(Vec::new()),
            sstables: Arc::clone(&current.sstables),
            sstable_ids: Arc::clone(&current.sstable_ids),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::clone(&current.sidecar_indexes),
            vector_indexes: Arc::clone(&current.vector_indexes),
        };
        replacement.check_invariants("test:install_snapshot_memtable");
        store.view.store(Arc::new(replacement));
    }

    #[test]
    fn flush_all_quarantined_rows_does_not_publish_zero_byte_sstable() {
        crate::quarantine::_reset_flush_quarantined_rows_total_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let store = file_backed_test_store(dir.path());
        let bad_row = Row {
            // Int32 clustering must be exactly 4 bytes. This simulates a legacy
            // malformed memtable row that predates the write-path validator.
            clustering: vec![0; 8],
            cells: vec![(0, CellValue::live(b"bad".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        install_snapshot_memtable(
            &store,
            vec![Partition {
                key: make_key("pk_bad"),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![bad_row],
            }],
        );

        store.flush().unwrap();

        assert_eq!(
            crate::quarantine::flush_quarantined_rows_total(),
            1,
            "the malformed legacy row must be preserved in row quarantine"
        );
        let data_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with("-Data.db"))
            })
            .collect();
        assert!(
            data_files.is_empty(),
            "an entirely quarantined flush must not publish zero-byte Data.db files: {data_files:?}"
        );
        let view = store.view.load();
        assert_eq!(view.sstables.len(), 0);
        assert_eq!(view.sstable_ids.len(), 0);
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
        // Open the legacy descriptors' readers (from the legacy store's pool /
        // retained in-memory components) and hand them to the new store, which
        // seeds them into its own pool. With a single SSTable far below the
        // cap, the seeded reader is never evicted, so no cross-store reopen is
        // attempted.
        let initial_sstables: Vec<Arc<SSTableReader<Vec<u8>>>> = legacy_view
            .sstables
            .iter()
            .map(|desc| legacy.open_reader(desc).expect("open legacy reader"))
            .collect();
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

    #[test]
    fn index_backfill_maps_current_ordinals_to_legacy_sstable_source_ordinals() {
        let key = make_key("pk-backfill-column-order");
        let store = store_with_legacy_order_sstable(
            two_column_schema("name", "zz"),
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
                        name: "aaa".to_string(),
                        type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                    },
                    ColumnDefinition {
                        name: "name".to_string(),
                        type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                    },
                    ColumnDefinition {
                        name: "zz".to_string(),
                        type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                    },
                ],
                extensions: Default::default(),
            },
            &key,
            make_two_column_row(b"John", b"x", 1000),
        );

        assert_eq!(
            store
                .source_regular_ordinal_for_sstable("1", 1)
                .expect("mapping must resolve for a readable SSTable"),
            Some(0),
            "current name ordinal 1 must backfill from old physical ordinal 0"
        );
        assert_eq!(
            store
                .source_regular_ordinal_for_sstable("1", 2)
                .expect("mapping must resolve for a readable SSTable"),
            Some(1),
            "current zz ordinal 2 must backfill from old physical ordinal 1"
        );
        assert_eq!(
            store
                .source_regular_ordinal_for_sstable("1", 0)
                .expect("mapping must resolve for a readable SSTable"),
            None,
            "newly-added aaa did not exist in the old SSTable"
        );
    }

    #[test]
    fn index_backfill_remaps_filtered_predicate_to_legacy_sstable_source_ordinals() {
        let key = make_key("pk-filter-backfill-column-order");
        let store = store_with_legacy_order_sstable(
            two_column_schema("name", "zz"),
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
                        name: "aaa".to_string(),
                        type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                    },
                    ColumnDefinition {
                        name: "name".to_string(),
                        type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                    },
                    ColumnDefinition {
                        name: "zz".to_string(),
                        type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                    },
                ],
                extensions: Default::default(),
            },
            &key,
            make_two_column_row(b"John", b"x", 1000),
        );

        let predicate = FilterPredicate::conjunction(vec![
            ferrosa_index::FilterClause::new(1, ferrosa_index::FilterOp::Eq, b"John".to_vec()),
            ferrosa_index::FilterClause::new(2, ferrosa_index::FilterOp::NotEq, b"z".to_vec()),
        ]);
        let remapped = store
            .source_filter_predicate_for_sstable("1", &predicate)
            .expect("predicate remap must resolve for a readable SSTable");
        let positions: Vec<usize> = remapped
            .clauses()
            .iter()
            .map(|clause| clause.column_position)
            .collect();
        assert_eq!(
            positions,
            vec![0, 1],
            "current predicate ordinals name=1, zz=2 must remap to old physical ordinals 0, 1"
        );

        let absent_new_column =
            FilterPredicate::single(0, ferrosa_index::FilterOp::Eq, b"aaa".to_vec());
        let remapped_absent = store
            .source_filter_predicate_for_sstable("1", &absent_new_column)
            .expect("predicate remap must resolve for a readable SSTable");
        assert!(
            remapped_absent.clauses().is_empty(),
            "a predicate on a newly-added column cannot match a legacy SSTable"
        );
        assert!(
            !ferrosa_index::evaluate_predicate_row(&remapped_absent, |_| Some(b"anything")),
            "empty remapped predicate must evaluate false"
        );
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

    /// End-to-end engine wiring: `range_iter_fragmented` must reassemble
    /// (flatten per key) to byte-identical output vs the whole-partition
    /// `range_iter`, while bounding each emitted fragment to `<= K` rows —
    /// across memtable + flushed-SSTable sources for one WIDE partition.
    /// This is the storage-level OOM-bound + equivalence oracle.
    #[tokio::test]
    async fn range_iter_fragmented_flattens_to_range_iter_across_sources() {
        let _k = crate::range_merger::set_rows_per_fragment(16);
        let store = test_store();

        // One wide partition "hot": half its rows flushed to an SSTable, half
        // still in the active memtable, plus a couple of narrow partitions so
        // the k-way merge has neighbours. Each write is a distinct clustering
        // row in the same partition.
        for ck in 0..40i32 {
            store
                .write(
                    &make_key("hot"),
                    make_row_with_ck(ck, format!("flushed{ck}").as_bytes(), 1000 + ck as i64),
                )
                .unwrap();
        }
        store.write(&make_key("aaa"), make_row(b"a", 1000)).unwrap();
        store.flush().unwrap();
        // Memtable half (newer ts on even cks — LWW must keep these).
        for ck in (0..40i32).filter(|c| c % 2 == 0) {
            store
                .write(
                    &make_key("hot"),
                    make_row_with_ck(ck, format!("memtable{ck}").as_bytes(), 5000 + ck as i64),
                )
                .unwrap();
        }
        store.write(&make_key("zzz"), make_row(b"z", 1000)).unwrap();

        // Whole-partition reference.
        let mut whole_stream = store.range_iter(None, None);
        let mut whole: Vec<Partition> = Vec::new();
        while let Some(p) = futures::StreamExt::next(&mut whole_stream).await {
            whole.push(Arc::unwrap_or_clone(p.unwrap()));
        }

        // Fragmented.
        let mut frag_stream = store.range_iter_fragmented(None, None);
        let mut frags: Vec<Partition> = Vec::new();
        while let Some(p) = futures::StreamExt::next(&mut frag_stream).await {
            let p = Arc::unwrap_or_clone(p.unwrap());
            assert!(
                p.rows.len() <= 16,
                "fragment {} exceeded K=16",
                p.rows.len()
            );
            frags.push(p);
        }

        // Flatten fragments per key.
        let mut flat: Vec<Partition> = Vec::new();
        for f in frags {
            match flat.last_mut() {
                Some(p) if p.key == f.key => p.rows.extend(f.rows),
                _ => flat.push(f),
            }
        }

        assert_eq!(
            flat, whole,
            "fragmented flatten != whole-partition range_iter"
        );
        // The hot partition must have fragmented (40 distinct cks > K=16).
        let hot = whole.iter().find(|p| p.key == make_key("hot")).unwrap();
        assert!(hot.rows.len() > 16, "fixture must produce a wide partition");
    }

    /// A scan producer that panics must end its stream with an ERROR, never a
    /// clean end-of-stream. The producer's sender drops during unwind, so
    /// without one the consumer sees the partitions delivered so far followed
    /// by a normal close: a truncated scan that reads as complete.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_scan_producer_ends_the_stream_with_an_error() {
        /// Fills the channel, then panics mid-scan.
        struct PanicsMidScan;
        impl super::PausableScan for PanicsMidScan {
            fn run(
                &mut self,
                _slot: &mut ferrosa_sched::ScanSlot,
                tx: &tokio::sync::mpsc::Sender<Result<Arc<Partition>>>,
            ) -> super::ScanRun {
                for i in 0..4 {
                    tx.try_send(Ok(Arc::new(make_partition(
                        &format!("before-panic-{i}"),
                        b"v",
                        1,
                    ))))
                    .expect("the channel has room for four");
                }
                panic!("simulated producer bug mid-scan");
            }
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Arc<Partition>>>(4);
        // Fill the channel to capacity before panicking, so the error has to
        // wait behind buffered partitions rather than find a free slot.
        super::spawn_resumable_range_scan(tx, PanicsMidScan);
        // Let the panic land while the buffer is still full.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        for _ in 0..4 {
            let item = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
                .await
                .expect("buffered item within 10 s")
                .expect("a partition sent before the panic");
            assert!(item.is_ok(), "partitions sent before the panic arrive");
        }
        let last = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("the stream must not hang after a producer panic");
        match last {
            Some(Err(e)) => assert!(
                e.to_string().contains("panicked"),
                "the error must say the scan producer panicked, got: {e}"
            ),
            Some(Ok(p)) => panic!("unexpected partition after the panic: {:?}", p.key),
            None => panic!(
                "the scan ended cleanly after its producer panicked: a truncated \
                 result would be reported as complete"
            ),
        }
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

    /// Regression for the COUNT(*) undercount bug (t_8c4e44e8): a bare
    /// `SELECT count(*)` over a table with N distinct partitions returned a
    /// nondeterministic undercount (observed 12/23/15 for N=50) even though a
    /// full materializing scan saw all N. COUNT(*) goes through
    /// `count_range` (ADR-020 fast path); the full scan goes through
    /// `range_iter`. Both must agree, and both must equal N regardless of
    /// storage layout (pure memtable, sstable-only, or a mix).
    /// A COUNT(*) with a partition-key predicate must count from METADATA,
    /// the same pass the unfiltered count uses, rather than falling back to a
    /// row walk.
    ///
    /// Measured on the live cluster 2026-09-14, entity_store, 103,664 rows:
    ///
    ///   COUNT(*) unfiltered                     103,664    0.32s
    ///   COUNT(*) WHERE tenant_id = ?            102,840   24.07s
    ///   every row shipped to the client          103,656    6.75s
    ///
    /// Any WHERE clause disqualifies the ADR-020 fast path (router.rs
    /// `no_where`), so the filtered count resolves through the secondary index
    /// and does one lookup per posting. With that tenant owning 99.2% of the
    /// table the index is the worst available plan — four times slower than
    /// the sequential scan it declined.
    ///
    /// The fix cannot be a key range. `DecoratedKey` orders by TOKEN first, so
    /// partitions sharing a partition-key component are scattered across the
    /// ring, not contiguous — a prefix range would count the wrong partitions
    /// and look plausible doing it. The predicate has to be applied per
    /// partition inside the existing metadata merge, where the key is already
    /// in hand and no cell payload is ever decoded.
    #[test]
    fn count_range_matching_counts_only_partitions_whose_key_passes() {
        let store = test_store();
        // Ten partitions, five of which "belong" to the tenant under test.
        for i in 0..10 {
            let owner = if i % 2 == 0 { "t-keep" } else { "t-other" };
            store
                .write(
                    &make_composite_key(&[owner, &format!("s{i}")]),
                    make_row(b"v", 1000 + i),
                )
                .unwrap();
        }

        let all = store.count_range(None, None).unwrap();
        assert_eq!(all, 10, "fixture must hold ten partitions, got {all}");

        let kept = store
            .count_range_matching(None, None, &|key: &DecoratedKey| {
                key.key.as_bytes().windows(6).any(|w| w == b"t-keep")
            })
            .unwrap();
        assert_eq!(
            kept, 5,
            "only the five matching partitions may be counted, got {kept}"
        );

        let none = store
            .count_range_matching(None, None, &|_: &DecoratedKey| false)
            .unwrap();
        assert_eq!(none, 0, "a predicate matching nothing counts nothing");

        let every = store
            .count_range_matching(None, None, &|_: &DecoratedKey| true)
            .unwrap();
        assert_eq!(
            every, all,
            "a predicate matching everything must equal the unfiltered count"
        );
    }

    #[test]
    fn count_range_counts_every_partition_memtable_only() {
        let store = test_store();
        const N: i64 = 50;
        for i in 0..N {
            store
                .write(&make_key(&format!("k{i:04}")), make_row(b"v", 1000 + i))
                .unwrap();
        }
        // Repeat to expose any per-call nondeterminism (the live repro
        // cycled 12/23/15 across consecutive calls).
        for call in 0..8 {
            let count = store.count_range(None, None).unwrap();
            assert_eq!(
                count, N as u64,
                "COUNT(*) call {call} must see all {N} partitions, got {count}",
            );
        }
    }

    /// Same invariant after a flush (sstable-only) and across a memtable +
    /// sstable mix — count_range must merge sources without dropping rows.
    #[test]
    fn count_range_counts_every_partition_after_flush_and_mixed() {
        let store = test_store();
        const N: i64 = 50;
        for i in 0..N {
            store
                .write(&make_key(&format!("k{i:04}")), make_row(b"v", 1000 + i))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(
            store.count_range(None, None).unwrap(),
            N as u64,
            "COUNT(*) over a single flushed sstable must equal {N}",
        );
        // Add N more distinct partitions into the fresh active memtable.
        for i in N..(2 * N) {
            store
                .write(&make_key(&format!("k{i:04}")), make_row(b"v", 1000 + i))
                .unwrap();
        }
        assert_eq!(
            store.count_range(None, None).unwrap(),
            (2 * N) as u64,
            "COUNT(*) over memtable + sstable must equal {}",
            2 * N,
        );
    }

    /// Regression for the COUNT(*) OVER-count bug: when the SAME
    /// primary key (partition key + clustering) is written more than
    /// once and flushed into separate SSTables, `count_range` must
    /// count each distinct primary key ONCE — collapsing the
    /// cross-SSTable duplicates the same way `merge::merge_partitions`
    /// does for the row-scan path. The observed live symptom on
    /// `agent_memory.typed_edges` was a per-node inflated count
    /// (50807 / 27928 / 46066) that scaled with the number of
    /// SSTables holding a copy of each row, over a data set of exactly
    /// 21168 distinct rows.
    #[test]
    fn count_range_dedups_duplicate_primary_keys_across_sstables() {
        let store = test_store();
        // A single partition holding R rows with DISTINCT clustering
        // keys (mirrors typed_edges: one partition, many clustered
        // edges). Write + flush the identical set THREE times so the
        // same (pk + clustering) rows land in three separate SSTables.
        const R: i32 = 40;
        const COPIES: usize = 3;
        for copy in 0..COPIES {
            for ck in 0..R {
                // Identical clustering; timestamp varies per copy so
                // the newest write legitimately wins on LWW — the row
                // identity (pk+clustering) is unchanged.
                let ts = 1000 + copy as i64;
                store
                    .write(&make_key("p0"), make_row_with_ck(ck, b"v", ts))
                    .unwrap();
            }
            store.flush().unwrap();
        }

        // The full row-scan path (range_iter) already dedups by
        // clustering, so it is the ground truth for the distinct count.
        let scanned: u64 = {
            let view = store.view.load_full();
            let sst_readers = store
                .open_readers_for_key_range(&view.sstables, None, None)
                .unwrap();
            let mut merger = crate::range_merger::merger_for_sources(
                Box::new(std::iter::empty()),
                Vec::new(),
                &sst_readers[..],
                None,
                None,
            )
            .unwrap();
            let mut total = 0u64;
            while let Some(p) = merger.next_merged_partition().unwrap() {
                total += p.rows.len() as u64;
                if p.static_row.is_some() {
                    total += 1;
                }
            }
            total
        };
        assert_eq!(
            scanned, R as u64,
            "row-scan path must see {R} distinct rows (ground truth)"
        );

        assert_eq!(
            store.count_range(None, None).unwrap(),
            R as u64,
            "COUNT(*) must count {R} DISTINCT primary keys, not {} \
             (once per SSTable copy)",
            R as usize * COPIES,
        );
    }

    /// Portable end-to-end reproduction of the COUNT(*) over-count
    /// ROOT CAUSE: two SSTables that BOTH hold the same partition key
    /// (with distinct clustering rows), but whose Partitions.db key
    /// bounds are NOT byte-comparable encoded (they fail
    /// `byte_comparable::decode`, exactly like the captured
    /// Cassandra-shaped typed_edges SSTables). The old
    /// `partition_into_disjoint_runs` compared those raw bounds as
    /// byte-comparable and, when they mis-ordered as "disjoint", fused
    /// the two overlapping SSTables into ONE concatenated run — so the
    /// merge heap saw a single source per key and never called
    /// `merge::merge_partitions`, double-counting every shared row.
    ///
    /// With the decode-guarded run grouping, non-decodable-bounds
    /// SSTables each become their own heap source, the heap groups the
    /// shared key across both, and the row set collapses to the true
    /// distinct count. Both the row-scan and metadata mergers must
    /// agree.
    #[test]
    fn count_range_dedups_when_sstable_bounds_are_not_byte_comparable() {
        let schema = test_schema();

        // Two SSTables, each holding partition "shared" with the SAME
        // three clustering keys (ck 1..=3). Distinct clusterings, so
        // the true distinct row count is 3 — NOT 6.
        let rows = |ts: i64| {
            vec![Partition {
                key: make_key("shared"),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![
                    make_row_with_ck(1, b"v", ts),
                    make_row_with_ck(2, b"v", ts),
                    make_row_with_ck(3, b"v", ts),
                ],
            }]
        };
        // Non-decodable bounds. byte_comparable encoding begins with a
        // component/terminator marker; 0xFF-prefixed bytes are rejected
        // by `decode` ("expected NEXT_COMPONENT at start"). Choose two
        // raw ranges that a naive byte comparison calls DISJOINT
        // (a.largest < b.smallest) to force the old fusion bug.
        let a = sstable_reader_with_raw_bounds(&schema, &rows(1000), &[0xFF, 0x00], &[0xFF, 0x10]);
        let b = sstable_reader_with_raw_bounds(&schema, &rows(2000), &[0xFF, 0x20], &[0xFF, 0x30]);
        // Sanity: the bounds really are non-decodable.
        assert!(
            ferrosa_sstable::byte_comparable::decode(a.smallest_key_bytes()).is_err(),
            "test setup: bounds must be non-byte-comparable to hit the bug"
        );

        let readers: Vec<Arc<ferrosa_sstable::reader::SSTableReader<Vec<u8>>>> =
            vec![Arc::new(a), Arc::new(b)];

        const EXPECTED: u64 = 3;

        for label in ["row-scan", "metadata"] {
            let mut merger = if label == "row-scan" {
                crate::range_merger::merger_for_sources(
                    Box::new(std::iter::empty()),
                    Vec::new(),
                    &readers[..],
                    None,
                    None,
                )
                .unwrap()
            } else {
                crate::range_merger::merger_for_metadata_sources(
                    Box::new(std::iter::empty()),
                    Vec::new(),
                    &readers[..],
                    None,
                    None,
                )
                .unwrap()
            };
            let mut total = 0u64;
            while let Some(p) = merger.next_merged_partition().unwrap() {
                total += p.rows.len() as u64;
                if p.static_row.is_some() {
                    total += 1;
                }
            }
            assert_eq!(
                total, EXPECTED,
                "{label} merger must dedup the shared key across both \
                 non-decodable-bounds SSTables to {EXPECTED}, got {total}"
            );
        }
    }

    /// Faithful reproduction of the live COUNT(*) over-count on
    /// `agent_memory.typed_edges` using the captured node SSTables
    /// (13 real files, some compressed, some carrying non-byte-comparable
    /// partition-index key bounds). Ground truth from direct SSTable
    /// forensics: exactly 21168 distinct primary keys (partition key +
    /// 3-column clustering). Both the row-scan merger
    /// (`merger_for_sources`) and the metadata merger
    /// (`merger_for_metadata_sources`, the COUNT(*) fast path) must
    /// collapse cross-SSTable duplicates to that same 21168 — the live
    /// bug inflated BOTH to ~50807 by fusing overlapping SSTables into a
    /// single concatenated "run" (their raw bounds failed
    /// `byte_comparable::decode` and mis-compared as disjoint), so the
    /// per-key duplicates never reached `merge::merge_partitions`.
    ///
    /// Gated: the captured SSTables live outside the repo (they are not
    /// checked in), so this is a `live-infra-tests` opt-in. Point
    /// `FERROSA_TEST_TYPED_EDGES_DIR` at a directory of captured
    /// `*-Data.db` (+ sibling components) to run it. Per the crate test
    /// policy it `panic!`s with setup instructions when the feature is
    /// enabled but the fixture is absent — never a silent skip. The
    /// portable guards for the same fix are
    /// `count_range_dedups_duplicate_primary_keys_across_sstables`
    /// (store level) and
    /// `range_merger::tests::group_by_key_isolates_undecodable_bounds_into_singleton_runs`
    /// (the run-grouping invariant).
    #[cfg(feature = "live-infra-tests")]
    #[test]
    fn count_range_metadata_merger_dedups_real_typed_edges_sstables() {
        let dir_var = std::env::var("FERROSA_TEST_TYPED_EDGES_DIR").unwrap_or_default();
        if dir_var.is_empty() {
            panic!(
                "FERROSA_TEST_TYPED_EDGES_DIR unset. This live-infra regression \
                 needs the captured agent_memory.typed_edges SSTables. Set \
                 FERROSA_TEST_TYPED_EDGES_DIR=<dir containing *-Data.db + sibling \
                 components> (e.g. the repair-tie-divergence/node1 capture)."
            );
        }
        let dir = std::path::PathBuf::from(&dir_var);
        if !dir.exists() {
            panic!(
                "FERROSA_TEST_TYPED_EDGES_DIR points at {}, which does not exist",
                dir.display()
            );
        }
        let dir = dir.as_path();

        // Open every real SSTable in the directory as an in-memory reader.
        let mut readers: Vec<Arc<ferrosa_sstable::reader::SSTableReader<Vec<u8>>>> = Vec::new();
        let mut data_files: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.to_str().map(|s| s.ends_with("-Data.db")).unwrap_or(false))
            .collect();
        data_files.sort();
        for path in &data_files {
            let name = path.file_name().unwrap().to_str().unwrap().to_string();
            let gen = name.trim_end_matches("-Data.db");
            let read = |suffix: &str| {
                std::fs::read(dir.join(format!("{gen}-{suffix}"))).unwrap_or_default()
            };
            let compression_info =
                std::fs::read(dir.join(format!("{gen}-CompressionInfo.db"))).ok();
            let reader = ferrosa_sstable::reader::SSTableReader::open(
                ferrosa_sstable::reader::SSTableComponents {
                    data: read("Data.db"),
                    partitions: read("Partitions.db"),
                    rows: read("Rows.db"),
                    filter: read("Filter.db"),
                    compression_info,
                    statistics: read("Statistics.db"),
                },
            )
            .unwrap();
            readers.push(Arc::new(reader));
        }
        assert!(
            !readers.is_empty(),
            "expected captured typed_edges SSTables in {}",
            dir.display()
        );

        const EXPECTED_DISTINCT: u64 = 21168;

        // Ground truth: the row-scan merger dedups by clustering.
        let scanned: u64 = {
            let mut merger = crate::range_merger::merger_for_sources(
                Box::new(std::iter::empty()),
                Vec::new(),
                &readers[..],
                None,
                None,
            )
            .unwrap();
            let mut total = 0u64;
            while let Some(p) = merger.next_merged_partition().unwrap() {
                total += p.rows.len() as u64;
                if p.static_row.is_some() {
                    total += 1;
                }
            }
            total
        };
        assert_eq!(
            scanned, EXPECTED_DISTINCT,
            "row-scan merger must count {EXPECTED_DISTINCT} distinct rows"
        );

        // The metadata (COUNT(*)) merger must agree.
        let counted: u64 = {
            let mut merger = crate::range_merger::merger_for_metadata_sources(
                Box::new(std::iter::empty()),
                Vec::new(),
                &readers[..],
                None,
                None,
            )
            .unwrap();
            let mut total = 0u64;
            while let Some(p) = merger.next_merged_partition().unwrap() {
                total += p.rows.len() as u64;
                if p.static_row.is_some() {
                    total += 1;
                }
            }
            total
        };
        assert_eq!(
            counted, EXPECTED_DISTINCT,
            "COUNT(*) metadata merger must dedup cross-SSTable duplicates \
             to {EXPECTED_DISTINCT}, got {counted}"
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
        let good_desc =
            SstableDescriptor::from_reader("good".to_string(), std::path::PathBuf::new(), &good);
        let corrupt_desc = SstableDescriptor::from_reader(
            "corrupt".to_string(),
            std::path::PathBuf::new(),
            &corrupt,
        );
        store.seed_reader(&good_desc, good);
        store.seed_reader(&corrupt_desc, corrupt);
        store.view.store(Arc::new(StoreView {
            active: new_memtable(),
            flushing: Arc::new(Vec::new()),
            sstables: Arc::new(vec![good_desc, corrupt_desc]),
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

    /// Regression for the residual read-vs-compaction data-loss window
    /// (fix/read-compaction-residual-window): when a point read opens an SSTable
    /// successfully and its bloom says the key IS present, but the partition
    /// fetch returns `Err` (the input's `Data.db` was deleted by a concurrent
    /// compaction *after* the cached reader opened and *before* the seek), the
    /// read must signal `stale_view_failed = true` so `with_retried_view` retries
    /// against the freshly-merged view — NOT swallow the error and return a
    /// spurious `Ok(None)` (silent data loss). Before the fix the mid-read `Err`
    /// arm of `read_with_view` left `sstable_open_failed = false`, so no retry
    /// fired and the committed key vanished.
    #[test]
    fn mid_read_fetch_error_signals_view_retry() {
        let store = test_store();
        let schema = test_schema();
        let key = make_key("k-residual");

        // An SSTable that holds `key` (bloom + index present) but whose Data.db
        // is truncated, so the partition fetch errors during the data seek —
        // modelling a file deleted out from under a still-cached reader.
        let truncated = Arc::new(sstable_reader_from_partitions(
            &schema,
            &[make_partition("k-residual", b"v", 1000)],
            Some(8),
        ));
        assert!(
            truncated.may_contain_key(&key),
            "bloom must say the key is present — that is what makes the silent-loss window dangerous"
        );

        let current = store.view.load_full();
        let desc = SstableDescriptor::from_reader(
            "truncated".to_string(),
            std::path::PathBuf::new(),
            &truncated,
        );
        store.seed_reader(&desc, truncated);
        store.view.store(Arc::new(StoreView {
            active: new_memtable(),
            flushing: Arc::new(Vec::new()),
            sstables: Arc::new(vec![desc.clone()]),
            sstable_ids: Arc::new(vec![("truncated".to_string(), std::path::PathBuf::new())]),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::new(vec![Arc::new(HashMap::new())]),
            vector_indexes: Arc::clone(&current.vector_indexes),
        }));

        let view = store.view.load_full();
        let (result, corrupt) = store.read_with_view(&view, &key, 0, None).unwrap();
        assert!(
            result.is_none(),
            "the truncated source yields no rows on this single view snapshot"
        );
        let corrupt = corrupt.expect(
            "a mid-read fetch error on a bloom-matching SSTable must request a view retry \
             (carry the failing SSTable id), not be swallowed into a silent Ok(None)",
        );
        assert_eq!(
            corrupt.gen, "truncated",
            "the retry signal must identify the failing SSTable by gen"
        );
    }

    /// Anti-entropy slice (feat/corrupt-sstable-anti-entropy): a point read that
    /// EXHAUSTS the view-retry bound on a genuinely-corrupt SSTable whose data
    /// is NOT resolvable from any healthy source (memtable / another SSTable)
    /// must FAIL LOUD — return `Err` that identifies the corrupt SSTable (its
    /// gen) — never a spurious `Ok(None)`. It must also QUARANTINE that SSTable
    /// so a subsequent read of the same key skips it instead of re-failing.
    #[test]
    fn exhausted_corrupt_read_unresolvable_fails_loud_and_quarantines() {
        let store = test_store();
        let schema = test_schema();
        let key = make_key("k-corrupt-only");

        // The only source holding the key is a corrupt (truncated) SSTable:
        // bloom says present, but every partition fetch errors. No memtable
        // copy, no second SSTable — the read cannot be resolved.
        let truncated = Arc::new(sstable_reader_from_partitions(
            &schema,
            &[make_partition("k-corrupt-only", b"v", 1000)],
            Some(8),
        ));
        assert!(
            truncated.may_contain_key(&key),
            "bloom must say the key is present — that is what makes the loss dangerous"
        );

        let current = store.view.load_full();
        let desc = SstableDescriptor::from_reader(
            "corrupt-gen".to_string(),
            std::path::PathBuf::new(),
            &truncated,
        );
        store.seed_reader(&desc, truncated);
        store.view.store(Arc::new(StoreView {
            active: new_memtable(),
            flushing: Arc::new(Vec::new()),
            sstables: Arc::new(vec![desc.clone()]),
            sstable_ids: Arc::new(vec![("corrupt-gen".to_string(), std::path::PathBuf::new())]),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::new(vec![Arc::new(HashMap::new())]),
            vector_indexes: Arc::clone(&current.vector_indexes),
        }));

        // First read: exhausts retries with nothing resolved -> fail loud.
        let err = store
            .read(&key)
            .expect_err("an unresolvable read over a corrupt SSTable must fail loud, not Ok(None)");
        assert!(
            err.to_string().contains("corrupt-gen"),
            "the error must identify the corrupt SSTable by gen, got: {err}"
        );

        // The corrupt SSTable must now be quarantined.
        assert!(
            store.is_sstable_quarantined("corrupt-gen"),
            "the corrupt SSTable must be quarantined so later reads can target/skip it"
        );

        // Second read of the same key: the quarantined SSTable is NOT skipped
        // (FMEA ST-56). Its data is gone locally, so the read keeps failing
        // typed, fast and without re-opening the file, rather than reading as
        // the key not existing.
        let again = store
            .read(&key)
            .expect_err("a quarantined generation over the key's token must keep failing");
        assert!(
            again.corrupt_sstable_range().is_some() && again.to_string().contains("corrupt-gen"),
            "expected the typed error naming corrupt-gen, got: {again}"
        );
    }

    /// A key held in the memtable is NOT proof the read is complete: the
    /// corrupt SSTable over its token range may hold older cells or rows of the
    /// same partition. With no known healthy replica the read fails typed
    /// (FMEA ST-56); the coordinator is what fails over to a replica. The
    /// generation is quarantined so repair can refill its range.
    #[test]
    fn corrupt_sstable_with_memtable_copy_still_fails_loud() {
        let store = test_store();
        let schema = test_schema();
        let key = make_key("k-resolvable");

        store.write(&key, make_row(b"live", 2000)).unwrap();

        let truncated = Arc::new(sstable_reader_from_partitions(
            &schema,
            &[make_partition("k-resolvable", b"stale", 1000)],
            Some(8),
        ));
        assert!(truncated.may_contain_key(&key));

        let current = store.view.load_full();
        let desc = SstableDescriptor::from_reader(
            "corrupt-gen-2".to_string(),
            std::path::PathBuf::new(),
            &truncated,
        );
        store.seed_reader(&desc, truncated);
        store.view.store(Arc::new(StoreView {
            active: Arc::clone(&current.active),
            flushing: Arc::new(Vec::new()),
            sstables: Arc::new(vec![desc.clone()]),
            sstable_ids: Arc::new(vec![(
                "corrupt-gen-2".to_string(),
                std::path::PathBuf::new(),
            )]),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::new(vec![Arc::new(HashMap::new())]),
            vector_indexes: Arc::clone(&current.vector_indexes),
        }));

        let err = store
            .read(&key)
            .expect_err("a memtable copy cannot vouch for a corrupt SSTable's rows");
        assert_names_sstable(&err, "corrupt-gen-2");
        assert!(
            store.is_sstable_quarantined("corrupt-gen-2"),
            "the corrupt SSTable is quarantined so anti-entropy repair can refill its range"
        );
        let again = store
            .read(&key)
            .expect_err("quarantine must not turn the failure into a short Ok");
        assert_names_sstable(&again, "corrupt-gen-2");
    }

    // -------------------------------------------------------------------------
    // Range/scan reads must fail loud when an SSTable in their view cannot be
    // opened or read (t_73659682). A partial result returned as success reads
    // as the missing rows not existing.
    // -------------------------------------------------------------------------

    /// Publish `healthy` (seeded in the reader pool) and an SSTable `gen`
    /// that holds `missing_keys` but whose reader cannot be opened: it is in
    /// the view, yet the flush target retains no components for it — the
    /// shape of an evicted local file whose S3 rehydrate failed.
    fn store_with_unopenable_sstable(
        healthy_keys: &[&str],
        missing_keys: &[&str],
    ) -> TableStore<InMemoryFlushTarget> {
        let store = test_store();
        let schema = test_schema();
        let mut descs = Vec::new();
        let healthy_parts: Vec<Partition> = healthy_keys
            .iter()
            .map(|k| make_partition(k, b"healthy", 1000))
            .collect();
        let healthy = Arc::new(sstable_reader_from_partitions(
            &schema,
            &healthy_parts,
            None,
        ));
        let healthy_desc = SstableDescriptor::from_reader(
            "healthy-gen".to_string(),
            std::path::PathBuf::new(),
            &healthy,
        );
        store.seed_reader(&healthy_desc, healthy);
        descs.push(healthy_desc);

        let missing_parts: Vec<Partition> = missing_keys
            .iter()
            .map(|k| make_partition(k, b"missing", 1000))
            .collect();
        let missing = Arc::new(sstable_reader_from_partitions(
            &schema,
            &missing_parts,
            None,
        ));
        // Deliberately NOT seeded: opening it must go to the flush target,
        // which has nothing for this generation.
        descs.push(SstableDescriptor::from_reader(
            "missing-gen".to_string(),
            std::path::PathBuf::new(),
            &missing,
        ));
        install_descriptors(&store, descs);
        store
    }

    fn install_descriptors(store: &TableStore<InMemoryFlushTarget>, descs: Vec<SstableDescriptor>) {
        let current = store.view.load_full();
        let ids: Vec<(String, std::path::PathBuf)> = descs
            .iter()
            .map(|d| (d.gen.clone(), std::path::PathBuf::new()))
            .collect();
        let sidecars = descs.iter().map(|_| Arc::new(HashMap::new())).collect();
        store.view.store(Arc::new(StoreView {
            active: new_memtable(),
            flushing: Arc::new(Vec::new()),
            sstables: Arc::new(descs),
            sstable_ids: Arc::new(ids),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::new(sidecars),
            vector_indexes: Arc::clone(&current.vector_indexes),
        }));
    }

    /// Like [`store_with_unopenable_sstable`] but the second SSTable opens and
    /// then fails mid-read (Data.db truncated), the residual compaction window.
    fn store_with_truncated_sstable() -> TableStore<InMemoryFlushTarget> {
        let store = test_store();
        let schema = test_schema();
        let healthy = Arc::new(sstable_reader_from_partitions(
            &schema,
            &[make_partition("a", b"healthy", 1000)],
            None,
        ));
        let healthy_desc = SstableDescriptor::from_reader(
            "healthy-gen".to_string(),
            std::path::PathBuf::new(),
            &healthy,
        );
        store.seed_reader(&healthy_desc, healthy);
        let truncated = Arc::new(sstable_reader_from_partitions(
            &schema,
            &[make_partition("c", b"missing", 1000)],
            Some(8),
        ));
        let truncated_desc = SstableDescriptor::from_reader(
            "truncated-gen".to_string(),
            std::path::PathBuf::new(),
            &truncated,
        );
        store.seed_reader(&truncated_desc, truncated);
        install_descriptors(&store, vec![healthy_desc, truncated_desc]);
        store
    }

    fn assert_names_sstable(err: &ferrosa_common::Error, gen: &str) {
        assert!(
            err.corrupt_sstable_range().is_some() && err.to_string().contains(gen),
            "expected a typed CorruptSstable error naming {gen}, got: {err}"
        );
    }

    #[test]
    fn read_range_limited_rows_fails_loud_on_unopenable_sstable() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        let err = store
            .read_range_limited_rows(None, None, 100, 0)
            .expect_err("a range read missing an SSTable must not return a partial Ok");
        assert_names_sstable(&err, "missing-gen");
        assert!(store.is_sstable_quarantined("missing-gen"));
    }

    #[test]
    fn read_range_limited_rows_fails_loud_on_mid_read_error() {
        let store = store_with_truncated_sstable();
        let err = store
            .read_range_limited_rows(None, None, 100, 0)
            .expect_err("a mid-read SSTable error must not return a partial Ok");
        assert_names_sstable(&err, "truncated-gen");
    }

    #[test]
    fn read_token_range_fails_loud_on_unopenable_sstable() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        let err = store
            .read_token_range(i64::MIN, i64::MAX, 100)
            .expect_err("a token-range read missing an SSTable must not return a partial Ok");
        assert_names_sstable(&err, "missing-gen");
        assert!(store.is_sstable_quarantined("missing-gen"));
    }

    #[test]
    fn read_token_range_fails_loud_on_mid_read_error() {
        let store = store_with_truncated_sstable();
        let err = store
            .read_token_range(i64::MIN, i64::MAX, 100)
            .expect_err("a decode error must not return a partial Ok");
        assert_names_sstable(&err, "truncated-gen");
    }

    #[test]
    fn read_token_range_bounded_fails_loud_on_unopenable_sstable() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        let err = store
            .read_token_range_bounded(i64::MIN, i64::MAX, 100, usize::MAX)
            .expect_err("a bounded read missing an SSTable must not return a partial Ok");
        assert_names_sstable(&err, "missing-gen");
    }

    #[test]
    fn read_token_range_bounded_fails_loud_on_mid_read_error() {
        let store = store_with_truncated_sstable();
        let err = store
            .read_token_range_bounded(i64::MIN, i64::MAX, 100, usize::MAX)
            .expect_err("a decode error must not return a partial Ok");
        assert_names_sstable(&err, "truncated-gen");
    }

    #[test]
    fn walk_token_range_fails_loud_on_unopenable_sstable() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        let mut seen = 0usize;
        let err = store
            .walk_token_range(i64::MIN, i64::MAX, |_| {
                seen += 1;
                Ok(())
            })
            .expect_err("a token walk missing an SSTable must not complete as Ok");
        assert_names_sstable(&err, "missing-gen");
        assert_eq!(
            seen, 0,
            "no partial results may be delivered before the error"
        );
    }

    #[test]
    fn walk_token_range_fails_loud_on_mid_read_error() {
        let store = store_with_truncated_sstable();
        store
            .walk_token_range(i64::MIN, i64::MAX, |_| Ok(()))
            .expect_err("a decode error mid-walk must surface, not be skipped");
    }

    #[test]
    fn walk_token_range_for_digest_fails_loud_on_unopenable_sstable() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        let err = store
            .walk_token_range_for_digest(i64::MIN, i64::MAX, |_, _, _, _| Ok(()))
            .expect_err("a digest walk missing an SSTable must not hash a partial range");
        assert_names_sstable(&err, "missing-gen");
    }

    // ST-41 residual: a decode error AFTER the SSTable opened (mid-stream) in a
    // streaming merge must be the same typed, retried, quarantined error as an
    // open failure — not the raw SSTable error.

    #[test]
    fn walk_token_range_mid_stream_decode_error_is_typed_and_quarantines() {
        let store = store_with_truncated_sstable();
        let err = store
            .walk_token_range(i64::MIN, i64::MAX, |_| Ok(()))
            .expect_err("a decode error mid-walk must surface, not be skipped");
        assert_names_sstable(&err, "truncated-gen");
        assert!(
            store.is_sstable_quarantined("truncated-gen"),
            "a generation that fails to decode after the retry bound must be quarantined"
        );
        assert_eq!(
            store
                .view_retry_exhausted
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the bounded view retries must have been exhausted exactly once"
        );
    }

    #[test]
    fn walk_token_range_for_digest_mid_stream_decode_error_is_typed_and_quarantines() {
        let store = store_with_truncated_sstable();
        let err = store
            .walk_token_range_for_digest(i64::MIN, i64::MAX, |_, _, _, emit| {
                emit(&mut |_row| Ok(()))
            })
            .expect_err("a decode error mid-digest-walk must surface, not be skipped");
        assert_names_sstable(&err, "truncated-gen");
        assert!(store.is_sstable_quarantined("truncated-gen"));
    }

    /// A store whose view holds one SSTable that opens but fails to decode
    /// (`truncated-gen`), and which a first scan retry replaces with its
    /// healthy compaction output (`merged-gen`) — what a compaction landing
    /// between two attempts does to a retired input.
    ///
    /// `truncate_data_to` picks WHERE the damaged input fails: 8 bytes decodes the
    /// header and fails in the rows (after a digest walk has delivered the key,
    /// so it is final), 3 bytes fails before anything is delivered (retriable).
    fn store_with_input_retired_on_first_retry(
        truncate_data_to: usize,
    ) -> Arc<TableStore<InMemoryFlushTarget>> {
        let store = Arc::new(test_store());
        let schema = test_schema();
        let truncated = Arc::new(sstable_reader_from_partitions(
            &schema,
            &[make_partition("c", b"old", 1000)],
            Some(truncate_data_to),
        ));
        let truncated_desc = SstableDescriptor::from_reader(
            "truncated-gen".to_string(),
            std::path::PathBuf::new(),
            &truncated,
        );
        store.seed_reader(&truncated_desc, truncated);
        install_descriptors(&store, vec![truncated_desc]);

        let merged = Arc::new(sstable_reader_from_partitions(
            &schema,
            &[make_partition("c", b"merged", 1000)],
            None,
        ));
        let merged_desc = SstableDescriptor::from_reader(
            "merged-gen".to_string(),
            std::path::PathBuf::new(),
            &merged,
        );
        store.seed_reader(&merged_desc, merged);

        let hook_store = Arc::clone(&store);
        let mut fired = false;
        read_race_test_hook::ON_SCAN_RETRY.with(|c| {
            *c.borrow_mut() = Some(Box::new(move || {
                assert!(!fired, "the retry hook must fire exactly once");
                fired = true;
                install_descriptors(&hook_store, vec![merged_desc.clone()]);
            }));
        });
        store
    }

    #[test]
    fn walk_token_range_mid_stream_error_from_retired_input_retries_and_succeeds() {
        let store = store_with_input_retired_on_first_retry(8);
        let mut keys = Vec::new();
        store
            .walk_token_range(i64::MIN, i64::MAX, |p| {
                keys.push(p.key.clone());
                Ok(())
            })
            .expect("a failure that clears on a fresh view must not surface");
        assert_eq!(
            keys,
            vec![make_key("c")],
            "the merged partition is served once"
        );
        assert!(
            !store.is_sstable_quarantined("truncated-gen"),
            "a transient retired-input window must not quarantine anything"
        );
        assert_eq!(
            store
                .view_retry_exhausted
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn walk_token_range_for_digest_mid_stream_error_from_retired_input_retries_and_succeeds() {
        let store = store_with_input_retired_on_first_retry(3);
        let mut keys = Vec::new();
        store
            .walk_token_range_for_digest(i64::MIN, i64::MAX, |k, _, _, emit| {
                keys.push(k.clone());
                emit(&mut |_row| Ok(()))
            })
            .expect("a failure that clears on a fresh view must not surface");
        assert_eq!(
            keys,
            vec![make_key("c")],
            "the merged partition is hashed once"
        );
        assert!(!store.is_sstable_quarantined("truncated-gen"));
    }

    #[test]
    fn walk_callback_error_is_not_mistaken_for_an_sstable_failure() {
        let store = test_store();
        let healthy = Arc::new(sstable_reader_from_partitions(
            &test_schema(),
            &[make_partition("a", b"healthy", 1000)],
            None,
        ));
        let desc = SstableDescriptor::from_reader(
            "healthy-gen".to_string(),
            std::path::PathBuf::new(),
            &healthy,
        );
        store.seed_reader(&desc, healthy);
        install_descriptors(&store, vec![desc]);
        let err = store
            .walk_token_range_for_digest(i64::MIN, i64::MAX, |_, _, _, emit| {
                emit(&mut |_row| Err(ferrosa_common::Error::InvalidData("hash sink full".into())))
            })
            .expect_err("a callback failure must fail the walk");
        assert!(
            err.corrupt_sstable_range().is_none() && err.to_string().contains("hash sink full"),
            "a row-callback error must pass through untyped, got: {err}"
        );
        assert!(store.quarantined_sstable_gens().is_empty());
    }

    // ST-41 residual: a generation whose object is known missing fails FAST
    // (still loud) instead of costing 8 reopen attempts on every range read.

    fn missing_cache_counters(store: &TableStore<InMemoryFlushTarget>) -> (u64, u64) {
        (
            store.missing_sstable_open_failures(),
            store.missing_sstable_fast_fails(),
        )
    }

    #[test]
    fn known_missing_generation_fails_fast_on_the_second_read() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        let err = store
            .read_range_limited_rows(None, None, 100, 0)
            .expect_err("first read fails loud");
        assert_names_sstable(&err, "missing-gen");
        assert_eq!(
            missing_cache_counters(&store),
            (1, 8),
            "one real open failure, then the 8 fresh-view retries fail fast without reopening"
        );

        let err = store
            .read_range_limited_rows(None, None, 100, 0)
            .expect_err("a known-missing generation must still fail loud, never a short Ok");
        assert_names_sstable(&err, "missing-gen");
        assert_eq!(
            missing_cache_counters(&store),
            (1, 17),
            "the second read must not re-attempt the open"
        );
    }

    #[test]
    fn known_missing_generation_fails_fast_in_the_bounded_merge_walk() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        store
            .walk_token_range(i64::MIN, i64::MAX, |_| Ok(()))
            .expect_err("first walk fails loud");
        let (failures, _) = missing_cache_counters(&store);
        assert_eq!(failures, 1);
        let err = store
            .walk_token_range(i64::MIN, i64::MAX, |_| Ok(()))
            .expect_err("a known-missing generation must still fail the walk");
        assert_names_sstable(&err, "missing-gen");
        assert_eq!(
            missing_cache_counters(&store).0,
            1,
            "no reopen on the second walk"
        );
    }

    #[test]
    fn restored_generation_is_dropped_from_the_negative_cache() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        store
            .read_range_limited_rows(None, None, 100, 0)
            .expect_err("first read fails loud");
        let missing = store
            .view
            .load()
            .sstables
            .iter()
            .find(|d| d.gen == "missing-gen")
            .cloned()
            .expect("missing-gen is in the view");

        // Repair restores the object: the reader is seeded for the generation.
        let restored = Arc::new(sstable_reader_from_partitions(
            &test_schema(),
            &[make_partition("c", b"missing", 1000)],
            None,
        ));
        store.seed_reader(&missing, restored);

        let before = missing_cache_counters(&store);
        let rows = store
            .read_range_limited_rows(None, None, 100, 0)
            .expect("a restored generation must be readable again at once");
        assert_eq!(rows.len(), 3, "all three partitions are served");
        assert_eq!(
            missing_cache_counters(&store),
            before,
            "the restored generation must not hit the negative cache"
        );
    }

    #[test]
    fn resolving_quarantine_clears_the_negative_cache_entry() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        store
            .read_range_limited_rows(None, None, 100, 0)
            .expect_err("first read fails loud and quarantines");
        assert!(store.is_sstable_quarantined("missing-gen"));

        assert!(store.resolve_sstable_quarantine("missing-gen"));
        assert!(!store.is_sstable_quarantined("missing-gen"));
        assert!(
            !store.resolve_sstable_quarantine("missing-gen"),
            "resolving an unknown generation reports that nothing was cleared"
        );

        store
            .read_range_limited_rows(None, None, 100, 0)
            .expect_err("still missing, so still loud");
        assert_eq!(
            store.missing_sstable_open_failures(),
            2,
            "a resolved entry is re-probed with a real open"
        );
    }

    #[test]
    fn negative_cache_entry_expires_after_its_ttl() {
        let ttl = std::time::Duration::from_secs(5);
        let cache = MissingSstableCache::new(ttl, 8);
        let t0 = Instant::now();
        cache.record_at("g1", t0);
        assert!(cache.is_known_missing_at("g1", t0 + std::time::Duration::from_secs(4)));
        assert!(
            !cache.is_known_missing_at("g1", t0 + ttl),
            "an entry at its TTL is expired and must be re-probed"
        );
        assert_eq!(cache.len(), 0, "an expired entry is dropped when observed");
    }

    #[test]
    fn negative_cache_is_bounded_and_prefers_dropping_expired_entries() {
        let ttl = std::time::Duration::from_secs(10);
        let cache = MissingSstableCache::new(ttl, 3);
        let t0 = Instant::now();
        cache.record_at("old", t0);
        cache.record_at("g2", t0 + std::time::Duration::from_secs(8));
        cache.record_at("g3", t0 + std::time::Duration::from_secs(9));
        // Full; "old" has expired by now, so it is the one to go.
        let now = t0 + std::time::Duration::from_secs(11);
        cache.record_at("g4", now);
        assert_eq!(cache.len(), 3);
        assert!(!cache.is_known_missing_at("old", now));
        assert!(cache.is_known_missing_at("g2", now));
        assert!(cache.is_known_missing_at("g4", now));

        // Nothing expired: the entry closest to expiry is evicted, size holds.
        cache.record_at("g5", now);
        assert_eq!(cache.len(), 3, "the cache never grows past its bound");
        assert!(cache.is_known_missing_at("g5", now));
    }

    #[test]
    fn negative_cache_forget_drops_the_entry() {
        let cache = MissingSstableCache::new(std::time::Duration::from_secs(5), 8);
        let t0 = Instant::now();
        cache.record_at("g1", t0);
        cache.forget("g1");
        assert!(!cache.is_known_missing_at("g1", t0));
        cache.forget("never-recorded");
    }

    #[test]
    fn time_series_cursor_fails_loud_on_unopenable_sstable() {
        let store = store_with_unopenable_sstable(&["a"], &["ts-key"]);
        let err = store
            .visit_time_series_window_rows(
                &make_key("ts-key"),
                0,
                i64::MAX,
                crate::timeseries::TimeSeriesTimestampUnit::Millis,
                |_| Ok(()),
            )
            .expect_err("a cursor missing an SSTable must not visit a partial window");
        assert_names_sstable(&err, "missing-gen");
    }

    #[test]
    fn fulltext_sidecarless_scan_fails_loud_on_unopenable_sstable() {
        let store = store_with_unopenable_sstable(&["a"], &["c"]);
        let _rotation: FlushOutcome = store.add_fulltext_index("fts_idx".to_string(), 0).unwrap();
        let err = store
            .fulltext_sstable_scan_missing_sidecar(
                "fts_idx",
                "missing",
                &std::collections::HashSet::new(),
                None,
            )
            .expect_err("a full-text scan missing an SSTable must not return partial hits");
        assert_names_sstable(&err, "missing-gen");
    }

    #[test]
    fn range_reads_do_not_skip_quarantined_sstables() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        store
            .read_range_limited_rows(None, None, 100, 0)
            .expect_err("first read fails and quarantines");
        assert!(store.is_sstable_quarantined("missing-gen"));
        store
            .read_range_limited_rows(None, None, 100, 0)
            .expect_err("a quarantined SSTable still belongs to the view: never skip it");
    }

    #[test]
    fn fulltext_scan_does_not_skip_quarantined_sstables() {
        let store = store_with_unopenable_sstable(&["a"], &["c"]);
        let _rotation: FlushOutcome = store.add_fulltext_index("fts_idx".to_string(), 0).unwrap();
        let covered = std::collections::HashSet::new();
        store
            .fulltext_sstable_scan_missing_sidecar("fts_idx", "x", &covered, None)
            .expect_err("first scan fails and quarantines");
        assert!(store.is_sstable_quarantined("missing-gen"));
        store
            .fulltext_sstable_scan_missing_sidecar("fts_idx", "x", &covered, None)
            .expect_err("a quarantined SSTable's rows are still missing: never skip it");
    }

    // -------------------------------------------------------------------------
    // Quarantine contract (FMEA ST-56): a quarantined generation is never
    // skipped. A read whose token overlaps it keeps failing with the typed
    // error until the generation leaves the view (repair, compaction, restore).
    // -------------------------------------------------------------------------

    /// Drop `gen` from the published view, as a completed repair or a
    /// compaction that retired it would.
    fn remove_generation_from_view(store: &TableStore<InMemoryFlushTarget>, gen: &str) {
        let current = store.view.load_full();
        let keep: Vec<usize> = (0..current.sstables.len())
            .filter(|i| current.sstables[*i].gen != gen)
            .collect();
        store.view.store(Arc::new(StoreView {
            active: Arc::clone(&current.active),
            flushing: current.flushing.clone(),
            sstables: Arc::new(keep.iter().map(|i| current.sstables[*i].clone()).collect()),
            sstable_ids: Arc::new(
                keep.iter()
                    .map(|i| current.sstable_ids[*i].clone())
                    .collect(),
            ),
            indexes: Arc::clone(&current.indexes),
            sidecar_indexes: Arc::new(
                keep.iter()
                    .map(|i| Arc::clone(&current.sidecar_indexes[*i]))
                    .collect(),
            ),
            vector_indexes: Arc::clone(&current.vector_indexes),
        }));
    }

    fn quarantine_via_point_read(store: &TableStore<InMemoryFlushTarget>) {
        store
            .read(&make_key("c"))
            .expect_err("the first read fails and quarantines the generation");
        assert!(store.is_sstable_quarantined("missing-gen"));
    }

    #[test]
    fn point_read_over_quarantined_generation_keeps_failing_typed() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        quarantine_via_point_read(&store);
        for _ in 0..3 {
            let err = store
                .read(&make_key("c"))
                .expect_err("a quarantined generation must not read as the key being absent");
            assert_names_sstable(&err, "missing-gen");
        }
    }

    #[test]
    fn read_limited_rows_over_quarantined_generation_keeps_failing_typed() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        quarantine_via_point_read(&store);
        let err = store
            .read_limited_rows(&make_key("c"), 10)
            .expect_err("a limited read must not skip the quarantined generation");
        assert_names_sstable(&err, "missing-gen");
        let err = store
            .read_limited_rows_from(&make_key("c"), &1i32.to_be_bytes(), 10)
            .expect_err("a resumed limited read must not skip the quarantined generation");
        assert_names_sstable(&err, "missing-gen");
    }

    #[test]
    fn clustering_row_read_over_quarantined_generation_keeps_failing_typed() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        quarantine_via_point_read(&store);
        let err = store
            .read_clustering_row(&make_key("c"), &1i32.to_be_bytes())
            .expect_err("a clustering-row read must not skip the quarantined generation");
        assert_names_sstable(&err, "missing-gen");
    }

    #[test]
    fn read_outside_quarantined_token_range_is_still_served() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        quarantine_via_point_read(&store);
        let served = store
            .read(&make_key("a"))
            .expect("a key outside the quarantined range is unaffected");
        assert!(served.is_some(), "the healthy generation still serves `a`");
    }

    #[test]
    fn index_read_with_quarantined_generation_fails_typed_not_empty() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        let _rotation: FlushOutcome = store
            .add_index("val_idx".to_string(), 0, IndexType::BTree)
            .unwrap();
        quarantine_via_point_read(&store);
        let err = collect_index_results(&store, "val_idx", &IndexKey(b"missing".to_vec()))
            .expect_err("an index read over a table with a quarantined generation must not be Ok");
        assert_names_sstable(&err, "missing-gen");
        let err = store
            .read_by_index_in_partition("val_idx", &IndexKey(b"missing".to_vec()), b"c")
            .expect_err("a partition-scoped index read must refuse too");
        assert_names_sstable(&err, "missing-gen");
        let err = store
            .read_by_index_cell_ranges("val_idx", &[(0, u64::MAX)])
            .expect_err("a cell-range index read must refuse too");
        assert_names_sstable(&err, "missing-gen");
    }

    #[test]
    fn reads_recover_once_the_quarantined_generation_leaves_the_view() {
        let store = store_with_unopenable_sstable(&["a", "b"], &["c"]);
        let _rotation: FlushOutcome = store
            .add_index("val_idx".to_string(), 0, IndexType::BTree)
            .unwrap();
        quarantine_via_point_read(&store);
        remove_generation_from_view(&store, "missing-gen");
        store
            .read(&make_key("c"))
            .expect("repair removed the generation: the read is answerable again");
        collect_index_results(&store, "val_idx", &IndexKey(b"x".to_vec()))
            .expect("index reads are answerable once the generation is gone");
    }

    /// A failure that clears on a fresh view (a compaction retiring the input
    /// mid-read) is retried and succeeds; only exhaustion reaches quarantine.
    #[test]
    fn view_retry_succeeds_when_failure_was_transient() {
        let store = test_store();
        let mut calls = 0;
        let out = store
            .with_retried_view("test_op", |_| {
                calls += 1;
                if calls == 1 {
                    Ok((
                        None::<u32>,
                        Some(CorruptSstableId {
                            gen: "g".to_string(),
                            dir: std::path::PathBuf::new(),
                            min_token: 0,
                            max_token: 1,
                        }),
                    ))
                } else {
                    Ok((Some(7), None))
                }
            })
            .expect("a failure that clears on a fresh view must not surface");
        assert_eq!(out, Some(7));
        assert!(
            !store.is_sstable_quarantined("g"),
            "a transient window must not quarantine"
        );
    }

    #[test]
    fn scan_retry_succeeds_when_failure_was_transient() {
        let store = test_store();
        let mut calls = 0;
        let out = store
            .with_retried_scan("test_op", || {
                calls += 1;
                if calls == 1 {
                    Err(ferrosa_common::Error::corrupt_sstable("g", 0, 1))
                } else {
                    Ok(42)
                }
            })
            .expect("a failure that clears on a fresh view must not surface");
        assert_eq!(out, 42);
        assert_eq!(calls, 2);
    }

    #[test]
    fn scan_retry_does_not_retry_other_errors() {
        let store = test_store();
        let mut calls = 0;
        let err = store
            .with_retried_scan::<()>("test_op", || {
                calls += 1;
                Err(ferrosa_common::Error::InvalidData("boom".into()))
            })
            .expect_err("non-SSTable errors propagate");
        assert_eq!(calls, 1);
        assert!(err.to_string().contains("boom"));
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
    fn concurrent_writes_and_file_flushes_preserve_acknowledged_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(file_backed_test_store(dir.path()));
        let key = make_key("concurrent_store_pk");
        let total = 2_000usize;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let flush_store = Arc::clone(&store);
        let flush_stop = Arc::clone(&stop);
        let flush_handle = std::thread::spawn(move || {
            let mut count = 0u64;
            while !flush_stop.load(std::sync::atomic::Ordering::Relaxed) {
                flush_store.flush().unwrap();
                count += 1;
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
            flush_store.flush().unwrap();
            count
        });

        for i in 0..total {
            store
                .write(
                    &key,
                    make_row_with_ck(i as i32, format!("r{i}").as_bytes(), i as i64 + 1),
                )
                .unwrap();
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let flush_count = flush_handle.join().unwrap();
        store.flush().unwrap();

        let partition = store
            .read(&key)
            .unwrap()
            .expect("partition must remain readable after concurrent flushes");
        assert_eq!(
            partition.rows.len(),
            total,
            "DATA LOSS: {total} rows written with {flush_count} concurrent TableStore flushes, got {}",
            partition.rows.len()
        );
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
            late_partition_needs_replay(&flushed_by_key, &Default::default(), &late_same_key),
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
            !late_partition_needs_replay(&flushed_by_key, &Default::default(), &flushed),
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

    #[test]
    fn successful_flush_clears_flushing_memtable_to_avoid_reingest() {
        let store = test_store();
        let key = make_key("k1");

        store.write(&key, make_row(b"v1", 1000)).unwrap();
        store.flush().unwrap();

        let view = store.view.load();
        assert!(
            view.flushing.is_empty(),
            "completed flush must clear the flushing memtable so future flushes do not re-ingest the already-flushed snapshot"
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
        let new_sst = store.open_reader(&view.sstables[0]).unwrap();
        drop(view);

        // Get the actual stored IDs.
        let view = store.view.load();
        let current_id_paths: Vec<(String, std::path::PathBuf)> =
            view.sstable_ids.iter().cloned().collect();
        drop(view);
        // Remove the 2 oldest (last 2 in the list).
        let input_id_paths: Vec<(String, std::path::PathBuf)> =
            current_id_paths.iter().rev().take(2).cloned().collect();

        let swap = store
            .swap_compacted_sstables(
                &input_id_paths,
                "compacted".to_string(),
                std::path::PathBuf::new(),
                new_sst,
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(swap, CompactionSwap::Swapped);
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

    /// An index read during a flush must find every row written before the
    /// memtable rotated (T1). After the swap the view's memtable indexes are
    /// the NEW memtable's; the frozen memtable's postings must stay readable
    /// until its sidecar is installed. The flush is held between the swap and
    /// the sidecar install by its own swap callback, so no sleep is involved.
    #[test]
    fn an_index_read_during_a_flush_finds_rows_written_before_the_rotation() {
        let store = Arc::new(test_store());
        let _rotation: FlushOutcome = store
            .add_index("idx_main".to_string(), 0, IndexType::BTree)
            .unwrap();
        store
            .write(&make_key("before"), make_row(b"v", 1000))
            .unwrap();

        let (rotated_tx, rotated_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let flush = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                store.flush_with_swap_callback(move || {
                    rotated_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
            })
        };
        rotated_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        // The memtable has rotated; no sidecar for its rows exists yet.
        let during = collect_index_results(&store, "idx_main", &IndexKey(b"v".to_vec()));
        release_tx.send(()).unwrap();
        assert_eq!(flush.join().unwrap().unwrap(), FlushOutcome::Published);

        let during = during.unwrap();
        assert_eq!(
            during.len(),
            1,
            "a row written before the rotation was missing from an index read during the flush"
        );
        let after = collect_index_results(&store, "idx_main", &IndexKey(b"v".to_vec())).unwrap();
        assert_eq!(after.len(), 1);
    }

    /// A compaction swap must not install its output once its inputs have left
    /// the view (T2). Compaction merges inputs A and B; TRUNCATE commits while
    /// it runs; then the swap arrives. Installing the merged output would bring
    /// the truncated rows back. The swap must refuse, and change nothing.
    #[test]
    fn a_compaction_swap_after_truncate_does_not_resurrect_rows() {
        let store = test_store();
        store.write(&make_key("a"), make_row(b"va", 1000)).unwrap();
        store.flush().unwrap();
        store.write(&make_key("b"), make_row(b"vb", 2000)).unwrap();
        store.flush().unwrap();
        let inputs: Vec<(String, std::path::PathBuf)> =
            store.view.load().sstable_ids.iter().cloned().collect();
        // Stand in for the merged output: an SSTable holding both rows.
        let merged = sstable_reader_from_partitions(
            &test_schema(),
            &[
                make_partition("a", b"va", 1000),
                make_partition("b", b"vb", 2000),
            ],
            None,
        );

        store.truncate().unwrap();
        let swapped = store.swap_compacted_sstables(
            &inputs,
            "merged".to_string(),
            std::path::PathBuf::new(),
            Arc::new(merged),
            HashMap::new(),
        );

        assert!(
            matches!(swapped, Ok(CompactionSwap::InputsGone)),
            "the swap must refuse an output whose inputs were truncated: {swapped:?}"
        );
        assert_eq!(store.sstable_count(), 0);
        assert!(
            store.read(&make_key("a")).unwrap().is_none(),
            "truncated row a resurrected"
        );
        assert!(
            store.read(&make_key("b")).unwrap().is_none(),
            "truncated row b resurrected"
        );
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
    fn read_limited_rows_from_memtable_returns_prefix() {
        let store = test_store();
        let key = make_key("wide-memtable");

        for ck in 0..100i32 {
            store
                .write(
                    &key,
                    make_row_with_ck(ck, format!("mem-{ck}").as_bytes(), 1000 + ck as i64),
                )
                .unwrap();
        }

        let partition = store
            .read_limited_rows(&key, 10)
            .unwrap()
            .expect("partition should exist");

        assert_eq!(partition.rows.len(), 10);
        assert_eq!(partition.rows[0].clustering, 0i32.to_be_bytes());
        assert_eq!(partition.rows[9].clustering, 9i32.to_be_bytes());
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
        let output_sst = store.open_reader(&view.sstables[0]).unwrap();
        drop(view);

        // Swap: this MUST remove the 2 inputs even though their paths
        // don't match the view's empty PathBuf.
        let swap = store
            .swap_compacted_sstables(
                &input_ids_with_real_path,
                "output".to_string(),
                fake_path,
                output_sst,
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(swap, CompactionSwap::Swapped);

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

        let mut visited = 0;
        store
            .read_by_index_each(
                "email_idx",
                &IndexKey(b"alice@test.com".to_vec()),
                &mut |partition| {
                    visited += 1;
                    assert_eq!(partition.key.key.as_bytes(), b"user1");
                    std::ops::ControlFlow::Break(())
                },
            )
            .unwrap();
        assert_eq!(visited, 1, "index iteration must stop at the consumer");

        let results =
            collect_index_results(&store, "email_idx", &IndexKey(b"alice@test.com".to_vec()))
                .unwrap();
        assert_eq!(results.len(), 1, "expected exactly one matching partition");
        assert_eq!(results[0].key.key.as_bytes(), b"user1");
    }

    /// Streaming index reads must return every high-cardinality match without
    /// constructing a result vector at the storage boundary.
    #[test]
    fn read_by_index_each_returns_every_row_above_ten_thousand() {
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
                name: "tenant".to_string(),
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
            vec![("tenant_idx".to_string(), 0_usize)],
        );

        let n = 10_001;
        for i in 0..n {
            store
                .write(
                    &make_key(&format!("edge-{i:06}")),
                    Row {
                        clustering: vec![0x00, 0x00, 0x00, 0x01],
                        cells: vec![(0, CellValue::live(b"t-1".to_vec(), 1000))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(1000),
                    },
                )
                .unwrap();
        }

        let mut visited = 0;
        store
            .read_by_index_each("tenant_idx", &IndexKey(b"t-1".to_vec()), &mut |_| {
                visited += 1;
                std::ops::ControlFlow::Continue(())
            })
            .expect("a large global index stream must remain valid");
        assert_eq!(visited, n);
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

        let results =
            collect_index_results(&store, "city_idx", &IndexKey(b"NYC".to_vec())).unwrap();
        assert_eq!(results.len(), 2, "expected both users from index");
        let pks: Vec<&[u8]> = results.iter().map(|p| p.key.key.as_bytes()).collect();
        assert!(pks.contains(&b"user1".as_slice()));
        assert!(pks.contains(&b"user2".as_slice()));
    }

    /// An index the table does not declare cannot answer. Returning no rows
    /// here is what let a planner/engine disagreement read as an empty table
    /// (t_50c8bc7d), so the read is refused and names the index.
    #[test]
    fn read_by_index_unknown_index_is_refused_not_empty() {
        use ferrosa_index::IndexKey;
        let store = test_store();
        store.write(&make_key("k"), make_row(b"v", 1000)).unwrap();
        let err = collect_index_results(&store, "nonexistent_idx", &IndexKey(b"anything".to_vec()))
            .expect_err("an undeclared index must not answer with zero rows");
        assert!(
            err.to_string().contains("nonexistent_idx"),
            "the refusal must name the index: {err}"
        );
    }

    // =========================================================================
    // Geo cell-range index read (read_by_index_cell_ranges)
    // =========================================================================

    /// Build a CQL `frozen<tuple<double,double>>` wire body for a geo point.
    fn geo_tuple_bytes(lat: f64, lon: f64) -> Vec<u8> {
        let mut v = Vec::with_capacity(24);
        for f in [lat, lon] {
            v.extend_from_slice(&8i32.to_be_bytes());
            v.extend_from_slice(&f.to_be_bytes());
        }
        v
    }

    /// Create a geo-indexed store with a single `frozen<tuple<double,double>>`
    /// `location` column at position 0 and a `Geo` index over it.
    fn geo_store() -> TableStore<InMemoryFlushTarget> {
        let schema = TableSchema {
            keyspace: "geo".to_string(),
            table: "places".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "location".to_string(),
                type_name:
                    "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.DoubleType,org.apache.cassandra.db.marshal.DoubleType))"
                        .to_string(),
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
            vec![],
        );
        let _rotation: FlushOutcome = store
            .add_index("loc_geo".to_string(), 0, IndexType::Geo)
            .unwrap();
        store
    }

    fn write_geo_point(store: &TableStore<InMemoryFlushTarget>, pk: &str, lat: f64, lon: f64) {
        let key = make_key(pk);
        store
            .write(
                &key,
                Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(geo_tuple_bytes(lat, lon), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();
    }

    #[test]
    fn read_by_index_cell_ranges_returns_points_in_range() {
        use ferrosa_index::geo::{cover_radius, encode_point, DEFAULT_COVER_LEVEL};

        let store = geo_store();
        // SF cluster + a far NYC point.
        write_geo_point(&store, "ferry", 37.7955, -122.3937);
        write_geo_point(&store, "union", 37.7880, -122.4074);
        write_geo_point(&store, "nyc", 40.7580, -73.9855);

        // Cover a 3km radius around the Ferry Building. The two SF points fall
        // inside; NYC does not.
        let ranges: Vec<(u64, u64)> = cover_radius(37.7955, -122.3937, 3000.0, DEFAULT_COVER_LEVEL)
            .iter()
            .map(|r| (r.start, r.end))
            .collect();
        let partitions = store.read_by_index_cell_ranges("loc_geo", &ranges).unwrap();

        // The cover is an over-approximation, but it must contain both SF cells
        // and must not contain NYC's cell.
        let nyc_id = encode_point(40.7580, -73.9855);
        assert!(
            !ranges.iter().any(|(s, e)| nyc_id >= *s && nyc_id <= *e),
            "NYC cell must be outside the SF cover ranges"
        );
        // Both SF partitions are fetched (refinement happens in the router).
        assert!(
            partitions.len() >= 2,
            "expected at least the two SF points, got {}",
            partitions.len()
        );
    }

    #[test]
    fn read_by_index_cell_ranges_dedups_and_bounds() {
        let store = geo_store();
        // Insert one well-formed point, then >cap raw entries at one cell id to
        // trip the fail-loud bound.
        write_geo_point(&store, "p1", 0.0, 0.0);

        let idx = store.get_memtable_index("loc_geo").unwrap();
        let cell = ferrosa_index::geo::encode_point(0.0, 0.0);
        let key = IndexKey(cell.to_be_bytes().to_vec());
        for i in 0..(INDEX_RESULT_CAP as u32 + 1) {
            idx.insert(
                key.clone(),
                RowPosition {
                    partition_key: format!("pk{i}").into_bytes(),
                    clustering_key: vec![],
                },
            );
        }
        let ranges = vec![(cell, cell)];
        let result = store.read_by_index_cell_ranges("loc_geo", &ranges);
        assert!(result.is_err(), "should fail loud when cap exceeded");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("10000") || msg.contains("ALLOW FILTERING"),
            "error should mention the cap or ALLOW FILTERING, got: {msg}"
        );
    }

    #[test]
    fn read_by_index_cell_ranges_unknown_index_is_empty() {
        let store = geo_store();
        write_geo_point(&store, "p1", 1.0, 2.0);
        let result = store
            .read_by_index_cell_ranges("nonexistent", &[(0, u64::MAX)])
            .unwrap();
        assert!(result.is_empty());
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
        let results =
            collect_index_results(&store, "city_idx", &IndexKey(b"NYC".to_vec())).unwrap();
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
        let results =
            collect_index_results(&store, "city_idx", &IndexKey(b"NYC".to_vec())).unwrap();
        assert_eq!(
            results.len(),
            2,
            "should find user1 (sidecar) + user2 (memtable)"
        );
        let pks: Vec<&[u8]> = results.iter().map(|p| p.key.key.as_bytes()).collect();
        assert!(pks.contains(&b"user1".as_slice()));
        assert!(pks.contains(&b"user2".as_slice()));
    }

    /// Phase 2 (per-type read dispatch): a phonetic index must point-lookup the
    /// memtable index by the *phonetic code* of the query term, not the raw
    /// bytes. Writing "John" and querying the phonetically-equivalent "Jon"
    /// must return the row via the index path — no full scan, no post-filter.
    #[test]
    fn read_by_index_phonetic_memtable_matches_by_code() {
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
                name: "name".to_string(),
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
            vec![("name_idx".to_string(), 0_usize)],
        );
        let _rotation: FlushOutcome = store
            .add_index("name_idx".to_string(), 0, IndexType::Phonetic)
            .unwrap();

        store
            .write(
                &make_key("user1"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"John".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();

        // Query with the phonetically equivalent "Jon" — the index path must
        // encode the term and find the row, even though the raw bytes differ.
        let results =
            collect_index_results(&store, "name_idx", &IndexKey(b"Jon".to_vec())).unwrap();
        assert_eq!(
            results.len(),
            1,
            "phonetic index must match 'Jon' to stored 'John' via code lookup"
        );
        assert_eq!(results[0].key.key.as_bytes(), b"user1");
    }

    /// `update_schema` must remap index ordinals when an added column
    /// re-sorts the regular columns. Regular columns are ordered by name, so
    /// `ALTER TABLE ADD aaa` shifts an index declared on `name` from ordinal
    /// 0 to 1; a stale declaration extracts the wrong cell on every
    /// subsequent write and the index serves false empty results (the
    /// memory-suite `fixed_phonetic_match` regression).
    #[test]
    fn update_schema_remaps_index_ordinal_when_added_column_sorts_first() {
        use ferrosa_index::IndexKey;

        let make_schema = |regulars: &[&str]| TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: regulars
                .iter()
                .map(|name| ColumnDefinition {
                    name: (*name).to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                })
                .collect(),
            extensions: Default::default(),
        };

        let store = TableStore::new_with_indexes(
            make_schema(&["name"]),
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            vec![("name_idx".to_string(), 0_usize)],
        );
        let _rotation: FlushOutcome = store
            .add_index("name_idx".to_string(), 0, IndexType::Phonetic)
            .unwrap();

        // ALTER TABLE ADD aaa_before — sorts before "name", shifting the
        // indexed column's ordinal from 0 to 1.
        store
            .update_schema(make_schema(&["aaa_before", "name"]))
            .unwrap();

        store
            .write(
                &make_key("user1"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    // Post-ALTER layout: cell ordinal 1 is "name".
                    cells: vec![(1, CellValue::live(b"John".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();

        let results =
            collect_index_results(&store, "name_idx", &IndexKey(b"Jon".to_vec())).unwrap();
        assert_eq!(
            results.len(),
            1,
            "index must keep matching after ALTER shifts the column ordinal"
        );
        assert_eq!(results[0].key.key.as_bytes(), b"user1");
    }

    /// A row flushed before `ALTER TABLE ADD` must still expose its cells under
    /// the same column names after the new schema reorders column ordinals.
    #[test]
    fn row_flushed_before_alter_still_reads_back_under_its_own_column() {
        let make_schema = |regulars: &[&str]| TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: regulars
                .iter()
                .map(|name| ColumnDefinition {
                    name: (*name).to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                })
                .collect(),
            extensions: Default::default(),
        };

        let store = TableStore::new(
            make_schema(&["name"]),
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        store
            .write(
                &make_key("user1"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"John".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();
        store.flush().unwrap();

        // Adding a lexically earlier column moves `name` from ordinal 0 to 1.
        store
            .update_schema(make_schema(&["aaa_before", "name"]))
            .unwrap();

        let partition = store
            .read(&make_key("user1"))
            .unwrap()
            .expect("pre-ALTER row must remain readable after ALTER");
        let name_cell = partition.rows[0]
            .cells
            .iter()
            .find(|(ordinal, _)| *ordinal == 1)
            .expect("pre-ALTER value must be remapped to name's post-ALTER ordinal");
        assert_eq!(name_cell.1.value.as_deref(), Some(b"John".as_slice()));
    }

    /// Phase 2: the same phonetic point-lookup must work through the sidecar
    /// after a flush, since the flushed sidecar inherits the memtable's
    /// phonetic-code keys.
    #[test]
    fn read_by_index_phonetic_sidecar_matches_by_code_after_flush() {
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
                name: "name".to_string(),
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
            vec![("name_idx".to_string(), 0_usize)],
        );
        let _rotation: FlushOutcome = store
            .add_index("name_idx".to_string(), 0, IndexType::Phonetic)
            .unwrap();

        store
            .write(
                &make_key("user1"),
                Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"John".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                },
            )
            .unwrap();
        store.flush().unwrap();

        let results =
            collect_index_results(&store, "name_idx", &IndexKey(b"Jon".to_vec())).unwrap();
        assert_eq!(
            results.len(),
            1,
            "phonetic sidecar must match 'Jon' to flushed 'John' via code lookup"
        );
        assert_eq!(results[0].key.key.as_bytes(), b"user1");
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

    #[test]
    fn smallest_sstable_metadata_batch_has_a_hard_candidate_cap() {
        const SSTABLES: usize = 80;
        const HARD_CAP: usize = 64;

        let store = test_store();
        for index in 0..SSTABLES {
            store
                .write(
                    &make_key(&format!("bounded-{index:03}")),
                    make_row(b"x", index as i64 + 1),
                )
                .unwrap();
            store.flush().unwrap();
        }

        let (available, selected) = store.smallest_sstable_metadata_batch(
            std::path::Path::new("/tmp/in-memory-sstables"),
            1_000,
            u64::MAX,
        );
        assert_eq!(available, SSTABLES);
        assert_eq!(
            selected.len(),
            HARD_CAP,
            "a user-supplied threshold must not turn the candidate Vec into backlog-sized memory"
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
    fn sstable_metadata_does_not_scan_data_stream_order() {
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

        assert_eq!(
            metadata.len(),
            1,
            "compaction planning must remain a lightweight metadata pass for SSTable {gen}; \
             Data.db order validation belongs in executor/repair paths"
        );
    }

    /// Corrupt a flushed Data.db into an out-of-order token stream, as both
    /// `startup_smoke_test_rejects_out_of_order_data_stream` and
    /// `startup_smoke_test_rejects_out_of_order_data_stream_even_with_stale_checksums`
    /// need to start from: write two partitions, flush, then rewrite Data.db
    /// with their headers swapped so the on-disk stream violates token order.
    /// Returns the table dir, the flushed generation, and the corrupted bytes
    /// (so callers can choose whether to rebuild Digest.crc32/CRC.db to match).
    fn corrupt_flushed_generation_into_out_of_order_stream(
        tmp: &tempfile::TempDir,
    ) -> (TableStore<crate::flush::FileFlushTarget>, u64, Vec<u8>) {
        let store = file_backed_test_store(tmp.path());
        let schema = test_schema();

        let first = make_partition("decision", b"first", 1000);
        let second = make_partition("org", b"second", 1000);
        assert!(
            first.key > second.key,
            "test keys must be descending by decorated token"
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
        std::fs::write(&data_path, &unsorted_data).unwrap();

        (store, gen, unsorted_data)
    }

    #[test]
    fn startup_smoke_test_rejects_out_of_order_data_stream() {
        let tmp = tempfile::tempdir().unwrap();
        let (_store, gen, unsorted_data) =
            corrupt_flushed_generation_into_out_of_order_stream(&tmp);

        // Checksums (T-011, commit b4d16012) were computed over the ORIGINAL,
        // correctly-ordered Data.db at flush time. Left stale, the CRC.db
        // chunk covering the rewritten bytes no longer matches them, and that
        // checksum layer fires before the token-order check ever runs — the
        // very next test asserts exactly that. This test exists to exercise
        // the token-order check specifically, so it rebuilds Digest.crc32 and
        // CRC.db to match the corrupted bytes: same corruption, checksums
        // that pass, so the failure is unambiguously the order check.
        let digest = ferrosa_sstable::checksum::format_digest(
            ferrosa_sstable::checksum::digest_bytes(&unsorted_data),
        );
        std::fs::write(tmp.path().join(format!("{gen}-Digest.crc32")), digest).unwrap();
        let crc = ferrosa_sstable::checksum::compute_chunk_crc(&unsorted_data, 65536);
        std::fs::write(tmp.path().join(format!("{gen}-CRC.db")), crc).unwrap();

        let err = crate::engine::StorageEngine::smoke_test_generation(tmp.path(), gen)
            .expect_err("startup/self-heal smoke test must detect Data.db token-order corruption");
        assert!(
            err.to_string().contains("partition order violation"),
            "error must name the token-order corruption, got: {err}"
        );
    }

    #[test]
    fn startup_smoke_test_rejects_out_of_order_data_stream_even_with_stale_checksums() {
        // Companion to the test above: the SAME corruption, but WITHOUT
        // rebuilding Digest.crc32/CRC.db, so the flush-time checksums are
        // stale against the rewritten Data.db. This must still be rejected —
        // by the CRC.db layer this time — so both detection layers (checksum
        // and token-order) are covered by a test that actually exercises them,
        // rather than one layer silently masking the other forever.
        let tmp = tempfile::tempdir().unwrap();
        let (_store, gen, _unsorted_data) =
            corrupt_flushed_generation_into_out_of_order_stream(&tmp);

        let err = crate::engine::StorageEngine::smoke_test_generation(tmp.path(), gen)
            .expect_err("startup/self-heal smoke test must detect stale-checksum corruption");
        let msg = err.to_string();
        assert!(
            msg.contains("CRC") || msg.contains("crc") || msg.contains("checksum"),
            "error must name the checksum-layer corruption when checksums are left stale, got: {err}"
        );
    }

    #[test]
    fn production_table_read_paths_must_not_materialize_unbounded_sstables() {
        let production_sources = [
            ("engine.rs", include_str!("engine.rs")),
            ("store.rs", include_str!("store.rs")),
            (
                "ferrosa-sstable/reader.rs",
                include_str!("../../ferrosa-sstable/src/reader.rs"),
            ),
            (
                "compaction/executor.rs",
                include_str!("compaction/executor.rs"),
            ),
            (
                "compaction/validator/driver.rs",
                include_str!("compaction/validator/driver.rs"),
            ),
            (
                "ferrosa-cql/router.rs",
                include_str!("../../ferrosa-cql/src/router.rs"),
            ),
            (
                "ferrosa-cluster/repair/trigger.rs",
                include_str!("../../ferrosa-cluster/src/repair/trigger.rs"),
            ),
            (
                "ferrosa-sstable-dump.rs",
                include_str!("../../ferrosa-sstable/src/bin/ferrosa-sstable-dump.rs"),
            ),
        ];

        let unbounded_read_all = concat!(".read_all", "_partitions(");
        let unbounded_partition_limit = concat!("read_partitions_limited", "(usize::MAX");
        for (name, source) in production_sources {
            let production = source.split("#[cfg(test)]").next().unwrap_or(source);
            assert!(
                !production.contains(unbounded_read_all),
                "{name} production code must stream SSTables; whole-table partition reads materialize an unbounded Vec"
            );
            assert!(
                !production.contains(unbounded_partition_limit),
                "{name} production code must not request an effectively unbounded materialized SSTable read"
            );
        }
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

    struct RecordingReadAt {
        bytes: Vec<u8>,
        reads: std::sync::Mutex<Vec<usize>>,
    }

    impl ferrosa_sstable::io::ReadAt for RecordingReadAt {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            let offset = offset as usize;
            let Some(available) = self.bytes.get(offset..) else {
                return Ok(0);
            };
            let n = available.len().min(buf.len());
            buf[..n].copy_from_slice(&available[..n]);
            self.reads.lock().expect("reads poisoned").push(buf.len());
            Ok(n)
        }

        fn len(&self) -> Result<u64> {
            Ok(self.bytes.len() as u64)
        }
    }

    #[test]
    fn quantized_artifact_reader_does_not_materialize_full_qvec_file() {
        let artifact = build_quantized_vector_artifact(
            &VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: DistanceMetric::L2,
                m: 4,
                ef_construction: 8,
            },
            vec![],
        )
        .expect("build empty quantized artifact");
        let reader = RecordingReadAt {
            bytes: artifact.clone(),
            reads: std::sync::Mutex::new(Vec::new()),
        };

        let _ = search_quantized_vector_artifact_reader(&reader, &[0.0, 0.0], 1, 4)
            .expect("empty artifact search should parse through positional reader");

        let reads = reader.reads.lock().expect("reads poisoned");
        assert!(
            reads.contains(&QVEC_HNSW_MAGIC.len()),
            "reader must validate the .qvec header with a bounded positional read, got {reads:?}"
        );
        assert!(
            reads.iter().all(|read_len| *read_len < artifact.len()),
            "quantized search must not issue a full-.qvec read, got reads {reads:?} for artifact len {}",
            artifact.len()
        );
    }

    #[test]
    fn quantized_ann_dispatch_uses_qvec_artifact_without_legacy_sidecar() {
        let flush_target = InMemoryFlushTarget::new();
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_quantized_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

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
                .unwrap()
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
    fn wiring_sharded_flush_failure_removes_all_staging() {
        use ferrosa_sstable::pump::test_support::{
            install_sink_hook, Fault, FaultySink, PumpOverrides,
        };
        let dir = tempfile::tempdir().unwrap();
        let _hook = install_sink_hook(
            dir.path().to_path_buf(),
            PumpOverrides::default(),
            Arc::new(|opened, sink| {
                if opened.path.ends_with("Data.db") {
                    let (faulty, _) = FaultySink::new(opened.mode);
                    Box::new(faulty.at(0, Fault::Enospc))
                } else {
                    sink
                }
            }),
        );
        let store = TableStore::new(
            test_schema(),
            crate::flush::FileFlushTarget::new_starting_at(dir.path().to_path_buf()).unwrap(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let mut partitions: Vec<_> = (0..4)
            .map(|i| Partition {
                key: make_key(&format!("key{i}")),
                deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                static_row: None,
                rows: vec![make_row(b"value", 1000)],
            })
            .collect();
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        let error = store
            .flush_sharded(
                partitions,
                2,
                4,
                ferrosa_storage::memtable::UNTRACKED_WRITE_EPOCH,
                &new_memtable(),
                store.schema.load_full(),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("No space") || error.to_string().contains("ENOSPC"),
            "{error}"
        );
        assert_eq!(store.sstable_count(), 0);
        // The shared staging root is infrastructure; only its owned per-shard
        // children must disappear. No generation or staged component may remain.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            assert_eq!(path, dir.path().join(".sstable-staging"));
            assert_eq!(std::fs::read_dir(path).unwrap().count(), 0);
        }
    }

    #[test]
    fn wiring_sharded_flush_uses_pump_and_reads_back() {
        use ferrosa_sstable::pump::test_support::{PumpOverrides, PumpTrace};
        let dir = tempfile::tempdir().unwrap();
        let trace = PumpTrace::install(
            dir.path().to_path_buf(),
            PumpOverrides {
                segment_bytes: Some(262_144),
                queue_depth: Some(1),
            },
        );
        let store = TableStore::new(
            test_schema(),
            crate::flush::FileFlushTarget::new_starting_at(dir.path().to_path_buf()).unwrap(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let mut partitions: Vec<_> = (0..20)
            .map(|i| {
                let mut p = Partition {
                    key: make_key(&format!("key{i:05}")),
                    deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                    static_row: None,
                    rows: Vec::new(),
                };
                p.rows
                    .push(make_row(format!("val{i:05}").as_bytes(), 1000 + i));
                p
            })
            .collect();
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        // Call the actual shard path with two shards even on a one-core runner.
        store
            .flush_sharded(
                partitions,
                2,
                20,
                ferrosa_storage::memtable::UNTRACKED_WRITE_EPOCH,
                &new_memtable(),
                store.schema.load_full(),
            )
            .unwrap();
        assert_eq!(store.sstable_count(), 2);
        assert!(
            trace.bypasses().is_empty(),
            "component writes bypassed pump: {:?}",
            trace.bypasses()
        );
        trace.assert_complete_sstables_with_deferred_sync(2);
        let files = trace.files();
        assert_eq!(
            files
                .iter()
                .filter(|f| f.opened.path.ends_with("Data.db"))
                .count(),
            2
        );
        assert!(files.iter().all(|f| !f.opened.path.ends_with("Data.raw")));
        let got = store.read_range(None, None, 40).unwrap();
        assert_eq!(got.len(), 20);
        for p in got {
            let key = std::str::from_utf8(p.key.key.as_bytes()).unwrap();
            assert_eq!(
                p.rows[0].cells[0].1.value.as_deref(),
                Some(format!("val{}", &key[3..]).as_bytes())
            );
        }
    }

    #[test]
    fn sharded_flush_reads_back_every_partition_without_loss() {
        // Slice #3 data-loss guard: a no-secondary-index table flushed with the
        // sharded encode path must read back EVERY partition exactly, and the
        // number of SSTables must equal the shard decision for the live pool
        // width (deterministic on any machine — 1 shard if width==1).
        let flush_target = InMemoryFlushTarget::new();
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            test_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );

        // Enough partitions to cross the sharding threshold (2 * MIN=512).
        let n = 1100usize;
        for i in 0..n {
            let key = format!("key{i:05}");
            let val = format!("val{i:05}");
            store
                .write(&make_key(&key), make_row(val.as_bytes(), 1000 + i as i64))
                .unwrap();
        }

        let expected_shards = crate::flush::desired_flush_shards(
            n,
            /* can_shard (no indexes) */ true,
            crate::flush_executor::width(),
        );
        store.flush().unwrap();

        assert_eq!(
            store.sstable_count(),
            expected_shards,
            "flush must produce exactly the decided number of shard SSTables"
        );

        // Read every partition back and verify none were lost or corrupted.
        let read = store.read_range(None, None, n * 2).unwrap();
        let got: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = read
            .iter()
            .map(|p| {
                let v = p.rows[0].cells[0]
                    .1
                    .value
                    .clone()
                    .expect("row cell must have a value");
                (p.key.key.as_bytes().to_vec(), v)
            })
            .collect();
        assert_eq!(
            got.len(),
            n,
            "every written partition must be readable back"
        );
        for i in 0..n {
            let key = format!("key{i:05}");
            let val = format!("val{i:05}");
            assert_eq!(
                got.get(key.as_bytes()).map(|v| v.as_slice()),
                Some(val.as_bytes()),
                "partition {key} lost or corrupted across the sharded flush"
            );
        }
    }

    #[test]
    fn quantized_ann_search_merges_active_memtable_with_flushed_qvec_even_when_offsets_overlap() {
        let flush_target = InMemoryFlushTarget::new();
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_quantized_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

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
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

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
            .unwrap()
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

    /// ANN during a flush must find rows written before the memtable rotated:
    /// the vector-index form of T1. The flush is held between the swap and
    /// the sidecar install by its swap callback.
    #[test]
    fn ann_during_a_flush_finds_rows_written_before_the_rotation() {
        let store = Arc::new(TableStore::new(
            vector_schema(),
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        ));
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();
        store
            .write(&make_key("k0"), make_vector_row(&[1.0, 0.0, 0.0], 1000))
            .unwrap();

        let (rotated_tx, rotated_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let flush = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                store.flush_with_swap_callback(move || {
                    rotated_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
            })
        };
        rotated_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let during = store.ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 1, 20);
        let raw = store.ann_search("vec_idx", &[1.0, 0.0, 0.0], 1, 20);
        release_tx.send(()).unwrap();
        assert_eq!(flush.join().unwrap().unwrap(), FlushOutcome::Published);

        assert_eq!(
            during.unwrap().len(),
            1,
            "ANN during the flush missed the frozen row"
        );
        assert_eq!(
            raw.unwrap().len(),
            1,
            "raw ANN during the flush missed the frozen row"
        );
        let after = store
            .ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 1, 20)
            .unwrap();
        assert_eq!(after.len(), 1);
    }

    /// A vector index declared AFTER rows were written must answer for those
    /// rows. Its DDL rotates the memtable, so the rows leave with the frozen
    /// memtable; the rotation's flush must give them a vector sidecar that ANN
    /// reads find (regression: `vector_index_registered_and_ann_orders_correctly`).
    #[test]
    fn a_vector_index_added_after_rows_answers_ann_for_them() {
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        store
            .write(&make_key("k0"), make_vector_row(&[1.0, 0.0, 0.0], 1000))
            .unwrap();
        store
            .write(&make_key("k1"), make_vector_row(&[0.0, 1.0, 0.0], 1001))
            .unwrap();
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

        let partitions = store
            .ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 1, 20)
            .unwrap();
        assert_eq!(
            partitions.len(),
            1,
            "ANN found no row written before the index"
        );
        assert_eq!(partitions[0].key.key.as_bytes(), b"k0");
    }

    /// (e) The in-memory scope set drifted from the scoped sidecars on disk,
    /// so ANN never probed some of them. Verification puts them back.
    #[test]
    fn verify_restores_scopes_missing_from_the_scope_set() {
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();
        for (key, vector) in [("k0", [1.0, 0.0, 0.0]), ("k1", [0.0, 1.0, 0.0])] {
            store
                .write(&make_key(key), make_vector_row(&vector, 1000))
                .unwrap();
        }
        store.flush().unwrap();
        store.vector_index_scopes.store(Arc::new(HashMap::new()));
        assert!(
            store
                .ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 2, 20)
                .unwrap()
                .is_empty(),
            "sanity: with the scopes lost ANN finds no flushed row"
        );

        let before = crate::metrics::index_repairs_total("vec_idx", "scope_set");
        let outcome = store.verify_vector_index("vec_idx", 8).unwrap();
        assert_eq!(outcome.scopes_restored, 2, "{outcome:?}");
        assert_eq!(outcome.pending, 0, "{outcome:?}");
        assert!(crate::metrics::index_repairs_total("vec_idx", "scope_set") > before);
        assert_eq!(
            store
                .ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 2, 20)
                .unwrap()
                .len(),
            2
        );
    }

    /// Rows flushed BEFORE a vector index exists have no vector sidecars.
    /// ANN must refuse (retryable backpressure) rather than answer without
    /// them, and the repair must rebuild them from the rows, after which
    /// every row answers. An interrupted build (scoped sidecars, no manifest)
    /// is treated the same way.
    #[test]
    fn ann_refuses_over_an_incomplete_generation_until_it_is_repaired() {
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        for (key, vector) in [("k0", [1.0, 0.0, 0.0]), ("k1", [0.0, 1.0, 0.0])] {
            store
                .write(&make_key(key), make_vector_row(&vector, 1000))
                .unwrap();
        }
        store.flush().unwrap();
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

        let refused = store.ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 2, 20);
        assert!(
            matches!(&refused, Err(e) if e.is_backpressure()),
            "ANN over an unindexed generation must refuse retryably, got {:?}",
            refused.map(|rows| rows.len())
        );

        let outcome = store.run_vector_repair("vec_idx").expect("no other run");
        assert_eq!(
            (outcome.repaired, outcome.vectors, outcome.failed),
            (1, 2, 0)
        );
        assert_eq!(
            store
                .ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 2, 20)
                .unwrap()
                .len(),
            2
        );

        // An interrupted rebuild: the scoped sidecars survive, the manifest
        // does not. The generation is incomplete again until rebuilt.
        let gen: u64 = store.vector_generations_pending("vec_idx").len() as u64;
        assert_eq!(gen, 0);
        let live = store.sstable_generation_ids();
        let gen: u64 = live[0].parse().unwrap();
        store
            .flush_target
            .remove_vector_sidecar(gen, &vector_manifest_name("vec_idx"))
            .unwrap();
        store.unmark_vector_ready(&live[0], "vec_idx");
        assert!(store
            .ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 2, 20)
            .is_err());
        assert_eq!(store.run_vector_repair("vec_idx").unwrap().repaired, 1);
        assert_eq!(
            store
                .ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 2, 20)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn ann_search_partitions_returns_nearest_rows_with_pk_in_score_order() {
        let flush_target = InMemoryFlushTarget::new();
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

        // k0 is the exact match for the query, k1 close, k2 far.
        store
            .write(&make_key("k0"), make_vector_row(&[1.0, 0.0, 0.0], 1000))
            .unwrap();
        store
            .write(&make_key("k1"), make_vector_row(&[0.9, 0.1, 0.0], 1001))
            .unwrap();
        store
            .write(&make_key("k2"), make_vector_row(&[0.0, 1.0, 0.0], 1002))
            .unwrap();

        let partitions = store
            .ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 2, 20)
            .expect("ann_search_partitions must not fail");

        // k < N: only the two nearest partitions come back.
        assert_eq!(
            partitions.len(),
            2,
            "k=2 must yield exactly two partitions, got {partitions:?}"
        );
        // Nearest-first: k0 then k1. The partition key is recovered from scope.
        assert_eq!(partitions[0].key.key.as_bytes(), b"k0");
        assert_eq!(partitions[1].key.key.as_bytes(), b"k1");
        // The recovered partition actually carries the row payload.
        assert!(
            !partitions[0].rows.is_empty(),
            "recovered partition must contain its row(s)"
        );
    }

    #[test]
    fn vector_index_created_after_writes_backfills_live_rows() {
        for method in [VectorIndexMethod::Hnsw, VectorIndexMethod::QuantizedIvf] {
            let flush_target = InMemoryFlushTarget::new();
            let store: TableStore<InMemoryFlushTarget> = TableStore::new(
                vector_schema(),
                flush_target,
                WriteOptions {
                    compression: None,
                    ..WriteOptions::default()
                },
            );

            store
                .write(&make_key("near"), make_vector_row(&[1.0, 0.0, 0.0], 1000))
                .unwrap();
            store
                .write(&make_key("far"), make_vector_row(&[0.0, 1.0, 0.0], 1001))
                .unwrap();

            let config = VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            };
            let _rotation: FlushOutcome =
                store.add_vector_index_with_method(config, method).unwrap();

            let partitions = store
                .ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 1, 20)
                .expect("a newly-created vector index must search pre-existing live rows");
            assert_eq!(
                partitions.len(),
                1,
                "{method:?} index should backfill rows present before CREATE INDEX"
            );
            assert_eq!(partitions[0].key.key.as_bytes(), b"near");
        }
    }

    #[test]
    fn ann_search_partitions_recovers_rows_after_flush() {
        let flush_target = InMemoryFlushTarget::new();
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

        store
            .write(&make_key("k0"), make_vector_row(&[1.0, 0.0, 0.0], 1000))
            .unwrap();
        store
            .write(&make_key("k1"), make_vector_row(&[0.0, 1.0, 0.0], 1001))
            .unwrap();
        // Flush moves the vectors into the persisted (scoped) sidecars; the row
        // payloads live in the SSTable. The index-consult path must still recover
        // the nearest partition by partition key.
        store.flush().unwrap();

        let partitions = store
            .ann_search_partitions("vec_idx", &[1.0, 0.0, 0.0], 1, 20)
            .expect("ann_search_partitions must not fail after flush");
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].key.key.as_bytes(), b"k0");
        assert!(!partitions[0].rows.is_empty());
    }

    #[test]
    fn ann_same_offset_results_from_different_sstable_generations_both_survive_merge() {
        let flush_target = InMemoryFlushTarget::new();
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

        store
            .write(&make_key("k0"), make_vector_row(&[1.0, 0.0, 0.0], 1000))
            .unwrap();
        store.flush().unwrap();
        store
            .write(&make_key("k1"), make_vector_row(&[0.9, 0.1, 0.0], 1001))
            .unwrap();
        store.flush().unwrap();

        let results = store
            .ann_search("vec_idx", &[1.0, 0.0, 0.0], 2, 20)
            .expect("ann_search must not fail");

        assert_eq!(
            results.len(),
            2,
            "two sidecars may both report row offset 0; merge identity must include SSTable generation: {results:?}"
        );
        assert!(
            results[0].score <= results[1].score,
            "same-offset cross-generation results must remain deterministically score ordered: {results:?}"
        );
    }

    #[test]
    fn partition_scoped_ann_search_excludes_other_prefixes() {
        let flush_target = InMemoryFlushTarget::new();
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

        let scope_a = make_key("tenant-a|session-1");
        let scope_b = make_key("tenant-b|session-1");
        store
            .write(&scope_a, make_vector_row(&[0.0, 1.0, 0.0], 1000))
            .unwrap();
        store
            .write(&scope_b, make_vector_row(&[1.0, 0.0, 0.0], 1001))
            .unwrap();

        let query = [1.0, 0.0, 0.0];
        let unscoped = store.ann_search("vec_idx", &query, 1, 20).unwrap();
        assert!(
            unscoped[0].score < 0.1,
            "control query should see the cross-prefix exact match"
        );

        let scoped = store
            .ann_search_in_partition_scope("vec_idx", scope_a.key.as_bytes(), &query, 1, 20)
            .expect("partition-scoped ANN search must not fail");
        assert_eq!(scoped.len(), 1);
        assert!(
            scoped[0].score > 1.0,
            "scoped query must exclude the closer vector in another tenant/session prefix: {:?}",
            scoped
        );

        store.flush().unwrap();
        let flushed_scoped = store
            .ann_search_in_partition_scope("vec_idx", scope_a.key.as_bytes(), &query, 1, 20)
            .expect("flushed partition-scoped ANN search must not fail");
        assert_eq!(flushed_scoped.len(), 1);
        assert!(
            flushed_scoped[0].score > 1.0,
            "flushed scoped query must still exclude vectors from other prefixes: {:?}",
            flushed_scoped
        );
    }

    #[test]
    fn vector_prefix_scope_reads_smaller_scoped_sidecar_than_unscoped_search() {
        let flush_target = InMemoryFlushTarget::new();
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
            vector_schema(),
            flush_target,
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "vec_idx".to_string(),
                column_position: 0,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

        let scope_a = make_key("tenant-a|session-1");
        let scope_b = make_key("tenant-b|session-1");
        store
            .write(&scope_a, make_vector_row(&[0.0, 1.0, 0.0], 1000))
            .unwrap();
        for i in 0..16 {
            let x = 1.0 - (i as f32 * 0.01);
            store
                .write(&scope_b, make_vector_row(&[x, 0.0, 0.0], 2000 + i))
                .unwrap();
        }
        store.flush().unwrap();

        let query = [1.0, 0.0, 0.0];
        store.flush_target.reset_vector_sidecar_bytes_read();
        let unscoped = store.ann_search("vec_idx", &query, 1, 20).unwrap();
        let unscoped_bytes = store.flush_target.vector_sidecar_bytes_read();
        assert!(
            unscoped[0].score < 0.1,
            "control query should see the cross-prefix nearest vector"
        );

        store.flush_target.reset_vector_sidecar_bytes_read();
        let scoped = store
            .ann_search_in_partition_scope("vec_idx", scope_a.key.as_bytes(), &query, 1, 20)
            .unwrap();
        let scoped_bytes = store.flush_target.vector_sidecar_bytes_read();

        assert_eq!(scoped.len(), 1);
        assert!(
            scoped[0].score > 1.0,
            "scoped query must exclude closer vectors in other tenant/session prefixes"
        );
        assert!(
            scoped_bytes < unscoped_bytes,
            "scoped ANN should read a smaller sidecar than unscoped search: scoped={scoped_bytes}, unscoped={unscoped_bytes}"
        );
    }

    #[test]
    fn sparse_vector_update_on_existing_row_becomes_visible_to_readback_and_ann() {
        let flush_target = InMemoryFlushTarget::new();
        let store: TableStore<InMemoryFlushTarget> = TableStore::new(
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
        let _rotation: FlushOutcome = store
            .add_vector_index(VectorIndexConfig {
                index_name: "entity_embedding_ann".to_string(),
                column_position: 1,
                metric: ferrosa_index::DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
            })
            .unwrap();

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

    // -------------------------------------------------------------------------
    // Bounded SSTable reader pool — Phase 2/4 gates (FileFlushTarget).
    // -------------------------------------------------------------------------

    /// Build a file-backed store with an explicit reader-pool cap, then flush
    /// `n_sstables` SSTables (one per flush). Each flush writes a fresh row for
    /// `pk-{i % distinct_keys}`, so keys recur across SSTables to exercise
    /// cross-source merge and tier splitting.
    fn file_store_with_many_sstables(
        dir: &std::path::Path,
        cap: usize,
        n_sstables: usize,
        distinct_keys: usize,
    ) -> TableStore<crate::flush::FileFlushTarget> {
        let mut store = file_backed_test_store(dir);
        let pool = Arc::new(crate::reader_pool::ReaderPool::new(cap));
        store.attach_reader_pool(pool, "bound-test".to_string());
        for i in 0..n_sstables {
            let key = make_key(&format!("pk-{}", i % distinct_keys.max(1)));
            store
                .write(&key, make_row(format!("v{i}").as_bytes(), 1000 + i as i64))
                .unwrap();
            store.flush().unwrap();
        }
        store
    }

    /// Re-attach a fresh pool of capacity `cap`, resetting the peak gauge so a
    /// subsequent read path's residency can be measured in isolation.
    fn reset_pool(store: &mut TableStore<crate::flush::FileFlushTarget>, cap: usize) {
        let pool = Arc::new(crate::reader_pool::ReaderPool::new(cap));
        store.attach_reader_pool(pool, "bound-test".to_string());
    }

    impl TableStore<crate::flush::FileFlushTarget> {
        /// Test-only: is the pool holding a resident reader for this raw gen
        /// string? Keys identically to the live read path via
        /// `SstableDescriptor::gen_num_for`, so a removed input gen showing up
        /// here would be a stale-reopen / non-eviction bug (FMEA #4).
        fn pool_contains_gen(&self, gen: &str) -> bool {
            let key = (
                self.pool_table_key.clone(),
                SstableDescriptor::gen_num_for(gen),
            );
            self.reader_pool.contains(&key)
        }
    }

    #[test]
    fn resident_reader_count_stays_within_cap_for_many_sstables() {
        // Phase 2 gate: load N >> cap SSTables on a FileFlushTarget store and
        // assert resident readers never exceed the cap.
        let dir = tempfile::tempdir().unwrap();
        let cap = 4;
        let n = 40;
        let store = file_store_with_many_sstables(dir.path(), cap, n, n);

        assert_eq!(store.sstable_count(), n, "all SSTables registered");
        assert!(
            store.resident_reader_count() <= cap,
            "resident readers {} must be <= cap {cap}",
            store.resident_reader_count()
        );
        for i in 0..n {
            let _ = store.read(&make_key(&format!("pk-{i}"))).unwrap();
        }
        assert!(
            store.resident_reader_count() <= cap,
            "resident readers {} must stay <= cap {cap} after reads",
            store.resident_reader_count()
        );
        assert!(
            store.peak_resident_readers() <= cap,
            "peak resident {} must be <= cap {cap}",
            store.peak_resident_readers()
        );
    }

    #[test]
    fn high_sstable_fanout_is_observable_without_unbounded_readers() {
        let dir = tempfile::tempdir().unwrap();
        let cap = 4usize;
        let n_sstables = 40usize;
        let mut store = file_store_with_many_sstables(dir.path(), cap, n_sstables, n_sstables);
        reset_pool(&mut store, cap);

        let metric = |text: &str, name: &str| -> u64 {
            text.lines()
                .find_map(|line| {
                    line.strip_prefix(name)
                        .and_then(|value| value.trim().parse::<u64>().ok())
                })
                .unwrap_or(0)
        };
        let before = crate::metrics::render_prometheus();
        let before_alerts = metric(&before, "ferrosa_storage_read_sstable_high_fanout_total ");

        let row = store.read(&make_key("pk-0")).unwrap();
        assert!(
            row.is_some(),
            "diagnostic must not truncate or fail the read"
        );

        let after = crate::metrics::render_prometheus();
        assert!(
            metric(&after, "ferrosa_storage_read_sstable_high_fanout_total ") > before_alerts,
            "a 40-SSTable point read must increment the high-fanout counter"
        );
        assert!(
            metric(&after, "ferrosa_storage_read_sstable_fanout_max ") >= n_sstables as u64,
            "fanout max must expose the complete descriptor scan"
        );
        assert!(
            store.peak_resident_readers() <= cap,
            "diagnostics must not open or retain more than the reader cap"
        );
        assert_eq!(
            store.reader_pool.soft_cap_breaches(),
            0,
            "diagnostics must preserve bounded reader-pool admission"
        );
    }

    #[test]
    fn peak_open_readers_stays_within_fanin_during_staged_merge() {
        // REGRESSION (read-merge reader-count unbounded): a single token-range
        // READ over N >> fanin FULL-OVERLAP SSTables must hold at most
        // `fanin_cap` readers concurrently. We prove this by capping the shared
        // pool at EXACTLY `fanin_cap`: if any single read needed more than
        // `fanin_cap` readers open at once, an in-use reader could not be
        // evicted and the pool would record a SOFT-CAP BREACH (and peak resident
        // would exceed the cap). `read_token_range` processes one source to
        // completion and drops it before opening the next, so even with the
        // pool pinned at the fan-in cap it never breaches.
        //
        // The *digest* path (`walk_token_range_for_digest`) and
        // `walk_token_range` now hold the same bound under full overlap via the
        // bounded multi-pass merge cascade — see
        // `digest_walk_reader_count_bounded_under_full_overlap` and
        // `walk_token_range_reader_count_bounded_under_full_overlap`.
        let fanin = crate::reader_pool::configured_read_merge_fanin();
        let n_sstables = fanin * 3 + 7; // comfortably above fanin
        let cap = fanin; // pool pinned at the fan-in cap
        let distinct_keys = 6;
        let dir = tempfile::tempdir().unwrap();
        let mut store = file_store_full_overlap(dir.path(), cap, n_sstables, distinct_keys);
        reset_pool(&mut store, cap);

        // Full-range read: every full-overlap SSTable participates in the merge
        // for every key, so a non-staged merge would try to hold all
        // `n_sstables` readers open at once — impossible under a pool capped at
        // `fanin`, forcing a soft-cap breach.
        let merged = store
            .read_token_range(i64::MIN, i64::MAX, distinct_keys)
            .unwrap();
        assert_eq!(
            merged.len(),
            distinct_keys,
            "fixture must merge to all keys"
        );

        assert!(
            store.peak_resident_readers() <= fanin,
            "read_token_range peak resident readers {} exceeded the fan-in cap \
             {fanin} (n_sstables={n_sstables})",
            store.peak_resident_readers()
        );
        assert_eq!(
            store.reader_pool.soft_cap_breaches(),
            0,
            "a single read_token_range needed more than fanin={fanin} readers \
             open at once (soft-cap breached) over {n_sstables} full-overlap \
             SSTables — read-merge reader-count is not bounded by the fan-in"
        );
    }

    /// REGRESSION — the "repair full-overlap reader-count OOM (node1)" wall,
    /// property #2 of `specs/proposed/repair-fuzz-harness-design.md`. Was a
    /// gated known-failure; now passes via the bounded multi-pass merge cascade.
    ///
    /// The digest walk used by the repair Merkle build
    /// (`repair::build_tree_for_range` -> `walk_token_range_for_digest`) used to
    /// hold **O(sstable_count)** SSTable readers open simultaneously under FULL
    /// token overlap, not **O(fan-in)** — even with the reader pool pinned at
    /// the fan-in cap. The fuzz harness shrank it to this deterministic repro:
    ///
    ///   cap = fanin = 4, n_sstables = 8, distinct_keys = 6, full overlap
    ///     -> BEFORE: digest peak resident readers = 8 (== n_sstables),
    ///        soft-cap breaches = 4. AFTER: peak <= 4, breaches == 0.
    ///
    /// On a node bloated with thousands of full-overlap SSTables (the
    /// `entity_store` shape) the old path opened every reader at once and
    /// OOM-killed the node under its cgroup. The fix cascades the overlapping
    /// inputs into ephemeral sorted runs in batches of `<= budget`, so neither
    /// the open-reader count nor the materialised-partition data scales with
    /// table size, while the digest XOR stays byte-identical (associative LWW).
    #[test]
    fn digest_walk_reader_count_bounded_under_full_overlap() {
        // Minimal shrunk repro from the harness. The pool is pinned at a small
        // cap and the SSTable count exceeds it — the bounded cascade keeps peak
        // resident at `cap` with zero soft-cap breaches. Uses an explicit cap
        // (not the env-configurable fan-in) so the repro is deterministic
        // regardless of `FERROSA_READ_MERGE_FANIN`.
        let cap = 4usize;
        let n_sstables = 8usize; // > cap → eviction must engage if bounded
        let distinct_keys = 6usize;
        let dir = tempfile::tempdir().unwrap();
        let mut store = file_store_full_overlap(dir.path(), cap, n_sstables, distinct_keys);
        reset_pool(&mut store, cap);

        let mut visited = 0usize;
        store
            .walk_token_range_for_digest(i64::MIN, i64::MAX, |_k, _d, _s, emit| {
                visited += 1;
                emit(&mut |_row| Ok(()))
            })
            .unwrap();
        assert_eq!(visited, distinct_keys, "digest must visit every merged key");

        assert!(
            store.peak_resident_readers() <= cap,
            "digest walk peak resident readers {} exceeded the pool cap {cap} \
             (n_sstables={n_sstables}); repair Merkle build opens O(sstable_count) \
             readers under full overlap (repair full-overlap reader-count OOM)",
            store.peak_resident_readers()
        );
        assert_eq!(
            store.reader_pool.soft_cap_breaches(),
            0,
            "digest walk soft-cap-breached over {n_sstables} full-overlap SSTables \
             with a pool cap of {cap} — repair Merkle build is not bounded by \
             the fan-in"
        );
    }

    /// REGRESSION (mirror of `digest_walk_reader_count_bounded_under_full_overlap`
    /// for the general read-merge walk). `walk_token_range` shared the same
    /// O(sstable_count)-open-readers flaw under full overlap (it opened every
    /// overlapping reader up front for a single-pass k-way merge); the bounded
    /// multi-pass cascade fixes both. Proves peak resident readers stays at the
    /// pool cap with zero soft-cap breaches, and that the walk still visits every
    /// merged key.
    #[test]
    fn walk_token_range_reader_count_bounded_under_full_overlap() {
        let cap = 4usize;
        let n_sstables = 8usize; // > cap → cascade must engage if bounded
        let distinct_keys = 6usize;
        let dir = tempfile::tempdir().unwrap();
        let mut store = file_store_full_overlap(dir.path(), cap, n_sstables, distinct_keys);
        reset_pool(&mut store, cap);

        let mut visited = 0usize;
        store
            .walk_token_range(i64::MIN, i64::MAX, |_p| {
                visited += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(visited, distinct_keys, "walk must visit every merged key");

        assert!(
            store.peak_resident_readers() <= cap,
            "walk_token_range peak resident readers {} exceeded the pool cap {cap} \
             (n_sstables={n_sstables}); read-merge opens O(sstable_count) readers \
             under full overlap",
            store.peak_resident_readers()
        );
        assert_eq!(
            store.reader_pool.soft_cap_breaches(),
            0,
            "walk_token_range soft-cap-breached over {n_sstables} full-overlap \
             SSTables with a pool cap of {cap} — read-merge is not bounded by the \
             fan-in"
        );
    }

    proptest::proptest! {
        // File-IO-heavy (each case flushes `n_sstables` real SSTables), so a
        // bounded case count keeps the in-crate lib suite fast. `PROPTEST_CASES`
        // overrides for a deep run.
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(16))]

        /// PROPERTY #2 (spec §"Invariant properties") — bounded memory under
        /// FULL token overlap. For randomly-sized full-overlap tables (every
        /// SSTable spans the ring and shares the key set), the digest walk must
        /// keep BOTH peak materialised partitions AND peak open readers within
        /// O(open sources)/O(budget), and visit every merged key — regardless of
        /// SSTable count or data volume. The MATERIALISED-PARTITION half asserts
        /// the `#[cfg(test)]`-only `inflight` gauge; the READER-COUNT half (added
        /// after the bounded multi-pass merge cascade landed) asserts the pool's
        /// peak residency and zero soft-cap breaches. Both now pass.
        #[test]
        fn property_digest_walk_data_bounded_under_full_overlap(
            n_sstables in 1usize..16,
            distinct_keys in 1usize..8,
        ) {
            let dir = tempfile::tempdir().unwrap();
            // Small fixed cap so the reader-count half (gated) reliably exceeds
            // it once n_sstables > cap; the materialised-partition half below is
            // cap-independent.
            let cap = 4usize;
            let mut store =
                file_store_full_overlap(dir.path(), cap, n_sstables, distinct_keys);
            reset_pool(&mut store, cap);

            inflight::reset();
            let mut visited = 0usize;
            store
                .walk_token_range_for_digest(i64::MIN, i64::MAX, |_k, _d, _s, emit| {
                    visited += 1;
                    emit(&mut |_row| Ok(()))
                })
                .unwrap();

            proptest::prop_assert_eq!(
                visited, distinct_keys,
                "digest must visit every merged key (data loss)"
            );
            let mat_peak = inflight::peak();
            proptest::prop_assert!(
                mat_peak <= distinct_keys,
                "materialised-partition peak {} scaled with table size under full \
                 overlap (n_sstables={}, distinct_keys={}); must be O(open sources), \
                 not O(total partitions {})",
                mat_peak, n_sstables, distinct_keys, n_sstables * distinct_keys
            );

            // READER-COUNT half — now passes via the bounded multi-pass merge
            // cascade (was gated behind `repair-fuzz-known-failures`).
            {
                let reader_peak = store.peak_resident_readers();
                proptest::prop_assert!(
                    reader_peak <= cap,
                    "peak open readers {} exceeded pool cap {} during full-overlap \
                     digest walk (n_sstables={})",
                    reader_peak, cap, n_sstables
                );
                proptest::prop_assert_eq!(
                    store.reader_pool.soft_cap_breaches(), 0u64,
                    "digest walk soft-cap-breached over {} full-overlap SSTables \
                     with pool cap {}",
                    n_sstables, cap
                );
            }
        }
    }

    #[test]
    fn streaming_token_range_read_is_byte_identical_to_single_pass() {
        // GOLDEN EQUIVALENCE (FMEA #2/#3): the streaming k-way merge in
        // `walk_token_range` must return exactly the same partitions as the
        // single-pass `read_token_range` for every token window, with the same
        // dedup/LWW/tombstone result. Many overlapping SSTables over few keys
        // exercise the cross-source cell-merge path.
        let dir = tempfile::tempdir().unwrap();
        let store = file_store_with_many_sstables(dir.path(), 256, 12, 5);

        let windows: [(i64, i64); 6] = [
            (i64::MIN, i64::MAX),
            (i64::MIN, 0),
            (0, i64::MAX),
            (-5_000_000_000_000_000_000, 5_000_000_000_000_000_000),
            (-1, 1),
            (i64::MIN + 1, i64::MAX - 1),
        ];

        let fixture_partition_limit = 5;
        for (start, end) in windows {
            let rtr = store
                .read_token_range(start, end, fixture_partition_limit)
                .unwrap();
            let mut walk: Vec<Partition> = Vec::new();
            store
                .walk_token_range(start, end, |p| {
                    walk.push(p.clone());
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                rtr, walk,
                "streaming walk_token_range diverged from single-pass \
                 read_token_range for window [{start}, {end})"
            );
        }
    }

    #[test]
    fn streaming_digest_walk_is_byte_identical_to_single_pass() {
        // GOLDEN EQUIVALENCE for the digest (repair Merkle) path: the streaming
        // digest walk must reconstruct exactly the same partitions, in token
        // order, as the single-pass `read_token_range`.
        let dir = tempfile::tempdir().unwrap();
        let store = file_store_with_many_sstables(dir.path(), 256, 10, 4);

        let mut digest: Vec<Partition> = Vec::new();
        store
            .walk_token_range_for_digest(i64::MIN, i64::MAX, |key, deletion, static_row, emit| {
                let mut rows: Vec<Row> = Vec::new();
                emit(&mut |row| {
                    rows.push(row.clone());
                    Ok(())
                })?;
                digest.push(Partition {
                    key: key.clone(),
                    deletion,
                    static_row: static_row.cloned(),
                    rows,
                });
                Ok(())
            })
            .unwrap();

        let rtr = store.read_token_range(i64::MIN, i64::MAX, 4).unwrap();
        assert_eq!(
            digest, rtr,
            "streaming digest walk partitions must match single-pass read_token_range"
        );
    }

    /// Build a file-backed store with `n_sstables`, each of which holds one row
    /// for EVERY one of `distinct_keys` keys, so every SSTable spans the full
    /// token range and overlaps every other (mirrors `entity_store`/`typed_edges`
    /// where each SSTable covers the whole ring). Total partitions across the
    /// table = `distinct_keys` (recurring across all SSTables), and an in-range
    /// scan must touch all of them. This is the shape that OOM-killed the node
    /// under tier materialisation.
    fn file_store_full_overlap(
        dir: &std::path::Path,
        cap: usize,
        n_sstables: usize,
        distinct_keys: usize,
    ) -> TableStore<crate::flush::FileFlushTarget> {
        let mut store = file_backed_test_store(dir);
        let pool = Arc::new(crate::reader_pool::ReaderPool::new(cap));
        store.attach_reader_pool(pool, "bound-test".to_string());
        for s in 0..n_sstables {
            for k in 0..distinct_keys.max(1) {
                let key = make_key(&format!("pk-{k}"));
                store
                    .write(
                        &key,
                        make_row(format!("v{s}-{k}").as_bytes(), 1000 + s as i64),
                    )
                    .unwrap();
            }
            store.flush().unwrap();
        }
        store
    }

    /// ST-41 residual: with more overlapping SSTables than the reader budget the
    /// scan cascades through `merge_sstable_iters`; a source that fails to decode
    /// mid-merge must surface as the typed error naming that generation and be
    /// quarantined, not leak the raw SSTable error from the spill pass.
    #[test]
    fn bounded_merge_mid_stream_decode_error_is_typed_and_quarantines() {
        let cap = 2usize;
        let dir = tempfile::tempdir().unwrap();
        let mut store = file_store_full_overlap(dir.path(), cap, 5, 6);
        reset_pool(&mut store, cap);

        let victim = store.view.load().sstables[0].clone();
        let base = if victim.dir.as_os_str().is_empty() {
            dir.path().to_path_buf()
        } else {
            victim.dir.clone()
        };
        let data_path = base.join(format!("{}-Data.db", victim.gen));
        // Drop the checksum components (a pre-T-011 generation has none, so
        // the reader treats them as "not checked") and cut Data.db short: the
        // reader then opens and positions cleanly on the first partition and
        // only fails when the merge decodes a later one.
        for suffix in ["CRC.db", "Digest.crc32"] {
            let p = base.join(format!("{}-{suffix}", victim.gen));
            if p.exists() {
                std::fs::remove_file(&p).unwrap();
            }
        }
        let data = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .unwrap_or_else(|e| panic!("open {}: {e}", data_path.display()));
        // Cut mid-partition (not on a partition boundary, which would read as a
        // clean end of data).
        data.set_len(data.metadata().unwrap().len() / 2 + 5)
            .unwrap();

        let err = store
            .walk_token_range(i64::MIN, i64::MAX, |_| Ok(()))
            .expect_err("a source that fails mid-merge must fail the cascade");
        assert_names_sstable(&err, &victim.gen);
        assert!(
            store.is_sstable_quarantined(&victim.gen),
            "the generation that failed mid-merge must be quarantined"
        );
    }

    /// LARGE-RANGE DATA-BOUND GATE (the gap that let the OOM regression through).
    ///
    /// With N >> fanin SSTables that each span the FULL token range, a
    /// full-range digest build (and the matching `walk_token_range`) must hold
    /// only `O(open sources)` partitions materialised at any instant — NOT
    /// `O(total partitions in range)`. The previous tier-materialising code
    /// collected every in-range partition of each tier into a `Vec<Partition>`
    /// up front, so peak in-flight grew with table size and OOM-killed the node.
    ///
    /// This asserts the test-only in-flight gauge stays a small constant
    /// (memtable-match order) and crucially does NOT scale with the partition
    /// count. It FAILS (RED) on tier materialisation and PASSES (GREEN) on the
    /// streaming merge.
    #[test]
    fn large_range_digest_is_data_bounded_not_table_bounded() {
        let dir = tempfile::tempdir().unwrap();
        // 40 SSTables, each holding all 12 keys → 480 partition copies on disk,
        // 12 distinct merged partitions in range, every SSTable full-overlap.
        let distinct_keys = 12;
        let n_sstables = 40;
        let store = file_store_full_overlap(dir.path(), 1024, n_sstables, distinct_keys);

        // Sanity: a single-pass read sees all distinct keys (the merged result).
        let merged = store
            .read_token_range(i64::MIN, i64::MAX, distinct_keys)
            .unwrap();
        assert_eq!(
            merged.len(),
            distinct_keys,
            "fixture must merge down to {distinct_keys} partitions"
        );

        // Digest walk: everything is flushed (no memtable matches), and the
        // streaming digest path materialises NO full SSTable partition (it uses
        // header-only + row streaming), so peak in-flight must be ~0 — and in any
        // case must NOT scale toward the total in-range partition count.
        inflight::reset();
        let mut visited = 0usize;
        store
            .walk_token_range_for_digest(i64::MIN, i64::MAX, |_k, _d, _s, emit| {
                visited += 1;
                emit(&mut |_row| Ok(()))
            })
            .unwrap();
        assert_eq!(visited, distinct_keys, "digest must visit every merged key");
        let digest_peak = inflight::peak();
        assert!(
            digest_peak <= distinct_keys,
            "digest peak in-flight partitions {digest_peak} scaled with table size \
             (tier materialisation regression); must be O(open sources), not \
             O(total partitions). With everything flushed it should be ~0."
        );

        // `walk_token_range` decodes one full partition per source for the
        // current key only — bounded by the open-source count, never the whole
        // table. Peak must stay well under the total partition copies on disk
        // (n_sstables * distinct_keys = 480).
        inflight::reset();
        let mut walked = 0usize;
        store
            .walk_token_range(i64::MIN, i64::MAX, |_p| {
                walked += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(walked, distinct_keys, "walk must visit every merged key");
        let walk_peak = inflight::peak();
        assert!(
            walk_peak <= distinct_keys,
            "walk_token_range peak in-flight partitions {walk_peak} scaled with \
             table size (tier materialisation regression); must be O(open \
             sources), not O(total partitions {})",
            n_sstables * distinct_keys
        );
    }

    /// Silent-corruption guard for the repair *fetch* path: looping
    /// `read_token_range_bounded` across a window must return byte-identical
    /// partitions, in token order, to the single-pass `read_token_range` for
    /// the same window, under every count/byte budget. This proves the
    /// streaming-merge migration preserved the merge semantics.
    #[test]
    fn bounded_fetch_is_byte_identical_to_single_pass_read_token_range() {
        let dir = tempfile::tempdir().unwrap();
        // 64 SSTables over 12 distinct keys → heavy cross-source recurrence so
        // the cell-merge / dedup / tombstone-preservation paths are exercised.
        let store = file_store_with_many_sstables(dir.path(), 1024, 64, 12);

        // Reference: single-pass with a fixture-sized finite limit.
        let reference = store.read_token_range(i64::MIN, i64::MAX, 12).unwrap();
        assert!(!reference.is_empty(), "fixture must produce partitions");

        // Loop the bounded fetch over the full window under a spread of
        // (max_partitions, max_bytes) budgets; the result must reassemble to the
        // single-pass reference regardless of how the chunk boundaries fall.
        let all_partitions = reference.len().max(1);
        let large_byte_budget = 1_000_000usize;
        let cases: &[(usize, usize)] = &[
            (all_partitions, large_byte_budget), // single chunk
            (3, large_byte_budget),              // count budget
            (all_partitions, 64),                // byte budget
            (2, large_byte_budget),              // tight count budget
            (1, large_byte_budget),              // one partition per chunk
            (all_partitions, 48),                // tight byte budget
        ];

        for &(max_partitions, max_bytes) in cases {
            let mut collected: Vec<Partition> = Vec::new();
            let mut cursor = i64::MIN;
            let mut guard_iters = 0usize;
            loop {
                guard_iters += 1;
                assert!(guard_iters < 100_000, "bounded loop failed to terminate");
                let (chunk, next) = store
                    .read_token_range_bounded(cursor, i64::MAX, max_partitions, max_bytes)
                    .unwrap();
                if !chunk.is_empty() {
                    assert!(
                        chunk.len() <= max_partitions,
                        "chunk len {} exceeded count budget {max_partitions}",
                        chunk.len()
                    );
                    if let Some(prev) = collected.last() {
                        assert!(chunk[0].key >= prev.key, "chunk boundary broke token order");
                    }
                    collected.extend(chunk.into_iter().map(Arc::unwrap_or_clone));
                }
                match next {
                    Some(c) => cursor = c,
                    None => break,
                }
            }
            assert_eq!(
                collected, reference,
                "bounded fetch (max_partitions={max_partitions}, \
                 max_bytes={max_bytes}) diverged from single-pass read_token_range"
            );
        }
    }

    /// LARGE-RANGE DATA-BOUND GATE for the repair *fetch* path. With N >> fanin
    /// full-overlap SSTables, a byte-budgeted bounded fetch over the full range
    /// must keep peak in-flight materialised partitions within the budget order
    /// — NOT the total partition count. The tier-materialising regression staged
    /// whole tiers into memory BEFORE the byte-budget check ever ran, so peak
    /// was O(table). The streaming merge checks the budget before merging the
    /// next partition, so peak is `max_partitions` plus a small per-key group.
    #[test]
    fn bounded_fetch_is_data_bounded_not_table_bounded() {
        let dir = tempfile::tempdir().unwrap();
        // 30 SSTables each holding all 10 keys → 300 partition copies on disk,
        // 10 merged partitions in range, full overlap.
        let distinct_keys = 10;
        let n_sstables = 30;
        let store = file_store_full_overlap(dir.path(), 1024, n_sstables, distinct_keys);

        // Tight count budget: at most 2 partitions per chunk.
        inflight::reset();
        let mut total = 0usize;
        let mut cursor = i64::MIN;
        let large_byte_budget = 1_000_000usize;
        loop {
            let (chunk, next) = store
                .read_token_range_bounded(cursor, i64::MAX, 2, large_byte_budget)
                .unwrap();
            total += chunk.len();
            match next {
                Some(c) => cursor = c,
                None => break,
            }
        }
        assert_eq!(total, distinct_keys, "bounded fetch must visit every key");
        let peak = inflight::peak();
        // Everything is flushed → memtable sources are empty, so the gauge
        // (which tracks materialised `Vec<Partition>` sources) must be ~0 and in
        // no case scale toward the 300 partition copies on disk.
        assert!(
            peak <= distinct_keys,
            "bounded fetch peak in-flight partitions {peak} scaled with table \
             size (tier materialisation regression); must be budget-bounded, not \
             O(total partitions {})",
            n_sstables * distinct_keys
        );
    }

    // -------------------------------------------------------------------------
    // Phase 6 — Swap correctness (FMEA #4: stale gen after swap; #11).
    // -------------------------------------------------------------------------

    /// FMEA #4: after a compaction swap removes input generations, those gens
    /// must be evicted from the reader pool and never reopened or served. Reads
    /// must return the post-compaction data from the output SSTable, not stale
    /// rows from the removed inputs.
    #[test]
    fn swap_evicts_removed_gens_no_stale_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = file_backed_test_store(dir.path());
        // Generous cap: the bound is irrelevant here — we are proving eviction
        // semantics, so we must not let LRU pressure mask a missing remove().
        reset_pool(&mut store, 64);

        // Three input SSTables, distinct partition keys, one per flush.
        for i in 0..3 {
            store
                .write(
                    &make_key(&format!("k{i}")),
                    make_row(format!("v{i}").as_bytes(), 1000 + i as i64),
                )
                .unwrap();
            store.flush().unwrap();
        }
        assert_eq!(store.sstable_count(), 3);

        // Prime the pool: read every input so all three gens are resident.
        for i in 0..3 {
            let p = store.read(&make_key(&format!("k{i}"))).unwrap();
            assert!(p.is_some(), "input k{i} must be readable before swap");
        }

        // Snapshot the three input (gen, dir) pairs to be removed.
        let view = store.view.load();
        let input_id_paths: Vec<(String, std::path::PathBuf)> =
            view.sstable_ids.iter().cloned().collect();
        drop(view);
        assert_eq!(input_id_paths.len(), 3);
        let removed_gens: Vec<String> = input_id_paths.iter().map(|(g, _)| g.clone()).collect();
        for gen in &removed_gens {
            assert!(
                store.pool_contains_gen(gen),
                "input gen {gen} must be resident in the pool before swap"
            );
        }

        // Build a real file-backed compaction output that merges all three
        // inputs into one partition-per-key with a NEWER timestamp, so a stale
        // read of a removed gen would be detectably different from the output.
        // The output is materialised on disk by a standalone FileFlushTarget
        // (its own directory + generation counter), exactly as the compaction
        // executor produces a brand-new SSTable that is NOT yet in the view.
        let mut merged = vec![
            make_partition("k0", b"merged0", 9000),
            make_partition("k1", b"merged1", 9001),
            make_partition("k2", b"merged2", 9002),
        ];
        // The SSTable writer requires partitions in decorated-key (token) order.
        merged.sort_by(|a, b| a.key.cmp(&b.key));
        let output = {
            let schema = store.schema();
            let header = crate::flush::build_serialization_header(schema.as_ref(), &merged);
            let mut w = ferrosa_sstable::writer::SSTableWriter::new(
                WriteOptions {
                    compression: None,
                    ..WriteOptions::default()
                },
                header,
            );
            for p in &merged {
                w.add_partition(p).unwrap();
            }
            w.finish().unwrap()
        };
        let out_dir = dir.path().join("compacted-out");
        std::fs::create_dir_all(&out_dir).unwrap();
        let out_target = crate::flush::FileFlushTarget::new_starting_at(out_dir.clone()).unwrap();
        let out_reader = {
            use crate::flush::FlushTarget;
            Arc::new(out_target.flush(output).unwrap())
        };
        // Synthetic output id, distinct from any numeric input gen (its pool key
        // lives in the high-bit synthetic space — no collision with inputs).
        let out_gen = "compacted".to_string();

        let swap = store
            .swap_compacted_sstables(
                &input_id_paths,
                out_gen.clone(),
                out_dir.clone(),
                out_reader,
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(swap, CompactionSwap::Swapped);
        // 3 inputs removed, 1 compacted output inserted → exactly 1 remaining.
        assert_eq!(store.sstable_count(), 1, "3 inputs - 3 + 1 output = 1");

        // Every removed gen is evicted from the pool (FMEA #4 — no stale reopen).
        for gen in &removed_gens {
            assert!(
                !store.pool_contains_gen(gen),
                "removed input gen {gen} must be evicted from the pool after swap"
            );
        }
        // The output gen is seeded and resident.
        assert!(
            store.pool_contains_gen(&out_gen),
            "compacted output gen must be seeded into the pool"
        );

        // Reads now return POST-COMPACTION data from the output SSTable, and the
        // removed gens are never reopened (a reopen would re-add their pool key).
        for i in 0..3 {
            let part = store
                .read(&make_key(&format!("k{i}")))
                .unwrap()
                .unwrap_or_else(|| panic!("k{i} must be served from the compacted output"));
            let cell = &part.rows[0].cells[0].1;
            assert_eq!(
                cell.value.as_deref(),
                Some(format!("merged{i}").as_bytes()),
                "k{i} must return post-compaction value, not stale input"
            );
        }
        for gen in &removed_gens {
            assert!(
                !store.pool_contains_gen(gen),
                "removed gen {gen} must never be reopened by post-swap reads"
            );
        }
        // Only the output reader is resident — no stale input reader lingers.
        assert_eq!(
            store.resident_reader_count(),
            1,
            "only the compacted output reader should be resident after the swap + reads"
        );
    }

    /// FMEA #10/#11: a reader `Arc` obtained before a swap/eviction must remain
    /// valid and return complete, correct data for the whole scan even though
    /// the pool entry for its gen was removed. The `Arc` keeps the reader alive;
    /// `pool.remove` only drops the pool's reference, never the in-flight one.
    #[test]
    fn held_reader_survives_concurrent_swap_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = file_backed_test_store(dir.path());
        reset_pool(&mut store, 64);

        // One input SSTable with several rows under one partition so a scan over
        // it is non-trivial (truncation/use-after-free would be observable).
        let key = make_key("scan-pk");
        for ck in 0..16i32 {
            store
                .write(
                    &key,
                    make_row_with_ck(ck, format!("c{ck}").as_bytes(), 1000 + ck as i64),
                )
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.sstable_count(), 1);

        // Acquire the reader Arc the way a live scan does, and snapshot its gen.
        let view = store.view.load();
        let desc = view.sstables[0].clone();
        let (input_gen, _) = view.sstable_ids[0].clone();
        drop(view);
        let held: Arc<SSTableReader<ferrosa_sstable::io::FileReadAt>> =
            store.open_reader(&desc).unwrap();
        assert!(store.pool_contains_gen(&input_gen));

        // Read the full partition through the held reader BEFORE eviction to get
        // the ground-truth row set.
        let before: Vec<Row> = held.get_partition(&key).unwrap().unwrap().rows;
        assert_eq!(
            before.len(),
            16,
            "all 16 clustering rows present pre-eviction"
        );

        // Now evict that exact gen from the pool (simulating a concurrent swap
        // that removed the input). The pool drops its reference; `held` keeps the
        // reader alive.
        store.reader_pool.remove(&store.pool_key(&desc));
        assert!(
            !store.pool_contains_gen(&input_gen),
            "gen must be gone from the pool after eviction"
        );

        // The held Arc must still read the COMPLETE, correct partition — no
        // panic, no truncation, no use-after-evict. Re-read mid/post eviction.
        let after: Vec<Row> = held.get_partition(&key).unwrap().unwrap().rows;
        assert_eq!(
            after, before,
            "held reader must yield identical, complete results across eviction"
        );

        // Strong count proves the pool is no longer one of the holders; the scan
        // owns the only live reference and it is still valid.
        assert_eq!(
            Arc::strong_count(&held),
            1,
            "after eviction the held Arc is the sole owner — reader still alive"
        );

        // A fresh read through the store reopens the gen (it is in the view), and
        // it returns the same rows — the eviction did not corrupt on-disk state.
        let reopened = store.read(&key).unwrap().unwrap();
        assert_eq!(
            reopened.rows, before,
            "reopened reader (post-eviction) returns the same complete partition"
        );
    }

    /// An index on a PARTITION-KEY component must be built and answer reads.
    ///
    /// A partition-key value is not a cell — it is encoded in the key — so the
    /// cell-based `add_index` path cannot see it, exactly as it cannot see a
    /// clustering column. Clustering columns already have their own path
    /// (`add_clustering_index`, t_430c4188) which decodes the composite
    /// clustering bytes at a declared component. This is the same thing for
    /// the other half of the primary key.
    ///
    /// Without it, `CREATE INDEX ... (tenant_id)` on
    /// `PRIMARY KEY ((tenant_id, session_id), entity_id)` is accepted, builds
    /// nothing, and every read through it returns zero rows over a table full
    /// of data (2026-09-10).
    #[test]
    fn an_index_on_a_partition_key_component_is_built_and_answers() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "entity_store".to_string(),
            // Composite partition key: (tenant_id, session_id).
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                       org.apache.cassandra.db.marshal.UTF8Type,\
                       org.apache.cassandra.db.marshal.UTF8Type)"
                .to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "body".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let store = TableStore::new(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );

        // Component 0 of the partition key is the tenant.
        let _rotation: FlushOutcome = store
            .add_partition_key_index("idx_by_tenant".to_string(), 0, IndexType::BTree)
            .unwrap();

        // Two tenants, several sessions each: the rows land in different
        // partitions under the same tenant, which is the shape that matters.
        let mut mine = 0usize;
        for i in 0..6 {
            let tenant = if i % 3 == 2 { "tenant-b" } else { "tenant-a" };
            if tenant == "tenant-a" {
                mine += 1;
            }
            let key = make_composite_key(&[tenant, &format!("session-{i}")]);
            store
                .write(
                    &key,
                    Row {
                        clustering: vec![],
                        cells: vec![(0, CellValue::live(format!("row-{i}").into_bytes(), 1000))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(1000),
                    },
                )
                .unwrap();
        }

        let mut found = 0usize;
        store
            .read_by_index_each(
                "idx_by_tenant",
                &IndexKey(b"tenant-a".to_vec()),
                &mut |_| {
                    found += 1;
                    std::ops::ControlFlow::Continue(())
                },
            )
            .expect("a partition-key index must answer");
        assert_eq!(
            found, mine,
            "an index on a partition-key component must return every row for that \
             component; wrote {mine} for tenant-a and the index returned {found}"
        );
    }

    // ── t_c5bccc65: a partition-key index is partition-granular ─────────────
    //
    // Every row of a partition shares its partition-key value, so the index
    // has nothing to say per row. It used to post (pk, clustering) for every
    // row and point-read each one: ~101k single-row reads for one tenant of
    // agent_memory.entity_store. It now posts (pk, []) once per partition and
    // streams the partition's rows in bounded chunks.

    fn clustered_entity_store() -> TableStore<InMemoryFlushTarget> {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "entity_store".to_string(),
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                       org.apache.cassandra.db.marshal.UTF8Type,\
                       org.apache.cassandra.db.marshal.UTF8Type)"
                .to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "entity_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "body".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let store = TableStore::new(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_partition_key_index("idx_by_tenant".to_string(), 0, IndexType::BTree)
            .unwrap();
        store
    }

    fn write_session(
        store: &TableStore<InMemoryFlushTarget>,
        tenant: &str,
        session: &str,
        rows: i32,
    ) {
        let key = make_composite_key(&[tenant, session]);
        for entity in 1..=rows {
            store
                .write(&key, make_row_with_ck(entity, b"entity", 1000))
                .unwrap();
        }
    }

    /// The row positions an index read delivers, in delivery order.
    fn tenant_rows(
        store: &TableStore<InMemoryFlushTarget>,
        after: Option<&RowPosition>,
    ) -> Vec<RowPosition> {
        let mut delivered = Vec::new();
        store
            .read_by_index_each_after(
                "idx_by_tenant",
                &IndexKey(b"tenant-a".to_vec()),
                after,
                &mut |partition| {
                    delivered.extend(partition.rows.iter().map(|row| RowPosition {
                        partition_key: partition.key.key.as_bytes().to_vec(),
                        clustering_key: row.clustering.clone(),
                    }));
                    std::ops::ControlFlow::Continue(())
                },
            )
            .unwrap();
        delivered
    }

    #[test]
    fn a_partition_key_index_posts_once_per_partition() {
        let store = clustered_entity_store();
        for session in ["s1", "s2", "s3"] {
            write_session(&store, "tenant-a", session, 4);
        }
        write_session(&store, "tenant-b", "s9", 4);

        let postings = store
            .get_memtable_index("idx_by_tenant")
            .expect("declared index")
            .lookup(&IndexKey(b"tenant-a".to_vec()));
        assert_eq!(
            postings.len(),
            3,
            "one posting per tenant-a partition, not one per row: {postings:?}"
        );
        assert!(
            postings.iter().all(|p| p.clustering_key.is_empty()),
            "a partition posting names the partition, not a row: {postings:?}"
        );
    }

    #[test]
    fn a_partition_key_index_read_streams_partitions_instead_of_point_reading_rows() {
        let store = clustered_entity_store();
        write_session(&store, "tenant-a", "s1", 4);
        write_session(&store, "tenant-a", "s2", 4);
        store.flush().unwrap();
        write_session(&store, "tenant-a", "s3", 4);
        write_session(&store, "tenant-b", "s9", 4);

        INDEX_POINT_READS.with(|count| count.set(0));
        INDEX_PARTITION_CHUNKS.with(|count| count.set(0));
        let delivered = tenant_rows(&store, None);

        let mut expected = Vec::new();
        for session in ["s1", "s2", "s3"] {
            for entity in 1..=4_i32 {
                expected.push(RowPosition {
                    partition_key: make_composite_key(&["tenant-a", session])
                        .key
                        .as_bytes()
                        .to_vec(),
                    clustering_key: entity.to_be_bytes().to_vec(),
                });
            }
        }
        expected.sort();
        assert_eq!(delivered, expected, "every tenant row, in row order, once");
        assert_eq!(
            INDEX_POINT_READS.with(|count| count.get()),
            0,
            "a partition-key index read must not point-read rows one at a time"
        );
        assert_eq!(
            INDEX_PARTITION_CHUNKS.with(|count| count.get()),
            3,
            "one bounded chunk per (narrow) partition"
        );
    }

    #[test]
    fn a_partition_key_index_read_resumes_inside_a_partition() {
        let store = clustered_entity_store();
        for session in ["s1", "s2", "s3"] {
            write_session(&store, "tenant-a", session, 4);
        }
        let all = tenant_rows(&store, None);
        assert_eq!(all.len(), 12);

        // Resume after the second row of the second partition.
        let resumed = tenant_rows(&store, Some(&all[5]));
        assert_eq!(
            resumed,
            all[6..].to_vec(),
            "a page resumed mid-partition continues that partition, then the rest"
        );
    }

    /// A sidecar written before this change posts every row. Beside the
    /// partition posting, those rows must still come back once each.
    #[test]
    fn legacy_row_postings_beside_a_partition_posting_yield_each_row_once() {
        let store = clustered_entity_store();
        write_session(&store, "tenant-a", "s1", 3);
        let key = make_composite_key(&["tenant-a", "s1"]);
        let index = store.get_memtable_index("idx_by_tenant").unwrap();
        for entity in 1..=3_i32 {
            index.insert(
                IndexKey(b"tenant-a".to_vec()),
                RowPosition {
                    partition_key: key.key.as_bytes().to_vec(),
                    clustering_key: entity.to_be_bytes().to_vec(),
                },
            );
        }

        assert_eq!(tenant_rows(&store, None).len(), 3, "each row once");
    }

    /// The gate that decides whether the PLANNER may select such an index.
    ///
    /// A memtable index that loses its entries at the first flush is worse
    /// than no index: the planner would choose it, read nothing, and report an
    /// empty result as success — which is the exact bug this whole line of work
    /// exists to kill. So this asserts across a flush, not before one.
    #[test]
    fn a_partition_key_index_still_answers_after_the_memtable_is_flushed() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "entity_store".to_string(),
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                       org.apache.cassandra.db.marshal.UTF8Type,\
                       org.apache.cassandra.db.marshal.UTF8Type)"
                .to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "body".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let store = TableStore::new(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_partition_key_index("idx_by_tenant".to_string(), 0, IndexType::BTree)
            .unwrap();

        let mut mine = 0usize;
        for i in 0..6 {
            let tenant = if i % 3 == 2 { "tenant-b" } else { "tenant-a" };
            if tenant == "tenant-a" {
                mine += 1;
            }
            let key = make_composite_key(&[tenant, &format!("session-{i}")]);
            store
                .write(
                    &key,
                    Row {
                        clustering: vec![],
                        cells: vec![(0, CellValue::live(format!("row-{i}").into_bytes(), 1000))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(1000),
                    },
                )
                .unwrap();
        }

        store.flush().expect("flush must succeed");

        // Writes AFTER the flush must be indexed too. The flush installs a
        // freshly-built index map; a family of indexes left out of it is
        // declared and never written to again, and every write below would be
        // dropped on the floor without a word.
        for i in 6..10 {
            let tenant = if i % 3 == 2 { "tenant-b" } else { "tenant-a" };
            if tenant == "tenant-a" {
                mine += 1;
            }
            let key = make_composite_key(&[tenant, &format!("session-{i}")]);
            store
                .write(
                    &key,
                    Row {
                        clustering: vec![],
                        cells: vec![(0, CellValue::live(format!("row-{i}").into_bytes(), 1000))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(1000),
                    },
                )
                .unwrap();
        }

        let mut found = 0usize;
        store
            .read_by_index_each(
                "idx_by_tenant",
                &IndexKey(b"tenant-a".to_vec()),
                &mut |_| {
                    found += 1;
                    std::ops::ControlFlow::Continue(())
                },
            )
            .expect("a partition-key index must answer after a flush");
        assert_eq!(
            found, mine,
            "a partition-key index must survive a flush; wrote {mine} rows for \
             tenant-a and the index returned {found} after flushing. A memtable-only \
             index that empties on flush must NOT be selectable by the planner."
        );
    }

    /// Regression guard for the SHARDED flush path.
    ///
    /// `can_shard` lists the index families that force the single-SSTable
    /// path, because the sharded path does not write sidecars. A family
    /// missing from that list flushes to an index containing nothing, and
    /// nothing says so — the table simply answers every indexed read with
    /// zero rows. The threshold is 2 * 512 partitions, so this test has to be
    /// big enough to cross it; a smaller one passes no matter what `can_shard`
    /// says and proves nothing.
    #[test]
    fn a_partition_key_index_survives_a_flush_large_enough_to_shard() {
        use ferrosa_index::IndexKey;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "entity_store".to_string(),
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                       org.apache.cassandra.db.marshal.UTF8Type,\
                       org.apache.cassandra.db.marshal.UTF8Type)"
                .to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "body".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let store = TableStore::new(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );
        let _rotation: FlushOutcome = store
            .add_partition_key_index("idx_by_tenant".to_string(), 0, IndexType::BTree)
            .unwrap();

        // Comfortably past 2 * MIN_PARTITIONS_PER_FLUSH_SHARD (512).
        let rows = 1200usize;
        let mut mine = 0usize;
        for i in 0..rows {
            let tenant = if i % 4 == 3 { "tenant-b" } else { "tenant-a" };
            if tenant == "tenant-a" {
                mine += 1;
            }
            let key = make_composite_key(&[tenant, &format!("session-{i}")]);
            store
                .write(
                    &key,
                    Row {
                        clustering: vec![],
                        cells: vec![(0, CellValue::live(format!("row-{i}").into_bytes(), 1000))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(1000),
                    },
                )
                .unwrap();
        }
        store.flush().expect("flush must succeed");

        let mut found = 0usize;
        store
            .read_by_index_each(
                "idx_by_tenant",
                &IndexKey(b"tenant-a".to_vec()),
                &mut |_| {
                    found += 1;
                    std::ops::ControlFlow::Continue(())
                },
            )
            .expect("a partition-key index must answer after a sharded-size flush");
        assert_eq!(
            found, mine,
            "a partition-key index must survive a flush large enough to shard; \
             wrote {mine} rows for tenant-a across {rows} partitions and the index \
             returned {found}. If this is 0, `can_shard` took the sharded path and \
             skipped sidecar construction."
        );
    }

    /// The structural guard: EVERY declared index family must appear in the
    /// live index map a flush installs.
    ///
    /// The two tests above catch the partition-key family specifically. This
    /// one catches the next family somebody adds, because the failure mode is
    /// silent by construction: `new_indexes` builds the map from an explicit
    /// list of families, and a family left off that list is declared, accepted,
    /// never written to, and reads back empty.
    #[test]
    fn every_declared_index_family_is_present_in_the_live_map_after_a_flush() {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "entity_store".to_string(),
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                       org.apache.cassandra.db.marshal.UTF8Type,\
                       org.apache.cassandra.db.marshal.UTF8Type)"
                .to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "entity_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "body".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let store = TableStore::new(
            schema,
            InMemoryFlushTarget::new(),
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
        );

        // One index of each family that exists today.
        let _rotation: FlushOutcome = store
            .add_index("idx_regular".to_string(), 0, IndexType::BTree)
            .unwrap();
        let _rotation: FlushOutcome = store
            .add_clustering_index("idx_clustering".to_string(), 0, IndexType::BTree)
            .unwrap();
        let _rotation: FlushOutcome = store
            .add_partition_key_index("idx_partition_key".to_string(), 0, IndexType::BTree)
            .unwrap();

        let declared: Vec<String> = store.catalog().scalar_index_names().cloned().collect();
        assert_eq!(
            declared.len(),
            3,
            "the test must declare one of each family"
        );

        store.flush().expect("flush must succeed");

        let live = store.view.load();
        let missing: Vec<&String> = declared
            .iter()
            .filter(|name| !live.indexes.contains_key(*name))
            .collect();
        assert!(
            missing.is_empty(),
            "every declared index must exist in the live map a flush installs; \
             missing {missing:?}. An index in this list is declared and never \
             maintained: writes skip it and reads of it return nothing, with no \
             error on either path."
        );
    }

    /// Drain `stream`, reading one item every `pace` so the producer keeps
    /// finding its channel full. Returns the keys in arrival order, stopping
    /// at `cap` so a scan that repeats itself fails its test instead of
    /// running forever.
    async fn drain_slowly(
        mut stream: std::pin::Pin<
            Box<dyn futures::stream::Stream<Item = Result<Arc<Partition>>> + Send>,
        >,
        pace: std::time::Duration,
        cap: usize,
        mut between: impl FnMut(usize),
    ) -> Vec<DecoratedKey> {
        let mut keys = Vec::new();
        while keys.len() < cap {
            let Some(item) = futures::StreamExt::next(&mut stream).await else {
                break;
            };
            keys.push(item.expect("the scan must not fail").key.clone());
            between(keys.len());
            tokio::time::sleep(pace).await;
        }
        keys
    }

    /// A store whose partitions are spread over two SSTables and the active
    /// memtable, so a resume re-opens a real k-way merge.
    fn store_across_sources(partitions: usize) -> TableStore<InMemoryFlushTarget> {
        let store = test_store();
        for i in 0..partitions {
            store
                .write(&make_key(&format!("k{i:05}")), make_row(b"v", 1000))
                .unwrap();
            if i == partitions / 3 || i == 2 * partitions / 3 {
                store.flush().unwrap();
            }
        }
        store
    }

    /// An `OwnedMerger` over the whole of `store`.
    fn owned_merger_over(
        store: &TableStore<InMemoryFlushTarget>,
    ) -> OwnedMerger<<InMemoryFlushTarget as FlushTarget>::Reader> {
        let view = store.view.load_full();
        let readers = open_pooled_readers(
            &store.reader_pool,
            &store.pool_table_key,
            &*store.flush_target,
            &view.sstables,
            None,
            None,
        )
        .unwrap();
        let schema = store.schema.load_full();
        OwnedMerger::open(view, readers, &schema, None, None, None).unwrap()
    }

    /// The `OwnedMerger` self-reference (unsafe) under the conditions it must
    /// survive, with no async runtime so Miri can run it
    /// (`cargo +nightly miri test -p ferrosa-storage --lib owned_merger`):
    /// moved between threads mid-scan, outliving the store it came from, and
    /// dropped, all yielding exactly what a plain merger yields.
    #[test]
    fn owned_merger_keeps_its_place_across_threads_and_outlives_its_store() {
        let store = store_across_sources(24);
        let reference: Vec<DecoratedKey> = {
            let mut owned = owned_merger_over(&store);
            std::iter::from_fn(|| owned.merger.next_merged_partition().unwrap())
                .map(|p| p.key.clone())
                .collect()
        };
        assert_eq!(reference.len(), 24);

        let mut owned = owned_merger_over(&store);
        // The merger owns what it borrows: the store can go first.
        drop(store);
        let mut keys = Vec::new();
        loop {
            let (back, chunk, done) = std::thread::spawn(move || {
                let mut chunk = Vec::new();
                for _ in 0..5 {
                    match owned.merger.next_merged_partition().unwrap() {
                        Some(p) => chunk.push(p.key.clone()),
                        None => return (owned, chunk, true),
                    }
                }
                (owned, chunk, false)
            })
            .join()
            .expect("puller thread");
            owned = back;
            keys.extend(chunk);
            if done {
                break;
            }
        }
        drop(owned);
        assert_eq!(keys, reference);
    }

    /// The projected variant: the merger reads the owned projection (`wanted`)
    /// on every SSTable partition it decodes, so Miri sees each use of that
    /// self-borrow after the struct has moved threads and the store is gone.
    #[test]
    fn owned_merger_reads_its_owned_projection_after_moving() {
        let store = store_across_sources(18);
        let view = store.view.load_full();
        let readers = open_pooled_readers(
            &store.reader_pool,
            &store.pool_table_key,
            &*store.flush_target,
            &view.sstables,
            None,
            None,
        )
        .unwrap();
        let schema = store.schema.load_full();
        let mut owned =
            OwnedMerger::open(view, readers, &schema, Some(vec![0]), None, None).unwrap();
        drop(store);
        let keys = std::thread::spawn(move || {
            let mut keys = Vec::new();
            while let Some(p) = owned.merger.next_merged_partition().unwrap() {
                keys.push(p.key.clone());
            }
            keys
        })
        .join()
        .expect("puller thread");
        assert_eq!(keys.len(), 18);
    }

    /// A panic mid-scan unwinds through the `OwnedMerger`: the merger is
    /// dropped before the readers, mappings and view it borrows.
    #[test]
    fn owned_merger_unwinds_cleanly_from_a_panic_mid_scan() {
        let store = store_across_sources(12);
        let mut owned = owned_merger_over(&store);
        drop(store);
        let outcome = std::thread::spawn(move || {
            owned
                .merger
                .next_merged_partition()
                .unwrap()
                .expect("a partition");
            panic!("mid-scan");
        })
        .join();
        assert!(outcome.is_err(), "the puller panicked and unwound");
    }

    /// A scan whose consumer keeps stalling pauses (no slot, no thread) and
    /// resumes over and over, and still yields exactly the partitions, in
    /// exactly the order, of an uninterrupted scan: no gap, no duplicate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_paused_range_scan_resumes_with_no_gap_or_duplicate() {
        const N: usize = 300;
        let store = store_across_sources(N);
        let reference: Vec<DecoratedKey> = {
            let mut stream = store.range_iter(None, None);
            let mut keys = Vec::new();
            while let Some(item) = futures::StreamExt::next(&mut stream).await {
                keys.push(item.unwrap().key.clone());
            }
            keys
        };
        assert_eq!(
            reference.len(),
            N,
            "the reference scan sees every partition"
        );

        let releases = ferrosa_sched::scan_releases_total();
        let resumes = range_scan_resumes_total();
        // Every time the 4-item channel fills, the producer pauses.
        let paused = store.whole_partition_range_scan(None, None, None, None);
        let keys = drain_slowly(
            paused,
            std::time::Duration::from_millis(1),
            2 * reference.len(),
            |_| {},
        )
        .await;

        assert_eq!(
            keys, reference,
            "a paused scan must yield exactly the reference"
        );
        let paused_runs = ferrosa_sched::scan_releases_total() - releases;
        assert!(
            paused_runs >= 10,
            "the slow consumer should have paused the scan many times, saw {paused_runs}"
        );
        assert!(
            range_scan_resumes_total() - resumes >= 10,
            "every pause must be followed by a resume"
        );
    }

    /// The projected variant pauses and resumes the same way, and its
    /// partition limit counts what was handed out before each pause.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_paused_projected_scan_honours_its_partition_limit() {
        let store = store_across_sources(120);
        let reference: Vec<DecoratedKey> = {
            let mut stream = store.range_iter(None, None);
            let mut keys = Vec::new();
            while let Some(item) = futures::StreamExt::next(&mut stream).await {
                keys.push(item.unwrap().key.clone());
            }
            keys
        };
        let paused = store.whole_partition_range_scan(Some(vec![0]), Some(50), None, None);
        let keys = drain_slowly(paused, std::time::Duration::from_millis(1), 100, |_| {}).await;
        assert_eq!(keys, reference[..50].to_vec());
    }

    /// Writes made while a scan is paused: every partition that existed when
    /// the scan began is yielded exactly once, nothing is yielded twice, and
    /// keys stay in order. A key written during the scan, ahead of it, may or
    /// may not appear — storage range scans are not snapshots, paused or not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_paused_scan_under_concurrent_writes_never_duplicates_or_drops() {
        const N: usize = 200;
        let store = std::sync::Arc::new(store_across_sources(N));
        let original: std::collections::BTreeSet<DecoratedKey> =
            (0..N).map(|i| make_key(&format!("k{i:05}"))).collect();
        let writer = std::sync::Arc::clone(&store);
        let paused = store.whole_partition_range_scan(None, None, None, None);
        // Bounded well above every key that can exist (originals plus one new
        // key per item read), so a scan that repeats itself stops and fails.
        let keys = drain_slowly(
            paused,
            std::time::Duration::from_millis(1),
            4 * N,
            move |seen| {
                // New partitions, overwrites of existing ones, and a flush, all
                // while the scan is mid-range.
                writer
                    .write(&make_key(&format!("new{seen:05}")), make_row(b"n", 2000))
                    .unwrap();
                writer
                    .write(
                        &make_key(&format!("k{:05}", seen % N)),
                        make_row(b"o", 3000),
                    )
                    .unwrap();
                if seen % 50 == 0 {
                    writer.flush().unwrap();
                }
            },
        )
        .await;

        assert!(
            keys.windows(2).all(|pair| pair[0] < pair[1]),
            "keys must be strictly increasing: no duplicate, no reordering"
        );
        let seen: std::collections::BTreeSet<DecoratedKey> = keys.iter().cloned().collect();
        let missing: Vec<_> = original.difference(&seen).collect();
        assert!(
            missing.is_empty(),
            "partitions dropped by the pauses: {missing:?}"
        );
    }
}

/// What a streamed scan resumed after a retired input skips (see
/// `RangeScan::resume_after_retired_input`).
#[cfg(test)]
mod scan_resume_tests {
    use super::*;
    use ferrosa_common::PartitionKey;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    fn key(k: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(k.as_bytes().to_vec()))
    }

    fn row(c: u8) -> Row {
        Row {
            clustering: vec![c],
            cells: vec![],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        }
    }

    fn partition(k: &str, rows: &[u8]) -> Partition {
        Partition {
            key: key(k),
            deletion: DeletionTime::new(5, 5),
            static_row: Some(row(0)),
            rows: rows.iter().map(|c| row(*c)).collect(),
        }
    }

    #[test]
    fn a_partition_delivered_whole_is_not_delivered_again() {
        let done = Delivered::advance(None, &partition("a", &[1, 2]), true);
        assert!(done.remainder(Arc::new(partition("a", &[1, 2]))).is_none());
    }

    #[test]
    fn a_partition_stopped_mid_way_resumes_after_its_last_row_without_its_header() {
        let mid = Delivered::advance(None, &partition("a", &[1, 2, 3]), false);
        let rest = mid
            .remainder(Arc::new(partition("a", &[1, 2, 3, 4, 5])))
            .expect("rows 4 and 5 are still owed");
        let clustering: Vec<u8> = rest.rows.iter().map(|r| r.clustering[0]).collect();
        assert_eq!(clustering, vec![4, 5]);
        assert_eq!(
            rest.deletion,
            DeletionTime::LIVE,
            "the header went out with the first fragment"
        );
        assert!(rest.static_row.is_none());
        assert!(
            mid.remainder(Arc::new(partition("a", &[2, 3]))).is_none(),
            "a re-read fragment holding only delivered rows is dropped"
        );
    }

    #[test]
    fn a_later_partition_passes_through_unchanged() {
        let mid = Delivered::advance(None, &partition("a", &[1]), false);
        assert_eq!(
            mid.remainder(Arc::new(partition("b", &[1]))),
            Some(Arc::new(partition("b", &[1])))
        );
    }

    #[test]
    fn a_fragment_without_rows_keeps_the_last_row_already_delivered() {
        let first = Delivered::advance(None, &partition("a", &[7]), false);
        let after_empty = Delivered::advance(Some(first), &partition("a", &[]), false);
        assert_eq!(after_empty.last_clustering, Some(vec![7]));
        let next_key = Delivered::advance(Some(after_empty), &partition("b", &[]), false);
        assert_eq!(
            next_key.last_clustering, None,
            "a new key starts with nothing delivered"
        );
    }

    #[test]
    fn only_a_missing_file_is_treated_as_a_retired_input() {
        let not_found =
            ferrosa_common::Error::Io(std::io::Error::from(std::io::ErrorKind::NotFound));
        let denied =
            ferrosa_common::Error::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert!(is_missing_file(&not_found));
        assert!(!is_missing_file(&denied));
    }
}
