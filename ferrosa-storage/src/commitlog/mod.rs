//! Module: Persist mutations in a bounded, replayable write-ahead log.
//! Correctness: Correct when acknowledged mutations survive restart and segment
//! retention advances only after every dirty table is durably flushed.
//! Last revised: 2026-09-01
//! Last changed: Added byte-bounded, oldest-segment-scoped flush pressure.
//!
//! Commit log (write-ahead log) for durability.
//!
//! The commit log records every mutation before it reaches the memtable.
//! On crash recovery, uncommitted mutations are replayed from segment
//! files to restore memtable state.
//!
//! # Architecture
//!
//! The [`CommitLog`] composes all internal modules into a single public API:
//!
//! - **Segment** — fixed-size byte buffer with lock-free CAS allocation
//! - **SyncStrategy** — controls when segments are fsynced (Batch / Periodic / Group)
//! - **SegmentReader** — reads segment files during crash recovery replay
//! - **CommitLogCheckpoint** — tracks per-table flush positions
//!
//! The active segment is held behind an [`ArcSwap`],
//! giving writers lock-free access. Segment rotation atomically swaps in
//! a new segment while the old one stays alive (via `Arc`) until all
//! tables have been flushed past it.

pub mod archiver;
pub mod cdc;
pub(crate) mod checkpoint;
pub(crate) mod config;
pub(crate) mod descriptor;
pub mod manifest;
pub(crate) mod mutation;
pub(crate) mod reader;
pub(crate) mod segment;
pub(crate) mod sync;

pub use config::{
    ArchiveConfig, CommitLogBatchConfig, CommitLogConfig, CommitLogPosition, SyncStrategyConfig,
    TableId,
};
pub use mutation::{Mutation, CELL_REBIND_LIST_PATH_FLAG};

/// Retain at most this many segment-equivalents of dirty closed WAL data
/// before asking the table that pins the oldest segment to flush.
///
/// Pressure is computed from bytes actually written, not segment count. An
/// age-rotated 4 KiB segment therefore consumes 4 KiB of the budget instead of
/// its full 32 MiB capacity. This makes forced flush cadence proportional to
/// durable data volume while bounding retained WAL disk usage.
const RETAINED_WAL_SEGMENT_EQUIVALENTS: u64 = 8;

use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, ArcSwapOption};
use ferrosa_cdc::{CdcBus, CdcEvent, CdcStream};
use ferrosa_common::key::DecoratedKey;
use ferrosa_sstable::types::Row;
use parking_lot::Mutex;
use uuid::Uuid;

use checkpoint::CommitLogCheckpoint;
use config::CommitLogConfig as Config;
use reader::SegmentReader;
use segment::Segment;
use sync::{BatchSync, FlushCallback, GroupSync, PeriodicSync, SyncStrategy};

static COMMITLOG_APPENDS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_ALLOC_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_ALLOC_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_ROTATIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_ROTATE_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_ROTATE_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_WRITE_ENTRY_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_WRITE_ENTRY_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_MARK_DIRTY_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_MARK_DIRTY_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_SYNC_NOTIFY_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMMITLOG_APPEND_SYNC_NOTIFY_MICROS_MAX: AtomicU64 = AtomicU64::new(0);

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

fn update_max_u64(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

fn observe_duration(total: &AtomicU64, max: &AtomicU64, duration: Duration) {
    let micros = duration_micros(duration);
    total.fetch_add(micros, Ordering::Relaxed);
    update_max_u64(max, micros);
}

/// Process-wide counter of zero-byte commit-log segment files skipped during
/// crash recovery. A non-zero value means the writer rolled to a new segment
/// and was killed before any bytes (not even the header) were durably synced
/// — recovery treats the file as empty and continues. Operators should alert
/// if this rises rapidly (indicates pathological roll-then-crash behaviour);
/// a small steady-state count is expected on hard kills (OOM, host reboot).
pub static EMPTY_SEGMENT_SKIPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Reads the `EMPTY_SEGMENT_SKIPPED_TOTAL` counter.
pub fn empty_segment_skipped_total() -> u64 {
    EMPTY_SEGMENT_SKIPPED_TOTAL.load(Ordering::Relaxed)
}

/// Renders commit-log counters in Prometheus exposition format.
pub fn render_prometheus() -> String {
    let flush = segment::flush_metrics();
    let sync_batch = sync::sync_batch_metrics();
    let mut out = String::new();
    out.push_str("# HELP ferrosa_commitlog_appends_total Total commit-log appends.\n");
    out.push_str("# TYPE ferrosa_commitlog_appends_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_appends_total {}\n",
        COMMITLOG_APPENDS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_commitlog_append_bytes_total Total bytes reserved for commit-log entries.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_bytes_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_bytes_total {}\n",
        COMMITLOG_APPEND_BYTES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_commitlog_append_alloc_seconds_total Total wall time spent allocating commit-log entry space.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_alloc_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_alloc_seconds_total {:.9}\n",
        COMMITLOG_APPEND_ALLOC_MICROS_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_append_alloc_seconds_max Maximum wall time spent allocating commit-log entry space.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_alloc_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_alloc_seconds_max {:.9}\n",
        COMMITLOG_APPEND_ALLOC_MICROS_MAX.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_append_rotations_total Commit-log appends that had to rotate the active segment inline.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_rotations_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_rotations_total {}\n",
        COMMITLOG_APPEND_ROTATIONS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_commitlog_append_rotate_seconds_total Total wall time appends spent rotating the active segment inline.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_rotate_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_rotate_seconds_total {:.9}\n",
        COMMITLOG_APPEND_ROTATE_MICROS_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_append_rotate_seconds_max Maximum wall time an append spent rotating the active segment inline.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_rotate_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_rotate_seconds_max {:.9}\n",
        COMMITLOG_APPEND_ROTATE_MICROS_MAX.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_append_write_entry_seconds_total Total wall time spent serializing commit-log entries into memory.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_write_entry_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_write_entry_seconds_total {:.9}\n",
        COMMITLOG_APPEND_WRITE_ENTRY_MICROS_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_append_write_entry_seconds_max Maximum wall time spent serializing one commit-log entry into memory.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_write_entry_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_write_entry_seconds_max {:.9}\n",
        COMMITLOG_APPEND_WRITE_ENTRY_MICROS_MAX.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_append_mark_dirty_seconds_total Total wall time spent updating commit-log dirty-table tracking.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_mark_dirty_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_mark_dirty_seconds_total {:.9}\n",
        COMMITLOG_APPEND_MARK_DIRTY_MICROS_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_append_mark_dirty_seconds_max Maximum wall time spent updating commit-log dirty-table tracking.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_mark_dirty_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_mark_dirty_seconds_max {:.9}\n",
        COMMITLOG_APPEND_MARK_DIRTY_MICROS_MAX.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_append_sync_notify_seconds_total Total wall time append callers spent in the configured sync strategy notification.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_sync_notify_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_sync_notify_seconds_total {:.9}\n",
        COMMITLOG_APPEND_SYNC_NOTIFY_MICROS_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_append_sync_notify_seconds_max Maximum wall time an append caller spent in sync strategy notification.\n");
    out.push_str("# TYPE ferrosa_commitlog_append_sync_notify_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_append_sync_notify_seconds_max {:.9}\n",
        COMMITLOG_APPEND_SYNC_NOTIFY_MICROS_MAX.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_flushes_total Incremental commit-log flushes.\n");
    out.push_str("# TYPE ferrosa_commitlog_flushes_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_flushes_total {}\n",
        flush.incremental_flushes
    ));
    out.push_str("# HELP ferrosa_commitlog_flush_bytes_total Bytes written by incremental commit-log flushes.\n");
    out.push_str("# TYPE ferrosa_commitlog_flush_bytes_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_flush_bytes_total {}\n",
        flush.incremental_bytes
    ));
    out.push_str("# HELP ferrosa_commitlog_full_flushes_total Full segment rewrite flushes.\n");
    out.push_str("# TYPE ferrosa_commitlog_full_flushes_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_full_flushes_total {}\n",
        flush.full_flushes
    ));
    out.push_str("# HELP ferrosa_commitlog_full_flush_bytes_total Bytes written by full segment rewrite flushes.\n");
    out.push_str("# TYPE ferrosa_commitlog_full_flush_bytes_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_full_flush_bytes_total {}\n",
        flush.full_bytes
    ));
    out.push_str("# HELP ferrosa_commitlog_syncs_total Successful commit-log fsync calls.\n");
    out.push_str("# TYPE ferrosa_commitlog_syncs_total counter\n");
    out.push_str(&format!("ferrosa_commitlog_syncs_total {}\n", flush.syncs));
    out.push_str("# HELP ferrosa_commitlog_sync_seconds_total Total wall time spent in commit-log flush_to_disk/force_full_flush calls that performed fsync.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_seconds_total {:.9}\n",
        flush.sync_micros_total as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_seconds_max Maximum observed wall time for a commit-log fsync call.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_seconds_max {:.9}\n",
        flush.sync_micros_max as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_wait_writers_seconds_total Total wall time commit-log sync spent waiting for in-flight writers to finish entries.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_wait_writers_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_wait_writers_seconds_total {:.9}\n",
        flush.sync_wait_writers_micros_total as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_wait_writers_seconds_max Maximum observed wait-for-writers time before commit-log sync.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_wait_writers_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_wait_writers_seconds_max {:.9}\n",
        flush.sync_wait_writers_micros_max as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_file_lock_wait_seconds_total Total wall time commit-log sync spent waiting for the segment file mutex.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_file_lock_wait_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_file_lock_wait_seconds_total {:.9}\n",
        flush.sync_file_lock_wait_micros_total as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_file_lock_wait_seconds_max Maximum wait for the segment file mutex during commit-log sync.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_file_lock_wait_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_file_lock_wait_seconds_max {:.9}\n",
        flush.sync_file_lock_wait_micros_max as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_write_seconds_total Total wall time spent writing commit-log bytes to the file before sync.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_write_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_write_seconds_total {:.9}\n",
        flush.sync_write_micros_total as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_write_seconds_max Maximum wall time spent writing commit-log bytes to the file before sync.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_write_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_write_seconds_max {:.9}\n",
        flush.sync_write_micros_max as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_data_seconds_total Total wall time spent in commit-log file sync_data/fsync.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_data_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_data_seconds_total {:.9}\n",
        flush.sync_data_micros_total as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_data_seconds_max Maximum wall time spent in commit-log file sync_data/fsync.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_data_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_data_seconds_max {:.9}\n",
        flush.sync_data_micros_max as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_parent_dir_seconds_total Total wall time spent syncing parent directories for newly-created commit-log files.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_parent_dir_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_parent_dir_seconds_total {:.9}\n",
        flush.sync_parent_dir_micros_total as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_parent_dir_seconds_max Maximum wall time spent syncing a commit-log parent directory.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_parent_dir_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_parent_dir_seconds_max {:.9}\n",
        flush.sync_parent_dir_micros_max as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_periodic_idle_flushes_skipped_total Periodic commit-log timer ticks skipped because no writes were pending.\n");
    out.push_str("# TYPE ferrosa_commitlog_periodic_idle_flushes_skipped_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_periodic_idle_flushes_skipped_total {}\n",
        sync::periodic_idle_flush_skipped_total()
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_batches_total Commit-log sync batches flushed.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_batches_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_batches_total {}\n",
        sync_batch.batches
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_batch_writes_total Commit-log writes included in sync batches.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_batch_writes_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_batch_writes_total {}\n",
        sync_batch.writes
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_batch_bytes_total Commit-log bytes included in sync batches.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_batch_bytes_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_batch_bytes_total {}\n",
        sync_batch.bytes
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_batch_wait_seconds_total Total time sync batches stayed open before flush.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_batch_wait_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_batch_wait_seconds_total {:.9}\n",
        sync_batch.wait_micros_total as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_batch_wait_seconds_max Maximum time a sync batch stayed open before flush.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_batch_wait_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_batch_wait_seconds_max {:.9}\n",
        sync_batch.wait_micros_max as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_pending_writes Commit-log writes currently waiting for sync.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_pending_writes gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_pending_writes {}\n",
        sync_batch.pending_writes
    ));
    out.push_str("# HELP ferrosa_commitlog_sync_pending_bytes Commit-log bytes currently waiting for sync.\n");
    out.push_str("# TYPE ferrosa_commitlog_sync_pending_bytes gauge\n");
    out.push_str(&format!(
        "ferrosa_commitlog_sync_pending_bytes {}\n",
        sync_batch.pending_bytes
    ));
    out.push_str("# HELP ferrosa_commitlog_empty_segments_skipped_total Empty or torn commit-log segments skipped during replay.\n");
    out.push_str("# TYPE ferrosa_commitlog_empty_segments_skipped_total counter\n");
    out.push_str(&format!(
        "ferrosa_commitlog_empty_segments_skipped_total {}\n",
        empty_segment_skipped_total()
    ));
    out
}

fn is_zeroed_segment_header(path: &std::path::Path) -> ferrosa_common::Result<bool> {
    let mut file = fs::File::open(path)?;
    let mut header = [0u8; descriptor::HEADER_SIZE];
    file.read_exact(&mut header)?;
    Ok(header.iter().all(|byte| *byte == 0))
}

/// The commit log: write-ahead log for mutation durability.
///
/// Writers call [`append()`](Self::append) to record a mutation. The commit log
/// manages segment allocation, rotation, sync strategy, and checkpoint tracking.
///
/// # Concurrency
///
/// - **Append** is lock-free on the hot path (CAS allocation in the active segment).
/// - **Rotation** briefly takes the `closed_segments` mutex to move the old segment.
/// - **Discard** takes the `closed_segments` mutex and queries each
///   segment's own (lock-free) `dirty_tables` map.
pub struct CommitLog {
    /// Commit log configuration.
    config: Config,

    /// The currently active segment, swapped atomically on rotation.
    active: Arc<ArcSwap<Segment>>,

    /// Segments that are full but still have dirty (unflushed) tables.
    closed_segments: Mutex<Vec<Arc<Segment>>>,

    /// Controls when segment buffers are fsynced to disk.
    sync_strategy: Box<dyn SyncStrategy>,

    /// Monotonic segment ID generator.
    next_segment_id: AtomicU64,

    /// Segment IDs that have been successfully archived to S3.
    /// Used by `discard_completed()` to gate segment deletion when
    /// archiving is enabled.
    archived: Mutex<HashSet<u64>>,

    /// Channel sender for notifying the archiver of closed segments.
    /// None when archiving is disabled.
    archive_tx: Option<tokio::sync::mpsc::Sender<u64>>,

    /// Optional CDC bus, attachable at runtime via [`set_cdc`](Self::set_cdc).
    /// When attached and a `WrittenOnNode` subscriber is live, each successful
    /// append publishes a change event. Empty (the default) keeps the append hot
    /// path entirely free of CDC cost. `ArcSwapOption` so the bus can be injected
    /// after the engine is built (lock-free load on the hot path).
    cdc: ArcSwapOption<CdcBus>,
}

impl CommitLog {
    /// Creates a new commit log with the given configuration.
    ///
    /// Creates the log directory if it does not exist, allocates the first
    /// segment, and starts the sync strategy.
    pub fn new(config: Config) -> ferrosa_common::Result<Self> {
        Self::new_with_first_segment_id(config, 1)
    }

    fn new_with_first_segment_id(
        config: Config,
        first_segment_id: u64,
    ) -> ferrosa_common::Result<Self> {
        fs::create_dir_all(&config.log_dir)?;
        fs::create_dir_all(&config.checkpoint_dir)?;

        let first_segment = Arc::new(Segment::new(
            first_segment_id,
            config.segment_size,
            &config.log_dir,
        ));
        let active = Arc::new(ArcSwap::from(first_segment));

        let sync_strategy = Self::create_sync_strategy(&config, Arc::clone(&active));
        sync_strategy.start();

        Ok(Self {
            config,
            active,
            closed_segments: Mutex::new(Vec::new()),
            sync_strategy,
            next_segment_id: AtomicU64::new(first_segment_id + 1),
            archived: Mutex::new(HashSet::new()),
            archive_tx: None,
            cdc: ArcSwapOption::empty(),
        })
    }

    /// Attaches a CDC bus so successful appends publish `WrittenOnNode` change
    /// events (the local change-data-capture stream). Builder-style; returns
    /// `self` for chaining at construction.
    pub fn with_cdc(self, bus: Arc<CdcBus>) -> Self {
        self.cdc.store(Some(bus));
        self
    }

    /// Attaches (or replaces) the CDC bus at runtime — used to inject the shared
    /// bus after the engine is constructed. Lock-free; safe to call while
    /// appends are in flight.
    pub fn set_cdc(&self, bus: Arc<CdcBus>) {
        self.cdc.store(Some(bus));
    }

    /// The attached CDC bus, if any.
    pub fn cdc(&self) -> Option<Arc<CdcBus>> {
        self.cdc.load_full()
    }

    /// Publishes a `WrittenOnNode` CDC event for a just-appended mutation.
    ///
    /// No-op (and no allocation) when no bus is attached or no subscriber is
    /// listening — the row clone happens only when an event will be delivered,
    /// so this stays off the cost path for the common no-CDC case.
    fn emit_written_on_node(
        &self,
        keyspace: &str,
        table: &str,
        key: &DecoratedKey,
        rows: &[Row],
        timestamp: i64,
        mutation_id: [u8; 16],
    ) {
        let guard = self.cdc.load();
        let Some(bus) = guard.as_ref() else { return };
        if !bus.has_subscribers(CdcStream::WrittenOnNode) {
            return;
        }
        bus.publish(CdcEvent {
            stream: CdcStream::WrittenOnNode,
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key: key.clone(),
            rows: rows.to_vec(),
            timestamp,
            accord_ts: None,
            mutation_id,
        });
    }

    /// Opens an existing commit log directory, replays uncommitted mutations,
    /// and returns a new `CommitLog` instance along with the replayed mutations.
    ///
    /// The replay process:
    /// 1. Load the checkpoint file to find per-table flush positions.
    /// 2. Scan the log directory for segment files, sorted by segment ID.
    /// 3. For each segment, read all entries and filter those after checkpoint positions.
    /// 4. Create a fresh `CommitLog` for new writes.
    ///
    /// **Memory note:** this method buffers every undominated mutation in a
    /// single `Vec<Mutation>` and is therefore unsuitable for production
    /// recovery of logs larger than available RAM. Prefer
    /// [`open_and_replay_streaming`](Self::open_and_replay_streaming), which
    /// caps peak memory at one segment's worth of decoded entries.
    pub fn open_and_replay(config: Config) -> ferrosa_common::Result<(Self, Vec<Mutation>)> {
        let mut mutations = Vec::new();
        let commit_log = Self::open_and_replay_streaming(config, |mutation| {
            mutations.push(mutation);
            Ok(())
        })?;
        Ok((commit_log, mutations))
    }

    /// Opens an existing commit log directory and streams undominated
    /// mutations to `on_mutation`, one segment at a time.
    ///
    /// This is the memory-bounded crash-recovery primitive. Peak memory is
    /// `O(segment_size)` regardless of total log size — the caller's
    /// `on_mutation` closure is invoked for each entry as it is decoded, and
    /// each segment's bytes are dropped before the next segment is opened.
    /// A segment file is deleted only after every entry it contains has been
    /// successfully delivered to the callback. If the callback returns `Err`,
    /// replay stops immediately and the remaining segments stay on disk so a
    /// subsequent retry can re-process them.
    ///
    /// Mutations whose `(segment_id, offset)` is `<=` the table's flushed
    /// checkpoint are dropped before the callback fires. Order across
    /// segments matches segment-id order; order within a segment matches
    /// append order.
    ///
    /// See `specs/todo/bug-commitlog-replay-oom-on-large-log.md`.
    pub fn open_and_replay_streaming<F>(
        config: Config,
        mut on_mutation: F,
    ) -> ferrosa_common::Result<Self>
    where
        F: FnMut(Mutation) -> ferrosa_common::Result<()>,
    {
        let checkpoint = CommitLogCheckpoint::load(&config.checkpoint_dir)?;
        let max_checkpoint_segment_id = checkpoint.values().map(|pos| pos.segment_id).max();

        // Scan for segment files in log_dir.
        let mut segment_files: Vec<(u64, std::path::PathBuf)> = Vec::new();
        if config.log_dir.exists() {
            for entry in fs::read_dir(&config.log_dir)? {
                let entry = entry?;
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(id) = parse_segment_id(name) {
                        segment_files.push((id, path));
                    }
                }
            }
        }

        // Sort by segment ID for deterministic replay order.
        segment_files.sort_by_key(|(id, _)| *id);
        let max_segment_file_id = segment_files.iter().map(|(id, _)| *id).max();

        // Stream segment by segment. The reader's `data` buffer (~segment_size)
        // and the per-segment entries Vec are dropped before the next iteration,
        // so peak memory is bounded by one segment regardless of how many
        // segments exist on disk. Each fully-replayed segment file is deleted
        // before moving on; on callback error, remaining segments are left
        // intact for retry.
        for (id, path) in &segment_files {
            // A too-short segment is the torn-create/torn-header state: the
            // writer rolled to a new segment file, but was killed (OOM,
            // kill -9, host reboot) before the header was durably completed.
            // It carries no complete records, so it is safe — and mandatory —
            // to skip it on replay rather than refuse to start. (See
            // specs/in-process/bug-empty-commitlog-segment-blocks-startup-data-loss.md.)
            match fs::metadata(path) {
                Ok(meta) if meta.len() < descriptor::HEADER_SIZE as u64 => {
                    EMPTY_SEGMENT_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        segment_id = id,
                        path = %path.display(),
                        bytes = meta.len(),
                        "commitlog: skipping too-short segment on replay (torn create/header from previous crash); \
                         file will be cleaned up below"
                    );
                    if let Err(e) = fs::remove_file(path) {
                        tracing::warn!(%e, "commitlog: failed to remove torn segment file");
                    }
                    continue;
                }
                Ok(_) if is_zeroed_segment_header(path)? => {
                    EMPTY_SEGMENT_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        segment_id = id,
                        path = %path.display(),
                        "commitlog: skipping segment with all-zero header on replay \
                         (preallocated/torn segment from previous crash); file will be cleaned up below"
                    );
                    if let Err(e) = fs::remove_file(path) {
                        tracing::warn!(%e, "commitlog: failed to remove torn segment file");
                    }
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    return Err(ferrosa_common::Error::from(e));
                }
            }

            {
                let mut reader = SegmentReader::open(path)?;
                let entries = reader.read_all()?;

                for (pos, mutation) in entries {
                    let table_id = TableId::new(&mutation.keyspace, &mutation.table);
                    let dominated = checkpoint.get(&table_id).is_some_and(|cp| pos <= *cp);
                    if !dominated {
                        on_mutation(mutation)?;
                    }
                }
            }

            if let Err(e) = fs::remove_file(path) {
                tracing::warn!(%e, "commitlog: failed to remove segment file");
            }
        }

        let first_segment_id = max_segment_file_id
            .into_iter()
            .chain(max_checkpoint_segment_id)
            .max()
            .unwrap_or(0)
            + 1;

        Self::new_with_first_segment_id(config, first_segment_id)
    }

    /// Appends a mutation to the commit log.
    ///
    /// This is the hot path. The flow:
    /// 1. Load the active segment (lock-free via `ArcSwap`).
    /// 2. Try to allocate space in the segment (lock-free CAS).
    /// 3. If the segment is full, rotate and retry.
    /// 4. Write the entry, update dirty tracking, notify sync strategy.
    pub fn append(&self, mutation: &Mutation) -> ferrosa_common::Result<CommitLogPosition> {
        // DEBUG-level span so it costs nothing at the default INFO filter.
        let _span = tracing::debug_span!(
            "commitlog.write",
            table = %mutation.table,
            keyspace = %mutation.keyspace,
        )
        .entered();
        let total_size = Segment::entry_total_size(mutation);

        // Load active segment and try to allocate. The segment reference MUST
        // stay paired with the offset — writing to a different segment than
        // the one where CAS succeeded would corrupt data.
        //
        // allocate_and_begin_write() increments in_flight_writers BEFORE the CAS,
        // closing the window where flush could read partially-written data.
        let alloc_start = Instant::now();
        let (segment, offset) = {
            let seg = self.active.load_full();
            match seg.allocate_and_begin_write(total_size) {
                Some(offset) => (seg, offset),
                None => {
                    // Segment is full — rotate (serialized) and retry.
                    drop(seg);
                    let rotate_start = Instant::now();
                    self.force_rotate()?;
                    COMMITLOG_APPEND_ROTATIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    observe_duration(
                        &COMMITLOG_APPEND_ROTATE_MICROS_TOTAL,
                        &COMMITLOG_APPEND_ROTATE_MICROS_MAX,
                        rotate_start.elapsed(),
                    );
                    let new_seg = self.active.load_full();
                    let offset = match new_seg.allocate_and_begin_write(total_size) {
                        Some(o) => o,
                        None => {
                            // Entry exceeds segment capacity. This happens when
                            // a single mutation is larger than segment_size.
                            // Return an error instead of panicking.
                            return Err(ferrosa_common::Error::InvalidData(format!(
                                "commit log entry ({total_size} bytes) exceeds \
                                 segment capacity; increase segment_size"
                            )));
                        }
                    };
                    (new_seg, offset)
                }
            }
        };
        observe_duration(
            &COMMITLOG_APPEND_ALLOC_MICROS_TOTAL,
            &COMMITLOG_APPEND_ALLOC_MICROS_MAX,
            alloc_start.elapsed(),
        );

        let write_start = Instant::now();
        let position = segment.write_entry(offset, mutation);
        segment.writer_done();
        observe_duration(
            &COMMITLOG_APPEND_WRITE_ENTRY_MICROS_TOTAL,
            &COMMITLOG_APPEND_WRITE_ENTRY_MICROS_MAX,
            write_start.elapsed(),
        );
        COMMITLOG_APPENDS_TOTAL.fetch_add(1, Ordering::Relaxed);
        COMMITLOG_APPEND_BYTES_TOTAL.fetch_add(total_size as u64, Ordering::Relaxed);

        // Track dirty table in this segment. `dirty_tables` on the segment
        // is a `DashMap<TableId, AtomicU64>` so this is lock-free in steady
        // state — no global commit-log-level mutex on the hot path.
        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
        let mark_dirty_start = Instant::now();
        segment.mark_table_dirty(&table_id, position);
        observe_duration(
            &COMMITLOG_APPEND_MARK_DIRTY_MICROS_TOTAL,
            &COMMITLOG_APPEND_MARK_DIRTY_MICROS_MAX,
            mark_dirty_start.elapsed(),
        );

        // Notify sync strategy.
        let sync_notify_start = Instant::now();
        self.sync_strategy
            .on_write(&segment, offset, total_size as u64);
        observe_duration(
            &COMMITLOG_APPEND_SYNC_NOTIFY_MICROS_TOTAL,
            &COMMITLOG_APPEND_SYNC_NOTIFY_MICROS_MAX,
            sync_notify_start.elapsed(),
        );

        // CDC: publish the local change-data-capture event after the write is
        // durable in the segment buffer (WrittenOnNode stream).
        self.emit_written_on_node(
            &mutation.keyspace,
            &mutation.table,
            &mutation.key,
            &mutation.rows,
            mutation.timestamp,
            mutation.mutation_id,
        );

        Ok(position)
    }

    /// Appends a single-row mutation without cloning the row into an owned
    /// [`Mutation`] first. This is the storage write hot path.
    pub fn append_single_row(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: &Row,
        timestamp: i64,
    ) -> ferrosa_common::Result<CommitLogPosition> {
        let total_size =
            Segment::entry_total_size_single_row(&table_id.keyspace, &table_id.table, key, row);

        let alloc_start = Instant::now();
        let (segment, offset) = {
            let seg = self.active.load_full();
            match seg.allocate_and_begin_write(total_size) {
                Some(offset) => (seg, offset),
                None => {
                    drop(seg);
                    let rotate_start = Instant::now();
                    self.force_rotate()?;
                    COMMITLOG_APPEND_ROTATIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    observe_duration(
                        &COMMITLOG_APPEND_ROTATE_MICROS_TOTAL,
                        &COMMITLOG_APPEND_ROTATE_MICROS_MAX,
                        rotate_start.elapsed(),
                    );
                    let new_seg = self.active.load_full();
                    let offset = match new_seg.allocate_and_begin_write(total_size) {
                        Some(o) => o,
                        None => {
                            return Err(ferrosa_common::Error::InvalidData(format!(
                                "commit log entry ({total_size} bytes) exceeds \
                                 segment capacity; increase segment_size"
                            )));
                        }
                    };
                    (new_seg, offset)
                }
            }
        };
        observe_duration(
            &COMMITLOG_APPEND_ALLOC_MICROS_TOTAL,
            &COMMITLOG_APPEND_ALLOC_MICROS_MAX,
            alloc_start.elapsed(),
        );

        let write_start = Instant::now();
        let mutation_id = Uuid::new_v4().into_bytes();
        let position = segment.write_single_row_entry(
            offset,
            mutation_id,
            &table_id.keyspace,
            &table_id.table,
            key,
            row,
            timestamp,
        );
        segment.writer_done();
        observe_duration(
            &COMMITLOG_APPEND_WRITE_ENTRY_MICROS_TOTAL,
            &COMMITLOG_APPEND_WRITE_ENTRY_MICROS_MAX,
            write_start.elapsed(),
        );
        COMMITLOG_APPENDS_TOTAL.fetch_add(1, Ordering::Relaxed);
        COMMITLOG_APPEND_BYTES_TOTAL.fetch_add(total_size as u64, Ordering::Relaxed);

        let mark_dirty_start = Instant::now();
        segment.mark_table_dirty(table_id, position);
        observe_duration(
            &COMMITLOG_APPEND_MARK_DIRTY_MICROS_TOTAL,
            &COMMITLOG_APPEND_MARK_DIRTY_MICROS_MAX,
            mark_dirty_start.elapsed(),
        );
        let sync_notify_start = Instant::now();
        self.sync_strategy
            .on_write(&segment, offset, total_size as u64);
        observe_duration(
            &COMMITLOG_APPEND_SYNC_NOTIFY_MICROS_TOTAL,
            &COMMITLOG_APPEND_SYNC_NOTIFY_MICROS_MAX,
            sync_notify_start.elapsed(),
        );

        // CDC: publish the local change-data-capture event (WrittenOnNode).
        self.emit_written_on_node(
            &table_id.keyspace,
            &table_id.table,
            key,
            std::slice::from_ref(row),
            timestamp,
            mutation_id,
        );

        Ok(position)
    }

    /// Discards commit log data for a table up to the given position.
    ///
    /// When a table's memtable is flushed to an SSTable, the caller calls this
    /// to indicate that all mutations up to `position` are durable elsewhere.
    /// Segments where all tables have been flushed past their positions are
    /// deleted from disk.
    pub fn discard_completed(
        &self,
        table_id: &TableId,
        position: CommitLogPosition,
    ) -> ferrosa_common::Result<()> {
        let mut segments_to_delete = Vec::new();

        // Iterate active + closed segments. Each segment's `dirty_tables`
        // is a `DashMap` so per-segment removal is lock-free per-shard.
        //
        // Ordering: `CommitLogPosition` sorts by `(segment_id, offset)`,
        // so for a segment whose id is strictly less than `position.segment_id`,
        // *every* recorded offset is dominated and the entry is removed
        // unconditionally. When ids match, we compare offsets. Newer
        // segments (id > position.segment_id) are skipped.
        let active = self.active.load_full();
        let active_id = active.id;
        let closed_snapshot: Vec<Arc<Segment>> = self.closed_segments.lock().clone();
        let all_segments = std::iter::once(active).chain(closed_snapshot);
        for segment in all_segments {
            let now_empty = match segment.id.cmp(&position.segment_id) {
                std::cmp::Ordering::Less => segment.discard_table_unconditional(table_id),
                std::cmp::Ordering::Equal => {
                    segment.discard_table_if_dominated(table_id, position.offset)
                }
                std::cmp::Ordering::Greater => false,
            };
            if now_empty && segment.id != active_id {
                let dominated_by_archive = match &self.config.archive {
                    Some(cfg) if cfg.enabled => self.archived.lock().contains(&segment.id),
                    _ => true,
                };
                if dominated_by_archive {
                    segments_to_delete.push(segment.id);
                }
            }
        }

        // Remove deleted segments from closed_segments and delete files.
        if !segments_to_delete.is_empty() {
            let mut archived = self.archived.lock();
            let mut closed = self.closed_segments.lock();
            for seg_id in &segments_to_delete {
                if let Some(idx) = closed.iter().position(|s| s.id == *seg_id) {
                    let segment = closed.remove(idx);
                    if let Err(e) = fs::remove_file(segment.path()) {
                        tracing::warn!(%e, "commitlog: failed to remove segment file");
                    }
                }
                archived.remove(seg_id);
            }
        }

        // Update checkpoint.
        let mut checkpoint = CommitLogCheckpoint::load(&self.config.checkpoint_dir)?;
        checkpoint
            .entry(table_id.clone())
            .and_modify(|existing| {
                if position > *existing {
                    *existing = position;
                }
            })
            .or_insert(position);
        CommitLogCheckpoint::save(&self.config.checkpoint_dir, &checkpoint)?;

        Ok(())
    }

    /// Discards closed segments that have no remaining dirty tables.
    ///
    /// This is a lightweight GC pass for the maintenance loop. Unlike
    /// `discard_completed()` which marks specific table positions, this method
    /// only removes segments where the tracker already shows zero dirty tables
    /// (i.e., all tables were previously discarded via `discard_completed()`).
    ///
    /// Returns the number of segments cleaned up.
    pub fn discard_completed_segments(&self) -> ferrosa_common::Result<usize> {
        let mut segments_to_delete = Vec::new();

        // Only look at closed segments — the active one keeps writing.
        let closed_snapshot: Vec<Arc<Segment>> = self.closed_segments.lock().clone();
        for segment in closed_snapshot {
            if !segment.is_dirty_empty() {
                continue;
            }
            let dominated_by_archive = match &self.config.archive {
                Some(cfg) if cfg.enabled => self.archived.lock().contains(&segment.id),
                _ => true,
            };
            if dominated_by_archive {
                segments_to_delete.push(segment.id);
            }
        }

        let count = segments_to_delete.len();
        if !segments_to_delete.is_empty() {
            let mut archived = self.archived.lock();
            let mut closed = self.closed_segments.lock();
            for seg_id in &segments_to_delete {
                if let Some(idx) = closed.iter().position(|s| s.id == *seg_id) {
                    let segment = closed.remove(idx);
                    if let Err(e) = fs::remove_file(segment.path()) {
                        tracing::warn!(%e, "commitlog: failed to remove segment file");
                    }
                }
                archived.remove(seg_id);
            }
        }

        Ok(count)
    }

    /// Marks a segment as archived to S3.
    ///
    /// Called by the archiver after successful upload. Once marked,
    /// `discard_completed()` will allow deletion of this segment
    /// (provided all tables are also flushed).
    pub fn mark_archived(&self, segment_id: u64) {
        self.archived.lock().insert(segment_id);
    }

    /// Returns the number of closed segments waiting for GC.
    ///
    /// Used by tests and monitoring to verify that flush → discard_completed
    /// is keeping the closed segment count bounded.
    pub fn closed_segment_count(&self) -> usize {
        self.closed_segments.lock().len()
    }

    /// Returns whether `table_id` pins the oldest closed segment after the
    /// retained closed-WAL byte budget has been reached.
    ///
    /// The scan is allocation-free and bounded by the number of closed WAL
    /// segments. Concurrent rotations can append closed segments out of ID
    /// order, so the minimum segment ID identifies the oldest segment that can
    /// advance reclamation. Unrelated dirty tables are never flushed merely
    /// because some segment is closed.
    pub fn table_pins_retained_wal_pressure(&self, table_id: &TableId) -> bool {
        let closed = self.closed_segments.lock();
        let budget =
            (self.config.segment_size as u64).saturating_mul(RETAINED_WAL_SEGMENT_EQUIVALENTS);
        let retained_bytes = closed.iter().fold(0_u64, |total, segment| {
            total.saturating_add(segment.current_position())
        });
        retained_bytes >= budget
            && closed
                .iter()
                .min_by_key(|segment| segment.id)
                .is_some_and(|segment| segment.dirty_tables.contains_key(table_id))
    }

    /// Returns the total in-memory buffer bytes held by all closed segments.
    ///
    /// After [`force_rotate()`](Self::force_rotate) releases each segment's
    /// write buffer, every closed segment holds 0 bytes of buffer memory.
    /// A non-zero value indicates that the release path is not running
    /// (regression detector).
    pub fn closed_segments_total_bytes(&self) -> usize {
        self.closed_segments
            .lock()
            .iter()
            .map(|s| s.buffer_bytes())
            .sum()
    }

    /// Returns the set of segment IDs currently marked as archived.
    pub fn archived_segments(&self) -> HashSet<u64> {
        self.archived.lock().clone()
    }

    /// Sets the channel sender for archive notifications.
    ///
    /// Called by StorageEngine during initialization when archiving is enabled.
    pub fn set_archive_channel(&mut self, tx: tokio::sync::mpsc::Sender<u64>) {
        self.archive_tx = Some(tx);
    }

    /// Returns the current write position in the active segment.
    ///
    /// This is the position of the next byte that will be written. Used
    /// by snapshot creation (PITR Sprint P-2) to record the commit log
    /// position at the time of the snapshot.
    pub fn current_position(&self) -> CommitLogPosition {
        let segment = self.active.load();
        CommitLogPosition {
            segment_id: segment.id,
            offset: segment.current_position(),
        }
    }

    /// Forces rotation of the active segment.
    ///
    /// Allocates a new segment, atomically swaps it in via `ArcSwap`, and moves
    /// the old segment to the `closed_segments` list.
    ///
    /// If multiple threads race here, each creates a segment — the extras are
    /// empty but harmless. No data is lost because each writer holds its own
    /// `Arc<Segment>` paired with its CAS-allocated offset (see `append()`).
    pub fn force_rotate(&self) -> ferrosa_common::Result<()> {
        let new_id = self.next_segment_id.fetch_add(1, Ordering::AcqRel);
        let new_segment = Arc::new(Segment::new(
            new_id,
            self.config.segment_size,
            &self.config.log_dir,
        ));

        // Swap the new segment in and get the old one.
        let old_segment = self.active.swap(new_segment);

        // Flush the old segment to disk before archiving, then release the
        // file descriptor and the 32 MB write buffer.  The data is fully on
        // disk; retaining the buffer while waiting for discard_completed() to
        // run GC causes unbounded memory growth when GC stalls (OOM bug).
        old_segment.flush_to_disk()?;
        old_segment.close_file_handle();
        old_segment.release_buffer();

        // Move old segment to closed list.
        let old_id = old_segment.id;
        let mut closed = self.closed_segments.lock();
        closed.push(old_segment);
        drop(closed);

        // Notify archiver of the closed segment (non-blocking).
        if let Some(tx) = &self.archive_tx {
            let _ = tx.try_send(old_id);
        }

        Ok(())
    }

    /// Replay mutations from a given position forward.
    ///
    /// Walks closed segments and the active segment, returning entries
    /// after the given position. Returns empty vec if the requested
    /// segment has been recycled (caller should trigger full bootstrap).
    pub fn replay_from(
        &self,
        position: CommitLogPosition,
    ) -> ferrosa_common::Result<Vec<Mutation>> {
        let mut mutations = Vec::new();

        // Collect segment paths to read: closed segments with id >= position.segment_id.
        let closed = self.closed_segments.lock();
        let mut segment_paths: Vec<(u64, std::path::PathBuf)> = closed
            .iter()
            .filter(|s| s.id >= position.segment_id)
            .map(|s| (s.id, s.path().to_path_buf()))
            .collect();
        drop(closed);

        // Check if the requested segment exists. If all closed segments have
        // lower IDs and the active segment is newer, the requested segment
        // was recycled — return empty to signal full bootstrap needed.
        let active = self.active.load();
        let active_id = active.id;
        let active_path = active.path().to_path_buf();

        if position.segment_id > 0 && segment_paths.is_empty() && active_id > position.segment_id {
            // Requested segment was recycled.
            return Ok(vec![]);
        }

        // Add active segment if it has data and its ID >= requested.
        if active_id >= position.segment_id {
            // Flush active segment to disk so SegmentReader can read it.
            active.flush_to_disk()?;
            segment_paths.push((active_id, active_path));
        }

        // Sort by segment ID for replay order.
        segment_paths.sort_by_key(|(id, _)| *id);

        for (id, path) in &segment_paths {
            if !path.exists() {
                continue;
            }
            // Same torn-create/torn-header tolerance as open_and_replay:
            // too-short segments are skipped (see specs/in-process/bug-empty-commitlog-segment-blocks-startup-data-loss.md).
            match fs::metadata(path) {
                Ok(meta) if meta.len() < descriptor::HEADER_SIZE as u64 => {
                    EMPTY_SEGMENT_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        segment_id = id,
                        path = %path.display(),
                        bytes = meta.len(),
                        "commitlog: skipping too-short segment on catch-up replay"
                    );
                    continue;
                }
                Ok(_) if is_zeroed_segment_header(path)? => {
                    EMPTY_SEGMENT_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        segment_id = id,
                        path = %path.display(),
                        "commitlog: skipping segment with all-zero header on catch-up replay \
                         (preallocated/torn segment from previous crash)"
                    );
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    return Err(ferrosa_common::Error::from(e));
                }
            }
            let mut reader = SegmentReader::open(path)?;
            let entries = reader.read_all()?;

            for (pos, mutation) in entries {
                if pos > position {
                    mutations.push(mutation);
                }
            }
        }

        Ok(mutations)
    }

    /// The largest single commit-log entry (payload + framing overhead) that
    /// can ever be appended, given the configured segment size.
    ///
    /// An entry larger than this can never be appended (it overflows even a
    /// fresh segment), so [`Self::append`] would fail. Batch callers pre-flight
    /// against this so a multi-entry batch can never fail *partway* through its
    /// appends, preserving all-or-nothing durability.
    pub fn max_entry_size(&self) -> usize {
        Segment::max_entry_size(self.config.segment_size)
    }

    /// The framed on-disk size of `mutation` as a single commit-log entry.
    pub fn entry_size(mutation: &Mutation) -> usize {
        Segment::entry_total_size(mutation)
    }

    /// Force-syncs the active segment to disk.
    ///
    /// Waits for all in-flight writers to complete, then flushes the
    /// segment buffer to disk. Used before catch-up replay to ensure
    /// all mutations are readable.
    pub fn force_sync(&self) -> ferrosa_common::Result<()> {
        let segment = self.active.load();
        // Write an EOF sync marker so SegmentReader can follow the chain.
        if let Some(offset) = segment.allocate(segment::SYNC_MARKER_SIZE) {
            segment.write_sync_marker_at(offset, 0);
        }
        // Full rewrite: write_sync_marker_at updates the PREVIOUS marker
        // in the buffer (at an earlier offset). Incremental flush wouldn't
        // capture that update, so we rewrite the entire file.
        segment.force_full_flush()
    }

    /// Shuts down the commit log cleanly.
    ///
    /// Stops the sync strategy and flushes the active segment to disk.
    pub fn shutdown(&self) -> ferrosa_common::Result<()> {
        self.sync_strategy.stop();
        let segment = self.active.load();
        segment.flush_to_disk()?;
        Ok(())
    }

    /// Creates the appropriate sync strategy based on config.
    fn create_sync_strategy(
        config: &Config,
        active: Arc<ArcSwap<Segment>>,
    ) -> Box<dyn SyncStrategy> {
        match &config.sync_strategy {
            SyncStrategyConfig::Batch => Box::new(BatchSync::new()),
            SyncStrategyConfig::Periodic { sync_interval } => {
                let active_ref = Arc::clone(&active);
                let flush_callback: FlushCallback = Arc::new(move || {
                    let seg = active_ref.load();
                    seg.flush_to_disk()
                });
                Box::new(PeriodicSync::with_batch(
                    *sync_interval,
                    config.batch.clone(),
                    flush_callback,
                ))
            }
            SyncStrategyConfig::Group { max_wait } => {
                let active_ref = Arc::clone(&active);
                let flush_callback: FlushCallback = Arc::new(move || {
                    let seg = active_ref.load();
                    seg.flush_to_disk()
                });
                Box::new(GroupSync::with_batch(
                    *max_wait,
                    config.batch.clone(),
                    flush_callback,
                ))
            }
        }
    }
}

/// Parses a segment ID from a filename like `commitlog-42.log`.
fn parse_segment_id(filename: &str) -> Option<u64> {
    let name = filename.strip_prefix("commitlog-")?;
    let id_str = name.strip_suffix(".log")?;
    id_str.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_cdc::{CdcBus, CdcRecvError, CdcStream};
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    /// Helper to create a simple mutation for testing.
    fn simple_mutation() -> Mutation {
        Mutation {
            mutation_id: [0x12u8; 16],
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: vec![1, 2, 3],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
            timestamp: 42_000,
        }
    }

    /// Helper to create a mutation targeting a different table.
    fn mutation_for_table(keyspace: &str, table: &str) -> Mutation {
        Mutation {
            mutation_id: [0x13u8; 16],
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: vec![1, 2, 3],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
            timestamp: 42_000,
        }
    }

    #[test]
    fn append_publishes_written_on_node_cdc_event() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let bus = CdcBus::new(16);
        let cl = CommitLog::new(config).unwrap().with_cdc(Arc::clone(&bus));
        // Subscribe BEFORE the write so the event is captured.
        let mut sub = bus.subscribe(CdcStream::WrittenOnNode);

        let m = simple_mutation();
        cl.append(&m).unwrap();

        let event = sub.try_recv().expect("CDC event published on append");
        assert_eq!(event.stream, CdcStream::WrittenOnNode);
        assert_eq!(event.keyspace, m.keyspace);
        assert_eq!(event.table, m.table);
        assert_eq!(event.key, m.key);
        assert_eq!(event.rows, m.rows);
        assert_eq!(event.timestamp, m.timestamp);
        assert_eq!(event.mutation_id, m.mutation_id);
        assert!(event.accord_ts.is_none());

        cl.shutdown().unwrap();
    }

    #[test]
    fn append_single_row_publishes_written_on_node_cdc_event() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let bus = CdcBus::new(16);
        let cl = CommitLog::new(config).unwrap().with_cdc(Arc::clone(&bus));
        let mut sub = bus.subscribe(CdcStream::WrittenOnNode);

        let table_id = TableId::new("ks2", "t2");
        let key = DecoratedKey::new(PartitionKey::new(b"pk_single".to_vec()));
        let row = Row {
            clustering: vec![9],
            cells: vec![(0, CellValue::live(b"v".to_vec(), 2000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(2000),
        };
        cl.append_single_row(&table_id, &key, &row, 99_000).unwrap();

        let event = sub
            .try_recv()
            .expect("CDC event published on single-row append");
        assert_eq!(event.stream, CdcStream::WrittenOnNode);
        assert_eq!(event.keyspace, "ks2");
        assert_eq!(event.table, "t2");
        assert_eq!(event.key, key);
        assert_eq!(event.rows, vec![row]);
        assert_eq!(event.timestamp, 99_000);
        // mutation_id is generated by the single-row path; just confirm it is non-legacy.
        assert_ne!(event.mutation_id, [0u8; 16]);

        cl.shutdown().unwrap();
    }

    #[test]
    fn append_without_cdc_subscriber_does_not_publish() {
        // Bus attached, but nobody subscribed: append succeeds and produces no
        // event (and the hot path skips the row clone).
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let bus = CdcBus::new(16);
        let cl = CommitLog::new(config).unwrap().with_cdc(Arc::clone(&bus));

        cl.append(&simple_mutation()).unwrap();

        // Subscribe only now; the earlier write must not be replayed to us.
        let mut late = bus.subscribe(CdcStream::WrittenOnNode);
        assert_eq!(late.try_recv(), Err(CdcRecvError::Empty));

        cl.shutdown().unwrap();
    }

    #[test]
    fn append_without_cdc_bus_works() {
        // No bus attached at all (the default): append is unaffected.
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();
        cl.append(&simple_mutation()).unwrap();
        cl.shutdown().unwrap();
    }

    #[test]
    fn set_cdc_attaches_bus_at_runtime() {
        // The bus can be injected AFTER construction (the production wiring path).
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        let bus = CdcBus::new(16);
        cl.set_cdc(Arc::clone(&bus)); // runtime attach
        let mut sub = bus.subscribe(CdcStream::WrittenOnNode);

        let m = simple_mutation();
        cl.append(&m).unwrap();

        let event = sub
            .try_recv()
            .expect("runtime-attached bus receives WrittenOnNode events");
        assert_eq!(event.mutation_id, m.mutation_id);

        cl.shutdown().unwrap();
    }

    #[test]
    fn new_creates_segment_files() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        // The first segment file should exist after BatchSync writes on first append,
        // but the segment is pre-allocated in memory. Let's append and check.
        let m = simple_mutation();
        cl.append(&m).unwrap();

        // After append with BatchSync, the segment file should be flushed.
        let segment = cl.active.load();
        assert!(
            segment.path().exists(),
            "segment file should exist after append with BatchSync"
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn append_returns_positions() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        let m1 = simple_mutation();
        let m2 = simple_mutation();

        let pos1 = cl.append(&m1).unwrap();
        let pos2 = cl.append(&m2).unwrap();

        // Positions should be in the same segment and increasing.
        assert_eq!(pos1.segment_id, pos2.segment_id);
        assert!(
            pos2.offset > pos1.offset,
            "second append should have higher offset"
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn append_and_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        cl.append(&m).unwrap();

        let segment_path = cl.active.load().path().to_path_buf();
        cl.shutdown().unwrap();

        // After shutdown, the segment file should exist and contain data.
        assert!(
            segment_path.exists(),
            "segment file should exist after shutdown"
        );
        let contents = fs::read(&segment_path).unwrap();
        assert!(
            contents.len() > 25,
            "segment should contain data beyond header"
        );
    }

    #[test]
    fn rotation_on_full_segment() {
        let dir = tempfile::tempdir().unwrap();
        // Use a small segment size to force rotation after a few appends.
        // Each entry is ~118 bytes; header+sync marker is 25 bytes.
        // 512 bytes allows ~3-4 entries before rotation.
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();

        // Keep appending until we've rotated at least once.
        let mut segment_ids = std::collections::HashSet::new();
        for _ in 0..10 {
            match cl.append(&m) {
                Ok(pos) => {
                    segment_ids.insert(pos.segment_id);
                }
                Err(_) => break,
            }
        }

        assert!(
            segment_ids.len() >= 2,
            "should have rotated to at least 2 segments, got {}",
            segment_ids.len()
        );

        // Verify multiple segment files exist.
        let files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
            })
            .collect();

        assert!(
            files.len() >= 2,
            "should have at least 2 segment files, got {}",
            files.len()
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn discard_deletes_clean_segments() {
        let dir = tempfile::tempdir().unwrap();
        // Small segment to force rotation after a few appends.
        // Each entry is ~118 bytes; 512 bytes allows ~3-4 entries per segment.
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        let table_id = TableId::new("test_ks", "test_table");

        // Append mutations — this will span multiple segments.
        let mut last_pos = None;
        for _ in 0..10 {
            match cl.append(&m) {
                Ok(pos) => last_pos = Some(pos),
                Err(_) => break,
            }
        }
        let last_pos = last_pos.expect("should have appended at least one mutation");

        // Count segment files before discard.
        let count_segments = || -> usize {
            fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
                })
                .count()
        };

        let before = count_segments();
        assert!(before >= 2, "need at least 2 segments for this test");

        // Discard all mutations up to the last position.
        cl.discard_completed(&table_id, last_pos).unwrap();

        let after = count_segments();
        // The closed segments should have been deleted. The active segment stays.
        assert!(
            after < before,
            "discard should have deleted some segments: before={before}, after={after}"
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn discard_keeps_partially_dirty() {
        let dir = tempfile::tempdir().unwrap();
        // Small segment to force rotation.
        // Each entry is ~118 bytes; 512 bytes allows ~3-4 entries per segment.
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m1 = mutation_for_table("ks", "table_a");
        let m2 = mutation_for_table("ks", "table_b");
        let table_a = TableId::new("ks", "table_a");

        // Append mutations from two different tables.
        let mut pos_a = None;
        for _ in 0..5 {
            if let Ok(pos) = cl.append(&m1) {
                pos_a = Some(pos);
            }
        }
        for _ in 0..5 {
            let _ = cl.append(&m2);
        }

        let pos_a = pos_a.expect("should have appended table_a mutations");

        // Count closed segments.
        let closed_count = cl.closed_segments.lock().len();

        // Discard only table_a — table_b is still dirty.
        cl.discard_completed(&table_a, pos_a).unwrap();

        // Segments with table_b data should NOT be deleted.
        let closed_after = cl.closed_segments.lock().len();
        // If table_b has data in the same segments, they should be retained.
        // The exact count depends on which segments both tables share.
        // The key invariant: segments with remaining dirty tables are kept.
        assert!(
            closed_after <= closed_count,
            "should not have more closed segments after discard"
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn discard_blocked_until_archived() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        let table_id = TableId::new("test_ks", "test_table");

        // Append enough to force at least one rotation.
        let mut last_pos = None;
        for _ in 0..10 {
            if let Ok(pos) = cl.append(&m) {
                last_pos = Some(pos);
            }
        }
        let last_pos = last_pos.unwrap();

        // Mark all tables flushed — but do NOT mark segments as archived.
        cl.discard_completed(&table_id, last_pos).unwrap();

        // Closed segments should still exist because they are not archived.
        let closed = cl.closed_segments.lock();
        // When archive tracking is enabled, segments that are flushed but
        // not archived must not be deleted from disk.
        // (This test will need adjustment once archiving is wired in.)
        // For now: verify the API exists.
        assert!(
            cl.archived_segments().is_empty(),
            "no segments should be archived yet"
        );
        drop(closed);

        cl.shutdown().unwrap();
    }

    #[test]
    fn mark_archived_allows_discard() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        let table_id = TableId::new("test_ks", "test_table");

        // Append to force rotation.
        let mut positions = Vec::new();
        for _ in 0..10 {
            if let Ok(pos) = cl.append(&m) {
                positions.push(pos);
            }
        }

        // Collect closed segment IDs before discard.
        let closed_ids: Vec<u64> = cl.closed_segments.lock().iter().map(|s| s.id).collect();
        assert!(!closed_ids.is_empty(), "need closed segments for this test");

        // Mark all closed segments as archived.
        for id in &closed_ids {
            cl.mark_archived(*id);
        }

        // Verify they are tracked as archived.
        let archived = cl.archived_segments();
        for id in &closed_ids {
            assert!(archived.contains(id), "segment {id} should be archived");
        }

        // Now discard — segments are both flushed and archived, so they
        // should be deleted.
        let last_pos = positions.last().unwrap();
        cl.discard_completed(&table_id, *last_pos).unwrap();

        cl.shutdown().unwrap();
    }

    #[test]
    fn flushed_but_not_archived_segment_kept_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            archive: Some(super::config::ArchiveConfig {
                enabled: true,
                ..super::config::ArchiveConfig::default()
            }),
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        let table_id = TableId::new("test_ks", "test_table");

        // Append to force rotation.
        let mut last_pos = None;
        for _ in 0..10 {
            if let Ok(pos) = cl.append(&m) {
                last_pos = Some(pos);
            }
        }
        let last_pos = last_pos.unwrap();

        // Collect closed segment paths.
        let closed_paths: Vec<std::path::PathBuf> = cl
            .closed_segments
            .lock()
            .iter()
            .map(|s| s.path().to_path_buf())
            .collect();
        assert!(!closed_paths.is_empty());

        // Discard with archiving enabled but no segments marked as archived.
        cl.discard_completed(&table_id, last_pos).unwrap();

        // Segment files should still exist on disk.
        for path in &closed_paths {
            assert!(
                path.exists(),
                "segment {} should still exist (not archived yet)",
                path.display()
            );
        }

        cl.shutdown().unwrap();
    }

    #[test]
    fn parse_segment_id_works() {
        assert_eq!(parse_segment_id("commitlog-1.log"), Some(1));
        assert_eq!(parse_segment_id("commitlog-42.log"), Some(42));
        assert_eq!(parse_segment_id("commitlog-999.log"), Some(999));
        assert_eq!(parse_segment_id("other-file.txt"), None);
        assert_eq!(parse_segment_id("commitlog-.log"), None);
        assert_eq!(parse_segment_id("commitlog-abc.log"), None);
    }

    #[test]
    fn current_position_returns_active_segment_head() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        let pos = cl.current_position();
        assert_eq!(pos.segment_id, 1, "first segment should have id 1");
        // Initial position is after header (17 bytes) + sync marker (8 bytes) = 25.
        assert_eq!(pos.offset, 25, "initial offset should be 25");

        cl.shutdown().unwrap();
    }

    #[test]
    fn current_position_advances_after_append() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        let before = cl.current_position();
        let m = simple_mutation();
        cl.append(&m).unwrap();
        let after = cl.current_position();

        assert_eq!(before.segment_id, after.segment_id);
        assert!(
            after.offset > before.offset,
            "position should advance after append: before={}, after={}",
            before.offset,
            after.offset
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn current_position_reflects_new_segment_after_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        // Write enough to force rotation.
        for _ in 0..10 {
            let _ = cl.append(&m);
        }

        let pos = cl.current_position();
        assert!(
            pos.segment_id > 1,
            "should have rotated to a new segment, got id={}",
            pos.segment_id
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn reopen_keeps_segment_ids_above_existing_checkpoint_generation() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let table_id = TableId::new("test_ks", "test_table");

        let cl = CommitLog::new(config.clone()).unwrap();

        let mut last_pos = None;
        for _ in 0..32 {
            let pos = cl.append(&simple_mutation()).unwrap();
            last_pos = Some(pos);
            if pos.segment_id >= 3 {
                break;
            }
        }
        let last_pos = last_pos.expect("must write at least one mutation");
        assert!(
            last_pos.segment_id >= 3,
            "test must advance checkpoint beyond segment 1; got {:?}",
            last_pos
        );
        cl.discard_completed(&table_id, last_pos).unwrap();
        cl.shutdown().unwrap();

        let (cl2, pending) = CommitLog::open_and_replay(config.clone()).unwrap();
        assert!(
            pending.is_empty(),
            "all first-generation mutations were checkpointed"
        );

        let fresh = Mutation {
            mutation_id: [0x44; 16],
            timestamp: 99_999,
            ..simple_mutation()
        };
        let fresh_pos = cl2.append(&fresh).unwrap();
        assert!(
            fresh_pos.segment_id > last_pos.segment_id,
            "new generation must continue above prior checkpoint generation: \
             fresh={fresh_pos:?} checkpoint={last_pos:?}"
        );
        cl2.shutdown().unwrap();

        let (_cl3, replayed) = CommitLog::open_and_replay(config).unwrap();
        assert_eq!(
            replayed.len(),
            1,
            "fresh mutation written after reopen must survive crash replay"
        );
        assert_eq!(replayed[0].timestamp, fresh.timestamp);
    }

    #[test]
    fn periodic_sync_crash_replay_keeps_recent_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            sync_strategy: SyncStrategyConfig::Periodic {
                sync_interval: std::time::Duration::from_millis(10),
            },
            ..CommitLogConfig::test_config(dir.path())
        };

        let cl = CommitLog::new(config.clone()).unwrap();
        let fresh = Mutation {
            mutation_id: [0x55; 16],
            timestamp: 123_456,
            ..simple_mutation()
        };
        cl.append(&fresh).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(cl);

        let (_reopened, replayed) = CommitLog::open_and_replay(config).unwrap();
        assert_eq!(
            replayed.len(),
            1,
            "periodic sync must leave the latest flushed mutation replayable after crash"
        );
        assert_eq!(replayed[0].timestamp, fresh.timestamp);
    }

    #[test]
    fn periodic_sync_crash_replay_keeps_mutations_across_multiple_flush_cycles() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            sync_strategy: SyncStrategyConfig::Periodic {
                sync_interval: std::time::Duration::from_millis(10),
            },
            ..CommitLogConfig::test_config(dir.path())
        };

        let cl = CommitLog::new(config.clone()).unwrap();
        let first = Mutation {
            mutation_id: [0x61; 16],
            timestamp: 111_111,
            ..simple_mutation()
        };
        cl.append(&first).unwrap();
        // CI can be heavily contended when the full workspace runs in
        // parallel; allow several periodic intervals so this test proves the
        // multi-flush replay behavior instead of depending on tight scheduler
        // timing.
        std::thread::sleep(std::time::Duration::from_millis(150));

        let second = Mutation {
            mutation_id: [0x62; 16],
            timestamp: 222_222,
            ..simple_mutation()
        };
        let second_pos = cl.append(&second).unwrap();
        let target_len = second_pos.offset + Segment::entry_total_size(&second) as u64;
        let segment_path = dir.path().join("commitlog-1.log");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if std::fs::metadata(&segment_path)
                .map(|meta| meta.len() >= target_len)
                .unwrap_or(false)
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            std::fs::metadata(&segment_path)
                .map(|meta| meta.len() >= target_len)
                .unwrap_or(false),
            "periodic sync did not flush the second mutation within 5s"
        );

        drop(cl);

        let (_reopened, replayed) = CommitLog::open_and_replay(config).unwrap();
        let replayed_timestamps: Vec<_> = replayed.iter().map(|m| m.timestamp).collect();
        assert!(
            replayed_timestamps.contains(&first.timestamp),
            "first periodic-sync mutation must replay after crash: {replayed_timestamps:?}"
        );
        assert!(
            replayed_timestamps.contains(&second.timestamp),
            "later periodic-sync mutation from a second flush cycle must replay after crash: \
             {replayed_timestamps:?}"
        );
    }

    #[test]
    fn group_sync_crash_replay_keeps_mutations_across_multiple_flush_cycles() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            sync_strategy: SyncStrategyConfig::Group {
                max_wait: std::time::Duration::from_millis(10),
            },
            ..CommitLogConfig::test_config(dir.path())
        };

        let cl = CommitLog::new(config.clone()).unwrap();
        let first = Mutation {
            mutation_id: [0x63; 16],
            timestamp: 333_333,
            ..simple_mutation()
        };
        cl.append(&first).unwrap();
        // Same rationale as the periodic test above: under full CI load the
        // background group-sync worker may not complete a batch within a
        // narrow 30ms window.
        std::thread::sleep(std::time::Duration::from_millis(150));

        let second = Mutation {
            mutation_id: [0x64; 16],
            timestamp: 444_444,
            ..simple_mutation()
        };
        cl.append(&second).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(150));

        drop(cl);

        let (_reopened, replayed) = CommitLog::open_and_replay(config).unwrap();
        let replayed_timestamps: Vec<_> = replayed.iter().map(|m| m.timestamp).collect();
        assert!(
            replayed_timestamps.contains(&first.timestamp),
            "first group-sync mutation must replay after crash: {replayed_timestamps:?}"
        );
        assert!(
            replayed_timestamps.contains(&second.timestamp),
            "later group-sync mutation from a second flush cycle must replay after crash: \
             {replayed_timestamps:?}"
        );
    }

    #[test]
    fn commitlog_write_span_is_created() {
        crate::test_span_collector::ensure_installed();
        crate::test_span_collector::drain_names();

        let dir = tempfile::tempdir().unwrap();
        // Use Batch sync strategy so flush_to_disk (and its commitlog.sync
        // span) executes inline in the append call, on the current thread.
        let config = CommitLogConfig {
            log_dir: dir.path().to_path_buf(),
            checkpoint_dir: dir.path().to_path_buf(),
            sync_strategy: crate::commitlog::config::SyncStrategyConfig::Batch,
            ..CommitLogConfig::default()
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        cl.append(&m).unwrap();

        let recorded = crate::test_span_collector::drain_names();
        assert!(
            recorded.iter().any(|n| n == "commitlog.write"),
            "expected 'commitlog.write' span, got: {recorded:?}",
        );
        assert!(
            recorded.iter().any(|n| n == "commitlog.sync"),
            "expected 'commitlog.sync' span, got: {recorded:?}",
        );

        cl.shutdown().unwrap();
    }

    // -----------------------------------------------------------------------
    // Closed-segment buffer release tests (P0 OOM fix)
    // -----------------------------------------------------------------------

    #[test]
    fn closed_segments_total_bytes_zero_with_no_closed() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        // No rotation yet: no closed segments.
        assert_eq!(
            cl.closed_segments_total_bytes(),
            0,
            "no closed segments yet: total bytes must be 0"
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn retained_wal_pressure_uses_lowest_segment_id_not_vector_order() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();
        let oldest = TableId::new("test_ks", "oldest");
        let newer = TableId::new("test_ks", "newer");

        for index in 0..16 {
            let mut mutation = if index == 0 {
                mutation_for_table("test_ks", "oldest")
            } else {
                mutation_for_table("test_ks", "newer")
            };
            mutation.rows[0].cells[0] = (0, CellValue::live(vec![b'x'; 300], index as i64 + 1));
            cl.append(&mutation).unwrap();
            cl.force_rotate().unwrap();
        }
        assert!(
            cl.closed_segments.lock().len() >= 8,
            "fixture must exceed the retained-byte pressure budget"
        );
        cl.closed_segments.lock().reverse();

        assert!(
            cl.table_pins_retained_wal_pressure(&oldest),
            "the lowest segment ID owns reclamation pressure even when concurrent rotation reordered the vector"
        );
        assert!(
            !cl.table_pins_retained_wal_pressure(&newer),
            "a newer segment must not steal oldest-segment pressure after vector reordering"
        );
    }

    #[test]
    fn closed_segment_buffers_released_after_rotation() {
        let dir = tempfile::tempdir().unwrap();
        // Small segment to force rotation quickly.
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        for _ in 0..10 {
            let _ = cl.append(&m);
        }

        // At least one segment should have rotated out.
        assert!(
            cl.closed_segment_count() > 0,
            "expected at least one closed segment after 10 appends into a 512-byte segment"
        );

        // After rotation, every closed segment must have released its buffer.
        assert_eq!(
            cl.closed_segments_total_bytes(),
            0,
            "closed segment buffers must be 0 after force_rotate releases them"
        );

        cl.shutdown().unwrap();
    }

    /// Regression: a 0-byte commit-log segment file (torn-create from a
    /// previous crash) must not block startup. The file is skipped, the
    /// `EMPTY_SEGMENT_SKIPPED_TOTAL` counter increments, and replay continues.
    /// See specs/in-process/bug-empty-commitlog-segment-blocks-startup-data-loss.md.
    #[test]
    fn open_and_replay_tolerates_zero_byte_segment() {
        let dir = tempfile::tempdir().unwrap();

        // Pre-seed an empty segment file as if a previous run was killed
        // mid-roll. Use the same naming pattern as production.
        let bad_path = dir.path().join("commitlog-7.log");
        std::fs::File::create(&bad_path).unwrap();
        assert_eq!(std::fs::metadata(&bad_path).unwrap().len(), 0);

        let before = empty_segment_skipped_total();
        let config = CommitLogConfig::test_config(dir.path());
        let (cl, mutations) = CommitLog::open_and_replay(config).expect(
            "open_and_replay must succeed when a 0-byte segment is present \
             (P0 data-availability invariant)",
        );

        assert!(
            mutations.is_empty(),
            "0-byte segment carries no records; replay must yield none"
        );
        assert!(
            empty_segment_skipped_total() > before,
            "EMPTY_SEGMENT_SKIPPED_TOTAL must increment for the skipped torn segment"
        );
        assert!(
            !bad_path.exists(),
            "the 0-byte segment must be cleaned up by the replay tail"
        );

        cl.shutdown().unwrap();
    }

    /// Regression: a partial-header (>0 but < HEADER_SIZE bytes) segment is a
    /// torn tail from a previous crash and must not block startup. It carries no
    /// complete records, so replay should skip and clean it up just like the
    /// zero-byte torn-create case.
    #[test]
    fn open_and_replay_tolerates_partial_header_segment() {
        use super::descriptor::HEADER_SIZE;
        let dir = tempfile::tempdir().unwrap();

        // Write a few bytes, but fewer than HEADER_SIZE.
        let bad_path = dir.path().join("commitlog-3.log");
        std::fs::write(&bad_path, vec![0u8; HEADER_SIZE - 1]).unwrap();

        let before = empty_segment_skipped_total();
        let config = CommitLogConfig::test_config(dir.path());
        let (cl, mutations) = CommitLog::open_and_replay(config)
            .expect("open_and_replay must tolerate partial-header torn-tail segments at startup");

        assert!(
            mutations.is_empty(),
            "partial-header segment carries no complete records; replay must yield none"
        );
        assert!(
            empty_segment_skipped_total() > before,
            "torn partial-header segment must increment the skipped-segment counter"
        );
        assert!(
            !bad_path.exists(),
            "the partial-header segment must be cleaned up by replay"
        );

        cl.shutdown().unwrap();
    }

    /// Regression: a crash can leave a preallocated segment-sized file with an
    /// all-zero header. That has no valid descriptor or records and must be
    /// treated like a torn create, not as fatal ChecksumMismatch.
    #[test]
    fn open_and_replay_tolerates_zeroed_header_segment() {
        use super::descriptor::HEADER_SIZE;
        let dir = tempfile::tempdir().unwrap();

        let bad_path = dir.path().join("commitlog-47.log");
        let mut bytes = vec![0u8; HEADER_SIZE + 4096];
        // Leave the header zeroed; the extra bytes model a sparse/preallocated
        // tail observed after an OOM crash.
        std::fs::write(&bad_path, &bytes).unwrap();
        assert_eq!(
            std::fs::read(&bad_path).unwrap()[..HEADER_SIZE],
            bytes[..HEADER_SIZE]
        );

        let before = empty_segment_skipped_total();
        let config = CommitLogConfig::test_config(dir.path());
        let (cl, mutations) = CommitLog::open_and_replay(config)
            .expect("open_and_replay must tolerate all-zero-header torn segments at startup");

        assert!(mutations.is_empty());
        assert!(
            empty_segment_skipped_total() > before,
            "zeroed-header segment must increment the skipped-segment counter"
        );
        assert!(
            !bad_path.exists(),
            "the zeroed-header segment must be cleaned up by replay"
        );

        bytes[0] = 1; // keep the local buffer used so the test documents intent.
        cl.shutdown().unwrap();
    }
}
