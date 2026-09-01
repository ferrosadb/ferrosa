//! Module: Record and render bounded process-wide storage telemetry.
//! Correctness: Correct when counters are monotonic, gauges reflect complete
//! operations, and observation never allocates in storage hot paths.
//! Last revised: 2026-09-01
//! Last changed: Added maximum and high-threshold SSTable read-fanout signals.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

#[derive(Clone, Copy)]
pub enum FlushPhase {
    LockWait,
    SwapMemtable,
    SnapshotMemtable,
    SortPartitions,
    ValidateRows,
    EncodeSstable,
    LocalWriteSstable,
    Total,
}

impl FlushPhase {
    fn label(self) -> &'static str {
        match self {
            Self::LockWait => "lock_wait",
            Self::SwapMemtable => "swap_memtable",
            Self::SnapshotMemtable => "snapshot_memtable",
            Self::SortPartitions => "sort_partitions",
            Self::ValidateRows => "validate_rows",
            Self::EncodeSstable => "encode_sstable",
            Self::LocalWriteSstable => "local_write_sstable",
            Self::Total => "total",
        }
    }

    fn idx(self) -> usize {
        match self {
            Self::LockWait => 0,
            Self::SwapMemtable => 1,
            Self::SnapshotMemtable => 2,
            Self::SortPartitions => 3,
            Self::ValidateRows => 4,
            Self::EncodeSstable => 5,
            Self::LocalWriteSstable => 6,
            Self::Total => 7,
        }
    }
}

const FLUSH_PHASES: [FlushPhase; 8] = [
    FlushPhase::LockWait,
    FlushPhase::SwapMemtable,
    FlushPhase::SnapshotMemtable,
    FlushPhase::SortPartitions,
    FlushPhase::ValidateRows,
    FlushPhase::EncodeSstable,
    FlushPhase::LocalWriteSstable,
    FlushPhase::Total,
];

#[derive(Clone, Copy)]
pub enum UploadPhase {
    SubmitWait,
    WorkerTask,
    FilePut,
    SyncAwait,
    ManifestSave,
    PendingLogAdd,
    PendingLogRemove,
    PendingLogCompactionAdd,
}

impl UploadPhase {
    fn label(self) -> &'static str {
        match self {
            Self::SubmitWait => "submit_wait",
            Self::WorkerTask => "worker_task",
            Self::FilePut => "file_put",
            Self::SyncAwait => "sync_await",
            Self::ManifestSave => "manifest_save",
            Self::PendingLogAdd => "pending_log_add",
            Self::PendingLogRemove => "pending_log_remove",
            Self::PendingLogCompactionAdd => "pending_log_compaction_add",
        }
    }

    fn idx(self) -> usize {
        match self {
            Self::SubmitWait => 0,
            Self::WorkerTask => 1,
            Self::FilePut => 2,
            Self::SyncAwait => 3,
            Self::ManifestSave => 4,
            Self::PendingLogAdd => 5,
            Self::PendingLogRemove => 6,
            Self::PendingLogCompactionAdd => 7,
        }
    }
}

const UPLOAD_PHASES: [UploadPhase; 8] = [
    UploadPhase::SubmitWait,
    UploadPhase::WorkerTask,
    UploadPhase::FilePut,
    UploadPhase::SyncAwait,
    UploadPhase::ManifestSave,
    UploadPhase::PendingLogAdd,
    UploadPhase::PendingLogRemove,
    UploadPhase::PendingLogCompactionAdd,
];

#[derive(Clone, Copy)]
pub enum CompactionPhase {
    QueueWait,
    OpenInputs,
    MergeRead,
    MergePartition,
    WriterAddPartition,
    WriterFinish,
    LocalWriteSstable,
    OutputVerify,
    PromoteOutput,
    S3UploadAwait,
    ManifestUpdate,
    InputCleanup,
    Total,
}

impl CompactionPhase {
    fn label(self) -> &'static str {
        match self {
            Self::QueueWait => "queue_wait",
            Self::OpenInputs => "open_inputs",
            Self::MergeRead => "merge_read",
            Self::MergePartition => "merge_partition",
            Self::WriterAddPartition => "writer_add_partition",
            Self::WriterFinish => "writer_finish",
            Self::LocalWriteSstable => "local_write_sstable",
            Self::OutputVerify => "output_verify",
            Self::PromoteOutput => "promote_output",
            Self::S3UploadAwait => "s3_upload_await",
            Self::ManifestUpdate => "manifest_update",
            Self::InputCleanup => "input_cleanup",
            Self::Total => "total",
        }
    }

    fn idx(self) -> usize {
        match self {
            Self::QueueWait => 0,
            Self::OpenInputs => 1,
            Self::MergeRead => 2,
            Self::MergePartition => 3,
            Self::WriterAddPartition => 4,
            Self::WriterFinish => 5,
            Self::LocalWriteSstable => 6,
            Self::OutputVerify => 7,
            Self::PromoteOutput => 8,
            Self::S3UploadAwait => 9,
            Self::ManifestUpdate => 10,
            Self::InputCleanup => 11,
            Self::Total => 12,
        }
    }
}

const COMPACTION_PHASES: [CompactionPhase; 13] = [
    CompactionPhase::QueueWait,
    CompactionPhase::OpenInputs,
    CompactionPhase::MergeRead,
    CompactionPhase::MergePartition,
    CompactionPhase::WriterAddPartition,
    CompactionPhase::WriterFinish,
    CompactionPhase::LocalWriteSstable,
    CompactionPhase::OutputVerify,
    CompactionPhase::PromoteOutput,
    CompactionPhase::S3UploadAwait,
    CompactionPhase::ManifestUpdate,
    CompactionPhase::InputCleanup,
    CompactionPhase::Total,
];

#[derive(Clone, Copy)]
pub enum WritePhase {
    AdmissionDisk,
    AdmissionMemtable,
    CommitLogAppend,
    MemtableWrite,
    InlineFlush,
    Observers,
    Total,
}

impl WritePhase {
    fn label(self) -> &'static str {
        match self {
            Self::AdmissionDisk => "admission_disk",
            Self::AdmissionMemtable => "admission_memtable",
            Self::CommitLogAppend => "commitlog_append",
            Self::MemtableWrite => "memtable_write",
            Self::InlineFlush => "inline_flush",
            Self::Observers => "observers",
            Self::Total => "total",
        }
    }

    fn idx(self) -> usize {
        match self {
            Self::AdmissionDisk => 0,
            Self::AdmissionMemtable => 1,
            Self::CommitLogAppend => 2,
            Self::MemtableWrite => 3,
            Self::InlineFlush => 4,
            Self::Observers => 5,
            Self::Total => 6,
        }
    }
}

const WRITE_PHASES: [WritePhase; 7] = [
    WritePhase::AdmissionDisk,
    WritePhase::AdmissionMemtable,
    WritePhase::CommitLogAppend,
    WritePhase::MemtableWrite,
    WritePhase::InlineFlush,
    WritePhase::Observers,
    WritePhase::Total,
];

#[derive(Clone, Copy)]
pub enum WriteFailureReason {
    DiskReserve,
    MemtableBackpressure,
    TableMissing,
    CommitLogAppend,
    MemtableWrite,
}

impl WriteFailureReason {
    fn label(self) -> &'static str {
        match self {
            Self::DiskReserve => "disk_reserve",
            Self::MemtableBackpressure => "memtable_backpressure",
            Self::TableMissing => "table_missing",
            Self::CommitLogAppend => "commitlog_append",
            Self::MemtableWrite => "memtable_write",
        }
    }

    fn idx(self) -> usize {
        match self {
            Self::DiskReserve => 0,
            Self::MemtableBackpressure => 1,
            Self::TableMissing => 2,
            Self::CommitLogAppend => 3,
            Self::MemtableWrite => 4,
        }
    }
}

const WRITE_FAILURE_REASONS: [WriteFailureReason; 5] = [
    WriteFailureReason::DiskReserve,
    WriteFailureReason::MemtableBackpressure,
    WriteFailureReason::TableMissing,
    WriteFailureReason::CommitLogAppend,
    WriteFailureReason::MemtableWrite,
];

static FLUSH_PHASE_MICROS_TOTAL: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
static FLUSH_PHASE_COUNT_TOTAL: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
static FLUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static FLUSH_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
static FLUSH_ROWS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FLUSH_PARTITIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FLUSH_LAST_BYTES: AtomicU64 = AtomicU64::new(0);
static FLUSH_LAST_ROWS: AtomicU64 = AtomicU64::new(0);
static FLUSH_LAST_PARTITIONS: AtomicU64 = AtomicU64::new(0);

static UPLOAD_PHASE_MICROS_TOTAL: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
static UPLOAD_PHASE_COUNT_TOTAL: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
static UPLOAD_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static UPLOAD_QUEUE_DEPTH_MAX: AtomicU64 = AtomicU64::new(0);
static UPLOAD_TASKS_TOTAL: AtomicU64 = AtomicU64::new(0);
static UPLOAD_FILES_TOTAL: AtomicU64 = AtomicU64::new(0);
static UPLOAD_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

static COMPACTION_PHASE_MICROS_TOTAL: [AtomicU64; 13] = [const { AtomicU64::new(0) }; 13];
static COMPACTION_PHASE_MICROS_MAX: [AtomicU64; 13] = [const { AtomicU64::new(0) }; 13];
static COMPACTION_PHASE_COUNT_TOTAL: [AtomicU64; 13] = [const { AtomicU64::new(0) }; 13];
static COMPACTION_SUBMITTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_SKIPPED_OVERLAP_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_STARTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_COMPLETED_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static COMPACTION_QUEUE_DEPTH_MAX: AtomicU64 = AtomicU64::new(0);
static COMPACTION_RUNNING: AtomicU64 = AtomicU64::new(0);
static COMPACTION_RUNNING_MAX: AtomicU64 = AtomicU64::new(0);
static COMPACTION_INPUT_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_OUTPUT_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_INPUT_ROWS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_OUTPUT_ROWS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_OUTPUT_PARTITIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_LAST_INPUT_BYTES: AtomicU64 = AtomicU64::new(0);
static COMPACTION_LAST_OUTPUT_BYTES: AtomicU64 = AtomicU64::new(0);
static COMPACTION_LAST_INPUT_ROWS: AtomicU64 = AtomicU64::new(0);
static COMPACTION_LAST_OUTPUT_ROWS: AtomicU64 = AtomicU64::new(0);
static COMPACTION_LAST_OUTPUT_PARTITIONS: AtomicU64 = AtomicU64::new(0);
/// Count of compaction input SSTable readers obtained via the engine-wide
/// reader pool (FMEA #11). Non-zero confirms compaction input opens are routed
/// through the bounded pool rather than opening unbounded readers directly.
static COMPACTION_POOL_INPUT_OPENS_TOTAL: AtomicU64 = AtomicU64::new(0);

static WRITE_PHASE_MICROS_TOTAL: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
static WRITE_PHASE_MICROS_MAX: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
static WRITE_PHASE_COUNT_TOTAL: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
static WRITE_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_FAILURE_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_FAILURE_REASON_TOTAL: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];
static WRITE_INLINE_FLUSH_TOTAL: AtomicU64 = AtomicU64::new(0);
static MEMTABLE_SIZE_BYTES_MAX: AtomicU64 = AtomicU64::new(0);
static MEMTABLE_FLUSH_THRESHOLD_BYTES: AtomicU64 = AtomicU64::new(0);
static MEMTABLE_BACKPRESSURE_BYTES: AtomicU64 = AtomicU64::new(0);

static RANGE_READ_TRUNCATED_TOTAL: AtomicU64 = AtomicU64::new(0);
static INDEX_RELOAD_SKIPPED_ROWS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_LIMITED_ROWS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_LIMITED_ROWS_FOUND_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_LIMITED_ROWS_SECONDS_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_LIMITED_ROWS_SECONDS_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static READ_LIMITED_ROWS_MEMTABLE_HITS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_LIMITED_ROWS_FLUSHING_HITS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_LIMITED_ROWS_SSTABLE_PRUNED_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_LIMITED_ROWS_SSTABLE_PROBES_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_LIMITED_ROWS_SSTABLE_HITS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_LIMITED_ROWS_SSTABLE_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_SSTABLE_FANOUT_MAX: AtomicU64 = AtomicU64::new(0);
static READ_SSTABLE_HIGH_FANOUT_TOTAL: AtomicU64 = AtomicU64::new(0);
static SSTABLE_REHYDRATION_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SSTABLE_REHYDRATION_SUCCESS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SSTABLE_REHYDRATION_FAILURE_TOTAL: AtomicU64 = AtomicU64::new(0);
static SSTABLE_REHYDRATION_COMPONENTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SSTABLE_REHYDRATION_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SSTABLE_REHYDRATION_SECONDS_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SSTABLE_REHYDRATION_SECONDS_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static SSTABLE_REHYDRATION_IN_FLIGHT: AtomicU64 = AtomicU64::new(0);
static SSTABLE_REHYDRATION_IN_FLIGHT_MAX: AtomicU64 = AtomicU64::new(0);

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

pub fn observe_flush_phase(phase: FlushPhase, duration: Duration) {
    let idx = phase.idx();
    FLUSH_PHASE_MICROS_TOTAL[idx].fetch_add(duration_micros(duration), Ordering::Relaxed);
    FLUSH_PHASE_COUNT_TOTAL[idx].fetch_add(1, Ordering::Relaxed);
}

pub fn observe_flush_output(bytes: u64, rows: u64, partitions: u64) {
    FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
    FLUSH_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
    FLUSH_ROWS_TOTAL.fetch_add(rows, Ordering::Relaxed);
    FLUSH_PARTITIONS_TOTAL.fetch_add(partitions, Ordering::Relaxed);
    FLUSH_LAST_BYTES.store(bytes, Ordering::Relaxed);
    FLUSH_LAST_ROWS.store(rows, Ordering::Relaxed);
    FLUSH_LAST_PARTITIONS.store(partitions, Ordering::Relaxed);
}

pub fn observe_upload_phase(phase: UploadPhase, duration: Duration) {
    let idx = phase.idx();
    UPLOAD_PHASE_MICROS_TOTAL[idx].fetch_add(duration_micros(duration), Ordering::Relaxed);
    UPLOAD_PHASE_COUNT_TOTAL[idx].fetch_add(1, Ordering::Relaxed);
}

pub fn inc_upload_queue_depth() {
    let depth = UPLOAD_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
    update_max_u64(&UPLOAD_QUEUE_DEPTH_MAX, depth);
}

pub fn dec_upload_queue_depth() {
    let _ = UPLOAD_QUEUE_DEPTH.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_sub(1))
    });
}

pub fn observe_upload_file(bytes: u64, duration: Duration) {
    UPLOAD_FILES_TOTAL.fetch_add(1, Ordering::Relaxed);
    UPLOAD_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
    observe_upload_phase(UploadPhase::FilePut, duration);
}

pub fn observe_upload_task(duration: Duration) {
    UPLOAD_TASKS_TOTAL.fetch_add(1, Ordering::Relaxed);
    observe_upload_phase(UploadPhase::WorkerTask, duration);
}

pub fn observe_compaction_phase(phase: CompactionPhase, duration: Duration) {
    let idx = phase.idx();
    let micros = duration_micros(duration);
    COMPACTION_PHASE_MICROS_TOTAL[idx].fetch_add(micros, Ordering::Relaxed);
    COMPACTION_PHASE_COUNT_TOTAL[idx].fetch_add(1, Ordering::Relaxed);
    update_max_u64(&COMPACTION_PHASE_MICROS_MAX[idx], micros);
}

pub fn inc_compaction_submitted() {
    COMPACTION_SUBMITTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_compaction_skipped_overlap() {
    COMPACTION_SKIPPED_OVERLAP_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_compaction_queue_depth() {
    let depth = COMPACTION_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
    update_max_u64(&COMPACTION_QUEUE_DEPTH_MAX, depth);
}

pub fn dec_compaction_queue_depth() {
    let _ = COMPACTION_QUEUE_DEPTH.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_sub(1))
    });
}

pub fn inc_compaction_running() {
    COMPACTION_STARTED_TOTAL.fetch_add(1, Ordering::Relaxed);
    let running = COMPACTION_RUNNING.fetch_add(1, Ordering::Relaxed) + 1;
    update_max_u64(&COMPACTION_RUNNING_MAX, running);
}

pub fn dec_compaction_running() {
    let _ = COMPACTION_RUNNING.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_sub(1))
    });
}

pub fn inc_compaction_failed() {
    COMPACTION_FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Record a compaction input reader obtained through the engine-wide reader
/// pool (FMEA #11). Called once per input SSTable per task when the executor is
/// pool-routed.
pub fn inc_compaction_pool_input_opens() {
    COMPACTION_POOL_INPUT_OPENS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Total compaction input readers obtained via the reader pool since startup.
pub fn compaction_pool_input_opens_total() -> u64 {
    COMPACTION_POOL_INPUT_OPENS_TOTAL.load(Ordering::Relaxed)
}

/// A capped range read hit its partition cap while more data still existed,
/// so the caller refused to silently truncate and failed loud instead. A
/// non-zero value indicates a query shape (ORDER BY / DISTINCT / aggregate /
/// function projection over `ALLOW FILTERING`) that scanned past the default
/// range-read window; the query must add a LIMIT, an index, or a narrower
/// predicate.
pub fn inc_range_read_truncated() {
    RANGE_READ_TRUNCATED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Total capped range reads that detected more data beyond the cap and failed
/// loud rather than truncating, since startup.
pub fn range_read_truncated_total() -> u64 {
    RANGE_READ_TRUNCATED_TOTAL.load(Ordering::Relaxed)
}

/// Record `n` persisted `system_schema.indexes` rows skipped as unresolvable
/// during an index reload (`reload_indexes_from_system_schema`).
///
/// A non-zero steady-state value means the cluster carries dangling index
/// registrations — typically debris from a DROP TABLE that predates the
/// tombstone cascade (forge t_ae06e925). The debris is visible here instead of
/// as per-orphan boot warns; clean it up with `DROP INDEX IF EXISTS` per
/// orphan (no automatic GC: a table can legitimately be mid-registration at
/// boot).
pub fn add_index_reload_skipped(n: u64) {
    INDEX_RELOAD_SKIPPED_ROWS_TOTAL.fetch_add(n, Ordering::Relaxed);
}

/// Total unresolvable `system_schema.indexes` rows skipped by index reloads
/// since startup. See [`add_index_reload_skipped`].
pub fn index_reload_skipped_rows_total() -> u64 {
    INDEX_RELOAD_SKIPPED_ROWS_TOTAL.load(Ordering::Relaxed)
}

pub fn observe_compaction_completed(
    duration: Duration,
    input_bytes: u64,
    output_bytes: u64,
    input_rows: u64,
    output_rows: u64,
    output_partitions: u64,
) {
    COMPACTION_COMPLETED_TOTAL.fetch_add(1, Ordering::Relaxed);
    COMPACTION_INPUT_BYTES_TOTAL.fetch_add(input_bytes, Ordering::Relaxed);
    COMPACTION_OUTPUT_BYTES_TOTAL.fetch_add(output_bytes, Ordering::Relaxed);
    COMPACTION_INPUT_ROWS_TOTAL.fetch_add(input_rows, Ordering::Relaxed);
    COMPACTION_OUTPUT_ROWS_TOTAL.fetch_add(output_rows, Ordering::Relaxed);
    COMPACTION_OUTPUT_PARTITIONS_TOTAL.fetch_add(output_partitions, Ordering::Relaxed);
    COMPACTION_LAST_INPUT_BYTES.store(input_bytes, Ordering::Relaxed);
    COMPACTION_LAST_OUTPUT_BYTES.store(output_bytes, Ordering::Relaxed);
    COMPACTION_LAST_INPUT_ROWS.store(input_rows, Ordering::Relaxed);
    COMPACTION_LAST_OUTPUT_ROWS.store(output_rows, Ordering::Relaxed);
    COMPACTION_LAST_OUTPUT_PARTITIONS.store(output_partitions, Ordering::Relaxed);
    observe_compaction_phase(CompactionPhase::Total, duration);
}

pub fn observe_write_phase(phase: WritePhase, duration: Duration) {
    let idx = phase.idx();
    let micros = duration_micros(duration);
    WRITE_PHASE_MICROS_TOTAL[idx].fetch_add(micros, Ordering::Relaxed);
    WRITE_PHASE_COUNT_TOTAL[idx].fetch_add(1, Ordering::Relaxed);
    update_max_u64(&WRITE_PHASE_MICROS_MAX[idx], micros);
}

pub fn inc_write_total() {
    WRITE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_write_failure() {
    WRITE_FAILURE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_write_failure_reason(reason: WriteFailureReason) {
    WRITE_FAILURE_TOTAL.fetch_add(1, Ordering::Relaxed);
    WRITE_FAILURE_REASON_TOTAL[reason.idx()].fetch_add(1, Ordering::Relaxed);
}

pub fn inc_write_inline_flush() {
    WRITE_INLINE_FLUSH_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn set_memtable_thresholds(flush_threshold_bytes: u64, backpressure_bytes: u64) {
    MEMTABLE_FLUSH_THRESHOLD_BYTES.store(flush_threshold_bytes, Ordering::Relaxed);
    MEMTABLE_BACKPRESSURE_BYTES.store(backpressure_bytes, Ordering::Relaxed);
}

pub fn observe_memtable_size(size_bytes: u64) {
    update_max_u64(&MEMTABLE_SIZE_BYTES_MAX, size_bytes);
}

#[allow(clippy::too_many_arguments)]
pub fn observe_read_limited_rows(
    duration: Duration,
    found: bool,
    memtable_hits: u64,
    flushing_hits: u64,
    sstable_pruned: u64,
    sstable_probes: u64,
    sstable_hits: u64,
    sstable_errors: u64,
) {
    READ_LIMITED_ROWS_TOTAL.fetch_add(1, Ordering::Relaxed);
    if found {
        READ_LIMITED_ROWS_FOUND_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    let micros = duration_micros(duration);
    READ_LIMITED_ROWS_SECONDS_MICROS_TOTAL.fetch_add(micros, Ordering::Relaxed);
    update_max_u64(&READ_LIMITED_ROWS_SECONDS_MICROS_MAX, micros);
    READ_LIMITED_ROWS_MEMTABLE_HITS_TOTAL.fetch_add(memtable_hits, Ordering::Relaxed);
    READ_LIMITED_ROWS_FLUSHING_HITS_TOTAL.fetch_add(flushing_hits, Ordering::Relaxed);
    READ_LIMITED_ROWS_SSTABLE_PRUNED_TOTAL.fetch_add(sstable_pruned, Ordering::Relaxed);
    READ_LIMITED_ROWS_SSTABLE_PROBES_TOTAL.fetch_add(sstable_probes, Ordering::Relaxed);
    READ_LIMITED_ROWS_SSTABLE_HITS_TOTAL.fetch_add(sstable_hits, Ordering::Relaxed);
    READ_LIMITED_ROWS_SSTABLE_ERRORS_TOTAL.fetch_add(sstable_errors, Ordering::Relaxed);
}

/// Record the number of immutable SSTable descriptors examined by one read
/// attempt. The caller classifies the configured operational threshold so this
/// hot path remains allocation-free and independent of table identity.
pub fn observe_read_sstable_fanout(fanout: usize, high_fanout: bool) {
    let fanout = fanout.min(u64::MAX as usize) as u64;
    update_max_u64(&READ_SSTABLE_FANOUT_MAX, fanout);
    if high_fanout {
        READ_SSTABLE_HIGH_FANOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn inc_sstable_rehydration_request() {
    SSTABLE_REHYDRATION_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_sstable_rehydration_in_flight() {
    let in_flight = SSTABLE_REHYDRATION_IN_FLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
    update_max_u64(&SSTABLE_REHYDRATION_IN_FLIGHT_MAX, in_flight);
}

pub fn dec_sstable_rehydration_in_flight() {
    let _ = SSTABLE_REHYDRATION_IN_FLIGHT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_sub(1))
    });
}

pub fn observe_sstable_rehydration_success(duration: Duration, components: u64, bytes: u64) {
    SSTABLE_REHYDRATION_SUCCESS_TOTAL.fetch_add(1, Ordering::Relaxed);
    SSTABLE_REHYDRATION_COMPONENTS_TOTAL.fetch_add(components, Ordering::Relaxed);
    SSTABLE_REHYDRATION_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
    let micros = duration_micros(duration);
    SSTABLE_REHYDRATION_SECONDS_MICROS_TOTAL.fetch_add(micros, Ordering::Relaxed);
    update_max_u64(&SSTABLE_REHYDRATION_SECONDS_MICROS_MAX, micros);
}

pub fn observe_sstable_rehydration_failure(duration: Duration) {
    SSTABLE_REHYDRATION_FAILURE_TOTAL.fetch_add(1, Ordering::Relaxed);
    let micros = duration_micros(duration);
    SSTABLE_REHYDRATION_SECONDS_MICROS_TOTAL.fetch_add(micros, Ordering::Relaxed);
    update_max_u64(&SSTABLE_REHYDRATION_SECONDS_MICROS_MAX, micros);
}

pub fn render_prometheus() -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP ferrosa_storage_writes_total StorageEngine::write calls completed successfully.\n",
    );
    out.push_str("# TYPE ferrosa_storage_writes_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_writes_total {}\n",
        WRITE_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_write_failures_total StorageEngine::write calls that returned an error.\n");
    out.push_str("# TYPE ferrosa_storage_write_failures_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_write_failures_total {}\n",
        WRITE_FAILURE_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_write_failures_by_reason_total StorageEngine::write failures partitioned by the admission or write phase that failed.\n");
    out.push_str("# TYPE ferrosa_storage_write_failures_by_reason_total counter\n");
    for reason in WRITE_FAILURE_REASONS {
        out.push_str(&format!(
            "ferrosa_storage_write_failures_by_reason_total{{reason=\"{}\"}} {}\n",
            reason.label(),
            WRITE_FAILURE_REASON_TOTAL[reason.idx()].load(Ordering::Relaxed)
        ));
    }
    out.push_str("# HELP ferrosa_storage_write_inline_flush_total StorageEngine::write calls that synchronously ran a memtable flush.\n");
    out.push_str("# TYPE ferrosa_storage_write_inline_flush_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_write_inline_flush_total {}\n",
        WRITE_INLINE_FLUSH_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_memtable_size_bytes_max Maximum observed active memtable size across write admission checks.\n");
    out.push_str("# TYPE ferrosa_storage_memtable_size_bytes_max gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_memtable_size_bytes_max {}\n",
        MEMTABLE_SIZE_BYTES_MAX.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_memtable_flush_threshold_bytes Configured memtable flush request threshold.\n");
    out.push_str("# TYPE ferrosa_storage_memtable_flush_threshold_bytes gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_memtable_flush_threshold_bytes {}\n",
        MEMTABLE_FLUSH_THRESHOLD_BYTES.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_memtable_backpressure_bytes Configured hard memtable write backpressure threshold.\n");
    out.push_str("# TYPE ferrosa_storage_memtable_backpressure_bytes gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_memtable_backpressure_bytes {}\n",
        MEMTABLE_BACKPRESSURE_BYTES.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_write_phase_seconds_total Total wall time spent in StorageEngine::write phases.\n");
    out.push_str("# TYPE ferrosa_storage_write_phase_seconds_total counter\n");
    out.push_str("# HELP ferrosa_storage_write_phase_seconds_max Maximum observed wall time for a StorageEngine::write phase.\n");
    out.push_str("# TYPE ferrosa_storage_write_phase_seconds_max gauge\n");
    out.push_str("# HELP ferrosa_storage_write_phase_total Number of observations for StorageEngine::write phases.\n");
    out.push_str("# TYPE ferrosa_storage_write_phase_total counter\n");
    for phase in WRITE_PHASES {
        let idx = phase.idx();
        let label = phase.label();
        let seconds = WRITE_PHASE_MICROS_TOTAL[idx].load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let max_seconds = WRITE_PHASE_MICROS_MAX[idx].load(Ordering::Relaxed) as f64 / 1_000_000.0;
        out.push_str(&format!(
            "ferrosa_storage_write_phase_seconds_total{{phase=\"{label}\"}} {seconds}\n"
        ));
        out.push_str(&format!(
            "ferrosa_storage_write_phase_seconds_max{{phase=\"{label}\"}} {max_seconds}\n"
        ));
        out.push_str(&format!(
            "ferrosa_storage_write_phase_total{{phase=\"{label}\"}} {}\n",
            WRITE_PHASE_COUNT_TOTAL[idx].load(Ordering::Relaxed)
        ));
    }

    out.push_str("# HELP ferrosa_storage_flushes_total Memtable flushes completed.\n");
    out.push_str("# TYPE ferrosa_storage_flushes_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_flushes_total {}\n",
        FLUSHES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_flush_bytes_total Bytes emitted by completed memtable flushes.\n",
    );
    out.push_str("# TYPE ferrosa_storage_flush_bytes_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_flush_bytes_total {}\n",
        FLUSH_BYTES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_flush_rows_total Rows emitted by completed memtable flushes.\n",
    );
    out.push_str("# TYPE ferrosa_storage_flush_rows_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_flush_rows_total {}\n",
        FLUSH_ROWS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_flush_partitions_total Partitions emitted by completed memtable flushes.\n");
    out.push_str("# TYPE ferrosa_storage_flush_partitions_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_flush_partitions_total {}\n",
        FLUSH_PARTITIONS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_flush_last_bytes Bytes emitted by the most recent completed memtable flush.\n");
    out.push_str("# TYPE ferrosa_storage_flush_last_bytes gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_flush_last_bytes {}\n",
        FLUSH_LAST_BYTES.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_flush_last_rows Rows emitted by the most recent completed memtable flush.\n");
    out.push_str("# TYPE ferrosa_storage_flush_last_rows gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_flush_last_rows {}\n",
        FLUSH_LAST_ROWS.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_flush_last_partitions Partitions emitted by the most recent completed memtable flush.\n");
    out.push_str("# TYPE ferrosa_storage_flush_last_partitions gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_flush_last_partitions {}\n",
        FLUSH_LAST_PARTITIONS.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_flush_phase_seconds_total Total wall time spent in memtable flush phases.\n");
    out.push_str("# TYPE ferrosa_storage_flush_phase_seconds_total counter\n");
    out.push_str("# HELP ferrosa_storage_flush_phase_total Number of observations for memtable flush phases.\n");
    out.push_str("# TYPE ferrosa_storage_flush_phase_total counter\n");
    for phase in FLUSH_PHASES {
        let idx = phase.idx();
        let label = phase.label();
        let seconds = FLUSH_PHASE_MICROS_TOTAL[idx].load(Ordering::Relaxed) as f64 / 1_000_000.0;
        out.push_str(&format!(
            "ferrosa_storage_flush_phase_seconds_total{{phase=\"{label}\"}} {seconds}\n"
        ));
        out.push_str(&format!(
            "ferrosa_storage_flush_phase_total{{phase=\"{label}\"}} {}\n",
            FLUSH_PHASE_COUNT_TOTAL[idx].load(Ordering::Relaxed)
        ));
    }

    out.push_str(
        "# HELP ferrosa_storage_upload_queue_depth Upload tasks currently queued or in progress.\n",
    );
    out.push_str("# TYPE ferrosa_storage_upload_queue_depth gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_upload_queue_depth {}\n",
        UPLOAD_QUEUE_DEPTH.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_upload_queue_depth_max Maximum observed upload queue depth.\n",
    );
    out.push_str("# TYPE ferrosa_storage_upload_queue_depth_max gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_upload_queue_depth_max {}\n",
        UPLOAD_QUEUE_DEPTH_MAX.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_upload_tasks_total Upload tasks processed by the upload worker.\n",
    );
    out.push_str("# TYPE ferrosa_storage_upload_tasks_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_upload_tasks_total {}\n",
        UPLOAD_TASKS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_upload_files_total SSTable component files uploaded.\n");
    out.push_str("# TYPE ferrosa_storage_upload_files_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_upload_files_total {}\n",
        UPLOAD_FILES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_upload_bytes_total SSTable component bytes uploaded.\n");
    out.push_str("# TYPE ferrosa_storage_upload_bytes_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_upload_bytes_total {}\n",
        UPLOAD_BYTES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_upload_phase_seconds_total Total wall time spent in upload and S3 sync phases.\n");
    out.push_str("# TYPE ferrosa_storage_upload_phase_seconds_total counter\n");
    out.push_str("# HELP ferrosa_storage_upload_phase_total Number of observations for upload and S3 sync phases.\n");
    out.push_str("# TYPE ferrosa_storage_upload_phase_total counter\n");
    for phase in UPLOAD_PHASES {
        let idx = phase.idx();
        let label = phase.label();
        let seconds = UPLOAD_PHASE_MICROS_TOTAL[idx].load(Ordering::Relaxed) as f64 / 1_000_000.0;
        out.push_str(&format!(
            "ferrosa_storage_upload_phase_seconds_total{{phase=\"{label}\"}} {seconds}\n"
        ));
        out.push_str(&format!(
            "ferrosa_storage_upload_phase_total{{phase=\"{label}\"}} {}\n",
            UPLOAD_PHASE_COUNT_TOTAL[idx].load(Ordering::Relaxed)
        ));
    }

    out.push_str("# HELP ferrosa_storage_compaction_submitted_total Compaction tasks submitted to the executor.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_submitted_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_submitted_total {}\n",
        COMPACTION_SUBMITTED_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_skipped_overlap_total Compaction tasks skipped because an input SSTable was already in flight.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_skipped_overlap_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_skipped_overlap_total {}\n",
        COMPACTION_SKIPPED_OVERLAP_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_started_total Compaction tasks started by executor workers.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_started_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_started_total {}\n",
        COMPACTION_STARTED_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_completed_total Compaction tasks completed successfully.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_completed_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_completed_total {}\n",
        COMPACTION_COMPLETED_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_failed_total Compaction tasks that failed in executor workers.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_failed_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_failed_total {}\n",
        COMPACTION_FAILED_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_compaction_queue_depth Compaction tasks waiting in executor queues.\n",
    );
    out.push_str("# TYPE ferrosa_storage_compaction_queue_depth gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_queue_depth {}\n",
        COMPACTION_QUEUE_DEPTH.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_queue_depth_max Maximum observed compaction executor queue depth.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_queue_depth_max gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_queue_depth_max {}\n",
        COMPACTION_QUEUE_DEPTH_MAX.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_running Compaction tasks currently running.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_running gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_running {}\n",
        COMPACTION_RUNNING.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_compaction_running_max Maximum observed concurrent compaction tasks.\n",
    );
    out.push_str("# TYPE ferrosa_storage_compaction_running_max gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_running_max {}\n",
        COMPACTION_RUNNING_MAX.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_pool_input_opens_total Compaction input readers obtained via the engine-wide reader pool (FMEA #11).\n");
    out.push_str("# TYPE ferrosa_storage_compaction_pool_input_opens_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_pool_input_opens_total {}\n",
        COMPACTION_POOL_INPUT_OPENS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_input_bytes_total Input bytes read by completed compactions.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_input_bytes_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_input_bytes_total {}\n",
        COMPACTION_INPUT_BYTES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_output_bytes_total Output bytes written by completed compactions.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_output_bytes_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_output_bytes_total {}\n",
        COMPACTION_OUTPUT_BYTES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_input_rows_total Input rows seen by completed compactions.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_input_rows_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_input_rows_total {}\n",
        COMPACTION_INPUT_ROWS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_output_rows_total Output rows emitted by completed compactions.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_output_rows_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_output_rows_total {}\n",
        COMPACTION_OUTPUT_ROWS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_output_partitions_total Output partitions emitted by completed compactions.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_output_partitions_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_output_partitions_total {}\n",
        COMPACTION_OUTPUT_PARTITIONS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_last_input_bytes Input bytes read by the most recent successful compaction.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_last_input_bytes gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_last_input_bytes {}\n",
        COMPACTION_LAST_INPUT_BYTES.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_last_output_bytes Output bytes written by the most recent successful compaction.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_last_output_bytes gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_last_output_bytes {}\n",
        COMPACTION_LAST_OUTPUT_BYTES.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_last_input_rows Input rows seen by the most recent successful compaction.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_last_input_rows gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_last_input_rows {}\n",
        COMPACTION_LAST_INPUT_ROWS.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_last_output_rows Output rows emitted by the most recent successful compaction.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_last_output_rows gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_last_output_rows {}\n",
        COMPACTION_LAST_OUTPUT_ROWS.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_last_output_partitions Output partitions emitted by the most recent successful compaction.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_last_output_partitions gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_last_output_partitions {}\n",
        COMPACTION_LAST_OUTPUT_PARTITIONS.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_phase_seconds_total Total wall time spent in compaction phases.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_phase_seconds_total counter\n");
    out.push_str("# HELP ferrosa_storage_compaction_phase_seconds_max Maximum observed wall time for a compaction phase.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_phase_seconds_max gauge\n");
    out.push_str("# HELP ferrosa_storage_compaction_phase_total Number of observations for compaction phases.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_phase_total counter\n");
    for phase in COMPACTION_PHASES {
        let idx = phase.idx();
        let label = phase.label();
        let seconds =
            COMPACTION_PHASE_MICROS_TOTAL[idx].load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let max_seconds =
            COMPACTION_PHASE_MICROS_MAX[idx].load(Ordering::Relaxed) as f64 / 1_000_000.0;
        out.push_str(&format!(
            "ferrosa_storage_compaction_phase_seconds_total{{phase=\"{label}\"}} {seconds}\n"
        ));
        out.push_str(&format!(
            "ferrosa_storage_compaction_phase_seconds_max{{phase=\"{label}\"}} {max_seconds}\n"
        ));
        out.push_str(&format!(
            "ferrosa_storage_compaction_phase_total{{phase=\"{label}\"}} {}\n",
            COMPACTION_PHASE_COUNT_TOTAL[idx].load(Ordering::Relaxed)
        ));
    }

    out.push_str(
        "# HELP ferrosa_storage_range_read_truncated_total Capped range reads that hit their cap with more data available and failed loud instead of truncating.\n",
    );
    out.push_str("# TYPE ferrosa_storage_range_read_truncated_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_range_read_truncated_total {}\n",
        RANGE_READ_TRUNCATED_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_index_reload_skipped_rows_total Unresolvable system_schema.indexes rows skipped during index reload (dangling registrations; clean up with DROP INDEX IF EXISTS).\n",
    );
    out.push_str("# TYPE ferrosa_storage_index_reload_skipped_rows_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_index_reload_skipped_rows_total {}\n",
        INDEX_RELOAD_SKIPPED_ROWS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_read_limited_rows_total Partition read_limited_rows calls.\n",
    );
    out.push_str("# TYPE ferrosa_storage_read_limited_rows_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_read_limited_rows_total {}\n",
        READ_LIMITED_ROWS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_read_limited_rows_found_total Partition read_limited_rows calls that found at least one source.\n");
    out.push_str("# TYPE ferrosa_storage_read_limited_rows_found_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_read_limited_rows_found_total {}\n",
        READ_LIMITED_ROWS_FOUND_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_read_limited_rows_seconds_total Total wall time spent in read_limited_rows.\n");
    out.push_str("# TYPE ferrosa_storage_read_limited_rows_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_read_limited_rows_seconds_total {:.9}\n",
        READ_LIMITED_ROWS_SECONDS_MICROS_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_storage_read_limited_rows_seconds_max Maximum observed wall time for read_limited_rows.\n");
    out.push_str("# TYPE ferrosa_storage_read_limited_rows_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_read_limited_rows_seconds_max {:.9}\n",
        READ_LIMITED_ROWS_SECONDS_MICROS_MAX.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_storage_read_limited_rows_memtable_hits_total Active memtable hits observed by read_limited_rows.\n");
    out.push_str("# TYPE ferrosa_storage_read_limited_rows_memtable_hits_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_read_limited_rows_memtable_hits_total {}\n",
        READ_LIMITED_ROWS_MEMTABLE_HITS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_read_limited_rows_flushing_hits_total Flushing memtable hits observed by read_limited_rows.\n");
    out.push_str("# TYPE ferrosa_storage_read_limited_rows_flushing_hits_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_read_limited_rows_flushing_hits_total {}\n",
        READ_LIMITED_ROWS_FLUSHING_HITS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_read_limited_rows_sstable_pruned_total SSTables skipped before index lookup by read_limited_rows.\n");
    out.push_str("# TYPE ferrosa_storage_read_limited_rows_sstable_pruned_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_read_limited_rows_sstable_pruned_total {}\n",
        READ_LIMITED_ROWS_SSTABLE_PRUNED_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_read_limited_rows_sstable_probes_total SSTable probes issued by read_limited_rows.\n");
    out.push_str("# TYPE ferrosa_storage_read_limited_rows_sstable_probes_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_read_limited_rows_sstable_probes_total {}\n",
        READ_LIMITED_ROWS_SSTABLE_PROBES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_read_limited_rows_sstable_hits_total SSTable hits observed by read_limited_rows.\n");
    out.push_str("# TYPE ferrosa_storage_read_limited_rows_sstable_hits_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_read_limited_rows_sstable_hits_total {}\n",
        READ_LIMITED_ROWS_SSTABLE_HITS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_read_limited_rows_sstable_errors_total SSTable read errors observed by read_limited_rows.\n");
    out.push_str("# TYPE ferrosa_storage_read_limited_rows_sstable_errors_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_read_limited_rows_sstable_errors_total {}\n",
        READ_LIMITED_ROWS_SSTABLE_ERRORS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_read_sstable_fanout_max Maximum SSTable descriptor fanout observed for one partition-read attempt.\n");
    out.push_str("# TYPE ferrosa_storage_read_sstable_fanout_max gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_read_sstable_fanout_max {}\n",
        READ_SSTABLE_FANOUT_MAX.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_read_sstable_high_fanout_total Partition-read attempts whose SSTable descriptor fanout exceeded the operational threshold.\n");
    out.push_str("# TYPE ferrosa_storage_read_sstable_high_fanout_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_read_sstable_high_fanout_total {}\n",
        READ_SSTABLE_HIGH_FANOUT_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_sstable_rehydration_requests_total SSTable read-through rehydration attempts.\n");
    out.push_str("# TYPE ferrosa_storage_sstable_rehydration_requests_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_sstable_rehydration_requests_total {}\n",
        SSTABLE_REHYDRATION_REQUESTS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_sstable_rehydration_success_total SSTable read-through rehydrations that restored at least one component or found the requested component present.\n");
    out.push_str("# TYPE ferrosa_storage_sstable_rehydration_success_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_sstable_rehydration_success_total {}\n",
        SSTABLE_REHYDRATION_SUCCESS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_sstable_rehydration_failure_total SSTable read-through rehydrations that failed.\n");
    out.push_str("# TYPE ferrosa_storage_sstable_rehydration_failure_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_sstable_rehydration_failure_total {}\n",
        SSTABLE_REHYDRATION_FAILURE_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_sstable_rehydration_components_total SSTable components restored by read-through rehydration.\n");
    out.push_str("# TYPE ferrosa_storage_sstable_rehydration_components_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_sstable_rehydration_components_total {}\n",
        SSTABLE_REHYDRATION_COMPONENTS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_sstable_rehydration_bytes_total Bytes restored by SSTable read-through rehydration.\n");
    out.push_str("# TYPE ferrosa_storage_sstable_rehydration_bytes_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_sstable_rehydration_bytes_total {}\n",
        SSTABLE_REHYDRATION_BYTES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_sstable_rehydration_seconds_total Total wall time spent in SSTable read-through rehydration.\n");
    out.push_str("# TYPE ferrosa_storage_sstable_rehydration_seconds_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_sstable_rehydration_seconds_total {:.9}\n",
        SSTABLE_REHYDRATION_SECONDS_MICROS_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_storage_sstable_rehydration_seconds_max Maximum observed wall time for one SSTable read-through rehydration.\n");
    out.push_str("# TYPE ferrosa_storage_sstable_rehydration_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_sstable_rehydration_seconds_max {:.9}\n",
        SSTABLE_REHYDRATION_SECONDS_MICROS_MAX.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_storage_sstable_rehydration_in_flight SSTable generations currently being restored by read-through rehydration.\n");
    out.push_str("# TYPE ferrosa_storage_sstable_rehydration_in_flight gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_sstable_rehydration_in_flight {}\n",
        SSTABLE_REHYDRATION_IN_FLIGHT.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_sstable_rehydration_in_flight_max Maximum concurrent SSTable read-through rehydrations.\n");
    out.push_str("# TYPE ferrosa_storage_sstable_rehydration_in_flight_max gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_sstable_rehydration_in_flight_max {}\n",
        SSTABLE_REHYDRATION_IN_FLIGHT_MAX.load(Ordering::Relaxed)
    ));

    out
}

/// Prometheus-compatible metrics for compaction S3 operations.
pub struct CompactionMetrics {
    /// Number of compacted SSTables successfully uploaded to S3.
    pub s3_uploads_total: AtomicI64,
    /// Number of input SSTables deleted from S3 after compaction.
    pub s3_deletes_total: AtomicI64,
    /// Total input bytes freed by completed compactions (gauge).
    pub input_bytes_reclaimed: AtomicI64,
}

impl CompactionMetrics {
    pub fn new() -> Self {
        Self {
            s3_uploads_total: AtomicI64::new(0),
            s3_deletes_total: AtomicI64::new(0),
            input_bytes_reclaimed: AtomicI64::new(0),
        }
    }

    /// Increments the S3 upload counter by 1.
    pub fn inc_s3_uploads(&self) {
        self.s3_uploads_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the S3 delete counter by 1.
    pub fn inc_s3_deletes(&self) {
        self.s3_deletes_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds `bytes` to the input bytes reclaimed gauge.
    pub fn add_bytes_reclaimed(&self, bytes: i64) {
        self.input_bytes_reclaimed
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Renders metrics in Prometheus exposition text format.
    pub fn to_prometheus_text(&self) -> String {
        format!(
            "# HELP ferrosa_compaction_s3_uploads_total Compacted SSTables uploaded to S3\n\
             # TYPE ferrosa_compaction_s3_uploads_total counter\n\
             ferrosa_compaction_s3_uploads_total {}\n\
             # HELP ferrosa_compaction_s3_deletes_total Input SSTables deleted from S3 after compaction\n\
             # TYPE ferrosa_compaction_s3_deletes_total counter\n\
             ferrosa_compaction_s3_deletes_total {}\n\
             # HELP ferrosa_compaction_input_bytes_reclaimed Total bytes freed by completed compactions\n\
             # TYPE ferrosa_compaction_input_bytes_reclaimed gauge\n\
             ferrosa_compaction_input_bytes_reclaimed {}\n",
            self.s3_uploads_total.load(Ordering::Relaxed),
            self.s3_deletes_total.load(Ordering::Relaxed),
            self.input_bytes_reclaimed.load(Ordering::Relaxed),
        )
    }
}

impl Default for CompactionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Prometheus-compatible metrics for PITR archiving and snapshots.
pub struct PitrMetrics {
    pub archive_segments_uploaded: AtomicI64,
    pub archive_upload_errors: AtomicI64,
    pub archive_lag_segments: AtomicI64,
    pub snapshots_total: AtomicI64,
}

impl PitrMetrics {
    pub fn new() -> Self {
        Self {
            archive_segments_uploaded: AtomicI64::new(0),
            archive_upload_errors: AtomicI64::new(0),
            archive_lag_segments: AtomicI64::new(0),
            snapshots_total: AtomicI64::new(0),
        }
    }

    pub fn inc_segments_uploaded(&self) {
        self.archive_segments_uploaded
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_upload_errors(&self) {
        self.archive_upload_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_archive_lag(&self, lag: i64) {
        self.archive_lag_segments.store(lag, Ordering::Relaxed);
    }

    pub fn set_snapshots_total(&self, count: i64) {
        self.snapshots_total.store(count, Ordering::Relaxed);
    }

    /// Renders metrics in Prometheus exposition text format.
    pub fn to_prometheus_text(&self) -> String {
        format!(
            "# HELP ferrosa_archive_segments_uploaded_total Total archived segments\n\
             # TYPE ferrosa_archive_segments_uploaded_total counter\n\
             ferrosa_archive_segments_uploaded_total {}\n\
             # HELP ferrosa_archive_upload_errors_total Total upload errors\n\
             # TYPE ferrosa_archive_upload_errors_total counter\n\
             ferrosa_archive_upload_errors_total {}\n\
             # HELP ferrosa_archive_lag_segments Current archive lag\n\
             # TYPE ferrosa_archive_lag_segments gauge\n\
             ferrosa_archive_lag_segments {}\n\
             # HELP ferrosa_snapshots_total Current snapshot count\n\
             # TYPE ferrosa_snapshots_total gauge\n\
             ferrosa_snapshots_total {}\n",
            self.archive_segments_uploaded.load(Ordering::Relaxed),
            self.archive_upload_errors.load(Ordering::Relaxed),
            self.archive_lag_segments.load(Ordering::Relaxed),
            self.snapshots_total.load(Ordering::Relaxed),
        )
    }
}

impl Default for PitrMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Prometheus-compatible metrics for NVMe pin/unpin operations.
pub struct PinMetrics {
    /// Number of tables currently pinned to NVMe (gauge).
    pub pinned_tables: AtomicI64,
    /// Total bytes occupied by pinned SSTables (gauge).
    pub pinned_bytes: AtomicI64,
    /// Total number of SSTable evictions caused by max_bytes enforcement (counter).
    pub pin_evictions_total: std::sync::atomic::AtomicU64,
}

impl PinMetrics {
    pub fn new() -> Self {
        Self {
            pinned_tables: AtomicI64::new(0),
            pinned_bytes: AtomicI64::new(0),
            pin_evictions_total: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Increments the pinned table gauge by 1.
    pub fn inc_pinned_tables(&self) {
        self.pinned_tables.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the pinned table gauge by 1.
    pub fn dec_pinned_tables(&self) {
        self.pinned_tables.fetch_sub(1, Ordering::Relaxed);
    }

    /// Adds `bytes` to the pinned bytes gauge.
    pub fn add_pinned_bytes(&self, bytes: i64) {
        self.pinned_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Subtracts `bytes` from the pinned bytes gauge.
    pub fn sub_pinned_bytes(&self, bytes: i64) {
        self.pinned_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Sets the pinned bytes gauge to an absolute value.
    pub fn set_pinned_bytes(&self, bytes: i64) {
        self.pinned_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Increments the pin eviction counter by 1.
    pub fn inc_pin_evictions(&self) {
        self.pin_evictions_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Renders metrics in Prometheus exposition text format.
    pub fn to_prometheus_text(&self) -> String {
        format!(
            "# HELP ferrosa_nvme_pinned_tables Number of tables pinned to NVMe\n\
             # TYPE ferrosa_nvme_pinned_tables gauge\n\
             ferrosa_nvme_pinned_tables {}\n\
             # HELP ferrosa_nvme_pinned_bytes Total bytes occupied by pinned SSTables\n\
             # TYPE ferrosa_nvme_pinned_bytes gauge\n\
             ferrosa_nvme_pinned_bytes {}\n\
             # HELP ferrosa_nvme_pin_evictions_total SSTables evicted by max_bytes enforcement\n\
             # TYPE ferrosa_nvme_pin_evictions_total counter\n\
             ferrosa_nvme_pin_evictions_total {}\n",
            self.pinned_tables.load(Ordering::Relaxed),
            self.pinned_bytes.load(Ordering::Relaxed),
            self.pin_evictions_total
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

impl Default for PinMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Atomic counters for flush and compaction operations.
///
/// Shared across the storage engine; incremented on each flush/compaction
/// and readable through the `system_observability.storage_stats` virtual table.
pub struct StorageOperationMetrics {
    /// Number of memtable flushes completed.
    pub flush_count: std::sync::atomic::AtomicU64,
    /// Number of compaction runs completed.
    pub compaction_count: std::sync::atomic::AtomicU64,
    /// Total bytes flushed to SSTables.
    pub bytes_flushed: std::sync::atomic::AtomicU64,
}

impl StorageOperationMetrics {
    pub fn new() -> Self {
        Self {
            flush_count: std::sync::atomic::AtomicU64::new(0),
            compaction_count: std::sync::atomic::AtomicU64::new(0),
            bytes_flushed: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Increment the flush counter by 1.
    pub fn inc_flush(&self) {
        self.flush_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Increment the compaction counter by 1.
    pub fn inc_compaction(&self) {
        self.compaction_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Add `bytes` to the total bytes flushed.
    pub fn add_bytes_flushed(&self, bytes: u64) {
        self.bytes_flushed
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Default for StorageOperationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_increment_and_render() {
        let m = PitrMetrics::new();
        m.inc_segments_uploaded();
        m.inc_segments_uploaded();
        m.inc_upload_errors();
        m.set_archive_lag(3);
        m.set_snapshots_total(5);
        let text = m.to_prometheus_text();
        assert!(text.contains("ferrosa_archive_segments_uploaded_total 2"));
        assert!(text.contains("ferrosa_archive_upload_errors_total 1"));
        assert!(text.contains("ferrosa_archive_lag_segments 3"));
        assert!(text.contains("ferrosa_snapshots_total 5"));
    }

    #[test]
    fn range_read_truncated_increment_and_read() {
        // Process-wide counter: assert on the delta, not an absolute value,
        // so the test is robust to other tests touching the same counter.
        let before = range_read_truncated_total();
        inc_range_read_truncated();
        inc_range_read_truncated();
        let after = range_read_truncated_total();
        assert_eq!(after - before, 2);

        // The counter is exported in the Prometheus text rendering.
        let text = render_prometheus();
        assert!(text.contains("ferrosa_storage_range_read_truncated_total"));
    }

    #[test]
    fn metrics_default_zero() {
        let m = PitrMetrics::new();
        let text = m.to_prometheus_text();
        assert!(text.contains("ferrosa_archive_segments_uploaded_total 0"));
    }

    #[test]
    fn metrics_thread_safe() {
        use std::sync::Arc;
        let m = Arc::new(PitrMetrics::new());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let m = Arc::clone(&m);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        m.inc_segments_uploaded();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.archive_segments_uploaded.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn pin_metrics_default_zero() {
        let m = PinMetrics::new();
        let text = m.to_prometheus_text();
        assert!(text.contains("ferrosa_nvme_pinned_tables 0"));
        assert!(text.contains("ferrosa_nvme_pinned_bytes 0"));
        assert!(text.contains("ferrosa_nvme_pin_evictions_total 0"));
    }

    #[test]
    fn pin_metrics_increment_and_render() {
        let m = PinMetrics::new();
        m.inc_pinned_tables();
        m.inc_pinned_tables();
        m.add_pinned_bytes(4096);
        m.inc_pin_evictions();
        let text = m.to_prometheus_text();
        assert!(text.contains("ferrosa_nvme_pinned_tables 2"));
        assert!(text.contains("ferrosa_nvme_pinned_bytes 4096"));
        assert!(text.contains("ferrosa_nvme_pin_evictions_total 1"));
    }

    #[test]
    fn pin_metrics_decrement() {
        let m = PinMetrics::new();
        m.inc_pinned_tables();
        m.add_pinned_bytes(2048);
        m.dec_pinned_tables();
        m.sub_pinned_bytes(2048);
        let text = m.to_prometheus_text();
        assert!(text.contains("ferrosa_nvme_pinned_tables 0"));
        assert!(text.contains("ferrosa_nvme_pinned_bytes 0"));
    }

    #[test]
    fn pin_metrics_set_pinned_bytes() {
        let m = PinMetrics::new();
        m.set_pinned_bytes(99999);
        let text = m.to_prometheus_text();
        assert!(text.contains("ferrosa_nvme_pinned_bytes 99999"));
    }

    #[test]
    fn storage_operation_metrics_increment() {
        let m = StorageOperationMetrics::new();
        assert_eq!(m.flush_count.load(std::sync::atomic::Ordering::Relaxed), 0);

        m.inc_flush();
        m.inc_flush();
        m.inc_compaction();
        m.add_bytes_flushed(4096);
        m.add_bytes_flushed(2048);

        assert_eq!(m.flush_count.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(
            m.compaction_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            m.bytes_flushed.load(std::sync::atomic::Ordering::Relaxed),
            6144
        );
    }

    #[test]
    fn storage_flush_and_upload_metrics_render() {
        observe_flush_phase(FlushPhase::EncodeSstable, Duration::from_micros(250));
        observe_flush_output(1024, 10, 2);
        observe_upload_phase(UploadPhase::SyncAwait, Duration::from_micros(500));
        observe_upload_file(2048, Duration::from_micros(750));
        set_memtable_thresholds(4096, 16384);
        observe_memtable_size(8192);

        let text = render_prometheus();
        assert!(text.contains("ferrosa_storage_flushes_total"));
        assert!(
            text.contains("ferrosa_storage_flush_phase_seconds_total{phase=\"encode_sstable\"}")
        );
        assert!(text.contains("ferrosa_storage_flush_phase_total{phase=\"local_write_sstable\"}"));
        assert!(text.contains("ferrosa_storage_upload_phase_seconds_total{phase=\"sync_await\"}"));
        assert!(text.contains("ferrosa_storage_upload_phase_total{phase=\"file_put\"}"));
        assert!(text.contains("ferrosa_storage_upload_bytes_total"));
        // These are global process gauges; other tests running in parallel may
        // observe larger values before this scrape. This test only verifies
        // that the metrics are rendered.
        assert!(text.contains("ferrosa_storage_memtable_size_bytes_max"));
        assert!(text.contains("ferrosa_storage_memtable_flush_threshold_bytes"));
        assert!(text.contains("ferrosa_storage_memtable_backpressure_bytes"));
    }
}
