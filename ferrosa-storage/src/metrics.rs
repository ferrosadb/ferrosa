//! Module: Record and render bounded process-wide storage telemetry.
//! Correctness: Correct when counters are monotonic, gauges reflect complete
//! operations, and observation never allocates in storage hot paths.
//! Last revised: 2026-09-26
//! Last changed: Export a bounded gauge for tables paused after digest failures.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
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

/// Why `FileFlushTarget::flush_files` refused to publish a staged SSTable
/// generation (`publication-safety.md` M2). Every reason ends the same way:
/// the `.tmp` component set is moved to `quarantine/` instead of being
/// promoted to a live name, so a startup scan never has to reason about it.
#[derive(Clone, Copy)]
pub enum PublicationRefusedReason {
    /// A staged `.tmp` component's on-disk length disagreed with the length
    /// the writer recorded for it.
    LengthMismatch,
    /// A staged `.tmp` component could not be fsynced durable before verify.
    Fsync,
    /// The recomputed `Digest.crc32` over the staged `.tmp` Data.db (read back
    /// from disk) disagreed with the producer's value (`publication-safety.md`
    /// M2 step 4 / M3, T-012). Runs unconditionally in flush and compaction —
    /// there is no environment variable that disables it.
    DigestMismatch,
    /// The pre-promote readback walk over the `.tmp` component set failed.
    ReadbackFailed,
}

impl PublicationRefusedReason {
    pub fn label(self) -> &'static str {
        match self {
            Self::LengthMismatch => "length_mismatch",
            Self::Fsync => "fsync",
            Self::DigestMismatch => "digest_mismatch",
            Self::ReadbackFailed => "readback_failed",
        }
    }

    fn idx(self) -> usize {
        match self {
            Self::LengthMismatch => 0,
            Self::Fsync => 1,
            Self::DigestMismatch => 2,
            Self::ReadbackFailed => 3,
        }
    }
}

const PUBLICATION_REFUSED_REASONS: [PublicationRefusedReason; 4] = [
    PublicationRefusedReason::LengthMismatch,
    PublicationRefusedReason::Fsync,
    PublicationRefusedReason::DigestMismatch,
    PublicationRefusedReason::ReadbackFailed,
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
static WRITE_ADMISSION_DELAYED_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_DELAY_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_DELAY_COUNT: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_DELAY_BUCKETS: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];
static WRITE_ADMISSION_REJECTED_HARD_MEMTABLE: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_REJECTED_HARD_FLUSH_LAG: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_PRESSURE_BY_TABLE: OnceLock<Mutex<HashMap<String, Vec<Weak<AtomicU64>>>>> =
    OnceLock::new();
const WRITE_ADMISSION_DELAY_BUCKET_MS: [u64; 6] = [1, 5, 10, 25, 50, 1_000];

/// Registers a table's pressure gauge. Registration happens once per table,
/// while per-write updates use only the gauge's atomic value.
pub fn register_write_admission_pressure(label: String, gauge: &Arc<AtomicU64>) {
    let registry = WRITE_ADMISSION_PRESSURE_BY_TABLE.get_or_init(Default::default);
    registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(label)
        .or_default()
        .push(Arc::downgrade(gauge));
}

/// Records one eviction audit pass. `written` is whether the durable record
/// was written; the gauges reflect the pass either way, so the latest decision
/// is visible even while the audit files are unwritable.
pub fn observe_eviction_audit(record: &crate::eviction_audit::PassRecord, written: bool) {
    EVICTION_AUDIT_PASSES_TOTAL.fetch_add(record.count, Ordering::Relaxed);
    if !written {
        EVICTION_AUDIT_WRITE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    EVICTION_AUDIT_LAST_UNIX_MS.store(record.last_unix_ms, Ordering::Relaxed);
    EVICTION_AUDIT_LAST_TRIGGER.store(record.trigger as u64, Ordering::Relaxed);
    EVICTION_AUDIT_LAST_EVICTED_GENERATIONS.store(record.evicted_generations, Ordering::Relaxed);
    EVICTION_AUDIT_LAST_EVICTED_BYTES.store(record.evicted_bytes, Ordering::Relaxed);
    EVICTION_AUDIT_LAST_MANIFEST_BYTES.store(record.manifest_bytes, Ordering::Relaxed);
    EVICTION_AUDIT_LAST_DISK_BYTES.store(record.disk_bytes, Ordering::Relaxed);
}

/// Records the outcome of one audit offload attempt.
pub fn observe_eviction_audit_offload(uploaded: bool, failed: bool) {
    if uploaded {
        EVICTION_AUDIT_OFFLOAD_UPLOADED_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    if failed {
        EVICTION_AUDIT_OFFLOAD_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn inc_write_admission_delayed() {
    WRITE_ADMISSION_DELAYED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn observe_write_admission_delay(duration: Duration) {
    let micros = duration.as_micros().min(u64::MAX as u128) as u64;
    WRITE_ADMISSION_DELAY_MICROS_TOTAL.fetch_add(micros, Ordering::Relaxed);
    WRITE_ADMISSION_DELAY_COUNT.fetch_add(1, Ordering::Relaxed);
    let millis = duration.as_millis().min(u64::MAX as u128) as u64;
    for (index, bound) in WRITE_ADMISSION_DELAY_BUCKET_MS.iter().enumerate() {
        if millis <= *bound {
            WRITE_ADMISSION_DELAY_BUCKETS[index].fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn inc_write_admission_rejected(reason: &'static str) {
    match reason {
        "hard_memtable" => {
            WRITE_ADMISSION_REJECTED_HARD_MEMTABLE.fetch_add(1, Ordering::Relaxed);
        }
        "hard_flush_lag" => {
            WRITE_ADMISSION_REJECTED_HARD_FLUSH_LAG.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

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

/// Planning rounds skipped because the compaction pipeline was already at or
/// above `FERROSA_COMPACTION_BACKPRESSURE_PRESSURE`. A steadily rising counter
/// is the signal that the pipeline is the bottleneck, not the planner: it is
/// this number of full planning rounds (select + per-task metadata rescan) that
/// were not paid. A zero value on a busy node means the gate never fired.
static COMPACTION_PLANNING_DEFERRED_TOTAL: AtomicU64 = AtomicU64::new(0);

pub fn inc_compaction_planning_deferred() {
    COMPACTION_PLANNING_DEFERRED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn compaction_planning_deferred_total() -> u64 {
    COMPACTION_PLANNING_DEFERRED_TOTAL.load(Ordering::Relaxed)
}
static COMPACTION_PAUSED_TABLES: AtomicU64 = AtomicU64::new(0);
/// Compaction tasks that returned `Err` because their `CancelToken` was
/// cancelled (T-021), as distinct from an ordinary failure.
static COMPACTION_RETIRE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_CANCELLED_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Sum, count and max of the latency from `CancelToken::cancel()` to the
/// checkpoint that observed it and returned `Err` (T-021,
/// `compaction_cancel_latency_seconds`).
static COMPACTION_CANCEL_LATENCY_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static COMPACTION_CANCEL_LATENCY_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static COMPACTION_CANCEL_LATENCY_COUNT: AtomicU64 = AtomicU64::new(0);
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
/// Deletion markers (partition / row / cell tombstones) compaction dropped because
/// they were past `gc_grace_seconds` and provably shadowed nothing outside it.
static COMPACTION_PURGED_MARKERS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Compactions whose every partition purged away; one was written anyway.
static COMPACTION_PURGE_HELD_BACK_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Compactions that ran without purging because the table's `gc_grace_seconds`
/// could not be read. Non-zero means a table option is corrupt; alert on it.
static COMPACTION_PURGE_POLICY_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Compactions rolled back after their replacement record committed but before
/// retirement (reader-open, sidecar-merge, or swap failure) -- T-022.
static COMPACTION_INTENT_ROLLBACK_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Startup reconciliations that rolled a `Promoting` record back because its
/// output was never promoted -- T-023.
static COMPACTION_RECONCILE_ROLLED_BACK_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Startup reconciliations that rolled a record forward, retiring whichever of
/// its listed inputs a crash had left live -- T-023 (the resurrection fix).
static COMPACTION_RECONCILE_ROLLED_FORWARD_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Startup reconciliations that found a promoted output whose `Digest.crc32`
/// did not match the record and quarantined it. Non-zero means promoted bytes
/// were corrupted after the fact; alert on it.
static COMPACTION_RECONCILE_DIGEST_MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Startup reconciliations that found a `.compaction-*.intent` file present
/// but unparseable. Left in place for operator inspection; alert on it.
static COMPACTION_RECONCILE_UNREADABLE_RECORD_TOTAL: AtomicU64 = AtomicU64::new(0);

static WRITE_PHASE_MICROS_TOTAL: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
static WRITE_PHASE_MICROS_MAX: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
static WRITE_PHASE_COUNT_TOTAL: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
static WRITE_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_FAILURE_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_FAILURE_REASON_TOTAL: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];
static SSTABLE_PUBLICATION_REFUSED_TOTAL: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static WRITE_INLINE_FLUSH_TOTAL: AtomicU64 = AtomicU64::new(0);

// Latest eviction-pass audit record (see `eviction_audit`), readable without
// touching the audit files. Gauges hold the most recent recorded pass.
static EVICTION_AUDIT_PASSES_TOTAL: AtomicU64 = AtomicU64::new(0);
static EVICTION_AUDIT_WRITE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static EVICTION_AUDIT_OFFLOAD_UPLOADED_TOTAL: AtomicU64 = AtomicU64::new(0);
static EVICTION_AUDIT_OFFLOAD_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static EVICTION_AUDIT_LAST_UNIX_MS: AtomicU64 = AtomicU64::new(0);
static EVICTION_AUDIT_LAST_TRIGGER: AtomicU64 = AtomicU64::new(0);
static EVICTION_AUDIT_LAST_EVICTED_GENERATIONS: AtomicU64 = AtomicU64::new(0);
static EVICTION_AUDIT_LAST_EVICTED_BYTES: AtomicU64 = AtomicU64::new(0);
static EVICTION_AUDIT_LAST_MANIFEST_BYTES: AtomicU64 = AtomicU64::new(0);
static EVICTION_AUDIT_LAST_DISK_BYTES: AtomicU64 = AtomicU64::new(0);
static MEMTABLE_SIZE_BYTES_MAX: AtomicU64 = AtomicU64::new(0);
static MEMTABLE_FLUSH_THRESHOLD_BYTES: AtomicU64 = AtomicU64::new(0);
static MEMTABLE_BACKPRESSURE_BYTES: AtomicU64 = AtomicU64::new(0);

static RANGE_READ_TRUNCATED_TOTAL: AtomicU64 = AtomicU64::new(0);
static INDEX_RELOAD_SKIPPED_ROWS_TOTAL: AtomicU64 = AtomicU64::new(0);
static INDEX_BACKFILL_BUILD_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static INDEX_BACKFILL_RETRIES_TOTAL: AtomicU64 = AtomicU64::new(0);
static VECTOR_GENERATIONS_REPAIRED_TOTAL: AtomicU64 = AtomicU64::new(0);
static VECTOR_GENERATION_REPAIR_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static VECTOR_GENERATIONS_PENDING: AtomicI64 = AtomicI64::new(0);
static INDEX_SIDECAR_MAPPED_BYTES: AtomicI64 = AtomicI64::new(0);
static INDEX_SIDECAR_MAPPED_FILES: AtomicI64 = AtomicI64::new(0);
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
static OBJECT_STORE_DOWNLOAD_PROGRESS_BYTES: AtomicU64 = AtomicU64::new(0);

/// Record bytes an object-store download has written locally, as they land
/// (per streamed chunk or ranged part), not when the whole file completes.
pub fn add_object_store_download_progress(bytes: u64) {
    OBJECT_STORE_DOWNLOAD_PROGRESS_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Monotonic count of downloaded bytes on this node: an I/O PROGRESS signal.
///
/// A storage walk that is waiting on a rehydrate yields no rows, yet it is
/// making progress while this advances. Streaming responders use it to decide
/// whether a heartbeat is honest (cluster `handle_stream_request`). It is
/// node-wide, so it can over-report progress for one walk while another walk
/// downloads; it never under-reports one that is downloading.
pub fn object_store_download_progress() -> u64 {
    OBJECT_STORE_DOWNLOAD_PROGRESS_BYTES.load(Ordering::Relaxed)
}

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

static UPLOAD_TASK_PANICS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// An upload, delete or index task panicked; its worker survived it.
pub fn inc_upload_task_panics() {
    UPLOAD_TASK_PANICS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn upload_task_panics_total() -> u64 {
    UPLOAD_TASK_PANICS_TOTAL.load(Ordering::Relaxed)
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

static COMPACTION_PANICS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// A compaction task panicked; it was failed and its input claims released.
pub fn inc_compaction_panics() {
    COMPACTION_PANICS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn compaction_panics_total() -> u64 {
    COMPACTION_PANICS_TOTAL.load(Ordering::Relaxed)
}

pub fn inc_compaction_failed() {
    COMPACTION_FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Set the process-wide count of tables paused after digest verification failures.
pub fn inc_compaction_paused_tables() {
    COMPACTION_PAUSED_TABLES.fetch_add(1, Ordering::Relaxed);
}

/// Decrement the process-wide count when a digest pause guard is released.
pub fn dec_compaction_paused_tables() {
    let _ = COMPACTION_PAUSED_TABLES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
        Some(count.saturating_sub(1))
    });
}

/// Record a compaction input reader obtained through the engine-wide reader
/// pool (FMEA #11). Called once per input SSTable per task when the executor is
/// pool-routed.
/// Records a compaction task cancelled rather than failed (T-021).
pub fn inc_compaction_cancelled() {
    COMPACTION_CANCELLED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Records one failed retirement operation; its intent remains available for replay.
pub fn inc_compaction_retire_failures() {
    COMPACTION_RETIRE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Records the latency from `CancelToken::cancel()` to the checkpoint that
/// observed it (`compaction_cancel_latency_seconds`).
pub fn observe_compaction_cancel_latency(duration: Duration) {
    let micros = duration_micros(duration);
    COMPACTION_CANCEL_LATENCY_MICROS_TOTAL.fetch_add(micros, Ordering::Relaxed);
    COMPACTION_CANCEL_LATENCY_COUNT.fetch_add(1, Ordering::Relaxed);
    update_max_u64(&COMPACTION_CANCEL_LATENCY_MICROS_MAX, micros);
}

#[cfg(test)]
pub fn compaction_cancelled_total() -> u64 {
    COMPACTION_CANCELLED_TOTAL.load(Ordering::Relaxed)
}

#[cfg(test)]
pub fn compaction_cancel_latency_count() -> u64 {
    COMPACTION_CANCEL_LATENCY_COUNT.load(Ordering::Relaxed)
}

pub fn inc_compaction_pool_input_opens() {
    COMPACTION_POOL_INPUT_OPENS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Record deletion markers dropped from a compaction's output under
/// `gc_grace_seconds`.
pub fn add_compaction_purged_markers(n: u64) {
    COMPACTION_PURGED_MARKERS_TOTAL.fetch_add(n, Ordering::Relaxed);
}

/// Record a compaction that skipped purging because `gc_grace_seconds` was unreadable.
pub fn inc_compaction_purge_policy_errors() {
    COMPACTION_PURGE_POLICY_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Compactions that skipped purging on an unreadable `gc_grace_seconds`.
pub fn compaction_purge_policy_errors_total() -> u64 {
    COMPACTION_PURGE_POLICY_ERRORS_TOTAL.load(Ordering::Relaxed)
}

/// Total deletion markers dropped by compaction since startup.
pub fn compaction_purged_markers_total() -> u64 {
    COMPACTION_PURGED_MARKERS_TOTAL.load(Ordering::Relaxed)
}

/// Record a fully-purged partition written anyway because it was the only thing
/// the compaction had left (an empty output cannot be swapped in).
pub fn inc_compaction_purge_held_back() {
    COMPACTION_PURGE_HELD_BACK_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Times a compaction wrote a fully-purged partition because nothing else survived.
pub fn compaction_purge_held_back_total() -> u64 {
    COMPACTION_PURGE_HELD_BACK_TOTAL.load(Ordering::Relaxed)
}

/// Record a compaction rolled back after its replacement record committed
/// (T-022): the promoted directory was removed, the record deleted, and the
/// inputs left untouched.
pub fn inc_compaction_intent_rollback() {
    COMPACTION_INTENT_ROLLBACK_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Total post-commit compaction rollbacks since startup.
pub fn compaction_intent_rollback_total() -> u64 {
    COMPACTION_INTENT_ROLLBACK_TOTAL.load(Ordering::Relaxed)
}

/// Record a startup reconciliation that rolled a `Promoting` record back
/// (T-023): its output was never promoted, so the record was deleted and its
/// inputs left live.
pub fn inc_compaction_reconcile_rolled_back() {
    COMPACTION_RECONCILE_ROLLED_BACK_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Total startup roll-backs since startup.
pub fn compaction_reconcile_rolled_back_total() -> u64 {
    COMPACTION_RECONCILE_ROLLED_BACK_TOTAL.load(Ordering::Relaxed)
}

/// Record a startup reconciliation that rolled a record forward: the output
/// matched its digest, so every input still on disk was retired.
pub fn inc_compaction_reconcile_rolled_forward() {
    COMPACTION_RECONCILE_ROLLED_FORWARD_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Total startup roll-forwards since startup -- the resurrection fix firing.
pub fn compaction_reconcile_rolled_forward_total() -> u64 {
    COMPACTION_RECONCILE_ROLLED_FORWARD_TOTAL.load(Ordering::Relaxed)
}

/// Record a startup reconciliation that quarantined a promoted output whose
/// `Digest.crc32` did not match its record.
pub fn inc_compaction_reconcile_digest_mismatch() {
    COMPACTION_RECONCILE_DIGEST_MISMATCH_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Total digest-mismatch quarantines found by startup reconciliation.
pub fn compaction_reconcile_digest_mismatch_total() -> u64 {
    COMPACTION_RECONCILE_DIGEST_MISMATCH_TOTAL.load(Ordering::Relaxed)
}

/// Record a startup reconciliation that found an unparseable
/// `.compaction-*.intent` file and left it in place for operator inspection.
pub fn inc_compaction_reconcile_unreadable_record() {
    COMPACTION_RECONCILE_UNREADABLE_RECORD_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Total unreadable compaction intent records found by startup reconciliation.
pub fn compaction_reconcile_unreadable_record_total() -> u64 {
    COMPACTION_RECONCILE_UNREADABLE_RECORD_TOTAL.load(Ordering::Relaxed)
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

/// A generation's vector sidecars were rebuilt from its rows by the vector
/// repair (FMEA ST-79).
pub fn vector_generation_repaired() {
    VECTOR_GENERATIONS_REPAIRED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// A vector repair of one generation failed; ANN over its index keeps
/// refusing until a later repair succeeds.
pub fn vector_generation_repair_failed() {
    VECTOR_GENERATION_REPAIR_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Generations rebuilt by the vector repair since startup.
pub fn vector_generations_repaired_total() -> u64 {
    VECTOR_GENERATIONS_REPAIRED_TOTAL.load(Ordering::Relaxed)
}

/// Generations a vector repair run found incomplete and has yet to finish
/// (`delta` positive when a run starts, negative as it settles each one).
pub fn add_vector_generations_pending(delta: i64) {
    VECTOR_GENERATIONS_PENDING.fetch_add(delta, Ordering::Relaxed);
}

/// Generations awaiting a vector rebuild right now; ANN over their index
/// refuses while this is non-zero for it.
pub fn vector_generations_pending() -> i64 {
    VECTOR_GENERATIONS_PENDING.load(Ordering::Relaxed)
}

/// `ferrosa_index_repairs_total{index,reason}`: index generations rebuilt
/// (or scope sets restored) by reason. Labelled, so a lock-free map.
static INDEX_REPAIRS_TOTAL: OnceLock<
    arc_swap::ArcSwap<std::collections::BTreeMap<(String, String), u64>>,
> = OnceLock::new();
/// `ferrosa_index_invalid{table,index}`: generations of an index currently
/// incomplete or invalid; ANN over it refuses while non-zero.
static INDEX_INVALID: OnceLock<
    arc_swap::ArcSwap<std::collections::BTreeMap<(String, String), u64>>,
> = OnceLock::new();

fn labelled(
    cell: &'static OnceLock<arc_swap::ArcSwap<std::collections::BTreeMap<(String, String), u64>>>,
) -> &'static arc_swap::ArcSwap<std::collections::BTreeMap<(String, String), u64>> {
    cell.get_or_init(|| arc_swap::ArcSwap::from_pointee(std::collections::BTreeMap::new()))
}

/// One index repair for `reason` (a rebuilt generation or a restored scope
/// set).
pub fn index_repaired(index: &str, reason: &str) {
    let key = (index.to_string(), reason.to_string());
    let counted = crate::lockfree::update(
        labelled(&INDEX_REPAIRS_TOTAL),
        "index repairs metric",
        |map| {
            let mut next = map.clone();
            *next.entry(key.clone()).or_default() += 1;
            (Some(next), ())
        },
    );
    if let Err(e) = counted {
        tracing::error!(%e, index, reason, "ferrosa_index_repairs_total not incremented");
    }
}

/// Repairs of `index` for `reason` since startup.
pub fn index_repairs_total(index: &str, reason: &str) -> u64 {
    labelled(&INDEX_REPAIRS_TOTAL)
        .load()
        .get(&(index.to_string(), reason.to_string()))
        .copied()
        .unwrap_or(0)
}

/// Set how many generations of `table`'s `index` are invalid right now.
pub fn set_index_invalid(table: &str, index: &str, generations: u64) {
    let key = (table.to_string(), index.to_string());
    let set = crate::lockfree::update(labelled(&INDEX_INVALID), "index invalid gauge", |map| {
        if map.get(&key).copied().unwrap_or(0) == generations {
            return (None, ());
        }
        let mut next = map.clone();
        if generations == 0 {
            next.remove(&key);
        } else {
            next.insert(key.clone(), generations);
        }
        (Some(next), ())
    });
    if let Err(e) = set {
        tracing::error!(%e, table, index, "ferrosa_index_invalid not updated");
    }
}

/// Generations of `table`'s `index` invalid as last observed.
pub fn index_invalid(table: &str, index: &str) -> u64 {
    labelled(&INDEX_INVALID)
        .load()
        .get(&(table.to_string(), index.to_string()))
        .copied()
        .unwrap_or(0)
}

/// A scalar index sidecar of `bytes` was memory-mapped (t_7ac6b0e3).
pub fn index_sidecar_mapped(bytes: u64) {
    INDEX_SIDECAR_MAPPED_BYTES.fetch_add(bytes as i64, Ordering::Relaxed);
    INDEX_SIDECAR_MAPPED_FILES.fetch_add(1, Ordering::Relaxed);
}

/// A mapped sidecar of `bytes` was unmapped (its last reader dropped).
pub fn index_sidecar_unmapped(bytes: u64) {
    INDEX_SIDECAR_MAPPED_BYTES.fetch_sub(bytes as i64, Ordering::Relaxed);
    INDEX_SIDECAR_MAPPED_FILES.fetch_sub(1, Ordering::Relaxed);
}

/// Bytes of scalar index sidecars currently memory-mapped. Mapped pages are
/// file-backed page cache the kernel reclaims under pressure, not heap.
pub fn index_sidecar_mapped_bytes() -> i64 {
    INDEX_SIDECAR_MAPPED_BYTES.load(Ordering::Relaxed)
}

/// Scalar index sidecar files currently memory-mapped.
pub fn index_sidecar_mapped_files() -> i64 {
    INDEX_SIDECAR_MAPPED_FILES.load(Ordering::Relaxed)
}

/// `ferrosa_index_not_current{table,index}`: generations of a secondary index
/// still pending a backfill; reads through the index are refused while it is
/// listed. `ferrosa_index_backfill_failed{table,index}`: 1 while its last
/// build failed and awaits a retry. Both are replaced whole by
/// [`set_index_backfill_status`].
static INDEX_NOT_CURRENT: OnceLock<
    arc_swap::ArcSwap<std::collections::BTreeMap<(String, String), u64>>,
> = OnceLock::new();
static INDEX_BACKFILL_FAILED: OnceLock<
    arc_swap::ArcSwap<std::collections::BTreeMap<(String, String), u64>>,
> = OnceLock::new();

/// One secondary index that is not current, as the backfill gauges report it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexBackfillGauge {
    /// `keyspace.table`.
    pub table: String,
    /// The index.
    pub index: String,
    /// Generations pending a backfill.
    pub pending_generations: u64,
    /// Whether its last build failed and awaits a retry.
    pub failed: bool,
}

/// Publish the not-current secondary indexes, replacing the previous set: an
/// index absent from `indexes` is current and leaves both gauges.
pub fn set_index_backfill_status(indexes: &[IndexBackfillGauge]) {
    let key = |g: &IndexBackfillGauge| (g.table.clone(), g.index.clone());
    let not_current = indexes
        .iter()
        .map(|g| (key(g), g.pending_generations))
        .collect();
    let failed = indexes
        .iter()
        .filter(|g| g.failed)
        .map(|g| (key(g), 1))
        .collect();
    labelled(&INDEX_NOT_CURRENT).store(std::sync::Arc::new(not_current));
    labelled(&INDEX_BACKFILL_FAILED).store(std::sync::Arc::new(failed));
}

/// Pending generations of `table`'s `index` as last published (0 = current).
pub fn index_not_current(table: &str, index: &str) -> u64 {
    labelled(&INDEX_NOT_CURRENT)
        .load()
        .get(&(table.to_string(), index.to_string()))
        .copied()
        .unwrap_or(0)
}

/// Whether `table`'s `index` was last published as failed.
pub fn index_backfill_failed(table: &str, index: &str) -> bool {
    labelled(&INDEX_BACKFILL_FAILED)
        .load()
        .contains_key(&(table.to_string(), index.to_string()))
}

/// One secondary-index backfill build failed.
pub fn index_backfill_build_failed() {
    INDEX_BACKFILL_BUILD_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Secondary-index backfill builds that failed since startup.
pub fn index_backfill_build_failures_total() -> u64 {
    INDEX_BACKFILL_BUILD_FAILURES_TOTAL.load(Ordering::Relaxed)
}

/// `generations` pending backfills were resubmitted by the healer.
pub fn index_backfill_retried(generations: u64) {
    INDEX_BACKFILL_RETRIES_TOTAL.fetch_add(generations, Ordering::Relaxed);
}

/// Generations the healer resubmitted since startup.
pub fn index_backfill_retries_total() -> u64 {
    INDEX_BACKFILL_RETRIES_TOTAL.load(Ordering::Relaxed)
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

/// Whole-value collection cells expanded into elements at the SSTable writer
/// boundary, per table (FMEA ST-66). Bounded by the number of tables.
static COLLECTION_BLOB_EXPANSIONS: OnceLock<dashmap::DashMap<String, AtomicU64>> = OnceLock::new();
/// (table, column) pairs that have already logged their first-expansion WARN.
static COLLECTION_BLOB_EXPANSION_WARNED: OnceLock<dashmap::DashSet<(String, String)>> =
    OnceLock::new();

/// Add `n` whole-value collection expansions for `table`.
pub fn add_collection_blob_expansions(table: &str, n: u64) {
    let map = COLLECTION_BLOB_EXPANSIONS.get_or_init(dashmap::DashMap::new);
    if let Some(counter) = map.get(table) {
        counter.fetch_add(n, Ordering::Relaxed);
        return;
    }
    map.entry(table.to_string())
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_add(n, Ordering::Relaxed);
}

/// Whole-value collection expansions recorded for `table` so far.
pub fn collection_blob_expansions_total(table: &str) -> u64 {
    COLLECTION_BLOB_EXPANSIONS
        .get()
        .and_then(|map| map.get(table).map(|c| c.load(Ordering::Relaxed)))
        .unwrap_or(0)
}

/// True exactly once per (table, column) in this process: the WARN edge.
pub fn first_collection_blob_expansion(table: &str, column: &str) -> bool {
    COLLECTION_BLOB_EXPANSION_WARNED
        .get_or_init(dashmap::DashSet::new)
        .insert((table.to_string(), column.to_string()))
}

pub fn inc_write_inline_flush() {
    WRITE_INLINE_FLUSH_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// A staged SSTable generation was refused publication and quarantined
/// instead of promoted (`publication-safety.md` M2, FMEA F13/ST-27).
pub fn inc_sstable_publication_refused(reason: PublicationRefusedReason) {
    SSTABLE_PUBLICATION_REFUSED_TOTAL[reason.idx()].fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
pub fn sstable_publication_refused_total(reason: PublicationRefusedReason) -> u64 {
    SSTABLE_PUBLICATION_REFUSED_TOTAL[reason.idx()].load(Ordering::Relaxed)
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
    out.push_str("# HELP ferrosa_storage_sstable_publication_refused_total Staged SSTable generations refused publication and quarantined instead of promoted.\n");
    out.push_str("# TYPE ferrosa_storage_sstable_publication_refused_total counter\n");
    for reason in PUBLICATION_REFUSED_REASONS {
        out.push_str(&format!(
            "ferrosa_storage_sstable_publication_refused_total{{reason=\"{}\"}} {}\n",
            reason.label(),
            SSTABLE_PUBLICATION_REFUSED_TOTAL[reason.idx()].load(Ordering::Relaxed)
        ));
    }
    for (name, kind, help, value) in [
        (
            "passes_total",
            "counter",
            "Eviction passes with pressure that were audited.",
            &EVICTION_AUDIT_PASSES_TOTAL,
        ),
        (
            "write_failures_total",
            "counter",
            "Eviction audit records that could not be written.",
            &EVICTION_AUDIT_WRITE_FAILURES_TOTAL,
        ),
        (
            "offload_uploaded_total",
            "counter",
            "Audit segments uploaded to S3 and removed locally.",
            &EVICTION_AUDIT_OFFLOAD_UPLOADED_TOTAL,
        ),
        (
            "offload_failures_total",
            "counter",
            "Audit segment offload attempts that failed.",
            &EVICTION_AUDIT_OFFLOAD_FAILURES_TOTAL,
        ),
        (
            "last_unix_ms",
            "gauge",
            "Time of the latest audited eviction pass.",
            &EVICTION_AUDIT_LAST_UNIX_MS,
        ),
        (
            "last_trigger",
            "gauge",
            "Trigger of the latest audited pass: 0 cache_cap, 1 free_space, 2 both, 3 recovered.",
            &EVICTION_AUDIT_LAST_TRIGGER,
        ),
        (
            "last_evicted_generations",
            "gauge",
            "Generations evicted by the latest audited pass.",
            &EVICTION_AUDIT_LAST_EVICTED_GENERATIONS,
        ),
        (
            "last_evicted_bytes",
            "gauge",
            "Bytes evicted by the latest audited pass.",
            &EVICTION_AUDIT_LAST_EVICTED_BYTES,
        ),
        (
            "last_manifest_bytes",
            "gauge",
            "Bytes the manifest claimed for the eviction candidates in the latest audited pass.",
            &EVICTION_AUDIT_LAST_MANIFEST_BYTES,
        ),
        (
            "last_disk_bytes",
            "gauge",
            "Bytes those candidates occupied on disk in the latest audited pass.",
            &EVICTION_AUDIT_LAST_DISK_BYTES,
        ),
    ] {
        out.push_str(&format!(
            "# HELP ferrosa_storage_eviction_audit_{name} {help}\n# TYPE ferrosa_storage_eviction_audit_{name} {kind}\nferrosa_storage_eviction_audit_{name} {}\n",
            value.load(Ordering::Relaxed)
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
    out.push_str("# HELP ferrosa_storage_write_admission_delayed_total CQL writes delayed in the soft-pressure zone.\n");
    out.push_str("# TYPE ferrosa_storage_write_admission_delayed_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_write_admission_delayed_total {}\n",
        WRITE_ADMISSION_DELAYED_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_write_admission_delay_seconds Soft-pressure wait duration.\n",
    );
    out.push_str("# TYPE ferrosa_storage_write_admission_delay_seconds histogram\n");
    for (index, bound) in WRITE_ADMISSION_DELAY_BUCKET_MS.iter().enumerate() {
        out.push_str(&format!(
            "ferrosa_storage_write_admission_delay_seconds_bucket{{le=\"{}\"}} {}\n",
            *bound as f64 / 1_000.0,
            WRITE_ADMISSION_DELAY_BUCKETS[index].load(Ordering::Relaxed)
        ));
    }
    out.push_str(&format!(
        "ferrosa_storage_write_admission_delay_seconds_bucket{{le=\"+Inf\"}} {}\n",
        WRITE_ADMISSION_DELAY_COUNT.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "ferrosa_storage_write_admission_delay_seconds_sum {:.6}\nferrosa_storage_write_admission_delay_seconds_count {}\n",
        WRITE_ADMISSION_DELAY_MICROS_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        WRITE_ADMISSION_DELAY_COUNT.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_write_admission_rejected_total Writes rejected at hard pressure by reason.\n");
    out.push_str("# TYPE ferrosa_storage_write_admission_rejected_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_write_admission_rejected_total{{reason=\"hard_memtable\"}} {}\nferrosa_storage_write_admission_rejected_total{{reason=\"hard_flush_lag\"}} {}\n",
        WRITE_ADMISSION_REJECTED_HARD_MEMTABLE.load(Ordering::Relaxed),
        WRITE_ADMISSION_REJECTED_HARD_FLUSH_LAG.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_write_pressure_ratio Per-table maximum of memtable and write-pump pressure, quantized to percentage points.\n");
    out.push_str("# TYPE ferrosa_storage_write_pressure_ratio gauge\n");
    if let Some(registry) = WRITE_ADMISSION_PRESSURE_BY_TABLE.get() {
        let mut registry = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.retain(|label, gauges| {
            gauges.retain(|gauge| gauge.strong_count() > 0);
            if !gauges.is_empty() {
                let value = gauges
                    .iter()
                    .filter_map(Weak::upgrade)
                    .map(|gauge| gauge.load(Ordering::Relaxed))
                    .max()
                    .unwrap_or(0);
                let escaped = label
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n");
                out.push_str(&format!(
                    "ferrosa_storage_write_pressure_ratio{{table=\"{escaped}\"}} {:.2}\n",
                    value as f64 / 100.0
                ));
            }
            !gauges.is_empty()
        });
    }
    out.push_str("# HELP ferrosa_storage_collection_blob_expansions_total Whole-value collection cells rewritten into element cells for a complex-framed SSTable.\n");
    out.push_str("# TYPE ferrosa_storage_collection_blob_expansions_total counter\n");
    if let Some(map) = COLLECTION_BLOB_EXPANSIONS.get() {
        for entry in map.iter() {
            let escaped = entry
                .key()
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            out.push_str(&format!(
                "ferrosa_storage_collection_blob_expansions_total{{table=\"{escaped}\"}} {}\n",
                entry.value().load(Ordering::Relaxed)
            ));
        }
    }
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
        "# HELP ferrosa_storage_upload_task_panics_total Object-store upload/delete tasks that panicked; the task's caller was told it failed and its worker kept running.\n",
    );
    out.push_str("# TYPE ferrosa_storage_upload_task_panics_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_upload_task_panics_total {}\n",
        UPLOAD_TASK_PANICS_TOTAL.load(Ordering::Relaxed)
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
    out.push_str("# HELP ferrosa_storage_compaction_panics_total Compaction tasks that panicked; each was failed, its input claims released and its worker kept running.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_panics_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_panics_total {}\n",
        COMPACTION_PANICS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_paused_tables Tables paused after repeated output digest or verification failures.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_paused_tables gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_paused_tables {}\n",
        COMPACTION_PAUSED_TABLES.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_legacy_ns_timestamps_normalised_total Legacy nanosecond cell timestamps (pre-t_cf637b6e Accord writes) read as microseconds, by where they were found. Zero on every node after compaction means the compatibility shim can be removed.\n");
    out.push_str("# TYPE ferrosa_storage_legacy_ns_timestamps_normalised_total counter\n");
    for source in ferrosa_common::cell_ts::LegacyNsSource::ALL {
        out.push_str(&format!(
            "ferrosa_storage_legacy_ns_timestamps_normalised_total{{source=\"{}\"}} {}\n",
            source.label(),
            ferrosa_common::cell_ts::legacy_ns_normalised_total(source)
        ));
    }
    out.push_str("# HELP ferrosa_storage_compaction_planning_deferred_total Planning rounds skipped because the compaction pipeline was already saturated (see FERROSA_COMPACTION_BACKPRESSURE_PRESSURE).\n");
    out.push_str("# TYPE ferrosa_storage_compaction_planning_deferred_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_planning_deferred_total {}\n",
        COMPACTION_PLANNING_DEFERRED_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_retire_failures_total Input retirement failures retained for reconciliation.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_retire_failures_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_retire_failures_total {}\n",
        COMPACTION_RETIRE_FAILURES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_cancelled_total Compaction tasks that returned Err because their CancelToken was cancelled.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_cancelled_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_cancelled_total {}\n",
        COMPACTION_CANCELLED_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_cancel_latency_seconds_sum Total latency from CancelToken::cancel() to the checkpoint that observed it.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_cancel_latency_seconds_sum counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_cancel_latency_seconds_sum {}\n",
        COMPACTION_CANCEL_LATENCY_MICROS_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str("# HELP ferrosa_storage_compaction_cancel_latency_seconds_count Observations of compaction cancel latency.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_cancel_latency_seconds_count counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_cancel_latency_seconds_count {}\n",
        COMPACTION_CANCEL_LATENCY_COUNT.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_cancel_latency_seconds_max Maximum observed compaction cancel latency.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_cancel_latency_seconds_max gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_cancel_latency_seconds_max {}\n",
        COMPACTION_CANCEL_LATENCY_MICROS_MAX.load(Ordering::Relaxed) as f64 / 1_000_000.0
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
    out.push_str("# HELP ferrosa_storage_compaction_purged_markers_total Deletion markers dropped by compaction after gc_grace_seconds.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_purged_markers_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_purged_markers_total {}\n",
        COMPACTION_PURGED_MARKERS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_purge_held_back_total Compactions that purged every partition and wrote one anyway so the output was non-empty.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_purge_held_back_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_purge_held_back_total {}\n",
        COMPACTION_PURGE_HELD_BACK_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP ferrosa_storage_compaction_purge_policy_errors_total Compactions that skipped tombstone purging because a table's gc_grace_seconds was unreadable; non-zero means a corrupt table option.\n");
    out.push_str("# TYPE ferrosa_storage_compaction_purge_policy_errors_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_compaction_purge_policy_errors_total {}\n",
        COMPACTION_PURGE_POLICY_ERRORS_TOTAL.load(Ordering::Relaxed)
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
        "# HELP ferrosa_index_repairs_total Index generations rebuilt from their rows, or scope sets restored, by reason (missing_sidecar, corrupt_sidecar, count_mismatch, dimension_mismatch, scope_set).\n",
    );
    out.push_str("# TYPE ferrosa_index_repairs_total counter\n");
    for ((index, reason), count) in labelled(&INDEX_REPAIRS_TOTAL).load().iter() {
        out.push_str(&format!(
            "ferrosa_index_repairs_total{{index=\"{index}\",reason=\"{reason}\"}} {count}\n"
        ));
    }
    out.push_str(
        "# HELP ferrosa_index_invalid Generations of an index that are incomplete or invalid; ANN over it refuses (retryable) while non-zero.\n",
    );
    out.push_str("# TYPE ferrosa_index_invalid gauge\n");
    for ((table, index), count) in labelled(&INDEX_INVALID).load().iter() {
        out.push_str(&format!(
            "ferrosa_index_invalid{{table=\"{table}\",index=\"{index}\"}} {count}\n"
        ));
    }
    out.push_str(
        "# HELP ferrosa_index_not_current Generations of a secondary index pending a backfill; reads through the index are refused (not current) while it is listed.\n",
    );
    out.push_str("# TYPE ferrosa_index_not_current gauge\n");
    for ((table, index), count) in labelled(&INDEX_NOT_CURRENT).load().iter() {
        out.push_str(&format!(
            "ferrosa_index_not_current{{table=\"{table}\",index=\"{index}\"}} {count}\n"
        ));
    }
    out.push_str(
        "# HELP ferrosa_index_backfill_failed 1 while a secondary index's last backfill build failed and awaits a retry.\n",
    );
    out.push_str("# TYPE ferrosa_index_backfill_failed gauge\n");
    for ((table, index), failed) in labelled(&INDEX_BACKFILL_FAILED).load().iter() {
        out.push_str(&format!(
            "ferrosa_index_backfill_failed{{table=\"{table}\",index=\"{index}\"}} {failed}\n"
        ));
    }
    out.push_str(
        "# HELP ferrosa_index_backfill_build_failures_total Secondary-index backfill builds that failed.\n",
    );
    out.push_str("# TYPE ferrosa_index_backfill_build_failures_total counter\n");
    out.push_str(&format!(
        "ferrosa_index_backfill_build_failures_total {}\n",
        INDEX_BACKFILL_BUILD_FAILURES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_index_backfill_retries_total Pending secondary-index generations resubmitted for a build after a failure or a stall.\n",
    );
    out.push_str("# TYPE ferrosa_index_backfill_retries_total counter\n");
    out.push_str(&format!(
        "ferrosa_index_backfill_retries_total {}\n",
        INDEX_BACKFILL_RETRIES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_vector_generations_repaired_total Generations whose vector sidecars were rebuilt from their rows (compacted without them, flushed before manifests, or a crashed build).\n",
    );
    out.push_str("# TYPE ferrosa_storage_vector_generations_repaired_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_vector_generations_repaired_total {}\n",
        VECTOR_GENERATIONS_REPAIRED_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_vector_generation_repair_failures_total Vector sidecar rebuilds of one generation that failed (ANN over that index refuses until one succeeds).\n",
    );
    out.push_str("# TYPE ferrosa_storage_vector_generation_repair_failures_total counter\n");
    out.push_str(&format!(
        "ferrosa_storage_vector_generation_repair_failures_total {}\n",
        VECTOR_GENERATION_REPAIR_FAILURES_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_vector_generations_pending Generations awaiting a vector sidecar rebuild; ANN over their index refuses (retryable) meanwhile.\n",
    );
    out.push_str("# TYPE ferrosa_storage_vector_generations_pending gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_vector_generations_pending {}\n",
        VECTOR_GENERATIONS_PENDING.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_index_sidecar_mapped_bytes Bytes of scalar index sidecars memory-mapped (reclaimable page cache, not heap).\n",
    );
    out.push_str("# TYPE ferrosa_storage_index_sidecar_mapped_bytes gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_index_sidecar_mapped_bytes {}\n",
        INDEX_SIDECAR_MAPPED_BYTES.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_storage_index_sidecar_mapped_files Scalar index sidecar files memory-mapped.\n",
    );
    out.push_str("# TYPE ferrosa_storage_index_sidecar_mapped_files gauge\n");
    out.push_str(&format!(
        "ferrosa_storage_index_sidecar_mapped_files {}\n",
        INDEX_SIDECAR_MAPPED_FILES.load(Ordering::Relaxed)
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
    fn write_admission_metrics_render_table_pressure_and_histogram() {
        let gauge = Arc::new(AtomicU64::new(73));
        register_write_admission_pressure("admission_test.unique".into(), &gauge);
        observe_write_admission_delay(Duration::from_millis(7));
        inc_write_admission_delayed();
        inc_write_admission_rejected("hard_memtable");

        let text = render_prometheus();
        assert!(text.contains(
            "ferrosa_storage_write_pressure_ratio{table=\"admission_test.unique\"} 0.73"
        ));
        assert!(text.contains("ferrosa_storage_write_admission_delay_seconds_bucket"));
        assert!(text.contains("ferrosa_storage_write_admission_delayed_total"));
        assert!(text
            .contains("ferrosa_storage_write_admission_rejected_total{reason=\"hard_memtable\"}"));
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
