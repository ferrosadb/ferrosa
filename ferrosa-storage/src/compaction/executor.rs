//! Correctness: A completed result is published once or its staging is reclaimed.
//! Last revised: 2026-09-27
//! Last changed: Publish retryable digest and readback failures through a bounded channel.
//! Module: Execute bounded streaming compaction work on background threads.
//! Correctness: Correct when input claims prevent overlap, completed outputs are
//! finalized once, and maintenance result batches remain explicitly bounded.
//! Last revised: 2026-09-26
//! Last changed: Made per-task output verification explicit so tests do not mutate process policy.
//!
//! Background compaction executor.
//!
//! Receives [`CompactionTask`]s via a channel, merges input SSTables on a
//! background thread using the existing `merge_partitions` logic, and sends
//! back [`CompactionResult`]s.
//!
//! **Cancellation (T-021, `compaction-cancel-safety.md` C1).** `try_submit`
//! creates one [`CancelToken`] per task, alongside the in-flight input claim.
//! It is checked (unconditionally, in every build) at every input open, the
//! top of each merge-loop partition, before `finish_to_directory`, before
//! `flush_files`, and once per readback-verify partition; a cancelled
//! checkpoint removes whatever this task has staged so far (loudly — no
//! `let _`) and returns `Err`. `poll_compactions` (`engine.rs`) checks the
//! same token once more, immediately before promoting the output — the last
//! point at which cancelling is free (`compaction-cancel-safety.md` C2);
//! after promotion the task rolls forward and T-022 owns the commit point.
//! [`CompactionExecutor::shutdown`] cancels every live token *before* joining
//! workers, so shutdown waits out one checkpoint interval rather than a
//! whole merge.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};
use ferrosa_common::{CancelReason, CancelToken};
use parking_lot::Mutex;

#[cfg(any(test, feature = "test-support"))]
use crate::compaction::cancel_harness::CancelPoint;
use crate::compaction::cancel_point;
use crate::store::SharedReaderPool;
use crate::upload::manager::SstableComponentBytes;

use super::control::{SubmissionTicket, TableCompactionPause, TaskTracker};
use super::metadata::{CompactionTask, SSTableMetadata};

/// Reader pool used to obtain compaction input SSTable readers so they count
/// against the engine-wide resident-reader bound (FMEA #11). Keyed identically
/// to the live read path: `(table_id, gen_num)` over `FileReadAt` readers.
type CompactionReaderPool = SharedReaderPool<ferrosa_sstable::io::FileReadAt>;

/// Fraction (as a divisor) of the configured memory limit that compaction may
/// budget across all concurrent tasks. `2` = at most half of RAM is charged to
/// compaction, leaving headroom for the read/write path against the node's
/// memory limit (e.g. the intentional 2 GB dev forcing function).
const COMPACTION_MEM_DIVISOR: u64 = 2;

/// Detect the node's configured memory limit in bytes: cgroup v2, then cgroup
/// v1, then total system RAM. `None` when nothing is detectable (e.g. macOS dev
/// without cgroups), in which case callers fall back to a CPU-only default.
fn detected_memory_limit_bytes() -> Option<u64> {
    // cgroup v2 unified hierarchy.
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let t = s.trim();
        if t != "max" {
            if let Ok(v) = t.parse::<u64>() {
                return Some(v);
            }
        }
    }
    // cgroup v1.
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(v) = s.trim().parse::<u64>() {
            // v1 encodes "unlimited" as a near-u64::MAX sentinel; ignore it.
            if v < (1u64 << 62) {
                return Some(v);
            }
        }
    }
    // Fall back to total system RAM from /proc/meminfo.
    if let Ok(s) = std::fs::read_to_string("/proc/meminfo") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                if let Some(kb) = rest
                    .split_whitespace()
                    .next()
                    .and_then(|x| x.parse::<u64>().ok())
                {
                    return Some(kb.saturating_mul(1024));
                }
            }
        }
    }
    None
}

/// Pure auto-tune: derive the concurrent-compaction cap from CPU count and the
/// (optional) configured memory limit. Bounded by BOTH resources — never more
/// than `cpus` (so each concurrent merge can own a worker) and never more than
/// `memory/2 / per-task-budget` (so peak compaction memory stays under half the
/// node's limit). Always at least 1. `None` memory → CPU-scaled default of 2.
fn auto_tuned_max_concurrent(
    cpus: usize,
    mem_limit_bytes: Option<u64>,
    parallelism_cap: usize,
    per_task_budget_bytes: u64,
) -> usize {
    let parallelism_cap = parallelism_cap.max(1);
    let cpu_cap = cpus.clamp(1, parallelism_cap);
    let mem_cap = match mem_limit_bytes {
        Some(mem) => {
            let budgeted = mem / COMPACTION_MEM_DIVISOR / per_task_budget_bytes.max(1);
            (budgeted as usize).clamp(1, parallelism_cap)
        }
        // No memory signal: keep the historical conservative default.
        None => 2,
    };
    cpu_cap.min(mem_cap).max(1)
}

/// Pure auto-tune for worker threads: one per CPU, bounded. Extra idle workers
/// are cheap (they block on `recv`), and having at least as many workers as the
/// concurrency cap lets every permitted merge run without head-of-line blocking.
fn auto_tuned_workers(cpus: usize, parallelism_cap: usize) -> usize {
    cpus.clamp(1, parallelism_cap.max(1))
}

fn available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(2)
}

/// Resolve the concurrent-compaction cap: explicit env override, else auto-tuned
/// from CPU + configured memory (never zero).
fn configured_max_concurrent_compactions() -> usize {
    let tuning = crate::runtime_tuning::storage_runtime_tuning();
    let default = auto_tuned_max_concurrent(
        available_cpus(),
        detected_memory_limit_bytes(),
        tuning.max_auto_compaction_parallelism,
        tuning.per_compaction_mem_budget_bytes,
    );
    crate::runtime_tuning::read_usize(
        "FERROSA_MAX_CONCURRENT_COMPACTIONS",
        default,
        1,
        tuning.max_auto_compaction_parallelism,
    )
}

/// Resolve the compaction worker-thread count: explicit env override, else
/// auto-tuned from CPU count (never zero).
fn configured_compaction_workers() -> usize {
    let tuning = crate::runtime_tuning::storage_runtime_tuning();
    let default = auto_tuned_workers(available_cpus(), tuning.max_auto_compaction_parallelism);
    crate::runtime_tuning::read_usize(
        "FERROSA_COMPACTION_WORKERS",
        default,
        1,
        tuning.max_auto_compaction_parallelism,
    )
}

/// A counting semaphore that caps the number of compaction merges running at
/// once across all worker threads. A worker acquires a permit before running
/// `execute_task` and releases it (via the `CompactionPermit` guard) when the
/// task finishes, so at most `cap` tasks ever execute concurrently regardless
/// of how many worker threads exist.
/// How saturated the whole compaction pipeline is, in `0.0..=1.0`.
///
/// `in_flight` is the count of tasks the executor is currently carrying — those
/// holding a merge permit plus those queued or waiting on the gate. `capacity`
/// is the configured merge concurrency. The planner uses this to defer before
/// it spends a full planning round (`select` + one metadata rescan per emitted
/// task) only to discover every worker queue is full: the task and result queues
/// are one and two deep per worker, so the pipeline saturates almost
/// immediately, and the planning work then buys no throughput while occupying
/// the maintenance task that should be draining results.
///
/// A zero capacity reports no pressure rather than dividing by zero; callers
/// pair this with [`compaction_planning_deferred`], whose default threshold
/// (`1.0`) makes the gate inert.
pub(crate) fn compaction_pressure(in_flight: usize, capacity: usize) -> f64 {
    if capacity == 0 {
        return 0.0;
    }
    (in_flight as f64 / capacity as f64).clamp(0.0, 1.0)
}

/// Whether the planner should skip this round because the compaction pipeline
/// is already saturated.
///
/// `threshold` is operator-set (`FERROSA_COMPACTION_BACKPRESSURE_PRESSURE`) and
/// validated into `0.0..=1.0`. The default is `1.0`: only a completely saturated
/// pipeline defers, so an idle or lightly loaded node behaves exactly as before.
/// A threshold of `0.0` disables the gate, for an operator who would rather
/// always attempt a plan than ever defer one.
pub(crate) fn compaction_planning_deferred(pressure: f64, threshold: f64) -> bool {
    threshold > 0.0 && pressure >= threshold
}

struct CompactionGate {
    permits: Receiver<()>,
    returned: Sender<()>,
}

impl CompactionGate {
    fn new(cap: usize) -> Self {
        let (returned, permits) = crossbeam_channel::bounded(cap.max(1));
        for _ in 0..cap.max(1) {
            returned.send(()).expect("new permit channel");
        }
        Self { permits, returned }
    }

    fn acquire<'a>(
        &'a self,
        cancel: &CancelToken,
        shutdown: &Receiver<()>,
    ) -> Option<CompactionPermit<'a>> {
        if cancel.is_cancelled() {
            return None;
        }
        crossbeam_channel::select! {
            recv(self.permits) -> _ => Some(CompactionPermit { gate: self }),
            recv(cancel.closed()) -> _ => None,
            recv(shutdown) -> _ => None,
        }
    }
}

struct CompactionPermit<'a> {
    gate: &'a CompactionGate,
}

impl Drop for CompactionPermit<'_> {
    fn drop(&mut self) {
        self.gate
            .returned
            .try_send(())
            .expect("permit returned exactly once");
    }
}

struct QueuedCompactionTask {
    task: CompactionTask,
    queued_at: Instant,
    /// This task's cancellation token (T-021), created in `try_submit`
    /// alongside the in-flight input claim.
    cancel: CancelToken,
}

fn env_flag_enabled(name: &str, default: bool) -> bool {
    match std::env::var(name).ok().as_deref() {
        Some("true" | "1" | "on" | "yes") => true,
        Some("false" | "0" | "off" | "no") => false,
        _ => default,
    }
}

/// How compaction reads its input `Data.db` files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InputReadMode {
    /// The shared, page-cached reader from the engine-wide pool (default).
    Cached,
    /// A private cache-bypassing reader with a read-ahead window of `window` bytes.
    DirectScan { window: usize },
}

/// Choose the input read mode from the (already-read) environment values.
///
/// `enabled` comes from the run-time switch (`FERROSA_COMPACTION_DIRECT_READ`,
/// then `FERROSA_DIRECT_IO`): ON by default, `=0` turns it off. `window_env` is
/// `FERROSA_COMPACTION_READAHEAD_BYTES`; an invalid value is ERROR-logged and the
/// default window is used, so a typo cannot silently change memory use.
fn input_read_mode(enabled: bool, window_env: Option<&str>) -> InputReadMode {
    if !enabled {
        return InputReadMode::Cached;
    }
    let window = ferrosa_sstable::scan::parse_scan_window(window_env).unwrap_or_else(|why| {
        tracing::error!(
            error = %why,
            default = ferrosa_sstable::scan::DEFAULT_SCAN_WINDOW,
            "compaction: invalid FERROSA_COMPACTION_READAHEAD_BYTES, using the default window"
        );
        ferrosa_sstable::scan::DEFAULT_SCAN_WINDOW
    });
    if let Some(raw) = window_env {
        if raw.trim().parse::<usize>().is_ok_and(|requested| {
            requested != window
                && requested > 0
                && requested <= ferrosa_sstable::scan::MAX_SCAN_WINDOW
        }) {
            tracing::warn!(
                configured = raw,
                effective = window,
                "compaction read-ahead window rounded up to a block multiple"
            );
        }
    }
    InputReadMode::DirectScan { window }
}

fn configured_input_read_mode() -> InputReadMode {
    static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    input_read_mode(
        ferrosa_sstable::direct::direct_wanted("FERROSA_COMPACTION_DIRECT_READ", &WARNED),
        std::env::var("FERROSA_COMPACTION_READAHEAD_BYTES")
            .ok()
            .as_deref(),
    )
}

/// Open an input `Data.db` for the merge in the requested mode.
fn open_input_data(
    path: &std::path::Path,
    mode: InputReadMode,
) -> ferrosa_common::Result<ferrosa_sstable::io::FileReadAt> {
    use ferrosa_sstable::io::FileReadAt;
    match mode {
        InputReadMode::Cached => FileReadAt::open(path),
        #[cfg(unix)]
        InputReadMode::DirectScan { window } => FileReadAt::open_scan(path, window),
        #[cfg(not(unix))]
        InputReadMode::DirectScan { .. } => Err(ferrosa_common::Error::InvalidData(
            "FERROSA_COMPACTION_DIRECT_READ is only supported on unix".into(),
        )),
    }
}

fn compaction_verify_output_enabled() -> bool {
    env_flag_enabled("FERROSA_COMPACTION_VERIFY_OUTPUT", true)
}

// Test-only fault-injection hook for the staged compaction output, called
// right after `finish_to_directory` returns and before `flush_files`
// renames/digest-verifies it. See the call site in
// `execute_task_with_policy` and `digest_verify_compaction_output_*`
// tests (T-012).
//
// `execute_task_with_policy` runs synchronously on the caller's thread
// (no internal thread handoff between `finish_to_directory` and the hook
// call below), so a `thread_local` -- rather than a process-wide `static
// Mutex` -- confines a test's injected corruption to its own call. `cargo
// test` runs test functions concurrently across threads; a shared `static`
// hook was visible to every OTHER compaction test running on a different
// thread at the same time and corrupted their outputs too.
// (A `///` doc comment here is a clippy error: rustdoc cannot attach
// documentation to a macro invocation like `thread_local!`.)
#[cfg(test)]
type CompactionOutputHook = dyn Fn(&ferrosa_sstable::writer::SSTableOutputFiles);

#[cfg(test)]
thread_local! {
    static COMPACTION_OUTPUT_HOOK: std::cell::RefCell<Option<Box<CompactionOutputHook>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn set_compaction_output_hook(
    hook: impl Fn(&ferrosa_sstable::writer::SSTableOutputFiles) + 'static,
) {
    COMPACTION_OUTPUT_HOOK.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
}

#[cfg(test)]
fn clear_compaction_output_hook() {
    COMPACTION_OUTPUT_HOOK.with(|h| *h.borrow_mut() = None);
}

fn ensure_compaction_component(
    path: &std::path::Path,
    required: bool,
    reject_empty: bool,
) -> std::result::Result<Option<u64>, String> {
    match std::fs::metadata(path) {
        Ok(meta) => {
            if reject_empty && meta.len() == 0 {
                return Err(format!(
                    "required SSTable component {} is empty",
                    path.display()
                ));
            }
            return Ok(Some(meta.len()));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(format!(
                "failed to inspect SSTable component {}: {e}",
                path.display()
            ));
        }
    }

    let restored = ferrosa_sstable::io::rehydrate_file(path).map_err(|e| {
        format!(
            "failed to rehydrate SSTable component {}: {e}",
            path.display()
        )
    })?;
    if !restored {
        return if required {
            Err(format!(
                "required SSTable component {} is missing",
                path.display()
            ))
        } else {
            Ok(None)
        };
    }

    match std::fs::metadata(path) {
        Ok(meta) => {
            if reject_empty && meta.len() == 0 {
                return Err(format!(
                    "required SSTable component {} is empty after rehydration",
                    path.display()
                ));
            }
            Ok(Some(meta.len()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !required => Ok(None),
        Err(e) => Err(format!(
            "failed to inspect rehydrated SSTable component {}: {e}",
            path.display()
        )),
    }
}

fn read_compaction_component(
    path: &std::path::Path,
    required: bool,
    reject_empty: bool,
) -> std::result::Result<Option<Vec<u8>>, String> {
    if ensure_compaction_component(path, required, reject_empty)?.is_none() {
        return Ok(None);
    }
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !required => Ok(None),
        Err(e) => Err(format!(
            "failed to read SSTable component {}: {e}",
            path.display()
        )),
    }
}

/// Removes a task's merge-time staging directory after cancellation.
///
/// `compaction-cancel-safety.md` C1: "On cancel, remove staging with loud
/// errors." Every failure is logged (never `let _`) rather than discarded —
/// a leaked staging directory is otherwise invisible until the next process
/// restart wipes `compaction/` wholesale. A missing directory is not an
/// error (nothing was staged yet, e.g. cancellation at `InputOpen`).
fn remove_staging_dir(staging_dir: &std::path::Path) {
    if let Err(e) = std::fs::remove_dir_all(staging_dir) {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::error!(
                %e,
                dir = %staging_dir.display(),
                "compaction cancel: failed to remove merge staging directory"
            );
        }
    }
}

/// Declared before the writer so its pumps join before scratch is removed.
/// Borrowing the path adds no allocation and also covers I/O errors between
/// explicit cancellation checkpoints.
struct StagingCleanup<'a>(&'a std::path::Path);

impl Drop for StagingCleanup<'_> {
    fn drop(&mut self) {
        remove_staging_dir(self.0);
    }
}

/// Removes every `{gen}-*` compaction-output component file under `dir`
/// after cancellation, once the output has already been flushed into `dir`
/// (post `flush_files`, pre-promote) but the task is rolling back rather than
/// completing. Used by the `VerifyPartition` checkpoint and by
/// `poll_compactions`'s `BeforePromote` check (`engine.rs`) — the two points
/// where the output lives at a real path but has not yet been promoted or
/// observed elsewhere. Every removal failure is logged loudly (no `let _`).
pub(crate) fn remove_staged_output_components(dir: &std::path::Path, gen: &str) {
    let prefix = format!("{gen}-");
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            tracing::error!(
                %e,
                dir = %dir.display(),
                "compaction cancel: failed to list staged output directory for cleanup"
            );
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::error!(%error, dir = %dir.display(),
                    "compaction cancel: failed to inspect staged output entry");
                continue;
            }
        };
        let name = entry.file_name();
        if name.to_string_lossy().starts_with(&prefix) {
            if let Err(e) = std::fs::remove_file(entry.path()) {
                tracing::error!(
                    %e,
                    path = %entry.path().display(),
                    "compaction cancel: failed to remove staged output component"
                );
            }
        }
    }
}

/// Result of a completed compaction.
#[derive(Debug)]
pub struct CompactionResult {
    /// The original task.
    pub task: CompactionTask,
    /// Metadata for the newly-created output SSTable.
    pub output: SSTableMetadata,
    /// Optional finished SSTable components captured before local file flush.
    ///
    /// This is reserved for a future truly streaming compaction writer. The
    /// current in-memory SSTable writer already owns full component buffers;
    /// cloning those buffers for direct upload doubled peak heap during large
    /// compactions and contributed to OOMs, so compaction now uploads from the
    /// flushed files instead.
    pub direct_upload: Option<CompactionDirectUpload>,
    /// This task's cancellation token (T-021). The merge already ran to
    /// completion by the time a `CompactionResult` exists, but the token can
    /// still be cancelled between here and `poll_compactions` promoting the
    /// output — the last free cancel point (`compaction-cancel-safety.md`
    /// C2) — so `poll_compactions` checks it once more before promoting.
    pub cancel: CancelToken,
}

/// Bounded notification that an output digest or readback verification failed.
#[derive(Debug)]
pub(crate) struct CompactionFailure {
    pub table_id: crate::TableId,
    pub message: String,
}

/// In-memory SSTable components produced by compaction.
#[derive(Debug, Clone)]
pub struct CompactionDirectUpload {
    pub files: Vec<SstableComponentBytes>,
}

impl CompactionDirectUpload {
    pub fn total_size_bytes(&self) -> u64 {
        self.files
            .iter()
            .map(SstableComponentBytes::size_bytes)
            .sum()
    }
}

/// Move a completed result into the bounded queue, or return ownership when
/// cancellation/shutdown wins. Count it before publishing so a fast receiver
/// cannot decrement an as-yet-unincremented counter. No retry loop or cloning.
fn send_result_or_cancel<T>(
    sender: &Sender<T>,
    result: T,
    pending: &AtomicUsize,
    cancel: &CancelToken,
    shutdown: &Receiver<()>,
) -> Option<T> {
    crossbeam_channel::select! {
        send(sender, {
            pending.fetch_add(1, Ordering::Release);
            result
        }) -> sent => match sent {
            Ok(()) => None,
            Err(error) => {
                pending.fetch_sub(1, Ordering::Release);
                Some(error.0)
            }
        },
        recv(cancel.closed()) -> _ => Some(result),
        recv(shutdown) -> _ => Some(result),
    }
}

fn send_failure_or_shutdown(
    sender: &Sender<CompactionFailure>,
    failure: CompactionFailure,
    pending: &AtomicUsize,
    shutdown: &Receiver<()>,
) -> bool {
    crossbeam_channel::select! {
        send(sender, {
            pending.fetch_add(1, Ordering::Release);
            failure
        }) -> sent => match sent {
            Ok(()) => true,
            Err(error) => {
                pending.fetch_sub(1, Ordering::Release);
                tracing::warn!(table_id = %error.0.table_id, "compaction: retry notification receiver stopped");
                false
            }
        },
        recv(shutdown) -> _ => false,
    }
}

fn is_digest_verification_failure(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("digest")
        || message.contains("corruption: output")
        || message.contains("output sstable is corrupt")
}

/// True for any compaction failure that is worth retrying with backoff and that
/// should eventually pause the table rather than loop forever.
///
/// Covers the digest/verification class (the compaction produced output that
/// did not verify) **and** the input-component class (an input generation's
/// component could not be found or rehydrated).
///
/// The input class matters as much as the digest class but was previously
/// excluded, which silently disabled every mitigation for it: the failure never
/// reached the retry controller, so the planner re-selected the same
/// unreadable input on the next pass with no backoff and no pause. The result
/// was an unbounded hot loop (`compaction_failed_total` climbing while
/// `compaction_completed_total` stayed 0) that pinned a core and prevented
/// evicted generations from ever merging away. Every message in this class is
/// produced by [`ensure_compaction_component`] and names the component as an
/// "SSTable component", which is the stable discriminator here.
fn is_retryable_compaction_failure(message: &str) -> bool {
    is_digest_verification_failure(message)
        || message.to_ascii_lowercase().contains("sstable component")
}

/// Runs compaction tasks on a background thread.
///
/// `StorageEngine` submits tasks via `submit()` and polls results via
/// `poll_results()`. The executor is stopped on `shutdown()`.
pub struct CompactionExecutor {
    task_txs: Vec<Sender<QueuedCompactionTask>>,
    next_worker: AtomicUsize,
    result_rx: Mutex<Receiver<CompactionResult>>,
    failure_rx: Mutex<Receiver<CompactionFailure>>,
    failure_notify: Arc<tokio::sync::Notify>,
    /// At most one result received by [`Self::await_result_available`] and not yet
    /// handed to a poll. Lock order: this slot, then `result_rx`.
    held_result: Mutex<Option<CompactionResult>>,
    handles: Mutex<Vec<thread::JoinHandle<()>>>,
    tracker: TaskTracker,
    /// Count of completed results sitting in `result_rx`, waiting to be
    /// drained by `poll_results`/`poll_results_bounded`. Incremented by a
    /// worker thread the instant it enqueues a finished result, decremented
    /// as each result is popped. This is the deterministic completion signal
    /// tests use instead of racing the filesystem for compaction output —
    /// unlike "does a file exist under compaction/", it cannot observe a
    /// half-written staging artifact as "done".
    pending_results: Arc<AtomicUsize>,
    pending_failures: Arc<AtomicUsize>,
    /// Closing this (dropping the sole sender in `shutdown()`) wakes every
    /// worker blocked in `select!` on its task channel immediately — no poll
    /// interval (T-021 CD1, decisions.md D7).
    shutdown_tx: Mutex<Option<Sender<()>>>,
}

impl Default for CompactionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionExecutor {
    /// Creates and starts the compaction executor without a reader pool.
    ///
    /// Input SSTables are opened directly. Used by tests and the compaction
    /// validator that drive `execute_task` synchronously. Production engines use
    /// [`Self::with_reader_pool`] so input readers count against the
    /// engine-wide resident-reader bound (FMEA #11).
    pub fn new() -> Self {
        Self::build(None)
    }

    /// Creates and starts the executor routing input opens through the
    /// engine-wide reader pool, so compaction's resident input readers are
    /// shared with and bounded by the same pool as the read/startup paths.
    pub fn with_reader_pool(pool: CompactionReaderPool) -> Self {
        Self::build(Some(pool))
    }

    fn build(reader_pool: Option<CompactionReaderPool>) -> Self {
        let tuning = *crate::runtime_tuning::storage_runtime_tuning();
        let worker_count = configured_compaction_workers();
        let max_concurrent = configured_max_concurrent_compactions();
        tracing::info!(
            worker_count,
            max_concurrent,
            cpus = available_cpus(),
            mem_limit_bytes = detected_memory_limit_bytes().unwrap_or(0),
            "compaction executor: auto-tuned parallelism (override with \
             FERROSA_COMPACTION_WORKERS / FERROSA_MAX_CONCURRENT_COMPACTIONS)"
        );
        let (result_tx, result_rx) = crossbeam_channel::bounded::<CompactionResult>(
            worker_count
                .saturating_mul(tuning.compaction_pipeline.result_queue_capacity_per_worker),
        );
        let (failure_tx, failure_rx) = crossbeam_channel::bounded::<CompactionFailure>(
            worker_count
                .saturating_mul(tuning.compaction_pipeline.result_queue_capacity_per_worker),
        );
        let failure_notify = Arc::new(tokio::sync::Notify::new());
        let tracker = TaskTracker::default();
        let pending_results = Arc::new(AtomicUsize::new(0));
        let pending_failures = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(CompactionGate::new(max_concurrent));
        // The planner reads merge capacity off the tracker, which every task
        // already shares; the gate's permit channel cannot be duplicated.
        let tracker = TaskTracker::with_capacity(tracker, max_concurrent);
        // The sole sender lives on `Self`; dropping it in `shutdown()` closes
        // this channel and wakes every worker's `select!` at once — no poll
        // interval (T-021 CD1, decisions.md D7).
        let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded::<()>(0);
        // `reader_pool` is already an `Arc`, so each worker gets a cheap clone.
        let mut task_txs = Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);

        for worker_idx in 0..worker_count {
            let (task_tx, task_rx) = crossbeam_channel::bounded::<QueuedCompactionTask>(
                tuning.compaction_pipeline.task_queue_capacity_per_worker,
            );
            task_txs.push(task_tx);
            let result_tx = result_tx.clone();
            let failure_tx = failure_tx.clone();
            let failure_notify = Arc::clone(&failure_notify);
            let tracker = tracker.clone();
            let pending_results = Arc::clone(&pending_results);
            let pending_failures = Arc::clone(&pending_failures);
            let gate = Arc::clone(&gate);
            let reader_pool = reader_pool.clone();
            let shutdown_rx = shutdown_rx.clone();

            let handle = thread::Builder::new()
                .name(format!("compaction-executor-{worker_idx}"))
                .spawn(move || {
                    loop {
                        // Blocking select, not a stop-flag check-then-recv
                        // poll (T-021 CD1, decisions.md D7): a worker parked
                        // here with no queued task wakes immediately when
                        // `shutdown()` drops `shutdown_tx`, not on the next
                        // poll slice.
                        let queued = crossbeam_channel::select! {
                            recv(task_rx) -> msg => match msg {
                                Ok(queued) => queued,
                                Err(_) => break,
                            },
                            recv(shutdown_rx) -> _ => break,
                        };
                        crate::metrics::dec_compaction_queue_depth();
                        crate::metrics::observe_compaction_phase(
                            crate::metrics::CompactionPhase::QueueWait,
                            queued.queued_at.elapsed(),
                        );
                        let task = queued.task;
                        let cancel = queued.cancel;
                        // Cap concurrent merges across all workers
                        // (FMEA #11): hold a permit for the duration of
                        // the merge. The running gauge is bumped only
                        // *after* the permit is taken, so
                        // `compaction_running_max` reflects tasks
                        // actually executing, never those blocked at the
                        // gate.
                        let permit = match gate.acquire(&cancel, &shutdown_rx) {
                            Some(permit) => permit,
                            None => {
                                // Cancelled before a permit freed:
                                // requeued inputs are released so a
                                // restart can reschedule them.
                                Self::release_in_flight_inputs(
                                    &tracker,
                                    &task,
                                );
                                continue;
                            }
                        };
                        crate::metrics::inc_compaction_running();
                        let task_start = Instant::now();
                        // A panic is this task's failure, not the worker's
                        // (t_8aae3ed7): it takes the Err path below, which
                        // releases the input claims. Before, the panic ended
                        // the thread with the claims held, so the table never
                        // drained and TRUNCATE/DROP waited forever; the
                        // worker's queued tasks leaked their claims too.
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || Self::execute_task_routed(&task, reader_pool.as_ref(), &cancel),
                        ))
                        .unwrap_or_else(|payload| {
                            crate::metrics::inc_compaction_panics();
                            let message = payload
                                .downcast_ref::<&str>()
                                .map(|s| s.to_string())
                                .or_else(|| payload.downcast_ref::<String>().cloned())
                                .unwrap_or_else(|| "non-string panic payload".to_string());
                            Err(format!("compaction panicked: {message}"))
                        });
                        crate::metrics::dec_compaction_running();
                        drop(permit);
                        match result {
                            Ok(output) => {
                                tracing::debug!(
                                    inputs = task.inputs.len(),
                                    pool_input_opens = output.pool_input_opens,
                                    elapsed_ms = task_start.elapsed().as_millis() as u64,
                                    "compaction: task finished"
                                );
                                let completed = CompactionResult {
                                    task,
                                    output: output.metadata,
                                    direct_upload: output.direct_upload,
                                    cancel: cancel.clone(),
                                };
                                if let Some(unsent) = send_result_or_cancel(
                                    &result_tx, completed, &pending_results, &cancel, &shutdown_rx,
                                ) {
                                    remove_staged_output_components(&unsent.output.path, &unsent.output.id);
                                    Self::release_in_flight_inputs(
                                        &tracker, &unsent.task,
                                    );
                                    if let Some(reason) = cancel.reason() {
                                        crate::metrics::inc_compaction_cancelled();
                                        if let Some(started) = cancel.cancelled_at() {
                                            crate::metrics::observe_compaction_cancel_latency(started.elapsed());
                                        }
                                        tracing::info!(table_id = %unsent.task.table_id, %reason,
                                            "compaction: cancelled while delivering result; staged output removed");
                                    } else {
                                        tracing::warn!(table_id = %unsent.task.table_id,
                                            "compaction: result delivery stopped; staged output removed, inputs remain live");
                                    }
                                }
                                tracker.changed.notify_waiters();
                            }
                            Err(e) => {
                                Self::release_in_flight_inputs(
                                    &tracker,
                                    &task,
                                );
                                crate::metrics::observe_compaction_phase(
                                    crate::metrics::CompactionPhase::Total,
                                    task_start.elapsed(),
                                );
                                // A cancelled checkpoint's error is
                                // distinguished by the token's own state, not
                                // by matching the error string: only a task
                                // whose token was actually cancelled can have
                                // `reason()` set.
                                if let Some(reason) = cancel.reason() {
                                    crate::metrics::inc_compaction_cancelled();
                                    if let Some(cancelled_at) = cancel.cancelled_at() {
                                        crate::metrics::observe_compaction_cancel_latency(
                                            cancelled_at.elapsed(),
                                        );
                                    }
                                    tracing::info!(
                                        %e, reason = %reason, table_id = %task.table_id,
                                        "compaction: task cancelled"
                                    );
                                } else {
                                    crate::metrics::inc_compaction_failed();
                                    tracing::error!(%e, table_id = %task.table_id, "compaction: task failed");
                                    if is_retryable_compaction_failure(&e) {
                                        let failure = CompactionFailure {
                                            table_id: task.table_id,
                                            message: e,
                                        };
                                        let sent = send_failure_or_shutdown(
                                            &failure_tx,
                                            failure,
                                            &pending_failures,
                                            &shutdown_rx,
                                        );
                                        if sent {
                                            failure_notify.notify_one();
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
                .expect("failed to spawn compaction executor thread");
            handles.push(handle);
        }

        Self {
            task_txs,
            next_worker: AtomicUsize::new(0),
            result_rx: Mutex::new(result_rx),
            failure_rx: Mutex::new(failure_rx),
            failure_notify,
            held_result: Mutex::new(None),
            handles: Mutex::new(handles),
            tracker,
            pending_results,
            pending_failures,
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
        }
    }

    /// Submits a compaction task to the background thread.
    ///
    /// Returns `Ok(false)` when one or more input SSTables are already claimed
    /// by another queued or running task. Callers that need to report whether
    /// a bounded maintenance batch was actually accepted can use this result
    /// instead of treating overlap suppression as a successful submission.
    pub fn try_submit(&self, task: CompactionTask) -> ferrosa_common::Result<bool> {
        let Some(ticket) = self.submission_ticket(&task.table_id) else {
            return Ok(false);
        };
        self.try_submit_with_ticket(task, &ticket)
    }

    pub(crate) fn submission_ticket(&self, table: &crate::TableId) -> Option<SubmissionTicket> {
        self.tracker.submission_ticket(table)
    }

    pub(crate) fn pause_table(
        &self,
        table: &crate::TableId,
        reason: CancelReason,
    ) -> TableCompactionPause {
        self.tracker.pause_table(table, reason)
    }

    pub(crate) fn changed(&self) -> &tokio::sync::Notify {
        &self.tracker.changed
    }

    /// How many compaction tasks this executor is currently carrying (queued,
    /// waiting on the merge gate, or running), across all tables. Paired with
    /// [`Self::compaction_capacity`] it gives the planner a saturation signal
    /// so it can defer before paying for a planning round that cannot submit.
    pub fn compaction_in_flight(&self) -> usize {
        self.tracker.in_flight_tasks()
    }

    /// Merge concurrency the executor runs at; zero means unknown.
    pub fn compaction_capacity(&self) -> usize {
        self.tracker.capacity()
    }

    /// Whether `table` still has a compaction task registered (queued, running,
    /// or finished but not yet finalized).
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn table_has_tasks(&self, table: &crate::TableId) -> bool {
        self.tracker.table_has_tasks(table)
    }

    pub(crate) fn try_submit_with_ticket(
        &self,
        task: CompactionTask,
        ticket: &SubmissionTicket,
    ) -> ferrosa_common::Result<bool> {
        crate::metrics::inc_compaction_submitted();
        let Some(cancel) = self.tracker.try_register(&task, ticket) else {
            crate::metrics::inc_compaction_skipped_overlap();
            return Ok(false);
        };
        // Registers `cancel` for the harness (T-021), scoped by table id —
        // the same scope `cancel_point!` passes. A test's cancel-point hook
        // can look this exact token up (`cancel_harness::cancel_now`) and
        // cancel it, not merely record having reached the point. Stays
        // registered for this task's whole lifetime, through
        // `poll_compactions`'s `BeforePromote` check, and is removed in
        // `release_in_flight_inputs` once the task is fully finalized.
        // No-op outside test/test-support builds.
        #[cfg(any(test, feature = "test-support"))]
        crate::compaction::cancel_harness::register_cancel_token(
            task.table_id.to_string(),
            cancel.clone(),
        );

        let worker_idx = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.task_txs.len();
        crate::metrics::inc_compaction_queue_depth();
        let queued = QueuedCompactionTask {
            task,
            queued_at: Instant::now(),
            cancel,
        };
        match self.task_txs[worker_idx].try_send(queued) {
            Ok(()) => Ok(true),
            Err(crossbeam_channel::TrySendError::Full(queued)) => {
                crate::metrics::dec_compaction_queue_depth();
                Self::release_in_flight_inputs(&self.tracker, &queued.task);
                tracing::debug!(
                    table_id = %queued.task.table_id,
                    inputs = queued.task.inputs.len(),
                    "compaction: bounded worker queue full; deferring task to a later maintenance poll"
                );
                Ok(false)
            }
            Err(crossbeam_channel::TrySendError::Disconnected(queued)) => {
                crate::metrics::dec_compaction_queue_depth();
                Self::release_in_flight_inputs(&self.tracker, &queued.task);
                Err(ferrosa_common::Error::InvalidFormat(
                    "compaction channel closed".into(),
                ))
            }
        }
    }

    /// Submits a compaction task and preserves the historical overlap behavior
    /// for callers that do not need to distinguish a skipped task.
    pub fn submit(&self, task: CompactionTask) -> ferrosa_common::Result<()> {
        self.try_submit(task).map(|_| ())
    }

    /// Polls for completed compaction results (non-blocking).
    pub fn poll_results(&self) -> Vec<CompactionResult> {
        self.poll_results_bounded(usize::MAX)
    }

    /// Polls at most `max_results` completed tasks without blocking.
    ///
    /// Production maintenance uses a small fixed cap so one large backlog can
    /// neither materialize every completion nor starve later maintenance
    /// stages. `poll_results()` remains for fixed-size unit-test fixtures.
    pub fn poll_results_bounded(&self, max_results: usize) -> Vec<CompactionResult> {
        let mut held = self.held_result.lock();
        let rx = self.result_rx.lock();
        let mut results = Vec::with_capacity(max_results.min(8));
        if max_results > 0 {
            if let Some(result) = held.take() {
                self.pending_results.fetch_sub(1, Ordering::Release);
                results.push(result);
            }
        }
        while results.len() < max_results {
            let Ok(result) = rx.try_recv() else {
                break;
            };
            self.pending_results.fetch_sub(1, Ordering::Release);
            results.push(result);
        }
        results
    }

    /// Drains a bounded batch of retryable output digest failures.
    pub(crate) fn poll_failures_bounded(&self, max_failures: usize) -> Vec<CompactionFailure> {
        let rx = self.failure_rx.lock();
        let mut failures = Vec::with_capacity(max_failures.min(8));
        while failures.len() < max_failures {
            let Ok(failure) = rx.try_recv() else {
                break;
            };
            self.pending_failures.fetch_sub(1, Ordering::Release);
            failures.push(failure);
        }
        failures
    }

    pub(crate) async fn wait_for_failure_notification(&self) {
        self.failure_notify.notified().await;
    }

    /// Block until a completed compaction result is available to
    /// [`Self::poll_results`], or `hang_guard` elapses. Returns whether one is.
    ///
    /// The result is NOT consumed: it is held and handed out by the next poll, so the
    /// caller then runs its usual integration path. This is the completion signal for
    /// callers (tests, mostly) that used to poll `poll_results` on a wall clock and
    /// raced the worker thread; `hang_guard` only turns a worker that never finishes
    /// into a failure instead of a hang.
    pub fn await_result_available(&self, hang_guard: std::time::Duration) -> bool {
        let mut held = self.held_result.lock();
        if held.is_some() {
            return true;
        }
        let rx = self.result_rx.lock();
        match rx.recv_timeout(hang_guard) {
            Ok(result) => {
                *held = Some(result);
                true
            }
            // Timeout: the worker never finished (or nothing was submitted).
            // Disconnected: the executor is shut down. Neither is a result.
            Err(_) => false,
        }
    }

    /// Number of completed compaction results currently sitting in the
    /// result queue, waiting to be drained by `poll_results`/
    /// `poll_results_bounded`.
    ///
    /// This is the deterministic signal for "the background compaction
    /// thread is done": incremented the instant a worker enqueues a
    /// finished result, decremented as each result is popped. Tests that
    /// need to wait for a submitted task to finish (without consuming the
    /// result themselves) should poll this rather than probing the
    /// filesystem for output files, which can observe an in-progress
    /// staging write as "done".
    pub fn pending_result_count(&self) -> usize {
        self.pending_results.load(Ordering::Acquire)
    }

    /// Blocks (via short polling sleeps) until at least one completed
    /// compaction result is waiting in the result queue, or `timeout`
    /// elapses.
    ///
    /// Returns `true` once a result is observed, `false` on timeout. Callers
    /// that require completion should treat a `false` return as fatal (panic
    /// with context) rather than silently proceeding — see
    /// `make_engine_with_pending_compaction` in `engine.rs` for the pattern.
    #[cfg(test)]
    pub(crate) async fn wait_for_result(&self, timeout: std::time::Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.pending_result_count() > 0 {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    pub(crate) fn request_operator_stop(
        &self,
        table_id: Option<&crate::TableId>,
    ) -> super::CompactionStopReport {
        self.tracker.request_operator_stop(table_id)
    }

    /// Request reclamation from the largest registered task. Keep only one
    /// disk-pressure cancellation outstanding until its input claim is released,
    /// so a burst of rejected writes cannot cancel every compaction at once.
    /// Admission must still independently verify that free space recovered.
    pub fn cancel_largest_for_disk_reserve(&self) -> Option<u64> {
        self.tracker.cancel_largest_for_disk_reserve()
    }

    /// Releases a successful task's inputs after its result has been finalized.
    ///
    /// Successful compactions must stay claimed while their result waits in the
    /// result queue. Otherwise a later flush can schedule the same inputs again,
    /// and the first finalized result can delete files the duplicate task still
    /// expects to read.
    pub fn release_task_inputs(&self, task: &CompactionTask) {
        Self::release_in_flight_inputs(&self.tracker, task);
    }

    /// Shuts down the compaction executor, waiting for the background thread.
    ///
    /// Cancels every live task's [`CancelToken`] and closes the worker
    /// shutdown channel *before* joining, so shutdown waits out one
    /// checkpoint interval per in-flight task instead of a whole merge
    /// (`compaction-cancel-safety.md` C1). This also reaches completed tasks
    /// whose `CompactionResult` is still sitting in the result queue,
    /// unfinalized by `poll_compactions`: `poll_compactions` checks the same
    /// token before promoting and discards the staged output instead.
    pub fn shutdown(&self) {
        self.tracker.cancel_all(CancelReason::Shutdown);
        // Dropping the sole sender closes the channel: every worker blocked
        // in `select!` on it wakes at once (no poll interval).
        self.shutdown_tx.lock().take();
        for handle in self.handles.lock().drain(..) {
            if let Err(panic) = handle.join() {
                // A worker thread panicking mid-compaction is not a "quiet"
                // shutdown outcome: something crashed rather than returning
                // `Err`, and swallowing that (`let _ = handle.join()`) would
                // hide it entirely (standing order: no silently discarded
                // errors). `Box<dyn Any + Send>` isn't `Debug`, so pull out
                // the message the common panic payload shapes carry.
                let message = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "non-string panic payload".to_string());
                tracing::error!(
                    panic = %message,
                    "compaction executor: worker thread panicked during shutdown"
                );
            }
        }
    }

    fn release_in_flight_inputs(tracker: &TaskTracker, task: &CompactionTask) {
        tracker.release(task);
        // The task is fully finalized (failed/cancelled during merge, or
        // promoted/rolled-back by `poll_compactions`): the harness registry
        // entry for this table (`try_submit` registered it) has no further
        // use. No-op outside test/test-support builds.
        #[cfg(any(test, feature = "test-support"))]
        crate::compaction::cancel_harness::unregister_cancel_token(&task.table_id.to_string());
    }
}

impl Drop for CompactionExecutor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl CompactionExecutor {
    #[cfg(test)]
    fn execute_task_observing<F>(
        task: &CompactionTask,
        observe_group_width: F,
    ) -> std::result::Result<ExecutedCompaction, String>
    where
        F: FnMut(usize),
    {
        // No external cancellation source for this test-only entry point: a
        // fresh, never-cancelled token. A test that wants to exercise
        // cancellation installs a `CancelHookGuard` + calls
        // `cancel_harness::cancel_now` — `execute_task_with_policy`
        // registers whatever token it is given (including this fresh one)
        // under the task's table-id scope for the duration of the call.
        let cancel = CancelToken::new();
        Self::execute_task_inner(task, None, &cancel, observe_group_width)
    }

    /// Execute a task with input opens routed through the engine-wide reader
    /// pool when one is configured (FMEA #11), checking `cancel` at every
    /// checkpoint (T-021). Used by the worker threads with each task's own
    /// real token.
    fn execute_task_routed(
        task: &CompactionTask,
        reader_pool: Option<&CompactionReaderPool>,
        cancel: &CancelToken,
    ) -> std::result::Result<ExecutedCompaction, String> {
        Self::execute_task_inner(task, reader_pool, cancel, |_| {})
    }

    /// Execute a single compaction task by merging input SSTables into one output.
    ///
    /// **Streaming compaction**: this function uses a k-way streaming merge
    /// across the input SSTables instead of materializing them all into
    /// `BTreeMap<key, Vec<Partition>>` + `Vec<merged>` (which OOM'd on
    /// tombstone-heavy workloads with wide partitions — see
    /// `cql_timeseries2` and IoT TTL patterns).
    ///
    /// Memory cost is now O(N_input_sstables × 1 partition) at any moment,
    /// independent of the total dataset size. The output's serialization
    /// header is built from the inputs' headers (bounded compute) rather
    /// than from a full data scan.
    // `pub(crate)` so the compaction validator can drive a real compaction
    // synchronously and diff the output against its oracle. Production worker
    // threads use [`Self::execute_task_routed`] (pool-routed); this direct-open
    // entry point exists only for tests and the validator harness.
    #[cfg(any(test, feature = "compaction-validator"))]
    pub(crate) fn execute_task(
        task: &CompactionTask,
    ) -> std::result::Result<ExecutedCompaction, String> {
        // Fresh, never-cancelled token: this entry point has no submitter to
        // hand it a real one. See `execute_task_observing`'s doc comment for
        // how a test still exercises real cancellation through it.
        let cancel = CancelToken::new();
        Self::execute_task_inner(task, None, &cancel, |_| {})
    }

    fn execute_task_inner<F>(
        task: &CompactionTask,
        reader_pool: Option<&CompactionReaderPool>,
        cancel: &CancelToken,
        observe_group_width: F,
    ) -> std::result::Result<ExecutedCompaction, String>
    where
        F: FnMut(usize),
    {
        Self::execute_task_with_policy(
            task,
            reader_pool,
            configured_input_read_mode(),
            compaction_verify_output_enabled(),
            cancel,
            observe_group_width,
        )
    }

    fn execute_task_with_policy<F>(
        task: &CompactionTask,
        reader_pool: Option<&CompactionReaderPool>,
        read_mode: InputReadMode,
        verify_output: bool,
        cancel: &CancelToken,
        mut observe_group_width: F,
    ) -> std::result::Result<ExecutedCompaction, String>
    where
        F: FnMut(usize),
    {
        use crate::compaction::purge;
        use crate::flush::{FileFlushTarget, FlushTarget};
        use crate::merge;
        use crate::range_merger::ColumnOrdinalMapping;
        use ferrosa_sstable::io::FileReadAt;
        use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
        use ferrosa_sstable::writer::SSTableWriter;
        use std::collections::BinaryHeap;

        tracing::info!(
            table_id = %task.table_id,
            inputs = task.inputs.len(),
            "compaction: starting streaming task"
        );

        let task_start = Instant::now();
        let mut input_size_bytes: u64 = 0;

        // 1. Open every input SSTable.  ANY missing/corrupt input aborts the
        //    whole compaction — silent skipping previously caused data loss
        //    because swap_compacted_sstables removes all inputs.
        //
        //    When `reader_pool` is set (production), the opened reader is
        //    obtained through the engine-wide bounded reader pool so
        //    compaction's resident input readers count against — and are
        //    shared/evictable with — the same global bound as the read and
        //    startup paths (FMEA #11). The strict validation below still runs
        //    on every input regardless of cache state, so abort-on-corrupt is
        //    unchanged. Readers are held as `Arc` for the duration of the
        //    merge; the pool never evicts an in-use reader (soft cap).
        let open_start = Instant::now();
        let mut readers: Vec<Arc<SSTableReader<FileReadAt>>> =
            Vec::with_capacity(task.inputs.len());
        // This task's own count of inputs opened through the reader pool: a local,
        // so nothing running beside it can change it.
        let mut pool_input_opens = 0usize;
        let pool_table_key = task.table_id.to_string();
        for input in &task.inputs {
            cancel_point!(&pool_table_key, CancelPoint::InputOpen);
            cancel.check().map_err(|c| c.to_string())?;
            let gen = &input.id;
            let dir = &input.path;

            let data_path = dir.join(format!("{gen}-Data.db"));
            let data_file_size = ensure_compaction_component(&data_path, true, true)?
                .expect("required component returns size");
            input_size_bytes = input_size_bytes.saturating_add(data_file_size);
            tracing::info!(
                %gen,
                data_file_size,
                path = ?data_path,
                "compaction: opening input SSTable"
            );

            let data = open_input_data(&data_path, read_mode)
                .map_err(|e| format!("aborting compaction: SSTable {gen}: {e}"))?;
            let partitions_path = dir.join(format!("{gen}-Partitions.db"));
            input_size_bytes = input_size_bytes.saturating_add(
                ensure_compaction_component(&partitions_path, true, true)?
                    .expect("required component returns size"),
            );
            let partitions_file = FileReadAt::open(&partitions_path)
                .map_err(|e| format!("aborting compaction: SSTable {gen}: {e}"))?;
            let rows_path = dir.join(format!("{gen}-Rows.db"));
            input_size_bytes = input_size_bytes.saturating_add(
                ensure_compaction_component(&rows_path, true, false)?
                    .expect("required component returns size"),
            );
            let rows = FileReadAt::open(&rows_path)
                .map_err(|e| format!("aborting compaction: SSTable {gen}: {e}"))?;
            let filter_path = dir.join(format!("{gen}-Filter.db"));
            let filter = read_compaction_component(&filter_path, true, false)?
                .ok_or_else(|| format!("aborting compaction: SSTable {gen}: Filter.db missing"))?;
            input_size_bytes = input_size_bytes.saturating_add(filter.len() as u64);
            let statistics_path = dir.join(format!("{gen}-Statistics.db"));
            let statistics =
                read_compaction_component(&statistics_path, true, true)?.ok_or_else(|| {
                    format!("aborting compaction: SSTable {gen}: Statistics.db missing")
                })?;
            input_size_bytes = input_size_bytes.saturating_add(statistics.len() as u64);
            let compression_info_path = dir.join(format!("{gen}-CompressionInfo.db"));
            let compression_info = read_compaction_component(&compression_info_path, false, false)?;
            input_size_bytes = input_size_bytes.saturating_add(
                compression_info
                    .as_ref()
                    .map(|bytes| bytes.len() as u64)
                    .unwrap_or(0),
            );

            let is_compressed = compression_info.is_some();

            // Strict open: validates and aborts on corruption regardless of
            // whether the pool already has this generation cached.
            let mut reader = SSTableReader::open(SSTableComponents {
                data,
                partitions: partitions_file,
                rows,
                filter,
                compression_info,
                statistics,
            })
            .map_err(|e| format!("aborting compaction: SSTable {gen} corrupt: {e}"))?;

            // Digest.crc32/CRC.db, when present, so per-chunk CRC.db
            // verification on uncompressed reads (already wired into the read
            // path) actually runs against merge inputs instead of silently
            // opting out -- this call site previously never loaded them
            // (T-012; `publication-safety.md` M1).
            ferrosa_sstable::reader::load_checksums_for_generation(
                &mut reader,
                dir,
                gen,
                is_compressed,
            );

            // A DirectScan reader is private to this task: parked in the shared pool
            // it would be handed to the live read path, which does point reads that
            // a one-pass window cannot serve. Its residency is bounded by
            // `task.inputs.len()` x two windows for the life of the merge instead of
            // by the pool (FMEA #11 covers only the pooled, cached mode).
            let pool_for_input = match read_mode {
                InputReadMode::Cached => reader_pool,
                InputReadMode::DirectScan { .. } => None,
            };
            let reader = match pool_for_input {
                Some(pool) => {
                    // Key identically to the live read/startup path so a
                    // generation opened for reads and one opened for compaction
                    // share a single resident reader.
                    let key = (
                        pool_table_key.clone(),
                        crate::store::SstableDescriptor::gen_num_for(gen),
                    );
                    crate::metrics::inc_compaction_pool_input_opens();
                    pool_input_opens += 1;
                    // Reader is already validated; the closure runs only on a
                    // cache miss (the just-opened reader is cached), otherwise
                    // the cached reader is returned and this one is dropped.
                    pool.get_or_open(key, move || Ok::<_, String>(reader))?
                }
                None => Arc::new(reader),
            };

            readers.push(reader);
        }
        crate::metrics::observe_compaction_phase(
            crate::metrics::CompactionPhase::OpenInputs,
            open_start.elapsed(),
        );

        if readers.is_empty() {
            return Err("no input SSTables to compact".into());
        }
        let mappings: Vec<ColumnOrdinalMapping> = readers
            .iter()
            // Compaction rewrites SSTables into an SSTable, so it stays in
            // SSTable ordinal space (crate::ordinal_space).
            .map(|reader| ColumnOrdinalMapping::for_rewrite(&task.schema, reader.header()))
            .collect();

        // 2. Build the output serialization header by combining the inputs'
        //    own headers.  Each input header records the min/max ts and
        //    ldt observed in that SSTable; the union is correct for the
        //    output (it's a strict superset of what's actually written
        //    because deletions can drop cells, but conservative is fine —
        //    drivers don't depend on it being tight).  Picking ferrosa's
        //    column model from the schema mirrors the legacy
        //    `flush::build_serialization_header` behaviour.
        let header = combine_input_headers(&task.schema, &readers);
        let output_header = header.clone();
        let header_min_ts = output_header.min_timestamp;
        let header_max_ts = output_header.max_timestamp;
        tracing::info!(
            min_ts = header_min_ts,
            max_ts = header_max_ts,
            "compaction: combined output serialization header"
        );

        // Compaction verifies the promoted output below with a streaming
        // readback. Keep the writer's generic verification off here so
        // finish() does not perform a second full output scan.
        let options = crate::engine::write_options_for_schema(&task.schema, false)
            .map_err(|e| format!("compaction: invalid write options: {e}"))?;
        let flush_target = FileFlushTarget::new_starting_at(task.output_dir.clone())
            .map_err(|e| format!("flush target: {e}"))?;
        let staging_dir = flush_target
            .file_output_staging_dir()
            .map_err(|e| format!("flush staging dir: {e}"))?
            .ok_or_else(|| "file flush target did not provide staging directory".to_string())?;
        let _staging_cleanup = StagingCleanup(&staging_dir);
        let mut writer = SSTableWriter::new_file_backed_with_cancel(
            options,
            output_header.clone(),
            staging_dir.join("Data.db"),
            cancel.clone(),
        )
        .map_err(|e| format!("writer staging: {e}"))?;

        // 3. K-way streaming merge across the input partition iterators.
        //
        // Min-heap (custom `Ord` flips the comparison) yields the
        // smallest partition key.  For each minimum key we drain every
        // reader currently exposing that key (replenishing as we go),
        // run `merge::merge_partitions`, write the result, and free it.
        let mut iters: Vec<ferrosa_sstable::reader::PartitionIter<'_, FileReadAt>> =
            Vec::with_capacity(readers.len());
        for r in &readers {
            let read_start = Instant::now();
            let iter = r
                .partitions_iter()
                .map_err(|e| format!("partitions_iter: {e}"))?;
            crate::metrics::observe_compaction_phase(
                crate::metrics::CompactionPhase::MergeRead,
                read_start.elapsed(),
            );
            iters.push(iter);
        }

        // Heap entry: the Partition is moved into the heap (no key
        // clones), with a custom `Ord` that sorts by the partition's
        // own DecoratedKey in token-comparable order.  This eliminates
        // the O(N) key-allocation pressure the previous (key.clone(),
        // idx) design caused.
        struct HeapEntry {
            partition: ferrosa_sstable::types::Partition,
            reader_idx: usize,
        }
        impl PartialEq for HeapEntry {
            fn eq(&self, other: &Self) -> bool {
                self.partition.key == other.partition.key
            }
        }
        impl Eq for HeapEntry {}
        impl PartialOrd for HeapEntry {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for HeapEntry {
            // BinaryHeap is a max-heap; we want a min-heap, so flip the
            // comparison (smaller DecoratedKey "wins" the pop).
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                other.partition.key.cmp(&self.partition.key)
            }
        }

        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(iters.len());
        let mut last_input_keys: Vec<Option<ferrosa_common::DecoratedKey>> =
            vec![None; iters.len()];
        for (idx, it) in iters.iter_mut().enumerate() {
            let read_start = Instant::now();
            let next = it.next_partition().map_err(|e| format!("iter init: {e}"))?;
            crate::metrics::observe_compaction_phase(
                crate::metrics::CompactionPhase::MergeRead,
                read_start.elapsed(),
            );
            if let Some(mut partition) = next {
                validate_compaction_input_key_order(
                    &task.inputs[idx].id,
                    &last_input_keys[idx],
                    &partition.key,
                )?;
                last_input_keys[idx] = Some(partition.key.clone());
                mappings[idx].remap_partition(&mut partition);
                heap.push(HeapEntry {
                    partition,
                    reader_idx: idx,
                });
            }
        }

        let mut total_input_rows: usize = 0;
        // Counts and min/max token across all merged output partitions so the
        // emitted SSTableMetadata can be filled without a second scan.
        let mut tally = OutputTally::default();
        let table_label = task.table_id.to_string();
        // First partition that purged down to nothing, kept (unpurged) in case
        // every partition does: an empty output cannot be swapped in.
        let mut held_back: Option<ferrosa_sstable::types::Partition> = None;
        let mut purged_markers: u64 = 0;
        // Test-only: which merge-loop iteration this is, so the cancel harness
        // can distinguish the first partition from every later one.
        #[cfg(any(test, feature = "test-support"))]
        let mut merge_loop_iteration: u64 = 0;

        while let Some(top) = heap.pop() {
            #[cfg(any(test, feature = "test-support"))]
            {
                cancel_point!(
                    &pool_table_key,
                    if merge_loop_iteration == 0 {
                        CancelPoint::MergePartitionFirst
                    } else {
                        CancelPoint::MergePartitionMiddle
                    }
                );
                merge_loop_iteration += 1;
            }
            if let Err(c) = cancel.check() {
                return Err(c.to_string());
            }
            // Drain all heap entries that share this key (multiple inputs
            // wrote the same partition).
            let HeapEntry {
                partition: first_partition,
                reader_idx: first_idx,
            } = top;
            // We need to compare future heap tops against this key to
            // collect duplicates. The partition itself moves into the
            // group; cheap to compare via a reference into `group`
            // afterward.
            total_input_rows += first_partition.rows.len();
            let mut group: Vec<ferrosa_sstable::types::Partition> = Vec::with_capacity(1);
            group.push(first_partition);
            // Advance reader first_idx.
            let read_start = Instant::now();
            let next = iters[first_idx]
                .next_partition()
                .map_err(|e| format!("iter advance: {e}"))?;
            crate::metrics::observe_compaction_phase(
                crate::metrics::CompactionPhase::MergeRead,
                read_start.elapsed(),
            );
            if let Some(next) = next {
                let mut next = next;
                validate_compaction_input_key_order(
                    &task.inputs[first_idx].id,
                    &last_input_keys[first_idx],
                    &next.key,
                )?;
                last_input_keys[first_idx] = Some(next.key.clone());
                mappings[first_idx].remap_partition(&mut next);
                heap.push(HeapEntry {
                    partition: next,
                    reader_idx: first_idx,
                });
            }
            // Drain other readers sitting at the same key.
            while heap.peek().map(|h| h.partition.key == group[0].key) == Some(true) {
                let HeapEntry {
                    partition,
                    reader_idx,
                } = heap.pop().expect("peek implies pop");
                total_input_rows += partition.rows.len();
                group.push(partition);
                let read_start = Instant::now();
                let next = iters[reader_idx]
                    .next_partition()
                    .map_err(|e| format!("iter advance: {e}"))?;
                crate::metrics::observe_compaction_phase(
                    crate::metrics::CompactionPhase::MergeRead,
                    read_start.elapsed(),
                );
                if let Some(next) = next {
                    let mut next = next;
                    validate_compaction_input_key_order(
                        &task.inputs[reader_idx].id,
                        &last_input_keys[reader_idx],
                        &next.key,
                    )?;
                    last_input_keys[reader_idx] = Some(next.key.clone());
                    mappings[reader_idx].remap_partition(&mut next);
                    heap.push(HeapEntry {
                        partition: next,
                        reader_idx,
                    });
                }
            }

            observe_group_width(group.len());

            let merge_start = Instant::now();
            let mut merged = merge::merge_partitions(group);
            // Compaction output MUST be monotonic. merge_partitions leaves a
            // single-source partition in its on-disk order, which for a legacy
            // SSTable can be non-monotonic (t_a0f922a3) — this rewrites it in
            // clustering order. Cheap O(n) sorted-check for the common already
            // -sorted case (all multi-source merges, all modern files).
            merge::ensure_partition_rows_sorted(&mut merged);
            crate::metrics::observe_compaction_phase(
                crate::metrics::CompactionPhase::MergePartition,
                merge_start.elapsed(),
            );
            if let Some(policy) = task.purge.as_ref() {
                // Physically reclaim rows a table tombstone (TRUNCATE) covers. The
                // logical effect is already immediate on every read; this is what
                // makes the space come back. The reserved table-tombstone partition
                // is exempt from the purge below — it may only be dropped once every
                // replica has purged the data it covers, which is not tracked yet, so
                // it is retained until the table is dropped.
                merge::reclaim_covers_table(&mut merged, policy.table_delete);
                if purge::has_purgeable_marker(&merged, policy)
                    && !crate::table_tombstone::is_table_tombstone_key(&merged.key)
                {
                    let original = held_back.is_none().then(|| merged.clone());
                    purged_markers += purge::purge_partition(&mut merged, policy).markers();
                    if purge::is_empty_partition(&merged) {
                        if held_back.is_none() {
                            held_back = original;
                        }
                        continue;
                    }
                }
            }
            emit_partition(
                &mut writer,
                &output_header,
                &table_label,
                merged,
                &mut tally,
            )?;
        }

        if tally.partitions == 0 {
            // Every partition purged away. Write the first one back unpurged so the
            // output is non-empty (the swap needs an output); it is dropped by the
            // next compaction that has other data. Counted so it is visible.
            let Some(kept) = held_back.take() else {
                return Err("no partitions to compact".into());
            };
            crate::metrics::inc_compaction_purge_held_back();
            tracing::warn!(
                table_id = %task.table_id,
                "compaction: every partition purged away; writing one unpurged partition \
                 so the output is not empty"
            );
            emit_partition(&mut writer, &output_header, &table_label, kept, &mut tally)?;
        }
        if purged_markers > 0 {
            crate::metrics::add_compaction_purged_markers(purged_markers);
            tracing::info!(
                table_id = %task.table_id,
                purged_markers,
                "compaction: dropped deletion markers past gc_grace_seconds"
            );
        }
        let merged_partition_count = tally.partitions;
        let merged_row_count = tally.rows;
        let (min_token, max_token) = (tally.min_token, tally.max_token);
        cancel_point!(&pool_table_key, CancelPoint::MergePartitionLast);
        if let Err(c) = cancel.check() {
            return Err(c.to_string());
        }

        tracing::info!(
            partitions = merged_partition_count,
            merged_row_count,
            total_input_rows,
            "compaction: streaming merge complete"
        );
        if merged_row_count < total_input_rows {
            // Reduction is the expected outcome whenever two input SSTables
            // touch the same (partition_key, clustering) tuple — that pair
            // collapses to a single output row via cell-level LWW (see
            // `merge::merge_partitions`). Tombstones suppressing older data
            // also reduce the count. Both are correct, normal compaction
            // semantics, not data loss.
            //
            // The actual data-loss check is the streaming readback below
            // (step 5): if the SSTable we just wrote disagrees with the
            // counts we computed in-memory, *that* is the ERROR we want
            // surfaced. This collapse signal is INFO so it stays useful for
            // post-mortems (e.g., "compaction collapsed N rows on table X
            // at time T") without polluting steady-state cluster logs.
            tracing::info!(
                total_input_rows,
                merged_row_count,
                collapsed = total_input_rows - merged_row_count,
                "compaction: rows collapsed by LWW or tombstone suppression (expected)"
            );
        }

        cancel_point!(&pool_table_key, CancelPoint::BeforeFinish);
        if let Err(c) = cancel.check() {
            return Err(c.to_string());
        }
        let finish_start = Instant::now();
        let output = writer
            .finish_to_directory_deferred_sync(&staging_dir)
            .map_err(|e| format!("finish: {e}"))?;
        crate::metrics::observe_compaction_phase(
            crate::metrics::CompactionPhase::WriterFinish,
            finish_start.elapsed(),
        );

        // Test-only fault injection point: lets a test corrupt the staged
        // output (still under `staging_dir`, before `flush_files` renames,
        // fsyncs and digest-verifies it) to prove the digest check in
        // `flush_files` below is unconditional -- including for compaction,
        // regardless of `FERROSA_COMPACTION_VERIFY_OUTPUT` (T-012).
        #[cfg(test)]
        COMPACTION_OUTPUT_HOOK.with(|h| {
            if let Some(hook) = h.borrow().as_ref() {
                hook(&output);
            }
        });

        let direct_upload = None;

        // 4. Promote staged output files via FileFlushTarget.
        cancel_point!(&pool_table_key, CancelPoint::BeforeFlushFiles);
        if let Err(c) = cancel.check() {
            return Err(c.to_string());
        }
        let local_write_start = Instant::now();
        let reader = flush_target
            .flush_deferred_files(output)
            .map_err(|e| format!("flush output: {e}"))?;
        crate::metrics::observe_compaction_phase(
            crate::metrics::CompactionPhase::LocalWriteSstable,
            local_write_start.elapsed(),
        );

        if verify_output {
            // 5. Streaming readback verification — count partitions and rows
            //    without materializing the output back into a Vec.  Catches
            //    Data.db / Partitions.db inconsistencies that would corrupt
            //    later reads.
            let mut readback_partitions: u64 = 0;
            let mut readback_rows: usize = 0;
            let verify_start = Instant::now();
            {
                let mut iter = reader
                    .partitions_iter()
                    .map_err(|e| format!("CORRUPTION: output partitions_iter failed: {e}"))?;
                while let Some(p) = iter
                    .next_partition()
                    .map_err(|e| format!("CORRUPTION: output read failed: {e}"))?
                {
                    cancel_point!(&pool_table_key, CancelPoint::VerifyPartition);
                    if let Err(c) = cancel.check() {
                        remove_staged_output_components(
                            &task.output_dir,
                            &flush_target.generation().to_string(),
                        );
                        return Err(c.to_string());
                    }
                    readback_partitions += 1;
                    readback_rows += p.rows.len();
                }
            }
            crate::metrics::observe_compaction_phase(
                crate::metrics::CompactionPhase::OutputVerify,
                verify_start.elapsed(),
            );
            if readback_partitions != merged_partition_count || readback_rows != merged_row_count {
                tracing::error!(
                    written_partitions = merged_partition_count,
                    written_rows = merged_row_count,
                    readback_partitions,
                    readback_rows,
                    "compaction: CORRUPTION DETECTED in output SSTable"
                );
                return Err(format!(
                    "compaction output SSTable is corrupt: expected {} partitions/{} rows, \
                     readback got {} partitions/{} rows",
                    merged_partition_count, merged_row_count, readback_partitions, readback_rows
                ));
            }
            tracing::info!(
                partitions = readback_partitions,
                rows = readback_rows,
                "compaction: output verified (streaming readback matches merge)"
            );
        }

        let gen = flush_target.generation();
        let output_id = format!("{gen}");
        let partition_count = merged_partition_count;

        let total_size: u64 = [
            format!("{gen}-Data.db"),
            format!("{gen}-Partitions.db"),
            format!("{gen}-Rows.db"),
            format!("{gen}-Filter.db"),
            format!("{gen}-Statistics.db"),
            format!("{gen}-TOC.txt"),
            format!("{gen}-CompressionInfo.db"),
        ]
        .iter()
        .filter_map(|name| {
            let path = task.output_dir.join(name);
            std::fs::metadata(&path).ok().map(|m| m.len())
        })
        .sum();

        // min/max token tracked inline during the streaming merge; if the
        // merge produced zero partitions we'd have returned above.

        // Use the combined-header timestamps (from input headers) for the
        // output metadata. Input metadata may have stale/incorrect values
        // that would propagate; the header values are authoritative.
        crate::metrics::observe_compaction_completed(
            task_start.elapsed(),
            input_size_bytes,
            total_size,
            total_input_rows as u64,
            merged_row_count as u64,
            partition_count,
        );
        Ok(ExecutedCompaction {
            metadata: SSTableMetadata {
                id: output_id,
                path: task.output_dir.clone(),
                size_bytes: total_size,
                min_token,
                max_token,
                min_timestamp: header_min_ts,
                max_timestamp: header_max_ts,
                partition_count,
                // Compaction always writes byte-comparable (BTI) output, so the
                // rewritten SSTable is never legacy-format — this is precisely how
                // a legacy file gets fixed (t_a0f922a3).
                legacy_format: false,
            },
            direct_upload,
            pool_input_opens,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ExecutedCompaction {
    pub metadata: SSTableMetadata,
    pub direct_upload: Option<CompactionDirectUpload>,
    /// How many of THIS task's inputs were opened through the engine-wide reader
    /// pool. The process-global `compaction_pool_input_opens_total` metric counts
    /// every task in the process, so a test (or anything else) that wants to know
    /// what one task did cannot read it while other tasks run in parallel.
    pub pool_input_opens: usize,
}

/// Build an output `SerializationHeader` from the inputs' own headers
/// plus the (current) table schema, in `O(N_inputs)` time and without
/// scanning Data.db.
///
/// Each input header already records the timestamp / ldt / ttl ranges
/// observed when that SSTable was written; the output is the union of
/// those ranges. The column model (key type, clustering, statics,
/// regular columns) comes from the schema — same convention as the
/// flush path used to use via `flush::build_serialization_header`.
fn combine_input_headers<R: ferrosa_sstable::io::ReadAt>(
    schema: &ferrosa_common::schema::TableSchema,
    readers: &[Arc<ferrosa_sstable::reader::SSTableReader<R>>],
) -> ferrosa_sstable::statistics::SerializationHeader {
    use ferrosa_common::{NO_DELETION_TIME, NO_TIMESTAMP, NO_TTL};
    use ferrosa_sstable::statistics::SerializationHeader;

    // The output SSTable must use the current schema's column model. Inputs
    // may have legacy physical column order, and the executor remaps decoded
    // cells to this schema before merge/write.
    let template = crate::flush::build_serialization_header(schema, &[]);

    let mut min_timestamp = NO_TIMESTAMP;
    let mut max_timestamp = i64::MIN;
    let mut min_local_deletion_time = NO_DELETION_TIME;
    let mut min_ttl = NO_TTL;
    // Propagate complex-collection framing: if ANY input SSTable stores complex
    // (per-element) columns, the compacted output must too, or the merged cell
    // paths would be dropped and the collection corrupted (D-write, t_83c4f093).
    let mut has_complex = false;

    for r in readers {
        let h = r.header();
        has_complex |= h.complex_collections;
        if h.min_timestamp != NO_TIMESTAMP
            && (min_timestamp == NO_TIMESTAMP || h.min_timestamp < min_timestamp)
        {
            min_timestamp = h.min_timestamp;
        }
        if h.max_timestamp > max_timestamp {
            max_timestamp = h.max_timestamp;
        }
        if h.min_local_deletion_time != NO_DELETION_TIME
            && (min_local_deletion_time == NO_DELETION_TIME
                || h.min_local_deletion_time < min_local_deletion_time)
        {
            min_local_deletion_time = h.min_local_deletion_time;
        }
        if h.min_ttl != NO_TTL && (min_ttl == NO_TTL || h.min_ttl < min_ttl) {
            min_ttl = h.min_ttl;
        }
    }

    if max_timestamp == i64::MIN {
        max_timestamp = NO_TIMESTAMP;
    }

    let mut header = SerializationHeader {
        complex_collections: has_complex,
        min_timestamp,
        max_timestamp,
        min_local_deletion_time,
        min_ttl,
        ..template
    };
    // A simple-framed input may hold whole-value collection cells, which a
    // complex-framed output expands (`emit_partition`) into elements plus a
    // deletion sentinel one microsecond older than the blob.
    if has_complex && readers.iter().any(|r| !r.header().complex_collections) {
        crate::memtable::widen_header_for_blob_sentinels(&mut header);
    }
    header
}

/// Counts and token span of the partitions written to the compaction output.
struct OutputTally {
    partitions: u64,
    rows: usize,
    min_token: i64,
    max_token: i64,
}

impl Default for OutputTally {
    fn default() -> Self {
        Self {
            partitions: 0,
            rows: 0,
            min_token: i64::MAX,
            max_token: i64::MIN,
        }
    }
}

/// Validate and write one merged partition, recording it in `tally`.
fn emit_partition(
    writer: &mut ferrosa_sstable::writer::SSTableWriter,
    header: &ferrosa_sstable::statistics::SerializationHeader,
    table: &str,
    mut merged: ferrosa_sstable::types::Partition,
    tally: &mut OutputTally,
) -> std::result::Result<(), String> {
    let write_start = Instant::now();
    // A legacy simple-framed input may carry whole-value collection cells that
    // a complex-framed output cannot hold as-is. The merged partition is
    // owned here, so it is expanded in place, never copied.
    crate::memtable::expand_collection_blobs_in_place(&mut merged, header, table)
        .map_err(|e| format!("write partition: {e}"))?;
    let merged = &merged;
    validate_partition_writable(merged, header).map_err(|e| format!("write partition: {e}"))?;
    writer
        .add_partition(merged)
        .map_err(|e| format!("write partition: {e}"))?;
    let token = merged.key.token.0;
    tally.rows += merged.rows.len();
    tally.partitions += 1;
    tally.min_token = tally.min_token.min(token);
    tally.max_token = tally.max_token.max(token);
    crate::metrics::observe_compaction_phase(
        crate::metrics::CompactionPhase::WriterAddPartition,
        write_start.elapsed(),
    );
    Ok(())
}

fn validate_partition_writable(
    partition: &ferrosa_sstable::types::Partition,
    header: &ferrosa_sstable::statistics::SerializationHeader,
) -> std::result::Result<(), String> {
    if let Some(static_row) = &partition.static_row {
        validate_row_writable(static_row, true, partition, header)?;
    }
    for row in &partition.rows {
        validate_row_writable(row, false, partition, header)?;
    }
    Ok(())
}

fn validate_compaction_input_key_order(
    gen: &str,
    previous: &Option<ferrosa_common::DecoratedKey>,
    next: &ferrosa_common::DecoratedKey,
) -> std::result::Result<(), String> {
    if let Some(previous) = previous {
        if next <= previous {
            return Err(format!(
                "aborting compaction: SSTable {gen} corrupt: Data.db partitions out of token order: \
                 key {:?} token {} <= previous key {:?} token {}",
                next.key.as_bytes(),
                next.token.0,
                previous.key.as_bytes(),
                previous.token.0
            ));
        }
    }
    Ok(())
}

fn validate_row_writable(
    row: &ferrosa_sstable::types::Row,
    is_static: bool,
    partition: &ferrosa_sstable::types::Partition,
    header: &ferrosa_sstable::statistics::SerializationHeader,
) -> std::result::Result<(), String> {
    use ferrosa_common::NO_TIMESTAMP;

    let row_kind = if is_static { "static row" } else { "row" };
    if row.primary_key_liveness.has_timestamp()
        && row.primary_key_liveness.timestamp < header.min_timestamp
    {
        return Err(format!(
            "invalid {row_kind} primary-key timestamp {} is below output header min_timestamp {} for partition token {}; original SSTables are preserved and startup repair/quarantine should remove the corrupt input",
            row.primary_key_liveness.timestamp,
            header.min_timestamp,
            partition.key.token.0
        ));
    }
    if !row.deletion.is_live() && row.deletion.marked_for_delete_at < header.min_timestamp {
        return Err(format!(
            "invalid {row_kind} deletion timestamp {} is below output header min_timestamp {} for partition token {}; original SSTables are preserved and startup repair/quarantine should remove the corrupt input",
            row.deletion.marked_for_delete_at,
            header.min_timestamp,
            partition.key.token.0
        ));
    }

    for (column_idx, cell) in &row.cells {
        let uses_row_timestamp = row.primary_key_liveness.has_timestamp()
            && cell.timestamp == row.primary_key_liveness.timestamp;
        if !uses_row_timestamp {
            if cell.timestamp == NO_TIMESTAMP {
                return Err(format!(
                    "invalid {row_kind} cell at column {column_idx} has NO_TIMESTAMP for partition token {}; original SSTables are preserved and startup repair/quarantine should remove the corrupt input",
                    partition.key.token.0
                ));
            }
            if cell.timestamp < header.min_timestamp {
                return Err(format!(
                    "invalid {row_kind} cell timestamp {} is below output header min_timestamp {} at column {column_idx} for partition token {}; original SSTables are preserved and startup repair/quarantine should remove the corrupt input",
                    cell.timestamp,
                    header.min_timestamp,
                    partition.key.token.0
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn register_pressure_task(
        executor: &CompactionExecutor,
        id: &str,
        bytes: u64,
    ) -> (CompactionTask, CancelToken) {
        let task = CompactionTask {
            inputs: vec![make_metadata(id, bytes)],
            output_dir: PathBuf::from("unused"),
            schema: test_table_schema(),
            table_id: test_table_id(),
            purge: None,
        };
        let ticket = executor.submission_ticket(&task.table_id).unwrap();
        let token = executor.tracker.try_register(&task, &ticket).unwrap();
        (task, token)
    }

    /// The planner must be able to see how deep the compaction pipeline is and
    /// defer before it pays a full planning round.
    ///
    /// Compaction task and result queues are one and two deep per worker, so the
    /// pipeline saturates almost immediately under load. `maybe_compact` then
    /// paid the whole cost of `select` plus one metadata rescan per emitted task
    /// only to find every worker queue full and return without submitting
    /// anything: the quadratic planning work (PR #517) multiplied by the
    /// flush-rate and 10 s tick, buying no throughput and holding the async
    /// maintenance task that should be draining results. These are the pure
    /// decision helpers the planner gates on.
    #[test]
    fn compaction_backpressure_reports_pipeline_saturation() {
        assert_eq!(compaction_pressure(0, 8), 0.0);
        assert!(compaction_pressure(1, 8) > 0.0);
        assert_eq!(compaction_pressure(8, 8), 1.0);
        assert!(
            compaction_pressure(7, 8) > compaction_pressure(3, 8),
            "pressure must be monotonic in the in-flight task count"
        );
        // A zero capacity must not divide by zero; treat it as no pressure.
        assert_eq!(compaction_pressure(0, 0), 0.0);
        assert_eq!(compaction_pressure(4, 0), 0.0);
    }

    #[test]
    fn compaction_backpressure_defers_at_operator_threshold() {
        // Default threshold is 1.0: only a fully saturated pipeline defers, so
        // this changes nothing for an idle or lightly loaded executor.
        assert!(!compaction_planning_deferred(0.0, 1.0));
        assert!(compaction_planning_deferred(1.0, 1.0));
        // A half-full pipeline with an operator-set 0.5 threshold defers.
        assert!(compaction_planning_deferred(0.5, 0.5));
        assert!(!compaction_planning_deferred(0.49, 0.5));
        // A zero threshold disables the gate entirely, for operators who would
        // rather always attempt a plan than ever defer one.
        assert!(!compaction_planning_deferred(0.0, 0.0));
        assert!(!compaction_planning_deferred(1.0, 0.0));
    }

    #[test]
    fn compaction_backpressure_executor_in_flight_count() {
        let executor = CompactionExecutor::new();
        assert_eq!(executor.compaction_in_flight(), 0);
        let (task, _token) = register_pressure_task(&executor, "a", 1);
        assert_eq!(executor.compaction_in_flight(), 1);
        executor.release_task_inputs(&task);
        assert_eq!(executor.compaction_in_flight(), 0);
    }

    #[test]
    fn cancel_source_disk_reserve_selects_largest_and_coalesces_until_release() {
        let executor = CompactionExecutor::new();
        let (small_task, small) = register_pressure_task(&executor, "small", 10);
        let (large_task, large) = register_pressure_task(&executor, "large", 100);
        assert_eq!(executor.cancel_largest_for_disk_reserve(), Some(100));
        assert_eq!(large.reason(), Some(CancelReason::DiskReserve));
        assert!(!small.is_cancelled());
        assert_eq!(executor.cancel_largest_for_disk_reserve(), None);
        executor.release_task_inputs(&large_task);
        assert_eq!(executor.cancel_largest_for_disk_reserve(), Some(10));
        assert_eq!(small.reason(), Some(CancelReason::DiskReserve));
        executor.release_task_inputs(&small_task);
    }

    #[test]
    fn cancel_source_disk_reserve_skips_tasks_cancelled_by_other_sources() {
        let executor = CompactionExecutor::new();
        let (small_task, small) = register_pressure_task(&executor, "small", 10);
        let (large_task, large) = register_pressure_task(&executor, "large", 100);
        large.cancel(CancelReason::Operator);
        assert_eq!(executor.cancel_largest_for_disk_reserve(), Some(10));
        assert_eq!(large.reason(), Some(CancelReason::Operator));
        assert_eq!(small.reason(), Some(CancelReason::DiskReserve));
        executor.release_task_inputs(&small_task);
        executor.release_task_inputs(&large_task);
    }

    #[test]
    fn cancel_source_result_full_queue_wakes_on_cancel() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        tx.send(11).unwrap();
        let pending = AtomicUsize::new(1);
        let (_shutdown, shutdown_rx) = crossbeam_channel::bounded(0);
        let cancel = CancelToken::new();
        std::thread::scope(|scope| {
            let (entered_tx, entered_rx) = crossbeam_channel::bounded(0);
            let (done_tx, done_rx) = crossbeam_channel::bounded(0);
            let (tx, pending, cancel, shutdown_rx) = (&tx, &pending, &cancel, &shutdown_rx);
            scope.spawn(move || {
                entered_tx.send(()).unwrap();
                done_tx
                    .send(send_result_or_cancel(tx, 22, pending, cancel, shutdown_rx))
                    .unwrap();
            });
            entered_rx.recv().unwrap();
            cancel.cancel(CancelReason::Operator);
            assert_eq!(
                done_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap(),
                Some(22)
            );
        });
        assert_eq!(pending.load(Ordering::Acquire), 1);
        assert_eq!(rx.recv().unwrap(), 11);
    }

    #[test]
    fn cancel_source_result_full_queue_wakes_on_shutdown() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        tx.send(11).unwrap();
        let pending = AtomicUsize::new(1);
        let (shutdown, shutdown_rx) = crossbeam_channel::bounded(0);
        let cancel = CancelToken::new();
        std::thread::scope(|scope| {
            let (entered_tx, entered_rx) = crossbeam_channel::bounded(0);
            let (done_tx, done_rx) = crossbeam_channel::bounded(0);
            let (tx, pending, cancel, shutdown_rx) = (&tx, &pending, &cancel, &shutdown_rx);
            scope.spawn(move || {
                entered_tx.send(()).unwrap();
                done_tx
                    .send(send_result_or_cancel(tx, 22, pending, cancel, shutdown_rx))
                    .unwrap();
            });
            entered_rx.recv().unwrap();
            drop(shutdown);
            assert_eq!(
                done_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap(),
                Some(22)
            );
        });
        assert_eq!(pending.load(Ordering::Acquire), 1);
        assert_eq!(rx.recv().unwrap(), 11);
    }

    #[test]
    fn cancel_source_result_counter_precedes_delivery_and_disconnect_reverses_it() {
        let (tx, rx) = crossbeam_channel::bounded(0);
        let pending = AtomicUsize::new(0);
        let (_shutdown, shutdown_rx) = crossbeam_channel::bounded(0);
        let cancel = CancelToken::new();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                assert_eq!(
                    send_result_or_cancel(&tx, 22, &pending, &cancel, &shutdown_rx),
                    None
                );
            });
            assert_eq!(rx.recv().unwrap(), 22);
            assert_eq!(pending.fetch_sub(1, Ordering::AcqRel), 1);
        });
        drop(rx);
        assert_eq!(
            send_result_or_cancel(&tx, 33, &pending, &cancel, &shutdown_rx),
            Some(33)
        );
        assert_eq!(pending.load(Ordering::Acquire), 0);
    }

    fn make_metadata(id: &str, size: u64) -> SSTableMetadata {
        SSTableMetadata {
            id: id.to_string(),
            path: PathBuf::from(format!("/tmp/{id}")),
            size_bytes: size,
            min_token: -100,
            max_token: 100,
            min_timestamp: 1000,
            max_timestamp: 2000,
            partition_count: 10,
            legacy_format: false,
        }
    }

    fn test_table_schema() -> ferrosa_common::schema::TableSchema {
        ferrosa_common::schema::TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![],
            extensions: Default::default(),
        }
    }

    fn test_table_id() -> crate::TableId {
        crate::TableId::new("test_ks", "test_table")
    }

    fn collect_reader_partitions<R: ferrosa_sstable::io::ReadAt>(
        reader: &ferrosa_sstable::reader::SSTableReader<R>,
    ) -> Vec<ferrosa_sstable::types::Partition> {
        let mut partitions = Vec::new();
        let mut iter = reader.partitions_iter().expect("stream partitions");
        while let Some(partition) = iter.next_partition().expect("read streamed partition") {
            partitions.push(partition);
        }
        partitions
    }

    /// Open an on-disk SSTable whose component files are `{dir}/{gen}-*.db`.
    fn open_sstable_reader(
        dir: &std::path::Path,
        gen: &str,
    ) -> ferrosa_sstable::reader::SSTableReader<ferrosa_sstable::io::FileReadAt> {
        let openf = |suffix: &str| {
            ferrosa_sstable::io::FileReadAt::open(dir.join(format!("{gen}-{suffix}"))).unwrap()
        };
        ferrosa_sstable::reader::SSTableReader::open(ferrosa_sstable::reader::SSTableComponents {
            data: openf("Data.db"),
            partitions: openf("Partitions.db"),
            rows: openf("Rows.db"),
            filter: std::fs::read(dir.join(format!("{gen}-Filter.db"))).unwrap(),
            compression_info: std::fs::read(dir.join(format!("{gen}-CompressionInfo.db"))).ok(),
            statistics: std::fs::read(dir.join(format!("{gen}-Statistics.db"))).unwrap(),
        })
        .unwrap()
    }

    /// Finding 1 regression (t_a0f922a3): compacting a SINGLE legacy SSTable
    /// whose rows are stored OUT of clustering order must produce MONOTONIC
    /// output. `merge_partitions` passes a single source through unchanged, so
    /// without `ensure_partition_rows_sorted` in the executor the rewrite would
    /// keep the bad order while stamping byte-comparable bounds — permanently
    /// non-monotonic and never re-selected.
    #[test]
    fn compaction_resorts_single_misordered_legacy_sstable() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();
        let cks = |p: &ferrosa_sstable::types::Partition| -> Vec<i32> {
            p.rows
                .iter()
                .map(|r| i32::from_be_bytes(r.clustering[..4].try_into().unwrap()))
                .collect()
        };
        let row = |n: i32| Row {
            clustering: n.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(format!("v{n}").into_bytes(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        // One partition, rows physically out of clustering order: 5,1,2,3,4.
        let misordered = ferrosa_sstable::types::Partition {
            key: DecoratedKey::new(PartitionKey::new(b"P".to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row(5), row(1), row(2), row(3), row(4)],
        };

        let dir = tmp.path().join("legacy");
        std::fs::create_dir_all(&dir).unwrap();
        let mut meta = write_sstable_to_dir(&dir, &[misordered], &schema);
        meta.legacy_format = true;

        // Precondition: the input SSTable really stores rows out of order.
        let in_reader = open_sstable_reader(&dir, &meta.id);
        let input = collect_reader_partitions(&in_reader);
        assert_eq!(
            cks(&input[0]),
            vec![5, 1, 2, 3, 4],
            "precondition: writer preserves the misordered rows on disk"
        );

        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();
        let task = CompactionTask {
            inputs: vec![meta],
            output_dir: output_dir.clone(),
            schema: schema.clone(),
            table_id: test_table_id(),
            purge: None,
        };
        let out_meta = CompactionExecutor::execute_task(&task)
            .expect("single-input legacy compaction must succeed")
            .metadata;

        let out_reader = open_sstable_reader(&output_dir, &out_meta.id);
        let output = collect_reader_partitions(&out_reader);
        assert_eq!(output.len(), 1);
        assert_eq!(
            cks(&output[0]),
            vec![1, 2, 3, 4, 5],
            "compaction MUST rewrite a single misordered legacy SSTable in monotonic order"
        );
    }

    #[test]
    fn submit_and_poll_result() {
        let executor = CompactionExecutor::new();

        let task = CompactionTask {
            inputs: vec![make_metadata("a", 1000), make_metadata("b", 2000)],
            output_dir: PathBuf::from("/tmp/output"),
            schema: test_table_schema(),
            table_id: test_table_id(),
            purge: None,
        };

        executor.submit(task).unwrap();

        // Wait for the task to complete (or fail).
        std::thread::sleep(std::time::Duration::from_millis(500));

        // Real execute_task does file I/O — with fake paths it will fail,
        // so we expect no results (error is logged, not sent).
        let results = executor.poll_results();
        assert_eq!(results.len(), 0);

        executor.shutdown();
    }

    fn real_compaction_task(tmp: &std::path::Path) -> CompactionTask {
        let schema = test_schema_with_columns();
        let mut inputs = Vec::new();
        for (name, range, ts) in [("a", 0..20, 1000), ("b", 10..30, 2000)] {
            let dir = tmp.join(format!("in_{name}"));
            std::fs::create_dir_all(&dir).unwrap();
            let partitions: Vec<_> = range
                .map(|i| make_test_partition(&format!("key_{i:04}"), "v", ts))
                .collect();
            inputs.push(write_sstable_to_dir(&dir, &partitions, &schema));
        }
        let output_dir = tmp.join("out");
        std::fs::create_dir_all(&output_dir).unwrap();
        CompactionTask {
            inputs,
            output_dir,
            schema,
            table_id: test_table_id(),
            purge: None,
        }
    }

    /// Waiting is a completion signal, not a wall-clock poll, and it must not consume
    /// the result: the caller's next poll still gets it.
    #[test]
    fn await_result_available_blocks_until_done_without_consuming_the_result() {
        let tmp = tempfile::tempdir().unwrap();
        let executor = CompactionExecutor::new();
        executor.submit(real_compaction_task(tmp.path())).unwrap();

        assert!(
            executor.await_result_available(std::time::Duration::from_secs(60)),
            "a real compaction produces a result"
        );
        // Asking again must not lose or duplicate it.
        assert!(executor.await_result_available(std::time::Duration::from_secs(60)));
        let results = executor.poll_results();
        assert_eq!(
            results.len(),
            1,
            "the awaited result is still delivered, once"
        );
        assert!(
            !executor.await_result_available(std::time::Duration::from_millis(20)),
            "nothing is pending once it was delivered"
        );
        executor.shutdown();
    }

    #[test]
    fn writer_callers_compaction_publishes_without_raw_scratch() {
        for compression in ["lz4", "none"] {
            let tmp = tempfile::tempdir().unwrap();
            let mut task = real_compaction_task(tmp.path());
            task.schema
                .extensions
                .insert("compression.class".into(), compression.into());
            let output = CompactionExecutor::execute_task(&task).unwrap();
            assert_eq!(output.metadata.partition_count, 30);
            assert_no_raw_scratch(&task.output_dir);
            let staged = task.output_dir.join(".sstable-staging");
            assert!(!staged.exists(), "successful publication removes staging");
        }
    }

    fn assert_no_raw_scratch(dir: &std::path::Path) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            assert_ne!(entry.file_name(), "Data.raw");
            if entry.file_type().unwrap().is_dir() {
                assert_no_raw_scratch(&entry.path());
            }
        }
    }

    #[test]
    fn writer_callers_compaction_cancellation_reaches_writer_and_cleans_staging() {
        let tmp = tempfile::tempdir().unwrap();
        let mut task = real_compaction_task(tmp.path());
        // Uncompressed writes reach the pump immediately, so this test isolates
        // token propagation from the compressor's bounded batch buffering.
        task.schema
            .extensions
            .insert("compression.class".into(), "none".into());
        let cancel = CancelToken::new();
        let err = match CompactionExecutor::execute_task_inner(&task, None, &cancel, |_| {
            // This callback runs after the partition checkpoint and immediately
            // before emission. Only the writer sees cancellation for this row.
            cancel.cancel(CancelReason::Operator);
        }) {
            Ok(_) => panic!("cancellation must stop the writer"),
            Err(err) => err,
        };
        assert!(
            err.contains("write partition"),
            "cancel must reach the pump: {err}"
        );
        let staging_root = task.output_dir.join(".sstable-staging");
        assert_eq!(std::fs::read_dir(staging_root).unwrap().count(), 0);
        for input in &task.inputs {
            assert!(
                input.path.join(format!("{}-Data.db", input.id)).exists(),
                "cancel must preserve input files"
            );
        }
    }

    #[test]
    fn await_result_available_reports_false_when_nothing_is_running() {
        let executor = CompactionExecutor::new();
        assert!(!executor.await_result_available(std::time::Duration::from_millis(20)));
        executor.shutdown();
    }

    #[test]
    fn shutdown_stops_cleanly() {
        let executor = CompactionExecutor::new();
        executor.shutdown();
        // Should not hang or panic.
    }

    #[test]
    fn successful_inputs_remain_claimed_until_result_finalization() {
        let executor = CompactionExecutor::new();
        let task = CompactionTask {
            inputs: vec![make_metadata("a", 1000), make_metadata("b", 2000)],
            output_dir: PathBuf::from("/tmp/output"),
            schema: test_table_schema(),
            table_id: test_table_id(),
            purge: None,
        };

        let ticket = executor.submission_ticket(&task.table_id).unwrap();
        assert!(executor.tracker.try_register(&task, &ticket).is_some());
        assert!(
            executor.tracker.try_register(&task, &ticket).is_none(),
            "inputs remain claimed until finalization"
        );
        executor.release_task_inputs(&task);
        assert!(executor.tracker.try_register(&task, &ticket).is_some());
        executor.release_task_inputs(&task);
        executor.shutdown();
    }

    /// t_8aae3ed7: a compaction that panics must release its input claims.
    /// Before, the panic killed the worker thread with the claims held, so
    /// the table never drained and TRUNCATE / DROP waited on it forever.
    #[test]
    fn a_panicking_compaction_releases_its_input_claims() {
        let tmp = tempfile::tempdir().unwrap();
        let mut task = real_compaction_task(tmp.path());
        // A table no other test uses: the hook below is keyed by table id.
        task.table_id = crate::TableId::new("panic_ks", "panic_compaction");
        let table = task.table_id.clone();
        let executor = CompactionExecutor::new();
        let hook = crate::compaction::cancel_harness::CancelHookGuard::install(
            table.to_string(),
            Arc::new(|_| panic!("injected compaction panic")),
        );
        let failed_before = crate::metrics::compaction_panics_total();

        assert!(executor.try_submit(task.clone()).unwrap());
        let started = Instant::now();
        while executor.table_has_tasks(&table) {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(30),
                "a panicked compaction leaked its input claims"
            );
            std::thread::yield_now();
        }
        assert!(crate::metrics::compaction_panics_total() > failed_before);
        drop(hook);

        assert!(
            executor.try_submit(task).unwrap(),
            "the inputs are claimable again"
        );
        assert!(
            executor.await_result_available(std::time::Duration::from_secs(30)),
            "the same inputs compact once the panic is gone"
        );
        executor.shutdown();
    }

    /// Helper: write a real SSTable to disk from partitions.
    fn write_sstable_to_dir(
        dir: &std::path::Path,
        partitions: &[ferrosa_sstable::types::Partition],
        schema: &ferrosa_common::schema::TableSchema,
    ) -> SSTableMetadata {
        use crate::flush::{self, FileFlushTarget, FlushTarget};
        use ferrosa_sstable::writer::SSTableWriter;
        use ferrosa_sstable::WriteOptions;

        // SSTableWriter requires partitions in token order
        let mut sorted_partitions = partitions.to_vec();
        sorted_partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = flush::build_serialization_header(schema, &sorted_partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options, header);
        for p in &sorted_partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let flush_target = FileFlushTarget::new(dir.to_path_buf()).unwrap();
        let _reader = flush_target.flush(output).unwrap();
        let gen = flush_target.generation();

        let total_size: u64 = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.metadata().ok().map(|m| m.len()))
            .sum();

        SSTableMetadata {
            id: format!("{gen}"),
            path: dir.to_path_buf(),
            size_bytes: total_size,
            min_token: partitions.first().map(|p| p.key.token.0).unwrap_or(0),
            max_token: partitions.last().map(|p| p.key.token.0).unwrap_or(0),
            min_timestamp: 1000,
            max_timestamp: 2000,
            partition_count: partitions.len() as u64,
            legacy_format: false,
        }
    }

    fn data_bytes_for_single_partition(
        schema: &ferrosa_common::schema::TableSchema,
        header_partitions: &[ferrosa_sstable::types::Partition],
        partition: &ferrosa_sstable::types::Partition,
    ) -> Vec<u8> {
        use crate::flush;
        use ferrosa_sstable::writer::SSTableWriter;
        use ferrosa_sstable::WriteOptions;

        let header = flush::build_serialization_header(schema, header_partitions);
        let mut writer = SSTableWriter::new(
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            header,
        );
        writer.add_partition(partition).unwrap();
        writer.finish().unwrap().data
    }

    fn make_test_partition(
        key: &str,
        value: &str,
        timestamp: i64,
    ) -> ferrosa_sstable::types::Partition {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        ferrosa_sstable::types::Partition {
            key: DecoratedKey::new(PartitionKey::new(key.as_bytes().to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: b"\x00\x00\x00\x01".to_vec(),
                cells: vec![(0, CellValue::live(value.as_bytes().to_vec(), timestamp))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }],
        }
    }

    fn test_schema_with_columns() -> ferrosa_common::schema::TableSchema {
        ferrosa_common::schema::TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ferrosa_common::schema::ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::schema::ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn column_order_schema(
        regular_columns: Vec<ferrosa_common::schema::ColumnDefinition>,
    ) -> ferrosa_common::schema::TableSchema {
        ferrosa_common::schema::TableSchema {
            keyspace: "test_ks".to_string(),
            table: "column_order".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns,
            extensions: Default::default(),
        }
    }

    fn column_order_partition(
        key: &str,
        cells: Vec<(u16, ferrosa_common::CellValue)>,
        timestamp: i64,
    ) -> ferrosa_sstable::types::Partition {
        use ferrosa_common::{DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        ferrosa_sstable::types::Partition {
            key: DecoratedKey::new(PartitionKey::new(key.as_bytes().to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![],
                cells,
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }],
        }
    }

    fn timestamp_bytes(ms: i64) -> Vec<u8> {
        ms.to_be_bytes().to_vec()
    }

    /// RED TEST: compaction must fail (not silently skip) when an input
    /// SSTable is unreadable. Silent skipping causes data loss because
    /// swap_compacted_sstables removes the unreadable input.
    #[test]
    fn compaction_fails_when_input_sstable_unreadable() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();

        // Write a valid SSTable (SSTable A) with 5 partitions
        let dir_a = tmp.path().join("sstable_a");
        std::fs::create_dir_all(&dir_a).unwrap();
        let partitions_a: Vec<_> = (0..5)
            .map(|i| make_test_partition(&format!("key_a_{i}"), "value_a", 1000))
            .collect();
        let meta_a = write_sstable_to_dir(&dir_a, &partitions_a, &schema);

        // Create a corrupt SSTable (SSTable B) — empty Data.db
        let dir_b = tmp.path().join("sstable_b");
        std::fs::create_dir_all(&dir_b).unwrap();
        std::fs::write(dir_b.join("1-Data.db"), b"").unwrap();
        std::fs::write(dir_b.join("1-Partitions.db"), b"corrupt").unwrap();
        std::fs::write(dir_b.join("1-Rows.db"), b"corrupt").unwrap();
        std::fs::write(dir_b.join("1-Filter.db"), b"corrupt").unwrap();
        std::fs::write(dir_b.join("1-Statistics.db"), b"corrupt").unwrap();
        let meta_b = SSTableMetadata {
            id: "1".to_string(),
            path: dir_b.clone(),
            size_bytes: 100,
            min_token: -100,
            max_token: 100,
            min_timestamp: 1000,
            max_timestamp: 2000,
            partition_count: 5,
            legacy_format: false,
        };

        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let task = CompactionTask {
            inputs: vec![meta_a, meta_b],
            output_dir,
            schema,
            table_id: test_table_id(),
            purge: None,
        };

        // This MUST return Err — compaction must not succeed with partial data
        let result = CompactionExecutor::execute_task(&task);
        assert!(
            result.is_err(),
            "Compaction must FAIL when an input SSTable is unreadable. \
             Succeeding with partial data causes data loss via swap."
        );
    }

    /// RED TEST: compaction must fail when an input SSTable's Data.db is missing.
    #[test]
    fn compaction_fails_when_input_data_file_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();

        // Valid SSTable A
        let dir_a = tmp.path().join("sstable_a");
        std::fs::create_dir_all(&dir_a).unwrap();
        let partitions_a: Vec<_> = (0..3)
            .map(|i| make_test_partition(&format!("key_a_{i}"), "value_a", 1000))
            .collect();
        let meta_a = write_sstable_to_dir(&dir_a, &partitions_a, &schema);

        // SSTable B: directory exists but no Data.db file
        let dir_b = tmp.path().join("sstable_b");
        std::fs::create_dir_all(&dir_b).unwrap();
        // No files written — Data.db missing
        let meta_b = SSTableMetadata {
            id: "1".to_string(),
            path: dir_b.clone(),
            size_bytes: 100,
            min_token: -100,
            max_token: 100,
            min_timestamp: 1000,
            max_timestamp: 2000,
            partition_count: 3,
            legacy_format: false,
        };

        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let task = CompactionTask {
            inputs: vec![meta_a, meta_b],
            output_dir,
            schema,
            table_id: test_table_id(),
            purge: None,
        };

        let result = CompactionExecutor::execute_task(&task);
        assert!(
            result.is_err(),
            "Compaction must FAIL when an input Data.db is missing. \
             Silent skip + swap = data loss."
        );
    }

    #[test]
    fn backpressure_read_ahead_config_logs_error_defaults_and_normalization() {
        #[derive(Clone)]
        struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Capture {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let capture = Capture(std::sync::Arc::clone(&output));
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || capture.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            for bad in ["junk", "0", "-1", "268435457"] {
                assert_eq!(
                    input_read_mode(true, Some(bad)),
                    InputReadMode::DirectScan {
                        window: ferrosa_sstable::scan::DEFAULT_SCAN_WINDOW
                    }
                );
            }
            assert_eq!(
                input_read_mode(true, Some("4097")),
                InputReadMode::DirectScan { window: 8192 }
            );
            assert_eq!(
                input_read_mode(true, None),
                InputReadMode::DirectScan {
                    window: ferrosa_sstable::scan::DEFAULT_SCAN_WINDOW
                }
            );
            assert_eq!(
                input_read_mode(true, Some("268435456")),
                InputReadMode::DirectScan {
                    window: ferrosa_sstable::scan::MAX_SCAN_WINDOW
                }
            );
        });
        let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("ERROR").count(), 4, "{logs}");
        assert_eq!(logs.matches("WARN").count(), 1, "{logs}");
        assert!(logs.contains("effective=8192"), "{logs}");
        assert!(
            logs.contains("configured=\"4097\"") || logs.contains("configured=4097"),
            "{logs}"
        );
    }

    #[test]
    fn input_read_mode_selection() {
        use ferrosa_sstable::scan::DEFAULT_SCAN_WINDOW;
        assert_eq!(input_read_mode(false, None), InputReadMode::Cached);
        assert_eq!(input_read_mode(false, Some("8192")), InputReadMode::Cached);
        assert_eq!(
            input_read_mode(true, None),
            InputReadMode::DirectScan {
                window: DEFAULT_SCAN_WINDOW
            }
        );
        assert_eq!(
            input_read_mode(true, Some("8192")),
            InputReadMode::DirectScan { window: 8192 }
        );
        // A bad window is reported (ERROR) and the default is used — never silent.
        assert_eq!(
            input_read_mode(true, Some("junk")),
            InputReadMode::DirectScan {
                window: DEFAULT_SCAN_WINDOW
            }
        );
    }

    /// Two inputs of `n` wide-ish partitions each, so a small read-ahead window
    /// must refill many times during the merge.
    fn two_inputs_for_scan(
        tmp: &std::path::Path,
        schema: &ferrosa_common::schema::TableSchema,
    ) -> Vec<SSTableMetadata> {
        let mut inputs = Vec::new();
        for (name, range, ts) in [("a", 0..150, 1000), ("b", 75..225, 2000)] {
            let dir = tmp.join(format!("sstable_{name}"));
            std::fs::create_dir_all(&dir).unwrap();
            let partitions: Vec<_> = range
                .map(|i| make_test_partition(&format!("key_{i:04}"), &"v".repeat(200), ts))
                .collect();
            inputs.push(write_sstable_to_dir(&dir, &partitions, schema));
        }
        inputs
    }

    fn read_output_partitions(
        dir: &std::path::Path,
        gen: &str,
    ) -> Vec<ferrosa_sstable::types::Partition> {
        collect_reader_partitions(&open_sstable_reader(dir, gen))
    }

    #[cfg(unix)]
    #[test]
    fn direct_scan_compaction_output_matches_cached_compaction() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();
        let inputs = two_inputs_for_scan(tmp.path(), &schema);
        let run = |mode: InputReadMode, name: &str| {
            let output_dir = tmp.path().join(name);
            std::fs::create_dir_all(&output_dir).unwrap();
            let task = CompactionTask {
                inputs: inputs.clone(),
                output_dir: output_dir.clone(),
                schema: schema.clone(),
                table_id: test_table_id(),
                purge: None,
            };
            let cancel = ferrosa_common::CancelToken::new();
            let done = CompactionExecutor::execute_task_with_policy(
                &task,
                None,
                mode,
                true,
                &cancel,
                |_| {},
            )
            .expect("compaction");
            read_output_partitions(&output_dir, &done.metadata.id)
        };

        // The counter is process-global and other tests run in parallel, so only a
        // monotonic lower bound is meaningful here; per-reader mode is checked in
        // `input_data_open_honours_the_read_mode`.
        let opens_before = ferrosa_sstable::direct::direct_read_files_total();
        let cached = run(InputReadMode::Cached, "out_cached");
        // 4096-byte window: far smaller than the input, forcing many refills.
        let scanned = run(InputReadMode::DirectScan { window: 4096 }, "out_scan");
        assert!(
            ferrosa_sstable::direct::direct_read_files_total() >= opens_before + 2,
            "one direct read per input Data.db"
        );
        assert_eq!(cached.len(), 225);
        assert_eq!(scanned, cached, "scan-mode output must be identical");
    }

    #[cfg(unix)]
    #[test]
    fn input_data_open_honours_the_read_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("x-Data.db");
        std::fs::write(&path, vec![7u8; 10_000]).unwrap();
        let cached = open_input_data(&path, InputReadMode::Cached).unwrap();
        assert!(!cached.is_scan());
        let scan = open_input_data(&path, InputReadMode::DirectScan { window: 4096 }).unwrap();
        assert!(scan.is_scan());
    }

    #[cfg(unix)]
    #[test]
    fn direct_scan_inputs_bypass_the_reader_pool() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();
        let inputs = two_inputs_for_scan(tmp.path(), &schema);
        let output_dir = tmp.path().join("out");
        std::fs::create_dir_all(&output_dir).unwrap();
        let task = CompactionTask {
            inputs,
            output_dir,
            schema,
            table_id: test_table_id(),
            purge: None,
        };
        let pool: CompactionReaderPool = Arc::new(crate::reader_pool::ReaderPool::new(256));
        let cancel = ferrosa_common::CancelToken::new();
        let result = CompactionExecutor::execute_task_with_policy(
            &task,
            Some(&pool),
            InputReadMode::DirectScan { window: 4096 },
            true,
            &cancel,
            |_| {},
        )
        .expect("compaction");
        // A cache-bypassing reader must never be parked in the shared pool, where
        // the live read path would pick it up and serve point reads through it.
        // Both facts are this task's own (a per-test pool, a per-task count), so
        // nothing else running in the process can move them.
        assert_eq!(pool.resident(), 0);
        assert_eq!(result.pool_input_opens, 0);
    }

    const PURGE_OLD_LDT: u32 = 1_000_000_000; // long past any test grace period

    fn partition_tombstone(key: &str, ts: i64, ldt: u32) -> ferrosa_sstable::types::Partition {
        use ferrosa_common::{DecoratedKey, PartitionKey};
        ferrosa_sstable::types::Partition {
            key: DecoratedKey::new(PartitionKey::new(key.as_bytes().to_vec())),
            deletion: ferrosa_sstable::types::DeletionTime::new(ts, ldt),
            static_row: None,
            rows: vec![],
        }
    }

    /// Compact the given per-input partition sets and return the output partitions.
    fn compact_with_purge(
        inputs: Vec<Vec<ferrosa_sstable::types::Partition>>,
        purge: Option<super::super::purge::PurgePolicy>,
    ) -> (ExecutedCompaction, Vec<ferrosa_sstable::types::Partition>) {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();
        let metas: Vec<_> = inputs
            .iter()
            .enumerate()
            .map(|(i, partitions)| {
                let dir = tmp.path().join(format!("in_{i}"));
                std::fs::create_dir_all(&dir).unwrap();
                write_sstable_to_dir(&dir, partitions, &schema)
            })
            .collect();
        let output_dir = tmp.path().join("out");
        std::fs::create_dir_all(&output_dir).unwrap();
        let task = CompactionTask {
            inputs: metas,
            output_dir: output_dir.clone(),
            schema,
            table_id: test_table_id(),
            purge,
        };
        let done = CompactionExecutor::execute_task(&task).expect("compaction");
        let out = read_output_partitions(&output_dir, &done.metadata.id);
        // Keep the tempdir alive only as long as needed: outputs were read above.
        (done, out)
    }

    /// Partition keys, sorted: output order is token order, not lexical.
    fn keys_of(partitions: &[ferrosa_sstable::types::Partition]) -> Vec<String> {
        let mut keys: Vec<String> = partitions
            .iter()
            .map(|p| String::from_utf8(p.key.key.as_bytes().to_vec()).unwrap())
            .collect();
        keys.sort();
        keys
    }

    fn live_and_dead_inputs() -> Vec<Vec<ferrosa_sstable::types::Partition>> {
        let live: Vec<_> = (0..5)
            .map(|i| make_test_partition(&format!("k{i}"), "v", 1000))
            .collect();
        vec![live, vec![partition_tombstone("k2", 2000, PURGE_OLD_LDT)]]
    }

    fn open_policy() -> super::super::purge::PurgePolicy {
        super::super::purge::PurgePolicy {
            gc_before: i64::from(PURGE_OLD_LDT) + 1,
            max_purgeable_timestamp: i64::MAX,
            table_delete: ferrosa_sstable::types::DeletionTime::LIVE,
        }
    }

    #[test]
    fn without_a_purge_policy_the_partition_tombstone_is_kept() {
        let (_, out) = compact_with_purge(live_and_dead_inputs(), None);
        assert_eq!(keys_of(&out), ["k0", "k1", "k2", "k3", "k4"]);
        let k2 = out.iter().find(|p| p.key.key.as_bytes() == b"k2").unwrap();
        assert!(!k2.deletion.is_live(), "the marker persists forever today");
    }

    #[test]
    fn an_expired_partition_tombstone_is_purged_and_its_partition_dropped() {
        let purged_before = crate::metrics::compaction_purged_markers_total();
        let (done, out) = compact_with_purge(live_and_dead_inputs(), Some(open_policy()));
        assert_eq!(keys_of(&out), ["k0", "k1", "k3", "k4"]);
        assert_eq!(done.metadata.partition_count, 4);
        assert!(crate::metrics::compaction_purged_markers_total() > purged_before);
    }

    #[test]
    fn a_tombstone_at_or_above_the_overlap_guard_is_kept() {
        // Data outside this compaction as old as ts 1500 might be shadowed by the
        // ts-2000 tombstone, so dropping it could resurrect that data.
        let policy = super::super::purge::PurgePolicy {
            max_purgeable_timestamp: 1500,
            ..open_policy()
        };
        let (_, out) = compact_with_purge(live_and_dead_inputs(), Some(policy));
        assert_eq!(keys_of(&out), ["k0", "k1", "k2", "k3", "k4"]);
    }

    #[test]
    fn a_tombstone_still_inside_gc_grace_is_kept() {
        let policy = super::super::purge::PurgePolicy {
            gc_before: i64::from(PURGE_OLD_LDT) - 1,
            ..open_policy()
        };
        let (_, out) = compact_with_purge(live_and_dead_inputs(), Some(policy));
        assert_eq!(keys_of(&out), ["k0", "k1", "k2", "k3", "k4"]);
    }

    #[test]
    fn purging_every_partition_still_produces_a_non_empty_output() {
        // An all-tombstone compaction would otherwise fail with "no partitions to
        // compact" and be retried forever. One held-back partition keeps it
        // terminating; it is dropped by the next compaction that has other data.
        let inputs = vec![
            vec![partition_tombstone("a", 500, PURGE_OLD_LDT)],
            vec![partition_tombstone("b", 600, PURGE_OLD_LDT)],
        ];
        let (done, out) = compact_with_purge(inputs, Some(open_policy()));
        assert_eq!(out.len(), 1, "exactly one held-back partition");
        assert_eq!(done.metadata.partition_count, 1);
    }

    /// GREEN TEST: compaction succeeds and preserves all data when all
    /// inputs are valid.
    #[test]
    fn compaction_preserves_all_data_when_inputs_valid() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();

        // SSTable A: 5 partitions
        let dir_a = tmp.path().join("sstable_a");
        std::fs::create_dir_all(&dir_a).unwrap();
        let partitions_a: Vec<_> = (0..5)
            .map(|i| make_test_partition(&format!("key_{i:04}"), "value_a", 1000))
            .collect();
        let meta_a = write_sstable_to_dir(&dir_a, &partitions_a, &schema);

        // SSTable B: 5 different partitions
        let dir_b = tmp.path().join("sstable_b");
        std::fs::create_dir_all(&dir_b).unwrap();
        let partitions_b: Vec<_> = (5..10)
            .map(|i| make_test_partition(&format!("key_{i:04}"), "value_b", 2000))
            .collect();
        let meta_b = write_sstable_to_dir(&dir_b, &partitions_b, &schema);

        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let task = CompactionTask {
            inputs: vec![meta_a, meta_b],
            output_dir: output_dir.clone(),
            schema: schema.clone(),
            table_id: test_table_id(),
            purge: None,
        };

        let result = CompactionExecutor::execute_task(&task);
        assert!(result.is_ok(), "compaction should succeed: {result:?}");

        let meta = result.unwrap().metadata;
        assert_eq!(
            meta.partition_count, 10,
            "all 10 partitions must be in output"
        );

        // Verify the output SSTable is readable and has all data
        let gen = &meta.id;
        let data = ferrosa_sstable::io::FileReadAt::open(output_dir.join(format!("{gen}-Data.db")))
            .unwrap();
        let partitions_file =
            ferrosa_sstable::io::FileReadAt::open(output_dir.join(format!("{gen}-Partitions.db")))
                .unwrap();
        let rows = ferrosa_sstable::io::FileReadAt::open(output_dir.join(format!("{gen}-Rows.db")))
            .unwrap();
        let filter = std::fs::read(output_dir.join(format!("{gen}-Filter.db"))).unwrap();
        let statistics = std::fs::read(output_dir.join(format!("{gen}-Statistics.db"))).unwrap();
        let compression_info =
            std::fs::read(output_dir.join(format!("{gen}-CompressionInfo.db"))).ok();

        let reader = ferrosa_sstable::reader::SSTableReader::open(
            ferrosa_sstable::reader::SSTableComponents {
                data,
                partitions: partitions_file,
                rows,
                filter,
                compression_info,
                statistics,
            },
        )
        .unwrap();

        let output_partitions = collect_reader_partitions(&reader);
        assert_eq!(
            output_partitions.len(),
            10,
            "all 10 partitions must be readable from output SSTable"
        );
    }

    /// `publication-safety.md` M3 / T-012: the digest check in `flush_files`
    /// runs unconditionally for compaction output, regardless of
    /// `FERROSA_COMPACTION_VERIFY_OUTPUT` -- that variable controls only the
    /// separate row/partition count walk. A corrupted output must be
    /// refused, and its inputs must remain live and untouched (F15): nothing
    /// retires them, because `execute_task` returns `Err` before its caller
    /// ever gets a chance to swap.
    #[test]
    fn digest_verify_compaction_output_corruption_refused_even_with_verify_output_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();

        let dir_a = tmp.path().join("sstable_a");
        std::fs::create_dir_all(&dir_a).unwrap();
        let partitions_a: Vec<_> = (0..5)
            .map(|i| make_test_partition(&format!("key_{i:04}"), "value_a", 1000))
            .collect();
        let meta_a = write_sstable_to_dir(&dir_a, &partitions_a, &schema);

        let dir_b = tmp.path().join("sstable_b");
        std::fs::create_dir_all(&dir_b).unwrap();
        let partitions_b: Vec<_> = (5..10)
            .map(|i| make_test_partition(&format!("key_{i:04}"), "value_b", 2000))
            .collect();
        let meta_b = write_sstable_to_dir(&dir_b, &partitions_b, &schema);

        let input_a_data = std::fs::read(dir_a.join(format!("{}-Data.db", meta_a.id))).unwrap();
        let input_b_data = std::fs::read(dir_b.join(format!("{}-Data.db", meta_b.id))).unwrap();

        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let task = CompactionTask {
            inputs: vec![meta_a.clone(), meta_b.clone()],
            output_dir: output_dir.clone(),
            schema: schema.clone(),
            table_id: test_table_id(),
            purge: None,
        };

        set_compaction_output_hook(|output| {
            let good = std::fs::read(&output.data).unwrap();
            let mut damaged = good.clone();
            for b in damaged.iter_mut().skip(good.len() / 4) {
                *b = 0xff;
            }
            std::fs::write(&output.data, &damaged).unwrap();
        });

        // Disable only this task's structural scan. Digest verification remains
        // unconditional, and parallel cancellation tests retain their scan.
        let result = CompactionExecutor::execute_task_with_policy(
            &task,
            None,
            configured_input_read_mode(),
            false,
            &CancelToken::new(),
            |_| {},
        );

        clear_compaction_output_hook();

        let msg = match result {
            Ok(_) => panic!(
                "a compaction whose output digest does not match must be refused even with \
                 FERROSA_COMPACTION_VERIFY_OUTPUT=0"
            ),
            Err(e) => e,
        };
        assert!(
            msg.contains("Digest.crc32 mismatch"),
            "refusal must be reported as a digest mismatch: {msg}"
        );

        assert_eq!(
            std::fs::read(dir_a.join(format!("{}-Data.db", meta_a.id))).unwrap(),
            input_a_data,
            "input A must remain untouched after a refused compaction"
        );
        assert_eq!(
            std::fs::read(dir_b.join(format!("{}-Data.db", meta_b.id))).unwrap(),
            input_b_data,
            "input B must remain untouched after a refused compaction"
        );
    }

    #[test]
    fn compaction_streaming_merge_only_holds_one_partition_per_input() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();

        let inputs: Vec<_> = (0..3)
            .map(|sstable_idx| {
                let dir = tmp.path().join(format!("sstable_{sstable_idx}"));
                std::fs::create_dir_all(&dir).unwrap();
                let partitions: Vec<_> = (0..200)
                    .map(|i| {
                        // Every SSTable has the same key sequence. The streaming
                        // compactor may group duplicate keys across inputs, but
                        // it must never materialize all 600 partitions at once.
                        make_test_partition(
                            &format!("shared_key_{i:04}"),
                            &format!("value_{sstable_idx}_{i}"),
                            1000 + sstable_idx,
                        )
                    })
                    .collect();
                write_sstable_to_dir(&dir, &partitions, &schema)
            })
            .collect();

        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();
        let task = CompactionTask {
            inputs: inputs.clone(),
            output_dir,
            schema,
            table_id: test_table_id(),
            purge: None,
        };

        let mut max_group_width = 0;
        let result = CompactionExecutor::execute_task_observing(&task, |width| {
            max_group_width = max_group_width.max(width);
        });

        assert!(result.is_ok(), "compaction should succeed: {result:?}");
        let meta = result.unwrap().metadata;
        assert_eq!(meta.partition_count, 200);
        assert_eq!(
            max_group_width,
            inputs.len(),
            "streaming compaction may hold at most one partition per input for a key"
        );
    }

    #[test]
    fn compaction_rejects_input_with_out_of_order_data_stream() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();
        let dir = tmp.path().join("sstable");
        std::fs::create_dir_all(&dir).unwrap();

        let first = make_test_partition("decision", "first", 1000);
        let second = make_test_partition("org", "second", 1000);
        assert!(
            first.key > second.key,
            "test keys must be descending by decorated token"
        );

        let meta = write_sstable_to_dir(&dir, &[first.clone(), second.clone()], &schema);
        let data_path = dir.join(format!("{}-Data.db", meta.id));
        let header_partitions = vec![second.clone(), first.clone()];
        let mut unsorted_data =
            data_bytes_for_single_partition(&schema, &header_partitions, &first);
        unsorted_data.extend(data_bytes_for_single_partition(
            &schema,
            &header_partitions,
            &second,
        ));
        std::fs::write(data_path, unsorted_data).unwrap();
        // The hand-crafted Data.db above no longer matches the Digest.crc32
        // / CRC.db that `write_sstable_to_dir` computed for the ORIGINAL
        // (sorted) bytes. Since T-012 loads them automatically on every
        // compaction input open, a stale CRC.db would fail chunk
        // verification on the very first read and mask the token-order
        // error this test actually exercises. Removing them makes this
        // generation "not checked" (T-011's old-SSTable tolerance) instead
        // of "checked against the wrong bytes".
        let _ = std::fs::remove_file(dir.join(format!("{}-Digest.crc32", meta.id)));
        let _ = std::fs::remove_file(dir.join(format!("{}-CRC.db", meta.id)));

        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();
        let task = CompactionTask {
            inputs: vec![meta],
            output_dir,
            schema,
            table_id: test_table_id(),
            purge: None,
        };

        let err = CompactionExecutor::execute_task(&task)
            .expect_err("compaction must reject an input whose Data.db stream is not token-sorted");
        assert!(
            err.contains("partitions out of token order"),
            "error must classify the input as corrupt, got: {err}"
        );
        assert!(
            !err.contains("keys must be added in sorted order"),
            "executor should reject the corrupt input before surfacing writer internals: {err}"
        );
    }

    #[test]
    fn compaction_remaps_legacy_input_column_ordinals_before_write() {
        use ferrosa_common::schema::ColumnDefinition;
        use ferrosa_common::CellValue;
        use ferrosa_sstable::io::FileReadAt;
        use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};

        let tmp = tempfile::tempdir().unwrap();
        let current_schema = column_order_schema(vec![
            ColumnDefinition {
                name: "created_at".to_string(),
                type_name: "org.apache.cassandra.db.marshal.TimestampType".to_string(),
            },
            ColumnDefinition {
                name: "description".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
        ]);
        let legacy_schema = column_order_schema(vec![
            ColumnDefinition {
                name: "description".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            ColumnDefinition {
                name: "created_at".to_string(),
                type_name: "org.apache.cassandra.db.marshal.TimestampType".to_string(),
            },
        ]);

        let current_dir = tmp.path().join("current");
        std::fs::create_dir_all(&current_dir).unwrap();
        let current = column_order_partition(
            "same-key",
            vec![
                (0, CellValue::live(timestamp_bytes(111), 1000)),
                (1, CellValue::live(b"current".to_vec(), 1000)),
            ],
            1000,
        );
        let current_meta = write_sstable_to_dir(&current_dir, &[current], &current_schema);

        let legacy_dir = tmp.path().join("legacy");
        std::fs::create_dir_all(&legacy_dir).unwrap();
        let legacy = column_order_partition(
            "same-key",
            vec![
                (0, CellValue::live(b"legacy".to_vec(), 2000)),
                (1, CellValue::live(timestamp_bytes(222), 2000)),
            ],
            2000,
        );
        let legacy_meta = write_sstable_to_dir(&legacy_dir, &[legacy], &legacy_schema);

        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();
        let task = CompactionTask {
            inputs: vec![legacy_meta, current_meta],
            output_dir: output_dir.clone(),
            schema: current_schema,
            table_id: crate::TableId::new("test_ks", "column_order"),
            purge: None,
        };

        let meta = CompactionExecutor::execute_task(&task)
            .expect("compaction must remap legacy ordinals before writing current-schema output")
            .metadata;
        assert_eq!(meta.partition_count, 1);

        let gen = &meta.id;
        let reader = SSTableReader::open(SSTableComponents {
            data: FileReadAt::open(output_dir.join(format!("{gen}-Data.db"))).unwrap(),
            partitions: FileReadAt::open(output_dir.join(format!("{gen}-Partitions.db"))).unwrap(),
            rows: FileReadAt::open(output_dir.join(format!("{gen}-Rows.db"))).unwrap(),
            filter: std::fs::read(output_dir.join(format!("{gen}-Filter.db"))).unwrap(),
            compression_info: std::fs::read(output_dir.join(format!("{gen}-CompressionInfo.db")))
                .ok(),
            statistics: std::fs::read(output_dir.join(format!("{gen}-Statistics.db"))).unwrap(),
        })
        .unwrap();
        let partitions = collect_reader_partitions(&reader);
        assert_eq!(partitions.len(), 1);
        let cells = &partitions[0].rows[0].cells;
        assert_eq!(cells[0].0, 0);
        assert_eq!(
            cells[0].1.value.as_deref(),
            Some(timestamp_bytes(222).as_slice())
        );
        assert_eq!(cells[1].0, 1);
        assert_eq!(cells[1].1.value.as_deref(), Some(b"legacy".as_slice()));
    }

    #[test]
    fn compaction_rejects_no_timestamp_cells_before_writer_panic() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, NO_TIMESTAMP};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

        let schema = test_schema_with_columns();
        let header = crate::flush::build_serialization_header(
            &schema,
            &[make_test_partition("good", "value", 1000)],
        );
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::new(b"bad".to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: 1i32.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(b"missing-ts".to_vec(), NO_TIMESTAMP))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            }],
        };

        let err = validate_partition_writable(&partition, &header).unwrap_err();
        assert!(
            err.contains("NO_TIMESTAMP"),
            "error must make the corrupt timestamp explicit: {err}"
        );
        assert!(
            err.contains("original SSTables are preserved"),
            "error must describe repair-safe compaction semantics: {err}"
        );
    }

    #[test]
    fn compaction_accepts_static_rows_without_liveness_when_cells_have_timestamps() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

        let schema = test_schema_with_columns();
        let header = crate::flush::build_serialization_header(
            &schema,
            &[make_test_partition("good", "value", 1000)],
        );
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::new(b"static".to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: Some(Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"static-value".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            }),
            rows: vec![],
        };

        validate_partition_writable(&partition, &header).unwrap();
    }

    // ---- FMEA #11: bounded compaction memory ----

    /// The concurrency gate must never let more than `cap` permits be held at
    /// once, no matter how many worker threads contend for them. This is the
    /// invariant `compaction_running_max <= cap` relies on.
    #[test]
    fn compaction_gate_caps_concurrent_holders() {
        use std::sync::atomic::AtomicUsize;

        const CAP: usize = 2;
        const WORKERS: usize = 8;
        const ITERS: usize = 50;

        let gate = Arc::new(CompactionGate::new(CAP));
        let cancel = CancelToken::new();
        let (_shutdown_tx, shutdown_rx) = crossbeam_channel::bounded(0);
        let live = Arc::new(AtomicUsize::new(0));
        let max_live = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..WORKERS {
            let gate = Arc::clone(&gate);
            let cancel = cancel.clone();
            let shutdown_rx = shutdown_rx.clone();
            let live = Arc::clone(&live);
            let max_live = Arc::clone(&max_live);
            handles.push(std::thread::spawn(move || {
                for _ in 0..ITERS {
                    let permit = gate
                        .acquire(&cancel, &shutdown_rx)
                        .expect("permit while not stopped");
                    let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                    max_live.fetch_max(now, Ordering::SeqCst);
                    // Hold the permit briefly so contention is real.
                    std::thread::yield_now();
                    live.fetch_sub(1, Ordering::SeqCst);
                    drop(permit);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert!(
            max_live.load(Ordering::SeqCst) <= CAP,
            "compaction gate allowed {} concurrent holders, cap was {CAP}",
            max_live.load(Ordering::SeqCst)
        );
    }

    /// A shutting-down executor must not deadlock a worker blocked on the gate:
    /// `acquire` returns `None` once the shutdown sender closes.
    #[test]
    fn compaction_gate_unblocks_on_shutdown() {
        let gate = Arc::new(CompactionGate::new(1));
        let cancel = CancelToken::new();
        let (_shutdown_tx, shutdown_rx) = crossbeam_channel::bounded(0);

        // Exhaust the single permit and hold it.
        let held = gate.acquire(&cancel, &shutdown_rx).expect("first permit");

        let waiter = {
            let gate = Arc::clone(&gate);
            let cancel = cancel.clone();
            let shutdown_rx = shutdown_rx.clone();
            std::thread::spawn(move || gate.acquire(&cancel, &shutdown_rx).is_none())
        };
        drop(_shutdown_tx);
        let returned_none = waiter.join().unwrap();
        assert!(
            returned_none,
            "waiter must observe shutdown and stop blocking, not wait forever"
        );
        drop(held);
    }

    #[test]
    fn compaction_gate_unblocks_on_table_cancellation() {
        let gate = CompactionGate::new(1);
        let cancel = CancelToken::new();
        let (_shutdown, shutdown_rx) = crossbeam_channel::bounded(0);
        let held = gate.acquire(&cancel, &shutdown_rx).unwrap();
        std::thread::scope(|scope| {
            let (done_tx, done_rx) = crossbeam_channel::bounded(1);
            let gate_ref = &gate;
            let cancel_ref = &cancel;
            let shutdown_ref = &shutdown_rx;
            scope.spawn(move || {
                done_tx
                    .send(gate_ref.acquire(cancel_ref, shutdown_ref).is_none())
                    .unwrap();
            });
            cancel.cancel(CancelReason::TableDropped);
            assert!(done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap());
        });
        drop(held);
    }

    /// With nothing set, compaction reads its inputs as a direct scan; the
    /// switch turns it off. Composes the same pieces `configured_input_read_mode`
    /// does, without the environment, which parallel tests cannot set safely.
    #[test]
    fn nothing_set_means_a_direct_scan_and_the_switch_turns_it_off() {
        use ferrosa_sstable::direct::resolve_switch;
        let mode = |specific: Option<&str>, master: Option<&str>| {
            let switch = resolve_switch("FERROSA_COMPACTION_DIRECT_READ", specific, master);
            input_read_mode(
                switch.on_platform(true) == ferrosa_sstable::direct::PlatformDecision::Direct,
                None,
            )
        };
        assert!(matches!(mode(None, None), InputReadMode::DirectScan { .. }));
        assert_eq!(mode(Some("0"), None), InputReadMode::Cached);
        assert_eq!(mode(None, Some("0")), InputReadMode::Cached);
        assert!(matches!(
            mode(Some("1"), Some("0")),
            InputReadMode::DirectScan { .. }
        ));
    }

    /// FMEA #11 fix #1: compaction input readers are obtained through the
    /// engine-wide reader pool. After a pool-routed compaction the input
    /// generations are resident in the pool (shared/evictable with the read
    /// path), the pool-routed-open counter advanced, and the merged output
    /// still contains every input partition (correctness unchanged).
    #[test]
    fn compaction_inputs_routed_through_reader_pool() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema_with_columns();
        let table_id = test_table_id();

        // Two non-overlapping input SSTables, 5 partitions each.
        let dir_a = tmp.path().join("sstable_a");
        std::fs::create_dir_all(&dir_a).unwrap();
        let partitions_a: Vec<_> = (0..5)
            .map(|i| make_test_partition(&format!("a_key_{i:02}"), "va", 1000))
            .collect();
        let meta_a = write_sstable_to_dir(&dir_a, &partitions_a, &schema);

        let dir_b = tmp.path().join("sstable_b");
        std::fs::create_dir_all(&dir_b).unwrap();
        let partitions_b: Vec<_> = (0..5)
            .map(|i| make_test_partition(&format!("b_key_{i:02}"), "vb", 1000))
            .collect();
        let meta_b = write_sstable_to_dir(&dir_b, &partitions_b, &schema);

        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();
        let task = CompactionTask {
            inputs: vec![meta_a.clone(), meta_b.clone()],
            output_dir,
            schema,
            table_id: table_id.clone(),
            purge: None,
        };

        let pool: CompactionReaderPool = Arc::new(crate::reader_pool::ReaderPool::new(256));
        assert_eq!(pool.resident(), 0, "pool starts empty");

        // The cached (pool-routed) mode is what this pins, so ask for it. It used
        // to be inherited from the default, which is now the direct scan.
        let cancel = ferrosa_common::CancelToken::new();
        let result = CompactionExecutor::execute_task_with_policy(
            &task,
            Some(&pool),
            InputReadMode::Cached,
            true,
            &cancel,
            |_| {},
        )
        .expect("compaction");

        // This task opened every input through the pool.
        assert_eq!(
            result.pool_input_opens,
            task.inputs.len(),
            "every input open must be pool-routed"
        );

        // Both input generations are resident in the pool, keyed exactly as the
        // read path keys them — proving the readers are shared, not opened on a
        // private path outside the bound.
        for input in &task.inputs {
            let key = (
                table_id.to_string(),
                crate::store::SstableDescriptor::gen_num_for(&input.id),
            );
            assert!(
                pool.get_or_open(key, || Err::<
                    ferrosa_sstable::reader::SSTableReader<ferrosa_sstable::io::FileReadAt>,
                    String,
                >("must already be cached".into()))
                    .is_ok(),
                "input generation {} must be resident in the pool after compaction",
                input.id
            );
        }

        // Correctness: every input partition survives the merge.
        let meta = result.metadata;
        assert_eq!(
            meta.partition_count,
            (partitions_a.len() + partitions_b.len()) as u64,
            "all input partitions must appear in the compacted output"
        );
    }

    /// `FERROSA_MAX_CONCURRENT_COMPACTIONS` parses to a positive cap and falls
    /// back to the resource auto-tune when unset/invalid.
    #[test]
    fn concurrent_compaction_cap_is_always_positive() {
        assert!(configured_max_concurrent_compactions() >= 1);
        assert!(configured_compaction_workers() >= 1);
    }

    /// Auto-tune is bounded by BOTH cpu and memory, and never zero.
    #[test]
    fn auto_tuned_concurrency_is_bounded_by_cpu_and_memory() {
        // 2 GB: half of RAM / 256 MB = 4 tasks, capped by CPUs.
        assert_eq!(
            auto_tuned_max_concurrent(16, Some(2 * 1024 * 1024 * 1024), 8, 256 * 1024 * 1024),
            4
        );
        // Few cpus cap below the memory allowance.
        assert_eq!(
            auto_tuned_max_concurrent(2, Some(2 * 1024 * 1024 * 1024), 8, 256 * 1024 * 1024),
            2
        );
        // Tiny memory floors at 1, never zero.
        assert_eq!(
            auto_tuned_max_concurrent(8, Some(64 * 1024 * 1024), 8, 256 * 1024 * 1024),
            1
        );
        // Huge memory is still capped by the parallelism ceiling and cpus.
        assert_eq!(
            auto_tuned_max_concurrent(64, Some(256u64 * 1024 * 1024 * 1024), 8, 256 * 1024 * 1024),
            8
        );
        // The operator-set parallelism ceiling is honored when raised.
        assert_eq!(
            auto_tuned_max_concurrent(64, Some(256u64 * 1024 * 1024 * 1024), 32, 256 * 1024 * 1024),
            32
        );
        // No memory signal → historical conservative default of 2 (cpu-capped).
        assert_eq!(auto_tuned_max_concurrent(8, None, 8, 256 * 1024 * 1024), 2);
        assert_eq!(auto_tuned_max_concurrent(1, None, 8, 256 * 1024 * 1024), 1);
        assert_eq!(
            auto_tuned_max_concurrent(64, None, 32, 256 * 1024 * 1024),
            2
        );
    }

    #[test]
    fn auto_tuned_workers_track_cpus_within_bounds() {
        assert_eq!(auto_tuned_workers(1, 8), 1);
        assert_eq!(auto_tuned_workers(4, 8), 4);
        assert_eq!(auto_tuned_workers(64, 8), 8);
        assert_eq!(auto_tuned_workers(64, 32), 32);
    }

    #[test]
    fn compaction_backoff_classifies_digest_and_readback_failures() {
        assert!(is_digest_verification_failure(
            "flush output: Digest mismatch for Data.db"
        ));
        assert!(is_digest_verification_failure(
            "CORRUPTION: output partitions_iter failed"
        ));
        assert!(is_digest_verification_failure(
            "compaction output SSTable is corrupt"
        ));
        assert!(!is_digest_verification_failure("finish: disk full"));
    }

    #[test]
    fn compaction_backoff_classifies_missing_input_components() {
        // Captured verbatim from a live occurrence (node2, table baselines.iot,
        // generation 1791247442385015, 2026-10-06T00:48:27.429459Z). That
        // generation had been evicted for cache_cap and was still selected as a
        // compaction input, so its Data.db was neither local nor rehydratable.
        // Before this class was retryable the failure never reached the retry
        // controller: the planner re-selected the same input immediately, with
        // no backoff and no pause, so `compaction_failed_total` climbed without
        // bound while `compaction_completed_total` stayed 0.
        assert!(is_retryable_compaction_failure(
            "required SSTable component /var/lib/ferrosa/sstables/baselines.iot/1791247442385015-Data.db is missing"
        ));
        assert!(is_retryable_compaction_failure(
            "required SSTable component /var/lib/ferrosa/sstables/baselines.iot/1-Partitions.db is empty after rehydration"
        ));
        assert!(is_retryable_compaction_failure(
            "failed to rehydrate SSTable component /var/lib/ferrosa/sstables/baselines.iot/1-Data.db: object not found"
        ));
        assert!(is_retryable_compaction_failure(
            "failed to inspect SSTable component /var/lib/ferrosa/sstables/baselines.iot/1-Data.db: permission denied"
        ));
        // Regression: the digest/verification class must stay retryable.
        assert!(is_retryable_compaction_failure(
            "flush output: Digest mismatch for Data.db"
        ));
        // Negative control: an unrelated transient error must NOT be folded in.
        // If it were, the predicate would be a blanket `true` and the table
        // would be paused for any error including harmless ones.
        assert!(!is_retryable_compaction_failure("finish: disk full"));
    }
}
