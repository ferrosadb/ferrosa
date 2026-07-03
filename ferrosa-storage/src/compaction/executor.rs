//! Background compaction executor.
//!
//! Receives [`CompactionTask`]s via a channel, merges input SSTables on a
//! background thread using the existing `merge_partitions` logic, and sends
//! back [`CompactionResult`]s.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use parking_lot::{Condvar, Mutex};

use crate::store::SharedReaderPool;
use crate::upload::manager::SstableComponentBytes;

use super::metadata::{CompactionTask, SSTableMetadata};

/// Reader pool used to obtain compaction input SSTable readers so they count
/// against the engine-wide resident-reader bound (FMEA #11). Keyed identically
/// to the live read path: `(table_id, gen_num)` over `FileReadAt` readers.
type CompactionReaderPool = SharedReaderPool<ferrosa_sstable::io::FileReadAt>;

/// Conservative peak-memory budget charged to ONE concurrent streaming
/// compaction. Since compaction merges a partition-group at a time (widest
/// partition × inputs + reader/writer buffers), not the whole SSTable set, a
/// few hundred MB is a safe worst case per task. Used to auto-tune the
/// concurrency cap from the configured memory limit.
const PER_COMPACTION_MEM_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Hard ceiling on auto-derived worker threads / concurrency, so a very large
/// host does not spawn an unreasonable number of compaction threads.
const MAX_AUTO_COMPACTION_PARALLELISM: usize = 8;

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
fn auto_tuned_max_concurrent(cpus: usize, mem_limit_bytes: Option<u64>) -> usize {
    let cpu_cap = cpus.clamp(1, MAX_AUTO_COMPACTION_PARALLELISM);
    let mem_cap = match mem_limit_bytes {
        Some(mem) => {
            let budgeted = mem / COMPACTION_MEM_DIVISOR / PER_COMPACTION_MEM_BUDGET_BYTES;
            (budgeted as usize).clamp(1, MAX_AUTO_COMPACTION_PARALLELISM)
        }
        // No memory signal: keep the historical conservative default.
        None => 2,
    };
    cpu_cap.min(mem_cap).max(1)
}

/// Pure auto-tune for worker threads: one per CPU, bounded. Extra idle workers
/// are cheap (they block on `recv`), and having at least as many workers as the
/// concurrency cap lets every permitted merge run without head-of-line blocking.
fn auto_tuned_workers(cpus: usize) -> usize {
    cpus.clamp(1, MAX_AUTO_COMPACTION_PARALLELISM)
}

fn available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(2)
}

/// Resolve the concurrent-compaction cap: explicit env override, else auto-tuned
/// from CPU + configured memory (never zero).
fn configured_max_concurrent_compactions() -> usize {
    if let Some(n) = std::env::var("FERROSA_MAX_CONCURRENT_COMPACTIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
    {
        return n;
    }
    auto_tuned_max_concurrent(available_cpus(), detected_memory_limit_bytes())
}

/// Resolve the compaction worker-thread count: explicit env override, else
/// auto-tuned from CPU count (never zero).
fn configured_compaction_workers() -> usize {
    if let Some(n) = std::env::var("FERROSA_COMPACTION_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
    {
        return n;
    }
    auto_tuned_workers(available_cpus())
}

/// A counting semaphore that caps the number of compaction merges running at
/// once across all worker threads. A worker acquires a permit before running
/// `execute_task` and releases it (via the `CompactionPermit` guard) when the
/// task finishes, so at most `cap` tasks ever execute concurrently regardless
/// of how many worker threads exist.
struct CompactionGate {
    cap: usize,
    available: Mutex<usize>,
    cv: Condvar,
}

impl CompactionGate {
    fn new(cap: usize) -> Self {
        let cap = cap.max(1);
        Self {
            cap,
            available: Mutex::new(cap),
            cv: Condvar::new(),
        }
    }

    /// Block until a permit is available, then take it. Returns a guard that
    /// returns the permit on drop. `stop` is polled so a shutting-down executor
    /// does not deadlock waiting on a permit that never frees.
    fn acquire<'a>(&'a self, stop: &AtomicBool) -> Option<CompactionPermit<'a>> {
        let mut available = self.available.lock();
        while *available == 0 {
            if stop.load(Ordering::Acquire) {
                return None;
            }
            // Bounded wait so shutdown is observed promptly even if no permit
            // frees up (Rule 2: no unbounded blocking).
            self.cv
                .wait_for(&mut available, std::time::Duration::from_millis(100));
        }
        *available -= 1;
        Some(CompactionPermit { gate: self })
    }
}

/// RAII permit: returns its slot to the [`CompactionGate`] on drop.
struct CompactionPermit<'a> {
    gate: &'a CompactionGate,
}

impl Drop for CompactionPermit<'_> {
    fn drop(&mut self) {
        let mut available = self.gate.available.lock();
        *available = (*available + 1).min(self.gate.cap);
        self.gate.cv.notify_one();
    }
}

struct QueuedCompactionTask {
    task: CompactionTask,
    queued_at: Instant,
}

fn env_flag_enabled(name: &str, default: bool) -> bool {
    match std::env::var(name).ok().as_deref() {
        Some("true" | "1" | "on" | "yes") => true,
        Some("false" | "0" | "off" | "no") => false,
        _ => default,
    }
}

fn compaction_verify_output_enabled() -> bool {
    env_flag_enabled("FERROSA_COMPACTION_VERIFY_OUTPUT", true)
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

/// Runs compaction tasks on a background thread.
///
/// `StorageEngine` submits tasks via `submit()` and polls results via
/// `poll_results()`. The executor is stopped on `shutdown()`.
pub struct CompactionExecutor {
    task_txs: Vec<std::sync::mpsc::Sender<QueuedCompactionTask>>,
    next_worker: AtomicUsize,
    result_rx: Mutex<std::sync::mpsc::Receiver<CompactionResult>>,
    handles: Mutex<Vec<thread::JoinHandle<()>>>,
    stop_flag: Arc<AtomicBool>,
    in_flight_inputs: Arc<Mutex<HashSet<String>>>,
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
        let (result_tx, result_rx) = std::sync::mpsc::channel::<CompactionResult>();
        let stop_flag = Arc::new(AtomicBool::new(false));
        let in_flight_inputs = Arc::new(Mutex::new(HashSet::new()));
        let gate = Arc::new(CompactionGate::new(max_concurrent));
        // `reader_pool` is already an `Arc`, so each worker gets a cheap clone.
        let mut task_txs = Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);

        for worker_idx in 0..worker_count {
            let (task_tx, task_rx) = std::sync::mpsc::channel::<QueuedCompactionTask>();
            task_txs.push(task_tx);
            let result_tx = result_tx.clone();
            let stop = Arc::clone(&stop_flag);
            let in_flight_inputs = Arc::clone(&in_flight_inputs);
            let gate = Arc::clone(&gate);
            let reader_pool = reader_pool.clone();

            let handle = thread::Builder::new()
                .name(format!("compaction-executor-{worker_idx}"))
                .spawn(move || {
                    while !stop.load(Ordering::Acquire) {
                        match task_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                            Ok(queued) => {
                                crate::metrics::dec_compaction_queue_depth();
                                crate::metrics::observe_compaction_phase(
                                    crate::metrics::CompactionPhase::QueueWait,
                                    queued.queued_at.elapsed(),
                                );
                                let task = queued.task;
                                // Cap concurrent merges across all workers
                                // (FMEA #11): hold a permit for the duration of
                                // the merge. The running gauge is bumped only
                                // *after* the permit is taken, so
                                // `compaction_running_max` reflects tasks
                                // actually executing, never those blocked at the
                                // gate.
                                let permit = match gate.acquire(&stop) {
                                    Some(permit) => permit,
                                    None => {
                                        // Shutting down before a permit freed:
                                        // requeued inputs are released so a
                                        // restart can reschedule them.
                                        Self::release_in_flight_inputs(&in_flight_inputs, &task);
                                        break;
                                    }
                                };
                                crate::metrics::inc_compaction_running();
                                let task_start = Instant::now();
                                let result = Self::execute_task_routed(&task, reader_pool.as_ref());
                                crate::metrics::dec_compaction_running();
                                drop(permit);
                                match result {
                                    Ok(output) => {
                                        let _ = result_tx.send(CompactionResult {
                                            task,
                                            output: output.metadata,
                                            direct_upload: output.direct_upload,
                                        });
                                    }
                                    Err(e) => {
                                        Self::release_in_flight_inputs(&in_flight_inputs, &task);
                                        crate::metrics::observe_compaction_phase(
                                            crate::metrics::CompactionPhase::Total,
                                            task_start.elapsed(),
                                        );
                                        crate::metrics::inc_compaction_failed();
                                        tracing::error!(%e, "compaction: task failed");
                                    }
                                }
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
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
            handles: Mutex::new(handles),
            stop_flag,
            in_flight_inputs,
        }
    }

    /// Submits a compaction task to the background thread.
    pub fn submit(&self, task: CompactionTask) -> ferrosa_common::Result<()> {
        crate::metrics::inc_compaction_submitted();
        if !Self::try_claim_in_flight_inputs(&self.in_flight_inputs, &task) {
            crate::metrics::inc_compaction_skipped_overlap();
            tracing::debug!(
                table_id = %task.table_id,
                inputs = task.inputs.len(),
                "compaction: skipping overlapping task already in flight"
            );
            return Ok(());
        }

        let worker_idx = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.task_txs.len();
        crate::metrics::inc_compaction_queue_depth();
        let queued = QueuedCompactionTask {
            task,
            queued_at: Instant::now(),
        };
        match self.task_txs[worker_idx].send(queued) {
            Ok(()) => Ok(()),
            Err(err) => {
                crate::metrics::dec_compaction_queue_depth();
                Self::release_in_flight_inputs(&self.in_flight_inputs, &err.0.task);
                Err(ferrosa_common::Error::InvalidFormat(
                    "compaction channel closed".into(),
                ))
            }
        }
    }

    /// Polls for completed compaction results (non-blocking).
    pub fn poll_results(&self) -> Vec<CompactionResult> {
        let rx = self.result_rx.lock();
        let mut results = Vec::new();
        while let Ok(result) = rx.try_recv() {
            results.push(result);
        }
        results
    }

    /// Releases a successful task's inputs after its result has been finalized.
    ///
    /// Successful compactions must stay claimed while their result waits in the
    /// result queue. Otherwise a later flush can schedule the same inputs again,
    /// and the first finalized result can delete files the duplicate task still
    /// expects to read.
    pub fn release_task_inputs(&self, task: &CompactionTask) {
        Self::release_in_flight_inputs(&self.in_flight_inputs, task);
    }

    /// Shuts down the compaction executor, waiting for the background thread.
    pub fn shutdown(&self) {
        self.stop_flag.store(true, Ordering::Release);
        for handle in self.handles.lock().drain(..) {
            let _ = handle.join();
        }
    }

    fn input_key(task: &CompactionTask, input_id: &str) -> String {
        format!("{}:{input_id}", task.table_id)
    }

    fn try_claim_in_flight_inputs(
        in_flight_inputs: &Mutex<HashSet<String>>,
        task: &CompactionTask,
    ) -> bool {
        let keys: Vec<String> = task
            .inputs
            .iter()
            .map(|input| Self::input_key(task, &input.id))
            .collect();
        let mut in_flight = in_flight_inputs.lock();
        if keys.iter().any(|key| in_flight.contains(key)) {
            return false;
        }
        in_flight.extend(keys);
        true
    }

    fn release_in_flight_inputs(in_flight_inputs: &Mutex<HashSet<String>>, task: &CompactionTask) {
        let mut in_flight = in_flight_inputs.lock();
        for input in &task.inputs {
            in_flight.remove(&Self::input_key(task, &input.id));
        }
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
        Self::execute_task_inner(task, None, observe_group_width)
    }

    /// Execute a task with input opens routed through the engine-wide reader
    /// pool when one is configured (FMEA #11). Used by the worker threads.
    fn execute_task_routed(
        task: &CompactionTask,
        reader_pool: Option<&CompactionReaderPool>,
    ) -> std::result::Result<ExecutedCompaction, String> {
        Self::execute_task_inner(task, reader_pool, |_| {})
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
        Self::execute_task_inner(task, None, |_| {})
    }

    fn execute_task_inner<F>(
        task: &CompactionTask,
        reader_pool: Option<&CompactionReaderPool>,
        mut observe_group_width: F,
    ) -> std::result::Result<ExecutedCompaction, String>
    where
        F: FnMut(usize),
    {
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
        let pool_table_key = task.table_id.to_string();
        for input in &task.inputs {
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

            let data = FileReadAt::open(&data_path)
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

            // Strict open: validates and aborts on corruption regardless of
            // whether the pool already has this generation cached.
            let reader = SSTableReader::open(SSTableComponents {
                data,
                partitions: partitions_file,
                rows,
                filter,
                compression_info,
                statistics,
            })
            .map_err(|e| format!("aborting compaction: SSTable {gen} corrupt: {e}"))?;

            let reader = match reader_pool {
                Some(pool) => {
                    // Key identically to the live read/startup path so a
                    // generation opened for reads and one opened for compaction
                    // share a single resident reader.
                    let key = (
                        pool_table_key.clone(),
                        crate::store::SstableDescriptor::gen_num_for(gen),
                    );
                    crate::metrics::inc_compaction_pool_input_opens();
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
            .map(|reader| ColumnOrdinalMapping::for_header(&task.schema, reader.header()))
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
        let mut writer = SSTableWriter::new_file_backed(
            options,
            output_header.clone(),
            staging_dir.join("Data.raw"),
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
        let mut merged_partition_count: u64 = 0;
        let mut merged_row_count: usize = 0;
        // Track min/max token across all merged output partitions so the
        // emitted SSTableMetadata can be filled without a second scan.
        let mut min_token: i64 = i64::MAX;
        let mut max_token: i64 = i64::MIN;

        while let Some(top) = heap.pop() {
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
            let merged = merge::merge_partitions(group);
            crate::metrics::observe_compaction_phase(
                crate::metrics::CompactionPhase::MergePartition,
                merge_start.elapsed(),
            );
            merged_row_count += merged.rows.len();
            merged_partition_count += 1;
            let token = merged.key.token.0;
            if token < min_token {
                min_token = token;
            }
            if token > max_token {
                max_token = token;
            }
            let write_start = Instant::now();
            validate_partition_writable(&merged, &output_header)
                .map_err(|e| format!("write partition: {e}"))?;
            writer
                .add_partition(&merged)
                .map_err(|e| format!("write partition: {e}"))?;
            crate::metrics::observe_compaction_phase(
                crate::metrics::CompactionPhase::WriterAddPartition,
                write_start.elapsed(),
            );
        }

        if merged_partition_count == 0 {
            return Err("no partitions to compact".into());
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

        let finish_start = Instant::now();
        let output = writer
            .finish_to_directory(staging_dir)
            .map_err(|e| format!("finish: {e}"))?;
        crate::metrics::observe_compaction_phase(
            crate::metrics::CompactionPhase::WriterFinish,
            finish_start.elapsed(),
        );
        let direct_upload = None;

        // 4. Promote staged output files via FileFlushTarget.
        let local_write_start = Instant::now();
        let reader = flush_target
            .flush_files(output)
            .map_err(|e| format!("flush output: {e}"))?;
        crate::metrics::observe_compaction_phase(
            crate::metrics::CompactionPhase::LocalWriteSstable,
            local_write_start.elapsed(),
        );

        if compaction_verify_output_enabled() {
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
        })
    }
}

#[derive(Debug)]
pub(crate) struct ExecutedCompaction {
    pub metadata: SSTableMetadata,
    pub direct_upload: Option<CompactionDirectUpload>,
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

    for r in readers {
        let h = r.header();
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

    SerializationHeader {
        min_timestamp,
        max_timestamp,
        min_local_deletion_time,
        min_ttl,
        ..template
    }
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

    #[test]
    fn submit_and_poll_result() {
        let executor = CompactionExecutor::new();

        let task = CompactionTask {
            inputs: vec![make_metadata("a", 1000), make_metadata("b", 2000)],
            output_dir: PathBuf::from("/tmp/output"),
            schema: test_table_schema(),
            table_id: test_table_id(),
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
        };

        assert!(CompactionExecutor::try_claim_in_flight_inputs(
            &executor.in_flight_inputs,
            &task
        ));
        assert!(
            !CompactionExecutor::try_claim_in_flight_inputs(&executor.in_flight_inputs, &task),
            "overlapping compaction must stay blocked while a completed result waits to be finalized"
        );

        executor.release_task_inputs(&task);
        assert!(
            CompactionExecutor::try_claim_in_flight_inputs(&executor.in_flight_inputs, &task),
            "poll_compactions finalization should release inputs for future compaction"
        );
        executor.release_task_inputs(&task);
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
        };

        let result = CompactionExecutor::execute_task(&task);
        assert!(
            result.is_err(),
            "Compaction must FAIL when an input Data.db is missing. \
             Silent skip + swap = data loss."
        );
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

        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();
        let task = CompactionTask {
            inputs: vec![meta],
            output_dir,
            schema,
            table_id: test_table_id(),
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
        let stop = Arc::new(AtomicBool::new(false));
        let live = Arc::new(AtomicUsize::new(0));
        let max_live = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..WORKERS {
            let gate = Arc::clone(&gate);
            let stop = Arc::clone(&stop);
            let live = Arc::clone(&live);
            let max_live = Arc::clone(&max_live);
            handles.push(std::thread::spawn(move || {
                for _ in 0..ITERS {
                    let permit = gate.acquire(&stop).expect("permit while not stopped");
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
    /// `acquire` returns `None` once `stop` is set.
    #[test]
    fn compaction_gate_unblocks_on_shutdown() {
        let gate = Arc::new(CompactionGate::new(1));
        let stop = Arc::new(AtomicBool::new(false));

        // Exhaust the single permit and hold it.
        let held = gate.acquire(&stop).expect("first permit");

        let waiter = {
            let gate = Arc::clone(&gate);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || gate.acquire(&stop).is_none())
        };
        // Give the waiter time to block on the unavailable permit.
        std::thread::sleep(std::time::Duration::from_millis(50));
        stop.store(true, Ordering::Release);
        let returned_none = waiter.join().unwrap();
        assert!(
            returned_none,
            "waiter must observe shutdown and stop blocking, not wait forever"
        );
        drop(held);
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
        };

        let pool: CompactionReaderPool = Arc::new(crate::reader_pool::ReaderPool::new(256));
        assert_eq!(pool.resident(), 0, "pool starts empty");
        let opens_before = crate::metrics::compaction_pool_input_opens_total();

        let result =
            CompactionExecutor::execute_task_inner(&task, Some(&pool), |_| {}).expect("compaction");

        // The pool-routed open counter advanced once per input.
        assert_eq!(
            crate::metrics::compaction_pool_input_opens_total() - opens_before,
            task.inputs.len() as u64,
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
        // 2 GB (the dev forcing function): half of RAM / 256 MB = 4 tasks,
        // capped by cpus. With ample cpus the memory bound (4) wins.
        assert_eq!(
            auto_tuned_max_concurrent(16, Some(2 * 1024 * 1024 * 1024)),
            4
        );
        // Few cpus cap below the memory allowance.
        assert_eq!(
            auto_tuned_max_concurrent(2, Some(2 * 1024 * 1024 * 1024)),
            2
        );
        // Tiny memory floors at 1, never zero.
        assert_eq!(auto_tuned_max_concurrent(8, Some(64 * 1024 * 1024)), 1);
        // Huge memory is still capped by the parallelism ceiling and cpus.
        assert_eq!(
            auto_tuned_max_concurrent(64, Some(256u64 * 1024 * 1024 * 1024)),
            MAX_AUTO_COMPACTION_PARALLELISM
        );
        // No memory signal → historical conservative default of 2 (cpu-capped).
        assert_eq!(auto_tuned_max_concurrent(8, None), 2);
        assert_eq!(auto_tuned_max_concurrent(1, None), 1);
    }

    #[test]
    fn auto_tuned_workers_track_cpus_within_bounds() {
        assert_eq!(auto_tuned_workers(1), 1);
        assert_eq!(auto_tuned_workers(4), 4);
        assert_eq!(auto_tuned_workers(64), MAX_AUTO_COMPACTION_PARALLELISM);
    }
}
