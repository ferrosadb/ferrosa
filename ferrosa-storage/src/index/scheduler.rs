//! Background index build scheduler.
//!
//! [`IndexBuildScheduler`] receives [`IndexBuildJob`]s via an mpsc channel and
//! processes them on N worker threads, following the same pattern as
//! `CompactionExecutor`.
//!
//! Workers currently only update the [`IndexStateTracker`] — actual SSTable
//! reading and index building will be wired in a later task.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use parking_lot::Mutex;

use super::tracker::IndexStateTracker;
use ferrosa_index::IndexType;

/// Priority for an index build job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildPriority {
    /// Normal priority — triggered by flush or compaction.
    Normal,
    /// High priority — eager build on flush/compaction completion.
    /// Keeps MemtableIndex (Layer 4) bounded to 0-1 entries in steady state.
    High,
    /// Initial build — the index was just created and needs a full build.
    Initial,
}

/// A job to build an index for a single SSTable.
#[derive(Debug, Clone)]
pub struct IndexBuildJob {
    /// SSTable to index.
    pub sstable_id: String,
    /// Name of the index to build.
    pub index_name: String,
    /// Type of the index.
    pub index_type: IndexType,
    /// (keyspace, table) the index belongs to.
    pub table: (String, String),
    /// Build priority.
    pub priority: BuildPriority,
    /// When this job was enqueued.
    pub enqueued_at: Instant,
    /// Column position within the row to index.
    pub column_position: usize,
    /// Partial-index predicate. `Some` only for [`IndexType::Filtered`] jobs:
    /// the build skips any row whose filter-column cell does not satisfy this
    /// predicate, so the sidecar holds only matching rows. `None` for every
    /// other index type (the whole-column index).
    pub filter_predicate: Option<ferrosa_index::FilterPredicate>,
}

/// Strategy for executing index builds.
///
/// The default implementation is [`LocalBackend`], which reads the SSTable
/// from local disk and produces sidecar entries in-process. Future
/// implementations can offload builds to remote nodes or external workers
/// by sending the SSTable's S3 path and index metadata over the network.
///
/// The trait is synchronous because the scheduler runs on dedicated OS
/// threads with a blocking mpsc channel (same pattern as `CompactionExecutor`).
pub trait IndexBuildBackend: Send + Sync {
    /// Build index entries for the given job.
    ///
    /// Returns sidecar entries on success, or a human-readable error string
    /// on failure. Failures are recorded in the `IndexStateTracker` but do
    /// not block compaction.
    fn build(&self, job: &IndexBuildJob) -> std::result::Result<IndexBuildResult, String>;
}

/// Result of a completed index build for a single SSTable.
///
/// Contains the sidecar entries produced by the backend for each index.
/// The scheduler uses this to write sidecar files and update the tracker.
#[derive(Debug, Clone)]
pub struct IndexBuildResult {
    /// SSTable that was indexed.
    pub sstable_id: String,
    /// Type of the index that was built. Lets the scheduler and reader
    /// selection stay type-correct after the build completes.
    pub index_type: IndexType,
    /// Per-index sidecar entries: index_name -> [(key, position)].
    pub sidecar_entries:
        HashMap<String, Vec<(ferrosa_index::IndexKey, ferrosa_index::RowPosition)>>,
    /// How long the build took.
    pub build_duration: std::time::Duration,
    /// If true, sidecar files were already written to S3 by the backend
    /// (e.g. `RemoteBackend`). The scheduler should skip local
    /// `SidecarWriter::write()`.
    pub sidecar_written_to_s3: bool,
    /// Object-backed artifacts already uploaded by the backend and validated
    /// for manifest publication.
    pub artifact_manifest_entries: Vec<crate::index::ArtifactManifestEntry>,
}

/// Default backend that builds indexes in-process from local SSTable files.
///
/// Reads the SSTable from `data_dir/{sstable_id}-Data.db`,
/// iterates all rows, and produces sidecar entries for each index.
pub struct LocalBackend {
    /// Root data directory where SSTable files live.
    data_dir: PathBuf,
}

impl LocalBackend {
    /// Create a new `LocalBackend` rooted at the given data directory.
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }
}

/// Encode a raw cell value into a sidecar key according to the index type.
///
/// `BTree`/`Hash`/`Composite`/`Filtered` use the raw value verbatim.
/// `Phonetic` returns the metaphone code of the UTF-8 text; non-UTF-8 or
/// empty-code values yield `None` (the row is skipped). `Vector`/`FullText`
/// are flush-built and must never reach this path.
pub(crate) fn encode_index_key(
    index_type: IndexType,
    value: &[u8],
) -> std::result::Result<Option<ferrosa_index::IndexKey>, String> {
    use ferrosa_index::phonetic::metaphone::MetaphoneEncoder;
    use ferrosa_index::phonetic::PhoneticEncoder;
    use ferrosa_index::IndexKey;

    match index_type {
        IndexType::BTree | IndexType::Hash | IndexType::Composite | IndexType::Filtered => {
            Ok(Some(IndexKey(value.to_vec())))
        }
        IndexType::Phonetic => {
            let text = match std::str::from_utf8(value) {
                Ok(t) => t,
                Err(_) => return Ok(None),
            };
            if text.is_empty() {
                return Ok(None);
            }
            let code = MetaphoneEncoder.encode(text);
            if code.is_empty() {
                Ok(None)
            } else {
                Ok(Some(IndexKey(code.into_bytes())))
            }
        }
        IndexType::Geo => match decode_geo_point(value) {
            Some((lat, lon)) => Ok(Some(IndexKey(
                ferrosa_index::geo::cell_id_key(lat, lon).to_vec(),
            ))),
            None => Ok(None),
        },
        IndexType::Vector | IndexType::FullText => Err(format!(
            "{index_type:?} indexes are flush-built and must not be scheduler-built"
        )),
    }
}

/// Decode a CQL `frozen<tuple<double,double>>` cell into `(lat, lon)`.
///
/// The wire layout is two elements, each `[i32 big-endian length][value]`.
/// A geo point therefore is `[0,0,0,8][8-byte f64 lat][0,0,0,8][8-byte f64 lon]`.
/// Returns `None` (skip the row) if the bytes are not a well-formed pair of
/// f64s rather than guessing — a malformed point must not silently index to a
/// wrong cell.
pub(crate) fn decode_geo_point(value: &[u8]) -> Option<(f64, f64)> {
    let lat = read_tuple_f64(value, 0)?;
    let lon = read_tuple_f64(value, 12)?;
    Some((lat, lon))
}

/// Read the f64 element at byte `at` of a CQL tuple body (4-byte length prefix
/// followed by an 8-byte big-endian f64).
fn read_tuple_f64(value: &[u8], at: usize) -> Option<f64> {
    let len_end = at.checked_add(4)?;
    let body_end = len_end.checked_add(8)?;
    if value.len() < body_end {
        return None;
    }
    let len = i32::from_be_bytes(value[at..len_end].try_into().ok()?);
    if len != 8 {
        return None;
    }
    let bytes: [u8; 8] = value[len_end..body_end].try_into().ok()?;
    Some(f64::from_be_bytes(bytes))
}

impl IndexBuildBackend for LocalBackend {
    fn build(&self, job: &IndexBuildJob) -> std::result::Result<IndexBuildResult, String> {
        use ferrosa_index::RowPosition;
        use ferrosa_sstable::io::FileReadAt;
        use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};

        let start = Instant::now();

        // Vector and FullText indexes are built on the flush path, not by the
        // scheduler. Skip them here so no garbage sidecar is produced.
        if matches!(job.index_type, IndexType::Vector | IndexType::FullText) {
            return Ok(IndexBuildResult {
                sstable_id: job.sstable_id.clone(),
                index_type: job.index_type,
                sidecar_entries: HashMap::new(),
                build_duration: start.elapsed(),
                sidecar_written_to_s3: false,
                artifact_manifest_entries: Vec::new(),
            });
        }

        let gen = &job.sstable_id;
        let dir = &self.data_dir;

        // Open SSTable components.
        let data = FileReadAt::open(dir.join(format!("{gen}-Data.db")))
            .map_err(|e| format!("open data: {e}"))?;
        let partitions_file = FileReadAt::open(dir.join(format!("{gen}-Partitions.db")))
            .map_err(|e| format!("open partitions: {e}"))?;
        let rows_file = FileReadAt::open(dir.join(format!("{gen}-Rows.db")))
            .map_err(|e| format!("open rows: {e}"))?;
        let filter = std::fs::read(dir.join(format!("{gen}-Filter.db")))
            .map_err(|e| format!("read filter: {e}"))?;
        let statistics = std::fs::read(dir.join(format!("{gen}-Statistics.db")))
            .map_err(|e| format!("read statistics: {e}"))?;
        let compression_info = std::fs::read(dir.join(format!("{gen}-CompressionInfo.db"))).ok();

        let reader = SSTableReader::open(SSTableComponents {
            data,
            partitions: partitions_file,
            rows: rows_file,
            filter,
            compression_info,
            statistics,
        })
        .map_err(|e| format!("open sstable: {e}"))?;

        // Stream partitions one at a time rather than materializing the whole
        // SSTable. `read_all_partitions()` would hold every decoded partition —
        // all cell values of all columns — in memory at once; on a large
        // SSTable that is an OOM hazard. `partitions_iter()` keeps a single
        // partition live plus the (much smaller) extracted index entries:
        // O(1) memory for uncompressed SSTables, O(decompressed chunk) for
        // compressed, independent of partition count. The index `entries` Vec
        // is inherently O(indexed rows) — a sidecar must index every row, so it
        // is not capped here (dropping entries would silently corrupt the
        // index); the win is not holding the full row payloads alongside it.
        let mut iter = reader
            .partitions_iter()
            .map_err(|e| format!("open partition iterator: {e}"))?;

        let mut entries: Vec<(ferrosa_index::IndexKey, RowPosition)> = Vec::new();

        while let Some(partition) = iter
            .next_partition()
            .map_err(|e| format!("read partition: {e}"))?
        {
            let pk_bytes = partition.key.key.as_bytes().to_vec();
            for row in &partition.rows {
                // Partial (filtered) index: skip rows whose filter-column cell
                // does not satisfy the predicate. The sidecar must hold ONLY
                // matching rows so the read path (which delegates to a plain
                // btree point/range lookup) returns the partial result set.
                // `evaluate_predicate` is the shared source of truth with the
                // memtable write path, so sidecar and memtable agree.
                if let Some(predicate) = job.filter_predicate.as_ref() {
                    let filter_cell = row
                        .cells
                        .iter()
                        .find(|(pos, _)| *pos as usize == predicate.column_position);
                    let matches = match filter_cell.and_then(|(_, c)| c.value.as_ref()) {
                        Some(value) => ferrosa_index::evaluate_predicate(predicate, value),
                        // Filter column absent or tombstoned -> does not match.
                        None => false,
                    };
                    if !matches {
                        continue;
                    }
                }

                // Find the cell at the declared column position.
                let cell_opt = row
                    .cells
                    .iter()
                    .find(|(pos, _)| *pos == job.column_position as u16);
                if let Some((_col_pos, cell)) = cell_opt {
                    if let Some(ref value) = cell.value {
                        if let Some(key) = encode_index_key(job.index_type, value)? {
                            entries.push((
                                key,
                                RowPosition {
                                    partition_key: pk_bytes.clone(),
                                    clustering_key: row.clustering.clone(),
                                },
                            ));
                        }
                    }
                }
            }
        }

        let mut sidecar_entries = HashMap::new();
        if !entries.is_empty() {
            sidecar_entries.insert(job.index_name.clone(), entries);
        }

        Ok(IndexBuildResult {
            sstable_id: job.sstable_id.clone(),
            index_type: job.index_type,
            sidecar_entries,
            build_duration: start.elapsed(),
            sidecar_written_to_s3: false,
            artifact_manifest_entries: Vec::new(),
        })
    }
}

/// Callback invoked after each index build job completes successfully.
pub type BuildCompleteCallback = Box<dyn Fn(&IndexBuildJob) + Send + Sync>;

/// Background scheduler that dispatches index build jobs to worker threads.
///
/// Follows the `CompactionExecutor` pattern: an mpsc channel feeds N worker
/// threads. Each worker pulls jobs, performs the build (stub for now), and
/// updates the shared [`IndexStateTracker`].
pub struct IndexBuildScheduler {
    task_tx: std::sync::mpsc::Sender<IndexBuildJob>,
    handles: Mutex<Vec<thread::JoinHandle<()>>>,
    stop_flag: Arc<AtomicBool>,
    /// Root data directory for writing sidecar files.
    #[allow(dead_code)]
    data_dir: Option<PathBuf>,
    #[allow(dead_code)]
    on_build_complete: Arc<Option<BuildCompleteCallback>>,
}

impl IndexBuildScheduler {
    /// Creates and starts the index build scheduler with `worker_count` threads.
    ///
    /// Uses a no-op stub backend for backward compatibility. Call
    /// [`with_backend()`](Self::with_backend) for production use with a real backend.
    pub fn new(worker_count: usize, tracker: Arc<IndexStateTracker>) -> Self {
        // Stub backend that does nothing -- preserves existing test behavior.
        struct StubBackend;
        impl IndexBuildBackend for StubBackend {
            fn build(&self, job: &IndexBuildJob) -> std::result::Result<IndexBuildResult, String> {
                Ok(IndexBuildResult {
                    sstable_id: job.sstable_id.clone(),
                    index_type: job.index_type,
                    sidecar_entries: HashMap::new(),
                    build_duration: std::time::Duration::from_millis(0),
                    sidecar_written_to_s3: false,
                    artifact_manifest_entries: Vec::new(),
                })
            }
        }
        Self::with_backend(worker_count, tracker, Arc::new(StubBackend))
    }

    /// Creates the scheduler with an optional completion callback (no backend).
    ///
    /// The callback is invoked on the worker thread after each successful
    /// build. The cluster layer uses this to propose `RaftOp::IndexStatus`.
    pub fn with_callback(
        worker_count: usize,
        tracker: Arc<IndexStateTracker>,
        on_build_complete: Option<BuildCompleteCallback>,
    ) -> Self {
        let (task_tx, task_rx) = std::sync::mpsc::channel::<IndexBuildJob>();
        let task_rx = Arc::new(Mutex::new(task_rx));
        let stop_flag = Arc::new(AtomicBool::new(false));
        let on_build_complete = Arc::new(on_build_complete);

        let mut handles = Vec::with_capacity(worker_count);

        for i in 0..worker_count {
            let rx = Arc::clone(&task_rx);
            let stop = Arc::clone(&stop_flag);
            let tracker = Arc::clone(&tracker);
            let callback = Arc::clone(&on_build_complete);

            let handle = thread::Builder::new()
                .name(format!("index-builder-{i}"))
                .spawn(move || {
                    Self::worker_loop(&rx, &stop, &tracker, &callback);
                })
                .expect("failed to spawn index builder thread");

            handles.push(handle);
        }

        Self {
            task_tx,
            handles: Mutex::new(handles),
            stop_flag,
            data_dir: None,
            on_build_complete,
        }
    }

    /// Creates a scheduler with the given backend for executing builds.
    pub fn with_backend(
        worker_count: usize,
        tracker: Arc<IndexStateTracker>,
        backend: Arc<dyn IndexBuildBackend>,
    ) -> Self {
        let (task_tx, task_rx) = std::sync::mpsc::channel::<IndexBuildJob>();
        let task_rx = Arc::new(Mutex::new(task_rx));
        let stop_flag = Arc::new(AtomicBool::new(false));
        let on_build_complete = Arc::new(None);

        let mut handles = Vec::with_capacity(worker_count);

        for i in 0..worker_count {
            let rx = Arc::clone(&task_rx);
            let stop = Arc::clone(&stop_flag);
            let tracker = Arc::clone(&tracker);
            let backend = Arc::clone(&backend);

            let handle = thread::Builder::new()
                .name(format!("index-builder-{i}"))
                .spawn(move || {
                    Self::worker_loop_with_backend(&rx, &stop, &tracker, &*backend);
                })
                .expect("failed to spawn index builder thread");

            handles.push(handle);
        }

        Self {
            task_tx,
            handles: Mutex::new(handles),
            stop_flag,
            data_dir: None,
            on_build_complete,
        }
    }

    /// Creates a scheduler with backend and data directory for sidecar writes.
    pub fn with_backend_and_data_dir(
        worker_count: usize,
        tracker: Arc<IndexStateTracker>,
        backend: Arc<dyn IndexBuildBackend>,
        data_dir: PathBuf,
    ) -> Self {
        let (task_tx, task_rx) = std::sync::mpsc::channel::<IndexBuildJob>();
        let task_rx = Arc::new(Mutex::new(task_rx));
        let stop_flag = Arc::new(AtomicBool::new(false));
        let on_build_complete = Arc::new(None);

        let mut handles = Vec::with_capacity(worker_count);

        for i in 0..worker_count {
            let rx = Arc::clone(&task_rx);
            let stop = Arc::clone(&stop_flag);
            let tracker = Arc::clone(&tracker);
            let backend = Arc::clone(&backend);
            let data_dir = data_dir.clone();

            let handle = thread::Builder::new()
                .name(format!("index-builder-{i}"))
                .spawn(move || {
                    Self::worker_loop_full(&rx, &stop, &tracker, &*backend, &data_dir);
                })
                .expect("failed to spawn index builder thread");

            handles.push(handle);
        }

        Self {
            task_tx,
            handles: Mutex::new(handles),
            stop_flag,
            data_dir: Some(data_dir),
            on_build_complete,
        }
    }

    /// Submits a build job to the worker pool.
    pub fn submit(&self, job: IndexBuildJob) -> ferrosa_common::Result<()> {
        self.task_tx
            .send(job)
            .map_err(|_| ferrosa_common::Error::InvalidFormat("index build channel closed".into()))
    }

    /// Shuts down the scheduler, waiting for all worker threads to finish.
    pub fn shutdown(&self) {
        self.stop_flag.store(true, Ordering::Release);
        let mut handles = self.handles.lock();
        for handle in handles.drain(..) {
            let _ = handle.join();
        }
    }

    /// Shuts down the scheduler, waiting up to `timeout` for in-flight builds.
    ///
    /// Sets the stop flag, then joins each worker thread with the given timeout.
    /// Workers that do not finish within the timeout are detached (their threads
    /// will exit when their current job completes, but we stop waiting).
    pub fn shutdown_with_timeout(&self, timeout: std::time::Duration) {
        self.stop_flag.store(true, Ordering::Release);
        let deadline = Instant::now() + timeout;
        let mut handles = self.handles.lock();
        for handle in handles.drain(..) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // Timeout exhausted; detach remaining threads.
                break;
            }
            let start = Instant::now();
            loop {
                if handle.is_finished() {
                    let _ = handle.join();
                    break;
                }
                if start.elapsed() >= remaining {
                    // Timeout for this thread; detach it.
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }

    /// Worker loop with optional callback after each successful build.
    fn worker_loop(
        rx: &Mutex<std::sync::mpsc::Receiver<IndexBuildJob>>,
        stop: &AtomicBool,
        tracker: &IndexStateTracker,
        on_build_complete: &Option<BuildCompleteCallback>,
    ) {
        while !stop.load(Ordering::Acquire) {
            let job = {
                let rx_guard = rx.lock();
                rx_guard.recv_timeout(std::time::Duration::from_millis(100))
            };

            match job {
                Ok(job) => {
                    let (keyspace, table) = &job.table;
                    tracker.mark_indexed(keyspace, table, &job.index_name, &job.sstable_id);
                    if let Some(cb) = on_build_complete.as_ref() {
                        cb(&job);
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    /// Worker loop that delegates to the backend.
    fn worker_loop_with_backend(
        rx: &Mutex<std::sync::mpsc::Receiver<IndexBuildJob>>,
        stop: &AtomicBool,
        tracker: &IndexStateTracker,
        backend: &dyn IndexBuildBackend,
    ) {
        while !stop.load(Ordering::Acquire) {
            let job = {
                let rx_guard = rx.lock();
                rx_guard.recv_timeout(std::time::Duration::from_millis(100))
            };

            match job {
                Ok(job) => {
                    let (keyspace, table) = &job.table;
                    match backend.build(&job) {
                        Ok(_result) => {
                            tracker.mark_indexed(keyspace, table, &job.index_name, &job.sstable_id);
                        }
                        Err(err) => {
                            tracker.mark_failed(
                                keyspace,
                                table,
                                &job.index_name,
                                err,
                                std::time::Duration::from_secs(60),
                            );
                        }
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    /// Worker loop that delegates to backend and writes sidecar files.
    fn worker_loop_full(
        rx: &Mutex<std::sync::mpsc::Receiver<IndexBuildJob>>,
        stop: &AtomicBool,
        tracker: &IndexStateTracker,
        backend: &dyn IndexBuildBackend,
        data_dir: &std::path::Path,
    ) {
        use crate::index::sidecar::SidecarWriter;

        while !stop.load(Ordering::Acquire) {
            let job = {
                let rx_guard = rx.lock();
                rx_guard.recv_timeout(std::time::Duration::from_millis(100))
            };

            match job {
                Ok(job) => {
                    let (keyspace, table) = &job.table;
                    match backend.build(&job) {
                        Ok(result) => {
                            // Write sidecar files locally unless the backend
                            // already wrote them to S3 (e.g. RemoteBackend).
                            if !result.sidecar_written_to_s3 {
                                for (index_name, entries) in &result.sidecar_entries {
                                    if entries.is_empty() {
                                        continue;
                                    }
                                    let path = data_dir
                                        .join(format!("{}-{}.sidecar", job.sstable_id, index_name));
                                    if let Err(e) = SidecarWriter::write(&path, entries) {
                                        tracing::error!(%e, path = %path.display(), "index-build: failed to write sidecar");
                                    }
                                }
                            }
                            tracker.mark_indexed(keyspace, table, &job.index_name, &job.sstable_id);
                        }
                        Err(err) => {
                            tracker.mark_failed(
                                keyspace,
                                table,
                                &job.index_name,
                                err,
                                std::time::Duration::from_secs(60),
                            );
                        }
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flush::FlushTarget;
    use crate::index::IndexStatus;
    use std::time::Duration;

    /// Build a CQL `frozen<tuple<double,double>>` wire body for a point.
    fn geo_tuple_bytes(lat: f64, lon: f64) -> Vec<u8> {
        let mut v = Vec::with_capacity(24);
        for f in [lat, lon] {
            v.extend_from_slice(&8i32.to_be_bytes());
            v.extend_from_slice(&f.to_be_bytes());
        }
        v
    }

    #[test]
    fn geo_index_encodes_cell_id_key() {
        let bytes = geo_tuple_bytes(37.7749, -122.4194);
        let key = encode_index_key(IndexType::Geo, &bytes)
            .expect("geo encode must not error")
            .expect("well-formed point yields a key");
        let expected = ferrosa_index::geo::cell_id_key(37.7749, -122.4194);
        assert_eq!(key.0, expected.to_vec());
    }

    #[test]
    fn geo_keys_are_sortable_by_proximity_prefix() {
        let near_a = encode_index_key(IndexType::Geo, &geo_tuple_bytes(37.7749, -122.4194))
            .unwrap()
            .unwrap();
        let near_b = encode_index_key(IndexType::Geo, &geo_tuple_bytes(37.7750, -122.4195))
            .unwrap()
            .unwrap();
        // Adjacent points share a long big-endian key prefix.
        assert_eq!(near_a.0[..3], near_b.0[..3]);
    }

    #[test]
    fn geo_index_skips_malformed_point() {
        // Too short / wrong length prefix => skip the row rather than mis-index.
        let result = encode_index_key(IndexType::Geo, &[0, 0, 0, 4, 1, 2, 3, 4]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn decode_geo_point_round_trips() {
        let bytes = geo_tuple_bytes(-12.5, 178.25);
        let (lat, lon) = decode_geo_point(&bytes).expect("valid tuple decodes");
        assert!((lat - (-12.5)).abs() < 1e-12);
        assert!((lon - 178.25).abs() < 1e-12);
    }

    #[test]
    fn scheduler_processes_jobs() {
        let tracker = Arc::new(IndexStateTracker::new());
        tracker.register_index("ks", "tbl", "my_idx");
        tracker.mark_pending("ks", "tbl", "my_idx", "sst-001", 500);

        let scheduler = IndexBuildScheduler::new(2, Arc::clone(&tracker));

        let job = IndexBuildJob {
            sstable_id: "sst-001".to_string(),
            index_name: "my_idx".to_string(),
            index_type: IndexType::BTree,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: None,
        };

        scheduler.submit(job).unwrap();

        // Wait for the worker to process the job.
        std::thread::sleep(Duration::from_millis(500));

        let state = tracker.get_state("ks", "tbl", "my_idx").unwrap();
        assert!(
            state.indexed_sstables.contains("sst-001"),
            "sst-001 should be marked as indexed"
        );
        assert!(
            state.pending_sstables.is_empty(),
            "no pending sstables should remain"
        );
        assert!(
            matches!(state.status, IndexStatus::Current),
            "status should be Current after indexing, got {:?}",
            state.status
        );

        scheduler.shutdown();
    }

    #[test]
    fn scheduler_shutdown_stops_cleanly() {
        let tracker = Arc::new(IndexStateTracker::new());
        let scheduler = IndexBuildScheduler::new(1, tracker);
        scheduler.shutdown();
        // Should not hang or panic.
    }

    #[test]
    fn scheduler_processes_multiple_jobs() {
        let tracker = Arc::new(IndexStateTracker::new());
        tracker.register_index("ks", "tbl", "idx");
        tracker.mark_pending("ks", "tbl", "idx", "sst-001", 100);
        tracker.mark_pending("ks", "tbl", "idx", "sst-002", 200);
        tracker.mark_pending("ks", "tbl", "idx", "sst-003", 300);

        let scheduler = IndexBuildScheduler::new(2, Arc::clone(&tracker));

        for id in ["sst-001", "sst-002", "sst-003"] {
            scheduler
                .submit(IndexBuildJob {
                    sstable_id: id.to_string(),
                    index_name: "idx".to_string(),
                    index_type: IndexType::Hash,
                    table: ("ks".to_string(), "tbl".to_string()),
                    priority: BuildPriority::Initial,
                    enqueued_at: Instant::now(),
                    column_position: 0,
                    filter_predicate: None,
                })
                .unwrap();
        }

        std::thread::sleep(Duration::from_millis(500));

        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert_eq!(state.total_builds, 3);
        assert!(state.pending_sstables.is_empty());
        assert_eq!(state.indexed_sstables.len(), 3);

        scheduler.shutdown();
    }

    #[test]
    fn index_build_result_creation() {
        use ferrosa_index::{IndexKey, RowPosition};

        let entries = vec![(
            IndexKey(b"val1".to_vec()),
            RowPosition {
                partition_key: b"pk1".to_vec(),
                clustering_key: b"ck1".to_vec(),
            },
        )];

        let mut sidecar_entries = std::collections::HashMap::new();
        sidecar_entries.insert("my_idx".to_string(), entries);

        let result = IndexBuildResult {
            sstable_id: "sst-001".to_string(),
            index_type: IndexType::BTree,
            sidecar_entries,
            build_duration: Duration::from_millis(42),
            sidecar_written_to_s3: false,
            artifact_manifest_entries: Vec::new(),
        };

        assert_eq!(result.sstable_id, "sst-001");
        assert_eq!(result.sidecar_entries.len(), 1);
        assert!(result.sidecar_entries.contains_key("my_idx"));
        assert_eq!(result.build_duration.as_millis(), 42);
    }

    #[test]
    fn index_build_backend_is_object_safe() {
        // Verify the trait can be used as a trait object.
        struct MockBackend;
        impl IndexBuildBackend for MockBackend {
            fn build(&self, _job: &IndexBuildJob) -> std::result::Result<IndexBuildResult, String> {
                Ok(IndexBuildResult {
                    sstable_id: "mock".to_string(),
                    index_type: IndexType::BTree,
                    sidecar_entries: std::collections::HashMap::new(),
                    build_duration: Duration::from_millis(0),
                    sidecar_written_to_s3: false,
                    artifact_manifest_entries: Vec::new(),
                })
            }
        }

        let backend: Arc<dyn IndexBuildBackend> = Arc::new(MockBackend);
        let job = IndexBuildJob {
            sstable_id: "sst-test".to_string(),
            index_name: "idx".to_string(),
            index_type: IndexType::BTree,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: None,
        };
        let result = backend.build(&job).unwrap();
        assert_eq!(result.sstable_id, "mock");
    }

    #[test]
    fn local_backend_construction() {
        // Verify LocalBackend can be constructed.
        let backend = LocalBackend::new(PathBuf::from("/tmp/test-data"));
        // Build on a nonexistent dir returns an error (no SSTable files).
        let job = IndexBuildJob {
            sstable_id: "sst-001".to_string(),
            index_name: "my_idx".to_string(),
            index_type: IndexType::BTree,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: None,
        };
        let result = backend.build(&job);
        assert!(result.is_err());
    }

    #[test]
    fn local_backend_builds_sidecar_from_sstable() {
        use ferrosa_common::cell::CellValue;
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
        use ferrosa_sstable::writer::SSTableWriter;
        use ferrosa_sstable::WriteOptions;

        let dir = tempfile::tempdir().unwrap();
        let sstable_dir = dir.path().to_path_buf();

        // Create a table schema with one clustering and one regular column.
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
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

        // Build two partitions with distinct values.
        let mut partitions = vec![
            Partition {
                key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x01],
                    cells: vec![(0, CellValue::live(b"alice".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                }],
            },
            Partition {
                key: DecoratedKey::new(PartitionKey::new(b"pk2".to_vec())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![0x00, 0x00, 0x00, 0x02],
                    cells: vec![(0, CellValue::live(b"bob".to_vec(), 2000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(2000),
                }],
            },
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = crate::flush::build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        // Write SSTable files to disk with generation "1".
        let flush_target = crate::flush::FileFlushTarget::new(sstable_dir.clone()).unwrap();
        let _reader = flush_target.flush(output).unwrap();
        let gen = flush_target.generation();

        // Build using LocalBackend.
        let backend = LocalBackend::new(sstable_dir.clone());
        let job = IndexBuildJob {
            sstable_id: format!("{gen}"),
            index_name: "val_idx".to_string(),
            index_type: IndexType::BTree,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: None,
        };
        let result = backend.build(&job).unwrap();

        assert_eq!(result.sstable_id, format!("{gen}"));
        // The backend should produce sidecar entries for the "val_idx" index.
        assert!(
            result.sidecar_entries.contains_key("val_idx"),
            "expected val_idx in sidecar_entries, got keys: {:?}",
            result.sidecar_entries.keys().collect::<Vec<_>>()
        );
        let entries = &result.sidecar_entries["val_idx"];
        assert_eq!(
            entries.len(),
            2,
            "expected 2 sidecar entries, got {}",
            entries.len()
        );
    }

    /// A Filtered (partial) index build must skip rows whose filter-column cell
    /// does not satisfy the predicate, so the sidecar holds only matching rows.
    /// Here the index is on `name` (col 0), partial on `status = 'active'`
    /// (col 1). Two rows are active, one is inactive — the sidecar must contain
    /// exactly the two active rows.
    #[test]
    fn local_backend_filtered_build_skips_non_matching_rows() {
        use ferrosa_common::cell::CellValue;
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        use ferrosa_index::{FilterOp, FilterPredicate};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
        use ferrosa_sstable::writer::SSTableWriter;
        use ferrosa_sstable::WriteOptions;

        let dir = tempfile::tempdir().unwrap();
        let sstable_dir = dir.path().to_path_buf();

        // name (col 0) is indexed; status (col 1) is the filter column.
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "status".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            extensions: Default::default(),
        };

        let mut partitions = vec![
            Partition {
                key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![
                        (0, CellValue::live(b"alice".to_vec(), 1000)),
                        (1, CellValue::live(b"active".to_vec(), 1000)),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                }],
            },
            Partition {
                key: DecoratedKey::new(PartitionKey::new(b"pk2".to_vec())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![
                        (0, CellValue::live(b"bob".to_vec(), 2000)),
                        (1, CellValue::live(b"inactive".to_vec(), 2000)),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(2000),
                }],
            },
            Partition {
                key: DecoratedKey::new(PartitionKey::new(b"pk3".to_vec())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![
                        (0, CellValue::live(b"carol".to_vec(), 3000)),
                        (1, CellValue::live(b"active".to_vec(), 3000)),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(3000),
                }],
            },
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = crate::flush::build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let flush_target = crate::flush::FileFlushTarget::new(sstable_dir.clone()).unwrap();
        let _reader = flush_target.flush(output).unwrap();
        let gen = flush_target.generation();

        let backend = LocalBackend::new(sstable_dir.clone());
        let job = IndexBuildJob {
            sstable_id: format!("{gen}"),
            index_name: "name_active_idx".to_string(),
            index_type: IndexType::Filtered,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: Some(FilterPredicate {
                column_position: 1,
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            }),
        };
        let result = backend.build(&job).unwrap();

        let entries = result
            .sidecar_entries
            .get("name_active_idx")
            .expect("filtered index must produce sidecar entries");
        // Only the two active rows (alice, carol) are indexed; bob is skipped.
        assert_eq!(
            entries.len(),
            2,
            "filtered index must hold only the 2 active rows, got {}",
            entries.len()
        );
        let mut pks: Vec<Vec<u8>> = entries
            .iter()
            .map(|(_, pos)| pos.partition_key.clone())
            .collect();
        pks.sort();
        assert_eq!(pks, vec![b"pk1".to_vec(), b"pk3".to_vec()]);
    }

    #[test]
    fn index_build_job_has_column_position() {
        let job = IndexBuildJob {
            sstable_id: "sst-001".to_string(),
            index_name: "val_idx".to_string(),
            index_type: IndexType::BTree,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 2,
            filter_predicate: None,
        };
        assert_eq!(job.column_position, 2);
    }

    #[test]
    fn local_backend_returns_error_for_missing_sstable() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().to_path_buf());
        let job = IndexBuildJob {
            sstable_id: "nonexistent".to_string(),
            index_name: "idx".to_string(),
            index_type: IndexType::BTree,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: None,
        };
        let result = backend.build(&job);
        assert!(result.is_err(), "expected error for missing SSTable");
        let err = result.unwrap_err();
        assert!(
            err.contains("open data"),
            "expected file-open error, got: {err}"
        );
    }

    #[test]
    fn scheduler_delegates_to_backend() {
        use std::sync::atomic::AtomicUsize;

        static BUILD_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct CountingBackend;
        impl IndexBuildBackend for CountingBackend {
            fn build(&self, job: &IndexBuildJob) -> std::result::Result<IndexBuildResult, String> {
                BUILD_COUNT.fetch_add(1, Ordering::Relaxed);
                Ok(IndexBuildResult {
                    sstable_id: job.sstable_id.clone(),
                    index_type: job.index_type,
                    sidecar_entries: std::collections::HashMap::new(),
                    build_duration: Duration::from_millis(1),
                    sidecar_written_to_s3: false,
                    artifact_manifest_entries: Vec::new(),
                })
            }
        }

        BUILD_COUNT.store(0, Ordering::Relaxed);
        let tracker = Arc::new(IndexStateTracker::new());
        tracker.register_index("ks", "tbl", "idx");
        tracker.mark_pending("ks", "tbl", "idx", "sst-001", 100);

        let backend: Arc<dyn IndexBuildBackend> = Arc::new(CountingBackend);
        let scheduler = IndexBuildScheduler::with_backend(2, Arc::clone(&tracker), backend);

        scheduler
            .submit(IndexBuildJob {
                sstable_id: "sst-001".to_string(),
                index_name: "idx".to_string(),
                index_type: IndexType::BTree,
                table: ("ks".to_string(), "tbl".to_string()),
                priority: BuildPriority::Normal,
                enqueued_at: Instant::now(),
                column_position: 0,
                filter_predicate: None,
            })
            .unwrap();

        std::thread::sleep(Duration::from_millis(500));

        assert_eq!(BUILD_COUNT.load(Ordering::Relaxed), 1);
        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert!(matches!(state.status, IndexStatus::Current));

        scheduler.shutdown();
    }

    #[test]
    fn scheduler_backend_failure_marks_index_failed() {
        struct FailingBackend;
        impl IndexBuildBackend for FailingBackend {
            fn build(&self, _job: &IndexBuildJob) -> std::result::Result<IndexBuildResult, String> {
                Err("disk full".to_string())
            }
        }

        let tracker = Arc::new(IndexStateTracker::new());
        tracker.register_index("ks", "tbl", "idx");
        tracker.mark_pending("ks", "tbl", "idx", "sst-001", 100);

        let backend: Arc<dyn IndexBuildBackend> = Arc::new(FailingBackend);
        let scheduler = IndexBuildScheduler::with_backend(1, Arc::clone(&tracker), backend);

        scheduler
            .submit(IndexBuildJob {
                sstable_id: "sst-001".to_string(),
                index_name: "idx".to_string(),
                index_type: IndexType::BTree,
                table: ("ks".to_string(), "tbl".to_string()),
                priority: BuildPriority::Normal,
                enqueued_at: Instant::now(),
                column_position: 0,
                filter_predicate: None,
            })
            .unwrap();

        std::thread::sleep(Duration::from_millis(500));

        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert!(
            matches!(state.status, IndexStatus::Failed { .. }),
            "expected Failed status, got {:?}",
            state.status
        );
        assert_eq!(state.total_build_errors, 1);

        scheduler.shutdown();
    }

    #[test]
    fn scheduler_shutdown_with_timeout_completes_in_flight_jobs() {
        use std::sync::atomic::AtomicUsize;

        static COMPLETED: AtomicUsize = AtomicUsize::new(0);

        struct SlowBackend;
        impl IndexBuildBackend for SlowBackend {
            fn build(&self, job: &IndexBuildJob) -> std::result::Result<IndexBuildResult, String> {
                std::thread::sleep(Duration::from_millis(200));
                COMPLETED.fetch_add(1, Ordering::Relaxed);
                Ok(IndexBuildResult {
                    sstable_id: job.sstable_id.clone(),
                    index_type: job.index_type,
                    sidecar_entries: HashMap::new(),
                    build_duration: Duration::from_millis(200),
                    sidecar_written_to_s3: false,
                    artifact_manifest_entries: Vec::new(),
                })
            }
        }

        COMPLETED.store(0, Ordering::Relaxed);
        let tracker = Arc::new(IndexStateTracker::new());
        tracker.register_index("ks", "tbl", "idx");
        tracker.mark_pending("ks", "tbl", "idx", "sst-001", 100);

        let backend: Arc<dyn IndexBuildBackend> = Arc::new(SlowBackend);
        let scheduler = IndexBuildScheduler::with_backend(1, Arc::clone(&tracker), backend);

        scheduler
            .submit(IndexBuildJob {
                sstable_id: "sst-001".to_string(),
                index_name: "idx".to_string(),
                index_type: IndexType::BTree,
                table: ("ks".to_string(), "tbl".to_string()),
                priority: BuildPriority::Normal,
                enqueued_at: Instant::now(),
                column_position: 0,
                filter_predicate: None,
            })
            .unwrap();

        // Give the job time to be picked up by the worker thread.
        std::thread::sleep(Duration::from_millis(50));

        // Shutdown with a 5-second timeout -- the 200ms job should complete.
        scheduler.shutdown_with_timeout(Duration::from_secs(5));

        assert_eq!(COMPLETED.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn scheduler_shutdown_timeout_does_not_hang_on_slow_job() {
        struct VerySlowBackend;
        impl IndexBuildBackend for VerySlowBackend {
            fn build(&self, job: &IndexBuildJob) -> std::result::Result<IndexBuildResult, String> {
                std::thread::sleep(Duration::from_secs(30));
                Ok(IndexBuildResult {
                    sstable_id: job.sstable_id.clone(),
                    index_type: job.index_type,
                    sidecar_entries: HashMap::new(),
                    build_duration: Duration::from_secs(30),
                    sidecar_written_to_s3: false,
                    artifact_manifest_entries: Vec::new(),
                })
            }
        }

        let tracker = Arc::new(IndexStateTracker::new());
        let backend: Arc<dyn IndexBuildBackend> = Arc::new(VerySlowBackend);
        let scheduler = IndexBuildScheduler::with_backend(1, Arc::clone(&tracker), backend);

        scheduler
            .submit(IndexBuildJob {
                sstable_id: "sst-slow".to_string(),
                index_name: "idx".to_string(),
                index_type: IndexType::BTree,
                table: ("ks".to_string(), "tbl".to_string()),
                priority: BuildPriority::Normal,
                enqueued_at: Instant::now(),
                column_position: 0,
                filter_predicate: None,
            })
            .unwrap();

        let start = Instant::now();
        // Short timeout: 500ms. The 30s job will not finish.
        scheduler.shutdown_with_timeout(Duration::from_millis(500));
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(2),
            "shutdown should return within ~500ms, took {:?}",
            elapsed
        );
    }

    #[test]
    fn scheduler_writes_sidecar_files_from_backend_result() {
        use ferrosa_index::{IndexKey, RowPosition};

        let dir = tempfile::tempdir().unwrap();
        let sidecar_dir = dir.path().to_path_buf();

        struct SidecarBackend;
        impl IndexBuildBackend for SidecarBackend {
            fn build(&self, job: &IndexBuildJob) -> std::result::Result<IndexBuildResult, String> {
                let mut sidecar_entries = HashMap::new();
                sidecar_entries.insert(
                    job.index_name.clone(),
                    vec![(
                        IndexKey(b"val1".to_vec()),
                        RowPosition {
                            partition_key: b"pk1".to_vec(),
                            clustering_key: vec![],
                        },
                    )],
                );
                Ok(IndexBuildResult {
                    sstable_id: job.sstable_id.clone(),
                    index_type: job.index_type,
                    sidecar_entries,
                    build_duration: Duration::from_millis(1),
                    sidecar_written_to_s3: false,
                    artifact_manifest_entries: Vec::new(),
                })
            }
        }

        let tracker = Arc::new(IndexStateTracker::new());
        tracker.register_index("ks", "tbl", "my_idx");
        tracker.mark_pending("ks", "tbl", "my_idx", "1", 100);

        let backend: Arc<dyn IndexBuildBackend> = Arc::new(SidecarBackend);
        let scheduler = IndexBuildScheduler::with_backend_and_data_dir(
            1,
            Arc::clone(&tracker),
            backend,
            sidecar_dir.clone(),
        );

        scheduler
            .submit(IndexBuildJob {
                sstable_id: "1".to_string(),
                index_name: "my_idx".to_string(),
                index_type: IndexType::BTree,
                table: ("ks".to_string(), "tbl".to_string()),
                priority: BuildPriority::Normal,
                enqueued_at: Instant::now(),
                column_position: 0,
                filter_predicate: None,
            })
            .unwrap();

        std::thread::sleep(Duration::from_millis(500));

        // Verify sidecar file was written.
        let sidecar_path = sidecar_dir.join("1-my_idx.sidecar");
        assert!(
            sidecar_path.exists(),
            "sidecar file should exist at {}",
            sidecar_path.display()
        );

        // Verify contents.
        let reader = crate::index::sidecar::SidecarReader::open(&sidecar_path).unwrap();
        assert_eq!(reader.entry_count(), 1);
        let results = reader.lookup(&IndexKey(b"val1".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk1");

        scheduler.shutdown();
    }

    #[test]
    fn scheduler_invokes_on_build_complete_callback() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let tracker = Arc::new(IndexStateTracker::new());
        tracker.register_index("ks", "tbl", "my_idx");
        tracker.mark_pending("ks", "tbl", "my_idx", "sst-001", 500);

        let callback_count = Arc::new(AtomicU32::new(0));
        let cb_count = Arc::clone(&callback_count);

        let scheduler = IndexBuildScheduler::with_callback(
            2,
            Arc::clone(&tracker),
            Some(Box::new(move |job: &IndexBuildJob| {
                cb_count.fetch_add(1, Ordering::SeqCst);
                assert_eq!(job.index_name, "my_idx");
            })),
        );

        scheduler
            .submit(IndexBuildJob {
                sstable_id: "sst-001".to_string(),
                index_name: "my_idx".to_string(),
                index_type: IndexType::BTree,
                table: ("ks".to_string(), "tbl".to_string()),
                priority: BuildPriority::Normal,
                enqueued_at: Instant::now(),
                column_position: 0,
                filter_predicate: None,
            })
            .unwrap();

        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(callback_count.load(Ordering::SeqCst), 1);

        scheduler.shutdown();
    }

    #[test]
    fn scheduler_callback_not_called_when_no_jobs() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let tracker = Arc::new(IndexStateTracker::new());
        let callback_count = Arc::new(AtomicU32::new(0));
        let cb_count = Arc::clone(&callback_count);

        let scheduler = IndexBuildScheduler::with_callback(
            1,
            tracker,
            Some(Box::new(move |_: &IndexBuildJob| {
                cb_count.fetch_add(1, Ordering::SeqCst);
            })),
        );

        std::thread::sleep(Duration::from_millis(300));
        scheduler.shutdown();

        assert_eq!(callback_count.load(Ordering::SeqCst), 0);
    }

    /// Build a single-column SSTable with one UTF-8 text value per partition and
    /// return its data directory and generation id. The column to index is at
    /// position 0 (the lone regular column).
    fn write_text_sstable(values: &[(&[u8], &[u8])]) -> (tempfile::TempDir, String) {
        use ferrosa_common::cell::CellValue;
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
        use ferrosa_sstable::writer::SSTableWriter;
        use ferrosa_sstable::WriteOptions;

        let dir = tempfile::tempdir().unwrap();
        let sstable_dir = dir.path().to_path_buf();

        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
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

        let mut partitions: Vec<Partition> = values
            .iter()
            .enumerate()
            .map(|(i, (pk, val))| Partition {
                key: DecoratedKey::new(PartitionKey::new(pk.to_vec())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: (i as u32 + 1).to_be_bytes().to_vec(),
                    cells: vec![(0, CellValue::live(val.to_vec(), 1000 + i as i64))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000 + i as i64),
                }],
            })
            .collect();
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = crate::flush::build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let flush_target = crate::flush::FileFlushTarget::new(sstable_dir).unwrap();
        let _reader = flush_target.flush(output).unwrap();
        let gen = flush_target.generation();
        (dir, format!("{gen}"))
    }

    #[test]
    fn local_backend_phonetic_emits_metaphone_keys() {
        use ferrosa_index::phonetic::metaphone::MetaphoneEncoder;
        use ferrosa_index::phonetic::PhoneticEncoder;
        use ferrosa_index::IndexKey;

        let (dir, gen) = write_text_sstable(&[(b"pk1", b"Smith"), (b"pk2", b"Jones")]);
        let backend = LocalBackend::new(dir.path().to_path_buf());
        let job = IndexBuildJob {
            sstable_id: gen,
            index_name: "name_phon".to_string(),
            index_type: IndexType::Phonetic,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: None,
        };

        let result = backend.build(&job).unwrap();
        assert_eq!(result.index_type, IndexType::Phonetic);

        let entries = result
            .sidecar_entries
            .get("name_phon")
            .expect("phonetic build should produce sidecar entries");

        let enc = MetaphoneEncoder;
        let raw_keys: Vec<IndexKey> = [b"Smith".to_vec(), b"Jones".to_vec()]
            .into_iter()
            .map(IndexKey)
            .collect();
        for (key, _pos) in entries {
            // Keys must be metaphone-encoded, never the raw column value.
            assert!(
                !raw_keys.contains(key),
                "phonetic key {key:?} is a raw value, not metaphone-encoded"
            );
            let text = std::str::from_utf8(&key.0).unwrap();
            assert!(
                text == enc.encode("Smith") || text == enc.encode("Jones"),
                "phonetic key {text:?} is not a known metaphone code"
            );
        }
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn local_backend_vector_produces_no_sidecar() {
        let (dir, gen) = write_text_sstable(&[(b"pk1", b"alice"), (b"pk2", b"bob")]);
        let backend = LocalBackend::new(dir.path().to_path_buf());
        let job = IndexBuildJob {
            sstable_id: gen,
            index_name: "vec_idx".to_string(),
            index_type: IndexType::Vector,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: None,
        };

        let result = backend.build(&job).unwrap();
        assert_eq!(result.index_type, IndexType::Vector);
        assert!(
            result.sidecar_entries.is_empty(),
            "vector columns are flush-built; scheduler must not emit a sidecar, got {:?}",
            result.sidecar_entries.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn local_backend_fulltext_produces_no_sidecar() {
        let (dir, gen) = write_text_sstable(&[(b"pk1", b"hello world")]);
        let backend = LocalBackend::new(dir.path().to_path_buf());
        let job = IndexBuildJob {
            sstable_id: gen,
            index_name: "ft_idx".to_string(),
            index_type: IndexType::FullText,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: None,
        };

        let result = backend.build(&job).unwrap();
        assert_eq!(result.index_type, IndexType::FullText);
        assert!(
            result.sidecar_entries.is_empty(),
            "fulltext is flush-built; scheduler must not emit a sidecar"
        );
    }

    #[test]
    fn local_backend_btree_emits_raw_value_keys() {
        use ferrosa_index::IndexKey;

        let (dir, gen) = write_text_sstable(&[(b"pk1", b"alice"), (b"pk2", b"bob")]);
        let backend = LocalBackend::new(dir.path().to_path_buf());
        let job = IndexBuildJob {
            sstable_id: gen,
            index_name: "name_idx".to_string(),
            index_type: IndexType::BTree,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: None,
        };

        let result = backend.build(&job).unwrap();
        assert_eq!(result.index_type, IndexType::BTree);

        let entries = result.sidecar_entries.get("name_idx").unwrap();
        let keys: Vec<IndexKey> = entries.iter().map(|(k, _)| k.clone()).collect();
        // BTree keeps raw column values, unchanged from before.
        assert!(keys.contains(&IndexKey(b"alice".to_vec())));
        assert!(keys.contains(&IndexKey(b"bob".to_vec())));
        assert_eq!(entries.len(), 2);
    }

    /// Regression guard for the streaming build path: building over many
    /// partitions must emit exactly one index entry per indexed row — proving
    /// the streaming `partitions_iter()` visits every partition with nothing
    /// dropped or truncated, while never materializing the whole SSTable.
    #[test]
    fn local_backend_build_streams_all_rows_across_partitions() {
        let values: Vec<(Vec<u8>, Vec<u8>)> = (0..50u32)
            .map(|i| {
                (
                    format!("pk{i:03}").into_bytes(),
                    format!("val{i:03}").into_bytes(),
                )
            })
            .collect();
        let refs: Vec<(&[u8], &[u8])> = values
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let (dir, gen) = write_text_sstable(&refs);
        let backend = LocalBackend::new(dir.path().to_path_buf());
        let job = IndexBuildJob {
            sstable_id: gen,
            index_name: "name_idx".to_string(),
            index_type: IndexType::BTree,
            table: ("ks".to_string(), "tbl".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            filter_predicate: None,
        };

        let result = backend.build(&job).unwrap();
        let entries = result
            .sidecar_entries
            .get("name_idx")
            .expect("btree build should produce sidecar entries");
        // Exactly one entry per row across all 50 partitions — nothing dropped.
        assert_eq!(
            entries.len(),
            50,
            "streaming build must visit every partition"
        );
        // Boundary partitions (first and last in token order) are indexed.
        let has = |v: &[u8]| entries.iter().any(|(k, _)| k.0 == v);
        assert!(
            has(b"val000") && has(b"val049"),
            "first and last partitions must both produce entries"
        );
    }

    #[test]
    fn scheduler_callback_invoked_for_each_job() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let tracker = Arc::new(IndexStateTracker::new());
        tracker.register_index("ks", "tbl", "idx");
        for id in ["sst-001", "sst-002", "sst-003"] {
            tracker.mark_pending("ks", "tbl", "idx", id, 100);
        }

        let callback_count = Arc::new(AtomicU32::new(0));
        let cb_count = Arc::clone(&callback_count);

        let scheduler = IndexBuildScheduler::with_callback(
            2,
            Arc::clone(&tracker),
            Some(Box::new(move |_: &IndexBuildJob| {
                cb_count.fetch_add(1, Ordering::SeqCst);
            })),
        );

        for id in ["sst-001", "sst-002", "sst-003"] {
            scheduler
                .submit(IndexBuildJob {
                    sstable_id: id.to_string(),
                    index_name: "idx".to_string(),
                    index_type: IndexType::Hash,
                    table: ("ks".to_string(), "tbl".to_string()),
                    priority: BuildPriority::Initial,
                    enqueued_at: Instant::now(),
                    column_position: 0,
                    filter_predicate: None,
                })
                .unwrap();
        }

        std::thread::sleep(Duration::from_millis(500));

        assert_eq!(callback_count.load(Ordering::SeqCst), 3);
        scheduler.shutdown();
    }
}
