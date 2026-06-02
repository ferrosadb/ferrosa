//! Upload manager: async task that uploads SSTables to S3-compatible storage.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parking_lot::Mutex;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

const S3_UPLOAD_PART_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_UPLOAD_WORKERS: usize = 8;
const DEFAULT_DELETE_WORKERS: usize = 2;

/// Local SSTable component file scheduled for upload.
#[derive(Debug, Clone)]
pub struct SstableComponentFile {
    /// Component filename, e.g. `1-Data.db`.
    pub name: String,
    /// Local path to read when the upload worker processes the task.
    pub path: PathBuf,
    /// Component size captured while scanning, used for manifest accounting.
    pub size_bytes: u64,
}

impl SstableComponentFile {
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let size_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        Self {
            name: name.into(),
            path,
            size_bytes,
        }
    }
}

/// SSTable component bytes scheduled for upload without rereading local files.
#[derive(Debug, Clone)]
pub struct SstableComponentBytes {
    /// Bare component name, e.g. `Data.db`.
    pub component: String,
    /// Component bytes captured from the SSTable writer output.
    pub data: Bytes,
}

impl SstableComponentBytes {
    pub fn new(component: impl Into<String>, data: Bytes) -> Self {
        Self {
            component: component.into(),
            data,
        }
    }

    pub fn size_bytes(&self) -> u64 {
        self.data.len() as u64
    }
}

/// A task to upload SSTable component files to object storage.
#[derive(Debug)]
pub enum UploadTask {
    /// Upload SSTable components.
    SSTable {
        /// Table identifier (e.g., "ks.table").
        table_id: String,
        /// SSTable identifier.
        sstable_id: String,
        /// Component files to read from disk when this task is processed.
        files: Vec<SstableComponentFile>,
        /// Notified when all component files have been uploaded successfully.
        ///
        /// `Some(tx)` causes the upload loop to send `Ok(())` after all files
        /// are written to S3, or `Err(message)` if any PUT fails.  The caller
        /// awaits the paired `Receiver` before updating the manifest, closing
        /// the crash window where the manifest could be updated before S3
        /// confirms the upload.
        ///
        /// `None` = fire-and-forget.
        on_complete: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    },
    /// Upload SSTable components already held in memory.
    SSTableBytes {
        /// Table identifier (e.g., "ks.table").
        table_id: String,
        /// SSTable identifier.
        sstable_id: String,
        /// Component bytes to PUT directly to object storage.
        files: Vec<SstableComponentBytes>,
        /// Notified when all component files have been uploaded successfully.
        on_complete: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    },
    /// Delete SSTable component files from object storage after a grace period.
    ///
    /// Sleeps for `grace_period` before issuing DELETE requests, allowing
    /// in-flight reads that hold a reference to the SSTable to complete.
    /// 404 / NotFound responses are treated as success (idempotent).
    DeleteSSTable {
        /// Table identifier (e.g., "ks.table").
        table_id: String,
        /// SSTable identifier to delete.
        sstable_id: String,
        /// How long to wait before issuing DELETE requests.
        grace_period: Duration,
        /// Notified when all component DELETE requests have completed.
        ///
        /// `Some(tx)` sends `Ok(())` on success or `Err(message)` on failure.
        /// `None` = fire-and-forget.
        on_complete: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    },
    /// Upload index files for an SSTable.
    IndexFiles {
        /// Table identifier (e.g., "ks.table").
        table_id: String,
        /// SSTable identifier that the index was built from.
        sstable_id: String,
        /// Index component files: (component_name, data).
        files: Vec<(String, Bytes)>,
    },
    /// Upload a commit log segment for PITR archiving.
    CommitLogSegment {
        /// Segment ID (used for the S3 key).
        segment_id: u64,
        /// Raw segment file bytes.
        data: Bytes,
        /// Pre-computed SHA-256 hex digest (stored in manifest, not validated here).
        sha256: String,
    },
    /// Shutdown signal.
    Shutdown,
}

impl UploadTask {
    fn counts_toward_queue_depth(&self) -> bool {
        !matches!(self, Self::Shutdown)
    }
}

enum DeleteTask {
    SSTable {
        table_id: String,
        sstable_id: String,
        grace_period: Duration,
        on_complete: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    },
}

enum DeleteReadyTask {
    SSTable {
        table_id: String,
        sstable_id: String,
        on_complete: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    },
}

async fn send_delete_to_worker(
    worker_txs: Arc<Vec<mpsc::Sender<DeleteReadyTask>>>,
    cursor: Arc<AtomicUsize>,
    task: DeleteReadyTask,
) {
    if worker_txs.is_empty() {
        if let DeleteReadyTask::SSTable {
            on_complete: Some(tx),
            ..
        } = task
        {
            let _ = tx.send(Err("delete worker pool stopped".into()));
        }
        return;
    }

    let idx = cursor.fetch_add(1, Ordering::Relaxed) % worker_txs.len();
    if let Err(e) = worker_txs[idx].send(task).await {
        let DeleteReadyTask::SSTable {
            on_complete: Some(tx),
            ..
        } = e.0
        else {
            return;
        };
        let _ = tx.send(Err("delete worker stopped".into()));
    }
}

/// Manages async uploads to S3-compatible storage.
///
/// Runs as a spawned tokio task on the caller-provided runtime handle.
/// Backpressure is provided by the bounded channel: when the queue is full,
/// `submit()` blocks until a slot opens.
pub struct UploadManager {
    task_tx: mpsc::Sender<UploadTask>,
    dispatcher_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    delete_scheduler_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    delete_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl UploadManager {
    /// Creates a new upload manager and spawns its background task.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        queue_depth: usize,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        Self::new_with_pools(
            store,
            prefix,
            queue_depth,
            DEFAULT_UPLOAD_WORKERS,
            DEFAULT_DELETE_WORKERS,
            runtime,
        )
    }

    /// Creates a new upload manager with explicit upload and delete pool sizes.
    pub fn new_with_pools(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        queue_depth: usize,
        upload_workers: usize,
        delete_workers: usize,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let upload_workers = upload_workers.max(1);
        let delete_workers = delete_workers.max(1);
        let queue_depth = queue_depth.max(1);
        let (tx, mut rx) = mpsc::channel::<UploadTask>(queue_depth);
        let (delete_tx, mut delete_rx) =
            mpsc::channel::<DeleteTask>(queue_depth.max(delete_workers));

        let mut delete_handles = Vec::with_capacity(delete_workers);
        let mut delete_worker_txs = Vec::with_capacity(delete_workers);
        for worker_id in 0..delete_workers {
            let store = Arc::clone(&store);
            let prefix = prefix.clone();
            let (worker_tx, mut worker_rx) = mpsc::channel::<DeleteReadyTask>(1);
            delete_worker_txs.push(worker_tx);
            delete_handles.push(runtime.spawn(async move {
                while let Some(task) = worker_rx.recv().await {
                    match task {
                        DeleteReadyTask::SSTable {
                            table_id,
                            sstable_id,
                            on_complete,
                        } => {
                            let result = Self::delete_sstable_after_grace(
                                &store,
                                &prefix,
                                &table_id,
                                &sstable_id,
                                Duration::ZERO,
                            )
                            .await;
                            if let Some(tx) = on_complete {
                                let _ = tx.send(result);
                            }
                        }
                    }
                }
                tracing::debug!(worker_id, "upload delete worker stopped");
            }));
        }
        let delete_worker_txs = Arc::new(delete_worker_txs);
        let delete_cursor = Arc::new(AtomicUsize::new(0));
        let delete_scheduler_runtime = runtime.clone();
        let delete_scheduler_handle = {
            let delete_worker_txs = Arc::clone(&delete_worker_txs);
            let delete_cursor = Arc::clone(&delete_cursor);
            runtime.spawn(async move {
                while let Some(task) = delete_rx.recv().await {
                    let DeleteTask::SSTable {
                        table_id,
                        sstable_id,
                        grace_period,
                        on_complete,
                    } = task;
                    let ready = DeleteReadyTask::SSTable {
                        table_id,
                        sstable_id,
                        on_complete,
                    };
                    if grace_period.is_zero() {
                        send_delete_to_worker(
                            Arc::clone(&delete_worker_txs),
                            Arc::clone(&delete_cursor),
                            ready,
                        )
                        .await;
                    } else {
                        let delete_worker_txs = Arc::clone(&delete_worker_txs);
                        let delete_cursor = Arc::clone(&delete_cursor);
                        delete_scheduler_runtime.spawn(async move {
                            tokio::time::sleep(grace_period).await;
                            send_delete_to_worker(delete_worker_txs, delete_cursor, ready).await;
                        });
                    }
                }
                tracing::debug!("upload delete scheduler stopped");
            })
        };

        let mut handles = Vec::with_capacity(upload_workers);
        let mut worker_txs = Vec::with_capacity(upload_workers);
        for worker_id in 0..upload_workers {
            let store = Arc::clone(&store);
            let prefix = prefix.clone();
            let delete_tx = delete_tx.clone();
            let (worker_tx, mut worker_rx) = mpsc::channel::<UploadTask>(1);
            worker_txs.push(worker_tx);
            handles.push(runtime.spawn(async move {
                while let Some(task) = worker_rx.recv().await {
                let count_queue_depth = task.counts_toward_queue_depth();
                let task_start = Instant::now();
                match task {
                    UploadTask::SSTable {
                        table_id,
                        sstable_id,
                        files,
                        on_complete,
                    } => {
                        // Distribute across 256 S3 prefixes for parallelism.
                        // S3 partitions by prefix — using the first 2 hex chars
                        // of a hash of the sstable_id gives even distribution
                        // and avoids the 3,500 PUT/s per-prefix limit.
                        let hex_prefix = hex_prefix_for(&sstable_id);
                        let mut upload_err: Option<String> = None;
                        for file in files {
                            // `name` is already in `{sstable_id}-{component}` form
                            // (e.g. "1-Data.db"). Strip the id prefix to get the bare
                            // component name so we can route through the shared key
                            // constructor and guarantee upload/download alignment.
                            let component = file
                                .name
                                .strip_prefix(&format!("{sstable_id}-"))
                                .unwrap_or(&file.name);
                            let path = sstable_object_key(
                                &prefix,
                                &hex_prefix,
                                &table_id,
                                &sstable_id,
                                component,
                            );
                            match Self::put_file_with_retry(&store, &path, &file.path, 5).await {
                                Ok(()) => {}
                                Err(UploadFileError::Missing) => {
                                    // The SSTable was compacted away (or
                                    // explicitly deleted) between the scan that
                                    // produced `files` and this read. Not a real
                                    // upload error — the compacted output is the
                                    // authoritative copy and will be uploaded
                                    // separately. Mark the task as "compacted
                                    // away" so the caller doesn't add it to the
                                    // manifest, but don't surface a tracing
                                    // ERROR for a benign race.
                                    let msg = format!(
                                        "skipped: source compacted away before upload \
                                         ({path}: {})",
                                        file.path.display()
                                    );
                                    tracing::info!(
                                        path = %file.path.display(),
                                        "s3 upload skipped — SSTable file compacted away before upload"
                                    );
                                    if upload_err.is_none() {
                                        upload_err = Some(msg);
                                    }
                                }
                                Err(UploadFileError::Read(e)) => {
                                    let msg = format!(
                                        "upload failed for {path}: failed to read {}: {e}",
                                        file.path.display()
                                    );
                                    tracing_or_eprintln(msg.clone());
                                    if upload_err.is_none() {
                                        upload_err = Some(msg);
                                    }
                                }
                                Err(UploadFileError::Store(e)) => {
                                    let msg = format!("upload failed for {path}: {e}");
                                    tracing_or_eprintln(msg.clone());
                                    // Record first error; continue so all files are attempted.
                                    if upload_err.is_none() {
                                        upload_err = Some(msg);
                                    }
                                }
                            }
                        }
                        // Notify caller of upload outcome when a completion channel
                        // was provided.  Receiver drop is silently ignored — the
                        // caller may have timed out or crashed.
                        if let Some(tx) = on_complete {
                            let result = match upload_err {
                                None => Ok(()),
                                Some(msg) => Err(msg),
                            };
                            let _ = tx.send(result);
                        }
                    }
                    UploadTask::SSTableBytes {
                        table_id,
                        sstable_id,
                        files,
                        on_complete,
                    } => {
                        let hex_prefix = hex_prefix_for(&sstable_id);
                        let mut upload_err: Option<String> = None;
                        for file in files {
                            let path = sstable_object_key(
                                &prefix,
                                &hex_prefix,
                                &table_id,
                                &sstable_id,
                                &file.component,
                            );
                            if let Err(e) =
                                Self::put_component_bytes_with_retry(&store, &path, file.data, 5)
                                    .await
                            {
                                let msg = format!("upload failed for {path}: {e}");
                                tracing_or_eprintln(msg.clone());
                                if upload_err.is_none() {
                                    upload_err = Some(msg);
                                }
                            }
                        }
                        if let Some(tx) = on_complete {
                            let result = match upload_err {
                                None => Ok(()),
                                Some(msg) => Err(msg),
                            };
                            let _ = tx.send(result);
                        }
                    }
                    UploadTask::DeleteSSTable {
                        table_id,
                        sstable_id,
                        grace_period,
                        on_complete,
                    } => {
                        let task = DeleteTask::SSTable {
                            table_id,
                            sstable_id,
                            grace_period,
                            on_complete,
                        };
                        if let Err(e) = delete_tx.send(task).await {
                            let DeleteTask::SSTable { on_complete, .. } = e.0;
                            if let Some(tx) = on_complete {
                                let _ = tx.send(Err("delete channel closed".into()));
                            }
                            tracing_or_eprintln("delete channel closed".to_string());
                        }
                    }
                    UploadTask::IndexFiles {
                        table_id,
                        sstable_id,
                        files,
                    } => {
                        // Use the same S3 prefix distribution as SSTables.
                        let hex_prefix = hex_prefix_for(&sstable_id);
                        for (name, data) in files {
                            let path = ObjectPath::from(format!(
                                "{prefix}/{hex_prefix}/{table_id}/{sstable_id}/{name}"
                            ));
                            if let Err(e) =
                                Self::put_with_retry(&store, &path, data.clone(), 5).await
                            {
                                tracing_or_eprintln(format!("index upload failed for {path}: {e}"));
                            }
                        }
                    }
                    UploadTask::CommitLogSegment {
                        segment_id,
                        data,
                        sha256: _,
                    } => {
                        let hex = hex_prefix_for(&segment_id.to_string());
                        let path = ObjectPath::from(format!(
                            "{prefix}/commitlog-archive/{hex}/{segment_id}.log"
                        ));
                        if let Err(e) = Self::put_with_retry(&store, &path, data.clone(), 5).await {
                            tracing_or_eprintln(format!(
                                "commitlog segment upload failed for {path}: {e}"
                            ));
                        }
                    }
                    UploadTask::Shutdown => break,
                }
                if count_queue_depth {
                    crate::metrics::observe_upload_task(task_start.elapsed());
                    crate::metrics::dec_upload_queue_depth();
                }
                }
                tracing::debug!(worker_id, "upload worker stopped");
            }));
        }
        let dispatcher_handle = runtime.spawn(async move {
            let mut next_worker = 0usize;
            while let Some(task) = rx.recv().await {
                if matches!(task, UploadTask::Shutdown) {
                    for worker_tx in &worker_txs {
                        let _ = worker_tx.send(UploadTask::Shutdown).await;
                    }
                    break;
                }

                if worker_txs.is_empty() {
                    break;
                }

                let idx = next_worker % worker_txs.len();
                next_worker = next_worker.wrapping_add(1);
                if worker_txs[idx].send(task).await.is_err() {
                    break;
                }
            }
            tracing::debug!("upload dispatcher stopped");
        });

        Self {
            task_tx: tx,
            dispatcher_handle: Mutex::new(Some(dispatcher_handle)),
            handles: Mutex::new(handles),
            delete_scheduler_handle: Mutex::new(Some(delete_scheduler_handle)),
            delete_handles: Mutex::new(delete_handles),
        }
    }

    /// Submits an upload task. Blocks if the queue is full (backpressure).
    pub async fn submit(&self, task: UploadTask) -> ferrosa_common::Result<()> {
        let count_queue_depth = task.counts_toward_queue_depth();
        if count_queue_depth {
            crate::metrics::inc_upload_queue_depth();
        }
        let start = Instant::now();
        match self.task_tx.send(task).await {
            Ok(()) => {
                crate::metrics::observe_upload_phase(
                    crate::metrics::UploadPhase::SubmitWait,
                    start.elapsed(),
                );
                Ok(())
            }
            Err(_) => {
                if count_queue_depth {
                    crate::metrics::dec_upload_queue_depth();
                }
                Err(ferrosa_common::Error::InvalidFormat(
                    "upload channel closed".into(),
                ))
            }
        }
    }

    /// Attempts to submit an upload task without waiting for queue capacity.
    ///
    /// Startup crash-recovery paths use this to avoid blocking listener binding
    /// behind slow object-store uploads. Failed submissions leave their
    /// pending-log entries intact for a later retry.
    pub fn try_submit(&self, task: UploadTask) -> ferrosa_common::Result<()> {
        let count_queue_depth = task.counts_toward_queue_depth();
        if count_queue_depth {
            crate::metrics::inc_upload_queue_depth();
        }
        let start = Instant::now();
        match self.task_tx.try_send(task) {
            Ok(()) => {
                crate::metrics::observe_upload_phase(
                    crate::metrics::UploadPhase::SubmitWait,
                    start.elapsed(),
                );
                Ok(())
            }
            Err(e) => {
                if count_queue_depth {
                    crate::metrics::dec_upload_queue_depth();
                }
                match e {
                    mpsc::error::TrySendError::Full(_) => Err(
                        ferrosa_common::Error::InvalidFormat("upload queue full".into()),
                    ),
                    mpsc::error::TrySendError::Closed(_) => Err(
                        ferrosa_common::Error::InvalidFormat("upload channel closed".into()),
                    ),
                }
            }
        }
    }

    /// Shuts down the upload manager, draining the queue.
    pub async fn shutdown(&self) {
        let _ = self.task_tx.send(UploadTask::Shutdown).await;
        if let Some(handle) = self.dispatcher_handle.lock().take() {
            let _ = handle.await;
        }
        let handles = self.handles.lock().drain(..).collect::<Vec<_>>();
        for handle in handles {
            let _ = handle.await;
        }
        if let Some(handle) = self.delete_scheduler_handle.lock().take() {
            handle.abort();
            let _ = handle.await;
        }
        let delete_handles = self.delete_handles.lock().drain(..).collect::<Vec<_>>();
        for handle in delete_handles {
            handle.abort();
            let _ = handle.await;
        }
    }

    async fn delete_sstable_after_grace(
        store: &dyn ObjectStore,
        prefix: &str,
        table_id: &str,
        sstable_id: &str,
        grace_period: Duration,
    ) -> Result<(), String> {
        tokio::time::sleep(grace_period).await;

        let hex_prefix = hex_prefix_for(sstable_id);
        let components = [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Index.db",
            "Filter.db",
            "Statistics.db",
            "CompressionInfo.db",
            "TOC.txt",
        ];
        let mut delete_err: Option<String> = None;
        for component in &components {
            let path = sstable_object_key(prefix, &hex_prefix, table_id, sstable_id, component);
            match store.delete(&path).await {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => {
                    let msg = format!("delete failed for {path}: {e}");
                    tracing_or_eprintln(msg.clone());
                    if delete_err.is_none() {
                        delete_err = Some(msg);
                    }
                }
            }
        }

        match delete_err {
            None => Ok(()),
            Some(msg) => Err(msg),
        }
    }

    /// Puts data to the object store with exponential backoff retry.
    async fn put_with_retry(
        store: &dyn ObjectStore,
        path: &ObjectPath,
        data: Bytes,
        max_retries: u32,
    ) -> Result<(), object_store::Error> {
        let mut delay = Duration::from_millis(100);

        for attempt in 0..=max_retries {
            match store.put(path, data.clone().into()).await {
                Ok(_) => return Ok(()),
                Err(e) if attempt < max_retries && is_transient(&e) => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => return Err(e),
            }
        }

        unreachable!()
    }

    async fn put_component_bytes_with_retry(
        store: &dyn ObjectStore,
        path: &ObjectPath,
        data: Bytes,
        max_retries: u32,
    ) -> Result<(), object_store::Error> {
        let size_bytes = data.len() as u64;
        let upload_start = Instant::now();
        let result = Self::put_with_retry(store, path, data, max_retries).await;
        crate::metrics::observe_upload_file(size_bytes, upload_start.elapsed());
        result
    }

    /// Streams a local file into object storage without eagerly materializing
    /// large SSTable components.
    ///
    /// Large components use multipart upload. Small components intentionally use
    /// a normal PUT because S3 rejects multipart uploads whose only part is
    /// smaller than the 5 MiB minimum (`EntityTooSmall`).
    async fn put_file_with_retry(
        store: &dyn ObjectStore,
        path: &ObjectPath,
        file_path: &std::path::Path,
        max_retries: u32,
    ) -> Result<(), UploadFileError> {
        let mut delay = Duration::from_millis(100);

        for attempt in 0..=max_retries {
            match Self::put_file_once(store, path, file_path).await {
                Ok(()) => return Ok(()),
                Err(UploadFileError::Store(e)) if attempt < max_retries && is_transient(&e) => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => return Err(e),
            }
        }

        unreachable!()
    }

    async fn put_file_once(
        store: &dyn ObjectStore,
        path: &ObjectPath,
        file_path: &std::path::Path,
    ) -> Result<(), UploadFileError> {
        let metadata = tokio::fs::metadata(file_path)
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => UploadFileError::Missing,
                _ => UploadFileError::Read(e),
            })?;
        let mut file = tokio::fs::File::open(file_path)
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => UploadFileError::Missing,
                _ => UploadFileError::Read(e),
            })?;

        let upload_start = Instant::now();
        let result = if metadata.len() < S3_UPLOAD_PART_BYTES as u64 {
            let mut data = Vec::with_capacity(metadata.len() as usize);
            file.read_to_end(&mut data)
                .await
                .map_err(UploadFileError::Read)?;
            store
                .put(path, Bytes::from(data).into())
                .await
                .map(|_| ())
                .map_err(UploadFileError::Store)
        } else {
            Self::put_reader_multipart_once(store, path, &mut file).await
        };

        crate::metrics::observe_upload_file(metadata.len(), upload_start.elapsed());
        result
    }

    async fn put_reader_multipart_once<R>(
        store: &dyn ObjectStore,
        path: &ObjectPath,
        reader: &mut R,
    ) -> Result<(), UploadFileError>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let mut upload = store
            .put_multipart(path)
            .await
            .map_err(UploadFileError::Store)?;
        let mut buf = vec![0u8; S3_UPLOAD_PART_BYTES];

        loop {
            let mut filled = 0;
            while filled < buf.len() {
                let bytes_read = reader
                    .read(&mut buf[filled..])
                    .await
                    .map_err(UploadFileError::Read)?;
                if bytes_read == 0 {
                    break;
                }
                filled += bytes_read;
            }

            if filled == 0 {
                break;
            }

            if let Err(e) = upload
                .put_part(Bytes::copy_from_slice(&buf[..filled]).into())
                .await
            {
                let _ = upload.abort().await;
                return Err(UploadFileError::Store(e));
            }

            if filled < buf.len() {
                break;
            }
        }

        upload
            .complete()
            .await
            .map(|_| ())
            .map_err(UploadFileError::Store)
    }
}

#[derive(Debug)]
enum UploadFileError {
    Missing,
    Read(std::io::Error),
    Store(object_store::Error),
}

impl std::fmt::Display for UploadFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => write!(f, "source file disappeared before upload"),
            Self::Read(e) => write!(f, "{e}"),
            Self::Store(e) => write!(f, "{e}"),
        }
    }
}

/// Compute a 2-character hex prefix from an SSTable ID for S3 key distribution.
///
/// Uses a simple hash (FNV-1a inspired) of the ID string to get even
/// distribution across 256 buckets. This avoids S3's per-prefix
/// throughput limits (3,500 PUT/s, 5,500 GET/s per partition).
pub fn hex_prefix_for(sstable_id: &str) -> String {
    let mut hash: u8 = 0;
    for byte in sstable_id.as_bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(*byte);
    }
    format!("{hash:02x}")
}

/// Build the canonical S3 object key for a single SSTable component file.
///
/// This is the **single source of truth** for the path format used by both
/// upload and download.  The key format is:
///
/// ```text
/// {prefix}/{hex}/{table_id}/{sstable_id}/{sstable_id}-{component}
/// ```
///
/// The `{sstable_id}-` prefix on the filename mirrors how the component files
/// are stored locally (e.g. `1-Data.db`), ensuring upload and download
/// paths are always identical.
pub fn sstable_object_key(
    prefix: &str,
    hex: &str,
    table_id: &str,
    sstable_id: &str,
    component: &str,
) -> ObjectPath {
    ObjectPath::from(format!(
        "{prefix}/{hex}/{table_id}/{sstable_id}/{sstable_id}-{component}"
    ))
}

/// Returns true for transient errors that should be retried.
fn is_transient(err: &object_store::Error) -> bool {
    matches!(
        err,
        object_store::Error::Generic { .. } | object_store::Error::Precondition { .. }
    )
}

/// Log helper for upload manager messages.
fn tracing_or_eprintln(msg: String) {
    tracing::info!("{}", msg);
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;
    use tokio::sync::Notify;

    fn make_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn hex_prefix_is_two_chars() {
        for id in ["1", "42", "abc123", "gen99-seq7", ""] {
            let prefix = hex_prefix_for(id);
            assert_eq!(prefix.len(), 2, "prefix for '{id}' should be 2 chars");
            assert!(
                prefix.chars().all(|c| c.is_ascii_hexdigit()),
                "prefix for '{id}' should be hex"
            );
        }
    }

    #[test]
    fn hex_prefix_distributes_across_buckets() {
        use std::collections::HashSet;
        let prefixes: HashSet<String> = (0..1000)
            .map(|i| hex_prefix_for(&format!("gen{i}")))
            .collect();
        // 1000 unique IDs should produce at least 50 distinct prefixes
        assert!(
            prefixes.len() >= 50,
            "expected >=50 distinct prefixes, got {}",
            prefixes.len()
        );
    }

    #[test]
    fn upload_round_trip() {
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let component_path = dir.path().join("abc123-Data.db");
            std::fs::write(&component_path, b"hello sstable data").unwrap();
            let store = Arc::new(InMemory::new());
            let manager = UploadManager::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "test".into(),
                16,
                &tokio::runtime::Handle::current(),
            );

            manager
                .submit(UploadTask::SSTable {
                    table_id: "ks.table".into(),
                    sstable_id: "abc123".into(),
                    files: vec![SstableComponentFile::new("abc123-Data.db", component_path)],
                    on_complete: None,
                })
                .await
                .unwrap();

            manager.shutdown().await;

            // Verify the file is in the store at the canonical path.
            // The upload manager strips the sstable_id prefix from the filename
            // and routes through sstable_object_key, so the S3 key is:
            //   {prefix}/{hex}/{table_id}/{sstable_id}/{sstable_id}-{component}
            let hex = hex_prefix_for("abc123");
            let path = sstable_object_key("test", &hex, "ks.table", "abc123", "Data.db");
            let result = store.get(&path).await.unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes.as_ref(), b"hello sstable data");
        });
    }

    #[test]
    fn upload_sstable_bytes_round_trip() {
        let rt = make_runtime();
        rt.block_on(async {
            let store = Arc::new(InMemory::new());
            let manager = UploadManager::new_with_pools(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "direct".into(),
                16,
                2,
                1,
                &tokio::runtime::Handle::current(),
            );
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

            manager
                .submit(UploadTask::SSTableBytes {
                    table_id: "ks.t".into(),
                    sstable_id: "42".into(),
                    files: vec![
                        SstableComponentBytes::new("Data.db", Bytes::from_static(b"direct data")),
                        SstableComponentBytes::new("TOC.txt", Bytes::from_static(b"Data.db\n")),
                    ],
                    on_complete: Some(tx),
                })
                .await
                .unwrap();

            rx.await.unwrap().unwrap();
            manager.shutdown().await;

            let hex = hex_prefix_for("42");
            let path = sstable_object_key("direct", &hex, "ks.t", "42", "Data.db");
            let result = store.get(&path).await.unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes.as_ref(), b"direct data");
        });
    }

    #[test]
    fn multiple_components_uploaded() {
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let components = [
                ("001-Data.db", b"data".as_slice()),
                ("001-Index.db", b"index".as_slice()),
                ("001-Filter.db", b"filter".as_slice()),
            ];
            for (name, data) in components {
                std::fs::write(dir.path().join(name), data).unwrap();
            }
            let store = Arc::new(InMemory::new());
            let manager = UploadManager::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "pfx".into(),
                16,
                &tokio::runtime::Handle::current(),
            );

            manager
                .submit(UploadTask::SSTable {
                    table_id: "ks.t".into(),
                    sstable_id: "001".into(),
                    files: vec![
                        SstableComponentFile::new("001-Data.db", dir.path().join("001-Data.db")),
                        SstableComponentFile::new("001-Index.db", dir.path().join("001-Index.db")),
                        SstableComponentFile::new(
                            "001-Filter.db",
                            dir.path().join("001-Filter.db"),
                        ),
                    ],
                    on_complete: None,
                })
                .await
                .unwrap();

            manager.shutdown().await;

            let hex = hex_prefix_for("001");
            // Files submitted as "Data.db" etc. (without sstable_id prefix) are
            // stored via sstable_object_key, producing {id}-{component} in S3.
            for component in ["Data.db", "Index.db", "Filter.db"] {
                let path = sstable_object_key("pfx", &hex, "ks.t", "001", component);
                let result = store.get(&path).await.unwrap();
                assert!(!result.bytes().await.unwrap().is_empty());
            }
        });
    }

    #[test]
    fn sstable_upload_streams_file_through_multipart_not_single_memory_payload() {
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let component_path = dir.path().join("stream-Data.db");
            let data = vec![0x5a; 9 * 1024 * 1024 + 17];
            std::fs::write(&component_path, &data).unwrap();

            let inner = Arc::new(InMemory::new());
            let store = Arc::new(RejectSinglePutStore::new(
                Arc::clone(&inner) as Arc<dyn ObjectStore>
            ));
            let multipart_started = Arc::clone(&store.multipart_started);
            let manager = UploadManager::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "pfx".into(),
                16,
                &tokio::runtime::Handle::current(),
            );
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

            manager
                .submit(UploadTask::SSTable {
                    table_id: "ks.t".into(),
                    sstable_id: "stream".into(),
                    files: vec![SstableComponentFile::new("stream-Data.db", &component_path)],
                    on_complete: Some(tx),
                })
                .await
                .unwrap();

            rx.await.unwrap().unwrap();
            manager.shutdown().await;

            assert_eq!(
                multipart_started.load(Ordering::SeqCst),
                1,
                "SSTable uploads must use multipart streaming from the local component file, not ObjectStore::put with one full-file Bytes payload"
            );
            let hex = hex_prefix_for("stream");
            let path = sstable_object_key("pfx", &hex, "ks.t", "stream", "Data.db");
            let result = inner.get(&path).await.unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes.len(), data.len());
            assert_eq!(&bytes[..32], &data[..32]);
        });
    }

    #[test]
    fn small_sstable_upload_uses_single_put_to_avoid_s3_entity_too_small() {
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let component_path = dir.path().join("small-Data.db");
            let data = b"tiny sstable component";
            std::fs::write(&component_path, data).unwrap();

            let inner = Arc::new(InMemory::new());
            let store = Arc::new(RejectSinglePutStore::allow_single_put(
                Arc::clone(&inner) as Arc<dyn ObjectStore>
            ));
            let multipart_started = Arc::clone(&store.multipart_started);
            let single_put_started = Arc::clone(&store.single_put_started);
            let manager = UploadManager::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "pfx".into(),
                16,
                &tokio::runtime::Handle::current(),
            );
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

            manager
                .submit(UploadTask::SSTable {
                    table_id: "ks.t".into(),
                    sstable_id: "small".into(),
                    files: vec![SstableComponentFile::new("small-Data.db", &component_path)],
                    on_complete: Some(tx),
                })
                .await
                .unwrap();

            rx.await.unwrap().unwrap();
            manager.shutdown().await;

            assert_eq!(
                multipart_started.load(Ordering::SeqCst),
                0,
                "S3 rejects multipart uploads for objects below its minimum part size"
            );
            assert_eq!(single_put_started.load(Ordering::SeqCst), 1);
            let hex = hex_prefix_for("small");
            let path = sstable_object_key("pfx", &hex, "ks.t", "small", "Data.db");
            let result = inner.get(&path).await.unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes.as_ref(), data);
        });
    }

    #[test]
    fn medium_sstable_upload_uses_single_put_to_avoid_one_part_multipart_entity_too_small() {
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let component_path = dir.path().join("medium-Data.db");
            let data = vec![0x6d; 6 * 1024 * 1024 + 17];
            std::fs::write(&component_path, &data).unwrap();

            let inner = Arc::new(InMemory::new());
            let store = Arc::new(RejectSinglePutStore::allow_single_put(
                Arc::clone(&inner) as Arc<dyn ObjectStore>
            ));
            let multipart_started = Arc::clone(&store.multipart_started);
            let single_put_started = Arc::clone(&store.single_put_started);
            let manager = UploadManager::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "pfx".into(),
                16,
                &tokio::runtime::Handle::current(),
            );
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

            manager
                .submit(UploadTask::SSTable {
                    table_id: "ks.t".into(),
                    sstable_id: "medium".into(),
                    files: vec![SstableComponentFile::new("medium-Data.db", &component_path)],
                    on_complete: Some(tx),
                })
                .await
                .unwrap();

            rx.await.unwrap().unwrap();
            manager.shutdown().await;

            assert_eq!(
                multipart_started.load(Ordering::SeqCst),
                0,
                "files smaller than the upload chunk size must not use one-part multipart upload"
            );
            assert_eq!(single_put_started.load(Ordering::SeqCst), 1);
            let hex = hex_prefix_for("medium");
            let path = sstable_object_key("pfx", &hex, "ks.t", "medium", "Data.db");
            let result = inner.get(&path).await.unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes.len(), data.len());
            assert_eq!(&bytes[..32], &data[..32]);
        });
    }

    #[test]
    fn multipart_upload_coalesces_short_file_reads_into_full_sized_non_final_parts() {
        let rt = make_runtime();
        rt.block_on(async {
            let part_sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
            let store = PartSizeRecordingStore::new(Arc::clone(&part_sizes));
            let path = ObjectPath::from("pfx/large-Data.db");
            let total_len = S3_UPLOAD_PART_BYTES + 21_527;
            let mut reader = ShortRead::new(total_len, 128 * 1024);

            UploadManager::put_reader_multipart_once(&store, &path, &mut reader)
                .await
                .unwrap();

            let sizes = part_sizes.lock().unwrap().clone();
            assert_eq!(
                sizes,
                vec![S3_UPLOAD_PART_BYTES, 21_527],
                "short file reads must be coalesced so only the final multipart part is below the chunk size"
            );
        });
    }

    #[test]
    fn queued_sstable_upload_reads_component_from_disk_when_processed() {
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let first_path = dir.path().join("1-Data.db");
            let second_path = dir.path().join("2-Data.db");
            std::fs::write(&first_path, b"first").unwrap();
            std::fs::write(&second_path, b"original").unwrap();

            let inner = Arc::new(InMemory::new());
            let store = Arc::new(BlockingFirstPutStore::new(
                Arc::clone(&inner) as Arc<dyn ObjectStore>
            ));
            let first_put_started = Arc::clone(&store.first_put_started);
            let release_first_put = Arc::clone(&store.release_first_put);
            let manager = UploadManager::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "pfx".into(),
                16,
                &tokio::runtime::Handle::current(),
            );

            manager
                .submit(UploadTask::SSTable {
                    table_id: "ks.t".into(),
                    sstable_id: "1".into(),
                    files: vec![SstableComponentFile::new("1-Data.db", first_path)],
                    on_complete: None,
                })
                .await
                .unwrap();

            first_put_started.notified().await;
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
            manager
                .submit(UploadTask::SSTable {
                    table_id: "ks.t".into(),
                    sstable_id: "2".into(),
                    files: vec![SstableComponentFile::new("2-Data.db", second_path.clone())],
                    on_complete: Some(tx),
                })
                .await
                .unwrap();

            std::fs::write(&second_path, b"updated").unwrap();
            release_first_put.notify_one();
            rx.await.unwrap().unwrap();
            manager.shutdown().await;

            let hex = hex_prefix_for("2");
            let path = sstable_object_key("pfx", &hex, "ks.t", "2", "Data.db");
            let result = inner.get(&path).await.unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(
                bytes.as_ref(),
                b"updated",
                "queued SSTable tasks must not retain pre-read component bytes"
            );
        });
    }

    #[test]
    fn missing_sstable_component_reports_compacted_away_skip() {
        // A source SSTable file disappearing between scan and read is the
        // compaction-vs-S3-sync race the upload manager treats as benign:
        // the compacted output of the merge will be uploaded under its own
        // generation, and the manifest should NOT pick up a partial copy of
        // the obsolete generation. The completion channel surfaces the
        // "skipped: source compacted away" sentinel so the caller can route
        // the outcome to INFO instead of ERROR.
        let rt = make_runtime();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let missing_path = dir.path().join("3-Data.db");
            std::fs::write(&missing_path, b"will be removed").unwrap();
            let file = SstableComponentFile::new("3-Data.db", &missing_path);
            std::fs::remove_file(&missing_path).unwrap();

            let store = Arc::new(InMemory::new());
            let manager = UploadManager::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "pfx".into(),
                16,
                &tokio::runtime::Handle::current(),
            );
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

            manager
                .submit(UploadTask::SSTable {
                    table_id: "ks.t".into(),
                    sstable_id: "3".into(),
                    files: vec![file],
                    on_complete: Some(tx),
                })
                .await
                .unwrap();

            let err = rx.await.unwrap().unwrap_err();
            assert!(
                err.starts_with("skipped: source compacted away"),
                "missing component should surface the compacted-away skip sentinel, got: {err}"
            );
            manager.shutdown().await;

            let hex = hex_prefix_for("3");
            let path = sstable_object_key("pfx", &hex, "ks.t", "3", "Data.db");
            assert!(
                store.get(&path).await.is_err(),
                "missing component must not create an object"
            );
        });
    }

    #[test]
    fn delete_sstable_removes_ferrosa_components() {
        let rt = make_runtime();
        rt.block_on(async {
            let store = Arc::new(InMemory::new());
            let manager = UploadManager::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "pfx".into(),
                16,
                &tokio::runtime::Handle::current(),
            );
            let table_id = "ks.t";
            let sstable_id = "7";
            let hex = hex_prefix_for(sstable_id);
            let components = [
                "Data.db",
                "Partitions.db",
                "Rows.db",
                "Filter.db",
                "Statistics.db",
                "TOC.txt",
                "CompressionInfo.db",
            ];

            for component in components {
                let path = sstable_object_key("pfx", &hex, table_id, sstable_id, component);
                store
                    .put(&path, Bytes::from_static(b"component").into())
                    .await
                    .unwrap();
            }

            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
            manager
                .submit(UploadTask::DeleteSSTable {
                    table_id: table_id.into(),
                    sstable_id: sstable_id.into(),
                    grace_period: Duration::from_millis(0),
                    on_complete: Some(tx),
                })
                .await
                .unwrap();

            rx.await.unwrap().unwrap();
            manager.shutdown().await;

            for component in components {
                let path = sstable_object_key("pfx", &hex, table_id, sstable_id, component);
                assert!(
                    store.get(&path).await.is_err(),
                    "{component} should be deleted by DeleteSSTable"
                );
            }
        });
    }

    #[test]
    fn deferred_delete_grace_does_not_exhaust_delete_workers() {
        let rt = make_runtime();
        rt.block_on(async {
            let store = Arc::new(InMemory::new());
            let manager = UploadManager::new_with_pools(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "pfx".into(),
                2,
                1,
                1,
                &tokio::runtime::Handle::current(),
            );
            let mut receivers = Vec::new();

            for i in 0..8 {
                let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
                manager
                    .submit(UploadTask::DeleteSSTable {
                        table_id: "ks.t".into(),
                        sstable_id: format!("stale-{i}"),
                        grace_period: Duration::from_secs(3600),
                        on_complete: Some(tx),
                    })
                    .await
                    .unwrap();
                receivers.push(rx);
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
            for rx in &mut receivers {
                assert!(
                    matches!(
                        rx.try_recv(),
                        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                    ),
                    "long-grace deletes should stay scheduled, not complete early with queue-full"
                );
            }

            manager.shutdown().await;
        });
    }

    #[test]
    fn upload_commit_log_segment() {
        let rt = make_runtime();
        rt.block_on(async {
            let store = Arc::new(InMemory::new());
            let manager = UploadManager::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "node1".into(),
                16,
                &tokio::runtime::Handle::current(),
            );

            let segment_data = Bytes::from_static(b"commit-log-segment-bytes");
            manager
                .submit(UploadTask::CommitLogSegment {
                    segment_id: 42,
                    data: segment_data.clone(),
                    sha256: "abc123def456".into(), // pragma: allowlist secret
                })
                .await
                .unwrap();

            manager.shutdown().await;

            // Verify the segment is in the store at the correct hex-prefixed path.
            let hex = hex_prefix_for("42");
            let path = ObjectPath::from(format!("node1/commitlog-archive/{hex}/42.log"));
            let result = store.get(&path).await.unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes.as_ref(), b"commit-log-segment-bytes");
        });
    }

    struct ShortRead {
        remaining: usize,
        chunk: usize,
    }

    impl ShortRead {
        fn new(total: usize, chunk: usize) -> Self {
            Self {
                remaining: total,
                chunk,
            }
        }
    }

    impl tokio::io::AsyncRead for ShortRead {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let n = self.remaining.min(self.chunk).min(buf.remaining());
            if n > 0 {
                buf.put_slice(&vec![0x42; n]);
                self.remaining -= n;
            }
            Poll::Ready(Ok(()))
        }
    }

    struct PartSizeRecordingStore {
        inner: Arc<dyn ObjectStore>,
        part_sizes: Arc<std::sync::Mutex<Vec<usize>>>,
    }

    impl PartSizeRecordingStore {
        fn new(part_sizes: Arc<std::sync::Mutex<Vec<usize>>>) -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                part_sizes,
            }
        }
    }

    impl std::fmt::Display for PartSizeRecordingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PartSizeRecordingStore")
        }
    }

    impl std::fmt::Debug for PartSizeRecordingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PartSizeRecordingStore")
        }
    }

    struct PartSizeRecordingUpload {
        part_sizes: Arc<std::sync::Mutex<Vec<usize>>>,
    }

    impl std::fmt::Debug for PartSizeRecordingUpload {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PartSizeRecordingUpload")
        }
    }

    #[async_trait::async_trait]
    impl object_store::MultipartUpload for PartSizeRecordingUpload {
        fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
            let part_sizes = Arc::clone(&self.part_sizes);
            Box::pin(async move {
                part_sizes.lock().unwrap().push(data.content_length());
                Ok(())
            })
        }

        async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
            Ok(object_store::PutResult {
                e_tag: None,
                version: None,
            })
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for PartSizeRecordingStore {
        async fn put_opts(
            &self,
            _location: &object_store::path::Path,
            _payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn put_multipart_opts(
            &self,
            _location: &object_store::path::Path,
            _opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            Ok(Box::new(PartSizeRecordingUpload {
                part_sizes: Arc::clone(&self.part_sizes),
            }))
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    struct RejectSinglePutStore {
        inner: Arc<dyn ObjectStore>,
        multipart_started: Arc<AtomicUsize>,
        single_put_started: Arc<AtomicUsize>,
        reject_single_put: bool,
    }

    impl RejectSinglePutStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                multipart_started: Arc::new(AtomicUsize::new(0)),
                single_put_started: Arc::new(AtomicUsize::new(0)),
                reject_single_put: true,
            }
        }

        fn allow_single_put(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                multipart_started: Arc::new(AtomicUsize::new(0)),
                single_put_started: Arc::new(AtomicUsize::new(0)),
                reject_single_put: false,
            }
        }
    }

    impl std::fmt::Display for RejectSinglePutStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RejectSinglePutStore")
        }
    }

    impl std::fmt::Debug for RejectSinglePutStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RejectSinglePutStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RejectSinglePutStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.single_put_started.fetch_add(1, Ordering::SeqCst);
            if self.reject_single_put {
                Err(object_store::Error::NotImplemented)
            } else {
                self.inner.put_opts(location, payload, opts).await
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.multipart_started.fetch_add(1, Ordering::SeqCst);
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    struct BlockingFirstPutStore {
        inner: Arc<dyn ObjectStore>,
        put_count: AtomicUsize,
        first_put_started: Arc<Notify>,
        release_first_put: Arc<Notify>,
    }

    impl BlockingFirstPutStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                put_count: AtomicUsize::new(0),
                first_put_started: Arc::new(Notify::new()),
                release_first_put: Arc::new(Notify::new()),
            }
        }
    }

    impl std::fmt::Display for BlockingFirstPutStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "BlockingFirstPutStore")
        }
    }

    impl std::fmt::Debug for BlockingFirstPutStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "BlockingFirstPutStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for BlockingFirstPutStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if self.put_count.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_put_started.notify_one();
                self.release_first_put.notified().await;
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }
}
