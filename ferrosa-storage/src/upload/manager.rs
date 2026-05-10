//! Upload manager: async task that uploads SSTables to S3-compatible storage.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parking_lot::Mutex;
use tokio::sync::mpsc;

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

/// Manages async uploads to S3-compatible storage.
///
/// Runs as a spawned tokio task on the caller-provided runtime handle.
/// Backpressure is provided by the bounded channel: when the queue is full,
/// `submit()` blocks until a slot opens.
pub struct UploadManager {
    task_tx: mpsc::Sender<UploadTask>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl UploadManager {
    /// Creates a new upload manager and spawns its background task.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        queue_depth: usize,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<UploadTask>(queue_depth);

        let handle = runtime.spawn(async move {
            while let Some(task) = rx.recv().await {
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
                            let data = match std::fs::read(&file.path) {
                                Ok(data) => Bytes::from(data),
                                Err(e) => {
                                    let msg = format!(
                                        "upload failed for {path}: failed to read {}: {e}",
                                        file.path.display()
                                    );
                                    tracing_or_eprintln(msg.clone());
                                    if upload_err.is_none() {
                                        upload_err = Some(msg);
                                    }
                                    continue;
                                }
                            };
                            if let Err(e) = Self::put_with_retry(&store, &path, data, 5).await {
                                let msg = format!("upload failed for {path}: {e}");
                                tracing_or_eprintln(msg.clone());
                                // Record first error; continue so all files are attempted.
                                if upload_err.is_none() {
                                    upload_err = Some(msg);
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
                    UploadTask::DeleteSSTable {
                        table_id,
                        sstable_id,
                        grace_period,
                        on_complete,
                    } => {
                        // Wait for in-flight reads to drain before deleting.
                        tokio::time::sleep(grace_period).await;

                        let hex_prefix = hex_prefix_for(&sstable_id);
                        // Standard SSTable component filenames.
                        let components = [
                            "Data.db",
                            "Index.db",
                            "Filter.db",
                            "Statistics.db",
                            "TOC.txt",
                        ];
                        let mut delete_err: Option<String> = None;
                        for component in &components {
                            let path = sstable_object_key(
                                &prefix,
                                &hex_prefix,
                                &table_id,
                                &sstable_id,
                                component,
                            );
                            match store.delete(&path).await {
                                Ok(_) => {}
                                Err(object_store::Error::NotFound { .. }) => {
                                    // 404 is success — already gone.
                                }
                                Err(e) => {
                                    let msg = format!("delete failed for {path}: {e}");
                                    tracing_or_eprintln(msg.clone());
                                    if delete_err.is_none() {
                                        delete_err = Some(msg);
                                    }
                                }
                            }
                        }
                        if let Some(tx) = on_complete {
                            let result = match delete_err {
                                None => Ok(()),
                                Some(msg) => Err(msg),
                            };
                            let _ = tx.send(result);
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
            }
        });

        Self {
            task_tx: tx,
            handle: Mutex::new(Some(handle)),
        }
    }

    /// Submits an upload task. Blocks if the queue is full (backpressure).
    pub async fn submit(&self, task: UploadTask) -> ferrosa_common::Result<()> {
        self.task_tx
            .send(task)
            .await
            .map_err(|_| ferrosa_common::Error::InvalidFormat("upload channel closed".into()))
    }

    /// Attempts to submit an upload task without waiting for queue capacity.
    ///
    /// Startup crash-recovery paths use this to avoid blocking listener binding
    /// behind slow object-store uploads. Failed submissions leave their
    /// pending-log entries intact for a later retry.
    pub fn try_submit(&self, task: UploadTask) -> ferrosa_common::Result<()> {
        self.task_tx.try_send(task).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => {
                ferrosa_common::Error::InvalidFormat("upload queue full".into())
            }
            mpsc::error::TrySendError::Closed(_) => {
                ferrosa_common::Error::InvalidFormat("upload channel closed".into())
            }
        })
    }

    /// Shuts down the upload manager, draining the queue.
    pub async fn shutdown(&self) {
        let _ = self.task_tx.send(UploadTask::Shutdown).await;
        let handle = self.handle.lock().take();
        if let Some(handle) = handle {
            let _ = handle.await;
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
    use std::sync::atomic::{AtomicUsize, Ordering};
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
    fn missing_sstable_component_reports_upload_failure() {
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
                err.contains("failed to read"),
                "missing component should report a read failure, got: {err}"
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
