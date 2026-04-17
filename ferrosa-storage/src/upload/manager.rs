//! Upload manager: async task that uploads SSTables to S3-compatible storage.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parking_lot::Mutex;
use tokio::sync::mpsc;

/// A task to upload SSTable component files to object storage.
#[derive(Debug)]
pub enum UploadTask {
    /// Upload SSTable components.
    SSTable {
        /// Table identifier (e.g., "ks.table").
        table_id: String,
        /// SSTable identifier.
        sstable_id: String,
        /// Component files: (component_name, data).
        files: Vec<(String, Bytes)>,
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
                        for (name, data) in files {
                            let path = ObjectPath::from(format!(
                                "{prefix}/{hex_prefix}/{table_id}/{sstable_id}/{name}"
                            ));
                            if let Err(e) =
                                Self::put_with_retry(&store, &path, data.clone(), 5).await
                            {
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
                            let path = ObjectPath::from(format!(
                                "{prefix}/{hex_prefix}/{table_id}/{sstable_id}/{component}"
                            ));
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
            let store = Arc::new(InMemory::new());
            let manager = UploadManager::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                "test".into(),
                16,
                &tokio::runtime::Handle::current(),
            );

            let data = Bytes::from_static(b"hello sstable data");
            manager
                .submit(UploadTask::SSTable {
                    table_id: "ks.table".into(),
                    sstable_id: "abc123".into(),
                    files: vec![("Data.db".into(), data.clone())],
                    on_complete: None,
                })
                .await
                .unwrap();

            manager.shutdown().await;

            // Verify the file is in the store.
            let hex = hex_prefix_for("abc123");
            let path = ObjectPath::from(format!("test/{hex}/ks.table/abc123/Data.db"));
            let result = store.get(&path).await.unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes.as_ref(), b"hello sstable data");
        });
    }

    #[test]
    fn multiple_components_uploaded() {
        let rt = make_runtime();
        rt.block_on(async {
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
                        ("Data.db".into(), Bytes::from_static(b"data")),
                        ("Index.db".into(), Bytes::from_static(b"index")),
                        ("Filter.db".into(), Bytes::from_static(b"filter")),
                    ],
                    on_complete: None,
                })
                .await
                .unwrap();

            manager.shutdown().await;

            let hex = hex_prefix_for("001");
            for component in ["Data.db", "Index.db", "Filter.db"] {
                let path = ObjectPath::from(format!("pfx/{hex}/ks.t/001/{component}"));
                let result = store.get(&path).await.unwrap();
                assert!(!result.bytes().await.unwrap().is_empty());
            }
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
}
