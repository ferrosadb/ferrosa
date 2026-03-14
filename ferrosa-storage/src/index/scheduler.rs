//! Background index build scheduler.
//!
//! [`IndexBuildScheduler`] receives [`IndexBuildJob`]s via an mpsc channel and
//! processes them on N worker threads, following the same pattern as
//! `CompactionExecutor`.
//!
//! Workers currently only update the [`IndexStateTracker`] — actual SSTable
//! reading and index building will be wired in a later task.

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
}

/// Background scheduler that dispatches index build jobs to worker threads.
///
/// Follows the `CompactionExecutor` pattern: an mpsc channel feeds N worker
/// threads. Each worker pulls jobs, performs the build (stub for now), and
/// updates the shared [`IndexStateTracker`].
pub struct IndexBuildScheduler {
    task_tx: std::sync::mpsc::Sender<IndexBuildJob>,
    handles: Mutex<Vec<thread::JoinHandle<()>>>,
    stop_flag: Arc<AtomicBool>,
}

impl IndexBuildScheduler {
    /// Creates and starts the index build scheduler with `worker_count` threads.
    pub fn new(worker_count: usize, tracker: Arc<IndexStateTracker>) -> Self {
        let (task_tx, task_rx) = std::sync::mpsc::channel::<IndexBuildJob>();
        let task_rx = Arc::new(Mutex::new(task_rx));
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::with_capacity(worker_count);

        for i in 0..worker_count {
            let rx = Arc::clone(&task_rx);
            let stop = Arc::clone(&stop_flag);
            let tracker = Arc::clone(&tracker);

            let handle = thread::Builder::new()
                .name(format!("index-builder-{i}"))
                .spawn(move || {
                    Self::worker_loop(&rx, &stop, &tracker);
                })
                .expect("failed to spawn index builder thread");

            handles.push(handle);
        }

        Self {
            task_tx,
            handles: Mutex::new(handles),
            stop_flag,
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
        // Drop the sender so workers see Disconnected. We can't drop self.task_tx
        // directly, but the stop flag will make workers exit on timeout.
        let mut handles = self.handles.lock();
        for handle in handles.drain(..) {
            let _ = handle.join();
        }
    }

    /// Worker loop: pull jobs from the shared receiver and process them.
    fn worker_loop(
        rx: &Mutex<std::sync::mpsc::Receiver<IndexBuildJob>>,
        stop: &AtomicBool,
        tracker: &IndexStateTracker,
    ) {
        while !stop.load(Ordering::Acquire) {
            // Lock the receiver briefly to pull one job.
            let job = {
                let rx_guard = rx.lock();
                rx_guard.recv_timeout(std::time::Duration::from_millis(100))
            };

            match job {
                Ok(job) => {
                    // Stub: actual SSTable reading and index building is deferred.
                    // For now, just mark the SSTable as indexed in the tracker.
                    let (keyspace, table) = &job.table;
                    tracker.mark_indexed(keyspace, table, &job.index_name, &job.sstable_id);
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
    use crate::index::IndexStatus;
    use std::time::Duration;

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
}
