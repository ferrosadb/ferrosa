//! Background compaction executor.
//!
//! Receives [`CompactionTask`]s via a channel, merges input SSTables on a
//! background thread using the existing `merge_partitions` logic, and sends
//! back [`CompactionResult`]s.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use parking_lot::Mutex;

use super::metadata::{CompactionTask, SSTableMetadata};

/// Result of a completed compaction.
#[derive(Debug)]
pub struct CompactionResult {
    /// The original task.
    pub task: CompactionTask,
    /// Metadata for the newly-created output SSTable.
    pub output: SSTableMetadata,
}

/// Runs compaction tasks on a background thread.
///
/// `StorageEngine` submits tasks via `submit()` and polls results via
/// `poll_results()`. The executor is stopped on `shutdown()`.
pub struct CompactionExecutor {
    task_tx: std::sync::mpsc::Sender<CompactionTask>,
    result_rx: Mutex<std::sync::mpsc::Receiver<CompactionResult>>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
    stop_flag: Arc<AtomicBool>,
}

impl Default for CompactionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionExecutor {
    /// Creates and starts the compaction executor background thread.
    pub fn new() -> Self {
        let (task_tx, task_rx) = std::sync::mpsc::channel::<CompactionTask>();
        let (result_tx, result_rx) = std::sync::mpsc::channel::<CompactionResult>();
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&stop_flag);

        let handle = thread::Builder::new()
            .name("compaction-executor".to_string())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    match task_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                        Ok(task) => {
                            // Execute compaction: merge input SSTables.
                            // For now this is a stub — full implementation requires
                            // SSTableReader/Writer integration which depends on the
                            // table schema being passed through the task.
                            let output = Self::execute_task(&task);
                            let _ = result_tx.send(CompactionResult { task, output });
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .expect("failed to spawn compaction executor thread");

        Self {
            task_tx,
            result_rx: Mutex::new(result_rx),
            handle: Mutex::new(Some(handle)),
            stop_flag,
        }
    }

    /// Submits a compaction task to the background thread.
    pub fn submit(&self, task: CompactionTask) -> ferrosa_common::Result<()> {
        self.task_tx
            .send(task)
            .map_err(|_| ferrosa_common::Error::InvalidFormat("compaction channel closed".into()))
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

    /// Shuts down the compaction executor, waiting for the background thread.
    pub fn shutdown(&self) {
        self.stop_flag.store(true, Ordering::Release);
        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }
    }

    /// Execute a single compaction task.
    ///
    /// This is a placeholder that creates minimal output metadata.
    /// Full implementation will use SSTableReader to read inputs,
    /// merge_partitions to merge, and SSTableWriter to write output.
    fn execute_task(task: &CompactionTask) -> SSTableMetadata {
        let total_size: u64 = task.inputs.iter().map(|i| i.size_bytes).sum();
        let min_token = task.inputs.iter().map(|i| i.min_token).min().unwrap_or(0);
        let max_token = task.inputs.iter().map(|i| i.max_token).max().unwrap_or(0);
        let min_ts = task
            .inputs
            .iter()
            .map(|i| i.min_timestamp)
            .min()
            .unwrap_or(0);
        let max_ts = task
            .inputs
            .iter()
            .map(|i| i.max_timestamp)
            .max()
            .unwrap_or(0);
        let partitions: u64 = task.inputs.iter().map(|i| i.partition_count).sum();

        let output_id = format!("compacted-{}", uuid_v4_stub());

        SSTableMetadata {
            id: output_id.clone(),
            path: task.output_dir.join(&output_id),
            size_bytes: total_size,
            min_token,
            max_token,
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            partition_count: partitions,
        }
    }
}

/// Simple monotonic ID generator (no uuid dependency needed).
fn uuid_v4_stub() -> u64 {
    use std::sync::atomic::AtomicU64;
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
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
        }
    }

    #[test]
    fn submit_and_poll_result() {
        let executor = CompactionExecutor::new();

        let task = CompactionTask {
            inputs: vec![
                make_metadata("a", 1000),
                make_metadata("b", 2000),
                make_metadata("c", 1500),
                make_metadata("d", 1200),
            ],
            output_dir: PathBuf::from("/tmp/output"),
        };

        executor.submit(task).unwrap();

        // Wait for the task to complete.
        std::thread::sleep(std::time::Duration::from_millis(500));

        let results = executor.poll_results();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].output.size_bytes, 5700); // sum of inputs
        assert_eq!(results[0].task.inputs.len(), 4);

        executor.shutdown();
    }

    #[test]
    fn shutdown_stops_cleanly() {
        let executor = CompactionExecutor::new();
        executor.shutdown();
        // Should not hang or panic.
    }
}
