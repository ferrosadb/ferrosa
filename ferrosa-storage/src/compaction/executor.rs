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
                        Ok(task) => match Self::execute_task(&task) {
                            Ok(output) => {
                                let _ = result_tx.send(CompactionResult { task, output });
                            }
                            Err(e) => {
                                eprintln!("[compaction] task failed: {e}");
                            }
                        },
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
}

impl Drop for CompactionExecutor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl CompactionExecutor {
    /// Execute a single compaction task by merging input SSTables into one output.
    fn execute_task(task: &CompactionTask) -> std::result::Result<SSTableMetadata, String> {
        use crate::flush::{self, FileFlushTarget, FlushTarget};
        use crate::merge;
        use ferrosa_sstable::io::FileReadAt;
        use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
        use ferrosa_sstable::writer::SSTableWriter;
        use ferrosa_sstable::WriteOptions;
        use std::collections::BTreeMap;

        // 1. Read all partitions from each input SSTable.
        let mut all_partitions: BTreeMap<Vec<u8>, Vec<ferrosa_sstable::types::Partition>> =
            BTreeMap::new();

        for input in &task.inputs {
            let gen = &input.id;
            let dir = &input.path;

            let data = FileReadAt::open(dir.join(format!("{gen}-Data.db")))
                .map_err(|e| format!("open data: {e}"))?;
            let partitions_file = FileReadAt::open(dir.join(format!("{gen}-Partitions.db")))
                .map_err(|e| format!("open partitions: {e}"))?;
            let rows = FileReadAt::open(dir.join(format!("{gen}-Rows.db")))
                .map_err(|e| format!("open rows: {e}"))?;
            let filter = std::fs::read(dir.join(format!("{gen}-Filter.db")))
                .map_err(|e| format!("read filter: {e}"))?;
            let statistics = std::fs::read(dir.join(format!("{gen}-Statistics.db")))
                .map_err(|e| format!("read statistics: {e}"))?;
            let compression_info =
                std::fs::read(dir.join(format!("{gen}-CompressionInfo.db"))).ok();

            let reader = SSTableReader::open(SSTableComponents {
                data,
                partitions: partitions_file,
                rows,
                filter,
                compression_info,
                statistics,
            })
            .map_err(|e| format!("open sstable: {e}"))?;

            let partitions = reader
                .read_all_partitions()
                .map_err(|e| format!("read partitions: {e}"))?;

            for p in partitions {
                all_partitions
                    .entry(p.key.key.as_bytes().to_vec())
                    .or_default()
                    .push(p);
            }
        }

        // 2. Merge partitions with the same key.
        let mut merged: Vec<ferrosa_sstable::types::Partition> = all_partitions
            .into_values()
            .map(merge::merge_partitions)
            .collect();

        // 3. Sort by key (SSTableWriter requires token order).
        merged.sort_by(|a, b| a.key.cmp(&b.key));

        if merged.is_empty() {
            return Err("no partitions to compact".into());
        }

        // 4. Build serialization header and write output SSTable.
        let header = flush::build_serialization_header(&task.schema, &merged);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options, header);
        for p in &merged {
            writer
                .add_partition(p)
                .map_err(|e| format!("write partition: {e}"))?;
        }
        let output = writer.finish().map_err(|e| format!("finish: {e}"))?;

        // 5. Write to output directory via FileFlushTarget.
        let flush_target = FileFlushTarget::new_starting_at(task.output_dir.clone())
            .map_err(|e| format!("flush target: {e}"))?;
        let _reader = flush_target
            .flush(output)
            .map_err(|e| format!("flush output: {e}"))?;

        let gen = flush_target.generation();
        let output_id = format!("{gen}");
        let partition_count = merged.len() as u64;

        let total_size: u64 = [
            format!("{gen}-Data.db"),
            format!("{gen}-Partitions.db"),
            format!("{gen}-Rows.db"),
            format!("{gen}-Filter.db"),
            format!("{gen}-Statistics.db"),
            format!("{gen}-TOC.txt"),
        ]
        .iter()
        .filter_map(|name| {
            let path = task.output_dir.join(name);
            std::fs::metadata(&path).ok().map(|m| m.len())
        })
        .sum();

        let min_token = merged.first().map(|p| p.key.token.0).unwrap_or(0);
        let max_token = merged.last().map(|p| p.key.token.0).unwrap_or(0);
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

        Ok(SSTableMetadata {
            id: output_id,
            path: task.output_dir.clone(),
            size_bytes: total_size,
            min_token,
            max_token,
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            partition_count,
        })
    }
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
}
