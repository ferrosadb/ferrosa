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

        eprintln!(
            "[compaction] starting task for {}: {} inputs",
            task.table_id,
            task.inputs.len()
        );

        // 1. Read all partitions from each input SSTable.
        //
        // CRITICAL: If ANY input SSTable fails to read, the entire compaction
        // must abort. Previously, unreadable SSTables were silently skipped
        // while the task succeeded. This caused data loss because
        // swap_compacted_sstables removes ALL input SSTables (including the
        // skipped unreadable ones) and replaces them with the output — which
        // only contains data from the successfully-read inputs.
        let mut all_partitions: BTreeMap<Vec<u8>, Vec<ferrosa_sstable::types::Partition>> =
            BTreeMap::new();
        let mut total_input_rows: usize = 0;

        for input in &task.inputs {
            let gen = &input.id;
            let dir = &input.path;

            let data_path = dir.join(format!("{gen}-Data.db"));
            match std::fs::metadata(&data_path) {
                Ok(meta) if meta.len() == 0 => {
                    return Err(format!(
                        "aborting compaction: SSTable {gen} has empty Data.db — \
                         cannot compact without all input data"
                    ));
                }
                Err(e) => {
                    return Err(format!(
                        "aborting compaction: SSTable {gen} Data.db missing: {e}"
                    ));
                }
                Ok(_) => {}
            }

            let data = FileReadAt::open(&data_path)
                .map_err(|e| format!("aborting compaction: SSTable {gen}: {e}"))?;
            let partitions_file = FileReadAt::open(dir.join(format!("{gen}-Partitions.db")))
                .map_err(|e| format!("aborting compaction: SSTable {gen}: {e}"))?;
            let rows = FileReadAt::open(dir.join(format!("{gen}-Rows.db")))
                .map_err(|e| format!("aborting compaction: SSTable {gen}: {e}"))?;
            let filter = std::fs::read(dir.join(format!("{gen}-Filter.db")))
                .map_err(|e| format!("aborting compaction: SSTable {gen}: {e}"))?;
            let statistics = std::fs::read(dir.join(format!("{gen}-Statistics.db")))
                .map_err(|e| format!("aborting compaction: SSTable {gen}: {e}"))?;
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
            .map_err(|e| format!("aborting compaction: SSTable {gen} corrupt: {e}"))?;

            let partitions = reader
                .read_all_partitions()
                .map_err(|e| format!("aborting compaction: SSTable {gen} read failed: {e}"))?;

            let input_row_count: usize = partitions.iter().map(|p| p.rows.len()).sum();
            total_input_rows += input_row_count;
            eprintln!(
                "[compaction]   input SSTable {gen}: {} partitions, {} rows",
                partitions.len(),
                input_row_count
            );

            for p in partitions {
                all_partitions
                    .entry(p.key.key.as_bytes().to_vec())
                    .or_default()
                    .push(p);
            }
        }

        eprintln!(
            "[compaction] total input: {} unique partition keys, {} total rows across all inputs",
            all_partitions.len(),
            total_input_rows
        );

        // 2. Merge partitions with the same key.
        let mut merged: Vec<ferrosa_sstable::types::Partition> = all_partitions
            .into_values()
            .map(merge::merge_partitions)
            .collect();

        // 3. Sort by key (SSTableWriter requires token order).
        merged.sort_by(|a, b| a.key.cmp(&b.key));

        let merged_row_count: usize = merged.iter().map(|p| p.rows.len()).sum();
        eprintln!(
            "[compaction] after merge: {} partitions, {} rows (input had {})",
            merged.len(),
            merged_row_count,
            total_input_rows
        );
        if merged_row_count < total_input_rows {
            eprintln!(
                "[compaction] WARNING: merge reduced rows from {} to {} — \
                 {} rows lost during merge (may be deletions or LWW overwrites)",
                total_input_rows,
                merged_row_count,
                total_input_rows - merged_row_count
            );
        }

        if merged.is_empty() {
            return Err("no partitions to compact".into());
        }

        // 4. Build serialization header and write output SSTable.
        let header = flush::build_serialization_header(&task.schema, &merged);
        eprintln!(
            "[compaction] header: min_ts={}, max_ts={}",
            header.min_timestamp, header.max_timestamp
        );
        let header_min_ts = header.min_timestamp;
        let header_max_ts = header.max_timestamp;
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
        let reader = flush_target
            .flush(output)
            .map_err(|e| format!("flush output: {e}"))?;

        // Readback verification: immediately re-read the output to ensure
        // the SSTable roundtrip is lossless.
        let readback_partitions = reader
            .read_all_partitions()
            .map_err(|e| format!("CORRUPTION: output SSTable readback failed: {e}"))?;
        let readback_row_count: usize = readback_partitions.iter().map(|p| p.rows.len()).sum();
        if readback_partitions.len() != merged.len() || readback_row_count != merged_row_count {
            eprintln!(
                "[compaction] CORRUPTION DETECTED: wrote {} partitions/{} rows, \
                 readback got {} partitions/{} rows",
                merged.len(),
                merged_row_count,
                readback_partitions.len(),
                readback_row_count
            );
            return Err(format!(
                "compaction output SSTable is corrupt: expected {} partitions/{} rows, \
                 readback got {} partitions/{} rows",
                merged.len(),
                merged_row_count,
                readback_partitions.len(),
                readback_row_count
            ));
        }
        eprintln!(
            "[compaction] output verified: {} partitions, {} rows (matches merge)",
            readback_partitions.len(),
            readback_row_count
        );

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

        // Use the actual header timestamps from the merged data, not the
        // input metadata. Input metadata may have stale/incorrect values
        // that propagate to future compactions.
        Ok(SSTableMetadata {
            id: output_id,
            path: task.output_dir.clone(),
            size_bytes: total_size,
            min_token,
            max_token,
            min_timestamp: header_min_ts,
            max_timestamp: header_max_ts,
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
        }
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

        let meta = result.unwrap();
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

        let output_partitions = reader.read_all_partitions().unwrap();
        assert_eq!(
            output_partitions.len(),
            10,
            "all 10 partitions must be readable from output SSTable"
        );
    }
}
