//! Background writer task that drains the span channel and writes
//! batches to `system_observability.spans` via `StorageEngine::write_observability()`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use super::layer::SpanRecord;

/// Default batch size: flush after this many spans.
const DEFAULT_BATCH_SIZE: usize = 100;
/// Default flush interval.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Background writer that drains span records from the channel and writes
/// them to the storage engine in batches.
pub struct TelemetryWriter {
    engine: Arc<StorageEngine>,
    receiver: mpsc::Receiver<SpanRecord>,
    cancel: CancellationToken,
    batch_size: usize,
    flush_interval: Duration,
}

impl TelemetryWriter {
    /// Create a new writer.
    pub fn new(
        engine: Arc<StorageEngine>,
        receiver: mpsc::Receiver<SpanRecord>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            engine,
            receiver,
            cancel,
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        }
    }

    /// Override the batch size (for testing).
    pub fn with_batch_size(mut self, size: usize) -> Self {
        assert!(size > 0, "batch size must be positive");
        self.batch_size = size;
        self
    }

    /// Override the flush interval (for testing).
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Run the writer loop. Returns when cancelled or the channel closes.
    ///
    /// Cancel-safe: uses `tokio::select!` with a cancellation token.
    /// Dropping the writer mid-flush is safe because `write_observability`
    /// is synchronous and each write is independent.
    pub async fn run(mut self) {
        let table_id = TableId::new(ferrosa_schema::system::observability::KEYSPACE, "spans");
        let mut batch = Vec::with_capacity(self.batch_size);

        loop {
            // Drain available records up to batch_size, or wait for flush interval.
            let should_exit = self.fill_batch(&mut batch).await;

            if !batch.is_empty() {
                self.write_batch(&table_id, &batch);
                batch.clear();
            }

            if should_exit {
                break;
            }
        }

        // Drain any remaining records after cancellation.
        while let Ok(record) = self.receiver.try_recv() {
            batch.push(record);
        }
        if !batch.is_empty() {
            self.write_batch(&table_id, &batch);
        }
    }

    /// Fill the batch buffer. Returns true if the writer should exit.
    async fn fill_batch(&mut self, batch: &mut Vec<SpanRecord>) -> bool {
        // First, try to receive at least one record (or timeout/cancel).
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return true,
            result = self.receiver.recv() => {
                match result {
                    Some(record) => batch.push(record),
                    None => return true, // Channel closed.
                }
            }
            _ = tokio::time::sleep(self.flush_interval) => return false,
        }

        // Non-blocking drain up to batch_size.
        while batch.len() < self.batch_size {
            match self.receiver.try_recv() {
                Ok(record) => batch.push(record),
                Err(_) => break,
            }
        }
        false
    }

    /// Write a batch of span records to storage.
    fn write_batch(&self, table_id: &TableId, batch: &[SpanRecord]) {
        for record in batch {
            let key = DecoratedKey::new(PartitionKey::new(record.trace_id.as_bytes().to_vec()));

            let ts = record.start_us;
            let mut clustering = Vec::with_capacity(24);
            clustering.extend_from_slice(&record.start_us.to_be_bytes());
            clustering.extend_from_slice(record.span_id.as_bytes());

            let parent_cell = match record.parent_id {
                Some(pid) => CellValue::live(pid.as_bytes().to_vec(), ts),
                None => CellValue::tombstone(ts, (ts / 1_000_000) as i32),
            };

            let row = Row {
                clustering,
                cells: vec![
                    (0, parent_cell),
                    (1, CellValue::live(record.node_id.as_bytes().to_vec(), ts)),
                    (2, CellValue::live(record.name.as_bytes().to_vec(), ts)),
                    (
                        3,
                        CellValue::live(record.duration_us.to_be_bytes().to_vec(), ts),
                    ),
                    (4, CellValue::live(record.status.as_bytes().to_vec(), ts)),
                    // attributes: empty map encoded as empty JSON object
                    (5, CellValue::live(b"{}".to_vec(), ts)),
                ],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            };

            // Errors are intentionally swallowed — telemetry must not crash
            // the node. In production, a metric counter would track failures.
            let _ = self.engine.write_observability(table_id, &key, row, ts);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_schema::system::observability;
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};
    use uuid::Uuid;

    fn test_engine(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
        };
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        engine
            .register_table(observability::spans_table_schema())
            .unwrap();
        engine
    }

    #[tokio::test]
    async fn writer_batches_and_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());

        let (sender, receiver) = mpsc::channel(100);
        let cancel = CancellationToken::new();

        // Send 5 span records.
        let node_id = Uuid::nil();
        for i in 0..5 {
            let record = SpanRecord {
                trace_id: Uuid::new_v4(),
                span_id: Uuid::new_v4(),
                parent_id: None,
                node_id,
                name: format!("test_span_{i}"),
                start_us: 1_000_000 + i as i64,
                duration_us: 100,
                status: "ok".to_string(),
            };
            sender.send(record).await.unwrap();
        }
        // Close sender to signal completion.
        drop(sender);

        let writer = TelemetryWriter::new(Arc::clone(&engine), receiver, cancel.clone())
            .with_batch_size(10)
            .with_flush_interval(Duration::from_millis(10));

        // Run the writer — it will drain the channel and exit when closed.
        writer.run().await;

        // Verify that spans were written to storage.
        let tid = TableId::new(observability::KEYSPACE, "spans");
        let partitions = engine.read_range(&tid, None, None, 100).unwrap();
        assert_eq!(
            partitions.len(),
            5,
            "all 5 spans should be written to storage"
        );
    }

    #[tokio::test]
    async fn writer_cancel_safe() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());

        let (sender, receiver) = mpsc::channel(100);
        let cancel = CancellationToken::new();

        // Send a few records.
        for i in 0..3 {
            let record = SpanRecord {
                trace_id: Uuid::new_v4(),
                span_id: Uuid::new_v4(),
                parent_id: None,
                node_id: Uuid::nil(),
                name: format!("cancel_span_{i}"),
                start_us: 2_000_000 + i as i64,
                duration_us: 50,
                status: "ok".to_string(),
            };
            sender.send(record).await.unwrap();
        }

        let cancel_clone = cancel.clone();
        let writer = TelemetryWriter::new(Arc::clone(&engine), receiver, cancel)
            .with_batch_size(2)
            .with_flush_interval(Duration::from_millis(5));

        // Cancel after a short delay.
        let handle = tokio::spawn(async move {
            writer.run().await;
        });

        // Give the writer a moment to start, then cancel.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_clone.cancel();
        drop(sender);

        // Writer should exit cleanly without panic.
        handle.await.expect("writer task must not panic on cancel");
    }
}
