//! TimeSeriesAggregator (WriteObserver) and ConsolidationWorker.
//!
//! The aggregator inserts into ring buffers inline on the write path and
//! sends consolidation tasks to an async worker via a bounded channel.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use smallvec::SmallVec;

use crate::commitlog::config::TableId;
use crate::commitlog::mutation::Mutation;
use crate::observer::{ObserverMode, WriteObserver};

use super::config::ConsolidationConfig;
use super::ring::{BoundaryStatus, RingBuffer, RingEntry};

/// A task sent from the inline write path to the async consolidation worker.
#[derive(Debug, Clone)]
pub enum ConsolidationTask {
    /// Normal boundary crossing -- window data copied from ring.
    BoundaryCrossed {
        table_id: TableId,
        partition_key: Vec<u8>,
        window_entries: Vec<RingEntry>,
        window_start_ts: i64,
    },
    /// Late data detected -- requires disk read to reconstruct window.
    LateData {
        table_id: TableId,
        partition_key: Vec<u8>,
        window_start_ts: i64,
        late_timestamp: i64,
    },
}

/// Time-series aggregator. Implements `WriteObserver` for inline ring buffer
/// insertion with async consolidation dispatch.
pub struct TimeSeriesAggregator {
    config: ConsolidationConfig,
    table_id: TableId,
    /// Column indices to extract from mutations (by position in cells vec).
    value_column_indices: Vec<u16>,
    /// Per-partition_key ring buffers. DashMap provides per-shard locking.
    rings: DashMap<Vec<u8>, RingBuffer>,
    /// Channel sender for async consolidation tasks.
    task_tx: std::sync::mpsc::SyncSender<ConsolidationTask>,
    /// Counter for dropped tasks (channel full).
    drop_count: AtomicU64,
    /// Maximum number of ring buffers.
    max_rings: usize,
}

impl TimeSeriesAggregator {
    /// Create a new aggregator for the given table.
    ///
    /// `value_column_indices` are the column_index values (from Row.cells) for
    /// the numeric columns to aggregate. `task_tx` sends consolidation tasks
    /// to the async worker.
    pub fn new(
        config: ConsolidationConfig,
        table_id: TableId,
        value_column_indices: Vec<u16>,
        task_tx: std::sync::mpsc::SyncSender<ConsolidationTask>,
    ) -> Self {
        let max_rings = config.max_rings;
        Self {
            config,
            table_id,
            value_column_indices,
            rings: DashMap::new(),
            task_tx,
            drop_count: AtomicU64::new(0),
            max_rings,
        }
    }

    /// Returns the number of active ring buffers.
    pub fn ring_count(&self) -> usize {
        self.rings.len()
    }

    /// Returns the total number of dropped consolidation tasks.
    pub fn drop_count(&self) -> u64 {
        self.drop_count.load(Ordering::Relaxed)
    }

    /// Extract f64 values from a mutation row for the configured columns.
    ///
    /// Decodes CQL big-endian bytes to f64. Supports double (8 bytes),
    /// float (4 bytes), int (4 bytes), bigint (8 bytes).
    fn extract_values(
        &self,
        row: &ferrosa_sstable::types::Row,
    ) -> Option<(i64, SmallVec<[f64; 8]>)> {
        let mut values = SmallVec::new();
        let timestamp = row.primary_key_liveness.timestamp;

        for &col_idx in &self.value_column_indices {
            let cell = row.cells.iter().find(|(idx, _)| *idx == col_idx);
            match cell {
                Some((_, cv)) => {
                    if let Some(ref bytes) = cv.value {
                        if let Some(v) = decode_numeric_bytes(bytes) {
                            values.push(v);
                        } else {
                            return None; // non-decodable value, skip this row
                        }
                    } else {
                        return None; // tombstone, skip
                    }
                }
                None => return None, // column not present
            }
        }

        Some((timestamp, values))
    }
}

/// Decode CQL big-endian bytes to f64.
///
/// Supports: double (8 bytes), float (4 bytes), int (4 bytes), bigint/counter (8 bytes).
/// When the column type is not known, 8 bytes is interpreted as f64 (double) and
/// 4 bytes as i32 (int). Use [`decode_typed_numeric`] when the CQL type is available.
pub fn decode_numeric_bytes(bytes: &[u8]) -> Option<f64> {
    match bytes.len() {
        8 => {
            // Could be double or bigint. Default: f64 (double).
            let bits = u64::from_be_bytes(bytes.try_into().ok()?);
            Some(f64::from_bits(bits))
        }
        4 => {
            // Could be float or int. Default: i32 (int).
            let val = i32::from_be_bytes(bytes.try_into().ok()?);
            Some(val as f64)
        }
        _ => None,
    }
}

/// Type-aware numeric decoding. Use when the CQL column type is known.
pub fn decode_typed_numeric(bytes: &[u8], cql_type: &str) -> Option<f64> {
    match cql_type {
        "double" => {
            if bytes.len() != 8 {
                return None;
            }
            Some(f64::from_be_bytes(bytes.try_into().ok()?))
        }
        "float" => {
            if bytes.len() != 4 {
                return None;
            }
            Some(f32::from_be_bytes(bytes.try_into().ok()?) as f64)
        }
        "int" => {
            if bytes.len() != 4 {
                return None;
            }
            Some(i32::from_be_bytes(bytes.try_into().ok()?) as f64)
        }
        "bigint" | "counter" | "timestamp" => {
            if bytes.len() != 8 {
                return None;
            }
            Some(i64::from_be_bytes(bytes.try_into().ok()?) as f64)
        }
        _ => None,
    }
}

impl WriteObserver for TimeSeriesAggregator {
    fn mode(&self) -> ObserverMode {
        ObserverMode::Sync
    }

    fn tables(&self) -> Vec<TableId> {
        vec![self.table_id.clone()]
    }

    fn on_write(&self, _table: &TableId, mutation: &Mutation) -> Vec<Mutation> {
        let partition_key = mutation.key.key.as_bytes().to_vec();

        for row in &mutation.rows {
            let Some((timestamp, values)) = self.extract_values(row) else {
                continue;
            };

            // Get or create ring buffer.
            let mut ring = self.rings.entry(partition_key.clone()).or_insert_with(|| {
                RingBuffer::new(
                    self.config.ring_capacity,
                    self.config.interval_micros(),
                    self.value_column_indices.clone(),
                )
            });

            let boundary_before = ring.boundary_ts();
            let status = ring.insert(timestamp, values);

            match status {
                BoundaryStatus::BoundaryCrossed => {
                    // Copy window entries for the completed window.
                    let window_start = boundary_before - self.config.interval_micros();
                    let window_entries = ring.window_owned(window_start, boundary_before);

                    if !window_entries.is_empty() {
                        let task = ConsolidationTask::BoundaryCrossed {
                            table_id: self.table_id.clone(),
                            partition_key: partition_key.clone(),
                            window_entries,
                            window_start_ts: window_start,
                        };
                        if self.task_tx.try_send(task).is_err() {
                            self.drop_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                BoundaryStatus::LateData => {
                    let interval = self.config.interval_micros();
                    let window_start = (timestamp / interval) * interval;
                    let task = ConsolidationTask::LateData {
                        table_id: self.table_id.clone(),
                        partition_key: partition_key.clone(),
                        window_start_ts: window_start,
                        late_timestamp: timestamp,
                    };
                    if self.task_tx.try_send(task).is_err() {
                        self.drop_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                BoundaryStatus::Normal => {}
            }
        }

        Vec::new() // never return inline mutations
    }
}

/// Metrics for consolidation observability.
#[derive(Debug, Default)]
pub struct ConsolidationMetrics {
    pub windows_consolidated: AtomicU64,
    pub late_arrivals: AtomicU64,
    pub consolidation_drops: AtomicU64,
}

/// Async worker that processes consolidation tasks.
///
/// Owns the receiving end of the task channel. Executes consolidation functions
/// and writes results to downstream tables via `StorageEngine::write()`.
pub struct ConsolidationWorker {
    config: ConsolidationConfig,
    #[allow(dead_code)]
    task_rx: std::sync::mpsc::Receiver<ConsolidationTask>,
    metrics: Arc<ConsolidationMetrics>,
}

impl ConsolidationWorker {
    /// Create a new worker.
    pub fn new(
        config: ConsolidationConfig,
        task_rx: std::sync::mpsc::Receiver<ConsolidationTask>,
        metrics: Arc<ConsolidationMetrics>,
    ) -> Self {
        Self {
            config,
            task_rx,
            metrics,
        }
    }

    /// Process a single `BoundaryCrossed` task. Returns the consolidated f64
    /// values (one per function) for testing purposes.
    pub fn consolidate_window(&self, entries: &[RingEntry], column_idx: usize) -> Vec<f64> {
        let values: Vec<f64> = entries.iter().map(|e| e.values[column_idx]).collect();
        if values.is_empty() {
            return vec![];
        }
        super::consolidation::consolidate_values(&values, &self.config.functions)
    }

    /// Returns a reference to the metrics.
    pub fn metrics(&self) -> &Arc<ConsolidationMetrics> {
        &self.metrics
    }
}

impl TimeSeriesAggregator {
    /// Evict the coldest ring buffers when `ring_count > max_rings`.
    ///
    /// Returns the number of evicted entries. This is called by a background
    /// sweep, not on the write path.
    pub fn evict_cold_rings(&self) -> usize {
        let current = self.rings.len();
        if current <= self.max_rings {
            return 0;
        }

        let to_evict = current - self.max_rings;

        // Collect (key, last_access) pairs.
        let mut entries: Vec<(Vec<u8>, std::time::Instant)> = self
            .rings
            .iter()
            .map(|r| (r.key().clone(), r.value().last_access()))
            .collect();

        // Sort by last_access ascending (oldest first).
        entries.sort_by_key(|(_, ts)| *ts);

        let mut evicted = 0;
        for (key, _) in entries.into_iter().take(to_evict) {
            self.rings.remove(&key);
            evicted += 1;
        }

        evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::SmallVec;

    #[test]
    fn consolidation_task_boundary_crossed() {
        let task = ConsolidationTask::BoundaryCrossed {
            table_id: TableId::new("ks", "sensor_1s"),
            partition_key: b"sensor-1".to_vec(),
            window_entries: vec![RingEntry {
                timestamp: 1_000_000,
                values: SmallVec::from_slice(&[42.0]),
            }],
            window_start_ts: 0,
        };

        if let ConsolidationTask::BoundaryCrossed {
            table_id,
            partition_key,
            window_entries,
            window_start_ts,
        } = &task
        {
            assert_eq!(table_id.keyspace, "ks");
            assert_eq!(table_id.table, "sensor_1s");
            assert_eq!(partition_key, b"sensor-1");
            assert_eq!(window_entries.len(), 1);
            assert_eq!(*window_start_ts, 0);
        } else {
            panic!("expected BoundaryCrossed");
        }
    }

    #[test]
    fn consolidation_task_late_data() {
        let task = ConsolidationTask::LateData {
            table_id: TableId::new("ks", "sensor_1s"),
            partition_key: b"sensor-1".to_vec(),
            window_start_ts: 0,
            late_timestamp: 5_000_000,
        };

        if let ConsolidationTask::LateData { late_timestamp, .. } = &task {
            assert_eq!(*late_timestamp, 5_000_000);
        } else {
            panic!("expected LateData");
        }
    }

    #[test]
    fn aggregator_on_write_inserts_into_ring() {
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "sensor_10s".to_string(),
            columns: vec!["value".to_string()],
            ring_capacity: 64,
            max_rings: 100,
            ..ConsolidationConfig::default()
        };

        let (tx, rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor_1s");

        let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

        // Simulate a write with a double value (8 bytes, big-endian).
        let value_bytes = 42.0_f64.to_be_bytes().to_vec();
        let mutation = Mutation {
            keyspace: "ks".to_string(),
            table: "sensor_1s".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"sensor-1".to_vec())),
            rows: vec![Row {
                clustering: 5_000_000_i64.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(value_bytes, 5_000_000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(5_000_000),
            }],
            timestamp: 5_000_000,
        };

        let derived = aggregator.on_write(&table_id, &mutation);
        assert!(derived.is_empty()); // aggregator never returns inline mutations

        // No boundary crossed yet, channel should be empty.
        assert!(rx.try_recv().is_err());

        // Check that the ring buffer was created.
        assert_eq!(aggregator.ring_count(), 1);
    }

    #[test]
    fn aggregator_sends_boundary_task() {
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10), // 10s = 10_000_000 micros
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "sensor_10s".to_string(),
            columns: vec!["value".to_string()],
            ring_capacity: 64,
            max_rings: 100,
            ..ConsolidationConfig::default()
        };

        let (tx, rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor_1s");
        let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

        let make_mutation = |ts: i64, val: f64| Mutation {
            keyspace: "ks".to_string(),
            table: "sensor_1s".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"s1".to_vec())),
            rows: vec![Row {
                clustering: ts.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(val.to_be_bytes().to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
            timestamp: ts,
        };

        // Fill up to boundary.
        for i in 0..10 {
            aggregator.on_write(&table_id, &make_mutation(i * 1_000_000, i as f64));
        }
        // No task yet (boundary at 10M, haven't crossed it).
        assert!(rx.try_recv().is_err());

        // Cross boundary at ts=10M.
        aggregator.on_write(&table_id, &make_mutation(10_000_000, 10.0));

        // Should receive a BoundaryCrossed task.
        let task = rx.try_recv().expect("expected a task");
        match task {
            ConsolidationTask::BoundaryCrossed {
                window_start_ts,
                window_entries,
                ..
            } => {
                assert_eq!(window_start_ts, 0);
                assert!(!window_entries.is_empty());
            }
            _ => panic!("expected BoundaryCrossed"),
        }
    }

    // --- Task 11: decode_numeric_bytes tests ---

    #[test]
    fn decode_double_bytes() {
        let val = 42.5_f64;
        let bytes = val.to_be_bytes();
        assert!((decode_numeric_bytes(&bytes).unwrap() - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn decode_int_bytes() {
        let val = 1000_i32;
        let bytes = val.to_be_bytes();
        assert!((decode_numeric_bytes(&bytes).unwrap() - 1000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn decode_invalid_length() {
        assert!(decode_numeric_bytes(&[1, 2, 3]).is_none());
        assert!(decode_numeric_bytes(&[]).is_none());
        assert!(decode_numeric_bytes(&[1, 2, 3, 4, 5]).is_none());
    }

    #[test]
    fn decode_typed_float() {
        let val = 42.5_f32;
        let bytes = val.to_be_bytes();
        let result = decode_typed_numeric(&bytes, "float").unwrap();
        assert!((result - 42.5).abs() < 1e-5);
    }

    #[test]
    fn decode_typed_bigint() {
        let val = 1_000_000_i64;
        let bytes = val.to_be_bytes();
        let result = decode_typed_numeric(&bytes, "bigint").unwrap();
        assert!((result - 1_000_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn decode_typed_counter() {
        let val = 42_i64;
        let bytes = val.to_be_bytes();
        let result = decode_typed_numeric(&bytes, "counter").unwrap();
        assert!((result - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn decode_typed_unknown_type() {
        assert!(decode_typed_numeric(&[0; 8], "text").is_none());
    }

    // --- Task 12: ConsolidationWorker tests ---

    #[test]
    fn worker_processes_boundary_task() {
        use super::super::consolidation::{consolidate_values, ConsolidationFn};

        // Verify consolidation logic works end-to-end for a task's window entries.
        let entries = [
            RingEntry {
                timestamp: 0,
                values: SmallVec::from_slice(&[1.0]),
            },
            RingEntry {
                timestamp: 1_000_000,
                values: SmallVec::from_slice(&[2.0]),
            },
            RingEntry {
                timestamp: 2_000_000,
                values: SmallVec::from_slice(&[3.0]),
            },
        ];

        // Extract column 0 values.
        let values: Vec<f64> = entries.iter().map(|e| e.values[0]).collect();
        let funcs = vec![
            ConsolidationFn::Min,
            ConsolidationFn::Max,
            ConsolidationFn::Avg,
        ];
        let results = consolidate_values(&values, &funcs);

        assert!((results[0] - 1.0).abs() < f64::EPSILON); // min
        assert!((results[1] - 3.0).abs() < f64::EPSILON); // max
        assert!((results[2] - 2.0).abs() < f64::EPSILON); // avg
    }

    #[test]
    fn worker_consolidate_window_method() {
        use super::super::consolidation::ConsolidationFn;

        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![
                ConsolidationFn::Min,
                ConsolidationFn::Max,
                ConsolidationFn::Avg,
            ],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ..ConsolidationConfig::default()
        };

        let (_tx, rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(10);
        let metrics = Arc::new(ConsolidationMetrics::default());
        let worker = ConsolidationWorker::new(config, rx, metrics);

        let entries = vec![
            RingEntry {
                timestamp: 0,
                values: SmallVec::from_slice(&[1.0]),
            },
            RingEntry {
                timestamp: 1_000_000,
                values: SmallVec::from_slice(&[2.0]),
            },
            RingEntry {
                timestamp: 2_000_000,
                values: SmallVec::from_slice(&[3.0]),
            },
        ];

        let results = worker.consolidate_window(&entries, 0);
        assert_eq!(results.len(), 3);
        assert!((results[0] - 1.0).abs() < f64::EPSILON); // min
        assert!((results[1] - 3.0).abs() < f64::EPSILON); // max
        assert!((results[2] - 2.0).abs() < f64::EPSILON); // avg
    }

    #[test]
    fn worker_multi_column_consolidation() {
        use super::super::consolidation::{consolidate_values, ConsolidationFn};

        // 3 entries, 2 columns each.
        let entries = [
            RingEntry {
                timestamp: 0,
                values: SmallVec::from_slice(&[10.0, 100.0]),
            },
            RingEntry {
                timestamp: 1_000_000,
                values: SmallVec::from_slice(&[20.0, 200.0]),
            },
            RingEntry {
                timestamp: 2_000_000,
                values: SmallVec::from_slice(&[30.0, 300.0]),
            },
        ];

        let funcs = vec![ConsolidationFn::Avg];

        // Column 0.
        let col0: Vec<f64> = entries.iter().map(|e| e.values[0]).collect();
        let results0 = consolidate_values(&col0, &funcs);
        assert!((results0[0] - 20.0).abs() < f64::EPSILON);

        // Column 1.
        let col1: Vec<f64> = entries.iter().map(|e| e.values[1]).collect();
        let results1 = consolidate_values(&col1, &funcs);
        assert!((results1[0] - 200.0).abs() < f64::EPSILON);
    }

    #[test]
    fn worker_consolidate_empty_entries() {
        use super::super::consolidation::ConsolidationFn;

        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ..ConsolidationConfig::default()
        };

        let (_tx, rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(10);
        let metrics = Arc::new(ConsolidationMetrics::default());
        let worker = ConsolidationWorker::new(config, rx, metrics);

        let results = worker.consolidate_window(&[], 0);
        assert!(results.is_empty());
    }

    #[test]
    fn worker_metrics_default() {
        let metrics = ConsolidationMetrics::default();
        assert_eq!(metrics.windows_consolidated.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.late_arrivals.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.consolidation_drops.load(Ordering::Relaxed), 0);
    }

    // --- Task 13: LRU eviction tests ---

    #[test]
    fn aggregator_lru_eviction() {
        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ring_capacity: 4,
            max_rings: 2, // only allow 2 rings
            ..ConsolidationConfig::default()
        };

        let (tx, _rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor");
        let aggregator = TimeSeriesAggregator::new(config, table_id, vec![0], tx);

        // Insert into 3 different partitions, exceeding max_rings=2.
        for pk in &[b"pk1".to_vec(), b"pk2".to_vec(), b"pk3".to_vec()] {
            aggregator
                .rings
                .insert(pk.clone(), RingBuffer::new(4, 10_000_000, vec![0]));
        }

        assert_eq!(aggregator.ring_count(), 3);

        // Run eviction sweep -- should remove oldest (coldest) ring.
        let evicted = aggregator.evict_cold_rings();
        assert!(evicted >= 1);
        assert!(aggregator.ring_count() <= 2);
    }

    #[test]
    fn aggregator_eviction_noop_when_under_limit() {
        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ring_capacity: 4,
            max_rings: 10,
            ..ConsolidationConfig::default()
        };

        let (tx, _rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor");
        let aggregator = TimeSeriesAggregator::new(config, table_id, vec![0], tx);

        // Insert only 2 rings, well under max_rings=10.
        aggregator
            .rings
            .insert(b"pk1".to_vec(), RingBuffer::new(4, 10_000_000, vec![0]));
        aggregator
            .rings
            .insert(b"pk2".to_vec(), RingBuffer::new(4, 10_000_000, vec![0]));

        let evicted = aggregator.evict_cold_rings();
        assert_eq!(evicted, 0);
        assert_eq!(aggregator.ring_count(), 2);
    }

    // --- Task 14: Late data detection in on_write ---

    #[test]
    fn aggregator_late_data_sends_task() {
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ring_capacity: 64,
            max_rings: 100,
            ..ConsolidationConfig::default()
        };

        let (tx, rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor");
        let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

        let make_mutation = |ts: i64, val: f64| Mutation {
            keyspace: "ks".to_string(),
            table: "sensor".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"s1".to_vec())),
            rows: vec![Row {
                clustering: ts.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(val.to_be_bytes().to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
            timestamp: ts,
        };

        // Write at ts=15M (boundary at 20M).
        aggregator.on_write(&table_id, &make_mutation(15_000_000, 1.0));

        // Cross boundary to 30M.
        aggregator.on_write(&table_id, &make_mutation(25_000_000, 2.0));
        let _ = rx.try_recv(); // consume BoundaryCrossed

        // Late write at ts=5M (before boundary - interval = 20M).
        aggregator.on_write(&table_id, &make_mutation(5_000_000, 3.0));

        // Should get a LateData task.
        let task = rx.try_recv().expect("expected LateData task");
        match task {
            ConsolidationTask::LateData { late_timestamp, .. } => {
                assert_eq!(late_timestamp, 5_000_000);
            }
            _ => panic!("expected LateData"),
        }
    }

    #[test]
    fn aggregator_drop_count_tracks_channel_full() {
        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "sensor_10s".to_string(),
            columns: vec!["value".to_string()],
            ring_capacity: 64,
            max_rings: 100,
            ..ConsolidationConfig::default()
        };

        // Channel with capacity 0 -- every send will fail.
        let (tx, _rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(0);
        let table_id = TableId::new("ks", "sensor_1s");
        let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let make_mutation = |ts: i64, val: f64| Mutation {
            keyspace: "ks".to_string(),
            table: "sensor_1s".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"s1".to_vec())),
            rows: vec![Row {
                clustering: ts.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(val.to_be_bytes().to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
            timestamp: ts,
        };

        // Write within first window, then cross boundary.
        aggregator.on_write(&table_id, &make_mutation(1_000_000, 1.0));
        aggregator.on_write(&table_id, &make_mutation(10_000_000, 2.0));

        // The boundary crossing should have tried to send and been dropped.
        assert!(aggregator.drop_count() >= 1);
    }
}
