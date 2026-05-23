//! TimeSeriesAggregator (WriteObserver) and ConsolidationWorker.
//!
//! The aggregator inserts into ring buffers inline on the write path and
//! sends consolidation tasks to an async worker via a bounded channel.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use dashmap::DashMap;
use smallvec::SmallVec;

use crate::commitlog::config::TableId;
use crate::commitlog::mutation::Mutation;
use crate::observer::{ObserverMode, WriteObserver};

use super::config::{ConsolidationConfig, TimeSeriesRuntimeSettings};
use super::consolidation::{
    emit_accumulated_streaming_results, Accumulator, ConsolidationFn, StreamingConsolidationError,
};
use super::materialization::TimeSeriesTimestampUnit;
use super::ring::{BoundaryStatus, RingBuffer, RingEntry};

/// A task sent from the inline write path to the async consolidation worker.
#[derive(Debug, Clone)]
pub enum ConsolidationTask {
    /// Normal boundary crossing -- window data copied from ring.
    BoundaryCrossed {
        table_id: TableId,
        partition_key: Vec<u8>,
        window_start_ts: i64,
        window_end_ts: i64,
    },
    /// Late data detected -- requires disk read to reconstruct window.
    LateData {
        table_id: TableId,
        partition_key: Vec<u8>,
        window_start_ts: i64,
        late_timestamp: i64,
    },
}

/// Bounded queue metadata exposed to storage-backed observability tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeSeriesQueueSnapshot {
    pub pending_tasks: u64,
    pub oldest_task_enqueued_at_ms: i64,
    pub oldest_task_age_ms: i64,
    pub oldest_window_start_ts: i64,
    pub oldest_window_end_ts: i64,
    pub oldest_task_type: &'static str,
}

#[derive(Debug, Clone)]
struct OldestQueuedTask {
    enqueued_at_ms: i64,
    window_start_ts: i64,
    window_end_ts: i64,
    task_type: &'static str,
}

impl ConsolidationTask {
    fn task_type(&self) -> &'static str {
        match self {
            ConsolidationTask::BoundaryCrossed { .. } => "window_close",
            ConsolidationTask::LateData { .. } => "late_data",
        }
    }

    fn window_start_ts(&self) -> i64 {
        match self {
            ConsolidationTask::BoundaryCrossed {
                window_start_ts, ..
            }
            | ConsolidationTask::LateData {
                window_start_ts, ..
            } => *window_start_ts,
        }
    }

    fn window_end_ts(&self, interval_micros: i64) -> i64 {
        match self {
            ConsolidationTask::BoundaryCrossed { window_end_ts, .. } => *window_end_ts,
            ConsolidationTask::LateData {
                window_start_ts, ..
            } => window_start_ts.saturating_add(interval_micros),
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// Time-series aggregator. Implements `WriteObserver` for inline ring buffer
/// insertion with async consolidation dispatch.
pub struct TimeSeriesAggregator {
    config: ConsolidationConfig,
    table_id: TableId,
    /// Column indices to extract from mutations (by position in cells vec).
    value_column_indices: Vec<u16>,
    /// CQL type names for each column (e.g., "double", "float", "int", "bigint").
    /// When present, enables type-aware decoding via `decode_typed_numeric`.
    /// Must be the same length as `value_column_indices`.
    column_types: Vec<String>,
    /// Unit used by the first clustering column in storage bytes.
    timestamp_unit: TimeSeriesTimestampUnit,
    /// Per-partition_key ring buffers. DashMap provides per-shard locking.
    rings: DashMap<Vec<u8>, RingBuffer>,
    /// Channel sender for async consolidation tasks.
    task_tx: std::sync::mpsc::SyncSender<ConsolidationTask>,
    /// Counter for dropped tasks (channel full).
    drop_count: AtomicU64,
    /// Counter for ring allocation rejections caused by memory/count caps.
    ring_budget_rejections: AtomicU64,
    /// Counter for ring evictions caused by memory/count caps.
    ring_evictions: AtomicU64,
    /// Counter for ring-thrash warning threshold crossings.
    ring_thrash_warnings: AtomicU64,
    /// Runtime-adjustable memory and warning controls.
    runtime_settings: Arc<TimeSeriesRuntimeSettings>,
    /// Maximum number of ring buffers.
    max_rings: usize,
    /// Optional shared metrics for observability.
    shared_metrics: Option<Arc<ConsolidationMetrics>>,
    /// Number of successfully enqueued tasks not yet drained by the materializer.
    pending_tasks: AtomicU64,
    /// Wall-clock enqueue time for the oldest pending task; -1 means empty.
    oldest_task_enqueued_at_ms: AtomicI64,
    /// Metadata for the oldest pending task. Retains one bounded descriptor only.
    oldest_task: Mutex<Option<OldestQueuedTask>>,
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
        let runtime_settings = Arc::new(TimeSeriesRuntimeSettings::from_config(&config));
        Self {
            config,
            table_id,
            value_column_indices,
            column_types: vec![],
            timestamp_unit: TimeSeriesTimestampUnit::Micros,
            rings: DashMap::new(),
            task_tx,
            drop_count: AtomicU64::new(0),
            ring_budget_rejections: AtomicU64::new(0),
            ring_evictions: AtomicU64::new(0),
            ring_thrash_warnings: AtomicU64::new(0),
            runtime_settings,
            max_rings,
            shared_metrics: None,
            pending_tasks: AtomicU64::new(0),
            oldest_task_enqueued_at_ms: AtomicI64::new(-1),
            oldest_task: Mutex::new(None),
        }
    }

    /// Create a new aggregator with CQL column type metadata for type-aware decoding.
    ///
    /// `column_types` must be the same length as `value_column_indices`, with CQL
    /// type names such as "double", "float", "int", "bigint", "counter".
    pub fn with_column_types(
        config: ConsolidationConfig,
        table_id: TableId,
        value_column_indices: Vec<u16>,
        column_types: Vec<String>,
        task_tx: std::sync::mpsc::SyncSender<ConsolidationTask>,
    ) -> Self {
        assert_eq!(
            value_column_indices.len(),
            column_types.len(),
            "column_types must match value_column_indices length"
        );
        let mut agg = Self::new(config, table_id, value_column_indices, task_tx);
        agg.column_types = column_types;
        agg
    }

    pub fn with_timestamp_unit(mut self, timestamp_unit: TimeSeriesTimestampUnit) -> Self {
        self.timestamp_unit = timestamp_unit;
        self
    }

    /// Create a new aggregator with CQL column type metadata and shared runtime settings.
    pub fn with_column_types_and_runtime_settings(
        config: ConsolidationConfig,
        table_id: TableId,
        value_column_indices: Vec<u16>,
        column_types: Vec<String>,
        task_tx: std::sync::mpsc::SyncSender<ConsolidationTask>,
        runtime_settings: Arc<TimeSeriesRuntimeSettings>,
    ) -> Self {
        assert_eq!(
            value_column_indices.len(),
            column_types.len(),
            "column_types must match value_column_indices length"
        );
        let mut agg = Self::with_runtime_settings(
            config,
            table_id,
            value_column_indices,
            task_tx,
            runtime_settings,
        );
        agg.column_types = column_types;
        agg
    }

    /// Create a new aggregator with CQL type metadata, shared runtime settings,
    /// and externally managed metrics.
    pub fn with_column_types_runtime_settings_and_metrics(
        config: ConsolidationConfig,
        table_id: TableId,
        value_column_indices: Vec<u16>,
        column_types: Vec<String>,
        task_tx: std::sync::mpsc::SyncSender<ConsolidationTask>,
        runtime_settings: Arc<TimeSeriesRuntimeSettings>,
        metrics: Arc<ConsolidationMetrics>,
    ) -> Self {
        let mut agg = Self::with_column_types_and_runtime_settings(
            config,
            table_id,
            value_column_indices,
            column_types,
            task_tx,
            runtime_settings,
        );
        agg.shared_metrics = Some(metrics);
        agg
    }

    /// Create a new aggregator with externally managed shared metrics.
    pub fn with_metrics(
        config: ConsolidationConfig,
        table_id: TableId,
        value_column_indices: Vec<u16>,
        task_tx: std::sync::mpsc::SyncSender<ConsolidationTask>,
        _metrics: Arc<ConsolidationMetrics>,
    ) -> Self {
        // Store metrics reference for observability; the aggregator still uses
        // its internal drop_count for the fast path.
        let mut agg = Self::new(config, table_id, value_column_indices, task_tx);
        agg.shared_metrics = Some(_metrics);
        agg
    }

    /// Create a new aggregator using externally managed runtime settings.
    pub fn with_runtime_settings(
        config: ConsolidationConfig,
        table_id: TableId,
        value_column_indices: Vec<u16>,
        task_tx: std::sync::mpsc::SyncSender<ConsolidationTask>,
        runtime_settings: Arc<TimeSeriesRuntimeSettings>,
    ) -> Self {
        let mut agg = Self::new(config, table_id, value_column_indices, task_tx);
        agg.runtime_settings = runtime_settings;
        agg
    }

    /// Returns the number of active ring buffers.
    pub fn ring_count(&self) -> usize {
        self.rings.len()
    }

    /// Returns the total number of dropped consolidation tasks.
    pub fn drop_count(&self) -> u64 {
        self.drop_count.load(Ordering::Relaxed)
    }

    /// Returns a reference to the shared metrics, if set.
    pub fn metrics(&self) -> Option<&Arc<ConsolidationMetrics>> {
        self.shared_metrics.as_ref()
    }

    /// Returns runtime-adjustable time-series controls.
    pub fn runtime_settings(&self) -> Arc<TimeSeriesRuntimeSettings> {
        Arc::clone(&self.runtime_settings)
    }

    /// Returns bounded queue metadata for observability without materializing queued tasks.
    pub fn queue_snapshot(&self) -> TimeSeriesQueueSnapshot {
        let pending_tasks = self.pending_tasks.load(Ordering::Relaxed);
        let now_ms = now_millis();
        let oldest_enqueued_at_ms = self.oldest_task_enqueued_at_ms.load(Ordering::Relaxed);
        let oldest_task_age_ms = if pending_tasks > 0 && oldest_enqueued_at_ms >= 0 {
            now_ms.saturating_sub(oldest_enqueued_at_ms)
        } else {
            0
        };
        let oldest = self.oldest_task.lock().clone();
        TimeSeriesQueueSnapshot {
            pending_tasks,
            oldest_task_enqueued_at_ms: if pending_tasks > 0 {
                oldest
                    .as_ref()
                    .map(|task| task.enqueued_at_ms)
                    .unwrap_or(oldest_enqueued_at_ms)
            } else {
                0
            },
            oldest_task_age_ms,
            oldest_window_start_ts: oldest.as_ref().map(|t| t.window_start_ts).unwrap_or(0),
            oldest_window_end_ts: oldest.as_ref().map(|t| t.window_end_ts).unwrap_or(0),
            oldest_task_type: oldest.as_ref().map(|t| t.task_type).unwrap_or("empty"),
        }
    }

    /// Marks one queued materialization task as drained by the materializer.
    pub fn note_materialization_task_drained(&self) {
        let previous = self
            .pending_tasks
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .unwrap_or(0);
        if previous <= 1 {
            self.oldest_task_enqueued_at_ms.store(-1, Ordering::Relaxed);
            *self.oldest_task.lock() = None;
        }
    }

    /// Stream one partition/window from the in-memory ring into consolidation results.
    ///
    /// Source values are never collected into a window buffer. The ring is
    /// visited entry-by-entry and folded into a single accumulator for the
    /// selected source column ordinal.
    pub fn emit_ring_window_results<F>(
        &self,
        partition_key: &[u8],
        window_start_ts: i64,
        window_end_ts: i64,
        source_column_ordinal: usize,
        functions: &[ConsolidationFn],
        emit: F,
    ) -> Result<bool, StreamingConsolidationError>
    where
        F: FnMut(f64),
    {
        let Some(ring) = self.rings.get(partition_key) else {
            return Ok(false);
        };

        let mut acc = Accumulator::new(false);
        ring.visit_window(window_start_ts, window_end_ts, |entry| {
            if let Some(value) = entry.values.get(source_column_ordinal) {
                acc.push(*value);
            }
        });
        let had_values = acc.count() > 0;
        emit_accumulated_streaming_results(&mut acc, functions, emit)?;
        Ok(had_values)
    }

    /// Returns the active ring cap after applying the optional memory budget.
    pub fn effective_max_rings(&self) -> usize {
        let Some(budget) = self.runtime_settings.ring_memory_budget_bytes() else {
            return self.max_rings;
        };

        let per_ring = RingBuffer::estimated_heap_bytes(
            self.config.ring_capacity,
            self.value_column_indices.len(),
        );
        if per_ring == 0 {
            return 0;
        }

        self.max_rings.min(budget / per_ring)
    }

    /// Returns the latest fully closed window start for a partition, when the
    /// partition still has an in-memory ring.
    pub fn watermark_window_start(&self, partition_key: &[u8]) -> Option<i64> {
        self.rings.get(partition_key).map(|ring| {
            ring.boundary_ts()
                .saturating_sub(self.config.interval_micros())
        })
    }

    fn ensure_capacity_for_new_ring(&self, partition_key: &[u8]) -> bool {
        if self.rings.contains_key(partition_key) {
            return true;
        }

        let effective_max = self.effective_max_rings();
        if effective_max == 0 {
            self.record_ring_budget_rejection(partition_key);
            return false;
        }

        if self.rings.len() >= effective_max {
            let evicted = self.evict_cold_rings_to_limit(effective_max.saturating_sub(1));
            if evicted == 0 && self.rings.len() >= effective_max {
                self.record_ring_budget_rejection(partition_key);
                return false;
            }
        }

        true
    }

    fn record_ring_budget_rejection(&self, partition_key: &[u8]) {
        self.ring_budget_rejections.fetch_add(1, Ordering::Relaxed);
        if let Some(ref m) = self.shared_metrics {
            m.ring_budget_rejections.fetch_add(1, Ordering::Relaxed);
        }
        tracing::warn!(
            table = %self.table_id.table,
            partition_key_len = partition_key.len(),
            ring_capacity = self.config.ring_capacity,
            max_rings = self.max_rings,
            ring_memory_budget_bytes = ?self.runtime_settings.ring_memory_budget_bytes(),
            "skipping time-series ring allocation because ring memory budget is exhausted"
        );
    }

    fn record_ring_evictions(&self, evicted: usize) {
        if evicted == 0 {
            return;
        }

        let total = self
            .ring_evictions
            .fetch_add(evicted as u64, Ordering::Relaxed)
            + evicted as u64;
        if let Some(ref m) = self.shared_metrics {
            m.ring_evictions
                .fetch_add(evicted as u64, Ordering::Relaxed);
        }
        let threshold = self.runtime_settings.ring_thrash_warn_evictions();
        if threshold != 0 && total / threshold > (total - evicted as u64) / threshold {
            self.ring_thrash_warnings.fetch_add(1, Ordering::Relaxed);
            if let Some(ref m) = self.shared_metrics {
                m.ring_thrash_warnings.fetch_add(1, Ordering::Relaxed);
            }
            tracing::warn!(
                table = %self.table_id.table,
                ring_evictions_total = total,
                ring_count = self.rings.len(),
                effective_max_rings = self.effective_max_rings(),
                "time-series ring buffer eviction thrashing detected"
            );
        }
    }

    /// Extract f64 values from a mutation row for the configured columns.
    ///
    /// Uses type-aware decoding when `column_types` is available, falling back
    /// to length-based heuristic via `decode_numeric_bytes` otherwise.
    fn note_materialization_task_enqueued(&self, task: &ConsolidationTask) {
        let enqueued_at_ms = now_millis();
        let previous = self.pending_tasks.fetch_add(1, Ordering::Relaxed);
        if previous == 0 {
            self.oldest_task_enqueued_at_ms
                .store(enqueued_at_ms, Ordering::Relaxed);
            *self.oldest_task.lock() = Some(OldestQueuedTask {
                enqueued_at_ms,
                window_start_ts: task.window_start_ts(),
                window_end_ts: task.window_end_ts(self.config.interval_micros()),
                task_type: task.task_type(),
            });
        }
    }

    fn enqueue_materialization_task(&self, task: &ConsolidationTask) -> bool {
        match self.task_tx.send(task.clone()) {
            Ok(()) => {
                self.note_materialization_task_enqueued(task);
                true
            }
            Err(_) => {
                self.drop_count.fetch_add(1, Ordering::Relaxed);
                if let Some(ref m) = self.shared_metrics {
                    m.consolidation_drops.fetch_add(1, Ordering::Relaxed);
                }
                false
            }
        }
    }

    fn extract_values(
        &self,
        row: &ferrosa_sstable::types::Row,
    ) -> Option<(i64, SmallVec<[f64; 8]>)> {
        let mut values = SmallVec::new();
        let timestamp = row_timestamp_micros(row, self.timestamp_unit);
        let has_types = !self.column_types.is_empty();

        for (i, &col_idx) in self.value_column_indices.iter().enumerate() {
            let cell = row.cells.iter().find(|(idx, _)| *idx == col_idx);
            match cell {
                Some((_, cv)) => {
                    if let Some(ref bytes) = cv.value {
                        let decoded = if has_types {
                            decode_typed_numeric(bytes, &self.column_types[i])
                        } else {
                            decode_numeric_bytes(bytes)
                        };
                        if let Some(v) = decoded {
                            values.push(v);
                        } else {
                            tracing::warn!(
                                table = %self.table_id.table,
                                column_index = col_idx,
                                byte_len = bytes.len(),
                                "failed to decode numeric bytes for consolidation column"
                            );
                            if let Some(ref m) = self.shared_metrics {
                                m.decode_failures.fetch_add(1, Ordering::Relaxed);
                            }
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

fn row_timestamp_micros(
    row: &ferrosa_sstable::types::Row,
    timestamp_unit: TimeSeriesTimestampUnit,
) -> i64 {
    if row.clustering.len() == std::mem::size_of::<i64>() {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&row.clustering);
        timestamp_unit.raw_to_micros(i64::from_be_bytes(bytes))
    } else {
        row.primary_key_liveness.timestamp
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

            if !self.ensure_capacity_for_new_ring(&partition_key) {
                continue;
            }

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
                    let window_start = boundary_before - self.config.interval_micros();
                    let task = ConsolidationTask::BoundaryCrossed {
                        table_id: self.table_id.clone(),
                        partition_key: partition_key.clone(),
                        window_start_ts: window_start,
                        window_end_ts: boundary_before,
                    };
                    if self.enqueue_materialization_task(&task) {
                        if let Some(ref m) = self.shared_metrics {
                            m.windows_consolidated.fetch_add(1, Ordering::Relaxed);
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
                    if self.enqueue_materialization_task(&task) {
                        if let Some(ref m) = self.shared_metrics {
                            m.late_arrivals.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                BoundaryStatus::Normal => {}
            }
        }

        Vec::new() // never return inline mutations
    }
}

/// Metrics for consolidation observability.
///
/// All counters use relaxed ordering since they are monotonic and
/// approximate counts are acceptable for observability.
#[derive(Debug, Default)]
pub struct ConsolidationMetrics {
    /// Total number of windows that have been consolidated.
    pub windows_consolidated: AtomicU64,
    /// Total number of late-arriving data points detected.
    pub late_arrivals: AtomicU64,
    /// Total number of consolidation tasks dropped (channel full).
    pub consolidation_drops: AtomicU64,
    /// Total number of failed numeric byte decodes during value extraction.
    pub decode_failures: AtomicU64,
    /// Total number of ring buffers evicted to stay within memory/count caps.
    pub ring_evictions: AtomicU64,
    /// Total number of times eviction volume crossed the configured thrash threshold.
    pub ring_thrash_warnings: AtomicU64,
    /// Total writes skipped because no ring could be allocated within budget.
    pub ring_budget_rejections: AtomicU64,
}

impl ConsolidationMetrics {
    /// Create a new metrics instance with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a point-in-time snapshot of all metric values.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            windows_consolidated: self.windows_consolidated.load(Ordering::Relaxed),
            late_arrivals: self.late_arrivals.load(Ordering::Relaxed),
            consolidation_drops: self.consolidation_drops.load(Ordering::Relaxed),
            decode_failures: self.decode_failures.load(Ordering::Relaxed),
            ring_evictions: self.ring_evictions.load(Ordering::Relaxed),
            ring_thrash_warnings: self.ring_thrash_warnings.load(Ordering::Relaxed),
            ring_budget_rejections: self.ring_budget_rejections.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of consolidation metrics for reporting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub windows_consolidated: u64,
    pub late_arrivals: u64,
    pub consolidation_drops: u64,
    pub decode_failures: u64,
    pub ring_evictions: u64,
    pub ring_thrash_warnings: u64,
    pub ring_budget_rejections: u64,
}

/// Async worker that processes consolidation tasks.
///
/// Owns the receiving end of the task channel. Executes consolidation functions
/// and writes results to downstream tables via `StorageEngine::write()`.
pub struct ConsolidationWorker {
    config: ConsolidationConfig,
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

    /// Returns a reference to the task receiver for polling.
    pub fn task_rx(&self) -> &std::sync::mpsc::Receiver<ConsolidationTask> {
        &self.task_rx
    }
}

impl TimeSeriesAggregator {
    /// Evict the coldest ring buffers when `ring_count > max_rings`.
    ///
    /// Returns the number of evicted entries. This is called by a background
    /// sweep, not on the write path.
    pub fn evict_cold_rings(&self) -> usize {
        self.evict_cold_rings_to_limit(self.effective_max_rings())
    }

    fn evict_cold_rings_to_limit(&self, max_rings: usize) -> usize {
        let current = self.rings.len();
        if current <= max_rings {
            return 0;
        }

        let to_evict = current - max_rings;

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

        self.record_ring_evictions(evicted);
        evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::SmallVec;

    fn make_double_mutation(partition_key: &str, ts: i64, val: f64) -> Mutation {
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        Mutation {
            mutation_id: [0x57u8; 16],
            keyspace: "ks".to_string(),
            table: "sensor".to_string(),
            key: DecoratedKey::new(PartitionKey::new(partition_key.as_bytes().to_vec())),
            rows: vec![Row {
                clustering: ts.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(val.to_be_bytes().to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
            timestamp: ts,
        }
    }

    #[test]
    fn consolidation_task_boundary_crossed() {
        let task = ConsolidationTask::BoundaryCrossed {
            table_id: TableId::new("ks", "sensor_1s"),
            partition_key: b"sensor-1".to_vec(),
            window_start_ts: 0,
            window_end_ts: 10_000_000,
        };

        if let ConsolidationTask::BoundaryCrossed {
            table_id,
            partition_key,
            window_start_ts,
            window_end_ts,
        } = &task
        {
            assert_eq!(table_id.keyspace, "ks");
            assert_eq!(table_id.table, "sensor_1s");
            assert_eq!(partition_key, b"sensor-1");
            assert_eq!(*window_start_ts, 0);
            assert_eq!(*window_end_ts, 10_000_000);
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
            mutation_id: [0x50u8; 16],
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
            mutation_id: [0x51u8; 16],
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
                window_end_ts,
                ..
            } => {
                assert_eq!(window_start_ts, 0);
                assert_eq!(window_end_ts, 10_000_000);
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

    #[test]
    fn aggregator_skips_ring_allocation_when_budget_cannot_fit_one_ring() {
        let metrics = Arc::new(ConsolidationMetrics::new());
        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ring_capacity: 64,
            max_rings: 100,
            ring_memory_budget_bytes: Some(1),
            ..ConsolidationConfig::default()
        };

        let (tx, _rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor");
        let aggregator =
            TimeSeriesAggregator::with_metrics(config, table_id.clone(), vec![0], tx, metrics);

        aggregator.on_write(&table_id, &make_double_mutation("pk1", 1_000_000, 1.0));

        assert_eq!(aggregator.ring_count(), 0);
        assert_eq!(
            aggregator
                .metrics()
                .unwrap()
                .ring_budget_rejections
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn aggregator_evicts_before_allocating_above_budget_capped_ring_limit() {
        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ring_capacity: 4,
            max_rings: 10,
            ring_memory_budget_bytes: Some(RingBuffer::estimated_heap_bytes(4, 1) * 2),
            ..ConsolidationConfig::default()
        };

        let (tx, _rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor");
        let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

        aggregator.on_write(&table_id, &make_double_mutation("pk1", 1_000_000, 1.0));
        aggregator.on_write(&table_id, &make_double_mutation("pk2", 1_000_000, 2.0));
        aggregator.on_write(&table_id, &make_double_mutation("pk3", 1_000_000, 3.0));

        assert_eq!(aggregator.ring_count(), 2);
        assert_eq!(aggregator.effective_max_rings(), 2);
    }

    #[test]
    fn aggregator_counts_and_warns_when_ring_evictions_thrash() {
        let metrics = Arc::new(ConsolidationMetrics::new());
        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ring_capacity: 4,
            max_rings: 1,
            ring_thrash_warn_evictions: 2,
            ..ConsolidationConfig::default()
        };

        let (tx, _rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor");
        let aggregator =
            TimeSeriesAggregator::with_metrics(config, table_id.clone(), vec![0], tx, metrics);

        aggregator.on_write(&table_id, &make_double_mutation("pk1", 1_000_000, 1.0));
        aggregator.on_write(&table_id, &make_double_mutation("pk2", 1_000_000, 2.0));
        aggregator.on_write(&table_id, &make_double_mutation("pk3", 1_000_000, 3.0));

        let snapshot = aggregator.metrics().unwrap().snapshot();
        assert_eq!(snapshot.ring_evictions, 2);
        assert_eq!(snapshot.ring_thrash_warnings, 1);
    }

    #[test]
    fn aggregator_runtime_settings_adjust_ring_budget_without_rebuild() {
        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ring_capacity: 4,
            max_rings: 10,
            ring_memory_budget_bytes: Some(RingBuffer::estimated_heap_bytes(4, 1)),
            ..ConsolidationConfig::default()
        };

        let (tx, _rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor");
        let aggregator = TimeSeriesAggregator::new(config, table_id, vec![0], tx);

        assert_eq!(aggregator.effective_max_rings(), 1);

        aggregator
            .runtime_settings()
            .set_ring_memory_budget_bytes(Some(RingBuffer::estimated_heap_bytes(4, 1) * 3));

        assert_eq!(aggregator.effective_max_rings(), 3);
    }

    #[test]
    fn aggregator_uses_shared_runtime_settings_handle() {
        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ring_capacity: 4,
            max_rings: 10,
            ..ConsolidationConfig::default()
        };
        let settings = Arc::new(TimeSeriesRuntimeSettings::new(
            Some(RingBuffer::estimated_heap_bytes(4, 1)),
            100,
        ));

        let (tx, _rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor");
        let aggregator = TimeSeriesAggregator::with_runtime_settings(
            config,
            table_id,
            vec![0],
            tx,
            Arc::clone(&settings),
        );

        assert_eq!(aggregator.effective_max_rings(), 1);

        settings.set_ring_memory_budget_bytes(Some(RingBuffer::estimated_heap_bytes(4, 1) * 4));

        assert_eq!(aggregator.effective_max_rings(), 4);
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
            mutation_id: [0x53u8; 16],
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
    fn aggregator_drop_count_tracks_disconnected_task_queue() {
        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "sensor_10s".to_string(),
            columns: vec!["value".to_string()],
            ring_capacity: 64,
            max_rings: 100,
            ..ConsolidationConfig::default()
        };

        let (tx, rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(1);
        drop(rx);
        let table_id = TableId::new("ks", "sensor_1s");
        let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let make_mutation = |ts: i64, val: f64| Mutation {
            mutation_id: [0x52u8; 16],
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

        // The boundary crossing should have tried to send and recorded the disconnected worker.
        assert!(aggregator.drop_count() >= 1);
    }

    #[test]
    fn aggregator_backpressures_instead_of_dropping_when_task_queue_is_full() {
        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "sensor_10s".to_string(),
            columns: vec!["value".to_string()],
            ring_capacity: 64,
            max_rings: 100,
            ..ConsolidationConfig::default()
        };

        let (tx, rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(1);
        let table_id = TableId::new("ks", "sensor_1s");
        let aggregator = std::sync::Arc::new(TimeSeriesAggregator::new(
            config,
            table_id.clone(),
            vec![0],
            tx,
        ));

        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let make_mutation = |ts: i64, val: f64| Mutation {
            mutation_id: [0x53u8; 16],
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

        aggregator.on_write(&table_id, &make_mutation(1_000_000, 1.0));
        aggregator.on_write(&table_id, &make_mutation(10_000_000, 2.0));
        let first = rx.try_recv().expect("first boundary task was enqueued");

        aggregator.on_write(&table_id, &make_mutation(20_000_000, 3.0));
        let _second = rx.try_recv().expect("second boundary task was enqueued");

        // Fill the channel with one pending task.
        aggregator.on_write(&table_id, &make_mutation(30_000_000, 4.0));

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker_aggregator = std::sync::Arc::clone(&aggregator);
        let worker_table = table_id.clone();
        let worker_mutation = make_mutation(40_000_000, 5.0);
        let handle = std::thread::spawn(move || {
            worker_aggregator.on_write(&worker_table, &worker_mutation);
            done_tx.send(()).unwrap();
        });

        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "full task queue should backpressure the write instead of dropping the rollup task"
        );

        assert!(matches!(
            first,
            ConsolidationTask::BoundaryCrossed {
                window_start_ts: 0,
                ..
            }
        ));
        let _third = rx.recv().expect("draining the queue unblocks sender");
        done_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("blocked write should complete after queue space is available");
        handle.join().unwrap();
        let _fourth = rx.recv().expect("blocked write enqueued its task");
        assert_eq!(
            aggregator.drop_count(),
            0,
            "backpressure must preserve rollup tasks instead of dropping them"
        );
    }

    // --- Task 22: ConsolidationMetrics tests ---

    #[test]
    fn consolidation_metrics_default() {
        let metrics = ConsolidationMetrics::default();
        assert_eq!(metrics.windows_consolidated.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.late_arrivals.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.consolidation_drops.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn consolidation_metrics_increment() {
        let metrics = ConsolidationMetrics::default();
        metrics.windows_consolidated.fetch_add(1, Ordering::Relaxed);
        metrics.late_arrivals.fetch_add(3, Ordering::Relaxed);
        assert_eq!(metrics.windows_consolidated.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.late_arrivals.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn consolidation_metrics_snapshot() {
        let metrics = ConsolidationMetrics::new();
        metrics.windows_consolidated.fetch_add(5, Ordering::Relaxed);
        metrics.late_arrivals.fetch_add(2, Ordering::Relaxed);
        metrics.consolidation_drops.fetch_add(1, Ordering::Relaxed);

        let snap = metrics.snapshot();
        assert_eq!(snap.windows_consolidated, 5);
        assert_eq!(snap.late_arrivals, 2);
        assert_eq!(snap.consolidation_drops, 1);
    }

    #[test]
    fn consolidation_metrics_concurrent_increments() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(ConsolidationMetrics::new());
        let mut handles = vec![];

        for _ in 0..4 {
            let m = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    m.windows_consolidated.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(metrics.windows_consolidated.load(Ordering::Relaxed), 400);
    }

    // --- FMEA Fix 2: Track decode failures in metrics ---

    #[test]
    fn metrics_track_decode_failures() {
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let metrics = Arc::new(ConsolidationMetrics::new());

        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["v".to_string()],
            ring_capacity: 64,
            max_rings: 100,
            ..ConsolidationConfig::default()
        };

        let (tx, _rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor");
        let aggregator = TimeSeriesAggregator::with_metrics(
            config,
            table_id.clone(),
            vec![0],
            tx,
            metrics.clone(),
        );

        // Send a mutation with non-decodable bytes (3 bytes -- not 4 or 8).
        let bad_bytes = vec![0xDE, 0xAD, 0xFF];
        let mutation = Mutation {
            mutation_id: [0x54u8; 16],
            keyspace: "ks".to_string(),
            table: "sensor".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: 1_000_000_i64.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(bad_bytes, 1_000_000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1_000_000),
            }],
            timestamp: 1_000_000,
        };

        aggregator.on_write(&table_id, &mutation);

        // decode_failures should have been incremented.
        assert_eq!(metrics.decode_failures.load(Ordering::Relaxed), 1);

        // Ring should NOT have been created (row was skipped).
        assert_eq!(aggregator.ring_count(), 0);
    }

    // --- FMEA Fix 5: Typed decode using column type metadata ---

    #[test]
    fn extract_values_uses_typed_decode() {
        use ferrosa_common::key::{DecoratedKey, PartitionKey};
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let config = ConsolidationConfig {
            interval: std::time::Duration::from_secs(10),
            functions: vec![super::super::consolidation::ConsolidationFn::Avg],
            target_table: "t".to_string(),
            columns: vec!["temp".to_string()],
            ring_capacity: 64,
            max_rings: 100,
            ..ConsolidationConfig::default()
        };

        let (tx, _rx) = std::sync::mpsc::sync_channel::<ConsolidationTask>(100);
        let table_id = TableId::new("ks", "sensor");

        // Create aggregator WITH column types -- "float" means 4-byte IEEE 754.
        let aggregator = TimeSeriesAggregator::with_column_types(
            config,
            table_id.clone(),
            vec![0],
            vec!["float".to_string()],
            tx,
        );

        // Encode a float32 value (42.5f32).
        let float_bytes = 42.5_f32.to_be_bytes().to_vec();
        let mutation = Mutation {
            mutation_id: [0x55u8; 16],
            keyspace: "ks".to_string(),
            table: "sensor".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: 1_000_000_i64.to_be_bytes().to_vec(),
                cells: vec![(0, CellValue::live(float_bytes, 1_000_000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1_000_000),
            }],
            timestamp: 1_000_000,
        };

        aggregator.on_write(&table_id, &mutation);

        // Ring should have been created -- typed decode of float succeeds.
        assert_eq!(aggregator.ring_count(), 1);

        // Verify the value was decoded correctly as float (not as i32).
        let ring = aggregator.rings.get(&b"pk1".to_vec()).unwrap();
        let entries = ring.window(0, 2_000_000);
        assert_eq!(entries.len(), 1);
        // float decode of 42.5f32 should be ~42.5 (not 1110179840 which is i32 interpretation).
        assert!(
            (entries[0].values[0] - 42.5).abs() < 0.01,
            "expected ~42.5, got {}",
            entries[0].values[0]
        );
    }
}
