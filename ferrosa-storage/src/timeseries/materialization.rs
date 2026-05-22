//! Materialization queue descriptors and target-row encoding for consolidated windows.
//!
//! Materialization requests describe a window to stream and never carry the
//! window's values. Workers must stream source rows through the consolidation
//! accumulator and pass the bounded function results into mutation encoding.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError, TrySendError};
use std::time::Duration;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use crate::commitlog::config::TableId;
use crate::commitlog::mutation::Mutation;

use super::consolidation::ConsolidationFn;

/// Target metadata needed to encode a consolidated window mutation.
#[derive(Debug, Clone, PartialEq)]
pub struct MaterializationTarget {
    pub source_table: TableId,
    pub target_table: TableId,
    pub interval: Duration,
    pub source_columns: Vec<String>,
    pub functions: Vec<ConsolidationFn>,
}

impl MaterializationTarget {
    /// Classify a candidate window against a current watermark and `late_window`.
    ///
    /// `Fresh` windows are at or ahead of the watermark. `Stale` windows are
    /// behind the watermark but still inside `late_window`. Older windows are
    /// classified as `Drop`.
    pub fn classify_late_window(
        &self,
        window_start_ts: i64,
        watermark_window_start_ts: i64,
        late_window: Duration,
    ) -> LateWindowClassification {
        if window_start_ts >= watermark_window_start_ts {
            return LateWindowClassification::Fresh;
        }

        let late_by = watermark_window_start_ts.saturating_sub(window_start_ts);
        if late_by <= late_window.as_micros() as i64 {
            LateWindowClassification::Stale
        } else {
            LateWindowClassification::Drop
        }
    }
}

/// Result of comparing a window against a target's late-data horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LateWindowClassification {
    Fresh,
    Stale,
    Drop,
}

/// Why a materialization task was enqueued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializationTaskKind {
    FreshBoundary,
    LateDataRecalculation,
}

impl MaterializationTaskKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FreshBoundary => "fresh_boundary",
            Self::LateDataRecalculation => "late_data_recalculation",
        }
    }
}

/// A queued request to materialize one target rollup row.
///
/// This is a descriptor only. It deliberately does not contain source values;
/// the worker must stream the configured window from a ring or storage cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct MaterializationRequest {
    pub target: MaterializationTarget,
    pub partition_key: Vec<u8>,
    pub window_start_ts: i64,
    pub window_end_ts: i64,
    pub kind: MaterializationTaskKind,
    pub retry_count: u32,
}

impl MaterializationRequest {
    /// Convert this descriptor into the target-row encoder.
    pub fn into_rollup(self) -> MaterializedRollup {
        MaterializedRollup {
            target: self.target,
            partition_key: self.partition_key,
            window_start_ts: self.window_start_ts,
        }
    }
}

/// A rollup row encoder before it is applied to storage.
#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedRollup {
    pub target: MaterializationTarget,
    pub partition_key: Vec<u8>,
    pub window_start_ts: i64,
}

impl MaterializedRollup {
    /// Encode the rollup as a mutation against the configured target table.
    ///
    /// Result cells are written as big-endian `double` bytes in function order.
    /// The only collection here is the bounded row cell set required by the
    /// storage `Row` API; source-window values must never be materialized.
    pub fn encode_mutation_from_results<I>(&self, results: I) -> Mutation
    where
        I: IntoIterator<Item = f64>,
    {
        self.encode_mutation_from_results_at(results, self.window_start_ts)
    }

    /// Encode the rollup using a write timestamp separate from the window key.
    ///
    /// Re-aggregation for late data must overwrite the previous rollup row.
    /// The clustering key stays at `window_start_ts`, but cell/liveness
    /// timestamps use the recomputation event timestamp.
    pub fn encode_mutation_from_results_at<I>(&self, results: I, write_timestamp: i64) -> Mutation
    where
        I: IntoIterator<Item = f64>,
    {
        let cells = results
            .into_iter()
            .enumerate()
            .map(|(idx, value)| {
                (
                    idx as u16,
                    CellValue::live(value.to_be_bytes().to_vec(), write_timestamp),
                )
            })
            .collect();

        Mutation {
            mutation_id: materialization_mutation_id(
                &self.target.target_table,
                &self.partition_key,
                self.window_start_ts,
            ),
            keyspace: self.target.target_table.keyspace.clone(),
            table: self.target.target_table.table.clone(),
            key: DecoratedKey::new(PartitionKey::new(self.partition_key.clone())),
            rows: vec![Row {
                clustering: self.window_start_ts.to_be_bytes().to_vec(),
                cells,
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(write_timestamp),
            }],
            timestamp: write_timestamp,
        }
    }
}

/// Bounded streaming queue for materialization descriptors.
pub struct MaterializationQueue {
    tx: SyncSender<MaterializationRequest>,
    rx: Receiver<MaterializationRequest>,
    metrics: MaterializationQueueMetrics,
}

impl MaterializationQueue {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = sync_channel(capacity);
        Self {
            tx,
            rx,
            metrics: MaterializationQueueMetrics::new(),
        }
    }

    pub fn enqueue(
        &self,
        request: MaterializationRequest,
    ) -> Result<(), Box<MaterializationRequest>> {
        match self.tx.try_send(request) {
            Ok(()) => {
                self.metrics.enqueued.fetch_add(1, Ordering::Relaxed);
                self.metrics.pending.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(request)) | Err(TrySendError::Disconnected(request)) => {
                self.metrics.dropped_full.fetch_add(1, Ordering::Relaxed);
                Err(Box::new(request))
            }
        }
    }

    pub fn drain_next(&self) -> Option<MaterializationRequest> {
        match self.rx.try_recv() {
            Ok(request) => {
                self.metrics.drained.fetch_add(1, Ordering::Relaxed);
                self.metrics.pending.fetch_sub(1, Ordering::Relaxed);
                Some(request)
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => None,
        }
    }

    pub fn len(&self) -> usize {
        self.metrics.pending.load(Ordering::Relaxed) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn metrics(&self) -> &MaterializationQueueMetrics {
        &self.metrics
    }
}

/// Atomic queue counters for observability.
#[derive(Debug, Default)]
pub struct MaterializationQueueMetrics {
    pub enqueued: AtomicU64,
    pub drained: AtomicU64,
    pub dropped_full: AtomicU64,
    pub dropped_stale: AtomicU64,
    pub pending: AtomicU64,
}

impl MaterializationQueueMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> MaterializationQueueSnapshot {
        MaterializationQueueSnapshot {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            drained: self.drained.load(Ordering::Relaxed),
            dropped_full: self.dropped_full.load(Ordering::Relaxed),
            dropped_stale: self.dropped_stale.load(Ordering::Relaxed),
            pending: self.pending.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time materialization queue metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializationQueueSnapshot {
    pub enqueued: u64,
    pub drained: u64,
    pub dropped_full: u64,
    pub dropped_stale: u64,
    pub pending: u64,
}

fn materialization_mutation_id(
    table_id: &TableId,
    partition_key: &[u8],
    window_start_ts: i64,
) -> [u8; 16] {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in table_id
        .keyspace
        .as_bytes()
        .iter()
        .chain(table_id.table.as_bytes())
        .chain(partition_key)
        .chain(window_start_ts.to_be_bytes().iter())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    let mut id = [0_u8; 16];
    id[..8].copy_from_slice(&hash.to_be_bytes());
    id[8..].copy_from_slice(&window_start_ts.to_be_bytes());
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_descriptor_converts_to_rollup_without_source_values() {
        let target = MaterializationTarget {
            source_table: TableId::new("ks", "sensor"),
            target_table: TableId::new("ks", "sensor_10s"),
            interval: Duration::from_secs(10),
            source_columns: vec!["value".to_string()],
            functions: vec![ConsolidationFn::Avg],
        };
        let request = MaterializationRequest {
            target: target.clone(),
            partition_key: b"sensor-1".to_vec(),
            window_start_ts: 0,
            window_end_ts: 10_000_000,
            kind: MaterializationTaskKind::FreshBoundary,
            retry_count: 0,
        };

        let rollup = request.into_rollup();

        assert_eq!(rollup.target, target);
        assert_eq!(rollup.partition_key, b"sensor-1");
        assert_eq!(rollup.window_start_ts, 0);
    }
}
