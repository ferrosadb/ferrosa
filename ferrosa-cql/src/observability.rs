//! CQL request metrics — atomic counters for request tracking.
//!
//! [`CqlMetrics`] provides per-opcode request counters and a global error counter.
//! Counters are incremented in the router after each dispatch and can be read
//! through the `system_observability.cql_stats` virtual table.

use std::sync::atomic::{AtomicU64, Ordering};

/// Opcode indices for the per-opcode request counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum CqlOpcode {
    Select = 0,
    Insert = 1,
    Update = 2,
    Delete = 3,
    Batch = 4,
    Ddl = 5,
    Prepare = 6,
    Other = 7,
}

/// Number of distinct CQL opcode buckets.
const NUM_OPCODES: usize = 8;

/// Opcode label strings for the virtual table, indexed by discriminant.
const OPCODE_LABELS: [&str; NUM_OPCODES] = [
    "SELECT", "INSERT", "UPDATE", "DELETE", "BATCH", "DDL", "PREPARE", "OTHER",
];

/// Atomic counters for CQL request tracking.
///
/// All operations are lock-free (`Ordering::Relaxed`) — these are
/// monotonic counters where exact ordering between reads is not required.
pub struct CqlMetrics {
    /// Per-opcode request counter (indexed by [`CqlOpcode`] discriminant).
    pub requests: [AtomicU64; NUM_OPCODES],
    /// Global error counter (incremented on any CQL error response).
    pub errors: AtomicU64,
}

impl CqlMetrics {
    /// Create a new metrics instance with all counters at zero.
    pub fn new() -> Self {
        Self {
            requests: std::array::from_fn(|_| AtomicU64::new(0)),
            errors: AtomicU64::new(0),
        }
    }

    /// Increment the request counter for the given opcode.
    pub fn inc_request(&self, opcode: CqlOpcode) {
        self.requests[opcode as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the global error counter.
    pub fn inc_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Read the request count for the given opcode.
    pub fn request_count(&self, opcode: CqlOpcode) -> u64 {
        self.requests[opcode as usize].load(Ordering::Relaxed)
    }

    /// Read the error count.
    pub fn error_count(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    /// Return all opcode labels and counts as a snapshot.
    pub fn snapshot(&self) -> Vec<(&'static str, u64)> {
        OPCODE_LABELS
            .iter()
            .enumerate()
            .map(|(i, label)| (*label, self.requests[i].load(Ordering::Relaxed)))
            .collect()
    }
}

impl Default for CqlMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Virtual table: system_observability.cql_stats
// ---------------------------------------------------------------------------

use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};
use std::sync::Arc;

/// Virtual table exposing CQL request counters.
///
/// Each row is one opcode bucket with its request count. A final row with
/// opcode `ERRORS` holds the global error count.
pub struct CqlStatsTable {
    metrics: Arc<CqlMetrics>,
    columns: Vec<VirtualColumnDef>,
}

impl CqlStatsTable {
    pub fn new(metrics: Arc<CqlMetrics>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "opcode".into(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "count".into(),
                data_type: DataType::BigInt,
            },
        ];
        Self { metrics, columns }
    }
}

impl VirtualTable for CqlStatsTable {
    fn name(&self) -> &str {
        "cql_stats"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let mut rows: Vec<VirtualRow> = self
            .metrics
            .snapshot()
            .into_iter()
            .map(|(label, count)| VirtualRow {
                cells: vec![
                    CellValue::live(label.as_bytes().to_vec(), 0),
                    CellValue::live(count.to_be_bytes().to_vec(), 0),
                ],
            })
            .collect();

        // Append the global error row.
        rows.push(VirtualRow {
            cells: vec![
                CellValue::live(b"ERRORS".to_vec(), 0),
                CellValue::live(self.metrics.error_count().to_be_bytes().to_vec(), 0),
            ],
        });

        rows
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cql_metrics_increment_on_request() {
        let metrics = CqlMetrics::new();

        // All counters start at zero.
        assert_eq!(metrics.request_count(CqlOpcode::Select), 0);
        assert_eq!(metrics.error_count(), 0);

        // Increment SELECT twice.
        metrics.inc_request(CqlOpcode::Select);
        metrics.inc_request(CqlOpcode::Select);
        assert_eq!(metrics.request_count(CqlOpcode::Select), 2);

        // Increment INSERT once.
        metrics.inc_request(CqlOpcode::Insert);
        assert_eq!(metrics.request_count(CqlOpcode::Insert), 1);

        // Other opcodes remain zero.
        assert_eq!(metrics.request_count(CqlOpcode::Update), 0);
        assert_eq!(metrics.request_count(CqlOpcode::Delete), 0);

        // Error counter.
        metrics.inc_error();
        metrics.inc_error();
        metrics.inc_error();
        assert_eq!(metrics.error_count(), 3);

        // Snapshot returns all 8 opcode buckets.
        let snap = metrics.snapshot();
        assert_eq!(snap.len(), 8);
        assert_eq!(snap[0], ("SELECT", 2));
        assert_eq!(snap[1], ("INSERT", 1));
    }

    #[test]
    fn cql_stats_virtual_table_reads_counters() {
        let metrics = Arc::new(CqlMetrics::new());
        metrics.inc_request(CqlOpcode::Select);
        metrics.inc_request(CqlOpcode::Batch);
        metrics.inc_error();

        let table = CqlStatsTable::new(metrics);
        let rows = table.read(None);

        // 8 opcode rows + 1 ERRORS row = 9.
        assert_eq!(rows.len(), 9);

        // Verify the SELECT row has opcode label as text and count as bigint bytes.
        assert_eq!(rows[0].cells[0], CellValue::live(b"SELECT".to_vec(), 0));
        assert_eq!(
            rows[0].cells[1],
            CellValue::live(1u64.to_be_bytes().to_vec(), 0)
        );

        // Verify the BATCH row (index 4).
        assert_eq!(rows[4].cells[0], CellValue::live(b"BATCH".to_vec(), 0));
        assert_eq!(
            rows[4].cells[1],
            CellValue::live(1u64.to_be_bytes().to_vec(), 0)
        );

        // Verify ERRORS row (last).
        assert_eq!(rows[8].cells[0], CellValue::live(b"ERRORS".to_vec(), 0));
        assert_eq!(
            rows[8].cells[1],
            CellValue::live(1u64.to_be_bytes().to_vec(), 0)
        );
    }
}
