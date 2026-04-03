//! Full scan reasons virtual table (T-33: O5.6).
//!
//! When the query planner chooses `ScanPlan::FullScan`, the predicate
//! column and operator are recorded here. This helps operators identify
//! queries that would benefit from secondary indexes.
//!
//! Virtual table: `system_observability.full_scan_reasons`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Maximum number of distinct full scan reasons to track.
const MAX_REASONS: usize = 1_000;

/// A recorded reason for a full scan.
#[derive(Debug, Clone)]
pub struct FullScanReason {
    /// The table that was full-scanned.
    pub keyspace: String,
    pub table_name: String,
    /// The predicate column that triggered the full scan (if any).
    pub predicate_column: String,
    /// The comparison operator used.
    pub operator: String,
    /// Number of times this reason was seen.
    pub count: u64,
    /// Last occurrence (epoch millis).
    pub last_seen_ms: i64,
}

/// Tracker for full scan occurrences.
pub struct FullScanTracker {
    reasons: RwLock<Vec<FullScanReasonEntry>>,
    total_full_scans: AtomicU64,
}

struct FullScanReasonEntry {
    keyspace: String,
    table_name: String,
    predicate_column: String,
    operator: String,
    count: u64,
    last_seen_ms: i64,
}

impl FullScanTracker {
    /// Create a new empty tracker.
    pub fn new() -> Self {
        Self {
            reasons: RwLock::new(Vec::new()),
            total_full_scans: AtomicU64::new(0),
        }
    }

    /// Record a full scan event.
    pub fn record(&self, keyspace: &str, table_name: &str, predicate_column: &str, operator: &str) {
        self.total_full_scans.fetch_add(1, Ordering::Relaxed);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let mut reasons = self.reasons.write().expect("FullScanTracker lock poisoned");

        // Check for an existing entry.
        for entry in reasons.iter_mut() {
            if entry.keyspace == keyspace
                && entry.table_name == table_name
                && entry.predicate_column == predicate_column
                && entry.operator == operator
            {
                entry.count += 1;
                entry.last_seen_ms = now_ms;
                return;
            }
        }

        // Evict oldest if at capacity.
        if reasons.len() >= MAX_REASONS {
            // Remove the entry with the oldest last_seen_ms.
            if let Some(oldest_idx) = reasons
                .iter()
                .enumerate()
                .min_by_key(|(_, e)| e.last_seen_ms)
                .map(|(i, _)| i)
            {
                reasons.swap_remove(oldest_idx);
            }
        }

        reasons.push(FullScanReasonEntry {
            keyspace: keyspace.to_string(),
            table_name: table_name.to_string(),
            predicate_column: predicate_column.to_string(),
            operator: operator.to_string(),
            count: 1,
            last_seen_ms: now_ms,
        });
    }

    /// Total full scans recorded since startup.
    pub fn total_full_scans(&self) -> u64 {
        self.total_full_scans.load(Ordering::Relaxed)
    }

    /// Number of distinct reasons tracked.
    pub fn reason_count(&self) -> usize {
        self.reasons
            .read()
            .expect("FullScanTracker lock poisoned")
            .len()
    }
}

impl Default for FullScanTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Virtual table: `system_observability.full_scan_reasons`
pub struct FullScanReasonsTable {
    tracker: Arc<FullScanTracker>,
    columns: Vec<VirtualColumnDef>,
}

impl FullScanReasonsTable {
    pub fn new(tracker: Arc<FullScanTracker>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "keyspace".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "table_name".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "predicate_column".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "operator".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "count".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "last_seen_ms".to_string(),
                data_type: DataType::BigInt,
            },
        ];
        Self { tracker, columns }
    }
}

impl VirtualTable for FullScanReasonsTable {
    fn name(&self) -> &str {
        "full_scan_reasons"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0, 1, 2]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let reasons = self
            .tracker
            .reasons
            .read()
            .expect("FullScanTracker lock poisoned");
        reasons
            .iter()
            .map(|e| {
                let cells = vec![
                    CellValue::live(e.keyspace.as_bytes().to_vec(), 0),
                    CellValue::live(e.table_name.as_bytes().to_vec(), 0),
                    CellValue::live(e.predicate_column.as_bytes().to_vec(), 0),
                    CellValue::live(e.operator.as_bytes().to_vec(), 0),
                    CellValue::live((e.count as i64).to_be_bytes().to_vec(), 0),
                    CellValue::live(e.last_seen_ms.to_be_bytes().to_vec(), 0),
                ];
                VirtualRow { cells }
            })
            .collect()
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_scan_tracker_records_and_deduplicates() {
        let tracker = Arc::new(FullScanTracker::new());
        tracker.record("myks", "users", "email", "=");
        tracker.record("myks", "users", "email", "=");
        assert_eq!(tracker.reason_count(), 1);
        assert_eq!(tracker.total_full_scans(), 2);

        let table = FullScanReasonsTable::new(tracker);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        // Count should be 2
        let count_bytes = rows[0].cells[4].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(count_bytes.try_into().unwrap()), 2);
    }

    #[test]
    fn full_scan_different_predicates_are_separate() {
        let tracker = FullScanTracker::new();
        tracker.record("ks", "t", "col_a", "=");
        tracker.record("ks", "t", "col_b", ">");
        assert_eq!(tracker.reason_count(), 2);
    }

    #[test]
    fn full_scan_table_metadata() {
        let tracker = Arc::new(FullScanTracker::new());
        let table = FullScanReasonsTable::new(tracker);
        assert_eq!(table.name(), "full_scan_reasons");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.columns().len(), 6);
    }
}
