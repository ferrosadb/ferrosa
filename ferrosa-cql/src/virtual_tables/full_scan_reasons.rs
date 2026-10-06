//! Full scan reasons virtual table (T-33: O5.6).
//!
//! When the query planner chooses `ScanPlan::FullScan`, the predicate
//! column and operator are recorded here. This helps operators identify
//! queries that would benefit from secondary indexes.
//!
//! Virtual table: `system_observability.full_scan_reasons`

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
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

/// The predicate key the scan warning deduplicates on. Kept identical to the
/// key the reason entries above are matched on, so "first scan of a plan" means
/// the same thing in the log as in `system_observability.full_scan_reasons`.
fn scan_warn_key(
    keyspace: &str,
    table_name: &str,
    predicate_column: &str,
    operator: &str,
) -> String {
    format!("{keyspace}|{table_name}|{predicate_column}|{operator}")
}

/// Tracker for full scan occurrences.
///
/// It owns BOTH the alertable signal and the log-dedup state, because they key
/// on the same tuple (`keyspace, table_name, predicate_column, operator`):
/// keeping them in one struct means "first scan of this plan" cannot come to
/// mean something different in the log than in
/// `system_observability.full_scan_reasons`.
pub struct FullScanTracker {
    reasons: RwLock<Vec<FullScanReasonEntry>>,
    total_full_scans: AtomicU64,
    /// Plans whose scan WARN has already been emitted on this node (DT-16).
    /// See [`FullScanTracker::scan_warn_is_first`].
    warn_seen: Mutex<HashSet<String>>,
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
            warn_seen: Mutex::new(HashSet::new()),
        }
    }

    /// Record a full scan event.
    ///
    /// Counts the scan unconditionally, whatever the caller decides to log —
    /// this is the alertable surface (`system_observability.full_scan_reasons`).
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

    /// Whether this scan is the FIRST of its `(keyspace, table_name,
    /// predicate_column, operator)` plan on this node since startup (DT-16):
    /// `true` means report it at WARN, `false` means the plan was already warned
    /// about and this repeat belongs at DEBUG.
    ///
    /// The dedup lives here rather than in the router because it keys on exactly
    /// the tuple the reason entries above are matched on, so "first scan of a
    /// plan" cannot come to mean something different in the log than in
    /// `system_observability.full_scan_reasons`.
    ///
    /// This demotes the LOG LINE ONLY. It does not touch `record` or
    /// `total_full_scans`, so a monitor on the virtual table still sees every
    /// scan. It is a separate call because the router decides the log line
    /// before the record site (and an ANN-served plan returns before that site),
    /// so it cannot be folded into `record`'s return value without mis-keying.
    ///
    /// Poison-tolerant: a panic elsewhere while holding this lock must not turn
    /// a log line into a panic here, so a poisoned lock is recovered.
    pub fn scan_warn_is_first(
        &self,
        keyspace: &str,
        table_name: &str,
        predicate_column: &str,
        operator: &str,
    ) -> bool {
        let key = scan_warn_key(keyspace, table_name, predicate_column, operator);
        let mut seen = self
            .warn_seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        seen.insert(key)
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

    fn visit_rows(&self, _predicate: Option<&RowPredicate>, visit: &mut dyn FnMut(VirtualRow)) {
        let reasons = self
            .tracker
            .reasons
            .read()
            .expect("FullScanTracker lock poisoned");
        for e in reasons.iter() {
            let cells = vec![
                CellValue::live(e.keyspace.as_bytes().to_vec(), 0),
                CellValue::live(e.table_name.as_bytes().to_vec(), 0),
                CellValue::live(e.predicate_column.as_bytes().to_vec(), 0),
                CellValue::live(e.operator.as_bytes().to_vec(), 0),
                CellValue::live((e.count as i64).to_be_bytes().to_vec(), 0),
                CellValue::live(e.last_seen_ms.to_be_bytes().to_vec(), 0),
            ];
            visit(VirtualRow { cells });
        }
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

    /// DT-16 at the unit level: the scan-WARN verdict is "once per plan", and
    /// asking for it does NOT touch the alertable count. The end-to-end router
    /// test proves the WARN/DEBUG split; this pins the contract the router
    /// depends on, including that a repeat of one plan is `false` and a novel
    /// plan is `true` again.
    #[test]
    fn scan_warn_is_first_is_once_per_plan_and_never_touches_the_count() {
        let tracker = FullScanTracker::new();
        assert!(
            tracker.scan_warn_is_first("ks", "t", "body", "="),
            "the first scan of a plan must warn"
        );
        for _ in 0..24 {
            assert!(
                !tracker.scan_warn_is_first("ks", "t", "body", "="),
                "a repeat of the same plan must NOT warn"
            );
        }
        // A different operator, column, table or keyspace is a different plan.
        assert!(tracker.scan_warn_is_first("ks", "t", "body", ">"));
        assert!(tracker.scan_warn_is_first("ks", "t", "label", "="));
        assert!(tracker.scan_warn_is_first("ks", "other", "body", "="));
        assert!(tracker.scan_warn_is_first("other", "t", "body", "="));

        // Asking whether to warn must not itself record a scan: the count is
        // owned by `record`, which the router calls unconditionally.
        assert_eq!(
            tracker.total_full_scans(),
            0,
            "scan_warn_is_first must not count a scan — the log line is demoted, \
             the signal is not"
        );
    }
}
