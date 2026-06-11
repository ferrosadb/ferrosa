//! Index usage virtual table (Phase 3: index observability).
//!
//! When the query planner chooses an index-backed `ScanPlan`
//! (`SingleIndex`, `IndexScanWithFilter`, or `IndexIntersection`) and the
//! router consults that index, the index name and plan kind are recorded
//! here. This mirrors `full_scan_reasons.rs` but for the positive case: it
//! proves that secondary indexes are actually being used, and which ones.
//!
//! Virtual table: `system_observability.index_usage`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Maximum number of distinct index-usage entries to track.
const MAX_ENTRIES: usize = 1_000;

/// Tracker for secondary-index usage occurrences.
pub struct IndexUsageTracker {
    entries: RwLock<Vec<IndexUsageEntry>>,
    total_index_hits: AtomicU64,
}

struct IndexUsageEntry {
    keyspace: String,
    table_name: String,
    index_name: String,
    /// The plan kind that consulted the index (e.g. `SingleIndex`,
    /// `IndexScanWithFilter`, `IndexIntersection`).
    plan_kind: String,
    count: u64,
    last_seen_ms: i64,
}

impl IndexUsageTracker {
    /// Create a new empty tracker.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            total_index_hits: AtomicU64::new(0),
        }
    }

    /// Record an index-usage event.
    pub fn record(&self, keyspace: &str, table_name: &str, index_name: &str, plan_kind: &str) {
        self.total_index_hits.fetch_add(1, Ordering::Relaxed);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let mut entries = self
            .entries
            .write()
            .expect("IndexUsageTracker lock poisoned");

        for entry in entries.iter_mut() {
            if entry.keyspace == keyspace
                && entry.table_name == table_name
                && entry.index_name == index_name
                && entry.plan_kind == plan_kind
            {
                entry.count += 1;
                entry.last_seen_ms = now_ms;
                return;
            }
        }

        if entries.len() >= MAX_ENTRIES {
            if let Some(oldest_idx) = entries
                .iter()
                .enumerate()
                .min_by_key(|(_, e)| e.last_seen_ms)
                .map(|(i, _)| i)
            {
                entries.swap_remove(oldest_idx);
            }
        }

        entries.push(IndexUsageEntry {
            keyspace: keyspace.to_string(),
            table_name: table_name.to_string(),
            index_name: index_name.to_string(),
            plan_kind: plan_kind.to_string(),
            count: 1,
            last_seen_ms: now_ms,
        });
    }

    /// Total index hits recorded since startup.
    pub fn total_index_hits(&self) -> u64 {
        self.total_index_hits.load(Ordering::Relaxed)
    }

    /// Number of distinct index-usage entries tracked.
    pub fn entry_count(&self) -> usize {
        self.entries
            .read()
            .expect("IndexUsageTracker lock poisoned")
            .len()
    }
}

impl Default for IndexUsageTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Virtual table: `system_observability.index_usage`
pub struct IndexUsageTable {
    tracker: Arc<IndexUsageTracker>,
    columns: Vec<VirtualColumnDef>,
}

impl IndexUsageTable {
    pub fn new(tracker: Arc<IndexUsageTracker>) -> Self {
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
                name: "index_name".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "plan_kind".to_string(),
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

impl VirtualTable for IndexUsageTable {
    fn name(&self) -> &str {
        "index_usage"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0, 1, 2, 3]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let entries = self
            .tracker
            .entries
            .read()
            .expect("IndexUsageTracker lock poisoned");
        entries
            .iter()
            .map(|e| {
                let cells = vec![
                    CellValue::live(e.keyspace.as_bytes().to_vec(), 0),
                    CellValue::live(e.table_name.as_bytes().to_vec(), 0),
                    CellValue::live(e.index_name.as_bytes().to_vec(), 0),
                    CellValue::live(e.plan_kind.as_bytes().to_vec(), 0),
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
    fn index_usage_tracker_records_and_deduplicates() {
        let tracker = Arc::new(IndexUsageTracker::new());
        tracker.record("myks", "users", "idx_email", "SingleIndex");
        tracker.record("myks", "users", "idx_email", "SingleIndex");
        assert_eq!(tracker.entry_count(), 1);
        assert_eq!(tracker.total_index_hits(), 2);

        let table = IndexUsageTable::new(tracker);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        let count_bytes = rows[0].cells[4].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(count_bytes.try_into().unwrap()), 2);
    }

    #[test]
    fn index_usage_different_indexes_are_separate() {
        let tracker = IndexUsageTracker::new();
        tracker.record("ks", "t", "idx_a", "SingleIndex");
        tracker.record("ks", "t", "idx_b", "IndexIntersection");
        assert_eq!(tracker.entry_count(), 2);
    }

    #[test]
    fn index_usage_table_metadata() {
        let tracker = Arc::new(IndexUsageTracker::new());
        let table = IndexUsageTable::new(tracker);
        assert_eq!(table.name(), "index_usage");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.columns().len(), 6);
    }
}
