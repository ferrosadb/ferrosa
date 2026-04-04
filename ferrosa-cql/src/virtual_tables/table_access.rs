//! Table access summary virtual table (T-34: O5.7).
//!
//! Per-table atomic counters: reads, writes, point_lookups, range_scans,
//! full_scans. Virtual table `system_observability.table_access_summary`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Atomic counters for a single table.
pub struct TableAccessCounters {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub point_lookups: AtomicU64,
    pub range_scans: AtomicU64,
    pub full_scans: AtomicU64,
}

impl TableAccessCounters {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            point_lookups: AtomicU64::new(0),
            range_scans: AtomicU64::new(0),
            full_scans: AtomicU64::new(0),
        }
    }
}

/// Key for the access tracker: (keyspace, table_name).
type TableKey = (String, String);

/// Concurrent tracker for per-table access statistics.
pub struct TableAccessTracker {
    counters: DashMap<TableKey, Arc<TableAccessCounters>>,
}

impl TableAccessTracker {
    /// Create a new empty tracker.
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }

    /// Get or create counters for a table.
    fn get_counters(&self, keyspace: &str, table: &str) -> Arc<TableAccessCounters> {
        let key = (keyspace.to_string(), table.to_string());
        self.counters
            .entry(key)
            .or_insert_with(|| Arc::new(TableAccessCounters::new()))
            .clone()
    }

    /// Record a read operation.
    pub fn record_read(&self, keyspace: &str, table: &str) {
        self.get_counters(keyspace, table)
            .reads
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write operation.
    pub fn record_write(&self, keyspace: &str, table: &str) {
        self.get_counters(keyspace, table)
            .writes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a point lookup.
    pub fn record_point_lookup(&self, keyspace: &str, table: &str) {
        self.get_counters(keyspace, table)
            .point_lookups
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a range scan.
    pub fn record_range_scan(&self, keyspace: &str, table: &str) {
        self.get_counters(keyspace, table)
            .range_scans
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a full scan.
    pub fn record_full_scan(&self, keyspace: &str, table: &str) {
        self.get_counters(keyspace, table)
            .full_scans
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Number of tables being tracked.
    pub fn table_count(&self) -> usize {
        self.counters.len()
    }
}

impl Default for TableAccessTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Virtual table: `system_observability.table_access_summary`
pub struct TableAccessSummaryTable {
    tracker: Arc<TableAccessTracker>,
    columns: Vec<VirtualColumnDef>,
}

impl TableAccessSummaryTable {
    pub fn new(tracker: Arc<TableAccessTracker>) -> Self {
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
                name: "reads".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "writes".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "point_lookups".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "range_scans".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "full_scans".to_string(),
                data_type: DataType::BigInt,
            },
        ];
        Self { tracker, columns }
    }
}

impl VirtualTable for TableAccessSummaryTable {
    fn name(&self) -> &str {
        "table_access_summary"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0, 1]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        self.tracker
            .counters
            .iter()
            .map(|entry| {
                let (ks, tbl) = entry.key();
                let c = entry.value();
                let cells = vec![
                    CellValue::live(ks.as_bytes().to_vec(), 0),
                    CellValue::live(tbl.as_bytes().to_vec(), 0),
                    CellValue::live(
                        (c.reads.load(Ordering::Relaxed) as i64)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
                    CellValue::live(
                        (c.writes.load(Ordering::Relaxed) as i64)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
                    CellValue::live(
                        (c.point_lookups.load(Ordering::Relaxed) as i64)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
                    CellValue::live(
                        (c.range_scans.load(Ordering::Relaxed) as i64)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
                    CellValue::live(
                        (c.full_scans.load(Ordering::Relaxed) as i64)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
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
    fn table_access_tracks_operations() {
        let tracker = Arc::new(TableAccessTracker::new());
        tracker.record_read("myks", "users");
        tracker.record_read("myks", "users");
        tracker.record_write("myks", "users");
        tracker.record_point_lookup("myks", "users");
        tracker.record_range_scan("myks", "users");
        tracker.record_full_scan("myks", "users");
        assert_eq!(tracker.table_count(), 1);

        let table = TableAccessSummaryTable::new(tracker);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        // reads = 2
        let reads_bytes = rows[0].cells[2].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(reads_bytes.try_into().unwrap()), 2);
        // writes = 1
        let writes_bytes = rows[0].cells[3].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(writes_bytes.try_into().unwrap()), 1);
    }

    #[test]
    fn table_access_multiple_tables() {
        let tracker = TableAccessTracker::new();
        tracker.record_read("ks1", "t1");
        tracker.record_read("ks2", "t2");
        assert_eq!(tracker.table_count(), 2);
    }

    #[test]
    fn table_access_summary_metadata() {
        let tracker = Arc::new(TableAccessTracker::new());
        let table = TableAccessSummaryTable::new(tracker);
        assert_eq!(table.name(), "table_access_summary");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.columns().len(), 7);
    }
}
