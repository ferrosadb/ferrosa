//! `system_views.secondary_indexes` virtual table.
//!
//! Provides operational metrics for every tracked secondary index, exposing
//! build status, staleness lag, pending work, and build statistics. This is
//! the runtime counterpart to `system_schema.indexes` — schema metadata
//! lives there, while live operational state lives here.

use std::sync::Arc;

use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

use super::tracker::{IndexStateTracker, IndexStatus};

/// Virtual table exposing per-index operational metrics from the
/// [`IndexStateTracker`].
///
/// Registered in the `system_views` keyspace as `secondary_indexes`.
pub struct SecondaryIndexesVirtualTable {
    tracker: Arc<IndexStateTracker>,
    columns: Vec<VirtualColumnDef>,
}

impl SecondaryIndexesVirtualTable {
    /// Create a new virtual table backed by the given tracker.
    pub fn new(tracker: Arc<IndexStateTracker>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "keyspace_name".to_string(),
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
                name: "index_type".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "status".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "indexed_sstable_count".to_string(),
                data_type: DataType::Int,
            },
            VirtualColumnDef {
                name: "pending_sstable_count".to_string(),
                data_type: DataType::Int,
            },
            VirtualColumnDef {
                name: "pending_bytes".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "lag_seconds".to_string(),
                data_type: DataType::Double,
            },
            VirtualColumnDef {
                name: "last_build_ms".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "total_builds".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "build_errors".to_string(),
                data_type: DataType::BigInt,
            },
        ];
        Self { tracker, columns }
    }
}

/// Map an [`IndexStatus`] to its string label for the `status` column.
fn status_label(status: &IndexStatus) -> &'static str {
    match status {
        IndexStatus::Current => "current",
        IndexStatus::Building => "building",
        IndexStatus::Stale { .. } => "stale",
        IndexStatus::Failed { .. } => "failed",
    }
}

/// Compute the staleness lag in seconds from an [`IndexStatus`].
fn lag_seconds(status: &IndexStatus) -> f64 {
    match status {
        IndexStatus::Stale { lag, .. } => lag.as_secs_f64(),
        _ => 0.0,
    }
}

impl VirtualTable for SecondaryIndexesVirtualTable {
    fn name(&self) -> &str {
        "secondary_indexes"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        // keyspace_name (0), table_name (1), index_name (2)
        &[0, 1, 2]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let states = self.tracker.all_states();
        let mut rows = Vec::with_capacity(states.len());

        for state in &states {
            let (keyspace, table) = &state.table;
            let status_text = status_label(&state.status);
            let lag = lag_seconds(&state.status);
            let last_build_ms: i64 = state
                .last_build_duration
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);

            rows.push(VirtualRow {
                cells: vec![
                    // keyspace_name (Text)
                    CellValue::live(keyspace.as_bytes().to_vec(), 0),
                    // table_name (Text)
                    CellValue::live(table.as_bytes().to_vec(), 0),
                    // index_name (Text)
                    CellValue::live(state.index_name.as_bytes().to_vec(), 0),
                    // index_type (Text) — derived from IndexState; tracker
                    // does not store type metadata, so use "secondary".
                    CellValue::live(b"secondary".to_vec(), 0),
                    // status (Text)
                    CellValue::live(status_text.as_bytes().to_vec(), 0),
                    // indexed_sstable_count (Int)
                    CellValue::live(
                        (state.indexed_sstables.len() as i32).to_be_bytes().to_vec(),
                        0,
                    ),
                    // pending_sstable_count (Int)
                    CellValue::live(
                        (state.pending_sstables.len() as i32).to_be_bytes().to_vec(),
                        0,
                    ),
                    // pending_bytes (Bigint)
                    CellValue::live((state.pending_bytes as i64).to_be_bytes().to_vec(), 0),
                    // lag_seconds (Double)
                    CellValue::live(lag.to_be_bytes().to_vec(), 0),
                    // last_build_ms (Bigint)
                    CellValue::live(last_build_ms.to_be_bytes().to_vec(), 0),
                    // total_builds (Bigint)
                    CellValue::live((state.total_builds as i64).to_be_bytes().to_vec(), 0),
                    // build_errors (Bigint)
                    CellValue::live((state.total_build_errors as i64).to_be_bytes().to_vec(), 0),
                ],
            });
        }

        rows
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_match_expected_names_and_count() {
        let tracker = Arc::new(IndexStateTracker::new());
        let table = SecondaryIndexesVirtualTable::new(tracker);
        let cols = table.columns();

        assert_eq!(cols.len(), 12);
        assert_eq!(cols[0].name, "keyspace_name");
        assert_eq!(cols[1].name, "table_name");
        assert_eq!(cols[2].name, "index_name");
        assert_eq!(cols[3].name, "index_type");
        assert_eq!(cols[4].name, "status");
        assert_eq!(cols[5].name, "indexed_sstable_count");
        assert_eq!(cols[6].name, "pending_sstable_count");
        assert_eq!(cols[7].name, "pending_bytes");
        assert_eq!(cols[8].name, "lag_seconds");
        assert_eq!(cols[9].name, "last_build_ms");
        assert_eq!(cols[10].name, "total_builds");
        assert_eq!(cols[11].name, "build_errors");

        // Verify data types.
        assert_eq!(cols[0].data_type, DataType::Text);
        assert_eq!(cols[5].data_type, DataType::Int);
        assert_eq!(cols[7].data_type, DataType::BigInt);
        assert_eq!(cols[8].data_type, DataType::Double);
    }

    #[test]
    fn empty_tracker_returns_no_rows() {
        let tracker = Arc::new(IndexStateTracker::new());
        let table = SecondaryIndexesVirtualTable::new(tracker);
        let rows = table.read(None);
        assert!(rows.is_empty());
    }

    #[test]
    fn tracker_with_registered_index_returns_correct_row() {
        let tracker = Arc::new(IndexStateTracker::new());
        tracker.register_index("my_ks", "my_tbl", "idx_email");

        let table = SecondaryIndexesVirtualTable::new(Arc::clone(&tracker));
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(row.cells.len(), 12);

        // keyspace_name
        assert_eq!(row.cells[0].value.as_deref(), Some(b"my_ks".as_slice()));
        // table_name
        assert_eq!(row.cells[1].value.as_deref(), Some(b"my_tbl".as_slice()));
        // index_name
        assert_eq!(row.cells[2].value.as_deref(), Some(b"idx_email".as_slice()));
        // index_type
        assert_eq!(row.cells[3].value.as_deref(), Some(b"secondary".as_slice()));
        // status — freshly registered index is "current"
        assert_eq!(row.cells[4].value.as_deref(), Some(b"current".as_slice()));
        // indexed_sstable_count = 0
        assert_eq!(
            row.cells[5].value.as_deref(),
            Some(0i32.to_be_bytes().as_slice())
        );
        // pending_sstable_count = 0
        assert_eq!(
            row.cells[6].value.as_deref(),
            Some(0i32.to_be_bytes().as_slice())
        );
        // pending_bytes = 0
        assert_eq!(
            row.cells[7].value.as_deref(),
            Some(0i64.to_be_bytes().as_slice())
        );
        // total_builds = 0
        assert_eq!(
            row.cells[10].value.as_deref(),
            Some(0i64.to_be_bytes().as_slice())
        );
        // build_errors = 0
        assert_eq!(
            row.cells[11].value.as_deref(),
            Some(0i64.to_be_bytes().as_slice())
        );
    }

    #[test]
    fn stale_index_shows_pending_metrics() {
        let tracker = Arc::new(IndexStateTracker::new());
        tracker.register_index("ks", "tbl", "idx");
        tracker.mark_pending("ks", "tbl", "idx", "sst-001", 4096);
        tracker.mark_pending("ks", "tbl", "idx", "sst-002", 2048);

        let table = SecondaryIndexesVirtualTable::new(Arc::clone(&tracker));
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        // status should be "stale"
        assert_eq!(row.cells[4].value.as_deref(), Some(b"stale".as_slice()));
        // pending_sstable_count = 2
        assert_eq!(
            row.cells[6].value.as_deref(),
            Some(2i32.to_be_bytes().as_slice())
        );
        // pending_bytes = 6144
        assert_eq!(
            row.cells[7].value.as_deref(),
            Some(6144i64.to_be_bytes().as_slice())
        );
    }

    #[test]
    fn table_metadata() {
        let tracker = Arc::new(IndexStateTracker::new());
        let table = SecondaryIndexesVirtualTable::new(tracker);
        assert_eq!(table.name(), "secondary_indexes");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.primary_key_columns(), &[0, 1, 2]);
        assert!(matches!(table.subscription_mode(), SubscriptionMode::None));
    }
}
