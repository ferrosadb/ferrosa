//! Virtual tables backed by storage engine internals.
//!
//! [`StorageStatsTable`] exposes per-(keyspace, table) storage statistics as a
//! virtual table queryable through the CQL native protocol under
//! `system_observability.storage_stats`.
//!
//! The actual wiring to `StorageEngine` is deferred behind the
//! [`StorageStatsProvider`] trait so that this module compiles and tests pass
//! before the engine exposes metrics.

use std::sync::Arc;

use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

// ---------------------------------------------------------------------------
// Public data types
// ---------------------------------------------------------------------------

/// Snapshot of storage statistics for a single (keyspace, table) pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageStats {
    pub keyspace: String,
    pub table_name: String,
    pub memtable_size_bytes: i64,
    pub memtable_count: i32,
    pub sstable_count: i32,
    pub sstable_size_bytes: i64,
    pub s3_object_count: i32,
    pub s3_bytes: i64,
    pub pending_compactions: i32,
}

// ---------------------------------------------------------------------------
// Provider trait
// ---------------------------------------------------------------------------

/// A source of [`StorageStats`] data.
///
/// `StorageEngine` will implement this trait once its internal metrics are
/// exposed. Tests use `MockStatsProvider`.
pub trait StorageStatsProvider: Send + Sync {
    /// Collect current storage statistics for all (keyspace, table) pairs.
    fn collect_stats(&self) -> Vec<StorageStats>;
}

// ---------------------------------------------------------------------------
// Virtual table implementation
// ---------------------------------------------------------------------------

/// Virtual table: `system_observability.storage_stats`
///
/// Exposes per-(keyspace, table) storage statistics pulled from a
/// [`StorageStatsProvider`] on every `read()`.
pub struct StorageStatsTable {
    provider: Arc<dyn StorageStatsProvider>,
    columns: Vec<VirtualColumnDef>,
}

impl StorageStatsTable {
    /// Create a new `StorageStatsTable` backed by `provider`.
    pub fn new(provider: Arc<dyn StorageStatsProvider>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "keyspace".into(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "table_name".into(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "memtable_size_bytes".into(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "memtable_count".into(),
                data_type: DataType::Int,
            },
            VirtualColumnDef {
                name: "sstable_count".into(),
                data_type: DataType::Int,
            },
            VirtualColumnDef {
                name: "sstable_size_bytes".into(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "s3_object_count".into(),
                data_type: DataType::Int,
            },
            VirtualColumnDef {
                name: "s3_bytes".into(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "pending_compactions".into(),
                data_type: DataType::Int,
            },
        ];
        Self { provider, columns }
    }
}

impl VirtualTable for StorageStatsTable {
    fn name(&self) -> &str {
        "storage_stats"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    /// Primary key: (keyspace, table_name) — indices 0 and 1.
    fn primary_key_columns(&self) -> &[usize] {
        &[0, 1]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        self.provider
            .collect_stats()
            .into_iter()
            .map(|s| {
                let cells = vec![
                    CellValue::live(s.keyspace.into_bytes(), 0),
                    CellValue::live(s.table_name.into_bytes(), 0),
                    CellValue::live(s.memtable_size_bytes.to_be_bytes().to_vec(), 0),
                    CellValue::live(s.memtable_count.to_be_bytes().to_vec(), 0),
                    CellValue::live(s.sstable_count.to_be_bytes().to_vec(), 0),
                    CellValue::live(s.sstable_size_bytes.to_be_bytes().to_vec(), 0),
                    CellValue::live(s.s3_object_count.to_be_bytes().to_vec(), 0),
                    CellValue::live(s.s3_bytes.to_be_bytes().to_vec(), 0),
                    CellValue::live(s.pending_compactions.to_be_bytes().to_vec(), 0),
                ];
                VirtualRow { cells }
            })
            .collect()
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }
}

// ===========================================================================
// ArchiveStatusTable — Task 4.3
// ===========================================================================

// ---------------------------------------------------------------------------
// Public data types
// ---------------------------------------------------------------------------

/// A point-in-time snapshot of commit-log archive status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveStatusRow {
    /// Number of commit-log segments not yet archived to S3.
    pub unarchived_segments: i64,
    /// Age in seconds of the oldest unarchived segment (0 when none pending).
    pub oldest_unarchived_age_secs: i64,
    /// ISO 8601 timestamp of the most recent successful archive operation.
    pub last_archive_success: String,
    /// Cumulative count of archive errors since node start.
    pub archive_errors_total: i64,
}

// ---------------------------------------------------------------------------
// Provider trait
// ---------------------------------------------------------------------------

/// Source of [`ArchiveStatusRow`] data.
///
/// `CommitLogArchiver` will implement this trait once it exposes its metrics.
/// Tests use a mock implementation.
pub trait ArchiveStatusProvider: Send + Sync {
    /// Return current archive status.
    fn archive_status(&self) -> ArchiveStatusRow;
}

// ---------------------------------------------------------------------------
// Virtual table implementation
// ---------------------------------------------------------------------------

/// Virtual table: `system_observability.archive_status`
///
/// Single-row table showing the current state of the commit-log archiver.
pub struct ArchiveStatusTable {
    provider: Arc<dyn ArchiveStatusProvider>,
    columns: Vec<VirtualColumnDef>,
}

impl ArchiveStatusTable {
    /// Create a new `ArchiveStatusTable` backed by `provider`.
    pub fn new(provider: Arc<dyn ArchiveStatusProvider>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "unarchived_segments".into(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "oldest_unarchived_age_secs".into(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "last_archive_success".into(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "archive_errors_total".into(),
                data_type: DataType::BigInt,
            },
        ];
        Self { provider, columns }
    }
}

impl VirtualTable for ArchiveStatusTable {
    fn name(&self) -> &str {
        "archive_status"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    /// Primary key: single synthetic row; no natural key — use index 0.
    fn primary_key_columns(&self) -> &[usize] {
        &[0]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let s = self.provider.archive_status();
        let cells = vec![
            CellValue::live(s.unarchived_segments.to_be_bytes().to_vec(), 0),
            CellValue::live(s.oldest_unarchived_age_secs.to_be_bytes().to_vec(), 0),
            CellValue::live(s.last_archive_success.into_bytes(), 0),
            CellValue::live(s.archive_errors_total.to_be_bytes().to_vec(), 0),
        ];
        vec![VirtualRow { cells }]
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }
}

// ===========================================================================
// SnapshotsTable — Task 4.4
// ===========================================================================

// ---------------------------------------------------------------------------
// Public data types
// ---------------------------------------------------------------------------

/// A single snapshot record returned by [`SnapshotsTable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfoRow {
    /// Human-readable snapshot name.
    pub name: String,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
    /// ISO 8601 expiry timestamp, or `None` for permanent snapshots.
    pub expires_at: Option<String>,
    /// Commit-log segment ID at which the snapshot was taken.
    pub commit_log_segment: i64,
    /// Byte offset within `commit_log_segment` at which the snapshot was taken.
    pub commit_log_offset: i64,
    /// Node identifier that created the snapshot.
    pub node_id: String,
}

// ---------------------------------------------------------------------------
// Provider trait
// ---------------------------------------------------------------------------

/// Source of [`SnapshotInfoRow`] data.
///
/// The snapshot subsystem will implement this trait. Tests use a mock.
pub trait SnapshotInfoProvider: Send + Sync {
    /// Return all known snapshots for this node.
    fn snapshot_info(&self) -> Vec<SnapshotInfoRow>;
}

// ---------------------------------------------------------------------------
// Virtual table implementation
// ---------------------------------------------------------------------------

/// Virtual table: `system_observability.snapshots`
///
/// Enumerates all point-in-time snapshots tracked by this node.
pub struct SnapshotsTable {
    provider: Arc<dyn SnapshotInfoProvider>,
    columns: Vec<VirtualColumnDef>,
}

impl SnapshotsTable {
    /// Create a new `SnapshotsTable` backed by `provider`.
    pub fn new(provider: Arc<dyn SnapshotInfoProvider>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "name".into(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "created_at".into(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "expires_at".into(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "commit_log_segment".into(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "commit_log_offset".into(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "node_id".into(),
                data_type: DataType::Text,
            },
        ];
        Self { provider, columns }
    }
}

impl VirtualTable for SnapshotsTable {
    fn name(&self) -> &str {
        "snapshots"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    /// Primary key: snapshot name (index 0).
    fn primary_key_columns(&self) -> &[usize] {
        &[0]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        self.provider
            .snapshot_info()
            .into_iter()
            .map(|s| {
                let expires_at_bytes = match s.expires_at {
                    Some(ts) => ts.into_bytes(),
                    None => Vec::new(),
                };
                let cells = vec![
                    CellValue::live(s.name.into_bytes(), 0),
                    CellValue::live(s.created_at.into_bytes(), 0),
                    CellValue::live(expires_at_bytes, 0),
                    CellValue::live(s.commit_log_segment.to_be_bytes().to_vec(), 0),
                    CellValue::live(s.commit_log_offset.to_be_bytes().to_vec(), 0),
                    CellValue::live(s.node_id.into_bytes(), 0),
                ];
                VirtualRow { cells }
            })
            .collect()
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

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    struct MockStatsProvider {
        data: Vec<StorageStats>,
    }

    impl MockStatsProvider {
        fn new(data: Vec<StorageStats>) -> Arc<Self> {
            Arc::new(Self { data })
        }
    }

    impl StorageStatsProvider for MockStatsProvider {
        fn collect_stats(&self) -> Vec<StorageStats> {
            self.data.clone()
        }
    }

    fn sample_stats(ks: &str, tbl: &str) -> StorageStats {
        StorageStats {
            keyspace: ks.into(),
            table_name: tbl.into(),
            memtable_size_bytes: 1024,
            memtable_count: 2,
            sstable_count: 5,
            sstable_size_bytes: 65536,
            s3_object_count: 3,
            s3_bytes: 131072,
            pending_compactions: 1,
        }
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn storage_stats_table_metadata() {
        let table = StorageStatsTable::new(MockStatsProvider::new(vec![]));

        assert_eq!(table.name(), "storage_stats");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.columns().len(), 9);

        let names: Vec<&str> = table.columns().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            &[
                "keyspace",
                "table_name",
                "memtable_size_bytes",
                "memtable_count",
                "sstable_count",
                "sstable_size_bytes",
                "s3_object_count",
                "s3_bytes",
                "pending_compactions",
            ]
        );

        // Primary key columns: keyspace (0) and table_name (1)
        assert_eq!(table.primary_key_columns(), &[0, 1]);
    }

    #[test]
    fn storage_stats_returns_provider_data() {
        let stats = vec![sample_stats("ks_a", "tbl_1"), sample_stats("ks_b", "tbl_2")];
        let table = StorageStatsTable::new(MockStatsProvider::new(stats));

        let rows = table.read(None);
        assert_eq!(rows.len(), 2);

        // Each row has exactly 9 cells.
        for row in &rows {
            assert_eq!(row.cells.len(), 9);
        }

        // Spot-check first row: keyspace bytes
        let ks_bytes = rows[0].cells[0].value.as_deref().unwrap();
        assert_eq!(ks_bytes, b"ks_a");

        // Spot-check second row: table_name bytes
        let tbl_bytes = rows[1].cells[1].value.as_deref().unwrap();
        assert_eq!(tbl_bytes, b"tbl_2");

        // Spot-check memtable_size_bytes encoding (big-endian i64 = 1024)
        let size_bytes = rows[0].cells[2].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(size_bytes.try_into().unwrap()), 1024i64);
    }

    #[test]
    fn storage_stats_is_pollable() {
        let table = StorageStatsTable::new(MockStatsProvider::new(vec![]));
        assert!(matches!(
            table.subscription_mode(),
            SubscriptionMode::Pollable
        ));
    }

    // -----------------------------------------------------------------------
    // ArchiveStatusTable helpers and tests
    // -----------------------------------------------------------------------

    struct MockArchiveStatusProvider {
        row: ArchiveStatusRow,
    }

    impl MockArchiveStatusProvider {
        fn new(row: ArchiveStatusRow) -> Arc<Self> {
            Arc::new(Self { row })
        }
    }

    impl ArchiveStatusProvider for MockArchiveStatusProvider {
        fn archive_status(&self) -> ArchiveStatusRow {
            self.row.clone()
        }
    }

    fn sample_archive_status() -> ArchiveStatusRow {
        ArchiveStatusRow {
            unarchived_segments: 3,
            oldest_unarchived_age_secs: 120,
            last_archive_success: "2026-03-19T00:00:00Z".into(),
            archive_errors_total: 7,
        }
    }

    #[test]
    fn archive_status_table_metadata() {
        let table =
            ArchiveStatusTable::new(MockArchiveStatusProvider::new(sample_archive_status()));

        assert_eq!(table.name(), "archive_status");
        assert_eq!(table.keyspace(), "system_observability");
        assert!(table
            .columns()
            .iter()
            .any(|c| c.name == "unarchived_segments"));
        assert!(table
            .columns()
            .iter()
            .any(|c| c.name == "oldest_unarchived_age_secs"));
        assert!(table
            .columns()
            .iter()
            .any(|c| c.name == "last_archive_success"));
        assert!(table
            .columns()
            .iter()
            .any(|c| c.name == "archive_errors_total"));
        assert_eq!(table.columns().len(), 4);
    }

    #[test]
    fn archive_status_returns_single_row() {
        let status = sample_archive_status();
        let table = ArchiveStatusTable::new(MockArchiveStatusProvider::new(status));

        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cells.len(), 4);

        // unarchived_segments == 3
        let seg_bytes = rows[0].cells[0].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(seg_bytes.try_into().unwrap()), 3i64);

        // oldest_unarchived_age_secs == 120
        let age_bytes = rows[0].cells[1].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(age_bytes.try_into().unwrap()), 120i64);

        // last_archive_success as UTF-8 text
        let ts_bytes = rows[0].cells[2].value.as_deref().unwrap();
        assert_eq!(ts_bytes, b"2026-03-19T00:00:00Z");

        // archive_errors_total == 7
        let err_bytes = rows[0].cells[3].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(err_bytes.try_into().unwrap()), 7i64);
    }

    #[test]
    fn archive_status_is_pollable() {
        let table =
            ArchiveStatusTable::new(MockArchiveStatusProvider::new(sample_archive_status()));
        assert!(matches!(
            table.subscription_mode(),
            SubscriptionMode::Pollable
        ));
    }

    // -----------------------------------------------------------------------
    // SnapshotsTable helpers and tests
    // -----------------------------------------------------------------------

    struct MockSnapshotInfoProvider {
        rows: Vec<SnapshotInfoRow>,
    }

    impl MockSnapshotInfoProvider {
        fn new(rows: Vec<SnapshotInfoRow>) -> Arc<Self> {
            Arc::new(Self { rows })
        }
    }

    impl SnapshotInfoProvider for MockSnapshotInfoProvider {
        fn snapshot_info(&self) -> Vec<SnapshotInfoRow> {
            self.rows.clone()
        }
    }

    fn sample_snapshot(name: &str, expires: bool) -> SnapshotInfoRow {
        SnapshotInfoRow {
            name: name.into(),
            created_at: "2026-03-19T01:00:00Z".into(),
            expires_at: if expires {
                Some("2026-04-19T01:00:00Z".into())
            } else {
                None
            },
            commit_log_segment: 42,
            commit_log_offset: 4096,
            node_id: "node-1".into(),
        }
    }

    #[test]
    fn snapshot_info_table_metadata() {
        let table = SnapshotsTable::new(MockSnapshotInfoProvider::new(vec![]));

        assert_eq!(table.name(), "snapshots");
        assert_eq!(table.keyspace(), "system_observability");
        assert!(table.columns().iter().any(|c| c.name == "name"));
        assert!(table.columns().iter().any(|c| c.name == "created_at"));
        assert!(table.columns().iter().any(|c| c.name == "expires_at"));
        assert!(table
            .columns()
            .iter()
            .any(|c| c.name == "commit_log_segment"));
        assert!(table
            .columns()
            .iter()
            .any(|c| c.name == "commit_log_offset"));
        assert!(table.columns().iter().any(|c| c.name == "node_id"));
        assert_eq!(table.columns().len(), 6);
    }

    #[test]
    fn snapshot_info_returns_provider_rows() {
        let snaps = vec![
            sample_snapshot("snap-a", true),
            sample_snapshot("snap-b", false),
        ];
        let table = SnapshotsTable::new(MockSnapshotInfoProvider::new(snaps));

        let rows = table.read(None);
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row.cells.len(), 6);
        }

        // First row: name == "snap-a"
        let name_bytes = rows[0].cells[0].value.as_deref().unwrap();
        assert_eq!(name_bytes, b"snap-a");

        // First row: expires_at present
        let exp_bytes = rows[0].cells[2].value.as_deref().unwrap();
        assert_eq!(exp_bytes, b"2026-04-19T01:00:00Z");

        // Second row: expires_at absent — encoded as empty bytes
        let exp_bytes2 = rows[1].cells[2].value.as_deref().unwrap();
        assert_eq!(exp_bytes2, b"");

        // Spot-check commit_log_segment == 42
        let seg_bytes = rows[0].cells[3].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(seg_bytes.try_into().unwrap()), 42i64);

        // Spot-check node_id
        let node_bytes = rows[1].cells[5].value.as_deref().unwrap();
        assert_eq!(node_bytes, b"node-1");
    }

    #[test]
    fn snapshots_is_pollable() {
        let table = SnapshotsTable::new(MockSnapshotInfoProvider::new(vec![]));
        assert!(matches!(
            table.subscription_mode(),
            SubscriptionMode::Pollable
        ));
    }
}
