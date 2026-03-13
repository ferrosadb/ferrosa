//! Virtual tables backed by storage engine internals.
//!
//! [`StorageStatsTable`] exposes per-(keyspace, table) storage statistics as a
//! virtual table queryable through the CQL native protocol under
//! `system_observability.storage_stats`.
//!
//! The actual wiring to [`StorageEngine`] is deferred behind the
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
/// [`StorageEngine`] will implement this trait once its internal metrics are
/// exposed. Tests use [`MockStatsProvider`].
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
}
