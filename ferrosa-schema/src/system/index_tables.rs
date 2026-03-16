//! `system_schema.indexes` virtual table.
//!
//! Provides a virtual table that exposes secondary index metadata from
//! the schema snapshot. Compatible with Cassandra's `system_schema.indexes`
//! table layout for CQL driver introspection.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_common::{CellValue, DataType};
use ferrosa_index::IndexType;

use crate::registry::SchemaSnapshot;
use crate::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Virtual table implementation for `system_schema.indexes`.
///
/// Reads the current schema snapshot's indexes map and materializes
/// rows on demand. The snapshot is shared via `Arc<ArcSwap<SchemaSnapshot>>`
/// for lock-free reads.
pub struct SystemSchemaIndexesTable {
    snapshot: Arc<ArcSwap<SchemaSnapshot>>,
    columns: Vec<VirtualColumnDef>,
}

impl SystemSchemaIndexesTable {
    /// Create a new `system_schema.indexes` virtual table backed by the
    /// given snapshot handle.
    pub fn new(snapshot: Arc<ArcSwap<SchemaSnapshot>>) -> Self {
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
                name: "kind".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "target".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "options".to_string(),
                data_type: DataType::Text,
            },
        ];
        Self { snapshot, columns }
    }
}

/// Map an `IndexType` to a human-readable kind string.
fn index_type_kind(index_type: &IndexType) -> &'static str {
    match index_type {
        IndexType::BTree => "btree",
        IndexType::Hash => "hash",
        IndexType::Composite => "composite",
        IndexType::Phonetic => "phonetic",
        IndexType::Filtered => "filtered",
    }
}

/// Format target columns for display.
fn format_target(target_columns: &[String], index_type: &IndexType) -> String {
    match index_type {
        IndexType::Composite => {
            format!("({})", target_columns.join(", "))
        }
        _ => target_columns.join(", "),
    }
}

impl VirtualTable for SystemSchemaIndexesTable {
    fn name(&self) -> &str {
        "indexes"
    }

    fn keyspace(&self) -> &str {
        "system_schema"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        // keyspace_name (0), table_name (1), index_name (2)
        &[0, 1, 2]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let snap = self.snapshot.load_full();
        let mut rows = Vec::new();

        for ((ks, tbl, name), index) in &snap.indexes {
            let kind = index_type_kind(&index.index_type).to_string();
            let target = format_target(&index.target_columns, &index.index_type);
            let options_json =
                serde_json::to_string(&index.options).unwrap_or_else(|_| "{}".to_string());

            rows.push(VirtualRow {
                cells: vec![
                    CellValue::live(ks.as_bytes().to_vec(), 0),
                    CellValue::live(tbl.as_bytes().to_vec(), 0),
                    CellValue::live(name.as_bytes().to_vec(), 0),
                    CellValue::live(kind.into_bytes(), 0),
                    CellValue::live(target.into_bytes(), 0),
                    CellValue::live(options_json.into_bytes(), 0),
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
    use crate::metadata::index::IndexMetadata;
    use ferrosa_index::IndexType;
    use std::collections::HashMap;

    fn test_snapshot_with_index() -> Arc<ArcSwap<SchemaSnapshot>> {
        let mut snap = SchemaSnapshot::new();
        snap.indexes.insert(
            ("ks1".into(), "tbl1".into(), "idx_email".into()),
            IndexMetadata {
                keyspace: "ks1".into(),
                table: "tbl1".into(),
                name: "idx_email".into(),
                index_type: IndexType::BTree,
                target_columns: vec!["email".into()],
                filter_predicate: None,
                options: HashMap::new(),
            },
        );
        Arc::new(ArcSwap::new(Arc::new(snap)))
    }

    #[test]
    fn indexes_table_columns() {
        let snap = Arc::new(ArcSwap::new(Arc::new(SchemaSnapshot::new())));
        let table = SystemSchemaIndexesTable::new(snap);
        let cols = table.columns();
        assert_eq!(cols.len(), 6);
        assert_eq!(cols[0].name, "keyspace_name");
        assert_eq!(cols[1].name, "table_name");
        assert_eq!(cols[2].name, "index_name");
        assert_eq!(cols[3].name, "kind");
        assert_eq!(cols[4].name, "target");
        assert_eq!(cols[5].name, "options");
    }

    #[test]
    fn indexes_table_returns_rows() {
        let snap = test_snapshot_with_index();
        let table = SystemSchemaIndexesTable::new(snap);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(row.cells.len(), 6);
        // Check keyspace_name cell
        assert_eq!(row.cells[0].value.as_deref(), Some(b"ks1".as_slice()));
        // Check table_name cell
        assert_eq!(row.cells[1].value.as_deref(), Some(b"tbl1".as_slice()));
        // Check index_name cell
        assert_eq!(row.cells[2].value.as_deref(), Some(b"idx_email".as_slice()));
        // Check kind cell
        assert_eq!(row.cells[3].value.as_deref(), Some(b"btree".as_slice()));
        // Check target cell
        assert_eq!(row.cells[4].value.as_deref(), Some(b"email".as_slice()));
    }

    #[test]
    fn indexes_table_metadata() {
        let snap = Arc::new(ArcSwap::new(Arc::new(SchemaSnapshot::new())));
        let table = SystemSchemaIndexesTable::new(snap);
        assert_eq!(table.name(), "indexes");
        assert_eq!(table.keyspace(), "system_schema");
        assert_eq!(table.primary_key_columns(), &[0, 1, 2]);
        assert!(matches!(table.subscription_mode(), SubscriptionMode::None));
    }

    #[test]
    fn indexes_table_composite_target_format() {
        let mut snap = SchemaSnapshot::new();
        snap.indexes.insert(
            ("ks1".into(), "tbl1".into(), "idx_composite".into()),
            IndexMetadata {
                keyspace: "ks1".into(),
                table: "tbl1".into(),
                name: "idx_composite".into(),
                index_type: IndexType::Composite,
                target_columns: vec!["a".into(), "b".into()],
                filter_predicate: None,
                options: HashMap::new(),
            },
        );
        let handle = Arc::new(ArcSwap::new(Arc::new(snap)));
        let table = SystemSchemaIndexesTable::new(handle);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        // Composite target should be formatted as (a, b)
        assert_eq!(
            rows[0].cells[4].value.as_deref(),
            Some(b"(a, b)".as_slice())
        );
        // Kind should be "composite"
        assert_eq!(
            rows[0].cells[3].value.as_deref(),
            Some(b"composite".as_slice())
        );
    }

    #[test]
    fn indexes_table_empty_snapshot() {
        let snap = Arc::new(ArcSwap::new(Arc::new(SchemaSnapshot::new())));
        let table = SystemSchemaIndexesTable::new(snap);
        let rows = table.read(None);
        assert!(rows.is_empty());
    }

    #[test]
    fn index_type_kind_hash() {
        assert_eq!(index_type_kind(&IndexType::Hash), "hash");
    }

    #[test]
    fn index_type_kind_phonetic() {
        assert_eq!(index_type_kind(&IndexType::Phonetic), "phonetic");
    }

    #[test]
    fn index_type_kind_filtered() {
        assert_eq!(index_type_kind(&IndexType::Filtered), "filtered");
    }

    #[test]
    fn indexes_table_hash_kind_in_row() {
        let mut snap = SchemaSnapshot::new();
        snap.indexes.insert(
            ("ks1".into(), "tbl1".into(), "idx_hash".into()),
            IndexMetadata {
                keyspace: "ks1".into(),
                table: "tbl1".into(),
                name: "idx_hash".into(),
                index_type: IndexType::Hash,
                target_columns: vec!["user_id".into()],
                filter_predicate: None,
                options: HashMap::new(),
            },
        );
        let handle = Arc::new(ArcSwap::new(Arc::new(snap)));
        let table = SystemSchemaIndexesTable::new(handle);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cells[3].value.as_deref(), Some(b"hash".as_slice()));
    }

    #[test]
    fn indexes_table_phonetic_kind_in_row() {
        let mut snap = SchemaSnapshot::new();
        snap.indexes.insert(
            ("ks1".into(), "tbl1".into(), "idx_phonetic".into()),
            IndexMetadata {
                keyspace: "ks1".into(),
                table: "tbl1".into(),
                name: "idx_phonetic".into(),
                index_type: IndexType::Phonetic,
                target_columns: vec!["last_name".into()],
                filter_predicate: None,
                options: HashMap::new(),
            },
        );
        let handle = Arc::new(ArcSwap::new(Arc::new(snap)));
        let table = SystemSchemaIndexesTable::new(handle);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].cells[3].value.as_deref(),
            Some(b"phonetic".as_slice())
        );
    }

    #[test]
    fn indexes_table_filtered_kind_in_row() {
        use ferrosa_index::FilterOp;
        use ferrosa_index::FilterPredicate;
        let mut snap = SchemaSnapshot::new();
        snap.indexes.insert(
            ("ks1".into(), "tbl1".into(), "idx_filtered".into()),
            IndexMetadata {
                keyspace: "ks1".into(),
                table: "tbl1".into(),
                name: "idx_filtered".into(),
                index_type: IndexType::Filtered,
                target_columns: vec!["status".into()],
                filter_predicate: Some(FilterPredicate {
                    column_position: 0,
                    op: FilterOp::Eq,
                    value: b"active".to_vec(),
                }),
                options: HashMap::new(),
            },
        );
        let handle = Arc::new(ArcSwap::new(Arc::new(snap)));
        let table = SystemSchemaIndexesTable::new(handle);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].cells[3].value.as_deref(),
            Some(b"filtered".as_slice())
        );
    }
}
