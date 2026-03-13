//! `system_schema` query functions.
//!
//! Provides row types and query functions for `system_schema.keyspaces`,
//! `system_schema.tables`, and `system_schema.columns`.

use std::collections::HashMap;

use uuid::Uuid;

use crate::metadata::column::ColumnKind;
use crate::registry::SchemaSnapshot;

/// A row from `system_schema.keyspaces`.
#[derive(Debug, Clone)]
pub struct KeyspaceRow {
    /// Keyspace name.
    pub keyspace_name: String,
    /// Whether durable writes are enabled.
    pub durable_writes: bool,
    /// Replication strategy and options as a map.
    pub replication: HashMap<String, String>,
}

/// A row from `system_schema.tables`.
#[derive(Debug, Clone)]
pub struct TableRow {
    /// Keyspace this table belongs to.
    pub keyspace_name: String,
    /// Table name.
    pub table_name: String,
    /// Unique table identifier.
    pub id: Uuid,
}

/// A row from `system_schema.columns`.
#[derive(Debug, Clone)]
pub struct ColumnRow {
    /// Keyspace this column's table belongs to.
    pub keyspace_name: String,
    /// Table this column belongs to.
    pub table_name: String,
    /// Column name.
    pub column_name: String,
    /// Column kind as a string: "partition_key", "clustering", "regular", "static".
    pub kind: String,
    /// Position within its kind group.
    pub position: i32,
    /// CQL type name.
    pub column_type: String,
    /// Clustering order: "asc", "desc", or "none".
    pub clustering_order: String,
}

/// Query `system_schema.keyspaces` from a snapshot.
pub fn query_keyspaces(snap: &SchemaSnapshot) -> Vec<KeyspaceRow> {
    snap.keyspaces
        .values()
        .map(|ks| {
            let mut replication = ks.replication.options.clone();
            replication.insert("class".to_string(), ks.replication.strategy.clone());
            KeyspaceRow {
                keyspace_name: ks.name.clone(),
                durable_writes: ks.durable_writes,
                replication,
            }
        })
        .collect()
}

/// Query `system_schema.tables` from a snapshot.
pub fn query_tables(snap: &SchemaSnapshot) -> Vec<TableRow> {
    snap.tables
        .values()
        .map(|t| TableRow {
            keyspace_name: t.keyspace.clone(),
            table_name: t.name.clone(),
            id: t.id,
        })
        .collect()
}

/// Query `system_schema.columns` from a snapshot.
pub fn query_columns(snap: &SchemaSnapshot) -> Vec<ColumnRow> {
    snap.tables
        .values()
        .flat_map(|t| {
            t.columns.values().map(move |c| ColumnRow {
                keyspace_name: t.keyspace.clone(),
                table_name: t.name.clone(),
                column_name: c.name.clone(),
                kind: match c.kind {
                    ColumnKind::PartitionKey => "partition_key".to_string(),
                    ColumnKind::Clustering => "clustering".to_string(),
                    ColumnKind::Regular => "regular".to_string(),
                    ColumnKind::Static => "static".to_string(),
                },
                position: c.position,
                column_type: c.column_type.clone(),
                clustering_order: match c.clustering_order {
                    crate::metadata::column::ClusteringOrder::Asc => "asc".to_string(),
                    crate::metadata::column::ClusteringOrder::Desc => "desc".to_string(),
                    crate::metadata::column::ClusteringOrder::None => "none".to_string(),
                },
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::column::{ClusteringOrder, ColumnMetadata};
    use crate::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use crate::metadata::table::{TableMetadata, TableParams};
    use std::collections::HashSet;

    fn test_keyspace_meta(name: &str) -> KeyspaceMetadata {
        KeyspaceMetadata {
            name: name.to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: {
                    let mut opts = HashMap::new();
                    opts.insert("replication_factor".to_string(), "1".to_string());
                    opts
                },
            },
        }
    }

    fn test_table_meta(keyspace: &str, table: &str) -> TableMetadata {
        TableMetadata {
            keyspace: keyspace.to_string(),
            name: table.to_string(),
            id: Uuid::new_v4(),
            columns: indexmap::IndexMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: std::collections::HashMap::new(),
            is_system: false,
        }
    }

    fn test_column(name: &str, kind: ColumnKind) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        }
    }

    #[test]
    fn query_keyspaces_reflects_snapshot() {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces
            .insert("ks1".to_string(), test_keyspace_meta("ks1"));
        let rows = query_keyspaces(&snap);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].keyspace_name, "ks1");
        assert!(rows[0].durable_writes);
        assert_eq!(
            rows[0].replication.get("class"),
            Some(&"SimpleStrategy".to_string())
        );
    }

    #[test]
    fn query_tables_includes_keyspace_and_table_name() {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces
            .insert("ks1".to_string(), test_keyspace_meta("ks1"));
        snap.tables.insert(
            ("ks1".to_string(), "t1".to_string()),
            test_table_meta("ks1", "t1"),
        );
        let rows = query_tables(&snap);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].keyspace_name, "ks1");
        assert_eq!(rows[0].table_name, "t1");
    }

    #[test]
    fn query_columns_lists_all_columns() {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces
            .insert("ks1".to_string(), test_keyspace_meta("ks1"));
        let mut table = test_table_meta("ks1", "t1");
        table
            .columns
            .insert("c1".to_string(), test_column("c1", ColumnKind::Regular));
        table.columns.insert(
            "c2".to_string(),
            test_column("c2", ColumnKind::PartitionKey),
        );
        snap.tables
            .insert(("ks1".to_string(), "t1".to_string()), table);
        let rows = query_columns(&snap);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn query_columns_reports_correct_kind() {
        let mut snap = SchemaSnapshot::new();
        let mut table = test_table_meta("ks1", "t1");
        table.columns.insert(
            "pk".to_string(),
            test_column("pk", ColumnKind::PartitionKey),
        );
        table
            .columns
            .insert("ck".to_string(), test_column("ck", ColumnKind::Clustering));
        table
            .columns
            .insert("s".to_string(), test_column("s", ColumnKind::Static));
        table
            .columns
            .insert("r".to_string(), test_column("r", ColumnKind::Regular));
        snap.tables
            .insert(("ks1".to_string(), "t1".to_string()), table);
        let rows = query_columns(&snap);
        assert_eq!(rows.len(), 4);

        let pk_row = rows.iter().find(|r| r.column_name == "pk").unwrap();
        assert_eq!(pk_row.kind, "partition_key");
        let ck_row = rows.iter().find(|r| r.column_name == "ck").unwrap();
        assert_eq!(ck_row.kind, "clustering");
        let s_row = rows.iter().find(|r| r.column_name == "s").unwrap();
        assert_eq!(s_row.kind, "static");
        let r_row = rows.iter().find(|r| r.column_name == "r").unwrap();
        assert_eq!(r_row.kind, "regular");
    }
}
