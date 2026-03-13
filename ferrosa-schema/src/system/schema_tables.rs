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
///
/// Includes virtual system keyspaces (`system`, `system_schema`, `system_auth`)
/// alongside user-created keyspaces. CQL drivers and cqlsh expect these to appear
/// in `system_schema.keyspaces` for introspection.
pub fn query_keyspaces(snap: &SchemaSnapshot) -> Vec<KeyspaceRow> {
    let mut rows: Vec<KeyspaceRow> = vec![
        system_keyspace_row("system"),
        system_keyspace_row("system_schema"),
        system_keyspace_row("system_auth"),
    ];
    rows.extend(snap.keyspaces.values().map(|ks| {
        let mut replication = ks.replication.options.clone();
        replication.insert("class".to_string(), ks.replication.strategy.clone());
        KeyspaceRow {
            keyspace_name: ks.name.clone(),
            durable_writes: ks.durable_writes,
            replication,
        }
    }));
    rows
}

fn system_keyspace_row(name: &str) -> KeyspaceRow {
    let mut replication = HashMap::new();
    replication.insert("class".to_string(), "LocalStrategy".to_string());
    KeyspaceRow {
        keyspace_name: name.to_string(),
        durable_writes: true,
        replication,
    }
}

/// Query `system_schema.tables` from a snapshot.
///
/// Includes virtual system tables alongside user tables so that CQL drivers
/// and cqlsh can discover them during introspection.
pub fn query_tables(snap: &SchemaSnapshot) -> Vec<TableRow> {
    let mut rows = system_table_rows();
    rows.extend(snap.tables.values().map(|t| TableRow {
        keyspace_name: t.keyspace.clone(),
        table_name: t.name.clone(),
        id: t.id,
    }));
    rows
}

/// Query `system_schema.columns` from a snapshot.
///
/// Includes columns for virtual system tables alongside user table columns.
pub fn query_columns(snap: &SchemaSnapshot) -> Vec<ColumnRow> {
    let mut rows = system_column_rows();
    rows.extend(snap.tables.values().flat_map(|t| {
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
    }));
    rows
}

/// Virtual system table entries for `system_schema.tables`.
fn system_table_rows() -> Vec<TableRow> {
    let tables: &[(&str, &str)] = &[
        ("system", "local"),
        ("system", "peers"),
        ("system", "peers_v2"),
        ("system_schema", "keyspaces"),
        ("system_schema", "tables"),
        ("system_schema", "columns"),
        ("system_schema", "types"),
        ("system_schema", "functions"),
        ("system_schema", "aggregates"),
        ("system_schema", "triggers"),
        ("system_schema", "views"),
        ("system_schema", "indexes"),
        ("system_auth", "roles"),
        ("system_auth", "role_members"),
        ("system_auth", "role_permissions"),
    ];
    tables
        .iter()
        .map(|(ks, tbl)| TableRow {
            keyspace_name: ks.to_string(),
            table_name: tbl.to_string(),
            id: Uuid::new_v4(),
        })
        .collect()
}

/// Virtual system column entries for `system_schema.columns`.
///
/// Provides minimal column metadata for system tables that cqlsh queries
/// during startup. Only includes the columns our router actually returns.
fn system_column_rows() -> Vec<ColumnRow> {
    let mut rows = Vec::new();

    // system.local columns (matches our route_select handler)
    let local_cols: &[(&str, &str, &str, i32)] = &[
        ("key", "partition_key", "text", 0),
        ("cluster_name", "regular", "text", -1),
        ("data_center", "regular", "text", -1),
        ("rack", "regular", "text", -1),
        ("host_id", "regular", "uuid", -1),
        ("partitioner", "regular", "text", -1),
        ("native_protocol_version", "regular", "text", -1),
        ("cql_version", "regular", "text", -1),
        ("release_version", "regular", "text", -1),
        ("schema_version", "regular", "uuid", -1),
        ("rpc_port", "regular", "int", -1),
        ("listen_address", "regular", "inet", -1),
        ("broadcast_address", "regular", "inet", -1),
        ("rpc_address", "regular", "inet", -1),
        ("bootstrapped", "regular", "text", -1),
    ];
    for (name, kind, cql_type, pos) in local_cols {
        rows.push(ColumnRow {
            keyspace_name: "system".to_string(),
            table_name: "local".to_string(),
            column_name: name.to_string(),
            kind: kind.to_string(),
            position: *pos,
            column_type: cql_type.to_string(),
            clustering_order: "none".to_string(),
        });
    }

    // system.peers / peers_v2 columns
    let peer_cols: &[(&str, &str, &str, i32)] = &[
        ("peer", "partition_key", "inet", 0),
        ("peer_port", "regular", "int", -1),
        ("data_center", "regular", "text", -1),
        ("rack", "regular", "text", -1),
        ("host_id", "regular", "uuid", -1),
        ("native_address", "regular", "inet", -1),
        ("native_port", "regular", "int", -1),
        ("schema_version", "regular", "uuid", -1),
        ("release_version", "regular", "text", -1),
    ];
    for table in &["peers", "peers_v2"] {
        for (name, kind, cql_type, pos) in peer_cols {
            rows.push(ColumnRow {
                keyspace_name: "system".to_string(),
                table_name: table.to_string(),
                column_name: name.to_string(),
                kind: kind.to_string(),
                position: *pos,
                column_type: cql_type.to_string(),
                clustering_order: "none".to_string(),
            });
        }
    }

    rows
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
        // System keyspaces (system, system_schema, system_auth) + user keyspace
        assert_eq!(rows.len(), 4);
        let user_rows: Vec<_> = rows.iter().filter(|r| r.keyspace_name == "ks1").collect();
        assert_eq!(user_rows.len(), 1);
        assert!(user_rows[0].durable_writes);
        assert_eq!(
            user_rows[0].replication.get("class"),
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
        let user_rows: Vec<_> = rows.iter().filter(|r| r.keyspace_name == "ks1").collect();
        assert_eq!(user_rows.len(), 1);
        assert_eq!(user_rows[0].table_name, "t1");
        // Should also include system tables
        assert!(rows.len() > 1);
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
        let user_rows: Vec<_> = rows.iter().filter(|r| r.keyspace_name == "ks1").collect();
        assert_eq!(user_rows.len(), 2);
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
        let rows: Vec<_> = rows.iter().filter(|r| r.keyspace_name == "ks1").collect();
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
