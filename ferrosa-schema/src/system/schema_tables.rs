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
    // Snapshot is the source of truth. `Schema::new()` seeds the four
    // system keyspaces, so for production callers the snapshot already
    // contains them. Bare `SchemaSnapshot::new()` callers (tests) don't,
    // so we fall back to hardcoded LocalStrategy rows only for the ones
    // the snapshot is missing. Emitting both was a latent bug: after the
    // `system_auth` default moved to NTS, duplicated rows caused cqlsh
    // to see the LocalStrategy version and mask the fix.
    let mut rows: Vec<KeyspaceRow> = snap
        .keyspaces
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
        .collect();
    for name in [
        "system",
        "system_schema",
        "system_auth",
        "system_observability",
    ] {
        if !snap.keyspaces.contains_key(name) {
            rows.push(system_keyspace_row(name));
        }
    }
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
            // p1-37: scylla 0.15 driver's CQL type parser doesn't recognize
            // `vector<...>` types and rejects the whole metadata fetch.
            // Advertise vectors as `blob` to drivers — the on-disk
            // representation is bytes anyway, and DML still works because
            // INSERT/SELECT serialize vectors as blob bytes.
            column_type: rewrite_unsupported_types(&c.column_type),
            clustering_order: match c.clustering_order {
                crate::metadata::column::ClusteringOrder::Asc => "asc".to_string(),
                crate::metadata::column::ClusteringOrder::Desc => "desc".to_string(),
                crate::metadata::column::ClusteringOrder::None => "none".to_string(),
            },
        })
    }));
    rows
}

/// Rewrite types that drivers may not parse to a compatible alternative.
/// Currently: `vector<...>` → `blob`. Other types pass through unchanged.
fn rewrite_unsupported_types(ty: &str) -> String {
    if ty.starts_with("vector<") {
        return "blob".to_string();
    }
    ty.to_string()
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
        ("system_observability", "connections"),
        ("system_observability", "active_queries"),
        ("system_observability", "consolidation_status"),
        ("system_observability", "storage_stats"),
        ("system_observability", "secondary_indexes"),
        ("system_observability", "archive_status"),
        ("system_observability", "snapshots"),
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
        ("tokens", "regular", "set<varchar>", -1),
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

    // system.peers / peers_v2 columns. p1-37: legacy `system.peers` MUST
    // include `rpc_address inet` (Cassandra 3.x legacy column) — drivers
    // like scylla 0.15 fail metadata fetch otherwise. `peers_v2` does
    // NOT include it (it uses native_address instead).
    let peers_v2_cols: &[(&str, &str, &str, i32)] = &[
        ("peer", "partition_key", "inet", 0),
        ("peer_port", "regular", "int", -1),
        ("data_center", "regular", "text", -1),
        ("rack", "regular", "text", -1),
        ("host_id", "regular", "uuid", -1),
        ("native_address", "regular", "inet", -1),
        ("native_port", "regular", "int", -1),
        ("schema_version", "regular", "uuid", -1),
        ("release_version", "regular", "text", -1),
        ("tokens", "regular", "set<varchar>", -1),
    ];
    // p1-37: legacy `system.peers` is the union of peers_v2's columns
    // PLUS `rpc_address` (the original Cassandra 3.x column drivers
    // type-check on). Cassandra 4+'s system.peers includes both, so
    // keeping both maximizes driver compatibility.
    let peers_v1_cols: &[(&str, &str, &str, i32)] = &[
        ("peer", "partition_key", "inet", 0),
        ("peer_port", "regular", "int", -1),
        ("data_center", "regular", "text", -1),
        ("rack", "regular", "text", -1),
        ("host_id", "regular", "uuid", -1),
        ("native_address", "regular", "inet", -1),
        ("native_port", "regular", "int", -1),
        ("rpc_address", "regular", "inet", -1),
        ("schema_version", "regular", "uuid", -1),
        ("release_version", "regular", "text", -1),
        ("tokens", "regular", "set<varchar>", -1),
    ];
    for (table, cols) in [("peers", peers_v1_cols), ("peers_v2", peers_v2_cols)] {
        for (name, kind, cql_type, pos) in cols {
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

    // system_observability.connections columns
    let connections_cols: &[(&str, &str, &str, i32)] = &[
        ("peer_address", "partition_key", "text", 0),
        ("peer_port", "clustering", "int", 0),
        ("state", "regular", "text", -1),
        ("username", "regular", "text", -1),
        ("idle_seconds", "regular", "int", -1),
        ("requests_served", "regular", "bigint", -1),
        ("protocol_version", "regular", "int", -1),
    ];
    for (name, kind, cql_type, pos) in connections_cols {
        rows.push(ColumnRow {
            keyspace_name: "system_observability".to_string(),
            table_name: "connections".to_string(),
            column_name: name.to_string(),
            kind: kind.to_string(),
            position: *pos,
            column_type: cql_type.to_string(),
            clustering_order: if *kind == "clustering" {
                "asc".to_string()
            } else {
                "none".to_string()
            },
        });
    }

    // system_observability.active_queries columns
    let active_queries_cols: &[(&str, &str, &str, i32)] = &[
        ("query_id", "partition_key", "bigint", 0),
        ("client_address", "regular", "text", -1),
        ("username", "regular", "text", -1),
        ("query_text", "regular", "text", -1),
        ("keyspace", "regular", "text", -1),
        ("start_time", "regular", "bigint", -1),
        ("elapsed_ms", "regular", "bigint", -1),
        ("state", "regular", "text", -1),
    ];
    for (name, kind, cql_type, pos) in active_queries_cols {
        rows.push(ColumnRow {
            keyspace_name: "system_observability".to_string(),
            table_name: "active_queries".to_string(),
            column_name: name.to_string(),
            kind: kind.to_string(),
            position: *pos,
            column_type: cql_type.to_string(),
            clustering_order: "none".to_string(),
        });
    }

    // system_observability.consolidation_status columns
    let consolidation_cols: &[(&str, &str, &str, i32)] = &[
        ("keyspace_name", "partition_key", "text", 0),
        ("table_name", "clustering", "text", 0),
        ("interval", "regular", "text", -1),
        ("target_table", "regular", "text", -1),
        ("functions", "regular", "text", -1),
        ("status", "regular", "text", -1),
    ];
    for (name, kind, cql_type, pos) in consolidation_cols {
        rows.push(ColumnRow {
            keyspace_name: "system_observability".to_string(),
            table_name: "consolidation_status".to_string(),
            column_name: name.to_string(),
            kind: kind.to_string(),
            position: *pos,
            column_type: cql_type.to_string(),
            clustering_order: if *kind == "clustering" {
                "asc".to_string()
            } else {
                "none".to_string()
            },
        });
    }

    // system_observability.storage_stats columns
    let storage_stats_cols: &[(&str, &str, &str, i32)] = &[
        ("keyspace", "partition_key", "text", 0),
        ("table_name", "clustering", "text", 0),
        ("memtable_size_bytes", "regular", "bigint", -1),
        ("memtable_count", "regular", "int", -1),
        ("sstable_count", "regular", "int", -1),
        ("sstable_size_bytes", "regular", "bigint", -1),
        ("s3_object_count", "regular", "int", -1),
        ("s3_bytes", "regular", "bigint", -1),
        ("pending_compactions", "regular", "int", -1),
    ];
    for (name, kind, cql_type, pos) in storage_stats_cols {
        rows.push(ColumnRow {
            keyspace_name: "system_observability".to_string(),
            table_name: "storage_stats".to_string(),
            column_name: name.to_string(),
            kind: kind.to_string(),
            position: *pos,
            column_type: cql_type.to_string(),
            clustering_order: if *kind == "clustering" {
                "asc".to_string()
            } else {
                "none".to_string()
            },
        });
    }

    // system_observability.secondary_indexes columns
    let secondary_indexes_cols: &[(&str, &str, &str, i32)] = &[
        ("keyspace_name", "partition_key", "text", 0),
        ("table_name", "clustering", "text", 0),
        ("index_name", "clustering", "text", 1),
        ("index_type", "regular", "text", -1),
        ("status", "regular", "text", -1),
        ("indexed_sstable_count", "regular", "int", -1),
        ("pending_sstable_count", "regular", "int", -1),
        ("pending_bytes", "regular", "bigint", -1),
        ("lag_seconds", "regular", "double", -1),
        ("last_build_ms", "regular", "bigint", -1),
        ("total_builds", "regular", "bigint", -1),
        ("build_errors", "regular", "bigint", -1),
    ];
    for (name, kind, cql_type, pos) in secondary_indexes_cols {
        rows.push(ColumnRow {
            keyspace_name: "system_observability".to_string(),
            table_name: "secondary_indexes".to_string(),
            column_name: name.to_string(),
            kind: kind.to_string(),
            position: *pos,
            column_type: cql_type.to_string(),
            clustering_order: if *kind == "clustering" {
                "asc".to_string()
            } else {
                "none".to_string()
            },
        });
    }

    // system_observability.archive_status columns
    let archive_status_cols: &[(&str, &str, &str, i32)] = &[
        ("unarchived_segments", "partition_key", "bigint", 0),
        ("oldest_unarchived_age_secs", "regular", "bigint", -1),
        ("last_archive_success", "regular", "text", -1),
        ("archive_errors_total", "regular", "bigint", -1),
    ];
    for (name, kind, cql_type, pos) in archive_status_cols {
        rows.push(ColumnRow {
            keyspace_name: "system_observability".to_string(),
            table_name: "archive_status".to_string(),
            column_name: name.to_string(),
            kind: kind.to_string(),
            position: *pos,
            column_type: cql_type.to_string(),
            clustering_order: "none".to_string(),
        });
    }

    // system_observability.snapshots columns
    let snapshots_cols: &[(&str, &str, &str, i32)] = &[
        ("name", "partition_key", "text", 0),
        ("created_at", "regular", "text", -1),
        ("expires_at", "regular", "text", -1),
        ("commit_log_segment", "regular", "bigint", -1),
        ("commit_log_offset", "regular", "bigint", -1),
        ("node_id", "regular", "text", -1),
    ];
    for (name, kind, cql_type, pos) in snapshots_cols {
        rows.push(ColumnRow {
            keyspace_name: "system_observability".to_string(),
            table_name: "snapshots".to_string(),
            column_name: name.to_string(),
            kind: kind.to_string(),
            position: *pos,
            column_type: cql_type.to_string(),
            clustering_order: "none".to_string(),
        });
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
        // System keyspaces (system, system_schema, system_auth,
        // system_observability) + user keyspace
        assert_eq!(rows.len(), 5);
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

    /// Regression test: cqlsh expects `tokens` in system_schema.columns for
    /// system.local. Without it, cqlsh fails to validate system.local queries
    /// and prints "'local' not found in keyspace 'system'".
    #[test]
    fn system_local_columns_include_tokens() {
        let snap = SchemaSnapshot::new();
        let rows = query_columns(&snap);
        let local_cols: Vec<_> = rows
            .iter()
            .filter(|r| r.keyspace_name == "system" && r.table_name == "local")
            .collect();
        let tokens_col = local_cols
            .iter()
            .find(|r| r.column_name == "tokens")
            .expect("system.local must have a 'tokens' column");
        assert_eq!(tokens_col.column_type, "set<varchar>");
        assert_eq!(tokens_col.kind, "regular");
    }

    /// Regression test for BUG-SYSTEM-PEERS-MISSING-TOKENS.
    /// cdrs-tokio requires `tokens` in system.peers to build its
    /// token-aware routing map. Without it, the driver enters an
    /// infinite error-retry loop and never establishes a session.
    #[test]
    fn system_peers_columns_include_tokens() {
        let snap = SchemaSnapshot::new();
        let rows = query_columns(&snap);
        for table in &["peers", "peers_v2"] {
            let peer_cols: Vec<_> = rows
                .iter()
                .filter(|r| r.keyspace_name == "system" && r.table_name == *table)
                .collect();
            let tokens_col = peer_cols
                .iter()
                .find(|r| r.column_name == "tokens")
                .unwrap_or_else(|| panic!("system.{table} must have a 'tokens' column"));
            assert_eq!(tokens_col.column_type, "set<varchar>");
            assert_eq!(tokens_col.kind, "regular");
        }
    }
}
