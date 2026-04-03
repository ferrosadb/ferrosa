//! System table persistence: column indices, TableSchema definitions,
//! and the `SystemTableMutation` type that bridges DDL mutations to storage writes.

// ---------------------------------------------------------------------------
// system_schema.keyspaces column indices
// ---------------------------------------------------------------------------
// Partition key: keyspace_name (text) -- encoded as partition key bytes, not a cell.
// Regular columns:

/// `durable_writes` boolean, stored as 1-byte (0x00 = false, 0x01 = true).
pub const KEYSPACES_COL_DURABLE_WRITES: u16 = 0;
/// `replication` map<text, text>, stored as JSON bytes.
pub const KEYSPACES_COL_REPLICATION: u16 = 1;

// ---------------------------------------------------------------------------
// system_schema.tables column indices
// ---------------------------------------------------------------------------
// Partition key: keyspace_name (text)
// Clustering key: table_name (text)
// Regular columns:

/// `id` uuid, stored as 16-byte UUID.
pub const TABLES_COL_ID: u16 = 0;

// ---------------------------------------------------------------------------
// system_schema.columns column indices
// ---------------------------------------------------------------------------
// Partition key: keyspace_name (text)
// Clustering key: table_name (text), column_name (text) -- composite
// Regular columns:

/// `kind` text ("partition_key", "clustering", "regular", "static").
pub const COLUMNS_COL_KIND: u16 = 0;
/// `position` int, stored as 4-byte big-endian i32.
pub const COLUMNS_COL_POSITION: u16 = 1;
/// `type` text (CQL type name).
pub const COLUMNS_COL_TYPE: u16 = 2;
/// `clustering_order` text ("asc", "desc", "none").
pub const COLUMNS_COL_CLUSTERING_ORDER: u16 = 3;

// ---------------------------------------------------------------------------
// system_auth.roles column indices
// ---------------------------------------------------------------------------
// Partition key: role (text)
// Regular columns:

/// `is_superuser` boolean.
pub const ROLES_COL_IS_SUPERUSER: u16 = 0;
/// `can_login` boolean.
pub const ROLES_COL_CAN_LOGIN: u16 = 1;
/// `salted_hash` text (nullable).
pub const ROLES_COL_SALTED_HASH: u16 = 2;

// ---------------------------------------------------------------------------
// system_auth.role_members column indices
// ---------------------------------------------------------------------------
// Partition key: role (text)
// Clustering key: member (text)
// No regular columns -- existence of the row is the data.

// ---------------------------------------------------------------------------
// system_auth.role_permissions column indices
// ---------------------------------------------------------------------------
// Partition key: role (text)
// Clustering key: resource (text)
// Regular columns:

/// `permissions` `set<text>`, stored as JSON array bytes.
pub const PERMISSIONS_COL_PERMISSIONS: u16 = 0;

// ---------------------------------------------------------------------------
// SystemTableMutation enum
// ---------------------------------------------------------------------------

use crate::auth::permission::{GrantEntry, Permission, Resource};
use crate::auth::role::RoleMetadata;
use crate::metadata::keyspace::KeyspaceMetadata;
use crate::metadata::table::TableMetadata;

/// A mutation to a system table, emitted by the Raft state machine after
/// applying a DDL or auth command. The `SystemTableWriter` converts these
/// into `StorageEngine::write()` calls.
#[derive(Debug, Clone)]
pub enum SystemTableMutation {
    // ---- system_schema.keyspaces ----
    /// A keyspace was created or altered (upsert row).
    KeyspaceCreated(KeyspaceMetadata),
    /// A keyspace was dropped (tombstone row).
    KeyspaceDropped(String),

    // ---- system_schema.tables + system_schema.columns ----
    /// A table was created or altered (upsert rows in both tables and columns).
    TableCreated(Box<TableMetadata>),
    /// A table was dropped (tombstone rows in both tables and columns).
    TableDropped { keyspace: String, table: String },

    // ---- system_auth.roles ----
    /// A role was created or altered (upsert row).
    RoleCreated(RoleMetadata),
    /// A role was dropped (tombstone row + clean up members/permissions).
    RoleDropped(String),

    // ---- system_auth.role_permissions ----
    /// A grant was added or modified (upsert row).
    GrantUpdated(GrantEntry),
    /// A permission was revoked. If all permissions removed, tombstone the row.
    PermissionRevoked {
        role: String,
        resource: Resource,
        permission: Permission,
    },
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Encode a boolean as a 1-byte cell value.
pub fn encode_bool(val: bool) -> Vec<u8> {
    vec![if val { 0x01 } else { 0x00 }]
}

/// Encode an i32 as a 4-byte big-endian cell value.
pub fn encode_i32(val: i32) -> Vec<u8> {
    val.to_be_bytes().to_vec()
}

/// Encode a UUID as 16-byte cell value.
pub fn encode_uuid(val: &uuid::Uuid) -> Vec<u8> {
    val.as_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// Row conversion functions
// ---------------------------------------------------------------------------

use std::time::{SystemTime, UNIX_EPOCH};

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

/// Returns the current timestamp in microseconds for cell timestamps.
fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// Convert a `KeyspaceMetadata` into a storage row for `system_schema.keyspaces`.
///
/// Returns (partition_key, row, timestamp).
pub fn keyspace_to_row(ks: &KeyspaceMetadata) -> (DecoratedKey, Row, i64) {
    let ts = now_micros();
    let key = DecoratedKey::new(PartitionKey::new(ks.name.as_bytes().to_vec()));

    let mut replication_map = ks.replication.options.clone();
    replication_map.insert("class".to_string(), ks.replication.strategy.clone());
    let replication_json = serde_json::to_vec(&replication_map).unwrap_or_default();

    let row = Row {
        clustering: vec![],
        cells: vec![
            (
                KEYSPACES_COL_DURABLE_WRITES,
                CellValue::live(encode_bool(ks.durable_writes), ts),
            ),
            (
                KEYSPACES_COL_REPLICATION,
                CellValue::live(replication_json, ts),
            ),
        ],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    };

    (key, row, ts)
}

/// Result of converting a `TableMetadata` to storage rows.
pub struct TableMutationRows {
    /// Row for `system_schema.tables`.
    pub table_row: SystemRow,
    /// Rows for `system_schema.columns` (one per column).
    pub column_rows: Vec<SystemRow>,
    /// Timestamp used for all cells.
    pub timestamp: i64,
}

/// A single row destined for a system table.
pub struct SystemRow {
    /// Partition key.
    pub key: DecoratedKey,
    /// Row data.
    pub row: Row,
}

/// Convert a `TableMetadata` into storage rows for `system_schema.tables`
/// and `system_schema.columns`.
pub fn table_to_rows(table: &TableMetadata) -> TableMutationRows {
    let ts = now_micros();
    let key = DecoratedKey::new(PartitionKey::new(table.keyspace.as_bytes().to_vec()));

    // system_schema.tables row: clustering = table_name, cell = id
    let table_row = SystemRow {
        key: key.clone(),
        row: Row {
            clustering: table.name.as_bytes().to_vec(),
            cells: vec![(TABLES_COL_ID, CellValue::live(encode_uuid(&table.id), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        },
    };

    // system_schema.columns rows: one per column
    let column_rows: Vec<SystemRow> = table
        .columns
        .values()
        .map(|col| {
            // Composite clustering: table_name + column_name
            let mut clustering = Vec::new();
            clustering.extend_from_slice(&(table.name.len() as u16).to_be_bytes());
            clustering.extend_from_slice(table.name.as_bytes());
            clustering.extend_from_slice(col.name.as_bytes());

            let kind_str = match col.kind {
                crate::metadata::column::ColumnKind::PartitionKey => "partition_key",
                crate::metadata::column::ColumnKind::Clustering => "clustering",
                crate::metadata::column::ColumnKind::Regular => "regular",
                crate::metadata::column::ColumnKind::Static => "static",
            };
            let order_str = match col.clustering_order {
                crate::metadata::column::ClusteringOrder::Asc => "asc",
                crate::metadata::column::ClusteringOrder::Desc => "desc",
                crate::metadata::column::ClusteringOrder::None => "none",
            };

            SystemRow {
                key: key.clone(),
                row: Row {
                    clustering,
                    cells: vec![
                        (
                            COLUMNS_COL_KIND,
                            CellValue::live(kind_str.as_bytes().to_vec(), ts),
                        ),
                        (
                            COLUMNS_COL_POSITION,
                            CellValue::live(encode_i32(col.position), ts),
                        ),
                        (
                            COLUMNS_COL_TYPE,
                            CellValue::live(col.column_type.as_bytes().to_vec(), ts),
                        ),
                        (
                            COLUMNS_COL_CLUSTERING_ORDER,
                            CellValue::live(order_str.as_bytes().to_vec(), ts),
                        ),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts),
                },
            }
        })
        .collect();

    TableMutationRows {
        table_row,
        column_rows,
        timestamp: ts,
    }
}

/// Convert a `RoleMetadata` into a storage row for `system_auth.roles`.
pub fn role_to_row(role: &RoleMetadata) -> (DecoratedKey, Row, i64) {
    let ts = now_micros();
    let key = DecoratedKey::new(PartitionKey::new(role.name.as_bytes().to_vec()));

    let hash_cell = match &role.salted_hash {
        Some(h) => CellValue::live(h.as_bytes().to_vec(), ts),
        None => CellValue::tombstone(ts, (ts / 1_000_000) as i32),
    };

    let row = Row {
        clustering: vec![],
        cells: vec![
            (
                ROLES_COL_IS_SUPERUSER,
                CellValue::live(encode_bool(role.is_superuser), ts),
            ),
            (
                ROLES_COL_CAN_LOGIN,
                CellValue::live(encode_bool(role.can_login), ts),
            ),
            (ROLES_COL_SALTED_HASH, hash_cell),
        ],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    };

    (key, row, ts)
}

/// Convert a `GrantEntry` into a storage row for `system_auth.role_permissions`.
pub fn grant_to_row(grant: &GrantEntry) -> (DecoratedKey, Row, i64) {
    let ts = now_micros();
    let key = DecoratedKey::new(PartitionKey::new(grant.role.as_bytes().to_vec()));

    let resource_str = grant.resource.to_string();
    let perms: Vec<String> = grant.permissions.iter().map(|p| p.to_string()).collect();
    let perms_json = serde_json::to_vec(&perms).unwrap_or_default();

    let row = Row {
        clustering: resource_str.as_bytes().to_vec(),
        cells: vec![(PERMISSIONS_COL_PERMISSIONS, CellValue::live(perms_json, ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    };

    (key, row, ts)
}

// ---------------------------------------------------------------------------
// System table TableSchema builders
// ---------------------------------------------------------------------------

use ferrosa_common::schema::{ColumnDefinition, TableSchema};

/// Returns `TableSchema` for `system_schema.keyspaces`.
pub fn keyspaces_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "system_schema".to_string(),
        table: "keyspaces".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![
            ColumnDefinition {
                name: "durable_writes".to_string(),
                type_name: "org.apache.cassandra.db.marshal.BooleanType".to_string(),
            },
            ColumnDefinition {
                name: "replication".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
        ],
        extensions: Default::default(),
    }
}

/// Returns `TableSchema` for `system_schema.tables`.
pub fn tables_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "system_schema".to_string(),
        table: "tables".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "table_name".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "id".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UUIDType".to_string(),
        }],
        extensions: Default::default(),
    }
}

/// Returns `TableSchema` for `system_schema.columns`.
pub fn columns_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "system_schema".to_string(),
        table: "columns".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![
            ColumnDefinition {
                name: "table_name".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            ColumnDefinition {
                name: "column_name".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
        ],
        static_columns: vec![],
        regular_columns: vec![
            ColumnDefinition {
                name: "kind".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            ColumnDefinition {
                name: "position".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            },
            ColumnDefinition {
                name: "type".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            ColumnDefinition {
                name: "clustering_order".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
        ],
        extensions: Default::default(),
    }
}

/// Returns `TableSchema` for `system_auth.roles`.
pub fn roles_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "system_auth".to_string(),
        table: "roles".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![
            ColumnDefinition {
                name: "is_superuser".to_string(),
                type_name: "org.apache.cassandra.db.marshal.BooleanType".to_string(),
            },
            ColumnDefinition {
                name: "can_login".to_string(),
                type_name: "org.apache.cassandra.db.marshal.BooleanType".to_string(),
            },
            ColumnDefinition {
                name: "salted_hash".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
        ],
        extensions: Default::default(),
    }
}

/// Returns `TableSchema` for `system_auth.role_members`.
pub fn role_members_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "system_auth".to_string(),
        table: "role_members".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "member".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![],
        extensions: Default::default(),
    }
}

/// Returns `TableSchema` for `system_auth.role_permissions`.
pub fn role_permissions_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "system_auth".to_string(),
        table: "role_permissions".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "resource".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "permissions".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

/// Returns all system table schemas for registration at bootstrap.
pub fn all_system_table_schemas() -> Vec<TableSchema> {
    let mut schemas = vec![
        keyspaces_table_schema(),
        tables_table_schema(),
        columns_table_schema(),
        roles_table_schema(),
        role_members_table_schema(),
        role_permissions_table_schema(),
    ];
    schemas.extend(super::observability::all_observability_table_schemas());
    schemas
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyspaces_column_indices_are_sequential() {
        assert_eq!(KEYSPACES_COL_DURABLE_WRITES, 0);
        assert_eq!(KEYSPACES_COL_REPLICATION, 1);
    }

    #[test]
    fn tables_column_indices_are_sequential() {
        assert_eq!(TABLES_COL_ID, 0);
    }

    #[test]
    fn columns_column_indices_are_sequential() {
        assert_eq!(COLUMNS_COL_KIND, 0);
        assert_eq!(COLUMNS_COL_POSITION, 1);
        assert_eq!(COLUMNS_COL_TYPE, 2);
        assert_eq!(COLUMNS_COL_CLUSTERING_ORDER, 3);
    }

    #[test]
    fn roles_column_indices_are_sequential() {
        assert_eq!(ROLES_COL_IS_SUPERUSER, 0);
        assert_eq!(ROLES_COL_CAN_LOGIN, 1);
        assert_eq!(ROLES_COL_SALTED_HASH, 2);
    }

    // -- Task 2: TableSchema builder tests --

    #[test]
    fn system_schema_keyspaces_table_schema() {
        let schema = keyspaces_table_schema();
        assert_eq!(schema.keyspace, "system_schema");
        assert_eq!(schema.table, "keyspaces");
        assert_eq!(schema.key_type, "org.apache.cassandra.db.marshal.UTF8Type");
        assert!(schema.clustering_columns.is_empty());
        assert_eq!(schema.regular_columns.len(), 2);
        assert_eq!(schema.regular_columns[0].name, "durable_writes");
        assert_eq!(schema.regular_columns[1].name, "replication");
    }

    #[test]
    fn tables_schema_has_clustering_key() {
        let schema = tables_table_schema();
        assert_eq!(schema.keyspace, "system_schema");
        assert_eq!(schema.table, "tables");
        assert_eq!(schema.clustering_columns.len(), 1);
        assert_eq!(schema.clustering_columns[0].name, "table_name");
        assert_eq!(schema.regular_columns.len(), 1);
    }

    #[test]
    fn columns_schema_has_composite_clustering() {
        let schema = columns_table_schema();
        assert_eq!(schema.clustering_columns.len(), 2);
        assert_eq!(schema.clustering_columns[0].name, "table_name");
        assert_eq!(schema.clustering_columns[1].name, "column_name");
        assert_eq!(schema.regular_columns.len(), 4);
    }

    #[test]
    fn roles_schema_layout() {
        let schema = roles_table_schema();
        assert_eq!(schema.keyspace, "system_auth");
        assert_eq!(schema.table, "roles");
        assert_eq!(schema.regular_columns.len(), 3);
    }

    #[test]
    fn role_members_schema_has_clustering() {
        let schema = role_members_table_schema();
        assert_eq!(schema.clustering_columns.len(), 1);
        assert_eq!(schema.clustering_columns[0].name, "member");
        assert!(schema.regular_columns.is_empty());
    }

    #[test]
    fn role_permissions_schema_layout() {
        let schema = role_permissions_table_schema();
        assert_eq!(schema.clustering_columns.len(), 1);
        assert_eq!(schema.clustering_columns[0].name, "resource");
        assert_eq!(schema.regular_columns.len(), 1);
    }

    #[test]
    fn all_system_table_schemas_returns_nine() {
        let schemas = all_system_table_schemas();
        assert_eq!(schemas.len(), 9);
        let names: Vec<_> = schemas.iter().map(|s| (&s.keyspace, &s.table)).collect();
        assert!(names.contains(&(&"system_schema".to_string(), &"keyspaces".to_string())));
        assert!(names.contains(&(&"system_schema".to_string(), &"tables".to_string())));
        assert!(names.contains(&(&"system_schema".to_string(), &"columns".to_string())));
        assert!(names.contains(&(&"system_auth".to_string(), &"roles".to_string())));
        assert!(names.contains(&(&"system_auth".to_string(), &"role_members".to_string())));
        assert!(names.contains(&(&"system_auth".to_string(), &"role_permissions".to_string())));
        assert!(names.contains(&(&"system_observability".to_string(), &"spans".to_string())));
        assert!(names.contains(&(&"system_observability".to_string(), &"metrics".to_string())));
        assert!(names.contains(&(&"system_observability".to_string(), &"slow_queries".to_string())));
    }

    // -- Task 4: SystemTableMutation tests --

    #[test]
    fn system_table_mutation_keyspace_created() {
        use crate::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
        let ks = KeyspaceMetadata {
            name: "my_ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: std::collections::HashMap::from([(
                    "replication_factor".to_string(),
                    "3".to_string(),
                )]),
            },
        };
        let mutation = SystemTableMutation::KeyspaceCreated(ks.clone());
        match &mutation {
            SystemTableMutation::KeyspaceCreated(k) => assert_eq!(k.name, "my_ks"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn system_table_mutation_role_created() {
        use crate::auth::role::RoleMetadata;
        let role = RoleMetadata {
            name: "admin".to_string(),
            is_superuser: true,
            can_login: true,
            salted_hash: Some("$2b$hash".to_string()),
            member_of: std::collections::HashSet::new(),
        };
        let mutation = SystemTableMutation::RoleCreated(role);
        match &mutation {
            SystemTableMutation::RoleCreated(r) => assert_eq!(r.name, "admin"),
            _ => panic!("wrong variant"),
        }
    }

    // -- Task 5: Encoding helper tests --

    #[test]
    fn encode_bool_true() {
        assert_eq!(encode_bool(true), vec![0x01]);
    }

    #[test]
    fn encode_bool_false() {
        assert_eq!(encode_bool(false), vec![0x00]);
    }

    #[test]
    fn encode_i32_value() {
        assert_eq!(encode_i32(42), 42i32.to_be_bytes().to_vec());
    }

    // -- Task 6: keyspace_to_row tests --

    #[test]
    fn keyspace_mutation_produces_row() {
        use crate::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};

        let ks = KeyspaceMetadata {
            name: "test_ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: std::collections::HashMap::from([(
                    "replication_factor".to_string(),
                    "3".to_string(),
                )]),
            },
        };

        let (key, row, timestamp) = keyspace_to_row(&ks);

        // Partition key is the keyspace name.
        assert_eq!(key.key.as_bytes(), b"test_ks");

        // Row should have 2 cells.
        assert_eq!(row.cells.len(), 2);

        // Cell 0: durable_writes = true (0x01).
        let (idx, cell) = &row.cells[0];
        assert_eq!(*idx, KEYSPACES_COL_DURABLE_WRITES);
        assert_eq!(cell.value.as_deref(), Some(&[0x01][..]));

        // Cell 1: replication as JSON.
        let (idx, cell) = &row.cells[1];
        assert_eq!(*idx, KEYSPACES_COL_REPLICATION);
        assert!(cell.value.is_some());

        // Timestamp should be positive.
        assert!(timestamp > 0);
    }

    // -- Task 7: table_to_rows tests --

    #[test]
    fn table_mutation_produces_tables_and_columns_rows() {
        use crate::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
        use crate::metadata::table::TableParams;

        let mut table = TableMetadata {
            keyspace: "ks".to_string(),
            name: "users".to_string(),
            id: uuid::Uuid::nil(),
            columns: indexmap::IndexMap::new(),
            partition_key: vec!["id".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: std::collections::HashSet::new(),
            extensions: std::collections::HashMap::new(),
            is_system: false,
        };
        table.columns.insert(
            "id".to_string(),
            ColumnMetadata {
                name: "id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        table.columns.insert(
            "name".to_string(),
            ColumnMetadata {
                name: "name".to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );

        let result = table_to_rows(&table);

        // table_row: written to system_schema.tables
        assert_eq!(result.table_row.key.key.as_bytes(), b"ks");
        assert_eq!(result.table_row.row.cells.len(), 1); // id column
                                                         // Clustering key should be the table name.
        assert_eq!(result.table_row.row.clustering, b"users");

        // column_rows: written to system_schema.columns
        assert_eq!(result.column_rows.len(), 2);
    }

    // -- Task 8: role_to_row tests --

    #[test]
    fn role_mutation_produces_row() {
        use crate::auth::role::RoleMetadata;

        let role = RoleMetadata {
            name: "analyst".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: Some("$2b$hash".to_string()),
            member_of: std::collections::HashSet::new(),
        };

        let (key, row, _ts) = role_to_row(&role);

        assert_eq!(key.key.as_bytes(), b"analyst");
        assert_eq!(row.cells.len(), 3);

        let (idx, cell) = &row.cells[0];
        assert_eq!(*idx, ROLES_COL_IS_SUPERUSER);
        assert_eq!(cell.value.as_deref(), Some(&[0x00][..])); // false

        let (idx, cell) = &row.cells[1];
        assert_eq!(*idx, ROLES_COL_CAN_LOGIN);
        assert_eq!(cell.value.as_deref(), Some(&[0x01][..])); // true

        let (idx, cell) = &row.cells[2];
        assert_eq!(*idx, ROLES_COL_SALTED_HASH);
        assert!(cell.value.is_some());
    }

    #[test]
    fn role_with_no_hash_produces_tombstone_cell() {
        use crate::auth::role::RoleMetadata;

        let role = RoleMetadata {
            name: "nohash".to_string(),
            is_superuser: false,
            can_login: false,
            salted_hash: None,
            member_of: std::collections::HashSet::new(),
        };

        let (_key, row, _ts) = role_to_row(&role);
        let (idx, cell) = &row.cells[2];
        assert_eq!(*idx, ROLES_COL_SALTED_HASH);
        // Null value -> tombstone cell.
        assert!(cell.value.is_none());
    }

    // -- Task 9: grant_to_row tests --

    #[test]
    fn grant_mutation_produces_row() {
        use crate::auth::permission::{GrantEntry, Permission, Resource};

        let grant = GrantEntry {
            role: "reader".to_string(),
            resource: Resource::Table("ks".to_string(), "t".to_string()),
            permissions: [Permission::Select].into_iter().collect(),
        };

        let (key, row, _ts) = grant_to_row(&grant);

        assert_eq!(key.key.as_bytes(), b"reader");
        // Clustering key = resource string.
        assert!(!row.clustering.is_empty());
        // One cell: permissions as JSON.
        assert_eq!(row.cells.len(), 1);
        let (idx, _cell) = &row.cells[0];
        assert_eq!(*idx, PERMISSIONS_COL_PERMISSIONS);
    }
}
