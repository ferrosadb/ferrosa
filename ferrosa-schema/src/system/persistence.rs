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

/// `permissions` set<text>, stored as JSON array bytes.
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
    }
}

/// Returns all system table schemas for registration at bootstrap.
pub fn all_system_table_schemas() -> Vec<TableSchema> {
    vec![
        keyspaces_table_schema(),
        tables_table_schema(),
        columns_table_schema(),
        roles_table_schema(),
        role_members_table_schema(),
        role_permissions_table_schema(),
    ]
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
    fn all_system_table_schemas_returns_six() {
        let schemas = all_system_table_schemas();
        assert_eq!(schemas.len(), 6);
        let names: Vec<_> = schemas.iter().map(|s| (&s.keyspace, &s.table)).collect();
        assert!(names.contains(&(&"system_schema".to_string(), &"keyspaces".to_string())));
        assert!(names.contains(&(&"system_schema".to_string(), &"tables".to_string())));
        assert!(names.contains(&(&"system_schema".to_string(), &"columns".to_string())));
        assert!(names.contains(&(&"system_auth".to_string(), &"roles".to_string())));
        assert!(names.contains(&(&"system_auth".to_string(), &"role_members".to_string())));
        assert!(names.contains(&(&"system_auth".to_string(), &"role_permissions".to_string())));
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
}
