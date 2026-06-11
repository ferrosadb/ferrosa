//! Writes system table mutations to the storage engine.
//!
//! [`SystemTableWriter`] bridges `ferrosa-schema` (which defines the mutation
//! types and row conversion functions) with `ferrosa-storage` (which provides
//! the write API). This module lives in `ferrosa-cluster` because it depends
//! on both crates, and placing it in either would create a circular dependency.

use std::sync::Arc;

use ferrosa_common::{DecoratedKey, PartitionKey};
use ferrosa_schema::system::persistence::{
    grant_to_row, index_to_rows, keyspace_to_row, role_to_row, table_to_rows, SystemTableMutation,
};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

/// Returns the current timestamp in microseconds for cell timestamps.
fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// Writes system table mutations to the storage engine.
///
/// Holds an `Arc<StorageEngine>` and translates `SystemTableMutation` values
/// into `StorageEngine::write()` calls. Each mutation writes to the appropriate
/// system table (`system_schema.*` or `system_auth.*`).
pub struct SystemTableWriter {
    engine: Arc<StorageEngine>,
}

impl SystemTableWriter {
    /// Create a new writer backed by the given storage engine.
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    /// Apply a system table mutation by writing to the storage engine.
    pub fn apply(&self, mutation: SystemTableMutation) -> ferrosa_common::Result<()> {
        match mutation {
            SystemTableMutation::KeyspaceCreated(ks) => {
                let (key, row, ts) = keyspace_to_row(&ks);
                let tid = TableId::new("system_schema", "keyspaces");
                self.engine.write(&tid, &key, row, ts)?;
            }
            SystemTableMutation::KeyspaceDropped(name) => {
                let ts = now_micros();
                let key = DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec()));
                let tid = TableId::new("system_schema", "keyspaces");
                let row = Row {
                    clustering: vec![],
                    cells: vec![],
                    deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
                    primary_key_liveness: LivenessInfo::NONE,
                };
                self.engine.write(&tid, &key, row, ts)?;
            }
            SystemTableMutation::TableCreated(table) => {
                let result = table_to_rows(&table);
                let tables_tid = TableId::new("system_schema", "tables");
                self.engine.write(
                    &tables_tid,
                    &result.table_row.key,
                    result.table_row.row,
                    result.timestamp,
                )?;
                let columns_tid = TableId::new("system_schema", "columns");
                for col_row in result.column_rows {
                    self.engine
                        .write(&columns_tid, &col_row.key, col_row.row, result.timestamp)?;
                }
            }
            SystemTableMutation::TableDropped { keyspace, table } => {
                let ts = now_micros();
                let key = DecoratedKey::new(PartitionKey::new(keyspace.as_bytes().to_vec()));

                // Tombstone the tables row.
                let tables_tid = TableId::new("system_schema", "tables");
                let row = Row {
                    clustering: table.as_bytes().to_vec(),
                    cells: vec![],
                    deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
                    primary_key_liveness: LivenessInfo::NONE,
                };
                self.engine.write(&tables_tid, &key, row, ts)?;
            }
            SystemTableMutation::RoleCreated(role) => {
                let member_of = role.member_of.clone();
                let (key, row, ts) = role_to_row(&role);
                let tid = TableId::new("system_auth", "roles");
                self.engine.write(&tid, &key, row, ts)?;

                let members_tid = TableId::new("system_auth", "role_members");
                for parent in &member_of {
                    let member_key =
                        DecoratedKey::new(PartitionKey::new(role.name.as_bytes().to_vec()));
                    let member_row = Row {
                        clustering: parent.as_bytes().to_vec(),
                        cells: vec![],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(ts),
                    };
                    self.engine
                        .write(&members_tid, &member_key, member_row, ts)?;
                }
            }
            SystemTableMutation::RoleDropped(name) => {
                let ts = now_micros();
                let key = DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec()));

                // Tombstone in system_auth.roles.
                let tid = TableId::new("system_auth", "roles");
                let row = Row {
                    clustering: vec![],
                    cells: vec![],
                    deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
                    primary_key_liveness: LivenessInfo::NONE,
                };
                self.engine.write(&tid, &key, row, ts)?;

                // Tombstone in system_auth.role_permissions.
                let perms_tid = TableId::new("system_auth", "role_permissions");
                let perms_row = Row {
                    clustering: vec![],
                    cells: vec![],
                    deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
                    primary_key_liveness: LivenessInfo::NONE,
                };
                self.engine.write(&perms_tid, &key.clone(), perms_row, ts)?;
            }
            SystemTableMutation::GrantUpdated(grant) => {
                let (key, row, ts) = grant_to_row(&grant);
                let tid = TableId::new("system_auth", "role_permissions");
                self.engine.write(&tid, &key, row, ts)?;
            }
            SystemTableMutation::PermissionRevoked {
                role,
                resource,
                permission: _,
            } => {
                // For simplicity, tombstone the entire resource row.
                // The next GrantUpdated will re-write remaining permissions.
                let ts = now_micros();
                let key = DecoratedKey::new(PartitionKey::new(role.as_bytes().to_vec()));
                let tid = TableId::new("system_auth", "role_permissions");
                let row = Row {
                    clustering: resource.to_string().as_bytes().to_vec(),
                    cells: vec![],
                    deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
                    primary_key_liveness: LivenessInfo::NONE,
                };
                self.engine.write(&tid, &key, row, ts)?;
            }
            SystemTableMutation::IndexCreated(index) => {
                let row = index_to_rows(&index);
                let ts = now_micros();
                let tid = TableId::new("system_schema", "indexes");
                self.engine.write(&tid, &row.key, row.row, ts)?;
            }
            SystemTableMutation::IndexDropped {
                keyspace,
                table,
                name,
            } => {
                let ts = now_micros();
                let key = DecoratedKey::new(PartitionKey::new(keyspace.as_bytes().to_vec()));

                // Composite clustering: [u16 len][table][u16 len][index_name].
                let mut clustering = Vec::new();
                clustering.extend_from_slice(&(table.len() as u16).to_be_bytes());
                clustering.extend_from_slice(table.as_bytes());
                clustering.extend_from_slice(&(name.len() as u16).to_be_bytes());
                clustering.extend_from_slice(name.as_bytes());

                let tid = TableId::new("system_schema", "indexes");
                let row = Row {
                    clustering,
                    cells: vec![],
                    deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
                    primary_key_liveness: LivenessInfo::NONE,
                };
                self.engine.write(&tid, &key, row, ts)?;
            }
        }
        Ok(())
    }
}
