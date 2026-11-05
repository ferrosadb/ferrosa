//! Writes system table mutations to the storage engine.
//!
//! [`SystemTableWriter`] bridges `ferrosa-schema` (which defines the mutation
//! types and row conversion functions) with `ferrosa-storage` (which provides
//! the write API). This module lives in `ferrosa-cluster` because it depends
//! on both crates, and placing it in either would create a circular dependency.
//! Correctness: ordered projections inspect bounded physical metadata, including hidden tombstones.
//! Last revised: 2026-11-05
//! Last changed: order role deletion and regrant beyond stored partition/row clocks.

use std::sync::Arc;

use ferrosa_common::{DecoratedKey, PartitionKey};
use ferrosa_schema::system::persistence::{
    function_clustering, function_to_row, grant_to_row, index_to_rows, keyspace_to_row,
    role_to_row, table_to_rows, type_to_row, SystemTableMutation,
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
#[derive(Clone)]
pub struct SystemTableWriter {
    engine: Arc<StorageEngine>,
    #[cfg(test)]
    permission_timestamp: Option<i64>,
    /// Test-only observability seam: records the OS thread on which the most
    /// recent [`SystemTableWriter::apply`] ran. The raft apply-offload
    /// regression test uses this to prove the synchronous system-table write
    /// executes on a blocking thread rather than inline on the raft worker.
    /// Per-instance (not global) so parallel tests cannot race the recording.
    #[cfg(test)]
    last_apply_thread: Arc<std::sync::Mutex<Option<std::thread::ThreadId>>>,
}

impl SystemTableWriter {
    /// Create a new writer backed by the given storage engine.
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self {
            engine,
            #[cfg(test)]
            permission_timestamp: None,
            #[cfg(test)]
            last_apply_thread: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Test-only handle to the thread recorder; see [`Self::last_apply_thread`].
    #[cfg(test)]
    pub(crate) fn last_apply_thread_handle(
        &self,
    ) -> Arc<std::sync::Mutex<Option<std::thread::ThreadId>>> {
        Arc::clone(&self.last_apply_thread)
    }

    #[cfg(test)]
    pub(crate) fn set_permission_timestamp_for_test(&mut self, timestamp: i64) {
        self.permission_timestamp = Some(timestamp);
    }

    fn permission_timestamp(&self) -> i64 {
        #[cfg(test)]
        if let Some(timestamp) = self.permission_timestamp {
            return timestamp;
        }
        now_micros()
    }

    /// Compute a node-local successor from bounded physical metadata pages.
    /// Raft serializes canonical projection mutation; other simultaneous writers
    /// are not supported by this ordering contract. Logical reads are unsuitable:
    /// an older partition delete can hide a newer row tombstone whose PK is old.
    /// `target=None` covers the entire partition before a partition delete.
    fn next_projection_timestamp(
        &self,
        table: &TableId,
        key: &DecoratedKey,
        target: Option<&[u8]>,
        wall: i64,
    ) -> ferrosa_common::Result<i64> {
        let mut previous = i64::MIN;
        let mut cursor: Option<Vec<u8>> = None;
        'pages: loop {
            let Some(partition) =
                self.engine
                    .read_projection_rows(table, key, cursor.as_deref(), 32)?
            else {
                break;
            };
            previous = previous.max(partition.deletion.marked_for_delete_at);
            for row in &partition.rows {
                if let Some(target) = target {
                    match row.clustering.as_slice().cmp(target) {
                        std::cmp::Ordering::Less => continue,
                        std::cmp::Ordering::Greater => break 'pages,
                        std::cmp::Ordering::Equal => {}
                    }
                }
                previous = previous
                    .max(row.deletion.marked_for_delete_at)
                    .max(row.primary_key_liveness.timestamp);
                for (_, cell) in &row.cells {
                    previous = previous.max(cell.timestamp);
                }
                if target.is_some() {
                    break 'pages;
                }
            }
            let Some(last) = partition.rows.last() else {
                break;
            };
            cursor = Some(last.clustering.clone());
        }
        Ok(wall.max(previous.checked_add(1).ok_or_else(|| {
            ferrosa_common::Error::InvalidData("canonical projection timestamp exhausted".into())
        })?))
    }

    fn next_role_timestamp(&self, key: &DecoratedKey, wall: i64) -> ferrosa_common::Result<i64> {
        self.next_projection_timestamp(&TableId::new("system_auth", "roles"), key, Some(&[]), wall)
    }

    fn next_permission_timestamp(
        &self,
        key: &DecoratedKey,
        resource: &[u8],
    ) -> ferrosa_common::Result<i64> {
        self.next_projection_timestamp(
            &TableId::new("system_auth", "role_permissions"),
            key,
            Some(resource),
            self.permission_timestamp(),
        )
    }

    /// Apply a system table mutation by writing to the storage engine.
    pub fn apply(&self, mutation: SystemTableMutation) -> ferrosa_common::Result<()> {
        #[cfg(test)]
        {
            *self.last_apply_thread.lock().unwrap() = Some(std::thread::current().id());
        }
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
                let (key, mut row, wall) = role_to_row(&role);
                let tid = TableId::new("system_auth", "roles");
                let ts = self.next_role_timestamp(&key, wall)?;
                row.primary_key_liveness.timestamp = ts;
                for (_, cell) in &mut row.cells {
                    cell.timestamp = ts;
                }
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
                let wall = now_micros();
                let key = DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec()));
                let tid = TableId::new("system_auth", "roles");
                let perms_tid = TableId::new("system_auth", "role_permissions");
                // Preflight both successors before the first effect, including
                // overflow, then dominate every stored grant in bounded pages.
                let role_ts = self.next_projection_timestamp(&tid, &key, None, wall)?;
                let perms_ts = self.next_projection_timestamp(&perms_tid, &key, None, wall)?;
                for (table, ts) in [(tid, role_ts), (perms_tid, perms_ts)] {
                    let row = Row {
                        clustering: vec![],
                        cells: vec![],
                        deletion: DeletionTime::new(ts, (wall / 1_000_000) as u32),
                        primary_key_liveness: LivenessInfo::NONE,
                    };
                    self.engine.write(&table, &key, row, ts)?;
                }
            }
            SystemTableMutation::GrantUpdated(grant) => {
                let (key, mut row, _) = grant_to_row(&grant);
                let ts = self.next_permission_timestamp(&key, &row.clustering)?;
                row.primary_key_liveness.timestamp = ts;
                for (_, cell) in &mut row.cells {
                    cell.timestamp = ts;
                }
                let tid = TableId::new("system_auth", "role_permissions");
                self.engine.write(&tid, &key, row, ts)?;
            }
            SystemTableMutation::PermissionRevoked {
                role,
                resource,
                permission: _,
            } => {
                // Only an empty post-state is tombstoned. A subsequent grant
                // must sort after this deletion even under clock regression.
                let key = DecoratedKey::new(PartitionKey::new(role.as_bytes().to_vec()));
                let ts = self.next_permission_timestamp(&key, resource.to_string().as_bytes())?;
                let tid = TableId::new("system_auth", "role_permissions");
                let row = Row {
                    clustering: resource.to_string().as_bytes().to_vec(),
                    cells: vec![],
                    deletion: DeletionTime::new(ts, (now_micros() / 1_000_000) as u32),
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
            SystemTableMutation::TypeCreated(udt) => {
                let row = type_to_row(&udt);
                let ts = now_micros();
                let tid = TableId::new("system_schema", "types");
                self.engine.write(&tid, &row.key, row.row, ts)?;
            }
            SystemTableMutation::TypeDropped { keyspace, name } => {
                let ts = now_micros();
                let key = DecoratedKey::new(PartitionKey::new(keyspace.as_bytes().to_vec()));
                let tid = TableId::new("system_schema", "types");
                // Clustering = type_name (single text column).
                let row = Row {
                    clustering: name.as_bytes().to_vec(),
                    cells: vec![],
                    deletion: DeletionTime::new(ts, (ts / 1_000_000) as u32),
                    primary_key_liveness: LivenessInfo::NONE,
                };
                self.engine.write(&tid, &key, row, ts)?;
            }
            SystemTableMutation::FunctionCreated(func) => {
                let row = function_to_row(&func);
                let ts = now_micros();
                let tid = TableId::new("system_schema", "functions");
                self.engine.write(&tid, &row.key, row.row, ts)?;
            }
            SystemTableMutation::FunctionDropped {
                keyspace,
                name,
                arg_types,
            } => {
                let ts = now_micros();
                let key = DecoratedKey::new(PartitionKey::new(keyspace.as_bytes().to_vec()));
                let tid = TableId::new("system_schema", "functions");
                // Composite clustering matches the create path so the tombstone
                // masks the matching overload row.
                let row = Row {
                    clustering: function_clustering(&name, &arg_types),
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
