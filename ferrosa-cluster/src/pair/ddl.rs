//! DDL coordination for pair mode.
//!
//! Provides `DdlOperation` (the serializable DDL enum), `DdlCoordinator`
//! (routes DDL to primary authority), and RPC handlers for forwarding and
//! schema sync.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::raft::NodeInfo;
use ferrosa_common::CqlType;
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_schema::metadata::aggregate::UserAggregateMetadata;
use ferrosa_schema::metadata::function::UserFunctionMetadata;
use ferrosa_schema::metadata::index::IndexMetadata;
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, KeyspaceUpdates};
use ferrosa_schema::metadata::table::{TableMetadata, TableUpdates};
use ferrosa_schema::metadata::user_type::UserTypeMetadata;
use ferrosa_schema::{
    is_system_keyspace, GrantEntry, Permission, Resource, RoleMetadata, RoleUpdates, Schema,
    SchemaSnapshot,
};
use ferrosa_storage::engine::StorageEngine;

use crate::error::{ClusterError, Result};
use crate::pair::PairRole;

/// Timeout for peer-to-peer DDL RPC calls.
const DDL_RPC_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// DdlOperation
// ---------------------------------------------------------------------------

/// A single DDL operation that can be forwarded and replicated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DdlOperation {
    CreateKeyspace(KeyspaceMetadata),
    DropKeyspace(String),
    CreateTable(Box<TableMetadata>),
    DropTable {
        keyspace: String,
        table: String,
    },
    AlterKeyspace {
        name: String,
        updates: KeyspaceUpdates,
    },
    AlterTable {
        keyspace: String,
        table: String,
        updates: Box<TableUpdates>,
    },
    CreateRole(RoleMetadata),
    AlterRole {
        name: String,
        updates: RoleUpdates,
    },
    DropRole(String),
    Grant(GrantEntry),
    Revoke {
        role: String,
        resource: Resource,
        permission: Permission,
    },
    /// Grant role membership: add `granted_role` to `member`'s `member_of`.
    /// Additive (one edge) so concurrent role grants replicate without
    /// clobbering each other.
    GrantRole {
        member: String,
        granted_role: String,
    },
    /// Revoke role membership: remove `granted_role` from `member`'s `member_of`.
    RevokeRole {
        member: String,
        granted_role: String,
    },
    CreateIndex(IndexMetadata),
    DropIndex {
        keyspace: String,
        table: String,
        index: String,
    },
    CreateType(UserTypeMetadata),
    DropType {
        keyspace: String,
        name: String,
    },
    CreateFunction(UserFunctionMetadata),
    DropFunction {
        keyspace: String,
        name: String,
        arg_types: Vec<CqlType>,
    },
    CreateAggregate(UserAggregateMetadata),
    DropAggregate {
        keyspace: String,
        name: String,
        arg_types: Vec<CqlType>,
    },
    /// Topology operation: add this node to the cluster voter set.
    ///
    /// Forwarded by a rejoining node to the existing Raft leader so the
    /// leader can call `client_write(RaftOp::JoinNode(..))` on behalf of
    /// the rejoiner.  Not used in pair-mode DDL coordination; present here
    /// so the `PairDdlForward` RPC path (which `ClusterDdlForwardHandler`
    /// already handles) can carry it without a new message type.
    JoinNode(NodeInfo),
}

impl DdlOperation {
    /// Serialize to JSON bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| ClusterError::Internal(format!("DdlOperation serialize: {e}")))
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| ClusterError::Internal(format!("DdlOperation deserialize: {e}")))
    }
}

// ---------------------------------------------------------------------------
// DdlEnvelope
// ---------------------------------------------------------------------------

/// Envelope carrying a DDL operation plus a primary-generated schema version.
///
/// The primary generates the UUID when applying DDL. The secondary receives
/// the same UUID so both nodes converge on identical schema versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdlEnvelope {
    pub op: DdlOperation,
    pub schema_version: Uuid,
}

impl DdlEnvelope {
    pub fn to_bytes(&self) -> Result<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| ClusterError::Internal(format!("DdlEnvelope serialize: {e}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| ClusterError::Internal(format!("DdlEnvelope deserialize: {e}")))
    }
}

// ---------------------------------------------------------------------------
// DdlCoordinator
// ---------------------------------------------------------------------------

/// Coordinates DDL in pair mode.
///
/// Primary: applies DDL locally, then replicates to secondary.
/// Secondary: forwards to primary (which applies + replicates back).
pub struct DdlCoordinator {
    role: Arc<ArcSwap<PairRole>>,
    peer_host_id: Uuid,
    pub(crate) schema: Arc<Schema>,
    engine: Arc<StorageEngine>,
    peer_manager: Arc<PeerManager>,
}

impl DdlCoordinator {
    pub fn new(
        role: Arc<ArcSwap<PairRole>>,
        peer_host_id: Uuid,
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
        peer_manager: Arc<PeerManager>,
    ) -> Self {
        Self {
            role,
            peer_host_id,
            schema,
            engine,
            peer_manager,
        }
    }

    /// Route a DDL operation based on current role.
    ///
    /// On the primary: applies DDL locally first, then best-effort
    /// replicates to the secondary. If replication fails, the DDL
    /// still succeeds — the secondary will catch up via schema sync
    /// when it reconnects.
    ///
    /// On the secondary: forwards to the primary. The secondary does
    /// not accept CQL connections, so this path only runs for internal
    /// operations.
    pub async fn coordinate_ddl(&self, op: DdlOperation) -> Result<()> {
        match **self.role.load() {
            PairRole::Primary => {
                self.apply_ddl_locally(&op)?;
                let version = Uuid::new_v4();
                self.schema.set_schema_version(version);
                if let Err(e) = self.replicate_ddl(&op, version).await {
                    tracing::warn!("pair DDL replication failed (applied locally): {e}");
                }
                Ok(())
            }
            PairRole::Secondary => self.forward_ddl(&op).await,
        }
    }

    /// Apply a DDL operation to the local schema and storage engine.
    pub(crate) fn apply_ddl_locally(&self, op: &DdlOperation) -> Result<()> {
        match op {
            DdlOperation::CreateKeyspace(ks) => {
                self.schema
                    .create_keyspace_internal(ks.clone())
                    .map_err(|e| ClusterError::Internal(format!("create_keyspace: {e}")))?;
            }
            DdlOperation::DropKeyspace(name) => {
                // Collect table IDs before dropping from schema so we can unregister them.
                let snap = self.schema.snapshot();
                let table_ids: Vec<_> = snap
                    .tables
                    .keys()
                    .filter(|(ks, _)| ks == name)
                    .map(|(ks, tbl)| ferrosa_storage::TableId::new(ks, tbl))
                    .collect();
                self.schema
                    .drop_keyspace_internal(name)
                    .map_err(|e| ClusterError::Internal(format!("drop_keyspace: {e}")))?;
                for tid in &table_ids {
                    self.engine
                        .unregister_table(tid)
                        .map_err(ClusterError::Storage)?;
                }
            }
            DdlOperation::CreateTable(table) => {
                self.schema
                    .create_table_internal(*table.clone())
                    .map_err(|e| ClusterError::Internal(format!("create_table: {e}")))?;
                let storage_schema = table.to_storage_schema();
                self.engine
                    .register_table(storage_schema)
                    .map_err(ClusterError::Storage)?;
            }
            DdlOperation::DropTable { keyspace, table } => {
                self.schema
                    .drop_table_internal(keyspace, table)
                    .map_err(|e| ClusterError::Internal(format!("drop_table: {e}")))?;
                let tid = ferrosa_storage::TableId::new(keyspace, table);
                self.engine
                    .unregister_table(&tid)
                    .map_err(ClusterError::Storage)?;
            }
            DdlOperation::AlterKeyspace { name, updates } => {
                self.schema
                    .alter_keyspace_internal(name, updates.clone())
                    .map_err(|e| ClusterError::Internal(format!("alter_keyspace: {e}")))?;
            }
            DdlOperation::AlterTable {
                keyspace,
                table,
                updates,
            } => {
                self.schema
                    .alter_table_internal(keyspace, table, *updates.clone())
                    .map_err(|e| ClusterError::Internal(format!("alter_table: {e}")))?;
                // Propagate the post-ALTER column set to the storage engine.
                // See bug-sstable-writer-produces-zero-byte-rows-db.md.
                let snap = self.schema.snapshot();
                if let Some(tbl) = snap.tables.get(&(keyspace.clone(), table.clone())) {
                    let tid = ferrosa_storage::TableId::new(keyspace, table);
                    self.engine
                        .update_table_schema(&tid, tbl.to_storage_schema())
                        .map_err(ClusterError::Storage)?;
                }
            }
            DdlOperation::CreateRole(role) => {
                self.schema
                    .create_role_internal(role.clone())
                    .map_err(|e| ClusterError::Internal(format!("create_role: {e}")))?;
            }
            DdlOperation::AlterRole { name, updates } => {
                self.schema
                    .alter_role_internal(name, updates.clone())
                    .map_err(|e| ClusterError::Internal(format!("alter_role: {e}")))?;
            }
            DdlOperation::DropRole(name) => {
                self.schema
                    .drop_role_internal(name)
                    .map_err(|e| ClusterError::Internal(format!("drop_role: {e}")))?;
            }
            DdlOperation::Grant(entry) => {
                self.schema
                    .grant_internal(entry.clone())
                    .map_err(|e| ClusterError::Internal(format!("grant: {e}")))?;
            }
            DdlOperation::Revoke {
                role,
                resource,
                permission,
            } => {
                self.schema
                    .revoke_internal(role, resource, permission)
                    .map_err(|e| ClusterError::Internal(format!("revoke: {e}")))?;
            }
            DdlOperation::GrantRole {
                member,
                granted_role,
            } => {
                self.schema
                    .grant_role_internal(member, granted_role)
                    .map_err(|e| ClusterError::Internal(format!("grant_role: {e}")))?;
            }
            DdlOperation::RevokeRole {
                member,
                granted_role,
            } => {
                self.schema
                    .revoke_role_internal(member, granted_role)
                    .map_err(|e| ClusterError::Internal(format!("revoke_role: {e}")))?;
            }
            DdlOperation::CreateIndex(ref idx) => {
                self.schema
                    .create_index_internal(idx.clone())
                    .map_err(|e| ClusterError::Internal(format!("create_index: {e}")))?;
                crate::system_table_writer::SystemTableWriter::new(Arc::clone(&self.engine))
                    .apply(
                        ferrosa_schema::system::persistence::SystemTableMutation::IndexCreated(
                            idx.clone(),
                        ),
                    )
                    .map_err(ClusterError::Storage)?;
            }
            DdlOperation::DropIndex {
                ref keyspace,
                ref table,
                ref index,
            } => {
                self.schema
                    .drop_index_internal(keyspace, table, index)
                    .map_err(|e| ClusterError::Internal(format!("drop_index: {e}")))?;
                crate::system_table_writer::SystemTableWriter::new(Arc::clone(&self.engine))
                    .apply(
                        ferrosa_schema::system::persistence::SystemTableMutation::IndexDropped {
                            keyspace: keyspace.clone(),
                            table: table.clone(),
                            name: index.clone(),
                        },
                    )
                    .map_err(ClusterError::Storage)?;
            }
            DdlOperation::CreateType(ref udt) => {
                self.schema
                    .create_type_internal(udt)
                    .map_err(|e| ClusterError::Internal(format!("create_type: {e}")))?;
                crate::system_table_writer::SystemTableWriter::new(Arc::clone(&self.engine))
                    .apply(
                        ferrosa_schema::system::persistence::SystemTableMutation::TypeCreated(
                            udt.clone(),
                        ),
                    )
                    .map_err(ClusterError::Storage)?;
            }
            DdlOperation::DropType {
                ref keyspace,
                ref name,
            } => {
                self.schema
                    .drop_type_internal(keyspace, name)
                    .map_err(|e| ClusterError::Internal(format!("drop_type: {e}")))?;
                crate::system_table_writer::SystemTableWriter::new(Arc::clone(&self.engine))
                    .apply(
                        ferrosa_schema::system::persistence::SystemTableMutation::TypeDropped {
                            keyspace: keyspace.clone(),
                            name: name.clone(),
                        },
                    )
                    .map_err(ClusterError::Storage)?;
            }
            DdlOperation::CreateFunction(ref func) => {
                self.schema
                    .create_function_internal(func)
                    .map_err(|e| ClusterError::Internal(format!("create_function: {e}")))?;
                crate::system_table_writer::SystemTableWriter::new(Arc::clone(&self.engine))
                    .apply(
                        ferrosa_schema::system::persistence::SystemTableMutation::FunctionCreated(
                            func.clone(),
                        ),
                    )
                    .map_err(ClusterError::Storage)?;
            }
            DdlOperation::DropFunction {
                ref keyspace,
                ref name,
                ref arg_types,
            } => {
                self.schema
                    .drop_function_internal(keyspace, name, arg_types)
                    .map_err(|e| ClusterError::Internal(format!("drop_function: {e}")))?;
                crate::system_table_writer::SystemTableWriter::new(Arc::clone(&self.engine))
                    .apply(
                        ferrosa_schema::system::persistence::SystemTableMutation::FunctionDropped {
                            keyspace: keyspace.clone(),
                            name: name.clone(),
                            arg_types: arg_types.clone(),
                        },
                    )
                    .map_err(ClusterError::Storage)?;
            }
            DdlOperation::CreateAggregate(ref agg) => {
                self.schema
                    .create_aggregate_internal(agg)
                    .map_err(|e| ClusterError::Internal(format!("create_aggregate: {e}")))?;
            }
            DdlOperation::DropAggregate {
                ref keyspace,
                ref name,
                ref arg_types,
            } => {
                self.schema
                    .drop_aggregate_internal(keyspace, name, arg_types)
                    .map_err(|e| ClusterError::Internal(format!("drop_aggregate: {e}")))?;
            }
            DdlOperation::JoinNode(_) => {
                // Topology-only operation — not applied locally in pair mode.
                // In cluster mode this is forwarded to the leader and executed
                // via client_write(RaftOp::JoinNode(..)).
            }
        }
        Ok(())
    }

    /// Send a DDL operation to the peer (as primary replicating to secondary)
    /// and wait for ACK with timeout.
    pub(crate) async fn replicate_ddl(
        &self,
        op: &DdlOperation,
        schema_version: Uuid,
    ) -> Result<()> {
        let envelope = DdlEnvelope {
            op: op.clone(),
            schema_version,
        };
        let body = envelope.to_bytes()?;
        let resp = tokio::time::timeout(
            DDL_RPC_TIMEOUT,
            self.peer_manager
                .send(self.peer_host_id, Message::PairDdlForward(body), Lane::Data),
        )
        .await
        .map_err(|_| ClusterError::Internal("DDL replication timed out".into()))?
        .map_err(ClusterError::Net)?;

        match resp {
            Message::PairDdlAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairDdlAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Forward a DDL operation to the primary (as secondary) and wait for ACK
    /// with timeout.
    async fn forward_ddl(&self, op: &DdlOperation) -> Result<()> {
        let body = op.to_bytes()?;
        let resp = tokio::time::timeout(
            DDL_RPC_TIMEOUT,
            self.peer_manager
                .send(self.peer_host_id, Message::PairDdlForward(body), Lane::Data),
        )
        .await
        .map_err(|_| ClusterError::Internal("DDL forward to primary timed out".into()))?
        .map_err(ClusterError::Net)?;

        match resp {
            Message::PairDdlAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairDdlAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Get current role.
    pub fn role(&self) -> PairRole {
        **self.role.load()
    }
}

// ---------------------------------------------------------------------------
// PairDdlForwardHandler
// ---------------------------------------------------------------------------

/// Handles incoming `PairDdlForward` messages.
///
/// Primary: applies locally + replicates to secondary, then ACKs.
/// Secondary: applies locally, then ACKs (no further replication).
pub struct PairDdlForwardHandler {
    role: Arc<ArcSwap<PairRole>>,
    coordinator: Arc<DdlCoordinator>,
}

impl PairDdlForwardHandler {
    pub fn new(role: Arc<ArcSwap<PairRole>>, coordinator: Arc<DdlCoordinator>) -> Self {
        Self { role, coordinator }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairDdlForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::PairDdlForward(b) => b,
            _ => return None,
        };

        // Try DdlEnvelope first, fall back to bare DdlOperation.
        let (op, schema_version) = match DdlEnvelope::from_bytes(&body) {
            Ok(env) => (env.op, Some(env.schema_version)),
            Err(_) => match DdlOperation::from_bytes(&body) {
                Ok(op) => (op, None),
                Err(e) => {
                    tracing::error!("failed to decode PairDdlForward: {e}");
                    return None;
                }
            },
        };

        let result = match **self.role.load() {
            PairRole::Primary => {
                // Forwarded DDL from secondary: apply locally, ACK immediately.
                // Replicate back to secondary in background to avoid deadlock
                // (calling replicate_ddl inside the RPC handler would block
                // the dispatch loop, creating a circular wait).
                if let Err(e) = self.coordinator.apply_ddl_locally(&op) {
                    tracing::error!("failed to apply forwarded DDL: {e}");
                    return None;
                }
                let version = Uuid::new_v4();
                self.coordinator.schema.set_schema_version(version);

                let coord = Arc::clone(&self.coordinator);
                let op_clone = op.clone();
                ferrosa_net::task_pool::TaskPool::current("pair-ddl-replicate").spawn(async move {
                    if let Err(e) = coord.replicate_ddl(&op_clone, version).await {
                        tracing::warn!("pair DDL replication-back failed: {e}");
                    }
                });

                Ok(())
            }
            PairRole::Secondary => {
                // Replicated DDL from primary: apply + set version
                let res = self.coordinator.apply_ddl_locally(&op);
                if res.is_ok() {
                    if let Some(v) = schema_version {
                        self.coordinator.schema.set_schema_version(v);
                    }
                }
                res
            }
        };

        match result {
            Ok(()) => Some(Message::PairDdlAck(Bytes::new())),
            Err(e) => {
                tracing::error!("PairDdlForward handler failed: {e}");
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WireSchemaSnapshot — JSON-safe snapshot format
// ---------------------------------------------------------------------------

/// JSON-serializable version of `SchemaSnapshot`.
///
/// `SchemaSnapshot` uses `HashMap<(String, String), TableMetadata>` for tables,
/// which serde_json can't serialize (tuple keys aren't valid JSON keys).
/// This struct converts tables to a `Vec` for wire transmission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::type_complexity)]
pub struct WireSchemaSnapshot {
    pub version: Uuid,
    pub keyspaces: std::collections::HashMap<String, KeyspaceMetadata>,
    pub tables: Vec<((String, String), TableMetadata)>,
    #[serde(default)]
    pub indexes: Vec<((String, String, String), IndexMetadata)>,
    pub roles: std::collections::HashMap<String, RoleMetadata>,
    pub grants: std::collections::HashMap<String, Vec<GrantEntry>>,
    #[serde(default)]
    pub types: Vec<((String, String), UserTypeMetadata)>,
    #[serde(default)]
    pub functions: Vec<((String, String, Vec<CqlType>), UserFunctionMetadata)>,
    #[serde(default)]
    pub aggregates: Vec<((String, String, Vec<CqlType>), UserAggregateMetadata)>,
}

impl WireSchemaSnapshot {
    /// Convert from a `SchemaSnapshot` for wire transmission.
    pub fn from_snapshot(snap: &SchemaSnapshot) -> Self {
        Self {
            version: snap.version,
            keyspaces: snap.keyspaces.clone(),
            tables: snap
                .tables
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            indexes: snap
                .indexes
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            roles: snap.roles.clone(),
            grants: snap.grants.clone(),
            types: snap
                .types
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            functions: snap
                .functions
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            aggregates: snap
                .aggregates
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    /// Convert to a `SchemaSnapshot` for local application.
    pub fn into_snapshot(self) -> SchemaSnapshot {
        SchemaSnapshot {
            version: self.version,
            keyspaces: self.keyspaces,
            tables: self.tables.into_iter().collect(),
            indexes: self.indexes.into_iter().collect(),
            roles: self.roles,
            grants: self.grants,
            types: self.types.into_iter().collect(),
            functions: self.functions.into_iter().collect(),
            aggregates: self.aggregates.into_iter().collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// PairSchemaSyncHandler
// ---------------------------------------------------------------------------

/// Handles incoming `PairSchemaSync` messages during catch-up.
///
/// Deserializes the `SchemaSnapshot`, registers all non-system tables with the
/// storage engine, then applies the snapshot to the schema registry.
pub struct PairSchemaSyncHandler {
    schema: Arc<Schema>,
    engine: Arc<StorageEngine>,
}

impl PairSchemaSyncHandler {
    pub fn new(schema: Arc<Schema>, engine: Arc<StorageEngine>) -> Self {
        Self { schema, engine }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairSchemaSyncHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::PairSchemaSync(b) => b,
            _ => return None,
        };

        let wire: WireSchemaSnapshot = match serde_json::from_slice(&body) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to decode PairSchemaSync: {e}");
                return None;
            }
        };
        let snapshot = wire.into_snapshot();

        // Register each non-system table with the storage engine.
        for ((keyspace, _table_name), table) in &snapshot.tables {
            if is_system_keyspace(keyspace) {
                continue;
            }
            let storage_schema = table.to_storage_schema();
            if let Err(e) = self.engine.register_table(storage_schema) {
                tracing::error!("failed to register table during schema sync: {e}");
                return None;
            }
        }

        // Apply snapshot to schema registry.
        if let Err(e) = self.schema.apply_snapshot(snapshot) {
            tracing::error!("failed to apply schema snapshot: {e}");
            return None;
        }

        Some(Message::PairDdlAck(Bytes::new()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};

    fn test_keyspace() -> KeyspaceMetadata {
        let mut opts = HashMap::new();
        opts.insert("replication_factor".to_string(), "1".to_string());
        KeyspaceMetadata {
            name: "test_ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: opts,
            },
        }
    }

    fn test_table() -> TableMetadata {
        use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
        let mut columns = IndexMap::new();
        columns.insert(
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
        TableMetadata {
            keyspace: "test_ks".to_string(),
            name: "test_tbl".to_string(),
            id: Uuid::new_v4(),
            columns,
            partition_key: vec!["id".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        }
    }

    /// End-to-end pair-mode replication for CREATE ROLE. Replays the
    /// secondary-side path: receive a serialized `DdlEnvelope`,
    /// `from_bytes`, then `create_role_internal` on the receiver's
    /// schema (the same call `apply_ddl_locally` makes for
    /// `DdlOperation::CreateRole`). Asserts the salted_hash arrives
    /// intact on the secondary — pre-fix the coordinator put None
    /// there and login on the secondary returned `Bad credentials`.
    #[test]
    fn pair_replication_propagates_role_with_salted_hash() {
        use ferrosa_schema::auth::role::RoleMetadata;

        // Coordinator-side: hash is already populated (per the auth
        // commit's coordinator-side hashing). Build the envelope
        // exactly as `DdlCoordinator::coordinate_ddl` would.
        let role = RoleMetadata {
            name: "pair_replicated".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: Some("$2a$10$primary-side-hash".to_string()),
            member_of: HashSet::new(),
        };
        let envelope = DdlEnvelope {
            op: DdlOperation::CreateRole(role),
            schema_version: Uuid::new_v4(),
        };

        // On the wire: serialise + deserialise.
        let bytes = envelope.to_bytes().unwrap();
        let received = DdlEnvelope::from_bytes(&bytes).unwrap();

        // Secondary-side apply: this is exactly what
        // `apply_ddl_locally` for `DdlOperation::CreateRole` does.
        let secondary_schema = test_replication_schema();
        match received.op {
            DdlOperation::CreateRole(r) => {
                secondary_schema.create_role_internal(r).unwrap();
            }
            _ => panic!("expected CreateRole envelope"),
        }

        // The secondary must have the role with the hash intact.
        let replicated = secondary_schema
            .snapshot()
            .roles
            .get("pair_replicated")
            .cloned()
            .expect("role must replicate to secondary");
        assert_eq!(
            replicated.salted_hash.as_deref(),
            Some("$2a$10$primary-side-hash"),
            "secondary's salted_hash must match the primary's — was None pre-fix"
        );
        assert!(replicated.can_login);
        assert!(!replicated.is_superuser);
    }

    fn test_replication_schema() -> Arc<ferrosa_schema::Schema> {
        use ferrosa_schema::{
            AuthMethod, DeploymentMode as SchemaDeploymentMode, LogAuditSink, PasswordHasher,
            PasswordPolicy, RateLimitConfig, SchemaConfig,
        };
        let config = SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(LogAuditSink),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: SchemaDeploymentMode::Development,
        };
        Arc::new(ferrosa_schema::Schema::new(config).unwrap())
    }

    /// Regression: `DdlOperation::CreateRole` must serialise the
    /// salted_hash field. The pair/cluster CREATE ROLE bug was that
    /// the coordinator sent a role with `salted_hash: None` and the
    /// applier persisted that None — so login failed for every role
    /// created via a multi-node DDL. The fix hashes on the
    /// coordinator and embeds the hash in the role; this test pins
    /// that the serialisation round-trips that hash unchanged.
    #[test]
    fn ddl_operation_create_role_preserves_salted_hash() {
        use ferrosa_schema::auth::role::RoleMetadata;
        use std::collections::HashSet;
        let role = RoleMetadata {
            name: "test_role".into(),
            is_superuser: false,
            can_login: true,
            salted_hash: Some("$2a$10$abcdef".into()),
            member_of: HashSet::new(),
        };
        let op = DdlOperation::CreateRole(role);
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::CreateRole(r) => {
                assert_eq!(r.name, "test_role");
                assert!(r.can_login);
                assert_eq!(
                    r.salted_hash.as_deref(),
                    Some("$2a$10$abcdef"),
                    "salted_hash must round-trip — was None pre-fix"
                );
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_create_keyspace_roundtrip() {
        let op = DdlOperation::CreateKeyspace(test_keyspace());
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::CreateKeyspace(ks) => assert_eq!(ks.name, "test_ks"),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_drop_keyspace_roundtrip() {
        let op = DdlOperation::DropKeyspace("test_ks".to_string());
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::DropKeyspace(name) => assert_eq!(name, "test_ks"),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_create_table_roundtrip() {
        let op = DdlOperation::CreateTable(Box::new(test_table()));
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::CreateTable(t) => {
                assert_eq!(t.keyspace, "test_ks");
                assert_eq!(t.name, "test_tbl");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_drop_table_roundtrip() {
        let op = DdlOperation::DropTable {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::DropTable { keyspace, table } => {
                assert_eq!(keyspace, "test_ks");
                assert_eq!(table, "test_tbl");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_from_bytes_invalid_json_returns_error() {
        let result = DdlOperation::from_bytes(b"not valid json at all!!!");
        assert!(result.is_err(), "expected error for invalid JSON");
        let err = result.unwrap_err();
        assert!(
            matches!(err, ClusterError::Internal(_)),
            "expected ClusterError::Internal, got {err:?}"
        );
    }

    #[test]
    fn ddl_operation_alter_keyspace_roundtrip() {
        use ferrosa_schema::metadata::keyspace::{KeyspaceUpdates, ReplicationParams};
        let mut opts = HashMap::new();
        opts.insert("replication_factor".to_string(), "3".to_string());
        let op = DdlOperation::AlterKeyspace {
            name: "ks".to_string(),
            updates: KeyspaceUpdates {
                replication: Some(ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: opts,
                }),
                durable_writes: None,
            },
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::AlterKeyspace { name, updates } => {
                assert_eq!(name, "ks");
                assert!(updates.replication.is_some());
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_alter_table_roundtrip() {
        use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
        use ferrosa_schema::metadata::table::TableUpdates;
        let op = DdlOperation::AlterTable {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            updates: Box::new(TableUpdates {
                params: None,
                add_columns: vec![ColumnMetadata {
                    name: "new_col".to_string(),
                    kind: ColumnKind::Regular,
                    position: 1,
                    column_type: "text".to_string(),
                    clustering_order: ClusteringOrder::None,
                    mask: None,
                }],
                drop_columns: vec![],
                extensions: None,
            }),
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::AlterTable {
                keyspace,
                table,
                updates,
            } => {
                assert_eq!(keyspace, "ks");
                assert_eq!(table, "tbl");
                assert_eq!(updates.add_columns.len(), 1);
                assert_eq!(updates.add_columns[0].name, "new_col");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_create_role_roundtrip() {
        use ferrosa_schema::RoleMetadata;
        let op = DdlOperation::CreateRole(RoleMetadata {
            name: "analyst".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
        });
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::CreateRole(role) => {
                assert_eq!(role.name, "analyst");
                assert!(!role.is_superuser);
                assert!(role.can_login);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_alter_role_roundtrip() {
        use ferrosa_schema::RoleUpdates;
        let op = DdlOperation::AlterRole {
            name: "analyst".to_string(),
            updates: RoleUpdates {
                is_superuser: Some(true),
                ..Default::default()
            },
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::AlterRole { name, updates } => {
                assert_eq!(name, "analyst");
                assert_eq!(updates.is_superuser, Some(true));
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_drop_role_roundtrip() {
        let op = DdlOperation::DropRole("analyst".to_string());
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::DropRole(name) => assert_eq!(name, "analyst"),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_grant_roundtrip() {
        use ferrosa_schema::{GrantEntry, Permission, Resource};
        let op = DdlOperation::Grant(GrantEntry {
            role: "analyst".to_string(),
            resource: Resource::Keyspace("ks".to_string()),
            permissions: [Permission::Select].into_iter().collect(),
        });
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::Grant(entry) => {
                assert_eq!(entry.role, "analyst");
                assert!(matches!(entry.resource, Resource::Keyspace(ref n) if n == "ks"));
                assert!(entry.permissions.contains(&Permission::Select));
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_revoke_roundtrip() {
        use ferrosa_schema::{Permission, Resource};
        let op = DdlOperation::Revoke {
            role: "analyst".to_string(),
            resource: Resource::Keyspace("ks".to_string()),
            permission: Permission::Select,
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::Revoke {
                role,
                resource,
                permission,
            } => {
                assert_eq!(role, "analyst");
                assert!(matches!(resource, Resource::Keyspace(ref n) if n == "ks"));
                assert_eq!(permission, Permission::Select);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_create_type_roundtrip() {
        use ferrosa_common::CqlType;
        let op = DdlOperation::CreateType(UserTypeMetadata {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("city".to_string(), CqlType::Varchar),
            ],
        });
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::CreateType(udt) => {
                assert_eq!(udt.keyspace, "ks");
                assert_eq!(udt.name, "address");
                assert_eq!(udt.fields.len(), 2);
                assert_eq!(udt.fields[0].0, "street");
                assert_eq!(udt.fields[1].0, "city");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_drop_type_roundtrip() {
        let op = DdlOperation::DropType {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::DropType { keyspace, name } => {
                assert_eq!(keyspace, "ks");
                assert_eq!(name, "address");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_create_index_roundtrip() {
        use ferrosa_index::IndexType;
        use ferrosa_schema::metadata::index::IndexMetadata;
        let op = DdlOperation::CreateIndex(IndexMetadata {
            keyspace: "qa_pair".to_string(),
            table: "kv".to_string(),
            name: "qa_pair_v_idx".to_string(),
            index_type: IndexType::Hash,
            target_columns: vec!["v".to_string()],
            filter_predicate: None,
            options: std::collections::HashMap::new(),
        });
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::CreateIndex(idx) => {
                assert_eq!(idx.keyspace, "qa_pair");
                assert_eq!(idx.table, "kv");
                assert_eq!(idx.name, "qa_pair_v_idx");
                assert!(matches!(idx.index_type, IndexType::Hash));
                assert_eq!(idx.target_columns, vec!["v"]);
                assert!(idx.filter_predicate.is_none());
                assert!(idx.options.is_empty());
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_drop_index_roundtrip() {
        let op = DdlOperation::DropIndex {
            keyspace: "qa_pair".to_string(),
            table: "kv".to_string(),
            index: "qa_pair_v_idx".to_string(),
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::DropIndex {
                keyspace,
                table,
                index,
            } => {
                assert_eq!(keyspace, "qa_pair");
                assert_eq!(table, "kv");
                assert_eq!(index, "qa_pair_v_idx");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn wire_schema_snapshot_roundtrip_with_tables() {
        let mut tables = std::collections::HashMap::new();
        tables.insert(
            ("test_ks".to_string(), "test_tbl".to_string()),
            test_table(),
        );
        let mut keyspaces = std::collections::HashMap::new();
        keyspaces.insert("test_ks".to_string(), test_keyspace());

        let snap = SchemaSnapshot {
            version: Uuid::new_v4(),
            keyspaces,
            tables,
            indexes: std::collections::HashMap::new(),
            roles: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
            types: std::collections::HashMap::new(),
            functions: std::collections::HashMap::new(),
            aggregates: std::collections::HashMap::new(),
        };

        let wire = WireSchemaSnapshot::from_snapshot(&snap);
        assert_eq!(wire.tables.len(), 1, "should have one table entry");
        assert_eq!(wire.keyspaces.len(), 1, "should have one keyspace");

        let restored = wire.into_snapshot();
        let key = ("test_ks".to_string(), "test_tbl".to_string());
        let tbl = restored.tables.get(&key);
        assert!(tbl.is_some(), "table should be present after roundtrip");
        assert_eq!(tbl.unwrap().name, "test_tbl");
        assert_eq!(tbl.unwrap().keyspace, "test_ks");
    }

    #[test]
    fn wire_schema_snapshot_json_roundtrip() {
        // Verifies the serde_json path used by send_schema_sync_to_peer.
        let mut tables = std::collections::HashMap::new();
        tables.insert(
            ("test_ks".to_string(), "test_tbl".to_string()),
            test_table(),
        );

        let snap = SchemaSnapshot {
            version: Uuid::new_v4(),
            keyspaces: std::collections::HashMap::new(),
            tables,
            indexes: std::collections::HashMap::new(),
            roles: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
            types: std::collections::HashMap::new(),
            functions: std::collections::HashMap::new(),
            aggregates: std::collections::HashMap::new(),
        };

        let wire = WireSchemaSnapshot::from_snapshot(&snap);
        let json = serde_json::to_vec(&wire).expect("serialization should succeed");
        assert!(!json.is_empty(), "JSON bytes should not be empty");

        let decoded: WireSchemaSnapshot =
            serde_json::from_slice(&json).expect("deserialization should succeed");
        let restored = decoded.into_snapshot();
        let key = ("test_ks".to_string(), "test_tbl".to_string());
        assert!(
            restored.tables.contains_key(&key),
            "table should survive JSON roundtrip"
        );
    }

    #[test]
    fn wire_schema_snapshot_preserves_types() {
        use ferrosa_common::CqlType;
        let mut types = std::collections::HashMap::new();
        types.insert(
            ("ks".to_string(), "address".to_string()),
            UserTypeMetadata {
                keyspace: "ks".to_string(),
                name: "address".to_string(),
                fields: vec![("street".to_string(), CqlType::Varchar)],
            },
        );

        let snap = SchemaSnapshot {
            version: Uuid::new_v4(),
            keyspaces: std::collections::HashMap::new(),
            tables: std::collections::HashMap::new(),
            indexes: std::collections::HashMap::new(),
            roles: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
            types,
            functions: std::collections::HashMap::new(),
            aggregates: std::collections::HashMap::new(),
        };

        let wire = WireSchemaSnapshot::from_snapshot(&snap);
        assert_eq!(wire.types.len(), 1);

        let restored = wire.into_snapshot();
        let udt = restored
            .types
            .get(&("ks".to_string(), "address".to_string()));
        assert!(udt.is_some());
        assert_eq!(udt.unwrap().fields.len(), 1);
    }
}
