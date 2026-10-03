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
                self.apply_ddl_locally(&op).await?;
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
    pub(crate) async fn apply_ddl_locally(&self, op: &DdlOperation) -> Result<()> {
        let _pauses =
            crate::ddl_path::pause_ddl_compactions(op, &self.schema, &self.engine).await?;
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
                crate::ddl_path::build_replicated_index(
                    &self.engine,
                    idx,
                    self.schema
                        .snapshot()
                        .tables
                        .get(&(idx.keyspace.clone(), idx.table.clone()))
                        .map(|t| t.partition_key.as_slice())
                        .unwrap_or(&[]),
                    "replicated DDL (pair)",
                )?;
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
                self.engine
                    .drop_index(&ferrosa_storage::TableId::new(keyspace, table), index)
                    .map_err(ClusterError::Storage)?;
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
                if let Err(e) = self.coordinator.apply_ddl_locally(&op).await {
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
                let res = self.coordinator.apply_ddl_locally(&op).await;
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
    role: Arc<ArcSwap<PairRole>>,
    /// Refuse a snapshot that lacks a table holding local data, instead of
    /// dropping it. Set for the cluster shape: see [`Self::for_cluster`].
    refuse_ambiguous_drops: bool,
}

impl PairSchemaSyncHandler {
    /// Pair mode: the peer is the one primary, so a table missing from its
    /// snapshot is a DROP this node missed, and is finished as one. `role` is
    /// this node's live pair role.
    pub fn new(
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
        role: Arc<ArcSwap<PairRole>>,
    ) -> Self {
        Self {
            schema,
            engine,
            role,
            refuse_ambiguous_drops: false,
        }
    }

    /// Cluster shape. The sender is whichever node believed it was the Raft
    /// leader, which can be a deposed leader or one still applying its log, so
    /// a table absent from its snapshot is ambiguous: a missed DROP or a sender
    /// that never learned the CREATE. Drops in a cluster come from Raft
    /// (`DropTable`, or a snapshot install that already refuses this case,
    /// CL-22). A snapshot that would drop a table holding local data is
    /// refused whole, loudly, rather than deleting it.
    pub fn for_cluster(
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
        role: Arc<ArcSwap<PairRole>>,
    ) -> Self {
        Self {
            schema,
            engine,
            role,
            refuse_ambiguous_drops: true,
        }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairSchemaSyncHandler {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::PairSchemaSync(b) => b,
            _ => return None,
        };

        // Only a receiver applies a peer's schema. A snapshot arriving at a
        // primary comes from a node that wrongly believes it leads (a restarted,
        // unpromoted node elects itself by host-id order until the promoted
        // peer corrects it). Applying it would read every table the stale node
        // lacks as a missed DROP and delete it, SSTables included.
        if **self.role.load() == PairRole::Primary {
            tracing::error!(
                peer = %from.0,
                "PairSchemaSync REFUSED: this node is the pair primary and its schema is \
                 authoritative; a peer pushing its own schema believes it leads and is \
                 stale. Nothing was applied or dropped."
            );
            return None;
        }

        let wire: WireSchemaSnapshot = match serde_json::from_slice(&body) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to decode PairSchemaSync: {e}");
                return None;
            }
        };
        let snapshot = wire.into_snapshot();
        let primary_version = snapshot.version;

        if let Err(e) = self.converge(&snapshot) {
            tracing::error!(%e, "PairSchemaSync: catch-up failed; not acknowledging");
            return None;
        }
        let divergent = schema_divergence(&snapshot, &self.schema.snapshot());
        if !divergent.is_empty() {
            tracing::error!(
                ?divergent,
                "PairSchemaSync: applied the primary's schema but this node still differs; \
                 not acknowledging, so a switchover to this node is refused"
            );
            return None;
        }

        // Converged: adopt the primary's version so the primary can verify it
        // from the ack before ever handing this node the primary role.
        self.schema.set_schema_version(primary_version);
        Some(Message::PairDdlAck(Bytes::copy_from_slice(
            primary_version.as_bytes(),
        )))
    }
}

impl PairSchemaSyncHandler {
    /// Bring this node's registry and engine to the primary's snapshot:
    /// insert what is missing, replace what differs (a missed ALTER, or a
    /// missed DROP + CREATE when the table id changed), and drop what the
    /// primary no longer has (a missed DROP).
    fn converge(&self, snapshot: &SchemaSnapshot) -> std::result::Result<(), String> {
        let local = self.schema.snapshot();

        // Tables this node's schema held that the snapshot no longer does:
        // DROPs this peer missed. Only schema-owned tables are candidates, so
        // engine-internal registrations (graph adjacency, the PostgreSQL KV
        // table) are never touched.
        let dropped: Vec<(String, String)> = local
            .tables
            .keys()
            .filter(|key| !snapshot.tables.contains_key(*key) && !is_system_keyspace(&key.0))
            .cloned()
            .collect();

        // Cluster shape: absence is not drop proof. Refuse the whole snapshot
        // before anything is applied or deleted (CL-22's rule, applied to the
        // schema push).
        if self.refuse_ambiguous_drops {
            self.refuse_data_bearing_drops(&dropped, snapshot)?;
        }

        for (keyspace, ks) in &snapshot.keyspaces {
            let stale = local.keyspaces.get(keyspace).is_some_and(|mine| mine != ks);
            if is_system_keyspace(keyspace) || !stale {
                continue;
            }
            let updates = KeyspaceUpdates {
                replication: Some(ks.replication.clone()),
                durable_writes: Some(ks.durable_writes),
            };
            self.schema
                .alter_keyspace_internal(keyspace, updates)
                .map_err(|e| format!("alter keyspace {keyspace}: {e}"))?;
        }
        for ((keyspace, name), table) in &snapshot.tables {
            if is_system_keyspace(keyspace) {
                continue;
            }
            match local.tables.get(&(keyspace.clone(), name.clone())) {
                Some(mine) if table_definition_matches(mine, table) => {}
                Some(mine) => self.replace_table(mine, table)?,
                None => self
                    .engine
                    .register_table(table.to_storage_schema())
                    .map_err(|e| format!("register {keyspace}.{name}: {e}"))?,
            }
        }

        // Insert everything this node lacks (keyspaces, tables, roles, ...).
        self.schema
            .apply_snapshot(snapshot.clone())
            .map_err(|e| format!("apply snapshot: {e}"))?;

        for (keyspace, table) in dropped {
            self.finish_missed_drop(&keyspace, &table)?;
        }
        Ok(())
    }

    /// Cluster shape: fail when a table the snapshot lacks still holds local
    /// data (SSTables or persisted index registrations). Nothing is applied.
    fn refuse_data_bearing_drops(
        &self,
        dropped: &[(String, String)],
        snapshot: &SchemaSnapshot,
    ) -> std::result::Result<(), String> {
        for (keyspace, table) in dropped {
            let tid = ferrosa_storage::TableId::new(keyspace, table);
            let data_bearing =
                crate::raft::state_machine::table_has_local_artifacts(&self.engine, &tid).map_err(
                    |e| {
                        format!(
                    "schema sync refused: could not check absent table {tid} for local data: {e}"
                )
                    },
                )?;
            if data_bearing {
                return Err(format!(
                    "schema sync refused: snapshot {} lacks table {tid}, which holds local data, \
                     and in a cluster absence is not proof of a DROP (the sender may be a deposed \
                     or lagging leader); nothing applied, nothing deleted",
                    snapshot.version
                ));
            }
        }
        Ok(())
    }

    /// Replace a table this node holds under a different definition.
    fn replace_table(
        &self,
        mine: &TableMetadata,
        theirs: &TableMetadata,
    ) -> std::result::Result<(), String> {
        let tid = ferrosa_storage::TableId::new(&theirs.keyspace, &theirs.name);
        if mine.id != theirs.id {
            // A different incarnation: the primary dropped and recreated it.
            // The local SSTables belong to the dropped one.
            self.engine
                .unregister_table(&tid)
                .map_err(|e| format!("unregister superseded {tid}: {e}"))?;
            self.engine
                .register_table(theirs.to_storage_schema())
                .map_err(|e| format!("register recreated {tid}: {e}"))?;
        } else if self.engine.table_schema(&tid).is_some() {
            // A missed ALTER. update_table_schema flushes rows written under
            // the old layout first, so they keep their write-time ordinals.
            self.engine
                .update_table_schema(&tid, theirs.to_storage_schema())
                .map_err(|e| format!("update {tid} to the primary's layout: {e}"))?;
        } else {
            self.engine
                .register_table(theirs.to_storage_schema())
                .map_err(|e| format!("register {tid}: {e}"))?;
        }
        self.schema
            .replace_table_internal(theirs.clone())
            .map_err(|e| format!("replace {tid} in the registry: {e}"))?;
        tracing::info!(
            table = %tid,
            "schema sync: applied the primary's definition of a table this peer held stale"
        );
        Ok(())
    }

    /// Finish a DROP this peer missed, as `DropTable` would have: release the
    /// table in the engine, delete its local SSTable directory, and remove it
    /// from the registry.
    fn finish_missed_drop(&self, keyspace: &str, table: &str) -> std::result::Result<(), String> {
        let tid = ferrosa_storage::TableId::new(keyspace, table);
        self.engine.unregister_table(&tid).map_err(|e| {
            format!(
                "unregister of dropped {tid} failed; its local SSTables remain and a \
                 recreated table of the same name would read them: {e}"
            )
        })?;
        self.schema
            .drop_table_internal(keyspace, table)
            .map_err(|e| format!("drop {tid} from the registry: {e}"))?;
        tracing::info!(
            table = %tid,
            "schema sync: unregistered a table dropped while this peer was behind"
        );
        Ok(())
    }
}

/// Whether two definitions of the same table lay data out identically and
/// describe the same incarnation.
pub(crate) fn table_definition_matches(a: &TableMetadata, b: &TableMetadata) -> bool {
    a.id == b.id
        && a.partition_key == b.partition_key
        && a.clustering_key == b.clustering_key
        && a.columns == b.columns
        && a.flags == b.flags
        && serde_json::to_value(&a.params).ok() == serde_json::to_value(&b.params).ok()
        && serde_json::to_value(&a.extensions).ok() == serde_json::to_value(&b.extensions).ok()
}

/// User keyspaces and tables on which `local` differs from the primary's
/// `snapshot`. Empty means converged.
///
/// Scope: keyspaces and tables (the definitions that decide data layout).
/// Roles, grants, indexes, types and functions are applied insert-only and
/// are not checked here.
pub(crate) fn schema_divergence(snapshot: &SchemaSnapshot, local: &SchemaSnapshot) -> Vec<String> {
    let mut out = Vec::new();
    for (name, ks) in &snapshot.keyspaces {
        if !is_system_keyspace(name) && local.keyspaces.get(name) != Some(ks) {
            out.push(format!("keyspace {name}"));
        }
    }
    for (key, table) in &snapshot.tables {
        if is_system_keyspace(&key.0) {
            continue;
        }
        match local.tables.get(key) {
            Some(mine) if table_definition_matches(mine, table) => {}
            _ => out.push(format!("table {}.{}", key.0, key.1)),
        }
    }
    for key in local.tables.keys() {
        if !is_system_keyspace(&key.0) && !snapshot.tables.contains_key(key) {
            out.push(format!("extra table {}.{}", key.0, key.1));
        }
    }
    out
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
            scram: None,
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

    fn pair_test_engine(dir: &std::path::Path) -> Arc<StorageEngine> {
        use ferrosa_storage::engine::StorageEngineConfig;
        use ferrosa_storage::{CommitLogConfig, CompactionConfig};
        Arc::new(
            StorageEngine::new(
                StorageEngineConfig {
                    commit_log: CommitLogConfig {
                        log_dir: dir.to_path_buf(),
                        checkpoint_dir: dir.to_path_buf(),
                        archive: None,
                        ..CommitLogConfig::default()
                    },
                    compaction: CompactionConfig::from_env(dir.join("compaction")),
                    object_store: None,
                    local_cache_max_bytes: 1024 * 1024,
                    local_disk_free_reserve_bytes: 0,
                    flush_threshold_bytes: 4096,
                    memtable_backpressure_bytes: u64::MAX,
                    flush_max_age_secs: 5,
                    data_dir: dir.to_path_buf(),
                    index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
                    auth_enabled: false,
                    auth_warn: false,
                    write_verify: false,
                    max_pending_replay_mutations_without_schema: 1024,
                    memtable_num_shards: 64,
                    cache_hot_window_secs: 900,
                },
                None,
            )
            .unwrap(),
        )
    }

    fn wire_with(tables: Vec<TableMetadata>) -> WireSchemaSnapshot {
        let ks = test_keyspace();
        WireSchemaSnapshot {
            version: Uuid::new_v4(),
            keyspaces: [(ks.name.clone(), ks)].into_iter().collect(),
            tables: tables
                .into_iter()
                .map(|t| ((t.keyspace.clone(), t.name.clone()), t))
                .collect(),
            indexes: vec![],
            roles: HashMap::new(),
            grants: HashMap::new(),
            types: vec![],
            functions: vec![],
            aggregates: vec![],
        }
    }

    /// Pair-mode catch-up applies a whole schema snapshot. It registered the
    /// snapshot's tables but never unregistered one the snapshot dropped, so a
    /// peer that missed a DROP kept the table live in its engine and, after a
    /// restart, orphaned its SSTable directory (2026-09-28).
    #[tokio::test]
    async fn pair_schema_sync_unregisters_a_table_the_snapshot_dropped() {
        use ferrosa_storage::TableId;
        let dir = tempfile::tempdir().unwrap();
        let engine = pair_test_engine(dir.path());
        let schema = test_replication_schema();
        let handler = sync_handler(&schema, &engine, PairRole::Secondary);
        let keep = test_table();
        let mut gone = test_table();
        gone.name = "gone_tbl".to_string();
        gone.id = Uuid::new_v4();
        let peer = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());

        let first = serde_json::to_vec(&wire_with(vec![keep.clone(), gone.clone()])).unwrap();
        assert!(handler
            .handle(peer, Message::PairSchemaSync(Bytes::from(first)))
            .await
            .is_some());
        let gone_id = TableId::new("test_ks", "gone_tbl");
        let gone_dir = dir.path().join("sstables").join(gone_id.to_string());
        assert!(engine.table_schema(&gone_id).is_some() && gone_dir.exists());

        let second = serde_json::to_vec(&wire_with(vec![keep])).unwrap();
        assert!(handler
            .handle(peer, Message::PairSchemaSync(Bytes::from(second)))
            .await
            .is_some());
        assert!(
            engine.table_schema(&gone_id).is_none(),
            "dropped table unregistered"
        );
        assert!(!gone_dir.exists(), "and its SSTable directory removed");
        assert!(engine
            .table_schema(&TableId::new("test_ks", "test_tbl"))
            .is_some());
    }

    /// The cluster-shape handler must not delete a data-bearing table because
    /// a pushed snapshot lacks it. The sender is whichever node thought it led
    /// Raft; a deposed or lagging leader's snapshot lacks tables created after
    /// its view, and this handler used to unregister them and delete their
    /// SSTables -- the same inference from absence CL-22 refuses for Raft
    /// snapshot installs.
    #[tokio::test]
    async fn cluster_schema_sync_refuses_to_drop_a_data_bearing_table() {
        use ferrosa_storage::TableId;
        let dir = tempfile::tempdir().unwrap();
        let engine = pair_test_engine(dir.path());
        let schema = test_replication_schema();
        let handler = PairSchemaSyncHandler::for_cluster(
            Arc::clone(&schema),
            Arc::clone(&engine),
            Arc::new(ArcSwap::from_pointee(PairRole::Secondary)),
        );
        let keep = test_table();
        let mut newer = test_table();
        newer.name = "newer_tbl".to_string();
        newer.id = Uuid::new_v4();
        let peer = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());

        let current = serde_json::to_vec(&wire_with(vec![keep.clone(), newer.clone()])).unwrap();
        assert!(handler
            .handle(peer, Message::PairSchemaSync(Bytes::from(current)))
            .await
            .is_some());
        let newer_id = TableId::new("test_ks", "newer_tbl");
        let newer_dir = dir.path().join("sstables").join(newer_id.to_string());
        std::fs::write(newer_dir.join("1-Data.db"), b"local durable artifact").unwrap();

        let stale = serde_json::to_vec(&wire_with(vec![keep])).unwrap();
        let reply = handler
            .handle(peer, Message::PairSchemaSync(Bytes::from(stale)))
            .await;
        assert!(reply.is_none(), "a refused sync must not be acknowledged");
        assert!(
            engine.table_schema(&newer_id).is_some(),
            "a data-bearing table absent from a pushed snapshot must stay registered"
        );
        assert!(
            newer_dir.join("1-Data.db").exists(),
            "and its SSTables must survive"
        );
        assert!(
            schema
                .snapshot()
                .tables
                .contains_key(&("test_ks".to_string(), "newer_tbl".to_string())),
            "a refused snapshot must not replace the schema either"
        );
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
            scram: None,
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
            scram: None,
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

    // -----------------------------------------------------------------------
    // Rejoin catch-up after a missed ALTER (nightly pair smoke, 2026-10-01..03)
    //
    // node1 created `kv (k, v)`, ALTERed it to add `extra`, wrote key1, and was
    // SIGKILLed. Its schema.json still held the pre-ALTER table. On rejoin the
    // promoted node2 pushed its post-ALTER schema, but catch-up only INSERTED
    // tables the secondary lacked, so node1 kept `kv (k, v)`. node2 then
    // replayed key1 laid out for `[extra, v]` (v at ordinal 1) into node1's
    // `[v]` layout, and after switchover node1 answered key1 with v = null.
    // -----------------------------------------------------------------------

    fn kv_table(id: Uuid, with_extra: bool) -> TableMetadata {
        use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
        let column = |name: &str, kind: ColumnKind| ColumnMetadata {
            name: name.to_string(),
            kind,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        };
        let mut columns = IndexMap::new();
        columns.insert("k".to_string(), column("k", ColumnKind::PartitionKey));
        columns.insert("v".to_string(), column("v", ColumnKind::Regular));
        if with_extra {
            columns.insert("extra".to_string(), column("extra", ColumnKind::Regular));
        }
        TableMetadata {
            keyspace: "test_ks".to_string(),
            name: "kv".to_string(),
            id,
            columns,
            partition_key: vec!["k".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        }
    }

    fn sync_msg(tables: Vec<TableMetadata>) -> (Uuid, Message) {
        let wire = wire_with(tables);
        let version = wire.version;
        let body = serde_json::to_vec(&wire).unwrap();
        (version, Message::PairSchemaSync(Bytes::from(body)))
    }

    fn sync_handler(
        schema: &Arc<ferrosa_schema::Schema>,
        engine: &Arc<StorageEngine>,
        role: PairRole,
    ) -> PairSchemaSyncHandler {
        PairSchemaSyncHandler::new(
            Arc::clone(schema),
            Arc::clone(engine),
            Arc::new(ArcSwap::from_pointee(role)),
        )
    }

    /// RED (a): a secondary that missed an ALTER must converge to the
    /// primary's table definition — registry AND storage engine — when the
    /// primary's schema snapshot arrives.
    #[tokio::test]
    async fn pair_schema_sync_applies_an_alter_the_secondary_missed() {
        use ferrosa_storage::TableId;
        let dir = tempfile::tempdir().unwrap();
        let engine = pair_test_engine(dir.path());
        let schema = test_replication_schema();
        let handler = sync_handler(&schema, &engine, PairRole::Secondary);
        let peer = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let id = Uuid::new_v4();

        // The secondary holds the pre-ALTER table (what node1 restarted with).
        let (_, stale) = sync_msg(vec![kv_table(id, false)]);
        assert!(handler.handle(peer, stale).await.is_some());

        // The primary's snapshot carries the ALTERed table.
        let (primary_version, current) = sync_msg(vec![kv_table(id, true)]);
        let ack = handler.handle(peer, current).await;

        let registry = schema.snapshot();
        let table = registry
            .tables
            .get(&("test_ks".to_string(), "kv".to_string()))
            .expect("kv must stay in the registry");
        assert!(
            table.columns.contains_key("extra"),
            "catch-up left the secondary's registry at the pre-ALTER definition \
             (columns {:?}); apply_snapshot only inserts tables the receiver lacks, so \
             an ALTER missed while offline is never applied",
            table.columns.keys().collect::<Vec<_>>()
        );
        let storage = engine
            .table_schema(&TableId::new("test_ks", "kv"))
            .expect("kv must stay registered with the engine");
        let regulars: Vec<&str> = storage
            .regular_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            regulars,
            vec!["extra", "v"],
            "the storage layout must match the primary's, or positional cells \
             replicated from the primary land on the wrong column"
        );
        match ack {
            Some(Message::PairDdlAck(payload)) => assert_eq!(
                payload.as_ref(),
                primary_version.as_bytes(),
                "the ack must report the schema version the secondary converged to"
            ),
            other => panic!("expected PairDdlAck carrying the converged version, got {other:?}"),
        }
        assert_eq!(schema.snapshot().version, primary_version);
    }

    /// RED (data-loss guard): a schema snapshot sent TO a primary comes from a
    /// node that wrongly believes it leads -- in the smoke run, the restarted
    /// unpromoted node1 elected itself by host-id order and pushed its stale
    /// schema to the promoted node2. Applying it would treat every table the
    /// stale node lacks as a missed DROP and delete it, SSTables included.
    #[tokio::test]
    async fn pair_schema_sync_is_refused_by_a_primary_and_drops_nothing() {
        use ferrosa_storage::TableId;
        let dir = tempfile::tempdir().unwrap();
        let engine = pair_test_engine(dir.path());
        let schema = test_replication_schema();
        let peer = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let id = Uuid::new_v4();

        // Install kv while this node is a secondary, as it would have been.
        let seed = sync_handler(&schema, &engine, PairRole::Secondary);
        let (_, install) = sync_msg(vec![kv_table(id, true)]);
        assert!(seed.handle(peer, install).await.is_some());
        let kv = TableId::new("test_ks", "kv");
        let kv_dir = dir.path().join("sstables").join(kv.to_string());
        assert!(engine.table_schema(&kv).is_some() && kv_dir.exists());

        // Now this node is the primary and a stale peer pushes a snapshot
        // without kv.
        let primary = sync_handler(&schema, &engine, PairRole::Primary);
        let (_, stale) = sync_msg(vec![]);
        let reply = primary.handle(peer, stale).await;

        assert!(
            !matches!(reply, Some(Message::PairDdlAck(_))),
            "a primary must refuse a peer's schema snapshot, got {reply:?}"
        );
        assert!(
            engine.table_schema(&kv).is_some(),
            "a stale peer's snapshot unregistered a table on the primary"
        );
        assert!(
            kv_dir.exists(),
            "a stale peer's snapshot deleted the primary's SSTable directory"
        );
        assert!(schema
            .snapshot()
            .tables
            .contains_key(&("test_ks".to_string(), "kv".to_string())));
    }

    /// RED (c): the value written before failover must read back after the
    /// rejoined secondary catches up. The primary replays key1 laid out for
    /// its own storage schema `[extra, v]`; once the secondary has converged,
    /// reading `v` by name must return `from_node1`, not null.
    #[tokio::test]
    async fn value_written_before_failover_reads_back_after_catch_up() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        use ferrosa_storage::TableId;

        let dir = tempfile::tempdir().unwrap();
        let engine = pair_test_engine(dir.path());
        let schema = test_replication_schema();
        let handler = sync_handler(&schema, &engine, PairRole::Secondary);
        let peer = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let id = Uuid::new_v4();

        let (_, stale) = sync_msg(vec![kv_table(id, false)]);
        assert!(handler.handle(peer, stale).await.is_some());
        let (_, current) = sync_msg(vec![kv_table(id, true)]);
        handler.handle(peer, current).await;

        // The primary's layout: regular columns sorted by name, `[extra, v]`.
        let primary_layout = kv_table(id, true).to_storage_schema();
        let v_on_primary = primary_layout
            .regular_columns
            .iter()
            .position(|c| c.name == "v")
            .unwrap() as u16;
        assert_eq!(v_on_primary, 1);

        let kv = TableId::new("test_ks", "kv");
        let key = DecoratedKey {
            token: Token(7),
            key: PartitionKey::new(b"key1".to_vec()),
        };
        let row = Row {
            clustering: vec![],
            cells: vec![(v_on_primary, CellValue::live(b"from_node1".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        engine
            .write(&kv, &key, row, 1000)
            .expect("catch-up replay of key1 must apply");

        let local = engine.table_schema(&kv).expect("kv registered");
        let v_local = local
            .regular_columns
            .iter()
            .position(|c| c.name == "v")
            .expect("v must exist locally") as u16;
        let partition = engine
            .read(&kv, &key)
            .unwrap()
            .expect("key1 must be present");
        let v = partition
            .rows
            .iter()
            .flat_map(|r| r.cells.iter())
            .find(|(idx, _)| *idx == v_local)
            .and_then(|(_, cell)| cell.value.clone());
        assert_eq!(
            v.as_deref(),
            Some(&b"from_node1"[..]),
            "key1's v read back as {v:?}: the replayed cell was laid out for the \
             primary's [extra, v] but the secondary still had [v], so it landed on \
             the wrong ordinal -- the silent v = null of the nightly smoke"
        );
    }
}
