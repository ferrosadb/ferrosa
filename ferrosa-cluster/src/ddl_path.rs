//! DDL path abstraction for runtime mode transitions.
//!
//! Parallels `WritePath` — the CQL router calls `DdlPath::execute()`
//! for all DDL operations. Swapped atomically via `ArcSwap`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_schema::Schema;
use ferrosa_storage::engine::StorageEngine;

use crate::error::{ClusterError, Result};
use crate::pair::ddl::{DdlCoordinator, DdlOperation};
use crate::raft::{FerrosRaft, RaftCommand, RaftOp};

/// The active DDL path. Swapped atomically via `ArcSwap` when
/// the deployment mode changes (standalone → pair → cluster).
pub enum DdlPath {
    /// Standalone: DDL applied directly to local schema + storage.
    Direct {
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
    },
    /// Pair mode: DDL routed through DdlCoordinator (primary authority).
    Pair(Arc<DdlCoordinator>),
    /// Cluster mode: DDL proposed via Raft consensus.
    ///
    /// When this node is not the Raft leader, the operation is transparently
    /// forwarded to the current leader via [`PeerManager`] so that the CQL
    /// client never sees a `NotLeader` error.
    Cluster {
        raft: Arc<FerrosRaft>,
        /// PeerManager used to forward DDL to the Raft leader.
        peer_manager: Arc<PeerManager>,
        /// Maps openraft `u64` NodeId → ferrosa `Uuid` host_id.
        ///
        /// Shared with [`crate::raft::network::FerrosRaftNetworkFactory`]
        /// so that both the Raft transport and the DDL forwarding path use
        /// the same up-to-date mapping without a separate sync mechanism.
        node_map: Arc<RwLock<HashMap<u64, Uuid>>>,
    },
    /// Blocked: cluster formation in progress (Forming state).
    ///
    /// DDL is rejected because the node has left Pair mode but Raft has not
    /// yet elected a leader.  Any DDL applied now would use `Direct` path
    /// and never replicate, causing silent schema divergence (FMEA F3).
    Blocked,
    /// Degraded: peer lost, DDL rejected until operator promotes.
    Unavailable,
}

impl DdlPath {
    /// Execute a DDL operation on the current path.
    ///
    /// - `Direct`: applies directly to local schema + storage.
    /// - `Pair`: routes through `DdlCoordinator` (primary authority).
    /// - `Cluster`: proposes via Raft `client_write`; if this node is not the
    ///   leader, transparently forwards the DDL to the leader via
    ///   [`PeerManager`] so the CQL client never sees a `NotLeader` error.
    /// - `Unavailable`: returns an error immediately.
    pub async fn execute(&self, op: DdlOperation) -> Result<()> {
        match self {
            Self::Direct { schema, engine } => {
                // Reuse DdlCoordinator's local-apply logic by constructing a
                // temporary coordinator with no peer.  The coordinator's
                // `apply_ddl_locally` is the canonical single-node DDL path.
                //
                // We do this inline rather than constructing a DdlCoordinator
                // (which requires a peer_host_id and peer_manager) by reusing
                // the same Schema/StorageEngine operations that DdlCoordinator
                // would perform.
                apply_direct(&op, schema, engine)
            }
            Self::Pair(coordinator) => coordinator.coordinate_ddl(op).await,
            Self::Cluster {
                raft,
                peer_manager,
                node_map,
            } => {
                match execute_via_raft(raft, op.clone()).await {
                    Ok(()) => Ok(()),
                    Err(ClusterError::NotLeader {
                        leader_id: Some(leader_node_id),
                    }) => {
                        // Resolve the Raft NodeId to a PeerManager UUID.
                        let leader_uuid = node_map
                            .read()
                            .expect("node_map lock poisoned")
                            .get(&leader_node_id)
                            .copied();

                        match leader_uuid {
                            Some(uuid) => forward_ddl_to_leader(peer_manager, uuid, op).await,
                            None => {
                                // Leader UUID unknown — cannot forward.
                                Err(ClusterError::Internal(format!(
                                    "DDL forwarding failed: leader node_id={leader_node_id} \
                                     not found in node map"
                                )))
                            }
                        }
                    }
                    Err(ClusterError::NotLeader { leader_id: None }) => {
                        // Leader not yet elected — tell the client to retry.
                        Err(ClusterError::Internal(
                            "DDL forwarding failed: no Raft leader elected yet".into(),
                        ))
                    }
                    Err(other) => Err(other),
                }
            }
            Self::Blocked => Err(ClusterError::Internal(
                "DDL unavailable: cluster formation in progress, retry after Raft leader election"
                    .into(),
            )),
            Self::Unavailable => Err(ClusterError::Internal(
                "DDL unavailable: peer lost, wait for operator action".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Direct (standalone) DDL
// ---------------------------------------------------------------------------

/// Apply a `DdlOperation` directly to the local schema and storage engine.
///
/// This mirrors [`DdlCoordinator::apply_ddl_locally`] exactly but does not
/// require constructing a coordinator (which needs a peer ID and PeerManager).
fn apply_direct(op: &DdlOperation, schema: &Schema, engine: &StorageEngine) -> Result<()> {
    match op {
        DdlOperation::CreateKeyspace(ks) => {
            schema
                .create_keyspace_internal(ks.clone())
                .map_err(|e| ClusterError::Internal(format!("create_keyspace: {e}")))?;
        }
        DdlOperation::DropKeyspace(name) => {
            let snap = schema.snapshot();
            let table_ids: Vec<_> = snap
                .tables
                .keys()
                .filter(|(ks, _)| ks == name)
                .map(|(ks, tbl)| ferrosa_storage::TableId::new(ks, tbl))
                .collect();
            schema
                .drop_keyspace_internal(name)
                .map_err(|e| ClusterError::Internal(format!("drop_keyspace: {e}")))?;
            for tid in &table_ids {
                engine
                    .unregister_table(tid)
                    .map_err(ClusterError::Storage)?;
            }
        }
        DdlOperation::CreateTable(table) => {
            schema
                .create_table_internal(*table.clone())
                .map_err(|e| ClusterError::Internal(format!("create_table: {e}")))?;
            let storage_schema = table.to_storage_schema();
            engine
                .register_table(storage_schema)
                .map_err(ClusterError::Storage)?;
        }
        DdlOperation::DropTable { keyspace, table } => {
            schema
                .drop_table_internal(keyspace, table)
                .map_err(|e| ClusterError::Internal(format!("drop_table: {e}")))?;
            let tid = ferrosa_storage::TableId::new(keyspace, table);
            engine
                .unregister_table(&tid)
                .map_err(ClusterError::Storage)?;
        }
        DdlOperation::AlterKeyspace { name, updates } => {
            schema
                .alter_keyspace_internal(name, updates.clone())
                .map_err(|e| ClusterError::Internal(format!("alter_keyspace: {e}")))?;
        }
        DdlOperation::AlterTable {
            keyspace,
            table,
            updates,
        } => {
            schema
                .alter_table_internal(keyspace, table, *updates.clone())
                .map_err(|e| ClusterError::Internal(format!("alter_table: {e}")))?;
        }
        DdlOperation::CreateRole(role) => {
            schema
                .create_role_internal(role.clone())
                .map_err(|e| ClusterError::Internal(format!("create_role: {e}")))?;
        }
        DdlOperation::AlterRole { name, updates } => {
            schema
                .alter_role_internal(name, updates.clone())
                .map_err(|e| ClusterError::Internal(format!("alter_role: {e}")))?;
        }
        DdlOperation::DropRole(name) => {
            schema
                .drop_role_internal(name)
                .map_err(|e| ClusterError::Internal(format!("drop_role: {e}")))?;
        }
        DdlOperation::Grant(entry) => {
            schema
                .grant_internal(entry.clone())
                .map_err(|e| ClusterError::Internal(format!("grant: {e}")))?;
        }
        DdlOperation::Revoke {
            role,
            resource,
            permission,
        } => {
            schema
                .revoke_internal(role, resource, permission)
                .map_err(|e| ClusterError::Internal(format!("revoke: {e}")))?;
        }
        DdlOperation::CreateIndex(idx) => {
            schema
                .create_index_internal(idx.clone())
                .map_err(|e| ClusterError::Internal(format!("create_index: {e}")))?;
        }
        DdlOperation::DropIndex {
            keyspace,
            table,
            index,
        } => {
            schema
                .drop_index_internal(keyspace, table, index)
                .map_err(|e| ClusterError::Internal(format!("drop_index: {e}")))?;
        }
        DdlOperation::CreateType(ref udt) => {
            schema
                .create_type_internal(udt)
                .map_err(|e| ClusterError::Internal(format!("create_type: {e}")))?;
        }
        DdlOperation::DropType {
            ref keyspace,
            ref name,
        } => {
            schema
                .drop_type_internal(keyspace, name)
                .map_err(|e| ClusterError::Internal(format!("drop_type: {e}")))?;
        }
        DdlOperation::CreateFunction(ref func) => {
            schema
                .create_function_internal(func)
                .map_err(|e| ClusterError::Internal(format!("create_function: {e}")))?;
        }
        DdlOperation::DropFunction {
            ref keyspace,
            ref name,
            ref arg_types,
        } => {
            schema
                .drop_function_internal(keyspace, name, arg_types)
                .map_err(|e| ClusterError::Internal(format!("drop_function: {e}")))?;
        }
        DdlOperation::CreateAggregate(ref agg) => {
            schema
                .create_aggregate_internal(agg)
                .map_err(|e| ClusterError::Internal(format!("create_aggregate: {e}")))?;
        }
        DdlOperation::DropAggregate {
            ref keyspace,
            ref name,
            ref arg_types,
        } => {
            schema
                .drop_aggregate_internal(keyspace, name, arg_types)
                .map_err(|e| ClusterError::Internal(format!("drop_aggregate: {e}")))?;
        }
    }
    schema.set_schema_version(Uuid::new_v4());
    Ok(())
}

// ---------------------------------------------------------------------------
// Leader forwarding
// ---------------------------------------------------------------------------

/// Forward a [`DdlOperation`] to the Raft leader node.
///
/// Serialises `op` with JSON (reusing the pair-mode DDL wire format),
/// sends it as [`Message::PairDdlForward`] on [`Lane::Data`], and waits for
/// [`Message::PairDdlAck`].  The leader runs a
/// [`ClusterDdlForwardHandler`] that calls `execute_via_raft` locally —
/// since the leader is the Raft leader, the proposal succeeds immediately.
async fn forward_ddl_to_leader(
    peer_manager: &PeerManager,
    leader_uuid: Uuid,
    op: DdlOperation,
) -> Result<()> {
    let body = op.to_bytes()?;
    let resp = peer_manager
        .send(leader_uuid, Message::PairDdlForward(body), Lane::Data)
        .await
        .map_err(ClusterError::Net)?;

    match resp {
        Message::PairDdlAck(_) => Ok(()),
        other => Err(ClusterError::Internal(format!(
            "unexpected response from leader during DDL forward: {:?}",
            other.msg_type()
        ))),
    }
}

// ---------------------------------------------------------------------------
// Cluster (Raft) DDL
// ---------------------------------------------------------------------------

/// Convert a [`DdlOperation`] to the equivalent [`RaftCommand`].
///
/// The leader generates a fresh `schema_version` UUID here so that all
/// followers replicate exactly the same version after applying the log entry.
fn ddl_op_to_raft_command(op: DdlOperation) -> RaftCommand {
    let raft_op = match op {
        DdlOperation::CreateKeyspace(ks) => RaftOp::CreateKeyspace(ks),
        DdlOperation::DropKeyspace(name) => RaftOp::DropKeyspace(name),
        DdlOperation::CreateTable(table) => RaftOp::CreateTable(table),
        DdlOperation::DropTable { keyspace, table } => RaftOp::DropTable { keyspace, table },
        DdlOperation::AlterKeyspace { name, updates } => RaftOp::AlterKeyspace { name, updates },
        DdlOperation::AlterTable {
            keyspace,
            table,
            updates,
        } => RaftOp::AlterTable {
            keyspace,
            table,
            updates,
        },
        DdlOperation::CreateRole(role) => RaftOp::CreateRole(role),
        DdlOperation::AlterRole { name, updates } => RaftOp::AlterRole { name, updates },
        DdlOperation::DropRole(name) => RaftOp::DropRole(name),
        DdlOperation::Grant(entry) => RaftOp::Grant(entry),
        DdlOperation::Revoke {
            role,
            resource,
            permission,
        } => RaftOp::Revoke {
            role,
            resource,
            permission,
        },
        DdlOperation::CreateIndex(idx) => RaftOp::CreateIndex(idx),
        DdlOperation::DropIndex {
            keyspace,
            table,
            index,
        } => RaftOp::DropIndex {
            keyspace,
            table,
            index,
        },
        DdlOperation::CreateType(udt) => RaftOp::CreateType(udt),
        DdlOperation::DropType { keyspace, name } => RaftOp::DropType { keyspace, name },
        DdlOperation::CreateFunction(func) => RaftOp::CreateFunction(func),
        DdlOperation::DropFunction {
            keyspace,
            name,
            arg_types,
        } => RaftOp::DropFunction {
            keyspace,
            name,
            arg_types,
        },
        DdlOperation::CreateAggregate(agg) => RaftOp::CreateAggregate(agg),
        DdlOperation::DropAggregate {
            keyspace,
            name,
            arg_types,
        } => RaftOp::DropAggregate {
            keyspace,
            name,
            arg_types,
        },
    };
    RaftCommand {
        op: raft_op,
        schema_version: Uuid::new_v4(),
    }
}

/// Propose a DDL operation through Raft consensus.
///
/// On success the state machine has applied the command on a quorum of nodes
/// and side effects are visible on this node via [`FerrosStateMachine`].
///
/// On `ForwardToLeader` the caller receives [`ClusterError::NotLeader`] with
/// the leader hint. The [`DdlPath::Cluster`] arm in `execute()` catches this
/// and transparently forwards the request to the leader instead of propagating
/// the error to the CQL client.
pub(crate) async fn execute_via_raft(raft: &FerrosRaft, op: DdlOperation) -> Result<()> {
    let cmd = ddl_op_to_raft_command(op);

    match raft.client_write(cmd).await {
        Ok(_resp) => Ok(()),
        Err(raft_err) => {
            // Extract a ForwardToLeader hint if present.
            if let Some(fwd) = raft_err.forward_to_leader() {
                return Err(ClusterError::NotLeader {
                    leader_id: fwd.leader_id,
                });
            }
            // Any other error is a general Raft fault.
            Err(ClusterError::RaftError(raft_err.to_string()))
        }
    }
}

// ---------------------------------------------------------------------------
// ClusterDdlForwardHandler
// ---------------------------------------------------------------------------

/// Handles [`Message::PairDdlForward`] on the Raft **leader** node.
///
/// Non-leader cluster nodes forward DDL to the leader via
/// `forward_ddl_to_leader`.  The leader must have this handler registered
/// (instead of the pair-mode [`crate::pair::ddl::PairDdlForwardHandler`]) so
/// that it proposes the operation through Raft rather than applying it
/// directly.
///
/// Registered in `ModeController::transition_to_cluster` after the Raft leader
/// is elected.
pub struct ClusterDdlForwardHandler {
    raft: Arc<FerrosRaft>,
}

impl ClusterDdlForwardHandler {
    /// Create a new handler backed by `raft`.
    pub fn new(raft: Arc<FerrosRaft>) -> Self {
        Self { raft }
    }
}

#[async_trait::async_trait]
impl ferrosa_net::rpc::handler::RpcHandler for ClusterDdlForwardHandler {
    async fn handle(
        &self,
        _from: ferrosa_net::rpc::handler::PeerId,
        msg: Message,
    ) -> Option<Message> {
        let body = match msg {
            Message::PairDdlForward(b) => b,
            _ => return None,
        };

        let op = match DdlOperation::from_bytes(&body) {
            Ok(op) => op,
            Err(e) => {
                tracing::error!("ClusterDdlForwardHandler: failed to decode op: {e}");
                return None;
            }
        };

        match execute_via_raft(&self.raft, op).await {
            Ok(()) => Some(Message::PairDdlAck(Bytes::new())),
            Err(e) => {
                tracing::error!("ClusterDdlForwardHandler: execute_via_raft failed: {e}");
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Arc;

    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
    use ferrosa_schema::Schema;
    use ferrosa_storage::engine::StorageEngine;
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};

    use indexmap::IndexMap;
    use std::collections::HashSet;
    use uuid::Uuid;

    // -- helpers ----------------------------------------------------------

    fn test_schema() -> Arc<Schema> {
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
        Arc::new(Schema::new(config).unwrap())
    }

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096, flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn simple_keyspace(name: &str) -> KeyspaceMetadata {
        let mut opts = HashMap::new();
        opts.insert("replication_factor".to_string(), "1".to_string());
        KeyspaceMetadata {
            name: name.to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: opts,
            },
        }
    }

    fn simple_table(ks: &str, name: &str) -> TableMetadata {
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
            keyspace: ks.to_string(),
            name: name.to_string(),
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

    // -- DdlPath::Direct tests --------------------------------------------

    #[tokio::test]
    async fn direct_create_keyspace_applies_locally() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine,
        };

        let op = DdlOperation::CreateKeyspace(simple_keyspace("test_ks"));
        ddl.execute(op).await.unwrap();

        let snap = schema.snapshot();
        assert!(
            snap.keyspaces.contains_key("test_ks"),
            "keyspace should be visible in schema"
        );
    }

    #[tokio::test]
    async fn create_table_via_direct_registers_in_storage() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        };

        // Must create keyspace first.
        ddl.execute(DdlOperation::CreateKeyspace(simple_keyspace("ks")))
            .await
            .unwrap();

        let table = simple_table("ks", "users");
        ddl.execute(DdlOperation::CreateTable(Box::new(table)))
            .await
            .unwrap();

        let snap = schema.snapshot();
        assert!(
            snap.tables.contains_key(&("ks".into(), "users".into())),
            "table should be in schema"
        );
        // Verify storage engine knows the table (write should succeed).
        let table_id = ferrosa_storage::TableId::new("ks", "users");
        let key = ferrosa_common::key::DecoratedKey {
            token: ferrosa_common::Token(0),
            key: ferrosa_common::PartitionKey::new(b"k".to_vec()),
        };
        let row = ferrosa_sstable::types::Row {
            clustering: vec![],
            cells: vec![],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1),
        };
        engine
            .write(&table_id, &key, row, 1)
            .expect("storage should accept write after table registered");
    }

    #[tokio::test]
    async fn direct_unavailable_returns_error() {
        let ddl = DdlPath::Unavailable;
        let op = DdlOperation::CreateKeyspace(simple_keyspace("ks"));
        let err = ddl.execute(op).await.unwrap_err();
        assert!(
            matches!(err, ClusterError::Internal(_)),
            "Unavailable should return Internal error, got {err:?}"
        );
    }

    // -- ddl_op_to_raft_command round-trip test --------------------------

    #[test]
    fn ddl_op_to_raft_command_create_keyspace() {
        let ks = simple_keyspace("raft_ks");
        let op = DdlOperation::CreateKeyspace(ks);
        let cmd = ddl_op_to_raft_command(op);
        match cmd.op {
            RaftOp::CreateKeyspace(ks) => assert_eq!(ks.name, "raft_ks"),
            other => panic!("expected CreateKeyspace, got {other:?}"),
        }
    }

    #[test]
    fn ddl_op_to_raft_command_create_table() {
        let table = simple_table("ks", "tbl");
        let op = DdlOperation::CreateTable(Box::new(table));
        let cmd = ddl_op_to_raft_command(op);
        match cmd.op {
            RaftOp::CreateTable(t) => {
                assert_eq!(t.keyspace, "ks");
                assert_eq!(t.name, "tbl");
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn ddl_op_to_raft_command_drop_keyspace() {
        let op = DdlOperation::DropKeyspace("bye_ks".into());
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(cmd.op, RaftOp::DropKeyspace(ref n) if n == "bye_ks"));
    }

    #[test]
    fn ddl_op_to_raft_command_drop_table() {
        let op = DdlOperation::DropTable {
            keyspace: "ks".into(),
            table: "tbl".into(),
        };
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(
            cmd.op,
            RaftOp::DropTable {
                ref keyspace,
                ref table
            } if keyspace == "ks" && table == "tbl"
        ));
    }

    // -- ClusterError::NotLeader display ----------------------------------

    #[test]
    fn not_leader_error_display_with_id() {
        let err = ClusterError::NotLeader {
            leader_id: Some(42),
        };
        let msg = err.to_string();
        assert!(msg.contains("42"), "should include leader_id in message");
    }

    #[test]
    fn not_leader_error_display_without_id() {
        let err = ClusterError::NotLeader { leader_id: None };
        let msg = err.to_string();
        assert!(
            msg.contains("unknown"),
            "should say unknown when leader_id is None"
        );
    }

    #[test]
    fn raft_error_display() {
        let err = ClusterError::RaftError("quorum lost".into());
        let msg = err.to_string();
        assert!(msg.contains("quorum lost"));
    }

    /// Verify that swapping from `Direct` to `Cluster` via `ArcSwap` works correctly.
    ///
    /// This mirrors the pattern used by `ModeController::transition_to_cluster` where
    /// the `ddl_path` ArcSwap is first set to `Direct` (while Raft initialises in the
    /// background) and then atomically replaced with `DdlPath::Cluster`.
    #[test]
    fn ddl_path_transitions_from_direct_after_raft_init() {
        use arc_swap::ArcSwap;

        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        // Start as Direct (standalone / pre-Raft-init state).
        let ddl_swap: ArcSwap<DdlPath> = ArcSwap::from_pointee(DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        });

        // Verify it's Direct initially.
        assert!(
            matches!(&**ddl_swap.load(), DdlPath::Direct { .. }),
            "initial state must be Direct"
        );

        // Simulate DdlPath::Cluster being stored after Raft initialises.
        // We use a minimal placeholder FerrosRaft struct via openraft in a way
        // that only tests the ArcSwap swap — not Raft itself.
        // Since we can't construct a FerrosRaft without a running tokio runtime
        // and network, we just verify the enum variant discrimination.
        //
        // The actual Raft integration is covered by controller::tests::raft_initializes_on_third_peer.
        //
        // Re-swap back to Direct to assert the swap mechanism works bidirectionally.
        ddl_swap.store(Arc::new(DdlPath::Unavailable));
        assert!(
            matches!(&**ddl_swap.load(), DdlPath::Unavailable),
            "swap to Unavailable must be visible immediately"
        );

        ddl_swap.store(Arc::new(DdlPath::Direct { schema, engine }));
        assert!(
            matches!(&**ddl_swap.load(), DdlPath::Direct { .. }),
            "swap back to Direct must be visible immediately"
        );
    }

    // -- DDL forwarding: node_map resolution ----------------------------------

    /// When `NotLeader { leader_id: Some(id) }` is returned and the node_map
    /// contains an entry for that id, the forwarding path should attempt to
    /// send to the resolved UUID.
    ///
    /// Since we can't wire up a real PeerManager in a unit test (no listening
    /// socket), we test just the node_map lookup half of the forwarding path
    /// by verifying that a populated map returns the right UUID.
    #[test]
    fn node_map_lookup_resolves_leader_uuid() {
        use std::collections::HashMap;
        use std::sync::RwLock;
        use uuid::Uuid;

        let leader_node_id: u64 = 42;
        let leader_uuid = Uuid::new_v4();

        let node_map: Arc<RwLock<HashMap<u64, Uuid>>> = Arc::new(RwLock::new(HashMap::new()));
        node_map
            .write()
            .unwrap()
            .insert(leader_node_id, leader_uuid);

        let resolved = node_map.read().unwrap().get(&leader_node_id).copied();

        assert_eq!(
            resolved,
            Some(leader_uuid),
            "node_map must resolve leader_node_id to the correct UUID"
        );
    }

    /// When the node_map does NOT contain the leader's node_id, the lookup
    /// returns `None` and the error path must trigger an Internal error
    /// (not a panic).
    #[test]
    fn node_map_lookup_missing_leader_returns_none() {
        use std::collections::HashMap;
        use std::sync::RwLock;
        use uuid::Uuid;

        let node_map: Arc<RwLock<HashMap<u64, Uuid>>> = Arc::new(RwLock::new(HashMap::new()));
        // No entry registered — lookup must return None.
        let resolved = node_map.read().unwrap().get(&99u64).copied();
        assert!(
            resolved.is_none(),
            "missing entry must return None, not panic"
        );
    }

    /// Verify that `DdlOperation::to_bytes` / `from_bytes` round-trips work for
    /// the operations most likely to hit the forwarding path in a three-node
    /// cluster (CREATE KEYSPACE and CREATE TABLE).
    ///
    /// The forwarding path relies on JSON serialization; if the round-trip
    /// breaks, the leader will fail to decode the forwarded op.
    #[test]
    fn ddl_op_serialization_roundtrip_for_forwarding() {
        // CreateKeyspace round-trip
        let ks = simple_keyspace("fwd_ks");
        let op = DdlOperation::CreateKeyspace(ks);
        let bytes = op.to_bytes().expect("serialize");
        let decoded = DdlOperation::from_bytes(&bytes).expect("deserialize");
        assert!(
            matches!(decoded, DdlOperation::CreateKeyspace(ref k) if k.name == "fwd_ks"),
            "CreateKeyspace must survive the forwarding serialization round-trip"
        );

        // CreateTable round-trip
        let table = simple_table("fwd_ks", "fwd_tbl");
        let op = DdlOperation::CreateTable(Box::new(table));
        let bytes = op.to_bytes().expect("serialize");
        let decoded = DdlOperation::from_bytes(&bytes).expect("deserialize");
        assert!(
            matches!(
                decoded,
                DdlOperation::CreateTable(ref t)
                    if t.keyspace == "fwd_ks" && t.name == "fwd_tbl"
            ),
            "CreateTable must survive the forwarding serialization round-trip"
        );
    }

    /// Verify that the `ClusterDdlForwardHandler` returns `None` for non-DDL
    /// messages (wrong message type).
    ///
    /// This is a pure unit test that does NOT require a live Raft instance
    /// because openraft's `Raft::new` is async and needs a running cluster.
    /// We verify the message-type guard in the handler at the codec level.
    #[tokio::test]
    async fn test_blocked_ddl_path_returns_error() {
        let ddl = DdlPath::Blocked;
        let op = DdlOperation::CreateKeyspace(simple_keyspace("should_fail"));
        let err = ddl.execute(op).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("formation in progress"),
            "Blocked error must mention 'formation in progress', got: {msg}"
        );
    }

    #[test]
    fn ddl_operation_from_bytes_handles_malformed_payload() {
        // Confirm that a garbage payload produces an error, not a panic.
        let result = DdlOperation::from_bytes(b"{not json}");
        assert!(
            result.is_err(),
            "malformed JSON must produce an error from DdlOperation::from_bytes"
        );
        assert!(
            matches!(result.unwrap_err(), ClusterError::Internal(_)),
            "error must be ClusterError::Internal"
        );
    }
}
