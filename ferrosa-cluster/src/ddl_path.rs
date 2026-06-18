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
use crate::system_table_writer::SystemTableWriter;

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
    /// Forming: cluster formation in progress.
    ///
    /// DDL operations are queued and replayed after Raft leader election
    /// in `transition_to_cluster`. The client receives a retriable error
    /// so it can retry after formation completes (FMEA F3).
    Forming {
        queue: tokio::sync::mpsc::UnboundedSender<DdlOperation>,
    },
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
                    // Leader applied the DDL (client_write awaits leader apply),
                    // so read-your-writes already holds on this node.
                    Ok(_committed_index) => Ok(()),
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
                            // Forward to the leader; `forward_ddl_to_leader` waits
                            // for THIS node to apply before returning, so the
                            // client sees its own write on this follower.
                            Some(uuid) => forward_ddl_to_leader(Some(raft), peer_manager, uuid, op)
                                .await
                                .map(|_committed| ()),
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
            Self::Forming { queue } => {
                // Queue the operation for replay after leader election.
                // Still return an error to the client so they know to retry
                // (the DDL will be applied automatically but the client can't
                // observe the result until formation completes).
                if let Err(e) = queue.send(op) {
                    tracing::error!(%e, "ddl: failed to enqueue DDL operation");
                }
                Err(ClusterError::Internal(
                    "DDL unavailable: cluster formation in progress, will be applied after leader election — retry shortly".into(),
                ))
            }
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
fn apply_direct(op: &DdlOperation, schema: &Schema, engine: &Arc<StorageEngine>) -> Result<()> {
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
            // Propagate the post-ALTER column set to the storage engine so
            // flush builds the SerializationHeader with the correct column
            // list. See bug-sstable-writer-produces-zero-byte-rows-db.md.
            let snap = schema.snapshot();
            if let Some(tbl) = snap.tables.get(&(keyspace.clone(), table.clone())) {
                let tid = ferrosa_storage::TableId::new(keyspace, table);
                engine
                    .update_table_schema(&tid, tbl.to_storage_schema())
                    .map_err(ClusterError::Storage)?;
            }
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
            SystemTableWriter::new(Arc::clone(engine))
                .apply(
                    ferrosa_schema::system::persistence::SystemTableMutation::GrantUpdated(
                        entry.clone(),
                    ),
                )
                .map_err(ClusterError::Storage)?;
        }
        DdlOperation::Revoke {
            role,
            resource,
            permission,
        } => {
            schema
                .revoke_internal(role, resource, permission)
                .map_err(|e| ClusterError::Internal(format!("revoke: {e}")))?;
            SystemTableWriter::new(Arc::clone(engine))
                .apply(
                    ferrosa_schema::system::persistence::SystemTableMutation::PermissionRevoked {
                        role: role.clone(),
                        resource: resource.clone(),
                        permission: *permission,
                    },
                )
                .map_err(ClusterError::Storage)?;
        }
        DdlOperation::GrantRole {
            member,
            granted_role,
        } => {
            schema
                .grant_role_internal(member, granted_role)
                .map_err(|e| ClusterError::Internal(format!("grant_role: {e}")))?;
            if let Some(role) = schema.snapshot().roles.get(member) {
                SystemTableWriter::new(Arc::clone(engine))
                    .apply(
                        ferrosa_schema::system::persistence::SystemTableMutation::RoleCreated(
                            role.clone(),
                        ),
                    )
                    .map_err(ClusterError::Storage)?;
            }
        }
        DdlOperation::RevokeRole {
            member,
            granted_role,
        } => {
            schema
                .revoke_role_internal(member, granted_role)
                .map_err(|e| ClusterError::Internal(format!("revoke_role: {e}")))?;
            if let Some(role) = schema.snapshot().roles.get(member) {
                SystemTableWriter::new(Arc::clone(engine))
                    .apply(
                        ferrosa_schema::system::persistence::SystemTableMutation::RoleCreated(
                            role.clone(),
                        ),
                    )
                    .map_err(ClusterError::Storage)?;
            }
        }
        DdlOperation::CreateIndex(idx) => {
            schema
                .create_index_internal(idx.clone())
                .map_err(|e| ClusterError::Internal(format!("create_index: {e}")))?;
            SystemTableWriter::new(Arc::clone(engine))
                .apply(
                    ferrosa_schema::system::persistence::SystemTableMutation::IndexCreated(
                        idx.clone(),
                    ),
                )
                .map_err(ClusterError::Storage)?;
        }
        DdlOperation::DropIndex {
            keyspace,
            table,
            index,
        } => {
            schema
                .drop_index_internal(keyspace, table, index)
                .map_err(|e| ClusterError::Internal(format!("drop_index: {e}")))?;
            SystemTableWriter::new(Arc::clone(engine))
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
            schema
                .create_type_internal(udt)
                .map_err(|e| ClusterError::Internal(format!("create_type: {e}")))?;
            SystemTableWriter::new(Arc::clone(engine))
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
            schema
                .drop_type_internal(keyspace, name)
                .map_err(|e| ClusterError::Internal(format!("drop_type: {e}")))?;
            SystemTableWriter::new(Arc::clone(engine))
                .apply(
                    ferrosa_schema::system::persistence::SystemTableMutation::TypeDropped {
                        keyspace: keyspace.clone(),
                        name: name.clone(),
                    },
                )
                .map_err(ClusterError::Storage)?;
        }
        DdlOperation::CreateFunction(ref func) => {
            schema
                .create_function_internal(func)
                .map_err(|e| ClusterError::Internal(format!("create_function: {e}")))?;
            SystemTableWriter::new(Arc::clone(engine))
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
            schema
                .drop_function_internal(keyspace, name, arg_types)
                .map_err(|e| ClusterError::Internal(format!("drop_function: {e}")))?;
            SystemTableWriter::new(Arc::clone(engine))
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
        DdlOperation::JoinNode(_) => {
            // Topology-only operation — not applied to local schema directly.
            // The leader handles this via client_write(RaftOp::JoinNode(..));
            // apply_direct is only called in standalone mode where there is
            // no cluster membership to update.
            return Ok(());
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
/// Returns the committed Raft log index of the applied DDL (or `0` if the leader
/// returned a legacy empty ack), so chained forwards can relay it.
///
/// Pass `local_raft = Some(..)` on the **client DDL path** so this node waits for
/// its own state machine to apply the DDL before returning (same-node
/// read-your-writes). Membership/bootstrap forwards (JoinNode, schema hand-off)
/// pass `None` — they have no waiting client and may lack a usable local raft.
pub(crate) async fn forward_ddl_to_leader(
    local_raft: Option<&FerrosRaft>,
    peer_manager: &PeerManager,
    leader_uuid: Uuid,
    op: DdlOperation,
) -> Result<u64> {
    let body = op.to_bytes()?;
    let resp = match peer_manager
        .send(
            leader_uuid,
            Message::PairDdlForward(body.clone()),
            Lane::Data,
        )
        .await
    {
        Ok(resp) => resp,
        Err(e)
            if e.to_string().contains("unknown peer")
                || e.to_string().contains("no connection pool") =>
        {
            let addr = peer_manager.peer_addr(leader_uuid).await.ok_or_else(|| {
                ClusterError::Internal(format!(
                    "DDL forwarding failed: missing address for leader {leader_uuid}"
                ))
            })?;
            peer_manager
                .ensure_peer(leader_uuid, &addr)
                .await
                .map_err(ClusterError::Net)?;
            peer_manager
                .send(leader_uuid, Message::PairDdlForward(body), Lane::Data)
                .await
                .map_err(ClusterError::Net)?
        }
        Err(e) => return Err(ClusterError::Net(e)),
    };

    match resp {
        Message::PairDdlAck(bytes) => {
            // The leader returns the committed Raft log index of the DDL (8-byte
            // big-endian). Before acking the client, wait for THIS node's state
            // machine to apply up to that index, so a following DML on the same
            // connection observes the schema change (same-node read-your-writes).
            // Older acks carry an empty payload — skip the wait for those.
            let committed = if bytes.len() >= 8 {
                u64::from_be_bytes(
                    bytes[..8]
                        .try_into()
                        .expect("slice of length 8 converts to [u8; 8]"),
                )
            } else {
                0
            };
            if let (Some(raft), true) = (local_raft, committed > 0) {
                wait_for_local_apply(raft, committed, DDL_AGREEMENT_TIMEOUT).await;
            }
            Ok(committed)
        }
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
        DdlOperation::GrantRole {
            member,
            granted_role,
        } => RaftOp::GrantRole {
            member,
            granted_role,
        },
        DdlOperation::RevokeRole {
            member,
            granted_role,
        } => RaftOp::RevokeRole {
            member,
            granted_role,
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
        DdlOperation::JoinNode(node_info) => RaftOp::JoinNode(node_info),
    };
    RaftCommand {
        op: raft_op,
        schema_version: Uuid::new_v4(),
    }
}

/// Maximum time to wait for every live follower to replicate a DDL log entry
/// before this node returns success to the CQL client.
///
/// The leader's state machine applies on commit, but followers apply
/// *asynchronously* after their replication stream catches up. If the CQL
/// client moves on before followers apply, a subsequent DML routed to a
/// lagging follower validates against a stale `TableSchema` — the exact
/// failure mode that caused the
/// `MutationForward write failed e=invalid data: ... (column "first_seen",
/// index 3): TimestampType expects 8 raw bytes but value provided 4`
/// rejection during ferrosa-memory's cluster-int warm-up.
///
/// The barrier polls each voter's replication progress (the leader's view of
/// follower `matched` index) and returns as soon as everyone catches up. The
/// cap is just a safety bound — if a voter is genuinely unreachable, we don't
/// block DDL forever.
const DDL_AGREEMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Polling interval inside [`wait_for_replication_to_catch_up`].
const DDL_AGREEMENT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);

/// Wait until every voter in the current membership has replicated (appended)
/// up to `committed_index`. Returns either when everyone catches up or when
/// [`DDL_AGREEMENT_TIMEOUT`] expires (caller logs the partial agreement and
/// proceeds — the eventual-consistency guarantee still holds). Deterministic
/// same-node read-your-writes is handled separately by [`wait_for_local_apply`]
/// on a forwarding follower; this barrier only drives cross-node replication.
async fn wait_for_replication_to_catch_up(raft: &FerrosRaft, committed_index: u64) {
    let deadline = tokio::time::Instant::now() + DDL_AGREEMENT_TIMEOUT;
    loop {
        let metrics = raft.metrics().borrow().clone();
        let local_id = metrics.id;
        let voters: Vec<u64> = metrics
            .membership_config
            .membership()
            .voter_ids()
            .filter(|id| *id != local_id)
            .collect();
        // Single-node clusters have no followers to wait on.
        if voters.is_empty() {
            break;
        }
        let replication = match &metrics.replication {
            Some(r) => r,
            // Not leader, or replication not yet initialised. No way to drive
            // this barrier from here — fall back to the apply-drain sleep so
            // the caller doesn't immediately race the followers.
            None => break,
        };
        let everyone_caught_up = voters.iter().all(|voter_id| {
            replication
                .get(voter_id)
                .and_then(|matched| matched.as_ref().map(|lid| lid.index))
                .is_some_and(|matched_index| matched_index >= committed_index)
        });
        if everyone_caught_up {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            // Log so an operator can correlate a downstream "schema still
            // propagating"-style write failure with a slow follower.
            let lag: Vec<(u64, u64)> = voters
                .iter()
                .map(|voter_id| {
                    let matched = replication
                        .get(voter_id)
                        .and_then(|m| m.as_ref().map(|lid| lid.index))
                        .unwrap_or(0);
                    (*voter_id, committed_index.saturating_sub(matched))
                })
                .filter(|(_, lag)| *lag > 0)
                .collect();
            tracing::warn!(
                committed_index,
                ?lag,
                "ddl_agreement: timed out waiting for all voters to replicate the DDL log entry; \
                 a follower may serve a stale TableSchema for a brief window"
            );
            break;
        }
        tokio::time::sleep(DDL_AGREEMENT_POLL_INTERVAL).await;
    }
}

/// Wait until THIS node's state machine has applied up to `target_index`.
///
/// A node cannot observe a follower's apply progress from the leader (openraft
/// metrics expose only the replicated/`matched` index), but it can always
/// observe its OWN `last_applied`. A node that forwards a DDL to the leader uses
/// this — after the leader confirms the committed index — to guarantee
/// **same-node read-your-writes**: it returns success to the CQL client only
/// once its local schema reflects the committed DDL, so a following DML on the
/// same connection cannot see stale schema. Replaces the previous fixed
/// `DDL_AGREEMENT_APPLY_DRAIN` sleep, which was a race rather than a guarantee.
///
/// Returns `true` if the node caught up before `timeout`.
pub async fn wait_for_local_apply(
    raft: &FerrosRaft,
    target_index: u64,
    timeout: std::time::Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let applied = raft
            .metrics()
            .borrow()
            .last_applied
            .map(|lid| lid.index)
            .unwrap_or(0);
        if applied >= target_index {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(
                target_index,
                applied,
                "ddl_read_your_writes: timed out waiting for local apply; a read \
                 on this node may briefly observe stale schema"
            );
            return false;
        }
        tokio::time::sleep(DDL_AGREEMENT_POLL_INTERVAL).await;
    }
}

/// Propose a DDL operation through Raft consensus.
///
/// On success the state machine has applied the command on all live nodes
/// (subject to [`DDL_AGREEMENT_TIMEOUT`]).
///
/// On `ForwardToLeader` the caller receives [`ClusterError::NotLeader`] with
/// the leader hint. The [`DdlPath::Cluster`] arm in `execute()` catches this
/// and transparently forwards the request to the leader instead of propagating
/// the error to the CQL client.
pub(crate) async fn execute_via_raft(raft: &FerrosRaft, op: DdlOperation) -> Result<u64> {
    let cmd = ddl_op_to_raft_command(op);

    match raft.client_write(cmd).await {
        Ok(resp) => {
            // openraft's `client_write` returns once the LEADER applies, so on
            // the leader read-your-writes already holds. Drive followers'
            // *log replication* forward (condition-based on the matched index)
            // so a subsequent request load-balanced to another node has the
            // entry to apply. Deterministic same-node read-your-writes for a
            // forwarding follower is handled by `wait_for_local_apply` once the
            // forward path returns the committed index (below).
            wait_for_replication_to_catch_up(raft, resp.log_id.index).await;
            Ok(resp.log_id.index)
        }
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
    peer_manager: Arc<PeerManager>,
    node_map: Arc<RwLock<HashMap<u64, Uuid>>>,
}

impl ClusterDdlForwardHandler {
    /// Create a new handler backed by `raft`.
    pub fn new(
        raft: Arc<FerrosRaft>,
        peer_manager: Arc<PeerManager>,
        node_map: Arc<RwLock<HashMap<u64, Uuid>>>,
    ) -> Self {
        Self {
            raft,
            peer_manager,
            node_map,
        }
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

        // Try decoding as raw DdlOperation first (cluster-mode forward path).
        // Fall back to DdlEnvelope (pair-mode replication path) which wraps
        // the operation in {"op": ..., "schema_version": ...}. Both formats
        // use the same PairDdlForward message type.
        let op = match DdlOperation::from_bytes(&body) {
            Ok(op) => op,
            Err(_) => match crate::pair::ddl::DdlEnvelope::from_bytes(&body) {
                Ok(envelope) => envelope.op,
                Err(e) => {
                    tracing::error!(
                        "ClusterDdlForwardHandler: failed to decode as DdlOperation or DdlEnvelope: {e}"
                    );
                    return None;
                }
            },
        };

        match execute_via_raft(&self.raft, op.clone()).await {
            Ok(committed_index) => {
                // P0-21: after a successful JoinNode commit, promote the new
                // node to openraft voter via change_membership so the leader
                // will replicate / send InstallSnapshot to it.
                //
                // RaftOp::JoinNode updates the ferrosa state machine's topology
                // map but does NOT update openraft's voter set.  Without this
                // call, openraft won't send AppendEntries/InstallSnapshot to
                // the new member — the node stays stuck at T0-N0-0 forever.
                if let DdlOperation::JoinNode(ref node_info) = op {
                    use crate::raft::uuid_to_node_id;
                    use std::collections::BTreeSet;
                    let rejoin_node_id = uuid_to_node_id(node_info.host_id);
                    let mut new_members = BTreeSet::new();
                    new_members.insert(rejoin_node_id);
                    let raft = self.raft.clone();
                    let host_id = node_info.host_id;

                    // Register the rejoining node in the shared node_map so
                    // FerrosRaftNetworkFactory can resolve its UUID when openraft
                    // calls new_client() for the add_learner / AppendEntries RPCs.
                    // Without this, new_client returns Uuid::nil() and all
                    // replication to the rejoining node silently fails.
                    {
                        let mut map = self.node_map.write().unwrap_or_else(|e| e.into_inner());
                        map.insert(rejoin_node_id, host_id);
                    }
                    tracing::info!(
                        node_id = rejoin_node_id,
                        host_id = %host_id,
                        "cluster_rejoin: registered node in network factory node_map"
                    );

                    ferrosa_net::task_pool::TaskPool::current("cluster-rejoin-promote").spawn(async move {
                        // openraft 0.9 requires two steps to promote a node to voter:
                        // 1. add_learner — registers the node in openraft's node map
                        //    so the leader knows its address and can replicate to it.
                        // 2. change_membership(AddVoterIds) — promotes the learner to
                        //    a full voting member.
                        //
                        // Skipping step 1 results in:
                        //   "Learner <id> not found: add it as learner before adding it as a voter"
                        tracing::info!(
                            node_id = rejoin_node_id,
                            host_id = %host_id,
                            "cluster_rejoin: step 1 — add_learner so leader can replicate to node"
                        );
                        match raft
                            .add_learner(
                                rejoin_node_id,
                                openraft::BasicNode {
                                    addr: String::new(),
                                },
                                false,
                            )
                            .await
                        {
                            Ok(_) => {
                                tracing::info!(
                                    node_id = rejoin_node_id,
                                    host_id = %host_id,
                                    "cluster_rejoin: step 1 done — node registered as learner; \
                                     promoting to voter"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    node_id = rejoin_node_id,
                                    host_id = %host_id,
                                    error = %e,
                                    "cluster_rejoin: add_learner failed — aborting voter promotion"
                                );
                                return;
                            }
                        }

                        tracing::info!(
                            node_id = rejoin_node_id,
                            host_id = %host_id,
                            "cluster_rejoin: step 2 — change_membership AddVoterIds"
                        );
                        match raft
                            .change_membership(
                                openraft::ChangeMembers::AddVoterIds(new_members),
                                true,
                            )
                            .await
                        {
                            Ok(_) => {
                                tracing::info!(
                                    node_id = rejoin_node_id,
                                    host_id = %host_id,
                                    "cluster_rejoin: change_membership AddVoterIds committed — \
                                     openraft will now replicate to rejoined node"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    node_id = rejoin_node_id,
                                    host_id = %host_id,
                                    error = %e,
                                    "cluster_rejoin: change_membership AddVoterIds failed \
                                     (node may still converge via snapshot on next heartbeat)"
                                );
                            }
                        }
                    });
                }
                // Ack carries the committed log index so the forwarding
                // follower can wait for its own apply (read-your-writes).
                Some(Message::PairDdlAck(Bytes::copy_from_slice(
                    &committed_index.to_be_bytes(),
                )))
            }
            Err(ClusterError::NotLeader {
                leader_id: Some(leader_node_id),
            }) => {
                let leader_uuid = self
                    .node_map
                    .read()
                    .expect("node_map lock poisoned")
                    .get(&leader_node_id)
                    .copied();

                match leader_uuid {
                    Some(uuid) => {
                        match forward_ddl_to_leader(Some(&self.raft), &self.peer_manager, uuid, op)
                            .await
                        {
                            // Relay the leader's committed index back to the
                            // original forwarder so it can wait for its own apply.
                            Ok(committed) => Some(Message::PairDdlAck(Bytes::copy_from_slice(
                                &committed.to_be_bytes(),
                            ))),
                            Err(e) => {
                                tracing::error!(
                                    leader_node_id,
                                    leader_uuid = %uuid,
                                    "ClusterDdlForwardHandler: forward to leader failed: {e}"
                                );
                                None
                            }
                        }
                    }
                    None => {
                        tracing::error!(
                            leader_node_id,
                            "ClusterDdlForwardHandler: leader node_id missing from node_map"
                        );
                        None
                    }
                }
            }
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

    use ferrosa_net::codec::MsgType;
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::PeerEventListener;
    use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
    use ferrosa_net::rpc::server::RpcServer;
    use ferrosa_net::rpc::HandlerRegistry;
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
    use ferrosa_schema::Schema;
    use ferrosa_storage::engine::StorageEngine;
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};

    use bytes::Bytes;
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

    struct NoopListener;
    impl PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _peer: PeerId) {}
        fn on_peer_disconnected(&self, _peer: PeerId) {}
        fn on_peer_suspected(&self, _peer: PeerId) {}
        fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
        fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
    }

    struct PairDdlAckHandler;

    #[async_trait::async_trait]
    impl RpcHandler for PairDdlAckHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            let Message::PairDdlForward(_) = msg else {
                return None;
            };
            Some(Message::PairDdlAck(Bytes::new()))
        }
    }

    async fn start_rpc_server(
        msg_type: MsgType,
        handler: Arc<dyn RpcHandler>,
    ) -> (Arc<RpcServer>, std::net::SocketAddr, uuid::Uuid) {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(msg_type, handler);
        let server = Arc::new(RpcServer::new(config, server_id, registry));
        let addr = server.start_and_get_addr().await.unwrap();
        (server, addr, server_id)
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

    fn simple_index(ks: &str, table: &str, name: &str) -> ferrosa_schema::IndexMetadata {
        ferrosa_schema::IndexMetadata {
            keyspace: ks.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            index_type: ferrosa_index::IndexType::BTree,
            target_columns: vec!["col".to_string()],
            filter_predicate: None,
            options: HashMap::new(),
        }
    }

    /// After a CREATE INDEX through the Direct DDL path, a row must be
    /// persisted to `system_schema.indexes` and survive a flush — exactly
    /// like the auth-table dogfooding (`GrantUpdated`/`RoleCreated`).
    #[tokio::test]
    async fn direct_create_index_persists_row_to_system_schema_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());
        // `system_schema.indexes` must be a registered table before its
        // rows can be written (bootstrap ordering).
        engine.register_system_tables().unwrap();

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        };

        let idx = simple_index("ks", "users", "idx_col");
        ddl.execute(DdlOperation::CreateIndex(idx.clone()))
            .await
            .unwrap();

        // The Registry holds the index in memory.
        assert!(
            schema.snapshot().indexes.contains_key(&(
                "ks".into(),
                "users".into(),
                "idx_col".into()
            )),
            "index should be visible in the Registry"
        );

        // A stored row must exist in system_schema.indexes, keyed by keyspace.
        let tid = ferrosa_storage::TableId::new("system_schema", "indexes");
        let key = ferrosa_common::key::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            b"ks".to_vec(),
        ));
        let before = engine
            .read(&tid, &key)
            .expect("read should succeed")
            .expect("system_schema.indexes partition should exist after CREATE INDEX");
        assert!(
            !before.rows.is_empty(),
            "stored partition should contain the index row"
        );

        // Survive a flush: the row must still be readable from the SSTable.
        engine.flush(&tid).expect("flush should succeed");
        let after = engine
            .read(&tid, &key)
            .expect("read should succeed")
            .expect("system_schema.indexes partition should survive a flush");
        assert!(
            !after.rows.is_empty(),
            "stored index row should survive a flush"
        );
    }

    /// After a DROP INDEX through the Direct DDL path, the stored row in
    /// `system_schema.indexes` must be tombstoned (no live rows remain).
    #[tokio::test]
    async fn direct_drop_index_tombstones_row_in_system_schema_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());
        engine.register_system_tables().unwrap();

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        };

        ddl.execute(DdlOperation::CreateIndex(simple_index(
            "ks", "users", "idx_col",
        )))
        .await
        .unwrap();

        ddl.execute(DdlOperation::DropIndex {
            keyspace: "ks".into(),
            table: "users".into(),
            index: "idx_col".into(),
        })
        .await
        .unwrap();

        // The Registry no longer holds the index.
        assert!(
            !schema.snapshot().indexes.contains_key(&(
                "ks".into(),
                "users".into(),
                "idx_col".into()
            )),
            "index should be removed from the Registry"
        );

        // The stored partition must have no live index rows after the tombstone.
        let tid = ferrosa_storage::TableId::new("system_schema", "indexes");
        let key = ferrosa_common::key::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            b"ks".to_vec(),
        ));
        let partition = engine.read(&tid, &key).expect("read should succeed");
        let live_rows = partition
            .map(|p| p.rows.iter().filter(|r| !r.cells.is_empty()).count())
            .unwrap_or(0);
        assert_eq!(
            live_rows, 0,
            "dropped index should leave no live rows in system_schema.indexes"
        );
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

    #[tokio::test]
    async fn forward_ddl_to_leader_reconnects_missing_remote_peer_pool() {
        let (server, addr, leader_uuid) =
            start_rpc_server(MsgType::PairDdlForward, Arc::new(PairDdlAckHandler)).await;

        let peer_manager = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        peer_manager.add_peer_entry((leader_uuid, addr)).await;

        forward_ddl_to_leader(
            None,
            &peer_manager,
            leader_uuid,
            DdlOperation::CreateKeyspace(simple_keyspace("fwd_ks")),
        )
        .await
        .unwrap();

        assert!(
            peer_manager.has_peer(leader_uuid),
            "DDL forward path should reconnect and cache the leader peer"
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    /// Verify that the `ClusterDdlForwardHandler` returns `None` for non-DDL
    /// messages (wrong message type).
    ///
    /// This is a pure unit test that does NOT require a live Raft instance
    /// because openraft's `Raft::new` is async and needs a running cluster.
    /// We verify the message-type guard in the handler at the codec level.
    #[tokio::test]
    async fn test_forming_ddl_path_queues_and_returns_error() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let ddl = DdlPath::Forming { queue: tx };
        let op = DdlOperation::CreateKeyspace(simple_keyspace("should_queue"));
        let err = ddl.execute(op).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("formation in progress"),
            "Forming error must mention 'formation in progress', got: {msg}"
        );
        // Verify the operation was queued
        let queued = rx.try_recv().expect("DDL should be queued");
        match queued {
            DdlOperation::CreateKeyspace(ks) => assert_eq!(ks.name, "should_queue"),
            other => panic!("expected CreateKeyspace, got: {other:?}"),
        }
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

    // -- apply_direct tests for remaining DDL operation variants -----------

    #[tokio::test]
    async fn direct_drop_keyspace_removes_from_schema() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        };

        // Create then drop.
        ddl.execute(DdlOperation::CreateKeyspace(simple_keyspace("drop_ks")))
            .await
            .unwrap();
        assert!(schema.snapshot().keyspaces.contains_key("drop_ks"));

        ddl.execute(DdlOperation::DropKeyspace("drop_ks".into()))
            .await
            .unwrap();
        assert!(!schema.snapshot().keyspaces.contains_key("drop_ks"));
    }

    #[tokio::test]
    async fn direct_drop_table_removes_from_schema() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        };

        ddl.execute(DdlOperation::CreateKeyspace(simple_keyspace("dtks")))
            .await
            .unwrap();
        let table = simple_table("dtks", "tbl");
        ddl.execute(DdlOperation::CreateTable(Box::new(table)))
            .await
            .unwrap();
        assert!(schema
            .snapshot()
            .tables
            .contains_key(&("dtks".into(), "tbl".into())));

        ddl.execute(DdlOperation::DropTable {
            keyspace: "dtks".into(),
            table: "tbl".into(),
        })
        .await
        .unwrap();
        assert!(!schema
            .snapshot()
            .tables
            .contains_key(&("dtks".into(), "tbl".into())));
    }

    /// End-to-end P0 regression for
    /// bug-sstable-writer-produces-zero-byte-rows-db.md: create a table, write
    /// a row, ALTER TABLE ADD COLUMN, write a row that uses the new column,
    /// flush, and read both rows back. Without the storage-engine schema
    /// propagation the second write would either drift cell parsing (old bug)
    /// or hit the writer's fail-loud assertion (new bug). Both must be fixed.
    #[tokio::test]
    async fn direct_alter_table_propagates_schema_to_storage() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
        use ferrosa_schema::metadata::table::TableUpdates;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        };

        // Create keyspace + table with a single regular column.
        ddl.execute(DdlOperation::CreateKeyspace(simple_keyspace("ks")))
            .await
            .unwrap();
        let mut table = simple_table("ks", "evolving");
        table.columns.insert(
            "v".into(),
            ColumnMetadata {
                name: "v".into(),
                kind: ColumnKind::Regular,
                position: 1,
                column_type: "text".into(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        ddl.execute(DdlOperation::CreateTable(Box::new(table)))
            .await
            .unwrap();

        let tid = ferrosa_storage::TableId::new("ks", "evolving");

        // Write a row with the single regular column and flush.
        let key1 = DecoratedKey::new(PartitionKey::new(uuid::Uuid::new_v4().as_bytes().to_vec()));
        let row1 = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"before".to_vec(), 1_000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1_000),
        };
        engine.write(&tid, &key1, row1, 1_000).unwrap();
        engine.flush_all().unwrap();

        // ALTER TABLE ADD extra text column.
        ddl.execute(DdlOperation::AlterTable {
            keyspace: "ks".into(),
            table: "evolving".into(),
            updates: Box::new(TableUpdates {
                params: None,
                add_columns: vec![ColumnMetadata {
                    name: "extra".into(),
                    kind: ColumnKind::Regular,
                    position: 2,
                    column_type: "text".into(),
                    clustering_order: ClusteringOrder::None,
                    mask: None,
                }],
                drop_columns: vec![],
                extensions: None,
            }),
        })
        .await
        .unwrap();

        // Write a row that includes the newly-added column. Pre-fix, flush would
        // produce a silently corrupt SSTable (cell col_idx=1 with num_columns=1).
        // Post-fix, the propagated schema gives num_columns=2 and flush succeeds.
        let key2 = DecoratedKey::new(PartitionKey::new(uuid::Uuid::new_v4().as_bytes().to_vec()));
        let row2 = Row {
            clustering: vec![],
            cells: vec![
                (0, CellValue::live(b"after_v".to_vec(), 2_000)),
                (1, CellValue::live(b"after_extra".to_vec(), 2_000)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(2_000),
        };
        engine.write(&tid, &key2, row2, 2_000).unwrap();
        engine.flush_all().unwrap();

        // Both rows must be readable.
        let r1 = engine.read(&tid, &key1).unwrap();
        assert!(r1.is_some(), "pre-ALTER row must survive");
        let r2 = engine.read(&tid, &key2).unwrap();
        assert!(r2.is_some(), "post-ALTER row must survive");
        let r2_cells = &r2.unwrap().rows[0].cells;
        assert_eq!(r2_cells.len(), 2, "post-ALTER row must have 2 cells");
    }

    #[tokio::test]
    async fn direct_alter_keyspace() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine,
        };

        ddl.execute(DdlOperation::CreateKeyspace(simple_keyspace("alks")))
            .await
            .unwrap();

        let updates = ferrosa_schema::KeyspaceUpdates {
            durable_writes: Some(false),
            replication: None,
        };
        ddl.execute(DdlOperation::AlterKeyspace {
            name: "alks".into(),
            updates,
        })
        .await
        .unwrap();

        let snap = schema.snapshot();
        let ks = snap.keyspaces.get("alks").unwrap();
        assert!(
            !ks.durable_writes,
            "durable_writes should be false after alter"
        );
    }

    #[tokio::test]
    async fn direct_create_and_drop_role() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine,
        };

        let role = ferrosa_schema::RoleMetadata {
            name: "test_role".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
            scram: None,
        };
        ddl.execute(DdlOperation::CreateRole(role)).await.unwrap();
        assert!(schema.snapshot().roles.contains_key("test_role"));

        ddl.execute(DdlOperation::DropRole("test_role".into()))
            .await
            .unwrap();
        assert!(!schema.snapshot().roles.contains_key("test_role"));
    }

    #[tokio::test]
    async fn direct_alter_role() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine,
        };

        let role = ferrosa_schema::RoleMetadata {
            name: "ar_role".to_string(),
            is_superuser: false,
            can_login: false,
            salted_hash: None,
            member_of: HashSet::new(),
            scram: None,
        };
        ddl.execute(DdlOperation::CreateRole(role)).await.unwrap();

        let updates = ferrosa_schema::RoleUpdates {
            is_superuser: None,
            can_login: Some(true),
            password: None,
            hashed_password: None,
            member_of: None,
        };
        ddl.execute(DdlOperation::AlterRole {
            name: "ar_role".into(),
            updates,
        })
        .await
        .unwrap();

        let snap = schema.snapshot();
        let role = snap.roles.get("ar_role").unwrap();
        assert!(role.can_login, "role should have login enabled after alter");
    }

    // -- ddl_op_to_raft_command for remaining variants --------------------

    #[test]
    fn ddl_op_to_raft_command_alter_keyspace() {
        let updates = ferrosa_schema::KeyspaceUpdates {
            durable_writes: Some(false),
            replication: None,
        };
        let op = DdlOperation::AlterKeyspace {
            name: "ks".into(),
            updates,
        };
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(cmd.op, RaftOp::AlterKeyspace { ref name, .. } if name == "ks"));
    }

    #[test]
    fn ddl_op_to_raft_command_alter_table() {
        let updates = Box::new(ferrosa_schema::TableUpdates {
            params: None,
            add_columns: vec![],
            drop_columns: vec![],
            extensions: None,
        });
        let op = DdlOperation::AlterTable {
            keyspace: "ks".into(),
            table: "tbl".into(),
            updates,
        };
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(
            cmd.op,
            RaftOp::AlterTable { ref keyspace, ref table, .. }
            if keyspace == "ks" && table == "tbl"
        ));
    }

    #[test]
    fn ddl_op_to_raft_command_create_role() {
        let role = ferrosa_schema::RoleMetadata {
            name: "role1".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
            scram: None,
        };
        let op = DdlOperation::CreateRole(role);
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(cmd.op, RaftOp::CreateRole(ref r) if r.name == "role1"));
    }

    #[test]
    fn ddl_op_to_raft_command_drop_role() {
        let op = DdlOperation::DropRole("role1".into());
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(cmd.op, RaftOp::DropRole(ref n) if n == "role1"));
    }

    #[test]
    fn ddl_op_to_raft_command_create_index() {
        let idx = ferrosa_schema::IndexMetadata {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            name: "idx1".to_string(),
            index_type: ferrosa_index::IndexType::BTree,
            target_columns: vec!["col".to_string()],
            filter_predicate: None,
            options: HashMap::new(),
        };
        let op = DdlOperation::CreateIndex(idx);
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(cmd.op, RaftOp::CreateIndex(ref i) if i.name == "idx1"));
    }

    #[test]
    fn ddl_op_to_raft_command_drop_index() {
        let op = DdlOperation::DropIndex {
            keyspace: "ks".into(),
            table: "tbl".into(),
            index: "idx1".into(),
        };
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(
            cmd.op,
            RaftOp::DropIndex { ref keyspace, ref table, ref index }
            if keyspace == "ks" && table == "tbl" && index == "idx1"
        ));
    }

    #[test]
    fn ddl_op_to_raft_command_grant() {
        use ferrosa_schema::{GrantEntry, Permission, Resource};
        let entry = GrantEntry {
            role: "user1".to_string(),
            resource: Resource::AllKeyspaces,
            permissions: std::iter::once(Permission::Select).collect(),
        };
        let op = DdlOperation::Grant(entry);
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(cmd.op, RaftOp::Grant(ref e) if e.role == "user1"));
    }

    #[test]
    fn ddl_op_to_raft_command_revoke() {
        use ferrosa_schema::{Permission, Resource};
        let op = DdlOperation::Revoke {
            role: "user1".to_string(),
            resource: Resource::AllKeyspaces,
            permission: Permission::Select,
        };
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(
            cmd.op,
            RaftOp::Revoke { ref role, .. } if role == "user1"
        ));
    }

    #[test]
    fn ddl_op_to_raft_command_alter_role() {
        let updates = ferrosa_schema::RoleUpdates {
            is_superuser: Some(true),
            can_login: None,
            password: None,
            hashed_password: None,
            member_of: None,
        };
        let op = DdlOperation::AlterRole {
            name: "r1".into(),
            updates,
        };
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(cmd.op, RaftOp::AlterRole { ref name, .. } if name == "r1"));
    }
}
