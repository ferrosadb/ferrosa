//! Raft consensus integration for Ferrosa.
//!
//! This module wires openraft into the ferrosa-cluster crate.  It declares
//! the `RaftTypeConfig` implementation, the `RaftCommand` application-data
//! enum, supporting node-state types, and a convenience helper for mapping
//! `Uuid` node identifiers to openraft's `u64` `NodeId` space.

pub mod election_guard;
pub mod group_id;
pub mod handlers;
pub mod log_store;
pub mod network;
pub mod snapshot_pusher;
pub mod state_machine;

pub use group_id::{RaftGroupId, DEFAULT_DC_NAME};

use std::io::Cursor;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use ferrosa_common::CqlType;
use ferrosa_schema::metadata::aggregate::UserAggregateMetadata;
use ferrosa_schema::metadata::function::UserFunctionMetadata;
use ferrosa_schema::metadata::index::IndexMetadata;
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, KeyspaceUpdates};
use ferrosa_schema::metadata::table::{TableMetadata, TableUpdates};
use ferrosa_schema::metadata::user_type::UserTypeMetadata;
use ferrosa_schema::{GrantEntry, Permission, Resource, RoleMetadata, RoleUpdates};

use crate::config::ClusterConfig;

// ---------------------------------------------------------------------------
// Raft type configuration
// ---------------------------------------------------------------------------

openraft::declare_raft_types!(
    /// Ferrosa's concrete openraft type configuration.
    pub FerrosRaftConfig:
        D            = RaftCommand,
        R            = RaftResponse,
        NodeId       = u64,
        Node         = openraft::BasicNode,
        Entry        = openraft::Entry<FerrosRaftConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

/// Raft log token — matches Cassandra's signed 64-bit Murmur3 hash range.
pub type Token = i64;

/// The running Raft node handle.
pub type FerrosRaft = openraft::Raft<FerrosRaftConfig>;

// ---------------------------------------------------------------------------
// NodeState
// ---------------------------------------------------------------------------

/// Lifecycle state of a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is bootstrapping and has not yet been accepted by the cluster.
    Joining,
    /// Node is active and serving traffic.
    Normal,
    /// Node is being drained before removal.
    Leaving,
    /// Node has been fully removed from the ring.
    Decommissioned,
}

// ---------------------------------------------------------------------------
// NodeInfo
// ---------------------------------------------------------------------------

/// Cluster-level metadata about a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// UUID assigned to this node at first boot (stable across restarts).
    pub host_id: Uuid,
    /// Internode address, e.g. `"192.168.1.10:7000"`.
    pub addr: String,
    /// Cassandra-style data-center name.
    pub data_center: String,
    /// Rack name within the data center.
    pub rack: String,
    /// Current lifecycle state.
    pub state: NodeState,
    /// CQL broadcast address (host:port) for system.peers.
    /// When set, overrides addr for native_address in system.peers.
    /// Parsed from FERROSA_CQL_BROADCAST on each node.
    #[serde(default)]
    pub cql_broadcast: Option<String>,
}

// ---------------------------------------------------------------------------
// IndexNodeStatus
// ---------------------------------------------------------------------------

/// Per-node build status for a secondary index.
///
/// Replicated via Raft so all nodes see consistent index readiness.
/// Used in [`state_machine::RaftState::index_state_map`] to track which
/// nodes have finished building which indexes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexNodeStatus {
    /// The node is actively building the index.
    Building,
    /// The index is fully built and ready for queries on this node.
    Ready,
    /// The last build attempt on this node failed.
    Failed(String),
    /// The index is out of date (e.g., compaction produced new SSTables).
    Stale,
}

// ---------------------------------------------------------------------------
// RaftOp — the concrete DDL/admin operation
// ---------------------------------------------------------------------------

/// The concrete DDL/admin operation. Carried inside [`RaftCommand`].
///
/// The DDL variants mirror [`crate::pair::ddl::DdlOperation`] so that the
/// same operations work in both pair mode and full Raft cluster mode.
/// The topology variants extend that set with node-membership changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftOp {
    // ---- DDL (schema mutations) ----------------------------------------
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
    CreateIndex(IndexMetadata),
    DropIndex {
        keyspace: String,
        table: String,
        index: String,
    },
    /// Report per-node index build status (Building/Ready/Failed/Stale).
    IndexStatus {
        /// openraft NodeId of the reporting node.
        node_id: u64,
        /// Keyspace of the indexed table.
        keyspace: String,
        /// Table the index belongs to.
        table: String,
        /// Name of the index.
        index_name: String,
        /// New status for this node's copy of the index.
        status: IndexNodeStatus,
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

    // ---- Topology (node-membership mutations) ---------------------------
    /// A new node is requesting admission to the cluster.
    JoinNode(NodeInfo),
    /// Refresh address metadata for an existing cluster member.
    UpdateNodeInfo(NodeInfo),
    /// A node is departing the cluster gracefully.
    LeaveNode {
        /// openraft `NodeId` for the departing node (`uuid_to_node_id(host_id)`).
        node_id: u64,
    },
    /// Reassign virtual token ranges to a node.
    AssignTokens {
        /// openraft `NodeId` of the target node.
        node_id: u64,
        /// New set of token assignments.
        tokens: Vec<Token>,
    },

    // ---- Config ---------------------------------------------------------
    /// Replace the cluster-wide configuration.
    UpdateConfig(ClusterConfig),

    // ---- Node lifecycle -------------------------------------------------
    /// Approve a node that has requested admission to the cluster.
    ApproveNode {
        host_id: Uuid,
    },
    /// Promote a node from Joining to Normal after bootstrap completes.
    SetNodeState {
        node_id: u64,
        state: NodeState,
    },
}

// ---------------------------------------------------------------------------
// RaftCommand — a Raft log entry wrapping an op + leader-generated version
// ---------------------------------------------------------------------------

/// A Raft log entry: an operation plus a leader-generated schema version.
///
/// The `schema_version` UUID is generated once by the Raft leader when the
/// command is created and replicated to all followers as part of the Raft log
/// entry. This ensures every node ends up with the same schema version after
/// applying the command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftCommand {
    /// The concrete operation to apply.
    pub op: RaftOp,
    /// Schema version UUID generated by the leader.
    pub schema_version: Uuid,
}

// ---------------------------------------------------------------------------
// RaftResponse
// ---------------------------------------------------------------------------

/// Response produced by the state machine after applying a [`RaftCommand`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftResponse {
    /// The command was applied successfully.
    Ok,
    /// The command was rejected with a human-readable reason.
    Error(String),
}

// ---------------------------------------------------------------------------
// Helper: uuid_to_node_id
// ---------------------------------------------------------------------------

/// Convert a `Uuid` to a `u64` openraft `NodeId` using its lower 64 bits.
///
/// This is deterministic (same UUID always produces the same ID) but relies on
/// the statistical uniqueness of the lower half of UUIDs to avoid collisions in
/// practice.  For `v4` (random) UUIDs the probability of a 64-bit collision
/// across typical cluster sizes (≤ 1 000 nodes) is negligible.
pub fn uuid_to_node_id(id: Uuid) -> u64 {
    let bytes = id.as_bytes();
    u64::from_le_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};

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

    // ---- uuid_to_node_id ------------------------------------------------

    #[test]
    fn uuid_to_node_id_deterministic() {
        let id = Uuid::new_v4();
        let a = uuid_to_node_id(id);
        let b = uuid_to_node_id(id);
        assert_eq!(a, b, "same UUID must always produce the same NodeId");
    }

    #[test]
    fn uuid_to_node_id_unique() {
        // Draw 1 000 random UUIDs and verify all 64-bit node IDs are distinct.
        let ids: Vec<u64> = (0..1_000)
            .map(|_| uuid_to_node_id(Uuid::new_v4()))
            .collect();
        let unique: std::collections::HashSet<u64> = ids.iter().copied().collect();
        assert_eq!(
            ids.len(),
            unique.len(),
            "1 000 random UUIDs must not collide in their lower 64 bits"
        );
    }

    /// Wrap a `RaftOp` into a `RaftCommand` with a random schema version.
    fn wrap(op: RaftOp) -> RaftCommand {
        RaftCommand {
            op,
            schema_version: Uuid::new_v4(),
        }
    }

    // ---- bincode serde round-trips --------------------------------------

    #[test]
    fn raft_command_serde_roundtrip() {
        let cmd = wrap(RaftOp::CreateKeyspace(simple_keyspace("test_ks")));
        // RaftCommand doesn't implement PartialEq, so encode/decode and spot-check.
        let encoded = bincode::serialize(&cmd).expect("serialize");
        let decoded: RaftCommand = bincode::deserialize(&encoded).expect("deserialize");
        match decoded.op {
            RaftOp::CreateKeyspace(ks) => assert_eq!(ks.name, "test_ks"),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn raft_command_join_node_roundtrip() {
        let host_id = Uuid::new_v4();
        let node = NodeInfo {
            host_id,
            addr: "10.0.0.1:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Joining,
            cql_broadcast: None,
        };
        let cmd = wrap(RaftOp::JoinNode(node.clone()));
        let encoded = bincode::serialize(&cmd).expect("serialize");
        let decoded: RaftCommand = bincode::deserialize(&encoded).expect("deserialize");
        match decoded.op {
            RaftOp::JoinNode(n) => {
                assert_eq!(n.host_id, host_id);
                assert_eq!(n.addr, "10.0.0.1:7000");
                assert_eq!(n.state, NodeState::Joining);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn approve_node_command_serializes() {
        let cmd = wrap(RaftOp::ApproveNode {
            host_id: Uuid::new_v4(),
        });
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: RaftCommand = bincode::deserialize(&bytes).unwrap();
        assert!(matches!(decoded.op, RaftOp::ApproveNode { .. }));
    }

    #[test]
    fn index_node_status_serde_roundtrip() {
        let statuses = vec![
            IndexNodeStatus::Building,
            IndexNodeStatus::Ready,
            IndexNodeStatus::Failed("disk full".to_string()),
            IndexNodeStatus::Stale,
        ];
        for status in statuses {
            let encoded = bincode::serialize(&status).expect("serialize");
            let decoded: IndexNodeStatus = bincode::deserialize(&encoded).expect("deserialize");
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn raft_command_index_status_roundtrip() {
        let node_id = uuid_to_node_id(Uuid::new_v4());
        let cmd = wrap(RaftOp::IndexStatus {
            node_id,
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            index_name: "idx_email".to_string(),
            status: IndexNodeStatus::Ready,
        });
        let encoded = bincode::serialize(&cmd).expect("serialize");
        let decoded: RaftCommand = bincode::deserialize(&encoded).expect("deserialize");
        match decoded.op {
            RaftOp::IndexStatus {
                node_id: rid,
                keyspace,
                table,
                index_name,
                status,
            } => {
                assert_eq!(rid, node_id);
                assert_eq!(keyspace, "ks");
                assert_eq!(table, "tbl");
                assert_eq!(index_name, "idx_email");
                assert_eq!(status, IndexNodeStatus::Ready);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn raft_command_index_status_failed_roundtrip() {
        let cmd = wrap(RaftOp::IndexStatus {
            node_id: 42,
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            index_name: "idx".to_string(),
            status: IndexNodeStatus::Failed("OOM".to_string()),
        });
        let encoded = bincode::serialize(&cmd).expect("serialize");
        let decoded: RaftCommand = bincode::deserialize(&encoded).expect("deserialize");
        match decoded.op {
            RaftOp::IndexStatus { status, .. } => {
                assert_eq!(status, IndexNodeStatus::Failed("OOM".to_string()));
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn raft_command_assign_tokens_roundtrip() {
        let node_id = uuid_to_node_id(Uuid::new_v4());
        let tokens: Vec<Token> = vec![-9_223_372_036_854_775_808, 0, 9_223_372_036_854_775_807];
        let cmd = wrap(RaftOp::AssignTokens {
            node_id,
            tokens: tokens.clone(),
        });
        let encoded = bincode::serialize(&cmd).expect("serialize");
        let decoded: RaftCommand = bincode::deserialize(&encoded).expect("deserialize");
        match decoded.op {
            RaftOp::AssignTokens {
                node_id: rid,
                tokens: rt,
            } => {
                assert_eq!(rid, node_id);
                assert_eq!(rt, tokens);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }
}
