//! Raft consensus integration for Ferrosa.
//!
//! This module wires openraft into the ferrosa-cluster crate.  It declares
//! the `RaftTypeConfig` implementation, the `RaftCommand` application-data
//! enum, supporting node-state types, and a convenience helper for mapping
//! `Uuid` node identifiers to openraft's `u64` `NodeId` space.

pub mod handlers;
pub mod log_store;
pub mod network;
pub mod state_machine;

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
}

// ---------------------------------------------------------------------------
// RaftCommand
// ---------------------------------------------------------------------------

/// Every command that can be written to the Raft log.
///
/// The DDL variants mirror [`crate::pair::ddl::DdlOperation`] so that the
/// same operations work in both pair mode and full Raft cluster mode.
/// The topology variants extend that set with node-membership changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
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

    // ---- Node admission ------------------------------------------------
    /// Approve a node that has requested admission to the cluster.
    ApproveNode {
        host_id: Uuid,
    },
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

    // ---- bincode serde round-trips --------------------------------------

    #[test]
    fn raft_command_serde_roundtrip() {
        let cmd = RaftCommand::CreateKeyspace(simple_keyspace("test_ks"));
        // RaftCommand doesn't implement PartialEq, so encode/decode and spot-check.
        let encoded = bincode::serialize(&cmd).expect("serialize");
        let decoded: RaftCommand = bincode::deserialize(&encoded).expect("deserialize");
        match decoded {
            RaftCommand::CreateKeyspace(ks) => assert_eq!(ks.name, "test_ks"),
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
        };
        let cmd = RaftCommand::JoinNode(node.clone());
        let encoded = bincode::serialize(&cmd).expect("serialize");
        let decoded: RaftCommand = bincode::deserialize(&encoded).expect("deserialize");
        match decoded {
            RaftCommand::JoinNode(n) => {
                assert_eq!(n.host_id, host_id);
                assert_eq!(n.addr, "10.0.0.1:7000");
                assert_eq!(n.state, NodeState::Joining);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn approve_node_command_serializes() {
        let cmd = RaftCommand::ApproveNode {
            host_id: Uuid::new_v4(),
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: RaftCommand = bincode::deserialize(&bytes).unwrap();
        assert!(matches!(decoded, RaftCommand::ApproveNode { .. }));
    }

    #[test]
    fn raft_command_assign_tokens_roundtrip() {
        let node_id = uuid_to_node_id(Uuid::new_v4());
        let tokens: Vec<Token> = vec![-9_223_372_036_854_775_808, 0, 9_223_372_036_854_775_807];
        let cmd = RaftCommand::AssignTokens {
            node_id,
            tokens: tokens.clone(),
        };
        let encoded = bincode::serialize(&cmd).expect("serialize");
        let decoded: RaftCommand = bincode::deserialize(&encoded).expect("deserialize");
        match decoded {
            RaftCommand::AssignTokens {
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
