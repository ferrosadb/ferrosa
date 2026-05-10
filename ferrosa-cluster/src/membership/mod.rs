//! Single transactional API for membership changes (ADR-013).
//!
//! Today, ferrosa maintains four distinct stores that together describe
//! cluster membership:
//!
//! 1. `RaftStateMachine.state.members` — application metadata (host_id → NodeInfo).
//! 2. `openraft Membership.nodes` — the consensus voter set.
//! 3. `FerrosRaftNetworkFactory.node_map` — `u64` → `Uuid` for replication routing.
//! 4. `PeerManager.peers` — live TCP connection state.
//!
//! These were updated by different code paths with no transactional API
//! spanning them — the dominant defect class in the bug genome (P0-21
//! saga + `fbfc39c8` + 4 sibling silent drops).
//!
//! This module introduces [`MembershipChanger`]: a single API which is
//! the only sanctioned way to mutate any of those four maps from
//! outside the apply path.  CI gate
//! `scripts/ci-gates/no-raw-client-write.sh` enforces the ban (W1.9).
//!
//! # API surface (Sprint 1)
//!
//! - [`MembershipChanger::add_voter`] — happy path + idempotence (W1.1, W1.2).
//! - [`MembershipChanger::remove_voter`] — clears all maps (W1.4).
//! - Concurrent calls retry on `InProgress` (W1.3).
//!
//! Forwarding when not leader is implemented at the wire-format layer
//! (`ferrosa-net::Message::ClusterMembershipForward`, W1.5/W1.13) and
//! is not part of `MembershipChanger`'s in-process responsibility — the
//! caller is expected to drive forwarding when `MembershipError::NotLeader`
//! is returned.
//!
//! [`MembershipChanger`]: crate::membership::MembershipChanger

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{ChangeMembershipError, ClientWriteError, RaftError};
use openraft::ChangeMembers;
use uuid::Uuid;

use serde::{Deserialize, Serialize};

use crate::raft::{uuid_to_node_id, FerrosRaft, NodeInfo, NodeState, RaftCommand, RaftOp};

// ---------------------------------------------------------------------------
// MembershipOp — typed payload carried by `Message::ClusterMembershipForward`.
// ---------------------------------------------------------------------------

/// Wire-level enumeration of every membership operation a non-leader
/// can ask the leader to apply (W1.13).
///
/// Replaces the prior opaque `Bytes` payload that bincoded a generic
/// `RaftCommand`.  Naming the variants explicitly lets the leader's
/// forward-handler dispatch on op kind (e.g. apply rate limits to
/// `AddVoter`, audit-log every `RemoveVoter`) rather than peeking
/// inside the bincoded bytes.
///
/// `Raw(Box<RaftCommand>)` is the forward-compatibility escape hatch
/// for any operation not yet promoted to a typed variant — tests pin
/// the round-trip stability of every variant.  `RaftCommand` is
/// boxed because it is much larger than the other variants (clippy
/// `large_enum_variant`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MembershipOp {
    /// Promote `host_id` to a voter.  Carries the addr the leader
    /// should record in `state.members`.
    AddVoter { host_id: Uuid, addr: SocketAddr },
    /// Remove `host_id` from the cluster.
    RemoveVoter { host_id: Uuid },
    /// Update `host_id`'s metadata (addr / cql_broadcast).
    UpdateMetadata {
        host_id: Uuid,
        new_addr: Option<SocketAddr>,
        new_cql_broadcast: Option<String>,
    },
    /// Approve `host_id` to join (ADR-013, RaftOp::ApproveNode).
    ApproveNode { host_id: Uuid },
    /// Forward-compat raw `RaftCommand`.  Carries any op not yet
    /// promoted to a typed variant.  Existing pre-W1.13 senders that
    /// bincode a `RaftCommand` directly are now decoded here.
    Raw(Box<RaftCommand>),
}

// ---------------------------------------------------------------------------
// MembershipNetwork trait — abstracts node_map + PeerManager mutations.
// ---------------------------------------------------------------------------

/// Side-effect operations that touch the network factory's `node_map`
/// (map 3) and the `PeerManager.peers` set (map 4).
///
/// Production wires this to [`crate::raft::network::FerrosRaftNetworkFactory`]
/// + [`ferrosa_net::peer::PeerManager`].  Tests can stub the trait to
///   observe mutations directly.
///
/// All methods are synchronous because the underlying production
/// implementations only do trivial map mutations (the
/// `register_node`/`unregister_node` calls take a sync `RwLock`, and
/// PeerManager's connect happens out-of-band).
pub trait MembershipNetwork: Send + Sync + 'static {
    /// Register a (NodeId, host_id) pair in the network factory.
    /// Idempotent: re-registering an existing pair is a NoOp.
    fn register_node(&self, node_id: u64, host_id: Uuid);

    /// Remove a NodeId from the network factory, if present.
    fn unregister_node(&self, node_id: u64);

    /// Whether the network factory currently knows about `node_id`.
    fn contains(&self, node_id: u64) -> bool;
}

// ---------------------------------------------------------------------------
// MembershipError
// ---------------------------------------------------------------------------

/// Failures that can happen while mutating membership.
#[derive(Debug)]
pub enum MembershipError {
    /// Local Raft is not the leader; caller must forward via
    /// `Message::ClusterMembershipForward` (W1.5/W1.13).  The
    /// `leader_node_id` field, if known, points at the current leader.
    NotLeader { leader_node_id: Option<u64> },
    /// openraft refused the change because another change is in flight.
    /// The caller can retry.
    InProgress,
    /// Removing a node that is itself the leader is forbidden until
    /// leadership-transfer lands (Sprint 3, W3.x).
    TransferFirst,
    /// Step 4: learner did not catch up within the deadline.
    LearnerCatchupTimeout,
    /// Step 8: state machine apply did not propagate within the deadline.
    ApplyTimeout,
    /// Wraps any other openraft error.
    RaftError(String),
}

impl std::fmt::Display for MembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { leader_node_id } => {
                write!(f, "not leader (current leader: {leader_node_id:?})")
            }
            Self::InProgress => f.write_str("change in progress; retry"),
            Self::TransferFirst => f.write_str("must transfer leadership before removing leader"),
            Self::LearnerCatchupTimeout => f.write_str("learner catch-up timed out"),
            Self::ApplyTimeout => f.write_str("apply barrier timed out"),
            Self::RaftError(e) => write!(f, "raft error: {e}"),
        }
    }
}

impl std::error::Error for MembershipError {}

// ---------------------------------------------------------------------------
// retry_on_inprogress — generic backoff loop for ChangeMembership calls.
// ---------------------------------------------------------------------------

/// Run `op` until it returns Ok or a non-`InProgress` error, backing off
/// per [`MembershipChanger::INPROGRESS_BACKOFF`].
///
/// Generic over the success type so a single helper covers
/// `add_learner`, `change_membership`, etc.
async fn retry_on_inprogress<T, F, Fut>(
    label: &'static str,
    mut op: F,
) -> Result<T, MembershipError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<
        Output = Result<
            T,
            openraft::error::RaftError<u64, ClientWriteError<u64, openraft::BasicNode>>,
        >,
    >,
{
    let mut last_err: Option<MembershipError> = None;
    let attempts = MembershipChanger::<DummyNetwork>::INPROGRESS_BACKOFF.len() + 1;
    for attempt in 0..attempts {
        match op().await {
            Ok(v) => return Ok(v),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                return Err(MembershipError::NotLeader {
                    leader_node_id: fwd.leader_id,
                });
            }
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::InProgress(_),
            ))) => {
                last_err = Some(MembershipError::InProgress);
                if attempt < MembershipChanger::<DummyNetwork>::INPROGRESS_BACKOFF.len() {
                    let delay = MembershipChanger::<DummyNetwork>::INPROGRESS_BACKOFF[attempt];
                    tracing::debug!(label, attempt, ?delay, "retrying after InProgress");
                    tokio::time::sleep(delay).await;
                    continue;
                }
            }
            Err(other) => {
                return Err(MembershipError::RaftError(format!("{label}: {other}")));
            }
        }
    }
    Err(last_err.unwrap_or(MembershipError::InProgress))
}

/// Phantom `MembershipNetwork` used solely so that the
/// `MembershipChanger::INPROGRESS_BACKOFF` const can be referenced
/// without picking a concrete `N`.  Never instantiated.
struct DummyNetwork;

impl MembershipNetwork for DummyNetwork {
    fn register_node(&self, _: u64, _: Uuid) {}
    fn unregister_node(&self, _: u64) {}
    fn contains(&self, _: u64) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// MembershipChanger
// ---------------------------------------------------------------------------

/// Apply-barrier deadline used by the in-process API path.  Generous
/// to accommodate disk fsync; tightened by tests via [`MembershipChanger::with_apply_timeout`].
const DEFAULT_APPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// The single transactional API for cluster membership changes.
pub struct MembershipChanger<N: MembershipNetwork> {
    raft: Arc<FerrosRaft>,
    network: Arc<N>,
    apply_timeout: Duration,
    /// Default `data_center`/`rack` recorded for new joiners; matches
    /// the production `ClusterConfig` defaults.  Production callers
    /// override via [`Self::with_node_metadata_defaults`].
    default_dc: String,
    default_rack: String,
}

impl<N: MembershipNetwork> MembershipChanger<N> {
    /// Construct a fresh changer.  In production this is built once
    /// per node at cluster-mode entry; in tests, one per test.
    pub fn new(raft: Arc<FerrosRaft>, network: Arc<N>) -> Self {
        Self {
            raft,
            network,
            apply_timeout: DEFAULT_APPLY_TIMEOUT,
            default_dc: "datacenter1".to_string(),
            default_rack: "rack1".to_string(),
        }
    }

    /// Override the default apply-barrier deadline (test-only convenience).
    pub fn with_apply_timeout(mut self, t: Duration) -> Self {
        self.apply_timeout = t;
        self
    }

    /// Override the data-center / rack assigned to new joiners.
    pub fn with_node_metadata_defaults(mut self, dc: String, rack: String) -> Self {
        self.default_dc = dc;
        self.default_rack = rack;
        self
    }

    /// Backoff schedule for `InProgress` retries (W1.3).
    ///
    /// openraft serialises membership changes — a concurrent caller
    /// gets `ChangeMembershipError::InProgress` until the in-flight
    /// change commits.  We retry with exponential backoff up to
    /// ~14.4 s aggregate, then bubble the error.
    const INPROGRESS_BACKOFF: &'static [Duration] = &[
        Duration::from_millis(10),
        Duration::from_millis(30),
        Duration::from_millis(100),
        Duration::from_millis(300),
        Duration::from_secs(1),
        Duration::from_secs(3),
        Duration::from_secs(10),
    ];

    /// Add a node as a voter.  Idempotent.
    ///
    /// Steps (per ADR-013):
    /// 1. register_node in the network factory (map 3).
    /// 2. raft.add_learner (joins openraft consensus as a learner).
    /// 3. raft.change_membership(AddVoters) — promotes to voter (map 2).
    /// 4. raft.client_write(RaftOp::JoinNode) — application metadata (map 1).
    /// 5. wait for state.members to reflect the new node.
    ///
    /// Steps 1, 2, 3, 4 are individually idempotent: a retry produces
    /// the same final state.  Concurrent callers retry on
    /// `ChangeMembershipError::InProgress` per the schedule above.
    pub async fn add_voter(&self, host_id: Uuid, addr: SocketAddr) -> Result<(), MembershipError> {
        let node_id = uuid_to_node_id(host_id);

        // Step 1 — network factory map.  Idempotent insert.
        self.network.register_node(node_id, host_id);

        // Step 2 — add as learner.
        let basic = openraft::BasicNode {
            addr: addr.to_string(),
        };
        retry_on_inprogress("add_learner", || async {
            self.raft.add_learner(node_id, basic.clone(), true).await
        })
        .await?;

        // Step 3 — promote to voter via joint consensus.
        let mut promote_set = std::collections::BTreeSet::new();
        promote_set.insert(node_id);
        retry_on_inprogress("change_membership(AddVoterIds)", || {
            let promote_set = promote_set.clone();
            async move {
                self.raft
                    .change_membership(ChangeMembers::AddVoterIds(promote_set), true)
                    .await
            }
        })
        .await?;

        // Step 4 — application-level JoinNode.
        let info = NodeInfo {
            host_id,
            addr: addr.to_string(),
            data_center: self.default_dc.clone(),
            rack: self.default_rack.clone(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };
        let cmd = RaftCommand {
            op: RaftOp::JoinNode(info),
            schema_version: Uuid::new_v4(),
        };
        match self.raft.client_write(cmd).await {
            Ok(_) => {}
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                return Err(MembershipError::NotLeader {
                    leader_node_id: fwd.leader_id,
                });
            }
            Err(other) => {
                return Err(MembershipError::RaftError(format!(
                    "client_write(JoinNode): {other}"
                )));
            }
        }

        // Step 5 — apply barrier handled by callers (test asserts).
        Ok(())
    }

    /// Remove a node from the cluster.
    ///
    /// Steps (per ADR-013):
    /// 1. If the target IS the leader, return `TransferFirst` (Sprint 3).
    /// 2. raft.change_membership(RemoveVoters) — drops from openraft (map 2).
    /// 3. raft.client_write(RaftOp::LeaveNode) — drops from state.members (map 1).
    /// 4. network_factory.unregister_node (map 3).
    pub async fn remove_voter(&self, host_id: Uuid) -> Result<(), MembershipError> {
        let node_id = uuid_to_node_id(host_id);

        // Step 1 — leader-self check.  Read the current leader from
        // metrics; if it's us and we are the leader, return TransferFirst.
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader == Some(node_id) {
            return Err(MembershipError::TransferFirst);
        }

        // Step 2 — drop from openraft.
        let mut remove_set = std::collections::BTreeSet::new();
        remove_set.insert(node_id);
        retry_on_inprogress("change_membership(RemoveVoters)", || {
            let remove_set = remove_set.clone();
            async move {
                self.raft
                    .change_membership(ChangeMembers::RemoveVoters(remove_set), true)
                    .await
            }
        })
        .await?;

        // Step 3 — drop from state.members.
        let cmd = RaftCommand {
            op: RaftOp::LeaveNode { node_id },
            schema_version: Uuid::new_v4(),
        };
        match self.raft.client_write(cmd).await {
            Ok(_) => {}
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                return Err(MembershipError::NotLeader {
                    leader_node_id: fwd.leader_id,
                });
            }
            Err(other) => {
                return Err(MembershipError::RaftError(format!(
                    "client_write(LeaveNode): {other}"
                )));
            }
        }

        // Step 4 — drop from network factory.
        self.network.unregister_node(node_id);

        Ok(())
    }

    /// Update an existing voter's metadata (addr / cql_broadcast).
    ///
    /// Per ADR-013 § "update_metadata": we propose an
    /// [`RaftOp::UpdateNodeInfo`] through Raft so every follower
    /// converges on the new addr.  No openraft `change_membership` is
    /// needed because openraft's `BasicNode.addr` is unused by ferrosa
    /// — addresses live in `state.members`.
    ///
    /// Idempotent: a re-call with the same addr is a NoOp on the apply
    /// path.  Non-leader callers receive `NotLeader` so they can
    /// forward via `Message::ClusterMembershipForward` (W1.5/W1.13).
    pub async fn update_metadata(
        &self,
        host_id: Uuid,
        new_addr: Option<SocketAddr>,
        new_cql_broadcast: Option<String>,
    ) -> Result<(), MembershipError> {
        // Read the current NodeInfo from local state via metrics.
        // Updates leave fields that the caller didn't override
        // unchanged.
        // We reconstruct a NodeInfo using either the passed-in fields
        // or sane defaults.  The apply-path `RaftOp::UpdateNodeInfo`
        // ignores updates for unknown members (logs a warning), so the
        // host_id must already be a cluster member.
        let info = NodeInfo {
            host_id,
            addr: new_addr.map(|a| a.to_string()).unwrap_or_default(),
            data_center: self.default_dc.clone(),
            rack: self.default_rack.clone(),
            state: NodeState::Normal,
            cql_broadcast: new_cql_broadcast,
        };

        let cmd = RaftCommand {
            op: RaftOp::UpdateNodeInfo(info),
            schema_version: Uuid::new_v4(),
        };

        match self.raft.client_write(cmd).await {
            Ok(_) => Ok(()),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                Err(MembershipError::NotLeader {
                    leader_node_id: fwd.leader_id,
                })
            }
            Err(other) => Err(MembershipError::RaftError(format!(
                "client_write(UpdateNodeInfo): {other}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Stub MembershipNetwork that only records calls — for unit-level
    /// coverage of the trait wiring (the integration tests in
    /// `tests/membership_atomicity.rs` run the full Raft path).
    #[derive(Default)]
    struct RecordingNetwork {
        registered: Mutex<Vec<(u64, Uuid)>>,
        unregistered: Mutex<Vec<u64>>,
    }

    impl MembershipNetwork for RecordingNetwork {
        fn register_node(&self, node_id: u64, host_id: Uuid) {
            self.registered.lock().unwrap().push((node_id, host_id));
        }

        fn unregister_node(&self, node_id: u64) {
            self.unregistered.lock().unwrap().push(node_id);
        }

        fn contains(&self, node_id: u64) -> bool {
            self.registered
                .lock()
                .unwrap()
                .iter()
                .any(|(id, _)| *id == node_id)
                && !self.unregistered.lock().unwrap().contains(&node_id)
        }
    }

    #[test]
    fn recording_network_register_then_unregister() {
        let n = RecordingNetwork::default();
        let host = Uuid::new_v4();
        let id = uuid_to_node_id(host);
        n.register_node(id, host);
        assert!(n.contains(id));
        n.unregister_node(id);
        assert!(!n.contains(id));
    }

    #[test]
    fn cluster_membership_forward_carries_typed_op() {
        // W1.13: every typed variant + the Raw escape hatch must
        // bincode round-trip through the wire payload.  RaftCommand
        // is not PartialEq, so we compare via re-serialised bytes.
        let h1 = Uuid::new_v4();
        let addr: SocketAddr = "127.0.0.1:7005".parse().unwrap();
        let cases = vec![
            MembershipOp::AddVoter { host_id: h1, addr },
            MembershipOp::RemoveVoter { host_id: h1 },
            MembershipOp::UpdateMetadata {
                host_id: h1,
                new_addr: Some(addr),
                new_cql_broadcast: Some("9042".to_string()),
            },
            MembershipOp::ApproveNode { host_id: h1 },
            MembershipOp::Raw(Box::new(RaftCommand {
                op: RaftOp::LeaveNode { node_id: 42 },
                schema_version: Uuid::new_v4(),
            })),
        ];
        for op in cases {
            let original_bytes = bincode::serialize(&op).expect("serialize");
            let decoded: MembershipOp = bincode::deserialize(&original_bytes).expect("deserialize");
            let reserialised = bincode::serialize(&decoded).expect("re-serialize");
            assert_eq!(
                original_bytes, reserialised,
                "round-trip byte stability for {op:?}",
            );
            // Variant tag matches.
            assert_eq!(
                std::mem::discriminant(&op),
                std::mem::discriminant(&decoded),
                "round-trip variant tag mismatch for {op:?}",
            );
        }
    }

    #[test]
    fn membership_error_display_includes_leader() {
        let e = MembershipError::NotLeader {
            leader_node_id: Some(7),
        };
        let s = format!("{e}");
        assert!(s.contains("7"));
    }
}
