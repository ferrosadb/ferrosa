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

use crate::raft::{
    uuid_to_node_id, FerrosRaft, NodeInfo, NodeState, RaftCommand, RaftGroupId, RaftOp,
    DEFAULT_DC_NAME,
};

use ferrosa_common::{AccordTimestamp, TxnId};

// ---------------------------------------------------------------------------
// NodeJoinConfig — knobs for `add_learner_only` (W8.2 / ADR-014).
// ---------------------------------------------------------------------------

/// Optional configuration for [`MembershipChanger::add_learner_only`].
///
/// `owns_tokens=true` (default) makes the learner participate in the
/// ring (read replicas, repair) — appropriate for capacity expansion
/// and DR replicas. `owns_tokens=false` keeps the learner as a
/// state-machine-only follower — appropriate for analytics or future
/// witness roles.
///
/// Construct with [`NodeJoinConfig::default`] for typical cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeJoinConfig {
    /// Whether this learner owns ring tokens.
    pub owns_tokens: bool,
}

impl Default for NodeJoinConfig {
    fn default() -> Self {
        Self { owns_tokens: true }
    }
}

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
// AccordDrainQuery — abstracted access to in-flight Accord txns (W7.8).
// ---------------------------------------------------------------------------

/// W7.8 / I-30 — interface the [`MembershipChanger::swap_dc`] uses to
/// query the Accord coordinator pool for in-flight transactions
/// referencing a leaving DC's voters.
///
/// In production this is wired to the cross-DC Accord coordinator's
/// txn registry (`AccordCoordinator` + `EpochDrain`); tests use a
/// deterministic stub. Decoupling avoids pulling the Accord runtime
/// into `membership/` and keeps `swap_dc` unit-testable.
pub trait AccordDrainQuery: Send + Sync {
    /// Return every Accord txn currently in-flight that references at
    /// least one of `voters` as a participant. An empty `Vec` means
    /// the drain has completed.
    fn inflight_for_voters(&self, voters: &[u64]) -> Vec<TxnId>;
}

/// Outcome of a [`MembershipChanger::swap_dc`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwapDcOutcome {
    /// The drain finished within the deadline. `iterations` records
    /// how many polls it took (operator-visible signal that gives
    /// production some idea how long the drain ran).
    Drained {
        /// Number of poll iterations until the drain reported zero
        /// in-flight txns.
        iterations: usize,
    },
    /// The drain did not complete before the deadline. `remaining`
    /// reports how many txns were still in flight at timeout — the
    /// operator MUST inspect them and either retry the swap or abort
    /// the txns explicitly.
    TimedOut {
        /// Remaining in-flight txns at the moment the deadline expired.
        remaining: usize,
    },
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

/// Pick a deterministic target voter for `transfer_to` (W4.14).
///
/// Excludes `self_node_id` (the node being decommissioned).  Returns
/// the lowest-numbered other voter from the current effective
/// membership; deterministic for tests, harmless in production where
/// any voter is fine.  Returns `None` when no other voter exists
/// (e.g. single-node cluster trying to remove its sole member —
/// callers must reject that earlier).
pub(crate) fn pick_transfer_target(
    metrics: &openraft::RaftMetrics<u64, openraft::BasicNode>,
    self_node_id: u64,
) -> Option<u64> {
    metrics
        .membership_config
        .membership()
        .voter_ids()
        .filter(|&id| id != self_node_id)
        .min()
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
///
/// W6.4 (ADR-015): a `MembershipChanger` is scoped to a single Raft
/// group via [`Self::dc_name`] / [`Self::group_id`]. Multi-DC
/// deployments instantiate one changer per DC; single-DC deployments
/// transparently use the [`DEFAULT_DC_NAME`] group via
/// [`Self::new`].
pub struct MembershipChanger<N: MembershipNetwork> {
    raft: Arc<FerrosRaft>,
    network: Arc<N>,
    apply_timeout: Duration,
    /// Default `data_center`/`rack` recorded for new joiners; matches
    /// the production `ClusterConfig` defaults.  Production callers
    /// override via [`Self::with_node_metadata_defaults`].
    default_dc: String,
    default_rack: String,
    /// DC this changer operates on (W6.4). Drives `default_dc` for
    /// new joiners by default and identifies the Raft group via
    /// [`RaftGroupId::for_dc`].
    dc_name: String,
}

impl<N: MembershipNetwork> MembershipChanger<N> {
    /// Construct a fresh changer.  In production this is built once
    /// per node at cluster-mode entry; in tests, one per test.
    ///
    /// Backward-compat: defaults to the [`DEFAULT_DC_NAME`] group, so
    /// existing single-DC callers do not need to pass a DC. Multi-DC
    /// callers should use [`Self::for_dc`].
    pub fn new(raft: Arc<FerrosRaft>, network: Arc<N>) -> Self {
        Self::for_dc(DEFAULT_DC_NAME, raft, network)
    }

    /// Construct a changer scoped to the given DC's Raft group (W6.4).
    ///
    /// The `raft` argument MUST be the `Arc<FerrosRaft>` for that DC's
    /// group — typically obtained via
    /// `controller.raft_for_dc(dc_name)`.
    pub fn for_dc(dc_name: impl Into<String>, raft: Arc<FerrosRaft>, network: Arc<N>) -> Self {
        let dc_name = dc_name.into();
        // Default new-joiner DC matches the changer's DC. Operators can
        // still override via [`Self::with_node_metadata_defaults`] for
        // unusual topologies.
        let default_dc = dc_name.clone();
        Self {
            raft,
            network,
            apply_timeout: DEFAULT_APPLY_TIMEOUT,
            default_dc,
            default_rack: "rack1".to_string(),
            dc_name,
        }
    }

    /// DC this changer is bound to.
    pub fn dc_name(&self) -> &str {
        &self.dc_name
    }

    /// [`RaftGroupId`] of the group this changer operates on.
    pub fn group_id(&self) -> RaftGroupId {
        RaftGroupId::for_dc(&self.dc_name)
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

    /// Maximum wall-clock to wait for the post-transfer new leader to
    /// be observable in metrics (W4.14).  openraft's Trigger::transfer_to
    /// returns once `current_leader` shifts; we pad the
    /// `election_timeout_max` budget by 2× to absorb engine-tick
    /// scheduling jitter on a slow CI host.
    pub(crate) const LEADERSHIP_TRANSFER_OBSERVE_MS: u64 = 5_000;

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

    async fn transfer_leadership_away_from(
        &self,
        node_id: u64,
        target: u64,
        context: &str,
    ) -> Result<Option<u64>, MembershipError> {
        let transfer_result = self.raft.trigger().transfer_to(target).await;
        let deadline =
            std::time::Instant::now() + Duration::from_millis(Self::LEADERSHIP_TRANSFER_OBSERVE_MS);
        loop {
            let m = self.raft.metrics().borrow().clone();
            if m.current_leader.is_some() && m.current_leader != Some(node_id) {
                return Ok(m.current_leader);
            }
            if std::time::Instant::now() >= deadline {
                return match transfer_result {
                    Ok(()) => Err(MembershipError::RaftError(format!(
                        "{context}: leadership transfer dispatched but new leader not observed"
                    ))),
                    Err(e) => Err(MembershipError::RaftError(format!(
                        "{context}: transfer_to({target}) failed and no replacement leader was observed: {e}"
                    ))),
                };
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// W7.8 / I-30 — Drain in-flight Accord transactions for a DC swap.
    ///
    /// Polls `drain` for transactions referencing any of `leaving_voters`
    /// every `poll_interval` until either the in-flight set is empty
    /// (returns [`SwapDcOutcome::Drained`]) or `deadline` elapses
    /// (returns [`SwapDcOutcome::TimedOut`]).
    ///
    /// The caller is expected to issue the joint
    /// `change_membership(AddVoters + RemoveVoters)` after a successful
    /// drain — the drain itself is the protocol invariant. Callers that
    /// hit `TimedOut` MUST decide whether to abort the in-flight txns
    /// (Accord recovery), retry the swap, or escalate.
    ///
    /// `deadline` defaults to 60 s in production callers per ADR-015
    /// W7.8 REFACTOR; tests pass tighter values.
    pub async fn swap_dc(
        &self,
        leaving_voters: &[u64],
        drain: &dyn AccordDrainQuery,
        deadline: Duration,
        poll_interval: Duration,
    ) -> Result<SwapDcOutcome, MembershipError> {
        let started = std::time::Instant::now();
        let mut iterations = 0usize;
        loop {
            let inflight = drain.inflight_for_voters(leaving_voters);
            if inflight.is_empty() {
                return Ok(SwapDcOutcome::Drained { iterations });
            }
            if started.elapsed() >= deadline {
                return Ok(SwapDcOutcome::TimedOut {
                    remaining: inflight.len(),
                });
            }
            tokio::time::sleep(poll_interval).await;
            iterations += 1;
        }
    }

    /// W7.6 — Apply-durability barrier for Accord vote-commits.
    ///
    /// Submit a `RaftOp::AccordApply { txn_id, hlc, mutation }` through
    /// this DC's Raft group and **block** until openraft reports the
    /// resulting log entry as applied on the local state machine
    /// (`wait().applied_index_at_least(commit_index)`).
    ///
    /// The Accord coordinator MUST call this — not raw `client_write`
    /// — so that "vote-committed" implies "durably applied on the
    /// local DC's Raft group". Without the barrier, the coordinator
    /// could mark a txn committed before the apply landed and lose
    /// data on crash.
    ///
    /// Idempotent at the state-machine layer: replays of the same
    /// `txn_id` short-circuit at `state.applied_accord_txns` (I-28).
    ///
    /// `apply_timeout` defaults to [`Self::with_apply_timeout`].
    /// Returns:
    /// - `Ok(())` once the local Raft has applied the entry;
    /// - `Err(MembershipError::NotLeader { .. })` if forwarded;
    /// - `Err(MembershipError::ApplyTimeout)` if the wait times out;
    /// - `Err(MembershipError::RaftError(..))` for openraft errors.
    pub async fn accord_vote_commit(
        &self,
        txn_id: TxnId,
        hlc: AccordTimestamp,
        mutation: Vec<u8>,
    ) -> Result<(), MembershipError> {
        let cmd = RaftCommand {
            op: RaftOp::AccordApply {
                txn_id,
                hlc,
                mutation,
            },
            schema_version: Uuid::new_v4(),
        };

        // Submit via openraft. ForwardToLeader → NotLeader.
        let resp = match self.raft.client_write(cmd).await {
            Ok(r) => r,
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                return Err(MembershipError::NotLeader {
                    leader_node_id: fwd.leader_id,
                });
            }
            Err(other) => {
                return Err(MembershipError::RaftError(format!(
                    "client_write(AccordApply): {other}"
                )));
            }
        };

        // Apply-durability barrier (W7.6). openraft's wait() resolves
        // when last_applied advances to or past the entry's log_id —
        // which is the durability guarantee Accord vote-commit
        // requires.
        let target_idx = resp.log_id.index;
        self.raft
            .wait(Some(self.apply_timeout))
            .applied_index_at_least(Some(target_idx), "accord_vote_commit")
            .await
            .map_err(|e| match e {
                openraft::metrics::WaitError::Timeout(_, _) => MembershipError::ApplyTimeout,
                openraft::metrics::WaitError::ShuttingDown => {
                    MembershipError::RaftError("raft is shutting down".into())
                }
            })?;

        Ok(())
    }

    // ---------------------------------------------------------------
    // Internal helper: shared peer-setup for add_voter / add_learner_only.
    // ---------------------------------------------------------------

    /// Step 1 + 2 of joining a node: register in the network factory
    /// and call `raft.add_learner`. Used by both [`Self::add_voter`]
    /// (which then promotes) and [`Self::add_learner_only`] (which
    /// stops here).
    async fn join_as_learner(
        &self,
        host_id: Uuid,
        addr: SocketAddr,
    ) -> Result<u64, MembershipError> {
        let node_id = uuid_to_node_id(host_id);

        // Network factory map. Idempotent insert.
        self.network.register_node(node_id, host_id);

        let basic = openraft::BasicNode {
            addr: addr.to_string(),
        };
        retry_on_inprogress("add_learner", || async {
            self.raft.add_learner(node_id, basic.clone(), true).await
        })
        .await?;

        Ok(node_id)
    }

    /// Submit a `RaftOp::JoinNode(NodeInfo)` so every follower's
    /// `state.members` reflects the new node. Returns the
    /// `MembershipError::NotLeader` variant if forwarded.
    async fn submit_join_node(&self, info: NodeInfo) -> Result<(), MembershipError> {
        let cmd = RaftCommand {
            op: RaftOp::JoinNode(info),
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
                "client_write(JoinNode): {other}"
            ))),
        }
    }

    // ---------------------------------------------------------------
    // W8.2 / W8.3 — Learner lifecycle
    // ---------------------------------------------------------------

    /// W8.2 — Add a node as a long-lived learner. Idempotent.
    ///
    /// Steps (per ADR-014):
    /// 1. register_node in the network factory (map 3).
    /// 2. `raft.add_learner` — joins openraft consensus as a learner.
    /// 3. `raft.client_write(RaftOp::JoinNode)` with `state =
    ///    NodeState::Learner { owns_tokens }` — application metadata
    ///    (map 1).
    ///
    /// Crucially this **omits** the `change_membership(AddVoters)` step
    /// that [`Self::add_voter`] performs. Quorum size is therefore
    /// unchanged; the new node receives `AppendEntries` and applies
    /// log entries but never votes.
    pub async fn add_learner_only(
        &self,
        host_id: Uuid,
        addr: SocketAddr,
        config: NodeJoinConfig,
    ) -> Result<(), MembershipError> {
        // Steps 1 + 2.
        self.join_as_learner(host_id, addr).await?;

        // Step 3 — application-level JoinNode with Learner state.
        let info = NodeInfo {
            host_id,
            addr: addr.to_string(),
            data_center: self.default_dc.clone(),
            rack: self.default_rack.clone(),
            state: NodeState::Learner {
                owns_tokens: config.owns_tokens,
            },
            cql_broadcast: None,
        };
        self.submit_join_node(info).await?;

        Ok(())
    }

    /// W8.3 — Promote an existing learner to a voter.
    ///
    /// Steps:
    /// 1. `raft.change_membership(AddVoterIds)` — promotes in openraft
    ///    (map 2). The learner's log was already replicated; this
    ///    extends it forward, never rewinds it.
    /// 2. `raft.client_write(RaftOp::SetNodeState { state: Normal })` —
    ///    application metadata (map 1).
    ///
    /// Idempotent: re-promoting an already-voter is a NoOp (openraft
    /// returns early).
    pub async fn promote_learner_to_voter(&self, host_id: Uuid) -> Result<(), MembershipError> {
        let node_id = uuid_to_node_id(host_id);

        // Step 1 — promote via joint consensus.
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

        // Step 2 — flip application state to Normal.
        let cmd = RaftCommand {
            op: RaftOp::SetNodeState {
                node_id,
                state: NodeState::Normal,
            },
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
                "client_write(SetNodeState=Normal): {other}"
            ))),
        }
    }

    /// W8.3 — Demote a voter back to a learner.
    ///
    /// Steps:
    /// 1. If the target is the current leader, transfer leadership to
    ///    another voter first (mirrors [`Self::remove_voter`]'s W4.14
    ///    self-transfer).
    /// 2. `raft.change_membership(RemoveVoters + AddLearners)` —
    ///    drops from voter set, adds back as learner (map 2).
    /// 3. `raft.client_write(RaftOp::SetNodeState { state: Learner
    ///    { owns_tokens: true } })` — application metadata (map 1).
    ///
    /// `owns_tokens=true` is the conservative default: a freshly
    /// demoted voter still has all the data and the ring should keep
    /// using it. Operators wanting `owns_tokens=false` should call
    /// `update_metadata` afterwards.
    pub async fn demote_voter_to_learner(&self, host_id: Uuid) -> Result<(), MembershipError> {
        let node_id = uuid_to_node_id(host_id);

        // Step 1 — leader-self transfer if needed.
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader == Some(node_id) {
            let target = pick_transfer_target(&metrics, node_id).ok_or_else(|| {
                MembershipError::RaftError(
                    "demote_voter_to_learner: no eligible voter for leadership transfer".into(),
                )
            })?;
            let new_leader = self
                .transfer_leadership_away_from(node_id, target, "demote_voter_to_learner")
                .await?;
            // Caller must re-issue the demote on the new leader.
            return Err(MembershipError::NotLeader {
                leader_node_id: new_leader,
            });
        }

        // Step 2 — joint-consensus swap.
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

        // Re-add as learner. openraft tracks learners and voters in the
        // same node-map; the previous step removed it from voters,
        // we re-introduce it as a learner via add_learner. This is a
        // NoOp if openraft already considers it a learner.
        let basic = metrics
            .membership_config
            .nodes()
            .find(|(id, _)| **id == node_id)
            .map(|(_, n)| n.clone())
            .unwrap_or(openraft::BasicNode::default());
        retry_on_inprogress("add_learner(demote)", || async {
            self.raft.add_learner(node_id, basic.clone(), true).await
        })
        .await?;

        // Step 3 — flip application state to Learner.
        let cmd = RaftCommand {
            op: RaftOp::SetNodeState {
                node_id,
                state: NodeState::Learner { owns_tokens: true },
            },
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
                "client_write(SetNodeState=Learner): {other}"
            ))),
        }
    }

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
        // Steps 1 + 2 — share with add_learner_only.
        let node_id = self.join_as_learner(host_id, addr).await?;

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
        self.submit_join_node(info).await?;

        // Step 5 — apply barrier handled by callers (test asserts).
        Ok(())
    }

    /// Remove a node from the cluster.
    ///
    /// Steps (per ADR-013, updated for Sprint 4 W4.14):
    /// 1. If the target IS the current leader, transfer leadership to
    ///    another voter via `raft.trigger().transfer_to(other)`,
    ///    awaiting the new leader before proceeding.  No more
    ///    `TransferFirst` punt onto the operator — the changer drives
    ///    the transfer itself.
    /// 2. raft.change_membership(RemoveVoters) — drops from openraft (map 2).
    /// 3. raft.client_write(RaftOp::LeaveNode) — drops from state.members (map 1).
    /// 4. network_factory.unregister_node (map 3).
    pub async fn remove_voter(&self, host_id: Uuid) -> Result<(), MembershipError> {
        let node_id = uuid_to_node_id(host_id);

        // Step 1 — leader-self transfer (W4.14).
        //
        // If `node_id` is the current leader, transfer leadership to
        // another voter before issuing change_membership.  This avoids
        // the in-flight write window that produced
        // S-07 (decommission of leader without transfer).  The choice
        // of target is the lowest-numbered other voter — deterministic
        // for tests, harmless in production where any voter is fine.
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader == Some(node_id) {
            let target = pick_transfer_target(&metrics, node_id).ok_or_else(|| {
                MembershipError::RaftError(
                    "decommission_leader_transfers_first: no eligible voter for leadership transfer"
                        .into(),
                )
            })?;
            let new_leader = self
                .transfer_leadership_away_from(node_id, target, "remove_voter")
                .await?;
            // After transfer the local node is a follower and Steps
            // 2-4 below will surface ForwardToLeader.  Caller drives
            // the forward via Message::ClusterMembershipForward as
            // for any non-leader change.
            return Err(MembershipError::NotLeader {
                leader_node_id: new_leader,
            });
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

    /// Approve `host_id` to join the cluster (auto_join=false path).
    ///
    /// Replicates [`RaftOp::ApproveNode`] through Raft so every
    /// follower's `state.approved_nodes` reflects it (W1.6, ADR-013
    /// § "RaftOp::ApproveNode is no longer dead code").  Today's
    /// `controller::approve_node` only mutates the local cache —
    /// that's a future-removed code path; new callers must go
    /// through this API.
    ///
    /// Idempotent: re-approving a host_id is a NoOp on apply.
    pub async fn approve_node(&self, host_id: Uuid) -> Result<(), MembershipError> {
        let cmd = RaftCommand {
            op: RaftOp::ApproveNode { host_id },
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
                "client_write(ApproveNode): {other}"
            ))),
        }
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
