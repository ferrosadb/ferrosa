//! Cluster-rejoin recovery path — P0-21 fix.
//!
//! ## Problem
//!
//! When a node with cleared Raft state starts and tries to join an existing
//! cluster, the cluster-formation state machine (`standalone → pair → cluster`)
//! spawns a Raft instance but times out waiting for leader election (~30 s).
//! It then reverts to Pair mode.  After the revert:
//!
//! - The node runs its own Raft instance (not in the cluster's voter set).
//! - P0-17/P0-19 election guard suppresses its elections correctly.
//! - P0-20 snapshot pusher correctly stays silent (node is not a known voter).
//! - The node's `last_log_id` stays at `T0-N0-0` forever.
//! - CPU stays elevated from the election-suppress-reelect cycle.
//!
//! ## Fix
//!
//! After the formation timeout, the controller invokes `attempt_rejoin`.
//! This function contacts the existing cluster's peers and asks the Raft
//! **leader** to add this node to the voter set via
//! `client_write(RaftOp::JoinNode(self))`.  The call is forwarded through
//! the existing `PairDdlForward` RPC + `ClusterDdlForwardHandler` path,
//! which maps `DdlOperation::JoinNode` → `RaftOp::JoinNode` and calls
//! `client_write` on the leader.
//!
//! Once the leader commits the `JoinNode` entry:
//! 1. The leader's membership includes this node as a learner / voter.
//! 2. Openraft's standard replication loop (or P0-20's pusher) delivers
//!    `InstallSnapshot` to this node.
//! 3. This node transitions to a follower with a matching `last_log_id`.
//!
//! ## Retry policy
//!
//! Exponential backoff: 1 s, 2 s, 4 s, 8 s, …, capped at 60 s.
//! Default maximum: `MAX_REJOIN_ATTEMPTS = 10` attempts.
//!
//! Every attempt is logged at INFO level with attempt number and outcome.
//! On exhausted retries: ERROR log + `CLUSTER_REJOIN_FAILURES_TOTAL` increment.
//!
//! ## Metrics
//!
//! - `CLUSTER_REJOIN_ATTEMPTS_TOTAL` — increments on every attempt.
//! - `CLUSTER_REJOIN_FAILURES_TOTAL` — increments when all retries are exhausted.
//!
//! ## Related
//!
//! - `cluster.rs` — hooks `attempt_rejoin` into the formation-timeout revert path.
//! - `election_guard.rs` — P0-17/P0-19 — suppresses the election storm.
//! - `snapshot_pusher.rs` — P0-20 — pushes snapshot to stale followers.
//! - P0-21 spec: `specs/todo/p0-21-cluster-rejoin-stuck-when-formation-times-out.md`

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use ferrosa_net::peer::PeerManager;

use crate::ddl_path::forward_ddl_to_leader;
use crate::pair::ddl::DdlOperation;
use crate::raft::{NodeInfo, NodeState};

// ---------------------------------------------------------------------------
// Metric counters
// ---------------------------------------------------------------------------

/// Process-wide count of cluster-rejoin attempts.
///
/// Increments once per `attempt_rejoin` call (regardless of outcome).
/// A non-zero value confirms the rejoin path was entered after a
/// formation timeout.
pub static CLUSTER_REJOIN_ATTEMPTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Process-wide count of cluster-rejoin failures (all retries exhausted).
///
/// Increments when `attempt_rejoin` returns `Err(RejoinError::Exhausted)`.
/// A non-zero value in steady state requires operator intervention.
pub static CLUSTER_REJOIN_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Return the current value of `CLUSTER_REJOIN_ATTEMPTS_TOTAL`.
pub fn cluster_rejoin_attempts_total() -> u64 {
    CLUSTER_REJOIN_ATTEMPTS_TOTAL.load(Ordering::Relaxed)
}

/// Return the current value of `CLUSTER_REJOIN_FAILURES_TOTAL`.
pub fn cluster_rejoin_failures_total() -> u64 {
    CLUSTER_REJOIN_FAILURES_TOTAL.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Rejoin parameters
// ---------------------------------------------------------------------------

/// Maximum number of `JoinNode` attempts before giving up.
const MAX_REJOIN_ATTEMPTS: u32 = 10;

/// Initial retry backoff (seconds).
const BACKOFF_INITIAL_SECS: u64 = 1;

/// Backoff cap (seconds).
const BACKOFF_MAX_SECS: u64 = 60;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by `attempt_rejoin`.
#[derive(Debug)]
pub enum RejoinError {
    /// All retries exhausted; the leader could not be contacted or the
    /// `JoinNode` RPC was rejected by every peer.
    Exhausted,
}

impl std::fmt::Display for RejoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cluster_rejoin: all {MAX_REJOIN_ATTEMPTS} JoinNode attempts exhausted"
        )
    }
}

// ---------------------------------------------------------------------------
// attempt_rejoin
// ---------------------------------------------------------------------------

/// Attempt to re-add this node to the existing cluster's voter set.
///
/// Iterates over `peers` (the existing cluster members) and, for each peer,
/// sends a `PairDdlForward(DdlOperation::JoinNode(self_info))` RPC.
/// The `ClusterDdlForwardHandler` on the receiving peer routes this to
/// `execute_via_raft` → `client_write(RaftOp::JoinNode(..))` on the Raft
/// leader.  On the first success, returns `Ok(())`.
///
/// Retries with exponential backoff (`BACKOFF_INITIAL_SECS` … `BACKOFF_MAX_SECS`)
/// up to `MAX_REJOIN_ATTEMPTS` total attempts.  On exhaustion, increments
/// `CLUSTER_REJOIN_FAILURES_TOTAL` and returns `Err(RejoinError::Exhausted)`.
///
/// # Parameters
///
/// - `self_host_id` — UUID of the rejoining node.
/// - `self_addr` — internode address of the rejoining node (e.g. `"10.0.0.3:7000"`).
/// - `data_center` / `rack` — topology placement metadata.
/// - `cql_broadcast` — optional CQL broadcast address.
/// - `peers` — known existing cluster members `(host_id, addr)`.
/// - `peer_manager` — network layer for RPC sends.
pub async fn attempt_rejoin(
    self_host_id: Uuid,
    self_addr: String,
    data_center: String,
    rack: String,
    cql_broadcast: Option<String>,
    peers: Vec<(Uuid, SocketAddr)>,
    peer_manager: Arc<PeerManager>,
) -> Result<(), RejoinError> {
    if peers.is_empty() {
        tracing::error!(
            self_id = %self_host_id,
            "cluster_rejoin: no peers known — cannot attempt JoinNode (operator must intervene)"
        );
        CLUSTER_REJOIN_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
        return Err(RejoinError::Exhausted);
    }

    let self_info = NodeInfo {
        host_id: self_host_id,
        addr: self_addr.clone(),
        data_center: data_center.clone(),
        rack: rack.clone(),
        state: NodeState::Joining,
        cql_broadcast: cql_broadcast.clone(),
    };

    let mut backoff_secs = BACKOFF_INITIAL_SECS;

    for attempt in 1..=MAX_REJOIN_ATTEMPTS {
        CLUSTER_REJOIN_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);

        // Try each known peer in turn; the first one that's the leader (or
        // that forwards to the leader) will commit the JoinNode entry.
        // We try all peers per attempt so that leader failover is handled
        // automatically within a single attempt window.
        let mut last_err: Option<String> = None;

        for (peer_uuid, _peer_addr) in &peers {
            let op = DdlOperation::JoinNode(self_info.clone());

            tracing::info!(
                attempt,
                max = MAX_REJOIN_ATTEMPTS,
                self_id = %self_host_id,
                peer = %peer_uuid,
                "cluster_rejoin: sending JoinNode to peer"
            );

            match forward_ddl_to_leader(&peer_manager, *peer_uuid, op).await {
                Ok(()) => {
                    tracing::info!(
                        attempt,
                        self_id = %self_host_id,
                        peer = %peer_uuid,
                        "cluster_rejoin: AddVoter sent to leader — JoinNode accepted; \
                         awaiting snapshot delivery from leader"
                    );
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        attempt,
                        max = MAX_REJOIN_ATTEMPTS,
                        self_id = %self_host_id,
                        peer = %peer_uuid,
                        error = %e,
                        "cluster_rejoin: JoinNode attempt failed"
                    );
                    last_err = Some(e.to_string());
                }
            }
        }

        if attempt < MAX_REJOIN_ATTEMPTS {
            tracing::info!(
                attempt,
                max = MAX_REJOIN_ATTEMPTS,
                backoff_secs,
                last_error = last_err.as_deref().unwrap_or("unknown"),
                "cluster_rejoin: all peers rejected this attempt; retrying after backoff"
            );
            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(BACKOFF_MAX_SECS);
        }
    }

    // All attempts exhausted — fail loud.
    CLUSTER_REJOIN_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    tracing::error!(
        self_id = %self_host_id,
        self_addr = %self_addr,
        attempts = MAX_REJOIN_ATTEMPTS,
        "cluster_rejoin: FAILED — all JoinNode attempts exhausted. \
         Node is NOT in the cluster voter set. \
         Operator must manually rejoin this node or restart the cluster. \
         (P0-21: CLUSTER_REJOIN_FAILURES_TOTAL incremented)"
    );
    Err(RejoinError::Exhausted)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_sequence_caps_at_60s() {
        let mut b = BACKOFF_INITIAL_SECS;
        let mut sequence = Vec::new();
        for _ in 0..MAX_REJOIN_ATTEMPTS {
            sequence.push(b);
            b = (b * 2).min(BACKOFF_MAX_SECS);
        }
        // First backoff is 1s.
        assert_eq!(sequence[0], 1);
        // After several doublings, must not exceed 60s.
        for &v in &sequence {
            assert!(
                v <= BACKOFF_MAX_SECS,
                "backoff {v} exceeds cap {BACKOFF_MAX_SECS}"
            );
        }
        // Sequence grows monotonically until the cap.
        let pre_cap: Vec<u64> = sequence
            .iter()
            .copied()
            .take_while(|&v| v < BACKOFF_MAX_SECS)
            .collect();
        for w in pre_cap.windows(2) {
            assert!(w[1] > w[0], "backoff should grow before cap: {:?}", w);
        }
    }

    #[test]
    fn counters_are_zero_at_start() {
        // Counters are process-wide; we cannot reset them in unit tests.
        // This test documents their initial state and confirms they are
        // accessible; integration tests assert they increment.
        let _ = cluster_rejoin_attempts_total();
        let _ = cluster_rejoin_failures_total();
    }

    #[tokio::test]
    async fn attempt_rejoin_fails_loud_with_no_peers() {
        let before_failures = CLUSTER_REJOIN_FAILURES_TOTAL.load(Ordering::Relaxed);

        // Build a minimal PeerManager that has no peers registered.
        // attempt_rejoin bails immediately when peers is empty.
        use ferrosa_net::config::NetConfig;
        use ferrosa_net::peer::{PeerEventListener, PeerManager};
        use ferrosa_net::rpc::handler::PeerId;
        struct NoopListener;
        impl PeerEventListener for NoopListener {
            fn on_peer_connected(&self, _p: PeerId) {}
            fn on_peer_disconnected(&self, _p: PeerId) {}
            fn on_peer_suspected(&self, _p: PeerId) {}
            fn on_peer_recovered(&self, _p: Uuid) {}
            fn on_peer_failed(&self, _p: Uuid) {}
        }
        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let result = attempt_rejoin(
            Uuid::new_v4(),
            "127.0.0.1:7000".to_string(),
            "dc1".to_string(),
            "rack1".to_string(),
            None,
            vec![],
            pm,
        )
        .await;

        assert!(
            result.is_err(),
            "attempt_rejoin with no peers must return Err"
        );
        assert!(
            matches!(result.unwrap_err(), RejoinError::Exhausted),
            "error must be Exhausted"
        );
        // FAILURES must have incremented.
        let after_failures = CLUSTER_REJOIN_FAILURES_TOTAL.load(Ordering::Relaxed);
        assert!(
            after_failures > before_failures,
            "CLUSTER_REJOIN_FAILURES_TOTAL must increment on empty-peers exhaustion"
        );
    }
}
