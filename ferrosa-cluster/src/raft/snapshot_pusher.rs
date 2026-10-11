//! Leader-side snapshot-push sweep — P0-20 fix.
//!
//! # Deprecation status (Sprint 4)
//!
//! Per ADR-012 and ADR-013, this module is scheduled for deletion in
//! W4.12.  Sprint 1's `MembershipChanger` ensures every voter is
//! registered atomically (eliminating the original P0-20 motivation:
//! "voter not in replication map"); Sprint 3's PreVote+CheckQuorum
//! makes the wiped-rebootstrap path (S-04) handled by openraft's
//! normal snapshot-on-log-inconsistency response.  Deletion is
//! gated by [`crate::controller::bootstrap::retirement_gate`] (a
//! 2-week clean Jepsen window).  **Do not add new dependencies on it.**
//!
//! ## Problem
//!
//! When a Raft follower is wiped (persistent state cleared) and restarted, its
//! term climbs unbounded while its log stays empty.  The election-storm watchdog
//! (P0-17/P0-19) suppresses the follower's elections to give the cluster leader
//! time to push an `InstallSnapshot`.  However, the leader may not proactively
//! attempt replication during the suppression window because:
//!
//!   1. The leader's replication stream to the wiped node hit a series of
//!      `HigherVote` responses (the wiped node's inflated term caused rejection)
//!      and the stream is in backoff.
//!   2. The snapshot has not been recently built, so there is nothing ready to
//!      send.
//!   3. The leader does not know the wiped node is now suppressed and therefore
//!      receptive.
//!
//! ## Fix
//!
//! A background task runs on each node, gating on `state == Leader`.  Every
//! `sweep_interval_ms` it inspects `raft.metrics().replication` to find peers
//! whose `matched` log index is more than `lag_threshold` entries behind the
//! leader's committed log index.  For those peers it:
//!
//!   1. Calls `raft.trigger().snapshot()` — ensures the state machine has a
//!      snapshot ready that covers the current committed state.
//!   2. Calls `raft.trigger().heartbeat()` — kicks the replication loop so it
//!      immediately tries to push the snapshot to all lagging peers (including
//!      the wiped node, once its term has stabilized below the leader's term).
//!   3. Increments `INSTALLSNAPSHOT_PUSHES_TOTAL` for operator visibility.
//!
//! ## Honest limitations
//!
//! The pusher cannot force a snapshot delivery if the leader's term is still
//! lower than the wiped node's term.  In that case every AppendEntries RPC is
//! rejected with `HigherVote`, causing the leader to step down.  A new leader
//! is elected at `follower_term + 1`, and THAT leader can replicate.  The
//! pusher fires again on the new leader, ensuring the snapshot is built and
//! the heartbeat is sent promptly.
//!
//! The combined effect: once the election guard has frozen the wiped node's
//! term and the cluster has settled on a leader whose term > frozen term, the
//! pusher drives convergence within one `sweep_interval_ms` cycle rather than
//! waiting for the next natural heartbeat.
//!
//! ## Metric
//!
//! `INSTALLSNAPSHOT_PUSHES_TOTAL` — a process-wide `AtomicU64` that increments
//! each time the sweeper fires a `trigger().snapshot()` + `trigger().heartbeat()`
//! pair for a lagging peer.  This counter is the observable signal that the
//! pusher is doing work; operators alert when it increments during a convergence
//! event.
//!
//! ## Related
//!
//! - `election_guard.rs` — P0-17/P0-19 — detects and suppresses election storms
//! - P0-20 spec: `specs/todo/p0-20-leader-no-installsnapshot-to-stale-candidate.md`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use openraft::{Raft, RaftTypeConfig, ServerState};

// ---------------------------------------------------------------------------
// Metric counter
// ---------------------------------------------------------------------------

/// Process-wide count of snapshot-push sweep firings.
///
/// Increments once per sweep cycle that detects at least one lagging peer and
/// calls `trigger().snapshot()` + `trigger().heartbeat()`.  A non-zero value
/// during a node-rejoin event confirms the pusher is driving convergence.
pub static INSTALLSNAPSHOT_PUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Return the current value of the snapshot-push counter.
///
/// Thread-safe; uses `Relaxed` ordering (monotonically increasing, no ordering
/// guarantee needed).
pub fn installsnapshot_pushes_total() -> u64 {
    INSTALLSNAPSHOT_PUSHES_TOTAL.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Per-peer push policy
// ---------------------------------------------------------------------------

/// Decides, per lagging peer, whether this sweep should push a snapshot.
///
/// The sweep's own condition ("this peer is lagging") is not by itself a reason
/// to push again. A peer can be lagging for a reason a heartbeat cannot fix,
/// and then every sweep pushes it another snapshot. Observed live on the dev
/// cluster: 39,167 `begin_receiving_snapshot` cycles on one follower against
/// 36,269 pushes on the leader, the follower answering `No need to install
/// snapshot` every time — the follower never catches up and the pair burns
/// CPU, disk and log for as long as the peer stays slow.
///
/// A peer that has advanced since its last push is pushed again at once; a peer
/// that has not advanced is pushed only after an exponentially growing backoff
/// (capped), so a stuck peer costs one push per backoff window instead of one
/// per sweep. Progress resets the backoff.
#[derive(Debug)]
pub struct LaggingPeerPolicy {
    lag_threshold: u64,
    min_backoff: Duration,
    max_backoff: Duration,
    peers: std::collections::HashMap<u64, PeerPushState>,
}

#[derive(Debug, Clone, Copy)]
struct PeerPushState {
    /// The peer's matched index at the last push. A peer that has moved past
    /// this since is making progress and is pushed again immediately.
    last_pushed_at_index: u64,
    last_push_at: tokio::time::Instant,
    /// Wait before pushing this peer again if it has not advanced.
    backoff: Duration,
}

impl LaggingPeerPolicy {
    pub fn new(lag_threshold: u64, min_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            lag_threshold,
            min_backoff,
            max_backoff,
            peers: std::collections::HashMap::new(),
        }
    }

    /// Whether `peer`, currently at `peer_index`, is due a snapshot+heartbeat.
    ///
    /// A peer within `lag_threshold` of the leader's committed index is never
    /// due, and is forgotten so that a later regression pushes at once instead
    /// of waiting out a backoff earned before it had caught up.
    pub fn should_push(
        &mut self,
        peer: u64,
        peer_index: u64,
        leader_committed: u64,
        now: tokio::time::Instant,
    ) -> bool {
        if leader_committed.saturating_sub(peer_index) <= self.lag_threshold {
            self.peers.remove(&peer);
            return false;
        }
        match self.peers.get_mut(&peer) {
            None => {
                self.peers.insert(
                    peer,
                    PeerPushState {
                        last_pushed_at_index: peer_index,
                        last_push_at: now,
                        backoff: self.min_backoff,
                    },
                );
                true
            }
            Some(state) => {
                let advanced = peer_index > state.last_pushed_at_index;
                if !advanced && now.duration_since(state.last_push_at) < state.backoff {
                    return false;
                }
                state.backoff = if advanced {
                    self.min_backoff
                } else {
                    (state.backoff * 2).min(self.max_backoff)
                };
                state.last_pushed_at_index = peer_index;
                state.last_push_at = now;
                true
            }
        }
    }
}

#[cfg(test)]
mod lagging_peer_policy_tests {
    use super::*;

    const MIN: Duration = Duration::from_secs(8);
    const MAX: Duration = Duration::from_secs(64);
    const THRESHOLD: u64 = 10;

    fn policy() -> LaggingPeerPolicy {
        LaggingPeerPolicy::new(THRESHOLD, MIN, MAX)
    }

    /// The sweep fires every second. A peer frozen at one index while the leader
    /// commits far ahead must NOT be pushed on every sweep — that is the live
    /// 39,167-cycle livelock. Pushes must follow the backoff, not the sweep.
    #[test]
    fn a_peer_that_never_advances_is_not_pushed_every_sweep() {
        let mut p = policy();
        let t0 = tokio::time::Instant::now();
        let mut pushes = 0;
        for i in 0..50u64 {
            if p.should_push(7, 100, 1000, t0 + Duration::from_secs(i)) {
                pushes += 1;
            }
        }
        assert!(
            pushes <= 7,
            "a stuck peer must be pushed on the backoff schedule, not every sweep: \
             {pushes} pushes in 50 sweeps"
        );
    }

    /// A peer that IS advancing must still be pushed immediately — no suppression.
    #[test]
    fn a_peer_that_advances_is_pushed_promptly() {
        let mut p = policy();
        let t0 = tokio::time::Instant::now();
        let mut index = 100u64;
        for i in 0..10u64 {
            assert!(
                p.should_push(7, index, 1000, t0 + Duration::from_secs(i)),
                "an advancing peer must be pushed on sweep {i}"
            );
            index += 50;
        }
    }

    /// Within the threshold no push is due, and the peer is forgotten so a later
    /// regression pushes at once instead of waiting out a backoff it earned
    /// before it had caught up.
    #[test]
    fn a_peer_within_threshold_is_never_pushed_and_is_forgotten() {
        let mut p = policy();
        let t0 = tokio::time::Instant::now();
        assert!(
            p.should_push(7, 100, 1000, t0),
            "first sight of a lagging peer pushes"
        );
        assert!(
            !p.should_push(7, 995, 1000, t0 + Duration::from_secs(1)),
            "caught up: no push"
        );
        assert!(
            p.should_push(7, 100, 1000, t0 + Duration::from_secs(2)),
            "re-lagging pushes at once"
        );
    }

    /// The backoff is capped: a long-stuck peer keeps being retried, just rarely.
    #[test]
    fn the_backoff_is_capped_so_a_stuck_peer_is_still_retried() {
        let mut p = policy();
        let t0 = tokio::time::Instant::now();
        let mut pushes = 0;
        for i in 0..600u64 {
            if p.should_push(7, 100, 1_000_000, t0 + Duration::from_secs(i)) {
                pushes += 1;
            }
        }
        assert!(
            pushes >= 5,
            "a stuck peer must still be retried, got {pushes} pushes in 600s"
        );
        assert!(
            pushes <= 90,
            "retries must stay capped by max_backoff, got {pushes} in 600s"
        );
    }
}

// ---------------------------------------------------------------------------
// Snapshot pusher task
// ---------------------------------------------------------------------------

/// Spawn the leader-side snapshot-push sweep for a Raft node.
///
/// The task runs indefinitely (or until `cancel` is triggered).  It only
/// performs work when the local node is in `Leader` state.  On each sweep
/// cycle (`sweep_interval_ms`) it checks for followers whose matched log
/// index is more than `lag_threshold` entries behind the leader's committed
/// log and, if any are found, triggers a snapshot build + heartbeat to kick
/// the replication loop.
///
/// # Parameters
///
/// - `raft` — the local Raft handle.
/// - `cancel` — cancellation token; the task exits cleanly when cancelled.
/// - `sweep_interval_ms` — how often to poll (default: 5 000 ms).
/// - `lag_threshold` — minimum entry lag before a push is triggered
///   (default: 10).  Set to 1 to trigger on any mismatch.
/// - `health` — receives every follower's lag on each leader sweep (and is
///   cleared while this node is not the leader), so metrics and readiness can
///   name a follower that has stopped catching up (outage 2026-10-10, D4).
pub async fn run_snapshot_pusher<C>(
    raft: Arc<Raft<C>>,
    cancel: tokio_util::sync::CancellationToken,
    sweep_interval_ms: u64,
    lag_threshold: u64,
    health: Arc<super::replication_health::ReplicationHealth>,
) where
    C: RaftTypeConfig<NodeId = u64>,
{
    let interval = Duration::from_millis(sweep_interval_ms);

    // Track the last committed index at which we triggered a snapshot build.
    // Only retrigger if the committed index has advanced (new entries applied)
    // or if enough time has passed (3× the sweep interval as a safety cooldown).
    let mut last_snapshot_trigger_committed: u64 = 0;
    let snapshot_cooldown = interval * 3;
    // Initialize in the past so the first cycle can trigger a snapshot
    // if needed (without waiting for the cooldown to expire).
    let mut last_snapshot_trigger_at =
        tokio::time::Instant::now() - snapshot_cooldown - Duration::from_millis(1);

    // One push per peer per backoff window, reset when the peer makes progress.
    // Without this a peer that lags for a reason a heartbeat cannot fix is
    // pushed a fresh snapshot on every sweep and can never catch up.
    let mut policy = LaggingPeerPolicy::new(lag_threshold, interval * 2, interval * 12);

    tracing::debug!(sweep_interval_ms, lag_threshold, "snapshot_pusher: started");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("snapshot_pusher: cancelled, exiting");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }

        // Only act when this node is the leader.
        let metrics = raft.metrics().borrow().clone();
        if metrics.state != ServerState::Leader {
            tracing::trace!(
                state = ?metrics.state,
                "snapshot_pusher: not leader, skipping"
            );
            // Lag is a leader's view; a stale one would keep naming peers
            // after leadership moved.
            health.clear();
            continue;
        }

        // Use last_log_index as the reference for lag calculation.
        // last_applied may lag behind last_log_index if the state machine
        // hasn't caught up, so use the more authoritative committed/log metric.
        let leader_committed = metrics
            .last_log_index
            .unwrap_or_else(|| metrics.last_applied.map(|l| l.index).unwrap_or(0));

        if leader_committed == 0 {
            // Leader has no committed entries yet — nothing to push.
            continue;
        }

        // `replication` is `Some(BTreeMap<NodeId, Option<LogId>>)` only when Leader.
        let replication = match metrics.replication {
            Some(r) => r,
            None => continue,
        };

        // Every follower's matched index, including voters missing from the
        // replication map (matched 0), feeds the per-peer lag metrics and the
        // stalled-follower readiness check.
        let local_id = metrics.id;
        let mut sweep: Vec<(u64, u64)> = replication
            .iter()
            .map(|(peer, matched)| (*peer, matched.as_ref().map(|lid| lid.index).unwrap_or(0)))
            .collect();
        sweep.extend(
            metrics
                .membership_config
                .membership()
                .voter_ids()
                .filter(|id| *id != local_id && !replication.contains_key(id))
                .map(|id| (id, 0)),
        );
        health.observe_sweep(leader_committed, lag_threshold, &sweep);

        // A peer is a *candidate* while it lags by more than `lag_threshold`,
        // but being a candidate is not by itself a reason to push again this
        // sweep: `policy` grants one push per peer and requires either progress
        // or a lapsed backoff before granting another.
        let sweep_now = tokio::time::Instant::now();
        let mut lagging_count = 0u64;
        let mut due_count = 0u64;
        // `matched = None` means the leader has not yet confirmed any log
        // entry from that peer — it is maximally lagging.
        for (peer_id, matched) in &replication {
            let peer_index = matched.as_ref().map(|lid| lid.index).unwrap_or(0);
            let lag = leader_committed.saturating_sub(peer_index);
            if lag > lag_threshold {
                lagging_count += 1;
                if policy.should_push(*peer_id, peer_index, leader_committed, sweep_now) {
                    due_count += 1;
                }
                tracing::debug!(
                    peer_id,
                    peer_index,
                    leader_committed,
                    lag,
                    "snapshot_pusher: peer is lagging"
                );
            }
        }

        // Also check for membership members absent from the replication map.
        // This can happen if openraft's vote-handler short-circuited the peer
        // before its replication stream was registered (P0-20 root cause).
        let membership_voters: Vec<u64> =
            metrics.membership_config.membership().voter_ids().collect();
        for voter_id in &membership_voters {
            if *voter_id == local_id {
                continue; // skip self
            }
            if !replication.contains_key(voter_id) {
                lagging_count += 1;
                if policy.should_push(*voter_id, 0, leader_committed, sweep_now) {
                    due_count += 1;
                }
                tracing::warn!(
                    peer_id = voter_id,
                    leader_committed,
                    "snapshot_pusher: member absent from replication map (P0-20 path)"
                );
            }
        }

        if lagging_count == 0 {
            continue;
        }

        // Trigger a snapshot build on the state machine, only if needed.
        //
        // Skip if the current snapshot already covers the committed log
        // (metrics.snapshot.index >= leader_committed).  Calling
        // trigger().snapshot() when the snapshot is current causes an
        // assertion failure in openraft's state machine worker
        // ("snapshot log id should be monotonically increasing").
        //
        // Also apply a per-cycle cooldown: don't retrigger more frequently
        // than 3× the sweep interval even if the committed index advances,
        // to avoid flooding the state machine with concurrent snapshot builds.
        let current_snapshot_index = metrics.snapshot.map(|s| s.index).unwrap_or(0);
        let snapshot_needed = current_snapshot_index < leader_committed;
        let now = tokio::time::Instant::now();
        let cooldown_elapsed = now.duration_since(last_snapshot_trigger_at) >= snapshot_cooldown;

        if snapshot_needed
            && (leader_committed > last_snapshot_trigger_committed || cooldown_elapsed)
        {
            if let Err(e) = raft.trigger().snapshot().await {
                tracing::warn!(
                    error = %e,
                    "snapshot_pusher: trigger().snapshot() failed — Raft may be shutting down"
                );
                return;
            }
            last_snapshot_trigger_committed = leader_committed;
            last_snapshot_trigger_at = now;
        }

        // Wake the replication streams only when a peer is actually due a push.
        // Re-waking on every sweep is what produced the live livelock: a peer
        // lagging for a reason a heartbeat cannot fix received a fresh snapshot
        // every sweep (39,167 cycles on the follower against 36,269 pushes
        // here) and could never catch up.
        if due_count == 0 {
            continue;
        }

        // Trigger a heartbeat to wake up all replication streams and push
        // the snapshot to the peers that are due one.
        if let Err(e) = raft.trigger().heartbeat().await {
            tracing::warn!(
                error = %e,
                "snapshot_pusher: trigger().heartbeat() failed — Raft may be shutting down"
            );
            return;
        }

        INSTALLSNAPSHOT_PUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            lagging_peers = lagging_count,
            due_peers = due_count,
            leader_committed,
            INSTALLSNAPSHOT_PUSHES_TOTAL = INSTALLSNAPSHOT_PUSHES_TOTAL.load(Ordering::Relaxed),
            "snapshot_pusher: triggered snapshot + heartbeat for {} lagging peer(s), {} due (P0-20)",
            lagging_count,
            due_count,
        );
    }
}
