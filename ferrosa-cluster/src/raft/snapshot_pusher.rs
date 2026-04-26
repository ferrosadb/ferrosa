//! Leader-side snapshot-push sweep — P0-20 fix.
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
pub async fn run_snapshot_pusher<C>(
    raft: Arc<Raft<C>>,
    cancel: tokio_util::sync::CancellationToken,
    sweep_interval_ms: u64,
    lag_threshold: u64,
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

        // Identify peers whose matched log is far behind the committed index.
        //
        // `matched = None` means the leader has not yet confirmed any log
        // entry from that peer — it is maximally lagging.
        let mut lagging_count = 0u64;
        for (peer_id, matched) in &replication {
            let peer_index = matched.as_ref().map(|lid| lid.index).unwrap_or(0);
            let lag = leader_committed.saturating_sub(peer_index);
            if lag > lag_threshold {
                lagging_count += 1;
                tracing::debug!(
                    peer_id,
                    peer_index,
                    leader_committed,
                    lag,
                    "snapshot_pusher: peer is lagging, will trigger snapshot + heartbeat"
                );
            }
        }

        // Also check for membership members absent from the replication map.
        // This can happen if openraft's vote-handler short-circuited the peer
        // before its replication stream was registered (P0-20 root cause).
        let membership_voters: Vec<u64> =
            metrics.membership_config.membership().voter_ids().collect();
        let local_id = metrics.id;
        for voter_id in &membership_voters {
            if *voter_id == local_id {
                continue; // skip self
            }
            if !replication.contains_key(voter_id) {
                lagging_count += 1;
                tracing::warn!(
                    peer_id = voter_id,
                    leader_committed,
                    "snapshot_pusher: member absent from replication map — \
                     triggering snapshot + heartbeat (P0-20 path)"
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

        // Trigger a heartbeat to wake up all replication streams and push
        // the snapshot to lagging peers.
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
            leader_committed,
            INSTALLSNAPSHOT_PUSHES_TOTAL = INSTALLSNAPSHOT_PUSHES_TOTAL.load(Ordering::Relaxed),
            "snapshot_pusher: triggered snapshot + heartbeat for {} lagging peer(s) (P0-20)",
            lagging_count,
        );
    }
}
