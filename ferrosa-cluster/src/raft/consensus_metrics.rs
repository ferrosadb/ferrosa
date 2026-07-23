//! Module: Export Raft consensus liveness (term, leadership, election-storm
//!   counter) as Prometheus metrics.
//! Correctness: Correct when `render_prometheus` emits the four series in
//!   text-exposition format, and `run_consensus_metrics_poller` publishes the
//!   term/leadership derived from `raft.current_leader()` (the same source
//!   `/readyz` trusts), not the raw `metrics().state` snapshot.
//! Last revised: 2026-07-22
//! Last changed: Re-added the current-term / is-leader / has-leader gauges,
//!   now fed by a DEDICATED poller (`run_consensus_metrics_poller`) that derives
//!   leadership from `current_leader()`. The earlier attempt published from the
//!   election-guard poll using `state == Leader`, which read 0 even for a ready
//!   leader (t_310ad227). The poller is its own task — not the guard — so the
//!   metric surface does not depend on the ADR-012-deprecated guard.
//!
//! # Why `current_leader()` not `metrics().state`
//!
//! `/readyz` gates on `raft.current_leader().await.is_some()` and is reliable.
//! The raw `RaftMetrics.state`/`current_term` snapshot did not reflect
//! leadership in ferrosa's formation path (is-leader read 0 on the leader), so
//! the poller compares `current_leader()` against this node's own id
//! (`metrics().id`) instead.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use openraft::{Raft, RaftTypeConfig};
use tokio_util::sync::CancellationToken;

/// Latest Raft `current_term` observed by the poller (0 before the first poll).
static RAFT_CURRENT_TERM: AtomicU64 = AtomicU64::new(0);
/// `1` if this node was the Raft leader at the last poll, else `0`.
static RAFT_IS_LEADER: AtomicU64 = AtomicU64::new(0);
/// `1` if a Raft leader was known cluster-wide at the last poll, else `0`.
static RAFT_HAS_LEADER: AtomicU64 = AtomicU64::new(0);

/// How often the poller samples leadership.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Publish the latest Raft liveness sample. Cheap; `Relaxed` because each field
/// is an independent last-writer-wins sample.
pub fn record_raft_state(current_term: u64, is_leader: bool, has_leader: bool) {
    RAFT_CURRENT_TERM.store(current_term, Ordering::Relaxed);
    RAFT_IS_LEADER.store(u64::from(is_leader), Ordering::Relaxed);
    RAFT_HAS_LEADER.store(u64::from(has_leader), Ordering::Relaxed);
}

/// Latest observed Raft `current_term` (0 before the first poll).
pub fn raft_current_term() -> u64 {
    RAFT_CURRENT_TERM.load(Ordering::Relaxed)
}

/// Whether this node was the Raft leader at the last poll.
pub fn raft_is_leader() -> bool {
    RAFT_IS_LEADER.load(Ordering::Relaxed) != 0
}

/// Whether a Raft leader was known at the last poll.
pub fn raft_has_leader() -> bool {
    RAFT_HAS_LEADER.load(Ordering::Relaxed) != 0
}

/// Pure leadership decision: this node leads iff `current_leader()` names it.
/// Separated so the mapping is unit-testable without a live Raft.
pub fn is_self_leader(current_leader: Option<u64>, my_id: u64) -> bool {
    current_leader == Some(my_id)
}

/// Poll Raft leadership once per second (`POLL_INTERVAL`) and publish it, until
/// cancelled. Spawned per cluster-mode node alongside — but independent of — the
/// election guard, so the metric surface outlives the ADR-012 guard.
///
/// Leadership comes from `current_leader()` (the reliable `/readyz` source);
/// the term comes from the metrics snapshot.
pub async fn run_consensus_metrics_poller<C>(raft: Arc<Raft<C>>, cancel: CancellationToken)
where
    C: RaftTypeConfig<NodeId = u64>,
{
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }
        let leader = raft.current_leader().await;
        let metrics = raft.metrics().borrow().clone();
        let is_leader = is_self_leader(leader, metrics.id);
        record_raft_state(metrics.current_term, is_leader, leader.is_some());
    }
}

/// Render the Raft consensus metrics in Prometheus text-exposition format.
///
/// The storm counter is read through
/// [`super::election_guard::election_storm_term_jumps_total`]; the term/leader
/// gauges reflect the last [`run_consensus_metrics_poller`] sample.
pub fn render_prometheus() -> String {
    format_metrics(
        raft_current_term(),
        raft_is_leader(),
        raft_has_leader(),
        super::election_guard::election_storm_term_jumps_total(),
    )
}

/// Pure formatter — separated from the atomics so it can be unit-tested against
/// exact output without racing the process globals.
fn format_metrics(
    current_term: u64,
    is_leader: bool,
    has_leader: bool,
    storm_jumps: u64,
) -> String {
    let leader = u64::from(is_leader);
    let has = u64::from(has_leader);
    format!(
        "# HELP ferrosa_raft_current_term Latest observed Raft current_term on this node.\n\
         # TYPE ferrosa_raft_current_term gauge\n\
         ferrosa_raft_current_term {current_term}\n\
         # HELP ferrosa_raft_is_leader 1 if this node was the Raft leader at the last poll, else 0.\n\
         # TYPE ferrosa_raft_is_leader gauge\n\
         ferrosa_raft_is_leader {leader}\n\
         # HELP ferrosa_raft_has_leader 1 if a Raft leader was known at the last poll, else 0.\n\
         # TYPE ferrosa_raft_has_leader gauge\n\
         ferrosa_raft_has_leader {has}\n\
         # HELP ferrosa_raft_election_storm_term_jumps_total Election-storm detections (P0-17/P0-19 watchdog); non-zero should alert.\n\
         # TYPE ferrosa_raft_election_storm_term_jumps_total counter\n\
         ferrosa_raft_election_storm_term_jumps_total {storm_jumps}\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_self_leader_matches_only_own_id() {
        assert!(is_self_leader(Some(7), 7));
        assert!(!is_self_leader(Some(7), 9));
        assert!(!is_self_leader(None, 7));
    }

    #[test]
    fn format_metrics_emits_all_series() {
        let out = format_metrics(5, true, true, 0);
        assert!(out.contains("\nferrosa_raft_current_term 5\n"), "{out}");
        assert!(out.contains("\nferrosa_raft_is_leader 1\n"), "{out}");
        assert!(out.contains("\nferrosa_raft_has_leader 1\n"), "{out}");
        assert!(
            out.contains("\nferrosa_raft_election_storm_term_jumps_total 0\n"),
            "{out}"
        );
        assert!(out.contains("# TYPE ferrosa_raft_is_leader gauge"), "{out}");
    }

    #[test]
    fn format_metrics_follower_with_known_leader() {
        // A follower: is_leader 0 but has_leader 1.
        let out = format_metrics(3, false, true, 2);
        assert!(out.contains("\nferrosa_raft_is_leader 0\n"), "{out}");
        assert!(out.contains("\nferrosa_raft_has_leader 1\n"), "{out}");
        assert!(
            out.contains("\nferrosa_raft_election_storm_term_jumps_total 2\n"),
            "{out}"
        );
    }

    #[test]
    fn record_and_render_round_trip() {
        // Only test that writes the process globals — no sibling races it.
        record_raft_state(42, true, true);
        assert_eq!(raft_current_term(), 42);
        assert!(raft_is_leader());
        assert!(raft_has_leader());
        let out = render_prometheus();
        assert!(out.contains("\nferrosa_raft_current_term 42\n"), "{out}");
        assert!(out.contains("\nferrosa_raft_is_leader 1\n"), "{out}");
    }
}
