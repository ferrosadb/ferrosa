//! Module: Export Raft consensus liveness as Prometheus metrics.
//! Correctness: Correct when `render_prometheus` emits the current term,
//!   leadership flag, and election-storm counter in text-exposition format, and
//!   `record_raft_state` round-trips the last-observed term/leadership.
//! Last revised: 2026-07-21
//! Last changed: New module — exposes `ferrosa_raft_current_term`,
//!   `ferrosa_raft_is_leader`, and `ferrosa_raft_election_storm_term_jumps_total`
//!   on `/metrics` so the B0 scan-storm regression (t_88223ad0 / T0.6) can assert
//!   "leader term stable, storm-jumps == 0" numerically instead of by log scrape.
//!
//! # Why a dedicated module
//!
//! The election-storm counter has, until now, been an in-process
//! [`AtomicU64`](std::sync::atomic::AtomicU64) with no Prometheus surface, and
//! the Raft term lives only inside openraft's in-process metrics. The P0-17
//! runbook (`CLAUDE.md`) says a non-zero storm counter "should trigger an
//! alert" — impossible without a scrapeable series. This module is that surface.
//!
//! It is intentionally **separate from** [`super::election_guard`]: per ADR-012
//! the guard is scheduled for deletion once the PreVote+CheckQuorum build's
//! retirement gate fires. Keeping the metric surface here means `web/api.rs`
//! depends on this permanent module, not on the guard. The guard is merely one
//! *publisher* — it calls [`record_raft_state`] from its 1 s poll. When the
//! guard is retired, relocate that single publish call to the PreVote build's
//! metrics hook; the metric names and this module stay put.

use std::sync::atomic::{AtomicU64, Ordering};

/// Last Raft `current_term` observed by a publisher (the election-guard poll).
///
/// Zero until the first publish. A gauge, not a counter: it reflects the latest
/// sample, and although Raft terms are monotonic within a cluster, exporting it
/// as a gauge lets a scraper assert "term did not move during the scan storm".
static RAFT_CURRENT_TERM: AtomicU64 = AtomicU64::new(0);

/// `1` if this node was the Raft leader at the last publish, else `0`.
static RAFT_IS_LEADER: AtomicU64 = AtomicU64::new(0);

/// Publish the latest Raft liveness sample.
///
/// Called from whichever task polls `raft.metrics()` (today the election-guard
/// watchdog, once per [`POLL_INTERVAL_MS`](super::election_guard)). Cheap enough
/// to call on every poll; uses `Relaxed` ordering because each field is an
/// independent last-writer-wins sample with no cross-field invariant.
pub fn record_raft_state(current_term: u64, is_leader: bool) {
    RAFT_CURRENT_TERM.store(current_term, Ordering::Relaxed);
    RAFT_IS_LEADER.store(u64::from(is_leader), Ordering::Relaxed);
}

/// Latest observed Raft `current_term` (0 before the first publish).
pub fn raft_current_term() -> u64 {
    RAFT_CURRENT_TERM.load(Ordering::Relaxed)
}

/// Whether this node was the Raft leader at the last publish.
pub fn raft_is_leader() -> bool {
    RAFT_IS_LEADER.load(Ordering::Relaxed) != 0
}

/// Render the Raft consensus metrics in Prometheus text-exposition format.
///
/// Emits three series. The storm counter is read through
/// [`super::election_guard::election_storm_term_jumps_total`] — the guard owns
/// the increment; this module owns the exposition (see the module note on the
/// ADR-012 relocation contract).
pub fn render_prometheus() -> String {
    format_metrics(
        raft_current_term(),
        raft_is_leader(),
        super::election_guard::election_storm_term_jumps_total(),
    )
}

/// Pure formatter for the three consensus series — separated from the atomics so
/// it can be unit-tested against exact output without racing the process globals.
fn format_metrics(current_term: u64, is_leader: bool, storm_jumps: u64) -> String {
    let leader = u64::from(is_leader);
    format!(
        "# HELP ferrosa_raft_current_term Latest observed Raft current_term on this node.\n\
         # TYPE ferrosa_raft_current_term gauge\n\
         ferrosa_raft_current_term {current_term}\n\
         # HELP ferrosa_raft_is_leader 1 if this node was the Raft leader at the last poll, else 0.\n\
         # TYPE ferrosa_raft_is_leader gauge\n\
         ferrosa_raft_is_leader {leader}\n\
         # HELP ferrosa_raft_election_storm_term_jumps_total Election-storm detections (P0-17/P0-19 watchdog); non-zero should alert.\n\
         # TYPE ferrosa_raft_election_storm_term_jumps_total counter\n\
         ferrosa_raft_election_storm_term_jumps_total {storm_jumps}\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_metrics_emits_all_three_series() {
        let out = format_metrics(7, true, 0);
        assert_eq!(
            out,
            "# HELP ferrosa_raft_current_term Latest observed Raft current_term on this node.\n\
             # TYPE ferrosa_raft_current_term gauge\n\
             ferrosa_raft_current_term 7\n\
             # HELP ferrosa_raft_is_leader 1 if this node was the Raft leader at the last poll, else 0.\n\
             # TYPE ferrosa_raft_is_leader gauge\n\
             ferrosa_raft_is_leader 1\n\
             # HELP ferrosa_raft_election_storm_term_jumps_total Election-storm detections (P0-17/P0-19 watchdog); non-zero should alert.\n\
             # TYPE ferrosa_raft_election_storm_term_jumps_total counter\n\
             ferrosa_raft_election_storm_term_jumps_total 0\n"
        );
    }

    #[test]
    fn format_metrics_is_leader_zero_when_follower() {
        let out = format_metrics(3, false, 5);
        assert!(
            out.contains("\nferrosa_raft_is_leader 0\n"),
            "follower must render is_leader 0, got:\n{out}"
        );
        assert!(
            out.contains("\nferrosa_raft_election_storm_term_jumps_total 5\n"),
            "storm counter must pass through, got:\n{out}"
        );
    }

    #[test]
    fn render_prometheus_reflects_recorded_state() {
        // This is the ONLY test that writes the process globals, so it does not
        // race a sibling test. The election guard does not run in unit tests, so
        // nothing else mutates these atomics here.
        record_raft_state(42, true);
        assert_eq!(raft_current_term(), 42);
        assert!(raft_is_leader());
        let out = render_prometheus();
        assert!(out.contains("\nferrosa_raft_current_term 42\n"), "{out}");
        assert!(out.contains("\nferrosa_raft_is_leader 1\n"), "{out}");
    }
}
