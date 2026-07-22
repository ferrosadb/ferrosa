//! Module: Export the Raft election-storm counter as a Prometheus metric.
//! Correctness: Correct when `render_prometheus` emits
//!   `ferrosa_raft_election_storm_term_jumps_total` in text-exposition format
//!   with the live counter value.
//! Last revised: 2026-07-22
//! Last changed: Reduced to the storm counter only. The current-term and
//!   is-leader gauges were removed: their publisher (the election guard's poll)
//!   does not run in every bootstrap path, so they read 0 even for a ready
//!   leader — a misleading metric. Trustworthy term/leadership gauges need a
//!   dedicated `current_leader()` poller and are deferred (t_88223ad0 follow-up).
//!
//! # Why a dedicated module
//!
//! The election-storm counter had no Prometheus surface, yet the P0-17 runbook
//! (`CLAUDE.md`) says a non-zero value "should trigger an alert" — impossible
//! without a scrapeable series. This module is that surface.
//!
//! It is intentionally **separate from** [`super::election_guard`]: per ADR-012
//! the guard is scheduled for deletion once the PreVote+CheckQuorum build's
//! retirement gate fires. Keeping the metric here means `web/api.rs` depends on
//! this permanent module, not on the guard.

/// Render the Raft consensus metrics in Prometheus text-exposition format.
///
/// The storm counter is read through
/// [`super::election_guard::election_storm_term_jumps_total`] — the guard owns
/// the increment; this module owns the exposition.
pub fn render_prometheus() -> String {
    format_metrics(super::election_guard::election_storm_term_jumps_total())
}

/// Pure formatter — separated from the atomic read so it can be unit-tested
/// against exact output.
fn format_metrics(storm_jumps: u64) -> String {
    format!(
        "# HELP ferrosa_raft_election_storm_term_jumps_total Election-storm detections (P0-17/P0-19 watchdog); non-zero should alert.\n\
         # TYPE ferrosa_raft_election_storm_term_jumps_total counter\n\
         ferrosa_raft_election_storm_term_jumps_total {storm_jumps}\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_metrics_emits_storm_counter_series() {
        assert_eq!(
            format_metrics(0),
            "# HELP ferrosa_raft_election_storm_term_jumps_total Election-storm detections (P0-17/P0-19 watchdog); non-zero should alert.\n\
             # TYPE ferrosa_raft_election_storm_term_jumps_total counter\n\
             ferrosa_raft_election_storm_term_jumps_total 0\n"
        );
    }

    #[test]
    fn format_metrics_passes_through_nonzero_count() {
        assert!(format_metrics(7).contains("\nferrosa_raft_election_storm_term_jumps_total 7\n"));
    }

    #[test]
    fn render_prometheus_reads_the_live_counter() {
        // The counter is process-global and monotonic; assert the series is
        // present and numeric rather than a specific value (other tests may have
        // incremented it).
        let out = render_prometheus();
        assert!(out.contains("ferrosa_raft_election_storm_term_jumps_total "));
        assert!(out.contains("# TYPE ferrosa_raft_election_storm_term_jumps_total counter"));
    }
}
