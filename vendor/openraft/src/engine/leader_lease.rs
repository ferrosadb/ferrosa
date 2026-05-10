//! LeaderLease — pure-function module for lease-aware vote/pre-vote rejection
//! (Ongaro §9.6 caveat). Ferrosa fork extension per ADR-012 (W3.2, W3.7).
//!
//! A follower that has heard from its current leader within the last election
//! timeout window must reject (Pre)VoteRequests from other candidates. This
//! prevents a partitioned candidate that has run up its term from disrupting
//! a healthy quorum on partition-heal — which is the runaway-term failure mode
//! documented in `bug-raft-stale-candidate-runaway-term-no-prevote.md`.
//!
//! The lease is "soft" in the etcd sense: the follower simply remembers the
//! last time it heard from the current leader. There is no explicit lease
//! grant; the leader does not need to renew anything. The follower just
//! checks "did I hear from the leader within `election_timeout`?" before
//! granting any (Pre)Vote.
//!
//! When a leader voluntarily steps down via CheckQuorum, it must surrender
//! its lease so followers can immediately grant pre-votes to a new candidate.
//! In ferrosa's wire-up that is `LeaderLease::invalidate()`, called from the
//! `leader_step_down` flow.

use std::time::Duration;

/// Decision returned by [`LeaderLease::is_active`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseStatus {
    /// Lease is active — reject (Pre)Vote requests from other candidates.
    Active {
        /// How long until the lease expires.
        remaining: Duration,
    },
    /// Lease has expired or never existed — granting follows the standard
    /// election-restriction predicate.
    Expired,
    /// Lease was explicitly invalidated (e.g., by CheckQuorum stepdown).
    Invalidated,
}

impl LeaseStatus {
    /// Whether the lease is currently active and (Pre)Votes must be rejected
    /// solely on the lease check.
    pub(crate) fn is_active(&self) -> bool {
        matches!(self, LeaseStatus::Active { .. })
    }
}

/// Pure-function predicate for the lease-aware (Pre)Vote rejection check.
///
/// Inputs are the timing facts and the election-timeout config; output is a
/// `LeaseStatus`. The wire-up into `following_handler::handle_pre_vote_req`
/// (and eventually `handle_vote_req`) calls this with `elapsed = now -
/// last_heard_from_leader`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LeaderLease {
    pub(crate) election_timeout_ms: u64,
    pub(crate) invalidated: bool,
}

impl LeaderLease {
    /// Evaluate whether the lease is currently active.
    ///
    /// `elapsed_since_last_leader_msg`: time since this voter last heard
    /// from the current leader (any AppendEntries / heartbeat / install
    /// snapshot RPC counts). `None` means "never" — lease is expired.
    pub(crate) fn is_active(&self, elapsed_since_last_leader_msg: Option<Duration>) -> LeaseStatus {
        if self.invalidated {
            return LeaseStatus::Invalidated;
        }
        let timeout = Duration::from_millis(self.election_timeout_ms);
        match elapsed_since_last_leader_msg {
            None => LeaseStatus::Expired,
            Some(elapsed) if elapsed >= timeout => LeaseStatus::Expired,
            Some(elapsed) => LeaseStatus::Active {
                remaining: timeout - elapsed,
            },
        }
    }

    /// Invalidate the lease (e.g., on CheckQuorum stepdown — ADR-012 W3.7).
    pub(crate) fn invalidate(&mut self) {
        self.invalidated = true;
    }

    /// Re-arm the lease (called when this voter receives a fresh
    /// AppendEntries/heartbeat from the current leader).
    pub(crate) fn rearm(&mut self) {
        self.invalidated = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease() -> LeaderLease {
        LeaderLease {
            election_timeout_ms: 1000,
            invalidated: false,
        }
    }

    #[test]
    fn lease_active_within_window() {
        let status = lease().is_active(Some(Duration::from_millis(500)));
        assert!(matches!(status, LeaseStatus::Active { .. }));
        assert!(status.is_active());
    }

    #[test]
    fn lease_expired_at_exact_timeout() {
        let status = lease().is_active(Some(Duration::from_millis(1000)));
        assert_eq!(status, LeaseStatus::Expired);
        assert!(!status.is_active());
    }

    #[test]
    fn lease_expired_past_timeout() {
        let status = lease().is_active(Some(Duration::from_millis(1500)));
        assert_eq!(status, LeaseStatus::Expired);
    }

    #[test]
    fn lease_expired_when_never_heard() {
        let status = lease().is_active(None);
        assert_eq!(status, LeaseStatus::Expired);
    }

    #[test]
    fn lease_invalidated_supersedes_window() {
        let mut l = lease();
        l.invalidate();
        // Even if elapsed is well within the window, an explicit invalidation
        // wins. This is W3.7 — CheckQuorum stepdown surrenders the lease so
        // PreVote on the new candidate can succeed immediately.
        let status = l.is_active(Some(Duration::from_millis(100)));
        assert_eq!(status, LeaseStatus::Invalidated);
        assert!(!status.is_active());
    }

    #[test]
    fn lease_rearm_reactivates_after_invalidation() {
        let mut l = lease();
        l.invalidate();
        l.rearm();
        let status = l.is_active(Some(Duration::from_millis(100)));
        assert!(matches!(status, LeaseStatus::Active { .. }));
    }

    #[test]
    fn lease_remaining_is_correct() {
        let l = LeaderLease {
            election_timeout_ms: 1000,
            invalidated: false,
        };
        match l.is_active(Some(Duration::from_millis(300))) {
            LeaseStatus::Active { remaining } => {
                assert_eq!(remaining, Duration::from_millis(700));
            }
            other => panic!("expected Active, got {:?}", other),
        }
    }
}
