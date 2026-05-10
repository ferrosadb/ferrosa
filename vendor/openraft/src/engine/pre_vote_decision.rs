//! PreVote and Vote decision logic — extracted as a pure-function module so
//! the protocol-level decision is unit-testable in isolation from the engine
//! async state machine.
//!
//! Ferrosa fork extension per ADR-012. W3.1 REFACTOR + W3.2.
//!
//! # Election restriction (Raft §5.4.1)
//!
//! A voter only grants a vote if the candidate's `last_log_id` is at least
//! as up-to-date as the voter's own. "At least as up-to-date" means:
//! - The candidate's last entry has a strictly greater term, OR
//! - The candidate's last entry has the same term and an index `>=` the
//!   voter's.
//!
//! This is the same predicate for both `Vote` (real) and `PreVote` (probe).
//!
//! # Lease-aware rejection (Ongaro §9.6)
//!
//! In addition to the election-restriction check, a follower that has heard
//! from the current leader within `election_timeout` must reject (Pre)Vote
//! requests. See [`crate::engine::leader_lease`].

use crate::engine::leader_lease::LeaseStatus;
use crate::LogId;
use crate::NodeId;

/// Decision returned by [`evaluate_pre_vote`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreVoteDecision {
    /// Grant the pre-vote.
    Grant,
    /// Reject because the lease is still active.
    RejectLeaseActive,
    /// Reject because the candidate's log is not at least as up-to-date.
    RejectStaleLog,
    /// Reject because the candidate's term is below this voter's.
    RejectStaleTerm,
}

impl PreVoteDecision {
    pub(crate) fn is_granted(&self) -> bool {
        matches!(self, PreVoteDecision::Grant)
    }
}

/// Evaluate a `PreVoteRequest` for grant/reject.
///
/// - `candidate_term`: the prospective term in `req.vote.leader_id().get_term()`.
/// - `voter_current_term`: this voter's current term.
/// - `candidate_last_log_id`: from `req.last_log_id`.
/// - `voter_last_log_id`: this voter's last log id.
/// - `lease_status`: result of `LeaderLease::is_active(...)`. If `Active`, the
///   lease check rejects regardless of log freshness.
pub(crate) fn evaluate_pre_vote<NID: NodeId>(
    candidate_term: u64,
    voter_current_term: u64,
    candidate_last_log_id: Option<&LogId<NID>>,
    voter_last_log_id: Option<&LogId<NID>>,
    lease_status: LeaseStatus,
) -> PreVoteDecision {
    // Check 1: candidate must not be on a stale term.
    // PreVote is permissive on equal-term (the candidate hasn't actually
    // incremented its term yet — that's the whole point) but rejects strictly
    // smaller terms.
    if candidate_term < voter_current_term {
        return PreVoteDecision::RejectStaleTerm;
    }

    // Check 2: lease-aware rejection (Ongaro §9.6 caveat).
    if lease_status.is_active() {
        return PreVoteDecision::RejectLeaseActive;
    }

    // Check 3: election restriction (Raft §5.4.1).
    if !is_log_up_to_date(candidate_last_log_id, voter_last_log_id) {
        return PreVoteDecision::RejectStaleLog;
    }

    PreVoteDecision::Grant
}

/// Election-restriction predicate (Raft §5.4.1). Pure function shared between
/// PreVote and real Vote handling — the W3.1 REFACTOR per ADR-012.
///
/// Returns `true` iff the candidate's log is at-least-as-up-to-date as the
/// voter's: candidate's last entry has a strictly greater term, OR same term
/// and index `>=`.
pub(crate) fn is_log_up_to_date<NID: NodeId>(
    candidate_last: Option<&LogId<NID>>,
    voter_last: Option<&LogId<NID>>,
) -> bool {
    match (candidate_last, voter_last) {
        (None, None) => true,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (Some(c), Some(v)) => {
            let c_term = c.leader_id.get_term();
            let v_term = v.leader_id.get_term();
            if c_term != v_term {
                c_term > v_term
            } else {
                c.index >= v.index
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CommittedLeaderId;
    use crate::LogId;

    fn lid(term: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, 0), index)
    }

    // --- is_log_up_to_date -----------------------------------------------------

    #[test]
    fn election_restriction_both_empty_is_up_to_date() {
        assert!(is_log_up_to_date::<u64>(None, None));
    }

    #[test]
    fn election_restriction_candidate_has_log_voter_empty_is_up_to_date() {
        assert!(is_log_up_to_date(Some(&lid(1, 1)), None));
    }

    #[test]
    fn election_restriction_voter_has_log_candidate_empty_is_stale() {
        assert!(!is_log_up_to_date(None, Some(&lid(1, 1))));
    }

    #[test]
    fn election_restriction_higher_term_wins() {
        assert!(is_log_up_to_date(Some(&lid(5, 1)), Some(&lid(4, 100))));
    }

    #[test]
    fn election_restriction_same_term_higher_index_wins() {
        assert!(is_log_up_to_date(Some(&lid(5, 10)), Some(&lid(5, 9))));
        assert!(is_log_up_to_date(Some(&lid(5, 10)), Some(&lid(5, 10))));
    }

    #[test]
    fn election_restriction_same_term_lower_index_loses() {
        assert!(!is_log_up_to_date(Some(&lid(5, 9)), Some(&lid(5, 10))));
    }

    #[test]
    fn election_restriction_lower_term_loses_even_with_high_index() {
        assert!(!is_log_up_to_date(Some(&lid(4, 1000)), Some(&lid(5, 1))));
    }

    // --- evaluate_pre_vote -----------------------------------------------------

    #[test]
    fn pre_vote_granted_when_log_up_to_date_and_lease_expired() {
        let d = evaluate_pre_vote::<u64>(
            5,
            5,
            Some(&lid(5, 10)),
            Some(&lid(5, 10)),
            LeaseStatus::Expired,
        );
        assert_eq!(d, PreVoteDecision::Grant);
        assert!(d.is_granted());
    }

    #[test]
    fn pre_vote_rejected_when_lease_active_even_if_log_up_to_date() {
        // W3.2 — lease check overrides log freshness.
        let d = evaluate_pre_vote::<u64>(
            5,
            5,
            Some(&lid(5, 10)),
            Some(&lid(5, 10)),
            LeaseStatus::Active {
                remaining: std::time::Duration::from_millis(500),
            },
        );
        assert_eq!(d, PreVoteDecision::RejectLeaseActive);
    }

    #[test]
    fn pre_vote_granted_after_lease_invalidation_w3_7() {
        // W3.7 — after CheckQuorum stepdown invalidates the lease, a new
        // candidate's PreVote succeeds.
        let d = evaluate_pre_vote::<u64>(
            6,
            5,
            Some(&lid(5, 10)),
            Some(&lid(5, 10)),
            LeaseStatus::Invalidated,
        );
        assert_eq!(d, PreVoteDecision::Grant);
    }

    #[test]
    fn pre_vote_rejected_when_log_stale() {
        let d = evaluate_pre_vote::<u64>(
            5,
            5,
            Some(&lid(5, 5)),  // candidate at index 5
            Some(&lid(5, 10)), // voter at index 10
            LeaseStatus::Expired,
        );
        assert_eq!(d, PreVoteDecision::RejectStaleLog);
    }

    #[test]
    fn pre_vote_rejected_when_term_stale() {
        let d = evaluate_pre_vote::<u64>(
            3, // candidate
            5, // voter
            Some(&lid(3, 100)),
            Some(&lid(5, 10)),
            LeaseStatus::Expired,
        );
        assert_eq!(d, PreVoteDecision::RejectStaleTerm);
    }

    #[test]
    fn pre_vote_lease_active_takes_priority_over_stale_log() {
        // Demonstrate that when both checks would fail, lease wins (it's
        // checked first after term).
        let d = evaluate_pre_vote::<u64>(
            5,
            5,
            Some(&lid(5, 1)),  // stale log
            Some(&lid(5, 10)),
            LeaseStatus::Active {
                remaining: std::time::Duration::from_millis(500),
            },
        );
        assert_eq!(d, PreVoteDecision::RejectLeaseActive);
    }

    /// W3.4 — the runaway-term repro: a partitioned candidate with stale log
    /// and inflated term must NOT have its PreVote granted.
    #[test]
    fn w3_4_runaway_term_repro_partitioned_candidate_with_stale_log() {
        // Voter is healthy: lease active because it still hears from leader.
        // Candidate woke up from partition with inflated prospective term.
        let d = evaluate_pre_vote::<u64>(
            100,               // partitioned candidate's inflated term
            5,                 // voter's current term
            Some(&lid(5, 1)),  // candidate's log frozen at partition time
            Some(&lid(5, 50)), // voter's log advanced during partition
            LeaseStatus::Active {
                remaining: std::time::Duration::from_millis(500),
            },
        );
        // Lease-active wins. With PreVote, the candidate gets a hard NO and
        // never increments its persisted term. This is the protocol fix for
        // bug-raft-stale-candidate-runaway-term-no-prevote.md.
        assert_eq!(d, PreVoteDecision::RejectLeaseActive);
    }
}
