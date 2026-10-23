//! Module: Live-leader evidence for readiness.
//! Correctness: Correct when [`leader_liveness`] answers `Live` only if this
//!   node has evidence from the current process that the leader it names is
//!   alive: as leader, a quorum ack within the window; as follower, an
//!   accepted AppendEntries from that leader at the current term within the
//!   window.
//! Last revised: 2026-10-23
//! Last changed: Created for the 2026-10-23 nightly control-cluster roll.
//!
//! # Why
//!
//! `/readyz` used to answer ready whenever `metrics.current_leader` was
//! `Some`. openraft derives that from the committed vote, and the vote is
//! persisted, so a restarted node names a leader before it has heard from
//! anybody. On 2026-10-23 a former leader restarted after the other two nodes
//! had elected a new one at a higher term. It came back naming ITSELF from its
//! stale vote and answered `{"ready":true}`. Three seconds later its election
//! timeout fired and it answered 503, and the nightly roll refused to continue.
//!
//! # Feeds
//!
//! - [`super::handlers::RaftAppendHandler`] calls [`LeaderContact::record`] for
//!   every AppendEntries this node accepts.
//! - The `/readyz` handler calls [`leader_liveness`].

use std::time::{Duration, Instant};

use openraft::ServerState;

/// The last AppendEntries this node accepted: who sent it, at which term, when.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Contact {
    pub leader: u64,
    pub term: u64,
    pub at: Instant,
}

/// The most recent accepted AppendEntries, since process start.
///
/// Deliberately in-memory only: evidence from before a restart is exactly the
/// stale evidence this exists to ignore.
#[derive(Debug, Default)]
pub struct LeaderContact {
    last: parking_lot::Mutex<Option<Contact>>,
}

impl LeaderContact {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an accepted AppendEntries from `leader` at `term`.
    pub fn record(&self, leader: u64, term: u64, at: Instant) {
        *self.last.lock() = Some(Contact { leader, term, at });
    }

    /// The last recorded contact, or `None` if none since process start.
    pub fn last(&self) -> Option<Contact> {
        *self.last.lock()
    }
}

/// The `(leader, term)` an AppendEntries proves live, if this node accepted it.
///
/// Every reply except `HigherVote` means openraft accepted the sender's vote
/// as the current leader's. `Conflict` is still contact: the leader is alive
/// and only the logs disagree. `HigherVote` means the sender is stale.
pub fn accepted_append(
    vote: &openraft::Vote<u64>,
    resp: &openraft::raft::AppendEntriesResponse<u64>,
) -> Option<(u64, u64)> {
    if matches!(resp, openraft::raft::AppendEntriesResponse::HigherVote(_)) {
        return None;
    }
    let leader_id = vote.leader_id();
    Some((leader_id.voted_for()?, leader_id.get_term()))
}

/// The parts of `RaftMetrics` the liveness decision reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderView {
    pub id: u64,
    pub state: ServerState,
    pub current_term: u64,
    pub current_leader: Option<u64>,
    pub millis_since_quorum_ack: Option<u64>,
}

/// Whether this node has live evidence of a leader, and if not, why not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderLiveness {
    Live,
    /// The node names no leader at all.
    NoLeader,
    /// This node is leader, but no quorum has acked it within the window.
    LeaderWithoutQuorum,
    /// The named leader has not been heard from since this process started.
    NoContactSinceStart,
    /// The last contact is older than the window, or from another term or leader.
    StaleContact,
}

impl LeaderLiveness {
    /// Operator-facing reason for a not-live answer; `None` when live.
    pub fn reason(self) -> Option<&'static str> {
        match self {
            Self::Live => None,
            Self::NoLeader => Some("no raft leader elected yet"),
            Self::LeaderWithoutQuorum => {
                Some("this node is raft leader but no quorum has acknowledged it recently")
            }
            Self::NoContactSinceStart => Some(
                "the raft leader named by this node's persisted vote has not contacted it \
                 since it started",
            ),
            Self::StaleContact => {
                Some("this node has not heard from its raft leader within the election timeout")
            }
        }
    }
}

/// Decide whether `view`'s leader is live, given this node's last `contact`.
///
/// `window` should be the election timeout maximum: past it, Raft itself
/// treats the leader as gone, so readiness must not claim otherwise.
pub fn leader_liveness(
    view: LeaderView,
    contact: Option<Contact>,
    now: Instant,
    window: Duration,
) -> LeaderLiveness {
    let Some(leader) = view.current_leader else {
        return LeaderLiveness::NoLeader;
    };
    if view.state == ServerState::Leader && leader == view.id {
        // openraft republishes metrics after every engine run, and a leader
        // runs one per heartbeat tick, so this age is at most one heartbeat old.
        let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
        return match view.millis_since_quorum_ack {
            Some(ms) if ms < window_ms => LeaderLiveness::Live,
            _ => LeaderLiveness::LeaderWithoutQuorum,
        };
    }
    // Any other state naming a leader, including a Follower naming ITSELF from
    // a stale vote (the 2026-10-23 case), needs contact from that leader.
    let Some(contact) = contact else {
        return LeaderLiveness::NoContactSinceStart;
    };
    let current = contact.leader == leader && contact.term == view.current_term;
    if current && now.saturating_duration_since(contact.at) < window {
        LeaderLiveness::Live
    } else {
        LeaderLiveness::StaleContact
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SELF_ID: u64 = 7;
    const LEADER: u64 = 3;
    const WINDOW: Duration = Duration::from_millis(6_000);

    fn follower_of(leader: u64, term: u64) -> LeaderView {
        LeaderView {
            id: SELF_ID,
            state: ServerState::Follower,
            current_term: term,
            current_leader: Some(leader),
            millis_since_quorum_ack: None,
        }
    }

    #[test]
    fn restarted_follower_naming_a_leader_from_its_persisted_vote_is_not_live() {
        // Given a node that just restarted: its committed vote names a leader,
        // and it has received nothing since it started.
        let view = follower_of(LEADER, 38_804);

        // When readiness asks whether the leader is live,
        let answer = leader_liveness(view, None, Instant::now(), WINDOW);

        // Then the persisted vote alone is not evidence.
        assert_eq!(answer, LeaderLiveness::NoContactSinceStart);
    }

    #[test]
    fn restarted_follower_naming_itself_from_a_stale_vote_is_not_live() {
        // The exact 2026-10-23 shape: the former leader restarts as a Follower
        // whose persisted committed vote names itself.
        let view = follower_of(SELF_ID, 38_805);
        let answer = leader_liveness(view, None, Instant::now(), WINDOW);
        assert_eq!(answer, LeaderLiveness::NoContactSinceStart);
    }

    #[test]
    fn follower_with_recent_contact_from_its_leader_at_the_current_term_is_live() {
        let now = Instant::now();
        let contact = Contact {
            leader: LEADER,
            term: 9,
            at: now - Duration::from_millis(500),
        };
        let answer = leader_liveness(follower_of(LEADER, 9), Some(contact), now, WINDOW);
        assert_eq!(answer, LeaderLiveness::Live);
    }

    #[test]
    fn follower_whose_last_contact_is_older_than_the_window_is_not_live() {
        let now = Instant::now();
        let contact = Contact {
            leader: LEADER,
            term: 9,
            at: now - WINDOW,
        };
        let answer = leader_liveness(follower_of(LEADER, 9), Some(contact), now, WINDOW);
        assert_eq!(answer, LeaderLiveness::StaleContact);
    }

    #[test]
    fn contact_from_an_earlier_term_is_not_evidence_for_the_current_one() {
        let now = Instant::now();
        let contact = Contact {
            leader: LEADER,
            term: 8,
            at: now,
        };
        let answer = leader_liveness(follower_of(LEADER, 9), Some(contact), now, WINDOW);
        assert_eq!(answer, LeaderLiveness::StaleContact);
    }

    #[test]
    fn contact_from_a_different_leader_is_not_evidence_for_the_named_one() {
        let now = Instant::now();
        let contact = Contact {
            leader: 4,
            term: 9,
            at: now,
        };
        let answer = leader_liveness(follower_of(LEADER, 9), Some(contact), now, WINDOW);
        assert_eq!(answer, LeaderLiveness::StaleContact);
    }

    #[test]
    fn node_naming_no_leader_is_not_live() {
        let view = LeaderView {
            current_leader: None,
            ..follower_of(LEADER, 9)
        };
        let answer = leader_liveness(view, None, Instant::now(), WINDOW);
        assert_eq!(answer, LeaderLiveness::NoLeader);
    }

    fn leader(millis_since_quorum_ack: Option<u64>) -> LeaderView {
        LeaderView {
            id: SELF_ID,
            state: ServerState::Leader,
            current_term: 9,
            current_leader: Some(SELF_ID),
            millis_since_quorum_ack,
        }
    }

    #[test]
    fn leader_acked_by_a_quorum_within_the_window_is_live() {
        let answer = leader_liveness(leader(Some(200)), None, Instant::now(), WINDOW);
        assert_eq!(answer, LeaderLiveness::Live);
    }

    #[test]
    fn leader_never_acked_by_a_quorum_is_not_live() {
        let answer = leader_liveness(leader(None), None, Instant::now(), WINDOW);
        assert_eq!(answer, LeaderLiveness::LeaderWithoutQuorum);
    }

    #[test]
    fn leader_whose_last_quorum_ack_is_older_than_the_window_is_not_live() {
        let answer = leader_liveness(leader(Some(6_000)), None, Instant::now(), WINDOW);
        assert_eq!(answer, LeaderLiveness::LeaderWithoutQuorum);
    }

    #[test]
    fn every_not_live_answer_names_a_reason() {
        for answer in [
            LeaderLiveness::NoLeader,
            LeaderLiveness::LeaderWithoutQuorum,
            LeaderLiveness::NoContactSinceStart,
            LeaderLiveness::StaleContact,
        ] {
            assert!(answer.reason().is_some(), "{answer:?} must say why");
        }
        assert_eq!(LeaderLiveness::Live.reason(), None);
    }

    #[test]
    fn accepted_append_entries_is_contact_from_the_senders_leader_and_term() {
        use openraft::raft::AppendEntriesResponse as R;
        let vote = openraft::Vote::new_committed(9, LEADER);
        for resp in [R::Success, R::PartialSuccess(None), R::Conflict] {
            assert_eq!(accepted_append(&vote, &resp), Some((LEADER, 9)), "{resp:?}");
        }
    }

    #[test]
    fn append_entries_refused_with_a_higher_vote_is_not_contact() {
        let vote = openraft::Vote::new_committed(8, LEADER);
        let resp = openraft::raft::AppendEntriesResponse::HigherVote(openraft::Vote::new(9, 4));
        assert_eq!(accepted_append(&vote, &resp), None);
    }

    #[test]
    fn recorded_contact_is_returned_and_replaced_by_newer_contact() {
        let tracker = LeaderContact::new();
        assert_eq!(tracker.last(), None, "no contact since start");
        let t0 = Instant::now();
        tracker.record(LEADER, 9, t0);
        tracker.record(4, 10, t0 + Duration::from_millis(1));
        assert_eq!(
            tracker.last(),
            Some(Contact {
                leader: 4,
                term: 10,
                at: t0 + Duration::from_millis(1)
            })
        );
    }
}
