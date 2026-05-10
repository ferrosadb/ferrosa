//! Tests for `Engine::handle_pre_vote_req` (ferrosa fork — ADR-012, W3.3, W3.7).
//!
//! PreVote (Ongaro §9.6) is a non-mutating probe: the receiver checks whether
//! it would grant a real vote to the candidate, **without** mutating its own
//! persistent vote state. The decision uses:
//!   1. Term check (PreVote permits equal-term: candidate hasn't actually
//!      incremented its persisted term yet).
//!   2. Lease-aware rejection (Ongaro §9.6 caveat): if this voter has heard
//!      from the current leader within `leader_lease`, reject regardless of
//!      log freshness.
//!   3. Election restriction (Raft §5.4.1): candidate's last_log_id must be
//!      at least as up-to-date.

use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::core::ServerState;
use crate::engine::testing::UTConfig;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::raft::PreVoteRequest;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::CommittedLeaderId;
use crate::EffectiveMembership;
use crate::LogId;
use crate::Membership;
use crate::TokioInstant;
use crate::Vote;

fn m12() -> Membership<u64, ()> {
    Membership::new(vec![btreeset! {1,2}], None)
}

/// Voter is node 2, has just heard from leader (node 1) and is in Follower
/// state with the lease active.
fn eng_with_active_lease() -> Engine<UTConfig> {
    let mut eng = Engine::default();
    eng.state.log_ids = LogIdList::new([LogId::new(CommittedLeaderId::new(0, 0), 0)]);
    eng.state.enable_validation(false);
    eng.config.id = 2;
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(0, 1, 1)), m12())));
    // Just heard from the leader (lease active).
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(1, 1));
    eng.state.server_state = eng.calc_server_state();
    eng.output.take_commands();
    eng
}

/// Voter is node 2, lease is expired (hasn't heard from leader in a long time).
fn eng_with_expired_lease() -> Engine<UTConfig> {
    let mut eng = eng_with_active_lease();
    // Set vote_last_modified to far in the past.
    let lease = eng.config.timer_config.leader_lease;
    eng.state.vote = UTime::new(
        TokioInstant::now() - lease - Duration::from_millis(100),
        Vote::new_committed(1, 1),
    );
    eng
}

#[test]
fn pre_vote_rejected_when_lease_active() -> anyhow::Result<()> {
    // W3.2 — lease-aware rejection: voter that has heard from the current
    // leader within the last election timeout MUST reject pre-votes.
    let mut eng = eng_with_active_lease();
    let initial_vote = eng.state.vote_ref().clone();

    let resp = eng.handle_pre_vote_req(PreVoteRequest {
        vote: Vote::new(2, 3),
        last_log_id: Some(log_id(2, 3, 100)),
    });

    assert!(!resp.vote_granted, "pre-vote must be rejected while lease is active");
    // Crucially, the persistent vote state is NOT mutated by handling a pre-vote.
    assert_eq!(initial_vote, *eng.state.vote_ref(), "PreVote handling must not mutate vote");
    Ok(())
}

#[test]
fn pre_vote_granted_when_lease_expired_and_log_up_to_date() -> anyhow::Result<()> {
    let mut eng = eng_with_expired_lease();
    let initial_vote = eng.state.vote_ref().clone();

    let resp = eng.handle_pre_vote_req(PreVoteRequest {
        vote: Vote::new(2, 3),
        last_log_id: Some(log_id(0, 0, 0)),
    });

    assert!(resp.vote_granted, "pre-vote must be granted when lease expired and log up-to-date");
    // Persistent vote unchanged.
    assert_eq!(initial_vote, *eng.state.vote_ref(), "PreVote handling must not mutate vote");
    Ok(())
}

#[test]
fn pre_vote_rejected_when_log_stale_even_with_expired_lease() -> anyhow::Result<()> {
    // Election restriction (Raft §5.4.1): candidate's log must be at least
    // as up-to-date as the voter's.
    let mut eng = eng_with_expired_lease();
    // Voter has logs through index 50 at term 1.
    eng.state.log_ids = LogIdList::new([log_id(1, 1, 50)]);
    eng.state.enable_validation(false);

    let resp = eng.handle_pre_vote_req(PreVoteRequest {
        vote: Vote::new(2, 3),
        last_log_id: Some(log_id(1, 1, 5)), // stale: behind voter
    });

    assert!(!resp.vote_granted, "pre-vote must be rejected when candidate's log is stale");
    Ok(())
}

#[test]
fn pre_vote_rejected_when_term_stale() -> anyhow::Result<()> {
    let mut eng = eng_with_expired_lease();
    // Voter's current term is 5 (move it forward).
    eng.state.vote.update(TokioInstant::now(), Vote::new(5, 1));
    // Force lease expired.
    let lease = eng.config.timer_config.leader_lease;
    eng.state.vote = UTime::new(
        TokioInstant::now() - lease - Duration::from_millis(100),
        Vote::new(5, 1),
    );

    let resp = eng.handle_pre_vote_req(PreVoteRequest {
        vote: Vote::new(3, 3), // candidate's term is 3 (stale)
        last_log_id: Some(log_id(0, 0, 0)),
    });

    assert!(!resp.vote_granted, "pre-vote must be rejected when candidate's term is stale");
    Ok(())
}

#[test]
fn pre_vote_does_not_mutate_vote_state() -> anyhow::Result<()> {
    // Property test: regardless of grant/reject, handling a PreVoteRequest
    // must NOT mutate self.state.vote, and must NOT emit a SaveVote command.
    // This is the core safety property that distinguishes PreVote from Vote.
    let mut eng = eng_with_expired_lease();
    let initial_vote = eng.state.vote_ref().clone();
    let initial_state = eng.state.server_state;

    // Throw a high-term pre-vote at the engine.
    let _resp = eng.handle_pre_vote_req(PreVoteRequest {
        vote: Vote::new(99, 3), // very high candidate term
        last_log_id: Some(log_id(99, 3, 1000)),
    });

    // Persistent vote unchanged.
    assert_eq!(initial_vote, *eng.state.vote_ref(), "PreVote must not mutate vote");
    // Server state unchanged.
    assert_eq!(initial_state, eng.state.server_state);
    // No SaveVote command emitted.
    let cmds = eng.output.take_commands();
    let has_save = cmds.iter().any(|c| matches!(c, crate::engine::Command::SaveVote { .. }));
    assert!(!has_save, "PreVote must not emit SaveVote");

    Ok(())
}

/// W3.3 + W3.4 — the runaway-term safety property at the engine level.
///
/// Scenario: a partitioned candidate (node 3, not modeled here directly —
/// instead we model the *voter* — node 2) has been disconnected and would,
/// without PreVote, repeatedly increment its term while trying real votes.
/// With PreVote, the voter (node 2, still hearing from the leader)
/// rejects the pre-vote on the lease check. The candidate must NOT advance
/// its persisted term as a result.
///
/// This test verifies the *voter's* side: it never updates its own vote
/// state in response to a pre-vote probe, no matter how high the
/// prospective term in the request. A separate test in
/// `pre_vote_decision::tests::w3_4_runaway_term_repro_partitioned_candidate_with_stale_log`
/// covers the decision predicate directly.
#[test]
fn candidate_does_not_advance_term_on_prevote_failure() -> anyhow::Result<()> {
    // The "candidate" here is whoever sent us this request. We model node 2
    // (the voter) and verify it does NOT mutate its vote when rejecting.
    let mut eng = eng_with_active_lease();
    let initial_vote = eng.state.vote_ref().clone();
    let initial_term = initial_vote.leader_id().get_term();
    assert_eq!(ServerState::Follower, eng.state.server_state);

    // Simulate a sequence of pre-vote probes from a stale candidate that
    // keeps trying with ever-higher prospective terms (the runaway pattern).
    for inflated_term in [10, 50, 100, 500, 1000] {
        let resp = eng.handle_pre_vote_req(PreVoteRequest {
            vote: Vote::new(inflated_term, 3),
            last_log_id: Some(log_id(0, 0, 0)),
        });
        assert!(!resp.vote_granted, "active lease must reject pre-vote");
        // Voter's persisted term is unchanged after every probe.
        assert_eq!(
            initial_term,
            eng.state.vote_ref().leader_id().get_term(),
            "voter's persisted term must not advance from a pre-vote probe"
        );
    }

    // Final invariant.
    assert_eq!(initial_vote, *eng.state.vote_ref());
    assert_eq!(ServerState::Follower, eng.state.server_state);

    Ok(())
}
