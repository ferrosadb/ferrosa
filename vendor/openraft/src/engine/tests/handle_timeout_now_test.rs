//! Tests for `Engine::handle_timeout_now_req` (ferrosa fork — ADR-012, W3.8).
//!
//! `TimeoutNow` is the Ongaro §3.10 leadership-transfer directive. On receipt
//! with `req.vote.term >= self.current_term`, the receiver advances its term,
//! transitions to `Candidate`, and immediately starts an election (skipping
//! both the election timer and the PreVote phase).

use std::sync::Arc;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::core::ServerState;
use crate::engine::testing::UTConfig;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::raft::TimeoutNowRequest;
use crate::raft::TimeoutNowResponse;
use crate::raft::VoteRequest;
use crate::testing::log_id;
use crate::CommittedLeaderId;
use crate::EffectiveMembership;
use crate::LogId;
use crate::Membership;
use crate::Vote;

fn m12() -> Membership<u64, ()> {
    Membership::new(vec![btreeset! {1,2}], None)
}

fn eng() -> Engine<UTConfig> {
    let mut eng = Engine::default();
    eng.state.log_ids = LogIdList::new([LogId::new(CommittedLeaderId::new(0, 0), 0)]);
    eng.state.enable_validation(false);
    eng.config.id = 2;
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(0, 1, 1)), m12())));
    // Start as a follower with a committed leader vote at term 1.
    eng.state.vote.update(crate::TokioInstant::now(), Vote::new_committed(1, 1));
    eng.state.server_state = eng.calc_server_state();
    eng.output.take_commands();
    eng
}

#[test]
fn timeout_now_rpc_starts_election() -> anyhow::Result<()> {
    // W3.8 — receiving a TimeoutNow from current leader transitions to
    // Candidate and starts an election immediately (skipping PreVote).
    let mut eng = eng();
    assert_eq!(ServerState::Follower, eng.state.server_state);
    let initial_term = eng.state.vote_ref().leader_id().get_term();

    let req = TimeoutNowRequest {
        vote: Vote::new_committed(1, 1), // current leader's vote
        last_log_id: Some(log_id(0, 0, 0)),
    };

    let resp: TimeoutNowResponse<u64> = eng.handle_timeout_now_req(req);

    assert!(resp.started_election, "follower should accept directive and start election");

    // Term advanced.
    let new_term = eng.state.vote_ref().leader_id().get_term();
    assert_eq!(initial_term + 1, new_term);

    // Now in Candidate state with a vote for self.
    assert_eq!(ServerState::Candidate, eng.state.server_state);
    assert_eq!(Some(2), eng.state.vote_ref().leader_id().voted_for());

    // Engine emitted SaveVote + SendVote commands (the standard election path).
    let cmds = eng.output.take_commands();
    let has_send_vote = cmds.iter().any(|c| matches!(c,
        Command::SendVote { vote_req: VoteRequest { vote, .. } } if vote.leader_id().voted_for() == Some(2)
    ));
    assert!(has_send_vote, "expected SendVote command, got {:?}", cmds);

    Ok(())
}

#[test]
fn timeout_now_rejected_when_sender_term_stale() -> anyhow::Result<()> {
    // Defense-in-depth: if a stale TimeoutNow arrives from a former leader,
    // the receiver must NOT start a new election (which would advance term
    // unnecessarily and could disrupt the cluster).
    let mut eng = eng();
    // Move our term forward — we know about a newer leader at term 5.
    eng.state.vote.update(crate::TokioInstant::now(), Vote::new_committed(5, 1));
    eng.state.server_state = eng.calc_server_state();

    let req = TimeoutNowRequest {
        vote: Vote::new_committed(1, 1), // stale (term 1 vs our term 5)
        last_log_id: Some(log_id(0, 0, 0)),
    };

    let resp = eng.handle_timeout_now_req(req);

    assert!(!resp.started_election, "stale TimeoutNow must be rejected");
    // Term unchanged.
    assert_eq!(5, eng.state.vote_ref().leader_id().get_term());
    Ok(())
}

#[test]
fn timeout_now_rejected_when_log_behind_leader() -> anyhow::Result<()> {
    // Defense-in-depth: TimeoutNow says the leader believes us caught up,
    // but if our last_log_id is genuinely behind the leader's, refuse —
    // a stale-log candidate cannot win anyway and would only burn a term.
    let mut eng = eng();
    // Our log is at index 5.
    eng.state.log_ids = LogIdList::new([log_id(1, 1, 5)]);
    eng.state.enable_validation(false);

    // Leader thinks last_log is at index 10 (we're behind).
    let req = TimeoutNowRequest {
        vote: Vote::new_committed(1, 1),
        last_log_id: Some(log_id(1, 1, 10)),
    };

    let resp = eng.handle_timeout_now_req(req);

    assert!(!resp.started_election, "TimeoutNow must be refused when our log is behind leader's");
    Ok(())
}
