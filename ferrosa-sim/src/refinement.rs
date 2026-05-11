//! TLA+ refinement check (W5.10).
//!
//! Sprint 5's headline goal: every transition the simulator records
//! in its [`Trace`] is a valid step of the TLA+ spec at
//! `specs/tla/raft.tla`.  When Apalache is available, the operator
//! exports the trace as `apalache.json` and feeds it through
//! `apalache-mc check --simulate`.  Until then this module
//! interprets a subset of the spec in Rust.
//!
//! The interpreter operates on an [`AbstractState`] that mirrors
//! the spec's variables: `currentTerm`, `votedFor`, `log_len`,
//! `state` (role).  Each [`TlaAction`] is a transition predicate;
//! invalid transitions (term going backwards, two leaders at the
//! same term, etc.) become [`RefinementError`]s.

use crate::node::NodeId;
use crate::trace::{TlaAction, Trace};
use std::collections::BTreeMap;

/// Per-node abstract state.  Same shape as the TLA+ spec's
/// `currentTerm`, `votedFor`, `state`, `log_len` mapping.
#[derive(Clone, Debug, Default)]
pub struct AbstractNode {
    /// Current Raft term.
    pub term: u64,
    /// Vote granted in the current term, if any.
    pub voted_for: Option<NodeId>,
    /// Role: 0 = Follower / PreVoter, 1 = Candidate, 2 = Leader.
    pub role: u8,
}

/// Aggregate abstract state.  Builds incrementally as the trace
/// is replayed action-by-action.
#[derive(Clone, Debug, Default)]
pub struct AbstractState {
    /// Per-node state.
    pub nodes: BTreeMap<NodeId, AbstractNode>,
}

/// One refinement-check failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefinementError {
    /// Index of the offending [`crate::TraceEntry`] in the trace.
    pub step: usize,
    /// Text describing why the action is not a valid TLA+ step.
    pub reason: String,
}

const ROLE_FOLLOWER: u8 = 0;
const ROLE_CANDIDATE: u8 = 1;
const ROLE_LEADER: u8 = 2;

impl AbstractState {
    /// Make sure `id` exists in `nodes`; insert a default record if
    /// not.
    fn ensure(&mut self, id: NodeId) -> &mut AbstractNode {
        self.nodes.entry(id).or_default()
    }
}

/// Replay `trace` step-by-step, returning the final state on
/// success or a [`RefinementError`] at the first illegal step.
pub fn check_trace(trace: &Trace) -> Result<AbstractState, RefinementError> {
    let mut state = AbstractState::default();
    for (i, entry) in trace.entries.iter().enumerate() {
        check_step(&mut state, &entry.action)
            .map_err(|reason| RefinementError { step: i, reason })?;
    }
    Ok(state)
}

/// Check and apply one [`TlaAction`].  Returns `Err(reason)` if the
/// transition is not permitted by the spec.
pub fn check_step(state: &mut AbstractState, action: &TlaAction) -> Result<(), String> {
    use TlaAction::*;
    match *action {
        // BecomeCandidate: term must strictly increase, role moves
        // away from Leader.
        BecomeCandidate { node, term } => {
            let s = state.ensure(node);
            if term <= s.term {
                return Err(format!(
                    "BecomeCandidate(node={node}, term={term}): \
                     new term must be > current ({})",
                    s.term
                ));
            }
            s.term = term;
            s.role = ROLE_CANDIDATE;
            s.voted_for = Some(node);
            Ok(())
        }
        // RequestVote: candidate's term must match its abstract
        // state.  The wire-level message itself does not mutate
        // anything.
        RequestVote { from, term, .. } => {
            let s = state.ensure(from);
            if term != s.term {
                return Err(format!(
                    "RequestVote(from={from}, term={term}): \
                     candidate's term must match its current ({})",
                    s.term
                ));
            }
            Ok(())
        }
        // GrantVote: voter's term updates upward to the candidate's.
        // Voter must not have already voted for someone else this
        // term (Election Safety precondition).
        GrantVote { from, to, term } => {
            // Voter side of the action carries `from = voter, to = candidate`.
            let voter = state.ensure(from);
            if term < voter.term {
                return Err(format!(
                    "GrantVote(voter={from}, term={term}): \
                     stale grant — voter is at term {}",
                    voter.term
                ));
            }
            if term > voter.term {
                voter.term = term;
                voter.voted_for = None;
            }
            if voter.voted_for.is_some() && voter.voted_for != Some(to) {
                return Err(format!(
                    "GrantVote(voter={from}, term={term}, to={to}): \
                     already voted for {:?}",
                    voter.voted_for
                ));
            }
            voter.voted_for = Some(to);
            Ok(())
        }
        // RejectVote: similar to GrantVote but no vote stored.
        RejectVote { from, term, .. } => {
            let voter = state.ensure(from);
            if term > voter.term {
                voter.term = term;
                voter.voted_for = None;
            }
            Ok(())
        }
        // BecomeLeader: term must match the candidate's.  Election
        // Safety: there must not be another leader at the same term.
        BecomeLeader { node, term } => {
            for (&id, s) in &state.nodes {
                if id != node && s.role == ROLE_LEADER && s.term == term {
                    return Err(format!(
                        "BecomeLeader(node={node}, term={term}): \
                         Election Safety violation — node {id} is \
                         already leader at this term"
                    ));
                }
            }
            let s = state.ensure(node);
            if term != s.term {
                return Err(format!(
                    "BecomeLeader(node={node}, term={term}): \
                     leader term must match candidate's current ({})",
                    s.term
                ));
            }
            s.role = ROLE_LEADER;
            Ok(())
        }
        // BecomeFollower: term must be ≥ current.
        BecomeFollower { node, term } => {
            let s = state.ensure(node);
            if term < s.term {
                return Err(format!(
                    "BecomeFollower(node={node}, term={term}): \
                     term went backwards from {}",
                    s.term
                ));
            }
            s.term = term;
            s.role = ROLE_FOLLOWER;
            s.voted_for = None;
            Ok(())
        }
        // AppendEntries (heartbeat): leader's term must match its
        // abstract state, recipient's term must be ≤.
        AppendEntries { from, to, term } => {
            let leader = state.ensure(from);
            if term != leader.term {
                return Err(format!(
                    "AppendEntries(leader={from}, term={term}): \
                     leader term must match current ({})",
                    leader.term
                ));
            }
            if leader.role != ROLE_LEADER {
                return Err(format!(
                    "AppendEntries(leader={from}, term={term}): \
                     sender is not in Leader role (got {})",
                    leader.role
                ));
            }
            let follower = state.ensure(to);
            if follower.term > term {
                return Err(format!(
                    "AppendEntries(to={to}, term={term}): \
                     follower at higher term {}",
                    follower.term
                ));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::SimulatedCluster;

    /// W5.10 RED → GREEN: a 3-voter cluster's trace replays cleanly
    /// against the TLA+ refinement interpreter.
    #[test]
    fn every_sim_transition_is_tla_permitted() {
        let mut cluster = SimulatedCluster::with_voters(3, 42);
        cluster.run_until_leader(10_000).unwrap();
        check_trace(cluster.trace()).expect("trace must satisfy refinement");
    }

    /// W5.10 — 1000-seed sweep.  Every seed's trace must refine to
    /// the spec.  This is the headline test of Sprint 5.
    #[test]
    fn refinement_holds_across_1000_seeds() {
        for seed in 0..1000_u64 {
            let mut cluster = SimulatedCluster::with_voters(3, seed);
            cluster.run_until_leader(10_000);
            if let Err(e) = check_trace(cluster.trace()) {
                panic!("seed {seed} step {} failed: {}", e.step, e.reason);
            }
        }
    }

    /// Sanity: a hand-crafted invalid trace (two leaders at the
    /// same term) is rejected.
    #[test]
    fn refinement_rejects_two_leaders_same_term() {
        let mut state = AbstractState::default();
        // Node 1 wins term 1.
        check_step(&mut state, &TlaAction::BecomeCandidate { node: 1, term: 1 }).unwrap();
        check_step(&mut state, &TlaAction::BecomeLeader { node: 1, term: 1 }).unwrap();
        // Node 2 also enters term 1, then attempts to also become
        // leader at term 1 — refinement must reject.
        check_step(&mut state, &TlaAction::BecomeCandidate { node: 2, term: 1 }).unwrap();
        let err =
            check_step(&mut state, &TlaAction::BecomeLeader { node: 2, term: 1 }).unwrap_err();
        assert!(
            err.contains("Election Safety"),
            "expected ElectionSafety violation, got: {err}"
        );
    }

    /// Sanity: a stale `BecomeFollower` (term going backwards) is
    /// rejected.
    #[test]
    fn refinement_rejects_term_regression() {
        let mut state = AbstractState::default();
        check_step(&mut state, &TlaAction::BecomeCandidate { node: 1, term: 5 }).unwrap();
        let err =
            check_step(&mut state, &TlaAction::BecomeFollower { node: 1, term: 3 }).unwrap_err();
        assert!(err.contains("term went backwards"));
    }
}
