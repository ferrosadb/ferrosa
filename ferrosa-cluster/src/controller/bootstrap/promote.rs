//! Phase 7 — Promote.
//!
//! Pre-condition: BootstrapStream completed.
//! Post-condition: every peer in the Raft state-machine member map
//! reports `NodeState::Normal`.  Today the leader emits
//! `RaftCommand { op: SetNodeState{Normal}, .. }` per joiner once
//! BootstrapComplete acks have landed.

use std::collections::BTreeMap;

use super::phase::{BootstrapError, BootstrapPhase};
use crate::raft::NodeState;

/// View of the Raft state-machine member map at the moment we want to
/// validate the post-condition.
#[derive(Clone, Debug, Default)]
pub struct PromoteState {
    pub member_states: BTreeMap<u64, NodeState>,
}

pub fn precondition(stream_completed: bool) -> Result<(), BootstrapError> {
    if stream_completed {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::Promote,
            "BootstrapStream post-condition not satisfied",
        ))
    }
}

pub fn postcondition(state: &PromoteState) -> Result<(), BootstrapError> {
    if state.member_states.is_empty() {
        return Err(BootstrapError::phase(
            BootstrapPhase::Promote,
            "member map is empty",
        ));
    }
    let mut not_normal: Vec<(u64, NodeState)> = Vec::new();
    for (node, st) in &state.member_states {
        if !matches!(st, NodeState::Normal) {
            not_normal.push((*node, *st));
        }
    }
    if not_normal.is_empty() {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::Promote,
            format!(
                "{n} member(s) not Normal: {not_normal:?}",
                n = not_normal.len()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn promote_postcondition_holds_when_all_members_normal() {
        let mut m = BTreeMap::new();
        m.insert(1, NodeState::Normal);
        m.insert(2, NodeState::Normal);
        m.insert(3, NodeState::Normal);
        let state = PromoteState { member_states: m };
        precondition(true).expect("stream ok");
        postcondition(&state).expect("all normal");
    }

    #[test]
    fn promote_flags_member_in_joining_state() {
        let mut m = BTreeMap::new();
        m.insert(1, NodeState::Normal);
        m.insert(2, NodeState::Joining);
        let state = PromoteState { member_states: m };
        let err = postcondition(&state).expect_err("Joining → err");
        assert_eq!(err.name(), BootstrapPhase::Promote);
    }

    #[test]
    fn promote_rejects_empty_map() {
        let state = PromoteState::default();
        assert!(postcondition(&state).is_err());
    }
}
