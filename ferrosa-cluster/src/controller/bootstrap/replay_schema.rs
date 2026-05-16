//! Phase 5 — ReplaySchema.
//!
//! Pre-condition: leader elected (WaitLeader post-condition holds).
//! Post-condition: every node in the cluster reports the same
//! `state.schema_version` as the leader.  Schema replay is a Raft
//! `client_write` driven from the leader; followers converge through
//! AppendEntries.
//!
//! The check is on `schema_version: Uuid` rather than the full schema
//! payload because Uuid equality is a sufficient witness once Raft has
//! committed: every entry replayed under the same Uuid will produce
//! the same applied state.

use std::collections::BTreeMap;

use uuid::Uuid;

use super::phase::{BootstrapError, BootstrapPhase};

/// Schema-replay snapshot.
#[derive(Clone, Debug)]
pub struct ReplaySchemaState {
    pub leader_node_id: u64,
    /// Schema version observed on each node (including the leader).
    pub node_schema_versions: BTreeMap<u64, Uuid>,
}

pub fn precondition(state: &ReplaySchemaState) -> Result<(), BootstrapError> {
    if !state
        .node_schema_versions
        .contains_key(&state.leader_node_id)
    {
        return Err(BootstrapError::phase(
            BootstrapPhase::ReplaySchema,
            format!(
                "leader node_id {} missing from schema-version map",
                state.leader_node_id
            ),
        ));
    }
    Ok(())
}

pub fn postcondition(state: &ReplaySchemaState) -> Result<(), BootstrapError> {
    let leader_version = state
        .node_schema_versions
        .get(&state.leader_node_id)
        .copied()
        .ok_or_else(|| {
            BootstrapError::phase(
                BootstrapPhase::ReplaySchema,
                "leader version vanished between pre and post",
            )
        })?;
    let mut divergent: Vec<(u64, Uuid)> = Vec::new();
    for (node, ver) in &state.node_schema_versions {
        if *ver != leader_version {
            divergent.push((*node, *ver));
        }
    }
    if divergent.is_empty() {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::ReplaySchema,
            format!(
                "{n} node(s) diverged from leader: {divergent:?}",
                n = divergent.len()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_postcondition_holds_when_all_nodes_match_leader() {
        let v = Uuid::from_bytes([7; 16]);
        let mut node_schema_versions = BTreeMap::new();
        node_schema_versions.insert(1, v);
        node_schema_versions.insert(2, v);
        node_schema_versions.insert(3, v);
        let state = ReplaySchemaState {
            leader_node_id: 1,
            node_schema_versions,
        };
        precondition(&state).expect("leader present → pre ok");
        postcondition(&state).expect("all match → post ok");
    }

    #[test]
    fn replay_postcondition_flags_divergent_follower() {
        let v_leader = Uuid::from_bytes([7; 16]);
        let v_follower = Uuid::from_bytes([8; 16]);
        let mut m = BTreeMap::new();
        m.insert(1, v_leader);
        m.insert(2, v_follower);
        let state = ReplaySchemaState {
            leader_node_id: 1,
            node_schema_versions: m,
        };
        let err = postcondition(&state).expect_err("divergence → err");
        assert_eq!(err.name(), BootstrapPhase::ReplaySchema);
    }

    #[test]
    fn precondition_requires_leader_in_map() {
        let state = ReplaySchemaState {
            leader_node_id: 1,
            node_schema_versions: BTreeMap::new(),
        };
        assert!(precondition(&state).is_err());
    }
}
