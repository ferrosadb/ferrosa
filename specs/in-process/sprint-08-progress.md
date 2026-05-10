---
type: sprint-progress
sprint: 8
status: in-progress
created: 2026-05-09
---

# Sprint 8 Progress Log

## Environment

- Worktree: `/home/bkearns/src/ferrosa-suite/sprint-08-learners-endurance/`
- Branch: `sprint-08-learners-endurance` (forked from `feature/raft-gap-close`)
- All prior sprints (1–7) merged into the parent branch.

## Headline path

`fly` CLI is unavailable in this environment, so per the spec's stuck criteria
the headline acceptance test for **W8.9** uses path **(b)**: a
`tier-endurance-sim` running 24 simulated hours via `ferrosa-sim`. ADR-016
"Layered verification stack" treats the simulator as a first-class verification
layer.

The Fly.io machinery (`ferrosa-jepsen` tier definition, region/topology spec,
Knossos rolling-window analysis) is wired into source so that an operator with
real Fly.io credentials can run `cargo run --bin ferrosa-jepsen -- tier
endurance --hours 24` once budget is approved.

## Per–work-item log

### W8.4 — CL-aware read routing for learners — DONE

- RED tests in `coordinator/cl_routing.rs`:
  - `local_one_routes_to_any_local_replica` — `LOCAL_ONE` keeps voter + learner.
  - `local_quorum_excludes_learners_from_quorum` — `LOCAL_QUORUM` drops learners.
  - `quorum_excludes_learners_from_quorum` — same for `QUORUM`.
  - `serial_forces_leader_round_trip_skips_learner` — `SERIAL` /
    `LOCAL_SERIAL` mark `leader_only=true` and exclude learners.
  - `all_includes_token_owning_learner` — `ALL` keeps every replica.
- GREEN: introduced `CLReplicaPolicy` + `replica_policy_for_cl` +
  `eligible_replicas_for_cl`. `coordinator::read::coordinate_read_with`
  now filters via `eligible_replicas_for_cl(cl, raw_replicas, &ring)` after
  `ring.replicas()`.
- Gate: 715/715 cluster lib tests + 4/4 learner-lifecycle pass; clippy + fmt clean.

### W8.2 — `MembershipChanger::add_learner_only` — DONE

- RED: `add_learner_only_does_not_make_voter` in
  `ferrosa-cluster/tests/learner_lifecycle.rs` asserts the learner lands as
  `NodeState::Learner` in `state.members`, openraft sees it as a learner (not a
  voter), and the voter set size is unchanged.
- GREEN: introduced `NodeJoinConfig { owns_tokens }` and the
  `add_learner_only` method on `MembershipChanger`. Refactored peer-setup
  into the shared `join_as_learner` + `submit_join_node` helpers consumed by
  both `add_voter` and `add_learner_only`.
- Gate: 4/4 learner-lifecycle integration tests + 710/710 cluster lib pass.
  Clippy + fmt clean.

### W8.3 — `promote_learner_to_voter` and `demote_voter_to_learner` — DONE

- RED:
  - `promote_learner_to_voter_preserves_log_position` — log index strictly
    advances; openraft promotes to voter; `state.members[N].state == Normal`.
  - `demote_voter_to_learner_preserves_application_state` — non-leader voter
    is removed from the voter set, re-added as a learner, and
    `state.members[N].state` becomes `Learner { .. }`.
  - `demote_voter_to_learner_transfers_leader_first_if_needed` — when the
    target is the current leader, the changer transfers leadership and
    returns `MembershipError::NotLeader`.
- GREEN: `promote_learner_to_voter` issues
  `change_membership(AddVoterIds)` followed by `RaftOp::SetNodeState =
  Normal`. `demote_voter_to_learner` performs leader-self transfer (mirrors
  W4.14), then `change_membership(RemoveVoters)` + `add_learner` +
  `RaftOp::SetNodeState = Learner`.
- Helper added to the harness: `current_leader_id()` and
  `raft_for_node_id()` so tests can chase leadership into freshly-added
  voters without panicking.
- Gate: 4/4 learner-lifecycle integration tests pass; clippy + fmt clean.

### W8.1 — `NodeState::Learner` lifecycle variant — DONE

- RED: `node_state_learner_distinct_from_voter` in `ferrosa-cluster/src/ring/mod.rs`
  asserts a learner with `owns_tokens=false` is excluded from `replicas()`.
  Failed initially because the variant didn't exist.
- GREEN: added `NodeState::Learner { owns_tokens: bool }` to `raft/mod.rs` plus
  `is_voter` / `is_learner` / `owns_tokens` accessors. `ring::replicas()` now
  treats `Learner { owns_tokens: true }` as eligible and `Learner { owns_tokens:
  false }` as excluded. `web/api.rs::node_state_to_str` updated for
  exhaustiveness.
- Gate: 710/710 cluster lib tests pass. `cargo clippy --all-targets -- -D
  warnings` clean. `cargo fmt --check` clean.

