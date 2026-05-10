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

