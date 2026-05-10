---
type: progress
sprint: 4
created: 2026-05-09
last-updated: 2026-05-09
---

# Sprint 4 Progress — Bootstrap decomposition + snapshot transport + bolt-on retirement

## Approach

Strict TDD throughout. Each WI lands as a single focused commit (or a small
red-green-refactor pair) on the `sprint-04-bootstrap-snapshot-bolt-on`
worktree branch. CI gates run before every commit.

Sprint 6 also touches `controller/cluster.rs::transition_to_cluster` for
multi-Raft plumbing. To avoid a head-on collision the bootstrap phase
decomposition lands as a **new `controller/bootstrap/` module with the pure
phase logic and explicit pre/post conditions**, while the in-place
implementation in `transition_to_cluster` is left intact. Once Sprint 6's
multi-Raft scaffolding settles, a follow-up sprint (or a coordinator-driven
merge) will rewire `transition_to_cluster` to consume the typed phases
directly. This keeps Sprint 4's changes additive and Sprint 6 unblocked.

## Per-work-item status

