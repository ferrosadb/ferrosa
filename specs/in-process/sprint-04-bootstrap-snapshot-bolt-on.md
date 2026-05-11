---
type: sprint
status: pending
priority: P1
created: 2026-05-09
sprint: 4
wave: 2
---

# Sprint 4: Bootstrap decomposition + snapshot transport + bolt-on retirement

> Branch: `sprint-04-bootstrap-snapshot-bolt-on`.
> Companion to: ADR-012 (bolt-on retirement), ADR-013 (Membership), ADR-018 (generic-snapshot-data).

## Goal

Decompose the 700-line bootstrap spaghetti into typed phases. Enable `generic-snapshot-data` with a dedicated `Lane::Snapshot`. Retire `election_guard` and `snapshot_pusher` (gated on a 2-week clean Jepsen window).

## Hard dependencies

- **Sprint 1 merged**: `MembershipChanger` API.
- **Sprint 3 merged**: PreVote + CheckQuorum + Leadership Transfer in fork; ferrosa repointed.

The bolt-on retirement step has an additional gate: 2 weeks of clean Jepsen smoke + standard tier runs against the Sprint 3 build. If those don't exist when Sprint 4 starts, the rest of Sprint 4 proceeds; the retirement work item (W4.10) blocks until the gate is satisfied.

## Pre-flight checks

```sh
cd /home/bkearns/src/ferrosa-suite/ferrosa
git checkout main && git pull
git log --grep "Sprint 1\|Sprint 3" --oneline | head      # verify dependencies merged
cargo test --workspace --lib && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git checkout -b sprint-04-bootstrap-snapshot-bolt-on
```

## TDD work items

### W4.1: Bootstrap phase types

**RED.** Test `bootstrap_phase_pre_post_conditions`: each phase has a `precondition()` and `postcondition()` returning `Result<(), BootstrapError>`. Test asserts the type signatures and that `BootstrapError` distinguishes phases.

**GREEN.** New module `controller/bootstrap/`:
```rust
pub enum BootstrapPhase {
    DeliverInvites,
    EstablishPools,
    CreateRaft,
    WaitLeader,
    ReplaySchema,
    BootstrapStream,
    Promote,
    DrainQueue,
}
pub enum BootstrapError {
    Phase { name: BootstrapPhase, source: anyhow::Error },
}
```

**REFACTOR.** None.

### W4.2: Phase 1 — DeliverInvites

**RED.** Test `deliver_invites_succeeds_to_all_peers`: 3-node setup; send invites; assert each peer received and acked.

**GREEN.** Extract the invite-delivery code from `controller/cluster.rs` into `controller/bootstrap/deliver_invites.rs`. Implement `precondition` (mode == Forming) and `postcondition` (every peer in `connected_peers` has acked).

**REFACTOR.** None.

### W4.3–W4.9: Phases 2–8

**RED → GREEN → REFACTOR per phase**. Each extracts ~50–100 lines from the existing 700-line bootstrap task into a typed phase with explicit pre/post conditions. Tests assert pre/post:

- W4.3 EstablishPools: postcondition all peer pools live on `Lane::Raft` and `Lane::Data`.
- W4.4 CreateRaft: postcondition `Arc<FerrosRaft>` published in three sinks (raft_tx, raft_instance_swap, ddl_path).
- W4.5 WaitLeader: postcondition `current_leader().await.is_some()` within 30s; otherwise returns `Phase::WaitLeader` error to caller.
- W4.6 ReplaySchema: postcondition every node's `state.schema_version` matches the leader's.
- W4.7 BootstrapStream: postcondition every owning replica has streamed its share.
- W4.8 Promote: postcondition every peer's `state.members[peer].state == Normal`.
- W4.9 DrainQueue: postcondition `ddl_queue_rx` is empty AND every queued DDL applied.

**REFACTOR per phase.** Pull common patterns into `controller/bootstrap/util.rs`.

### W4.10: Bolt-on retirement gate

**RED.** Test `bolt_on_retirement_gate_passes`: collects last 2 weeks of CI Jepsen results; asserts zero `ELECTION_STORM_TERM_JUMPS_TOTAL` increments; asserts the runaway-term repro produced zero term advances.

**GREEN.** No code change yet. The test passes only if the gate is satisfied. If it fails, file a PreVote/CheckQuorum bug and block W4.11/W4.12.

### W4.11: Retire `election_guard`

**RED.** Test `election_guard_module_does_not_exist`: compile-time `cfg!(not(feature = "election_guard"))` — should be unconditional now. Currently `election_guard.rs` exists.

**GREEN.** Delete `ferrosa-cluster/src/raft/election_guard.rs`. Remove all references from `controller/cluster.rs`. Keep `ELECTION_STORM_TERM_JUMPS_TOTAL` metric exposed but always zero (for one release, for downstream dashboards).

**REFACTOR.** Update `specs/raft-failure-mode-matrix.md` S-30 to note retirement.

### W4.12: Retire `snapshot_pusher`

**RED.** Test `snapshot_pusher_module_does_not_exist`. Currently `snapshot_pusher.rs` exists.

**GREEN.** Delete `ferrosa-cluster/src/raft/snapshot_pusher.rs`. Verify openraft's normal snapshot-on-log-inconsistency handles wiped-rebootstrap (S-04 in failure-mode matrix). The Sprint 1 `MembershipChanger` ensures every voter is registered atomically, eliminating P0-20.

**REFACTOR.** Update S-04 in failure-mode matrix.

### W4.13: `Lane::Snapshot` separation

**RED.** Test `snapshot_install_does_not_block_heartbeats`: stream a 100 MB snapshot to a follower while sustaining 1000 AppendEntries/sec; assert `RAFT_LANE_DELAY_P99` on `Lane::Raft` stays under `heartbeat_interval / 2 = 150ms`.

**GREEN.**
- New `Lane::Snapshot` (or reuse `Lane::Bulk` with a documented allocation). Update `ferrosa-net/src/codec.rs` if a new lane.
- Implement `ferrosa-cluster/src/raft/snapshot_transport.rs` using openraft's `generic-snapshot-data` feature. Custom transport over the dedicated lane.
- Cargo.toml: add `generic-snapshot-data` to openraft features.

**REFACTOR.** Move existing chunking constants into the new module.

### W4.14: Decommission flow uses Leadership Transfer

**RED.** Test `decommission_leader_transfers_first`: 3-node cluster; decommission the current leader; assert leadership transferred BEFORE LeaveNode applied; zero failed writes during decommission.

**GREEN.** Update `MembershipChanger::remove_voter` (Sprint 1 W1.4 stub): if target is current leader, call `raft.trigger().transfer_to(other_voter).await` first.

**REFACTOR.** None.

### W4.15: Steady-state failure-mode tests

**RED.** Implement integration tests for S-01 through S-37 from `specs/raft-failure-mode-matrix.md` §1–§6. Each scenario gets one test. Expected behaviour as documented in the matrix.

**GREEN.** Tests should pass against the post-Sprint-3 codebase. Any failure indicates a Sprint 1/3 regression OR a documented behaviour mismatch with reality (update the matrix).

**REFACTOR.** Pull common cluster-bring-up boilerplate into a `tests/common/`.

## Acceptance criteria (sprint-level)

- [ ] `bootstrap_phase_pre_post_conditions` (W4.1)
- [ ] All 8 phase tests pass (W4.2–W4.9)
- [ ] `bolt_on_retirement_gate_passes` (W4.10) — gate satisfied
- [ ] `election_guard_module_does_not_exist` (W4.11) and `snapshot_pusher_module_does_not_exist` (W4.12)
- [ ] `snapshot_install_does_not_block_heartbeats` (W4.13)
- [ ] `decommission_leader_transfers_first` (W4.14)
- [ ] All 37 tests for §1–§6 of failure-mode matrix pass (W4.15)
- [ ] CI green; no regressions in existing tests.

## Parallelization within Sprint 4

- **Track A (Bootstrap phases)**: W4.1–W4.9 — serialize because each phase builds on the previous.
- **Track B (Bolt-on retirement)**: W4.10 (gate), W4.11, W4.12 — gated by W4.10; can proceed in parallel with track A.
- **Track C (Snapshot transport)**: W4.13 — independent.
- **Track D (Tests)**: W4.14, W4.15 — partial dependency on tracks A and B.

A 3-engineer team finishes Sprint 4 in ~2 weeks.

## Risks

- **R1 — Bootstrap decomposition reveals latent races**: refactoring 700 lines is risky. Mitigation: every commit green; revert any commit that breaks an existing test.
- **R2 — Bolt-on retirement gate fails**: file a PreVote/CheckQuorum bug; W4.11/W4.12 deferred to a later sprint.
- **R3 — `Lane::Snapshot` interacts with existing lane allocations**: mitigation: review `PriorityPool` and `lane_actor.rs` for assumptions.

## Completion checklist

- [ ] Branch + PR.
- [ ] CI green.
- [ ] Coordinator updated.

## Kickoff prompt for an agent

> Sprint 4. Spec at `specs/in-process/sprint-04-bootstrap-snapshot-bolt-on.md`. Hard dependencies: Sprints 1 + 3 must be merged on `main` of `/home/bkearns/src/ferrosa-suite/ferrosa/`. Verify with `git log --grep "Sprint 1\|Sprint 3"`.
>
> Execute W4.1–W4.15 in TDD order. The bolt-on retirement gate (W4.10) requires 2 weeks of clean Jepsen runs against Sprint 3; if not satisfied, defer W4.11–W4.12 and document the failure mode.
>
> Worktree: `/home/bkearns/src/ferrosa-suite/sprint-04/` off main. Strict TDD throughout. CI green every commit.
>
> Companion reading: ADR-012, ADR-013, ADR-018, `specs/raft-failure-mode-matrix.md` §1–§6.
