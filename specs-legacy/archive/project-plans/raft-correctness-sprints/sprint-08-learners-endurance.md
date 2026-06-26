---
type: sprint
status: pending
priority: P2
created: 2026-05-09
sprint: 8
wave: 5
---

# Sprint 8: Learner replicas + endurance run + witness evaluation

> Branch: `sprint-08-learners-endurance`.
> Companion to: ADR-014 (learners), ADR-015 (multi-DC + witness deferral).

## Goal

Implement long-lived learner replicas with operator API and CL-aware read routing. Run a 24h Fly.io tri-DC endurance test. Write a sharp design doc evaluating witness replicas (defer-or-do).

## Hard dependencies

- **All prior sprints merged** (1, 2, 3, 4, 5, 6, 7).

## Pre-flight checks

```sh
cd /home/bkearns/src/ferrosa-suite/ferrosa
git checkout main && git pull
git log --grep "Sprint [1-7]" --oneline | head    # all 7 merged
cargo test --workspace --lib && cargo clippy && cargo fmt --check
git checkout -b sprint-08-learners-endurance
fly auth status                                    # need Fly.io account for endurance
```

## TDD work items

### W8.1: `NodeState::Learner` lifecycle variant

**RED.** Test `node_state_learner_distinct_from_voter`: a node with `state.members[N].state == NodeState::Learner` is excluded from quorum calculations and from `ring.replicas()` if `owns_tokens=false`.

**GREEN.** Add `NodeState::Learner { owns_tokens: bool }` variant. Update `sync_ring()` to consult the variant.

**REFACTOR.** None.

### W8.2: `MembershipChanger::add_learner_only`

**RED.** Test `add_learner_only_does_not_make_voter`: call `MembershipChanger::add_learner_only(host_id, addr, NodeJoinConfig::default())`; assert `state.members[host_id].state == Learner`; openraft has it as Learner not Voter; quorum unchanged.

**GREEN.** Implement: peer_manager.ensure_peer; network_factory.register_node; raft.add_learner; RaftOp::JoinNode with `state: NodeState::Learner`. Skip the `change_membership(AddVoters)` step.

**REFACTOR.** Share peer-setup code with `add_voter`.

### W8.3: `promote_learner_to_voter` and `demote_voter_to_learner`

**RED.** Two tests:
- `promote_learner_to_voter_preserves_log_position`: ensure log advances continuously; no rewind.
- `demote_voter_to_learner_transfers_leader_first_if_needed`.

**GREEN.** Implement both. Promote = `change_membership(AddVoters)` + `RaftOp::SetNodeState(Normal)`. Demote = `transfer_leader` if needed + `change_membership(RemoveVoters + AddLearners)` + `RaftOp::SetNodeState(Learner)`.

**REFACTOR.** None.

### W8.4: CL-aware read routing for learners

**RED.** Three tests:
- `local_one_routes_to_any_local_replica`: voter or learner, whichever is closest.
- `local_quorum_excludes_learners_from_quorum`.
- `quorum_excludes_learners_from_quorum`.
- `serial_forces_leader_round_trip_skips_learner`.

**GREEN.** Update `coordinator/read.rs` to consult `NodeState::Learner` when computing replica sets per CL.

**REFACTOR.** Pull CL→eligible-roles into a table.

### W8.5: Operator commands

**RED.** Three tests:
- `ferrosa_ctl_cluster_add_learner`.
- `ferrosa_ctl_cluster_promote_to_voter`.
- `ferrosa_ctl_cluster_demote_to_learner`.

Each runs the command and asserts the lifecycle change committed via Raft.

**GREEN.** Add commands to `ferrosa-ctl`.

**REFACTOR.** None.

### W8.6: Token ownership per learner

**RED.** Test `learner_with_owns_tokens_false_excluded_from_replicas`: a learner with `owns_tokens=false` does not appear in `ring.replicas(token)`; reads with CL=ALL skip it.

**GREEN.** Update `ring/mod.rs::replicas` to consult the per-node `owns_tokens` flag.

**REFACTOR.** None.

### W8.7: Repair behaviour for learners

**RED.** Test `learner_with_owns_tokens_true_participates_in_repair`: triggered repair includes the learner; learner's data converges.

**GREEN.** Update repair logic to consult `owns_tokens`.

**REFACTOR.** None.

### W8.8: Learner-replica endurance: 1h sim run

**RED.** Test `endurance_1h_with_learners_under_load`: Sprint 5 sim; 3+3 dual-DC with 1 learner per DC; sustained Jepsen workload for 1h simulated; assert zero linearizability violations, zero membership invariant violations.

**GREEN.** Wire learners into the existing sim cluster builder.

**REFACTOR.** None.

### W8.9: 24h Fly.io tri-DC endurance run

**RED.** Add `tier-endurance` to `ferrosa-jepsen`. Topology: tri-DC (iad/cdg/nrt) on Fly.io. 3 voters + 1 learner per DC. 24h. Knossos every 10min on rolling history. Currently fails — tier doesn't exist.

**GREEN.** Wire the Fly.io machinery (already partial in `ferrosa-jepsen/src/flyio.rs`). Schedule a single end-of-sprint run as the acceptance gate.

**REFACTOR.** None.

### W8.10: Witness replicas — design evaluation

**RED.** Write `specs/in-process/witness-replicas-evaluation.md`. Cost analysis: per-DC voter ($X/month) vs witness ($Y/month). Engineering effort estimate: 2000–4000 LOC in openraft fork (per ADR-015 / Agent D). Concrete go/no-go recommendation with rationale.

**GREEN.** Document is the deliverable; no code.

**REFACTOR.** None.

### W8.11: Migration to openraft 1.0 evaluation

**RED.** Write `specs/in-process/openraft-1.0-migration-evaluation.md`. Track upstream openraft 1.0 progress; estimate migration effort; identify which of our fork patches (CheckQuorum, Leadership Transfer) may have landed upstream; PreVote will not have. Concrete go/no-go.

**GREEN.** Document is the deliverable.

**REFACTOR.** None.

## Acceptance criteria

- [ ] Learner lifecycle (W8.1, W8.2, W8.3, W8.4, W8.5, W8.6, W8.7).
- [ ] `endurance_1h_with_learners_under_load` (W8.8).
- [ ] 24h Fly.io endurance run completes; zero linearizability violations; zero membership invariant violations (W8.9).
- [ ] Witness evaluation doc lands with go/no-go (W8.10).
- [ ] openraft 1.0 evaluation doc lands (W8.11).

## Parallelization within Sprint 8

- **Track A (Learner code)**: W8.1, W8.2, W8.3, W8.4, W8.5, W8.6, W8.7 — mostly serial.
- **Track B (Endurance)**: W8.8, W8.9 — depends on A.
- **Track C (Evaluation)**: W8.10, W8.11 — independent.

A 3-engineer team finishes in ~2 weeks (excluding the 24h endurance wall time).

## Risks

- **R1 — Fly.io infra costs**: 24h tri-DC = $$. Budget approval before W8.9.
- **R2 — Endurance run reveals a bug**: this is the *point*. File the bug, fix it, rerun. Sprint completes only on a clean run.
- **R3 — Witness evaluation says "yes do it"**: opens Sprint 9. Document the dependencies.

## Completion checklist

- [ ] Branch + PR.
- [ ] CI green.
- [ ] 24h endurance passes (single successful run; rerun on any failure).
- [ ] Witness + openraft-1.0 evaluation docs landed.
- [ ] Coordinator file updated; Raft Correctness Program declared closed (or extended to Sprint 9 if witness goes ahead).

## Kickoff prompt for an agent

> Sprint 8. Spec at `specs/in-process/sprint-08-learners-endurance.md`. Hard dependencies: ALL prior sprints (1–7) merged.
>
> Implement learner replicas; run 24h Fly.io tri-DC endurance; write witness + openraft-1.0 evaluations.
>
> Companion reading: ADR-014 (learners), ADR-015 (multi-DC + witness rationale), `jepsen-e2e-test-plan.md` §"Endurance".
>
> Worktree at `/home/bkearns/src/ferrosa-suite/sprint-08/`. Strict TDD. The 24h endurance run is the headline acceptance gate; budget Fly.io accordingly. The two evaluation docs are *not* speculative — they should produce concrete go/no-go recommendations with effort estimates.
