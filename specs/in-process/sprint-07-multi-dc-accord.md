---
type: sprint
status: pending
priority: P1
created: 2026-05-09
sprint: 7
wave: 4
---

# Sprint 7: Multi-DC Accord cross-DC adapter + reorder-by-timestamp apply

> Branch: `sprint-07-multi-dc-accord`.
> Companion to: ADR-015 (multi-DC), Cassandra CEP-15 (Accord).

## Goal

Cross-DC consistency via Accord. Reorder-by-timestamp apply on `FerrosStateMachine`. Idempotent apply by Accord txn ID. Apply-durability barrier for Accord vote-commits. Joint-consensus DC swap drains in-flight Accord txns. Bank workload at QUORUM holds for 1h under `dc-partition + dc-slow`.

## Hard dependencies

- **Sprint 5 merged**: sim harness for verification; TLA+ multi-DC spec extension.
- **Sprint 6 merged**: per-DC Raft scaffolding.

## Pre-flight checks

```sh
cd /home/bkearns/src/ferrosa-suite/ferrosa
git checkout main && git pull
git log --grep "Sprint 5\|Sprint 6" --oneline | head
cargo test --workspace --lib && cargo clippy && cargo fmt --check
git checkout -b sprint-07-multi-dc-accord
```

## TDD work items

### W7.1: HLC + max-skew tracking on `FerrosStateMachine`

**RED.** Test `state_machine_tracks_hlc_watermark`: each apply step updates an HLC watermark; assert the watermark advances monotonically.

**GREEN.** Add `state.hlc_watermark: HlcTimestamp` and `state.max_observed_skew: Duration`. Update on every Accord-marked apply.

**REFACTOR.** None.

### W7.2: Reorder buffer for Accord-marked entries

**RED.** Test `apply_buffers_out_of_order_accord_entries`: feed two `RaftOp::AccordApply` entries with timestamps t1 < t2 in reverse order; assert state machine buffers t2, applies t1 first, then t2.

**GREEN.** Add `state.accord_apply_buffer: BTreeMap<AccordTimestamp, RaftOp>`. On apply of an Accord-marked entry, insert into buffer; advance watermark; drain entries with timestamp ≤ watermark.

**REFACTOR.** Pull the buffer into a `ReorderBuffer` struct with explicit watermark API.

### W7.3: Watermark advancement under bounded skew

**RED.** Test `watermark_advances_with_max_skew_200ms`: feed entries with HLC timestamps; watermark advances when `now() - 200ms > entry.timestamp`.

**GREEN.** Watermark = `min(entry.hlc for entry in buffer) - max_skew`. Advance on a timer (every `heartbeat_interval`).

**REFACTOR.** Configurable via `FERROSA_HLC_MAX_SKEW_MS`; default 200.

### W7.4: Reorder buffer stalls when skew exceeds bound

**RED.** Test `reorder_buffer_stalls_above_max_skew`: feed an entry with HLC 500ms in the future relative to local clock; max skew 200ms; assert the buffer holds the entry; cross-DC writes pause.

**GREEN.** Verify the W7.3 logic. Add a `RAFT_ACCORD_REORDER_BUFFER_DEPTH` gauge. Alarm if depth > 100.

**REFACTOR.** None.

### W7.5: Idempotent apply by Accord txn ID

**RED.** Test `accord_apply_idempotent`: apply the same `RaftOp::AccordApply { txn_id, .. }` twice; assert the second is a NoOp; resulting state is identical.

**GREEN.** Add `state.applied_accord_txns: BTreeMap<AccordTxnId, AppliedRecord>`. On apply, check; if present, skip.

**REFACTOR.** Garbage-collect old entries (older than max_skew × 100, say) to bound memory.

### W7.6: Apply-durability barrier for Accord votes

**RED.** Test `accord_vote_commit_waits_for_apply`: an Accord vote-commit calls `MembershipChanger::accord_vote_commit`; the commit returns only after `wait().applied_index_at_least(commit_index).await`.

**GREEN.** Implement the wait via openraft's `Raft::wait()` API.

**REFACTOR.** None.

### W7.7: Cross-DC write fan-out via Accord

**RED.** Test `cross_dc_write_uses_accord`: in 3+3 dual-DC, write at QUORUM; assert the path goes through Accord coordinator (verify via metrics or trace).

**GREEN.** Replace the Sprint 6 `NotImplemented` stub in `coordinator/write.rs` with an Accord coordination call. Path:
1. Coordinator receives write; identifies cross-DC nature from token ownership across DCs.
2. Initiates Accord pre-accept across both DCs' Raft groups.
3. On pre-accept majority (per-DC + cross-DC), commits via Accord apply.
4. Each DC's Raft group commits its share.

**REFACTOR.** Pull common Accord-coordination patterns into `accord/coordinator.rs`.

### W7.8: Joint-consensus DC swap drains Accord

**RED.** Test `dc_swap_drains_accord`: in 3+3 dual-DC with active Accord traffic, swap DC1 → DC3 via `MembershipChanger::swap_dc`; assert the swap waits for in-flight Accord txns referencing DC1 voters to complete or abort before the joint config commits.

**GREEN.** `MembershipChanger::swap_dc` (new method): query Accord coordinator pool for in-flight txns; for each that references a leaving DC's voter, wait for completion or abort; then issue the joint `change_membership(AddVoters + RemoveVoters)`.

**REFACTOR.** Bound the wait at 60s; on timeout, fail the swap and document.

### W7.9: TLA+ multi-DC extension

**RED.** Extend `specs/tla/raft.tla` to multi-group with Accord-like cross-group ordering. Apalache-check at N=2 DCs × 3 voters; max_term=5; max_log=15. New invariants: `CrossDcAtomicity`, `WatermarkMonotonicity`.

**GREEN.** Iterate the spec.

**REFACTOR.** None.

### W7.10: Sim test — bank workload at QUORUM under dc-partition

**RED.** Test `bank_at_quorum_under_dc_partition_holds_invariant`: Sprint 5 sim; 3+3 dual-DC; bank workload at QUORUM = 4 (3-of-3 in DC1 + 1-of-3 in DC2 minimum); inject `dc-partition` for 30 simulated seconds; assert balance-conservation invariant holds across the partition heal.

**GREEN.** With W7.1–W7.8 in place, this should pass. Debug if not.

**REFACTOR.** None.

### W7.11: 1h Jepsen integration test

**RED.** Add `tier-multi-dc` Jepsen tier: T3 topology, bank workload at QUORUM, `dc-partition + dc-slow` composed nemesis, 1h duration. Currently fails — tier doesn't exist.

**GREEN.** Add the tier. Run in nightly CI.

**REFACTOR.** None.

## Acceptance criteria

- [ ] `state_machine_tracks_hlc_watermark` (W7.1).
- [ ] `apply_buffers_out_of_order_accord_entries` (W7.2).
- [ ] `watermark_advances_with_max_skew_200ms`, `reorder_buffer_stalls_above_max_skew` (W7.3, W7.4).
- [ ] `accord_apply_idempotent` (W7.5).
- [ ] `accord_vote_commit_waits_for_apply` (W7.6).
- [ ] `cross_dc_write_uses_accord` (W7.7).
- [ ] `dc_swap_drains_accord` (W7.8).
- [ ] Apalache multi-DC check clean (W7.9).
- [ ] `bank_at_quorum_under_dc_partition_holds_invariant` (W7.10).
- [ ] 1h Jepsen tier passes (W7.11).

## Parallelization within Sprint 7

- **Track A (State machine ordering)**: W7.1, W7.2, W7.3, W7.4, W7.5, W7.6.
- **Track B (Coordinator + DC swap)**: W7.7, W7.8 — depends on A.
- **Track C (Verification)**: W7.9, W7.10, W7.11 — depends on A and B.

A 3-engineer team finishes in ~3 weeks.

## Risks

- **R1 — HLC implementation drift**: ferrosa already has HLC for Accord; verify it's the same one used by the reorder buffer.
- **R2 — Accord recovery interacts with Raft membership change**: W7.8 specifically. Mitigation: TLA+ models this; fix any spec violations.
- **R3 — 1h Jepsen runs are slow**: nightly only. Mitigation: faster `tier-multi-dc-smoke` for PR-time signal.

## Completion checklist

- [ ] Branch + PR.
- [ ] CI green; nightly multi-DC tier passing for at least 3 nights.

## Kickoff prompt for an agent

> Sprint 7. Spec at `specs/in-process/sprint-07-multi-dc-accord.md`. Hard dependencies: Sprints 5 + 6 merged.
>
> Wire Accord cross-DC writes onto Sprint 6's per-DC Raft scaffolding. Reorder-by-timestamp + idempotence + apply-durability barrier + DC-swap drain.
>
> Companion reading: ADR-015, Cassandra CEP-15 (Accord protocol), `specs/raft-failure-mode-matrix.md` §9.
>
> Worktree at `/home/bkearns/src/ferrosa-suite/sprint-07/`. Strict TDD. Bank workload + balance invariant under dc-partition is the headline acceptance test.
