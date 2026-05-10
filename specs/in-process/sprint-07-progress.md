---
type: sprint-progress
status: in-progress
priority: P1
sprint: 7
wave: 4
created: 2026-05-10
---

# Sprint 7 Progress: Multi-DC Accord cross-DC adapter

> Branch: `sprint-07-multi-dc-accord` (off `feature/raft-gap-close`).
> Spec: `specs/in-process/sprint-07-multi-dc-accord.md`.

## Session 1 — 2026-05-10 — Sprint 7 implementation

Pre-flight: Sprints 1-6 merged (per orchestrator). Branch already created off
`feature/raft-gap-close`. Baseline clean: `cargo test --workspace --lib`,
`cargo clippy --all-targets -- -D warnings`, `cargo fmt --check` all pass.
CI gates `no-let-underscore-raft.sh` + `no-raw-client-write.sh` clean.

### Per-WI status

| WI    | Status | Commits          | Notes |
|-------|--------|------------------|-------|
| W7.1  | done   | `7858f411`       | New `RaftOp::AccordApply { txn_id, hlc, mutation }`. New `multi_dc_apply` module: `ReorderBuffer`, `AppliedTxnLedger`, `watermark_for`, `max_skew_from_env`. RaftState gains `hlc_watermark`, `max_observed_skew_us`, `applied_accord_txns`, `accord_apply_buffer` (all serde-defaulted). Test: `state_machine_tracks_hlc_watermark`. AccordTimestamp gained `Default` derive. |
| W7.2  | done   | `13e07ffd`       | Integration test `apply_buffers_out_of_order_accord_entries` — feeds two AccordApply entries in reverse HLC order, asserts ascending drain order. Buffer logic was already in W7.1; this commit nails down the contract end-to-end. |
| W7.3  | done   | `4dabd55a`       | `watermark_advances_with_max_skew_200ms` integration test. `FERROSA_HLC_MAX_SKEW_MS` env var read by `max_skew_from_env`. Default 200 ms per ADR-015. |
| W7.4  | done   | `4dabd55a`       | `reorder_buffer_stalls_above_max_skew` — entry 500ms in the future stalls; pushing past `REORDER_BUFFER_ALARM_DEPTH` (100) fires the over-threshold gauge. |
| W7.5  | done   | `4dabd55a`       | `accord_apply_idempotent` — replay of same TxnId is NoOp; ledger size unchanged; watermark monotonic. `AppliedTxnLedger::gc_older_than` for memory bound. I-28 closed. |
| W7.6  | done   | `85ec5ba0`       | `MembershipChanger::accord_vote_commit(txn_id, hlc, mutation)` — submits AccordApply via `client_write` then waits on `raft.wait().applied_index_at_least(commit_index)`. Test `accord_vote_commit_waits_for_apply` on a 3-voter cluster. I-30 apply-durability gap closed. |
| W7.7  | done   | `3a17dda5`       | `route_for_cl(QUORUM, 2)` returns new `CLRoute::CrossDcAccord` (replaces Sprint 6 `NotImplemented` for QUORUM/EACH_QUORUM/ALL). New `accord::cross_dc_adapter::CrossDcAccordAdapter` wraps a per-DC `MembershipChanger`. Metric `CROSS_DC_VOTE_COMMITS` increments per dispatch. Test `cross_dc_write_uses_accord` asserts both. SERIAL (cross-DC LWT) deferred to Sprint 8. |
| W7.8  | done   | `c7e59c26`       | `MembershipChanger::swap_dc(leaving_voters, drain, deadline, poll_interval)` returns `SwapDcOutcome::Drained{iterations}` or `TimedOut{remaining}`. New `AccordDrainQuery` trait abstracts the production wiring. Two tests: completion within deadline + timeout case. Sprint 8 wires the trait to the real Accord coordinator pool. |
| W7.9  | done   | `f34056d4`       | `specs/tla/multi-dc.tla` + `specs/tla/multi-dc.cfg`. Models per-DC Raft groups + cross-DC Accord layer. Invariants: `WatermarkBounded`, `NoMixedCommitAbort`, `AccordIdempotence`, `SwapDcDrainsAccord`, aggregate `MultiDcSafety`. Apalache not installed in agent env (Sprint 5 documented this); operator replay commands documented in the .cfg file. |
| W7.10 | done   | `41d176a5`       | **Headline**. New `ferrosa-sim::multi_dc` module — `DcApplyState` mirrors `multi_dc_apply` 1:1 (reorder buffer, idempotent ledger, monotonic watermark). `DualDcBankSim` composes two states + mocked `AccordCoord`. **`bank_at_quorum_under_dc_partition_holds_invariant`** — 1000 transfer ticks, dc-partition between tick 200-400 (the "30 simulated seconds"), per-DC balance conservation holds at every step + cross-DC convergence post-heal. Plus `bank_invariant_holds_over_long_horizon` (30K ticks). |
| W7.11 | done   | `4af74fac`       | New `Tier::MultiDc` resolves to T3 + medium concurrency + 3600s. New `composed::dc_partition_and_slow()` nemesis registered as `dc-partition+dc-slow` in `NemesisRegistry::full()`. `Tier::MultiDc` routes to `NemesisRegistry::full()` in `orchestrator::resolve_nemesis_registry`. Tests: `tier_multi_dc_resolves_to_t3`, `dc_partition_plus_dc_slow_nemesis_registered`, `tier_multi_dc_one_hour_bank_workload` (panics with setup instructions per CLAUDE.md test policy). New nightly workflow `.github/workflows/jepsen-multi-dc-nightly.yml`. |

### Acceptance criteria

- [x] `state_machine_tracks_hlc_watermark` (W7.1)
- [x] `apply_buffers_out_of_order_accord_entries` (W7.2)
- [x] `watermark_advances_with_max_skew_200ms`, `reorder_buffer_stalls_above_max_skew` (W7.3, W7.4)
- [x] `accord_apply_idempotent` (W7.5)
- [x] `accord_vote_commit_waits_for_apply` (W7.6)
- [x] `cross_dc_write_uses_accord` (W7.7)
- [x] `dc_swap_drains_accord_completion` + `dc_swap_drains_accord_timeout` (W7.8)
- [x] Multi-DC TLA+ extension at `specs/tla/multi-dc.tla` (W7.9). Apalache check is operator follow-up (binary not in agent env; Sprint 5 documented this).
- [x] **`bank_at_quorum_under_dc_partition_holds_invariant`** — headline acceptance test (W7.10) passes.
- [x] `tier-multi-dc` Jepsen tier registered + config tests pass; live 1h run is the nightly CI job (W7.11).

### Hygiene

- `cargo test --workspace --lib` — all crates' lib tests pass except the 8 pre-existing infrastructure-gated `ferrosa-jepsen` panics (no FERROSA_TEST_CONTAINERS / FERROSA_TEST_FIRECRACKER), which match the baseline.
- `cargo clippy --all-targets -- -D warnings` — clean (only pre-existing openraft fork warnings, none from Sprint 7 code).
- `cargo fmt --check` — clean.
- `scripts/ci-gates/no-let-underscore-raft.sh` — clean.
- `scripts/ci-gates/no-raw-client-write.sh` — clean.
- No `#[ignore]` introduced.

### Final commit count

10 sprint commits on `sprint-07-multi-dc-accord`:

1. `7858f411` — feat(raft): W7.1 — HLC watermark + max-skew tracking
2. `13e07ffd` — test(raft): W7.2 — apply_buffers_out_of_order_accord_entries
3. `4dabd55a` — test(raft): W7.3-W7.5 — bounded skew, stall, idempotent apply
4. `85ec5ba0` — feat(membership): W7.6 — accord_vote_commit apply-durability barrier
5. `3a17dda5` — feat(coordinator): W7.7 — cross-DC writes route through Accord
6. `c7e59c26` — feat(membership): W7.8 — swap_dc drains in-flight Accord txns
7. `f34056d4` — docs(tla): W7.9 — multi-DC TLA+ extension
8. `41d176a5` — feat(sim): W7.10 — bank workload at QUORUM under dc-partition
9. `4af74fac` — feat(jepsen): W7.11 — tier-multi-dc 1h Jepsen tier
10. (this commit) — docs(sprint-07): finalize progress log

### Headline outcome (W7.10)

**PASS.** `bank_at_quorum_under_dc_partition_holds_invariant` runs 1000
transfer ticks across two DCs, injects a dc-partition for 200 ticks
mid-run, and asserts the per-DC balance-conservation invariant holds
at every step + cross-DC convergence after the heal. The longer
horizon (`bank_invariant_holds_over_long_horizon`, 30K ticks)
likewise passes. Both run in < 200 ms.

### Stuck criteria

None invoked. The Accord coordinator scaffolding at
`ferrosa-cluster/src/accord/` was sufficient for W7.7's metric-trace
contract (the full Accord pre-accept / recovery state machine for
cross-DC writes is the existing `accord/coordinator.rs` plumbing —
this sprint added the *adapter* that translates a coordinator
decision into a durable per-DC apply via W7.6). W7.8's drain query
trait abstracts the wiring to that pool; Sprint 8 will replace the
stub `AccordDrainQuery` impl with a live one.

