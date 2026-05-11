---
type: sprint
status: pending
priority: P1
created: 2026-05-09
sprint: 5
wave: 3
---

# Sprint 5: Deterministic simulation harness + TLA+ skeleton

> Branch: `sprint-05-sim-tla`.
> Companion to: ADR-016 (verification stack), ADR-017 (sim harness).

## Goal

Build a Madsim-based deterministic simulator that runs the bootstrap-task transition tests at 10K seeds/min. Write a TLA+ spec covering PreVote + AppendEntries + joint-consensus membership + snapshot install. Apalache-check at bounded sizes. Add a refinement check that observed simulator transitions are permitted by the spec.

## Hard dependencies

- **Sprint 4 merged**: bootstrap phases are typed; transition tests exist as integration tests to port to sim.

## Pre-flight checks

```sh
cd /home/bkearns/src/ferrosa-suite/ferrosa
git checkout main && git pull
git log --grep "Sprint 4" --oneline | head    # verify dependency
cargo test --workspace --lib && cargo clippy && cargo fmt --check
git checkout -b sprint-05-sim-tla
which apalache || go install github.com/apalache-mc/apalache@latest    # Apalache toolchain
```

## TDD work items

### W5.1: New crate `ferrosa-sim`

**RED.** Test `sim_crate_compiles_and_runs_empty_test`: `cargo test -p ferrosa-sim` returns "no tests"; the crate compiles. Currently the crate doesn't exist.

**GREEN.** `cargo new --lib ferrosa-sim`. Add to workspace. Empty `lib.rs`.

**REFACTOR.** Add `Cargo.toml` deps: `madsim`, `tokio` (the madsim shim), `tracing`.

### W5.2: Madsim integration — single node bring-up

**RED.** Test `madsim_runs_single_node`: under `madsim::test`, spawn a `SimulatedNode` and assert it reaches `DeploymentMode::Standalone`. Currently fails — `SimulatedNode` doesn't exist.

**GREEN.** Build a `SimulatedNode` wrapping `ferrosa-cluster::ModeController` with all dependencies fed through madsim's runtime. This requires ferrosa-cluster code to be runtime-trait-generic; if it isn't, this work item exposes the refactor scope.

**REFACTOR.** Pull common test-cluster setup into `ferrosa-sim/src/cluster.rs`.

### W5.3: 3-node bring-up under sim

**RED.** Test `madsim_runs_3_node_to_cluster`: spawn 3 `SimulatedNode`s; connect them via simulated network; assert all reach `DeploymentMode::Cluster` and one is leader.

**GREEN.** Implement simulated network in `ferrosa-sim/src/network.rs`. Madsim provides this; we wrap it.

**REFACTOR.** None.

### W5.4: Reproducibility — same seed, same trace

**RED.** Test `same_seed_produces_same_trace`: run the W5.3 test with seed 42 twice; collect transition logs; assert identical event sequences.

**GREEN.** Configure madsim's seeded RNG + time source. Verify ferrosa code does not read wall-clock or use unseeded `rand`. If it does, refactor to use injected sources.

**REFACTOR.** Document the determinism contract in `ferrosa-sim/README.md`.

### W5.5: Topology nemeses under sim

**RED.** Three tests:
- `sim_nemesis_partition_halves`
- `sim_nemesis_kill_minority`
- `sim_nemesis_add_node`

Each uses a Madsim `Nemesis` to perturb the running 3-node cluster, then asserts the cluster recovers within a bounded simulated time. Currently fails — nemeses don't exist.

**GREEN.** Implement the three nemeses in `ferrosa-sim/src/nemesis/`. Use madsim's `NetSim` for partitions, process spawn/kill for `kill-minority`, container-spawn analogue for `add-node`.

**REFACTOR.** Extract a `Nemesis` trait shared with Sprint 2's Jepsen nemesis registry where possible.

### W5.6: Port Sprint 4 transition tests to sim

**RED.** For each of the 8 bootstrap phases (W4.2–W4.9), port the integration test to run under `ferrosa-sim`. Tests run at 10K seeds/min.

**GREEN.** Implement test plumbing. Each test takes a `seed: u64` parameter; run via `cargo test --features sim -- --test-threads=16`.

**REFACTOR.** Common `simulated_cluster_with_seed` helper.

### W5.7: TLA+ spec — Raft skeleton

**RED.** Write `specs/tla/raft.tla` with safety invariants (Election Safety, Log Matching, Leader Completeness, State Machine Safety) and operators for AppendEntries, RequestVote. No PreVote, no membership change yet — that's W5.8/W5.9.

**GREEN (= Apalache check)**. Run `apalache check --inv=ElectionSafety raft.tla`. Check at bounded N=3, max_term=5, max_log=10. Initially fails if there's a typo in the spec; iterate until clean.

**REFACTOR.** None.

### W5.8: TLA+ spec — PreVote

**RED.** Add PreVote action to `raft.tla`. New invariant: `NoTermAdvanceWithoutPreVoteMajority`.

**GREEN (= Apalache check)**. Iterate the spec until Apalache reports no violations.

**REFACTOR.** Pull the election-restriction predicate into a shared operator.

### W5.9: TLA+ spec — joint-consensus membership

**RED.** Add `ChangeMembership` action implementing the joint-consensus protocol from Ongaro §4.3. New invariant: `JointConsensusSafety`.

**GREEN.** Apalache check at N=3 → 5 voter swap. Iterate.

**REFACTOR.** None.

### W5.10: TLA+ refinement check

**RED.** Test `every_sim_transition_is_tla_permitted`: a property test runs the sim across 1000 seeds, captures every observed engine transition tagged with a TLA+ action name, and verifies each is a valid step of the spec. Currently fails — no tagging mechanism.

**GREEN.** Add `TlaTrace` events to ferrosa-cluster's engine, emitted on each state transition. The sim collects them. A separate verifier (`ferrosa-sim/src/refinement.rs`) checks each trace step is a valid TLA+ step using a small interpreter or by exporting traces to TLC.

**REFACTOR.** Production builds compile out the trace events via a `cfg`.

### W5.11: Nightly CI workflow `sim-100k`

**RED.** Workflow `nightly-sim.yml` runs `cargo test -p ferrosa-sim --release -- --seeds 100000 --workers 16`. Initially fails — workflow doesn't exist.

**GREEN.** Add the workflow. Schedule: nightly. Failure auto-files a GitHub issue with the seed.

**REFACTOR.** Cache build artifacts between runs.

## Acceptance criteria

- [ ] `ferrosa-sim` crate exists and compiles (W5.1).
- [ ] `madsim_runs_3_node_to_cluster` (W5.3).
- [ ] `same_seed_produces_same_trace` (W5.4).
- [ ] Three nemesis tests (W5.5).
- [ ] All 8 Sprint 4 transition tests run under sim (W5.6).
- [ ] `apalache check --inv=ElectionSafety raft.tla` clean (W5.7).
- [ ] PreVote + joint-consensus invariants hold under Apalache (W5.8, W5.9).
- [ ] `every_sim_transition_is_tla_permitted` passes for 1000 seeds (W5.10).
- [ ] Nightly workflow `sim-100k` runs and reports.

## Parallelization within Sprint 5

- **Track A (Sim infra)**: W5.1, W5.2, W5.3, W5.4, W5.5 — serialize.
- **Track B (TLA+)**: W5.7, W5.8, W5.9 — independent until W5.10.
- **Track C (Tests + CI)**: W5.6, W5.10, W5.11 — depends on A and B.

A 2-engineer team finishes in ~2 weeks.

## Risks

- **R1 — Madsim doesn't compose with ferrosa's code**: ferrosa may use APIs Madsim doesn't shim. Mitigation: W5.2 surfaces this immediately. If serious, fall back to in-house single-threaded event loop.
- **R2 — Apalache state explosion**: bounded sizes may still be too large. Mitigation: tighter bounds; add symmetry reduction.
- **R3 — Wall-clock reads in ferrosa break determinism**: mitigation: audit and inject. Sprint 1 already moved most things to `parking_lot`/proper async.

## Completion checklist

- [ ] Branch + PR.
- [ ] CI green; nightly workflow passing for at least 3 nights.

## Kickoff prompt for an agent

> Sprint 5. Spec at `specs/in-process/sprint-05-sim-tla.md`. Hard dependency: Sprint 4 merged.
>
> Two parallel tracks: (A) Madsim sim harness, (B) TLA+ spec + Apalache. Final integration via refinement check.
>
> Companion reading: ADR-016, ADR-017, Ongaro dissertation §4.3 / §9.6 for the spec details.
>
> Worktree at `/home/bkearns/src/ferrosa-suite/sprint-05/` off main. Strict TDD. CI green every commit.
>
> If Madsim doesn't compose with ferrosa code (W5.2 surfaces this), document and fall back to a smaller in-house simulator — do not block the rest of the sprint.
