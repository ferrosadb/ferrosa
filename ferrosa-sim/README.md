# ferrosa-sim

> Deterministic, single-threaded discrete-event simulator for the Ferrosa Raft
> layer, plus a Rust-side TLA+ refinement check. A seed in, a checkable trace out.
> Sprint 5 (ADR-016, ADR-017).

## What this crate is

A small, **dependency-free** testing crate — it depends on *no* other ferrosa
crate (only `serde` + `tracing`). It models the *protocol-level* state of the
Raft layer — terms, votes, roles, election timeouts, heartbeats, PreVote,
joint-consensus membership — and a separate multi-DC Accord-apply bank workload,
all under a seeded integer clock. The same seed always produces the same
`Trace`, so a failing run is reproducible and its trace can be replayed against
the TLA+ spec.

It is an **in-house** simulator (decision **ADR-017**) rather than Madsim:
`ferrosa-cluster` pulls in openraft 0.9 + sled + a custom network stack that
would all need shimming, and Sprint 5's headline goal — a TLA+ refinement check
— needs a *protocol-level* model, not a full process simulator. This crate
models only the variables the spec at `specs/tla/raft.tla` cares about; the real
`FerrosRaft` (sled, networking, schema replay) is exercised by the in-process
harness in `ferrosa-cluster/tests/`, not here.

## Determinism contract

Two `SimulatedCluster` runs constructed with the **same seed** and the **same
voter count** produce **byte-identical traces**. The contract holds because:

1. **No wall-clock reads.** All time is `Tick`, an integer counter advanced by
   the event loop. No `std::time::Instant`, no `tokio::time::Instant`.
2. **Single-threaded event loop.** No `tokio::spawn`, no scheduling choice that
   depends on the OS scheduler.
3. **Seeded RNG.** `rng::SeededRng` is a splitmix64 generator owned by the
   `SimulatedCluster`. Election-timeout randomization is the only source of
   non-determinism, and it draws from this generator alone.
4. **Stable event ordering.** The event queue is a `BinaryHeap&lt;Scheduled&gt;`
   keyed by `(deadline, monotonic seq)`. Two events with the same deadline fire
   in insertion order — never in pointer-address or hash order.
5. **`BTreeMap`, never `HashMap`.** All per-node state and peer iteration uses
   ordered maps so the broadcast order of `RequestVote`s is independent of the
   build's `RandomState` seed.

The W5.4 test `same_seed_produces_same_trace` pins the contract in CI.

## What's implemented

- **Seeded PRNG** (`rng`) — a splitmix64; same seed = same stream, no global
  state, no allocation.
- **Protocol node + cluster** (`node`, `cluster`) — `SimulatedNode` (term, vote,
  role, log length, commit index) and `SimulatedCluster`, an N-voter
  discrete-event loop over `ElectionTimeout`, `RequestVote`/`Reply`,
  `PreVoteRequest`/`Reply`, `Heartbeat` events. Drivers: `run_until_leader`,
  `run_for`, `schedule_leader_heartbeat`.
- **Nemeses** (`nemesis`) — `PartitionHalves`, `KillMinority`, `AddNode`
  implementing an `apply`/`heal` trait, over the cluster mutators
  (`partition_pair`, `kill`/`revive`, `add_voter`/`remove_voter`).
- **PreVote** (`cluster::with_pre_vote`) — election timeouts run a
  non-term-bumping PreVote round before real candidacy, suppressing term
  inflation in a minority partition.
- **Joint-consensus membership** (`propose_membership`/`commit_membership`/
  `is_joint_quorum`) — a Cold+Cnew joint phase requiring a majority in *both* sets.
- **Bootstrap-phase model** (`bootstrap`) — an 8-phase pre/post-condition
  pipeline mirroring `ferrosa-cluster`'s `BootstrapPhase` enum, run against the
  sim cluster.
- **Trace + refinement** (`trace`, `refinement`, `spec`) — every transition is
  recorded as a named `TlaAction`; `check_trace` replays it through a Rust
  interpreter of the spec, and `spec::check_all` runs the snapshot safety
  invariants (`ElectionSafety`, `LeaderAppendOnly`, `StateMachineSafety`, …).
- **Multi-DC bank workload** (`multi_dc`) — a *separate* model: two DCs with
  per-DC HLC-ordered reorder buffers + idempotent ledgers, a mocked Accord
  coordinator, a `dc-partition` nemesis, optional learner replicas, and bank
  balance-conservation / convergence invariants. This is the module
  `ferrosa-jepsen` consumes.

## Public API (key entry points)

| Area | Items |
|------|-------|
| RNG | `SeededRng::{new, next_u64, gen_range}` |
| Node | `SimulatedNode`, `NodeId`, `Role` |
| Cluster | `SimulatedCluster::{with_voters, with_pre_vote, run_until_leader, run_for, leader, deployment_mode, partition_pair, kill, revive, add_voter, propose_membership, commit_membership, is_joint_quorum, trace}` |
| Nemesis | `Nemesis` trait, `PartitionHalves`, `KillMinority`, `AddNode` |
| Bootstrap | `BootstrapPhase`, `BootstrapState`, `run_phase`, `run_full_bootstrap` |
| Trace | `Trace`, `TraceEntry`, `TlaAction` |
| Refinement | `check_trace`, `check_step`, `AbstractState`, `RefinementError` |
| Invariants | `spec::{election_safety, leader_append_only, state_machine_safety, check_all, InvariantResult}` |
| Multi-DC | `DualDcBankSim`, `DcApplyState`, `AccordCoord`, `Transfer`, `AccordEntry` |

## Dependencies

**Calls** (ferrosa crates this depends on): **none.** This crate deliberately
depends on *no* other ferrosa crate — the whole point of ADR-017's in-house
choice. Where it needs a shape from `ferrosa-cluster` (e.g. `DeploymentMode`, the
bootstrap phases, the multi-DC apply types) it **mirrors** the enum/struct by
hand and keeps it in lock-step manually. External deps: `serde` (trace
serialization), `tracing`, and `proptest` (dev-only).

**Called by** (crates that depend on this):

- **`ferrosa-jepsen`** — `ferrosa-jepsen/src/endurance_sim.rs` consumes
  `ferrosa_sim::multi_dc::DualDcBankSim` for its endurance workload.

## Tests

39 in-crate `#[test]`s across the modules, plus seed-sweep loops
(`refinement_holds_across_1000_seeds`, `sim_full_bootstrap_seed_sweep`,
multi-DC long-horizon / endurance). No legitimately ignored tests. The
`seed_throughput` example (`cargo run --release --example seed_throughput -p
ferrosa-sim -- <n>`) reports seeds/min. The `spec`/`refinement` tests assert the
external files `specs/tla/raft.tla` and `.github/workflows/nightly-sim.yml`
exist — so removing either breaks `cargo test`.

## Why not Madsim?

ADR-017 grants the implementer authority to fall back from Madsim if integration
friction is too high. Sprint 5 elects the in-house fallback up-front: the
headline goal is the TLA+ refinement check, which requires a protocol-level
simulator, not a full process simulator over openraft + sled. Madsim adoption
remains an option for a future sprint if the in-process integration tests in
`ferrosa-cluster/tests/` grow hard-to-reproduce flakes.

## Specs

- [Architecture overview](specs/overview.md) — module map, the event loop, invariants, what's modelled vs. not
- [Roadmap](specs/roadmap.md) — Now / Next / Later
