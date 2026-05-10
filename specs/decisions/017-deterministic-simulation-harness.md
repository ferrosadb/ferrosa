# ADR-017: Deterministic Simulation Harness

> Date: 2026-05-09
> Status: Proposed
> Companion to: ADR-016 (Verification stack)

## Context

Jepsen runs slowly (seconds per test) and is non-deterministic by nature (real network, real clocks). Bugs that require specific timing or partial-failure orderings are hard to catch and harder to reproduce. TigerBeetle, FoundationDB, and resilience.io demonstrate the alternative: a single-threaded, seeded discrete-event simulator that runs the whole system in process at thousands of seeds per minute, with full time-travel and exact replay.

## Decision

Build a deterministic simulator for ferrosa-cluster's Raft layer. Single-threaded execution, seeded RNG, time-control, full nemesis matrix. Targets 10K seeds/min baseline.

### Architecture

New crate `ferrosa-sim`:

- An `Async` runtime trait that ferrosa code can be compiled against — the simulator implements it as a discrete-event loop. Production uses tokio. (Madsim achieves this via a feature flag; we either depend on Madsim or write a smaller equivalent.)
- A `SimulatedCluster` builder that produces `N` `SimulatedNode` instances, each with simulated disk + network.
- A `Nemesis` trait for fault injection: partition, kill, slow-network, clock-skew, disk-fail. Same shape as Jepsen's nemesis registry, but synchronous and time-travelable.
- A `Workload` trait for client behaviour. Reuses Jepsen workload traits where possible.
- A `Checker` trait — invariant assertions that run after every event.
- Seed: a `u64` that fully determines execution. Same seed = same trace.

### Time control

The simulator owns a `SimulatedTime` that advances on event firing. `tokio::time::sleep` is rerouted to enqueue an event at `now + duration`. No wall-clock time is read.

### Topology nemesis matrix

The full cross-product:

```
{topology: [3, 5, 3+3, 3+3+3]}
× {workload: [register, bank, lwt-{1..16}, membership-churn, forward-probe, late-join-flood]}
× {nemesis: [partition-halves, partition-ring, partition-one, kill-minority, kill-majority,
             pause-node, clock-skew, slow-network, packet-loss, disk-slow, disk-fail,
             add-node, decommission, learner-promote, re-ip, random-startup-order,
             dc-partition, dc-slow, dc-flap]}
× {seed: 0..N}
```

Sprint 5 implements the framework + topology = 3 + workloads register/bank + the 5 most important nemeses. Sprint subsequent expands.

### TLA+ refinement check

Each simulator transition is tagged with the corresponding TLA+ action name (e.g., `BecomeFollower`, `AppendEntries`, `JoinNodeApply`). A property-based test over many seeds asserts that every observed transition is permitted by the TLA+ spec at `specs/tla/raft.tla`. This bridges design and implementation: a simulator failure that is *not* a TLA+-permitted transition is an implementation bug; one that *is* permitted is a TLA+ bug.

### CI integration

Three modes:

- `cargo test --features sim` — fast (1 K seeds, <2 min) on every PR.
- Nightly `--seeds 100000 --workers 16` — heavyweight (~30 min) on a separate workflow.
- `--seed N --replay` for debugging a specific failure.

### Integration with existing tests

Sprint 5 ports the bootstrap-task decomposition tests from Sprint 4 to run under simulation. Subsequent sprints port their tests as they land.

## Rationale

The bug genome has multiple bugs that would have been caught instantly by a deterministic sim:

- Concurrent late-joins race (bug commit `74a33ff3` — RC-3 seed determination): a concurrent test under sim catches it on seed 0.
- Mutex-across-await deadlock (`9fa74ed4`): Loom catches it; sim catches it too on workloads with concurrent sends.
- Membership drift (`5256ff10` and the P0-21 saga): post-step assertions catch every variant.
- Bootstrap-task silent error (`44a7e6bb`): sim's "no silent return" check fires on the first instance.

Per TigerBeetle's experience: 10K seeds/min × 24 h = 14.4M scenarios per night. State-space rare bugs become routine.

## Consequences

### Positive

- Reproducibility: every test failure is replayable by seed.
- Fast iteration: developer can run sim seeds locally faster than Jepsen.
- Regression-resistant: a fixed bug is encoded as a passing seed; subsequent regressions fail it again.
- Doesn't require infra (Docker, Firecracker, SSH).

### Negative

- Build effort: 1 large sprint to get the framework working. Maintenance: ongoing.
- Madsim or in-house? Madsim is mature but adds a dep; in-house is more code but tailored. **Decision: try Madsim first; fork or replace if it doesn't fit.**
- Code paths that read wall-clock or use `std::thread::spawn` outside tokio must be eliminated or adapted.

### Neutral

- The simulator's runtime trait must be adopted by ferrosa-cluster (and ferrosa-net, since cluster uses it). This is a non-trivial refactor in itself, but it is mostly one-line changes from `tokio::spawn` to a trait method.

## Open questions

1. **Madsim vs in-house.** Madsim has known limitations (some libraries don't compose). In-house is more work but more control. Sprint 5 starts with Madsim; if friction is high, fork.
2. **Disk simulation depth.** Full block-level fidelity (FoundationDB style) or coarser (only fsync ordering)? **Default: coarser.** Coarse is sufficient to catch the recovery saga bugs; full fidelity is Sprint 9+ if needed.
3. **Network packet drop granularity.** Per-message (drop-individual-packets) or per-link-period (drop-all-during-window)? **Default: per-message.** More general; Jepsen's tc/netem is per-link-period and that's a known limitation.

## Acceptance criteria (Sprint 5)

- New crate `ferrosa-sim` (or `ferrosa-jepsen` feature-gated).
- 3-node cluster bring-up runs under sim; converges to a leader within bounded simulated time.
- Reproducibility: same seed produces same trace; assertable.
- 10K seeds/min throughput on a developer laptop.
- All Sprint 4 transition tests run under sim.
- Apalache TLA+ refinement check: 100% of observed simulator transitions are permitted by the spec.
- Nightly CI workflow runs `--seeds 100000`; failures filed automatically.

## References

- TigerBeetle simulator (https://tigerbeetle.com).
- FoundationDB simulation testing (Aleksey Charapko, "Why FoundationDB" series).
- Madsim (https://github.com/madsim-rs/madsim).
- `specs/raft-correctness-plan.md` Sprint 5.
- ADR-016.
