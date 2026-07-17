# ferrosa-jepsen

> Jepsen-style distributed correctness harness for Ferrosa: generate
> concurrent workloads, inject faults (nemeses), record a history, and check
> it for linearizability and membership-invariant violations.

## What this crate is

`ferrosa-jepsen` is a **test-harness crate** (library + `ferrosa-jepsen` binary),
not part of the shipped database. It drives a real or simulated Ferrosa cluster
through workload + fault-injection combinations, records an operation history,
and runs correctness checkers over it. It is the home of the `tier-*` runs the
project uses as correctness evidence.

Nothing in the workspace depends on this crate — it is a leaf in the dependency
graph. It calls `ferrosa-sim` for the simulator-backed endurance path.

## What's implemented

- **Orchestrator** (`orchestrator.rs`) — the run loop: for each topology it
  provisions a cluster (Docker, see below), then iterates
  topology × concurrency × driver × nemesis × workload, runs each combination,
  records history, checks linearizability + workload invariants, and emits a
  `RunReport` (JSON + HTML).
- **Tiers / config** (`config.rs`) — `Tier` (`Smoke`, `Standard`, `Full`,
  `Endurance`, `MultiDc`), `Topology` (T1 3-node, T2 5-node, T3 3+3 dual-DC,
  T4 3+3+3 tri-DC), `Concurrency` (Low/Medium/High). Each tier resolves to a
  topology set, concurrency set, and run duration.
- **Workloads** (`workload/`) — `register` (single-value register),
  `bank` (conservation-of-money), 16 `lwt-*` LWT patterns, plus the Sprint 2
  membership/forwarding workloads `forward-probe`, `membership-churn`,
  `late-join-flood`. 21 workloads in the `phase1` registry.
- **Nemeses / fault injection** (`chaos/`) — `NemesisAction` trait
  (`inject`/`heal`). Network (partition halves/ring/one, slow, jitter, packet
  loss — via `iptables`/`tc` over SSH), process (kill minority/majority, pause),
  clock (skew small/large, strobe), disk (slow/fail), WAN/cross-DC (`wan_bridge`),
  and `composed` multi-fault nemeses. Registries: `phase1` (7), `phase2` (18),
  `full` (25+ incl. WAN + composed).
- **Checkers** (`checker/`):
  - **Linearizability** — a native Rust WGL-style backtracking checker
    (`check_linearizability`) with a register model (read/write/CAS/serial-read),
    a 100k-node search bound, and minimal counterexamples. This is the checker
    that actually runs in unit tests and CI.
  - **Membership invariants** (`checker/membership.rs`) — structural checks
    I-06–I-10, I-13 over per-node membership snapshots.
  - **Knossos** (`checker/knossos.rs`) — shells out to Clojure/`lein`; only runs
    when a `jepsen_dir` *and* a written history file are both supplied.
  - **Elle** (`checker/elle.rs`) — types exist; `UnifiedChecker` always sets the
    Elle result to `None` (not wired to a running checker).
- **Drivers** (`driver/`) — `rust_driver` connects to a real cluster via the
  `scylla` driver (`phase1`, the only wired driver). `ContainerDriver` shells out
  to per-language Docker images (Python/Go/Node/Java/C#) — `phase2`, not used by
  the default tier resolution.
- **Cluster backends**:
  - **Docker Compose / external contacts** (`docker_provision.rs`) — the
    orchestrator provisions a 3-node cluster when `FERROSA_TEST_CONTAINERS` is
    set. A caller can instead provide `FERROSA_TEST_CLUSTER_NODES` (including
    six T3 endpoints); those endpoints are always dialed as a real CQL cluster
    and are never torn down by the driver. The multi-DC tier rejects a missing
    live cluster rather than silently using `MockCqlSession`. Uses
    `container_runtime()` (docker→podman).
  - **Firecracker** (`firecracker.rs`, `cluster.rs`) — microVM provisioning
    primitives exist but are **not wired** into the orchestrator/run path;
    `cluster.rs` is the only consumer and is itself unreferenced.
  - **Fly.io** (`flyio.rs`, `chaos/`) — machine-management primitives and a
    Fly-SSH nemesis transport for T3/T4. Set `FERROSA_JEPSEN_FLY_APP` plus
    ordered `FERROSA_JEPSEN_FLY_MACHINE_IDS` to execute WAN chaos on real Fly
    machines; the same WAN actions use `container_runtime exec` for the local
    T3 compose topology.
- **Endurance sim** (`endurance_sim.rs`) — the **sim-equivalent endurance run**
  (W8.9, ADR-016). Drives `ferrosa_sim::multi_dc::DualDcBankSim` (voters +
  per-DC learners) over a 24-simulated-hour horizon with periodic rolling-window
  conservation + learner-divergence checks. This is the **headline acceptance
  gate** used when Fly.io is unavailable, and it runs under default `cargo test`.
- **History** (`history.rs`) — `Operation`/`Op`/`OpResult`, JSONL serialization,
  per-key filtering. **CLI** (`main.rs`) — `run`, `report`, `tier-endurance-sim`.

## CLI

```bash
ferrosa-jepsen run --tier smoke                 # run a tier
ferrosa-jepsen run --tier multi-dc --output json
ferrosa-jepsen tier-endurance-sim --smoke       # sim endurance (<1s)
ferrosa-jepsen report list                       # (report subcommands are stubs)
```

## Tests

- **232** in-crate unit tests (checker correctness, registries, config
  resolution, the endurance-sim smoke + tri-DC headline runs, etc.) — these run
  under default `cargo test -p ferrosa-jepsen`.
- **30** integration test functions across `tests/`. The live-infra ones
  (Docker mini-Jepsen, smoke tier, topology invariants, nemesis correctness)
  are gated behind the `live-infra-tests` feature and `panic!` with setup
  instructions when their `FERROSA_TEST_*` prerequisite is absent —
  `live_infra_contract.rs` pins that contract. Zero `#[ignore]`.

Local live-infra form:

```bash
FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-jepsen \
  --features live-infra-tests --test docker_mini_jepsen -- --nocapture
```

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-sim`** — `multi_dc::DualDcBankSim`, the deterministic dual-DC bank
  simulator that backs the sim-equivalent endurance run (`endurance_sim.rs`).

External: `tokio`, `scylla` (CQL driver), `russh` (SSH for fault injection),
`reqwest`, `clap`, `serde`/`serde_json`, `tracing`, `anyhow`.

**Called by** (crates that depend on this):

- **NONE** — this is a test-harness crate at the leaf of the dependency graph.

## Specs

- [Architecture overview](specs/overview.md) — module map, run pipeline, data flow
- [FMEA](specs/fmea.md) — live-cluster, driver-routing, and WAN-nemesis failure controls
- [Roadmap](specs/roadmap.md) — Now / Next / Later
