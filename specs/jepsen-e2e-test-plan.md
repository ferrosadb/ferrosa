# End-to-End Accord Transaction Verification (ferrosa-jepsen)

> Last updated: 2026-03-23
> Status: Approved
> Crate: `ferrosa-jepsen` (new, standalone)

## Overview

ferrosa-jepsen is a standalone Rust crate with Clojure Jepsen integration that provides end-to-end verification of Accord transaction linearizability from the CQL endpoint. It tests real multi-node clusters under real failure injection, validating that strict serializability holds across all 6 CQL drivers (Python, Go, Node.js, Java, C#, Rust), all LWT patterns, and all Jepsen failure modes.

## Architecture

Three-layer test architecture:

```
Layer 1: Orchestrator (Rust binary — "ferrosa-jepsen")
├── Provisions Firecracker VMs or Fly.io machines
├── Deploys ferrosa binaries + configures cluster
├── Runs Jepsen workloads via CQL drivers (all 6 languages)
├── Injects failures via chaos controller
├── Records operation history with nanosecond timestamps
└── Validates linearizability via Knossos/Elle

Layer 2: Chaos Plane
├── Docker chaos: pause/stop/disconnect containers
├── Firecracker chaos: kill/pause/snapshot-restore VMs
├── Network chaos: tc/netem on VM tap interfaces
│   ├── Per-link latency (uniform, normal, pareto distributions)
│   ├── Packet loss (random, burst, correlation)
│   ├── Jitter (random delay variance)
│   ├── Reordering (% of packets delivered out-of-order)
│   └── Duplication + corruption
├── WAN simulator service (between DC groups)
│   └── Dedicated Firecracker VM running tc/netem as inline bridge
├── Clock chaos: faketime injection per VM
└── Disk chaos: dm-flakey for fsync failures

Layer 3: Workload Generators (per driver)
├── Register workload: single-key read/write linearizability
├── Bank workload: multi-account transfer balance invariant
├── LWT workload: all 16 patterns under concurrent contention
├── Set workload: add/read elements, no lost updates
├── CAS-register: compare-and-swap retry loops
└── Queue workload: enqueue/dequeue ordering
```

## Topology Progression

Each phase runs the full workload + nemesis matrix before progressing to the next topology.

| Phase | Topology | Nodes | RF | Focus |
|-------|----------|-------|----|-------|
| T1 | 3-node single-DC | 3 Firecracker VMs | 3 | Basic quorum, all failure modes |
| T2 | 5-node single-DC | 5 Firecracker VMs | 5 | Fast/slow quorum paths |
| T3 | 3+3 dual-DC | 6 VMs + WAN bridge | 3 per DC | LOCAL_SERIAL vs SERIAL, DC partition |
| T4 | 3+3+3 tri-DC | 9 VMs + 3 WAN bridges | 3 per DC | Electorate reconfiguration, geo-failures |

## Nemesis Matrix

### Individual Nemeses

| Nemesis | Method | What it tests |
|---------|--------|---------------|
| `partition-halves` | iptables drop between two node groups | Majority/minority split-brain |
| `partition-ring` | Each node can only reach its neighbors | Partial connectivity, no clean majority |
| `partition-one` | Isolate single node from all others | Stale leaseholder, recovery coordinator activation |
| `kill-minority` | SIGKILL minority of nodes | Crash recovery, protocol log replay, sidecar reconstruction |
| `kill-majority` | SIGKILL majority of nodes | Cluster unavailability detection, no false commits |
| `pause-node` | SIGSTOP/SIGCONT (or Firecracker pause) | Long GC simulation, message reordering on resume |
| `clock-skew-small` | faketime +/- 100ms per node | Within SkewMax tolerance — should be transparent |
| `clock-skew-large` | faketime +/- 5s per node | Exceeds SkewMax — must reject PreAccept, not corrupt |
| `clock-strobe` | Rapid NTP-style clock jumps (forward then back) | HLC monotonicity under NTP step changes |
| `slow-network` | tc netem delay 50-500ms uniform | Timeout pressure, deadline-based message release |
| `jitter-network` | tc netem delay 10ms +/- 200ms normal distribution | Realistic WAN — message arrival reordering |
| `packet-loss` | tc netem loss 5-30% random | Retransmit pressure, duplicate detection |
| `packet-corrupt` | tc netem corrupt 1-5% | CRC validation, frame rejection |
| `packet-reorder` | tc netem reorder 25% gap 5ms | Out-of-order delivery, ReorderBuffer validation |
| `disk-slow` | dm-flakey with delay mode | Fsync-before-ack latency, timeout interactions |
| `disk-fail` | dm-flakey with drop writes | Persist-before-reply safety, crash recovery |

### WAN Bridge Nemeses (T3/T4 only)

| Nemesis | Method | What it tests |
|---------|--------|---------------|
| `dc-partition` | Drop all traffic on WAN bridge | Full DC isolation, LOCAL_SERIAL still works within DC |
| `dc-slow` | WAN bridge adds 50-200ms RTT | Cross-DC Accord round-trip under realistic latency |
| `dc-asymmetric` | One direction slow, other normal | Asymmetric partition healing, ballot convergence |
| `dc-flap` | WAN bridge toggles every 5-30s | Electorate reconfiguration under instability |
| `dc-lossy` | WAN bridge drops 10-40% packets randomly | Cross-DC recovery coordinator activation |

### Composed Nemeses

| Composition | What it validates |
|-------------|-------------------|
| `partition-halves` + `clock-skew-large` | No linearizability violation even with stale clocks in minority |
| `kill-minority` + `jitter-network` | Recovery under realistic network conditions |
| `pause-node` + `packet-loss` | Resume after pause with lost messages — state machine converges |
| `dc-partition` + `kill-minority` (within surviving DC) | Cascading failure — only 1-2 nodes left in each DC |
| All nemeses random | 60-second random schedule, each nemesis fires 2-5 times |

## LWT Workload Specification

Each of the 16 LWT patterns gets a dedicated workload that runs under every nemesis in every topology. Each workload has a correctness invariant that the linearizability checker validates.

| # | Pattern | Workload | Invariant |
|---|---------|----------|-----------|
| 1 | INSERT IF NOT EXISTS | 10 clients race to insert same PK | Exactly one gets `[applied]=true`, others get `false` + current values |
| 2 | INSERT IF NOT EXISTS + TTL | Insert with TTL=2s, wait for expiry, re-insert race | Second insert succeeds after TTL expiry, never during |
| 3 | UPDATE IF col = ? | CAS counter: read, increment, write IF old_value | Final value = number of successful CAS operations, no lost updates |
| 4 | UPDATE IF col1 = ? AND col2 = ? | Two-field CAS: version + status | Only valid (version, status) transitions succeed |
| 5 | UPDATE IF EXISTS | Conditional update on potentially-deleted row | Update fails on non-existent row, succeeds on existing |
| 6 | DELETE IF col = ? | Conditional delete of row only if status = 'pending' | Only 'pending' rows deleted, 'active' rows survive |
| 7 | DELETE IF EXISTS | Race to delete same row | Exactly one client succeeds, others get `[applied]=false` |
| 8 | BATCH mixed IF conditions | Batch: INSERT IF NOT EXISTS + UPDATE IF col = ? | Atomic — either all applied or none |
| 9 | BATCH multi-row same partition | 3 rows, each with own IF condition | All-or-nothing across clustering keys |
| 10 | LWT + counter columns | Conditional increment: IF counter > threshold | Counter monotonically increases, threshold respected |
| 11 | LWT + collection mutations | IF tags CONTAINS 'x', then UPDATE tags + {'y'} | Set grows monotonically, no lost additions |
| 12 | LWT + TTL interactions | CAS on row, concurrent TTL expiry | No phantom CAS success on expired row |
| 13 | LWT + static columns | CAS on static column, read from regular column | Static column CAS serialized across all rows in partition |
| 14 | LWT result set format | All patterns verify `[applied]` column + current values | Wire format matches Cassandra spec for every driver |
| 15 | SERIAL/LOCAL_SERIAL reads | Read at SERIAL without write, concurrent writes happening | SERIAL read sees latest committed write, never stale |
| 16 | BEGIN TRANSACTION multi-statement | Cross-partition transfer: debit + credit in one txn | Balance invariant across partitions |

### Driver Matrix

Each workload runs with all 6 drivers. The test harness starts one workload process per driver, all hitting the same cluster simultaneously. This validates that linearizability holds regardless of driver implementation differences (connection pooling, retry behavior, protocol version negotiation).

- Python (cassandra-driver)
- Go (gocql)
- Node.js (datastax driver)
- Java (java-driver)
- C# (CassandraCSharpDriver)
- Rust (cdrs-tokio)

### Concurrency Levels

| Level | Clients per driver | Total concurrent | Purpose |
|-------|-------------------|------------------|---------|
| Low | 2 | 12 | Baseline correctness |
| Medium | 10 | 60 | Contention on hot keys |
| High | 50 | 300 | Overload, backpressure, timeout handling |

## Linearizability Verification

Three independent verification methods, all must agree.

### Method 1: Jepsen + Knossos (strict linearizability)

- Real Clojure Jepsen framework running as the orchestrator for formal verification runs
- Each operation recorded as `{:type :invoke/:ok/:fail/:info, :f :read/:write/:cas, :value v, :time t}`
- Knossos model checker verifies the history against a sequential specification
- For CAS operations: the sequential spec is a single register with compare-and-swap semantics
- For bank workloads: Elle checker verifies strict serializability (detects G0, G1a, G1b, G1c, G2 anomalies)
- Verdict: cryptographic proof of linearizability or a minimal counterexample

### Method 2: Custom Rust Checker (fast feedback)

- Existing `LinearizabilityChecker` extended to support CAS operations and multi-key transactions
- Runs inline during every test for immediate feedback
- Records wall-clock intervals `[invoke_time, complete_time]` per operation
- Builds a happens-before DAG from non-overlapping intervals
- Verifies every read value was the result of the most recent completed write in some valid sequential ordering
- Faster than Knossos (O(n log n) vs NP-complete), catches most bugs but not all anomalies

### Method 3: Invariant Assertions (domain-specific)

- Bank: `SUM(balances) == initial_total` after every operation batch
- Counter: `final_value == count(successful_CAS_operations)`
- Set: `final_set ⊇ all_acknowledged_additions`
- Register: every read returns a value that was written
- Queue: FIFO ordering preserved, no duplicates, no lost items
- Run continuously during the test, fail immediately on violation

### History Recording

```
Client (any driver)
  |
  +-- invoke_time = Instant::now()
  +-- execute CQL query via driver
  +-- complete_time = Instant::now()
  +-- result = {applied, current_values, error}
  +-- append to shared history log (append-only file, one per client)

Post-test:
  |
  +-- Merge all client histories by wall-clock
  +-- Feed to Knossos (Jepsen verification)
  +-- Feed to Rust checker (fast verification)
  +-- Run invariant assertions
```

### Clock Synchronization

- Firecracker VMs on same host share host clock via `clock_gettime(CLOCK_MONOTONIC)`
- Fly.io uses `chrony` with tight NTP sync (< 1ms offset)
- Measured offset recorded in history so checker can account for it

## Infrastructure

### Local Development (Firecracker)

```
ferrosa-jepsen (orchestrator binary)
+-- firecracker-pool/
|   +-- Provisions VMs from rootfs snapshot (~100ms boot)
|   +-- tap interfaces with tc/netem per-link
|   +-- VM specs: 2 vCPU, 1GB RAM per node
+-- wan-bridge VMs (T3/T4 only)
|   +-- Dedicated Firecracker VM per DC link
|   +-- Two tap interfaces (one per DC)
|   +-- tc/netem rules applied dynamically by chaos controller
+-- jepsen-runner/
|   +-- Clojure Jepsen process (lein run)
|   +-- Connects to ferrosa nodes via CQL
|   +-- Nemesis schedule driven by orchestrator
+-- driver-runners/
    +-- 6 containers (one per language driver)
    +-- Each runs workload generator against cluster
    +-- History files written to shared volume
```

### Fly.io Long-Duration (real geo-distributed)

```
Regions:
  iad (US-East)  -- 3 ferrosa machines
  cdg (Europe)   -- 3 ferrosa machines
  nrt (Asia)     -- 3 ferrosa machines

WAN chaos:
  Each region runs a sidecar "chaos-proxy" Fly machine
  Proxy intercepts inter-region traffic via Fly private network
  Injects latency/loss/jitter/reorder per the nemesis schedule
  Real RTT: iad<->cdg ~80ms, iad<->nrt ~150ms, cdg<->nrt ~250ms

Driver runners:
  Fly machines in each region running all 6 drivers
  Tests both local-DC and cross-DC CQL paths

Orchestrator:
  Single Fly machine running ferrosa-jepsen
  Coordinates nemesis schedule across all regions
  Collects history files via Fly volumes
  Runs Knossos/Elle verification post-test
```

## CLI Interface

```bash
# Quick smoke -- 5 minutes, T1 only, Rust driver
ferrosa-jepsen run --tier smoke

# Standard -- 45 min, T1+T2, all drivers, all nemeses
ferrosa-jepsen run --tier standard

# Full matrix -- 4 hours, all topologies, all combinations
ferrosa-jepsen run --tier full

# Endurance -- 24h soak on Fly.io
ferrosa-jepsen run --tier endurance --fly-region iad,cdg,nrt

# Single pattern debug -- rerun one failing combination
ferrosa-jepsen run --topology t1 --nemesis partition-halves --pattern lwt-cas-register --driver gocql --concurrency high

# Report on last run
ferrosa-jepsen report --last
ferrosa-jepsen report --compare run-001 run-002
```

## Execution Tiers

| Tier | Duration | Scope | Where |
|------|----------|-------|-------|
| Smoke | ~5 min | T1, 3 nemeses, 16 LWT, Rust driver, low concurrency, Rust checker | Firecracker local |
| Standard | ~45 min | T1+T2, all 16 nemeses, 16 LWT, all 6 drivers, low+medium, Rust+Knossos | Firecracker local |
| Full | ~4 hours | All 4 topologies, 16+5 nemeses, 16 LWT, 6 drivers, 3 concurrency levels, all checkers | Firecracker local |
| Endurance | 24 hours | T4 tri-DC, continuous random nemesis, 16 LWT cycling, 6 drivers high concurrency, Knossos every 10min | Fly.io multi-region |

## Crate Structure

```
ferrosa-jepsen/
+-- Cargo.toml
+-- src/
|   +-- main.rs              -- CLI: run, report, compare
|   +-- orchestrator.rs      -- Provisions cluster, coordinates test
|   +-- firecracker.rs       -- Firecracker VM management
|   +-- flyio.rs             -- Fly.io machine management
|   +-- chaos/
|   |   +-- mod.rs           -- NemesisSchedule, composed nemeses
|   |   +-- network.rs       -- tc/netem injection
|   |   +-- process.rs       -- kill/pause/resume
|   |   +-- clock.rs         -- faketime injection
|   |   +-- disk.rs          -- dm-flakey
|   |   +-- wan_bridge.rs    -- Inter-DC chaos proxy
|   +-- workload/
|   |   +-- mod.rs           -- WorkloadRunner trait
|   |   +-- register.rs      -- Single-key linearizable register
|   |   +-- bank.rs          -- Multi-account balance transfer
|   |   +-- lwt.rs           -- All 16 LWT patterns
|   |   +-- set.rs           -- Set add/read
|   |   +-- cas_register.rs  -- CAS retry loop
|   |   +-- queue.rs         -- FIFO queue
|   +-- driver/
|   |   +-- mod.rs           -- DriverRunner trait
|   |   +-- python.rs        -- cassandra-driver subprocess
|   |   +-- go.rs            -- gocql subprocess
|   |   +-- node.rs          -- datastax driver subprocess
|   |   +-- java.rs          -- java-driver subprocess
|   |   +-- csharp.rs        -- CassandraCSharpDriver subprocess
|   |   +-- rust.rs          -- cdrs-tokio in-process
|   +-- checker/
|   |   +-- mod.rs           -- CheckerResult, history merging
|   |   +-- linearizability.rs -- Rust O(n log n) checker
|   |   +-- knossos.rs       -- Jepsen/Knossos JVM bridge
|   |   +-- elle.rs          -- Elle transactional checker
|   |   +-- invariant.rs     -- Domain-specific invariants
|   +-- report/
|       +-- mod.rs           -- HTML report generator
|       +-- timeline.rs      -- Operation timeline visualization
|       +-- anomaly.rs       -- Counterexample formatter
+-- jepsen/                  -- Clojure Jepsen project
|   +-- project.clj
|   +-- src/ferrosa/         -- Jepsen test definitions
+-- drivers/                 -- Per-language workload generators
|   +-- python/
|   +-- go/
|   +-- node/
|   +-- java/
|   +-- csharp/
|   +-- rust/
+-- rootfs/                  -- Firecracker VM rootfs build
```

## Full Test Matrix

```
4 topologies x 16 nemeses x 16 LWT patterns x 6 drivers x 3 concurrency levels = 18,432 test combinations
```

### Coverage by tier

| Tier | Combinations | Verification |
|------|-------------|--------------|
| Smoke | 768 (T1, 3 nemeses, 16 LWT, Rust, low) | Rust checker |
| Standard | 6,144 (T1+T2, 16 nemeses, 16 LWT, 6 drivers, low+med) | Rust + Knossos |
| Full | 18,432 (all) | Rust + Knossos + Elle + invariants |
| Endurance | Continuous cycling over 24h | Knossos every 10 min on rolling window |

## Key Design Decisions

- **Real Jepsen (Knossos/Elle)** over custom-only checker: Knossos is the gold standard for linearizability proofs. Our Rust checker is fast feedback but NP-complete problems need the real solver.
- **Firecracker over Docker** for local: Sub-second boot, real kernel isolation, tap interfaces for precise tc/netem control. Docker bridge networking can't do surgical per-link chaos.
- **Fly.io over AWS** for geo-distributed: Simpler provisioning, real multi-region with WireGuard mesh, per-machine pricing is cost-effective for burst testing.
- **All 6 drivers** not just one: Different drivers have different connection pooling, retry, and protocol negotiation behavior. A bug visible in one driver may be masked by another's retry logic.
- **Three independent checkers** must agree: Defense in depth — if Knossos and our Rust checker disagree, that's a checker bug to investigate, not a false pass.

## Related Specs

- [Accord Consensus Protocol](accord.md)
- [Accord Project Plan](accord-project-plan.md)
- [Testing Infrastructure](testing.md)
- [Threat Model — Accord](threat-model-accord.md)
- [FMEA — Accord](fmea-accord.md)
