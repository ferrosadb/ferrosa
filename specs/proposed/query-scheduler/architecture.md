---
title: "Query QoS / Fair Scheduler — Architecture"
status: proposed
component: ferrosa-sched (new) + ferrosa-cql, ferrosa-cluster, ferrosa-storage, ferrosa-index
last_revised: 2026-07-20
executive_summary: >
  A CFS-inspired, two-dimensional (CPU + I/O), hierarchical (per-tenant) fair
  scheduler for ferrosa. Consensus and interactive queries are isolated by
  reservation; analytical scans and background jobs run in a bounded, fairly
  scheduled pool that can never oversubscribe the cores. The load-bearing
  refactor is cooperative yielding: every long operation is decomposed into
  resumable chunks that check in with the scheduler at page/candidate
  boundaries. Phase 0 (reservation + bounded pool) fixes the raft-heartbeat
  starvation regression (t_88223ad0) on its own.
---

# Query QoS / Fair Scheduler — Architecture

> Read `decisions.md` first — this spec assumes DR-1..10 are locked.

## Current state (as-is, from code reconnaissance)

Ferrosa already has strong **runtime isolation** but **no query QoS**:

- **Consensus is isolated at the runtime level.** Raft runs on a dedicated
  8-thread runtime (`ferrosa/src/runtime.rs:38`) plus a per-peer OS thread with
  its own current-thread runtime (`ferrosa-net/src/lane_actor.rs:444`). Data,
  CQL, and background each get their own runtime (`runtime.rs:48/57/66`).
- **Scan classification is already computed** at `ferrosa-cql/src/planner.rs`
  (`ScanPlan` enum) and inspected at `router.rs:5082-5156`
  (`ScanPlan::FullScan`, `s.allow_filtering`, `unbounded_scan_shape`, `LIMIT`).
- **Streaming scans page with backpressure.** `TableStore::range_iter` /
  `range_iter_projected` (`store.rs:2810/2703`) spawn a `spawn_blocking`
  producer feeding a bounded mpsc (`STREAM_BUFFER = 4`); the coordinator rides
  `Lane::Bulk` (`write_path.rs:657`, `coordinator/range_read_stream.rs:1557`).
- **Piecemeal throttles exist** but no unified scheduler: CQL per-connection
  in-flight `Semaphore(max_in_flight)` (`connection.rs:346`), index-builder
  `Semaphore(max_workers)` (`worker.rs:103`), compaction `CompactionGate`
  (Mutex+Condvar, `compaction/executor.rs:143`), write admission
  (`coordinator/write.rs:320`).

**Why consensus still starved (root cause of `t_88223ad0`).** Two gaps:

1. **`max_blocking_threads` is never configured** — every runtime uses tokio's
   default of **512** (`runtime.rs`, confirmed absent). The storage streaming
   producers `spawn_blocking` **one unbounded task per concurrent scan**
   (`store.rs:2747/2844/2937/3017`). A full-table `ALLOW FILTERING` fan-out
   across replicas spawns a swarm of CPU-hot blocking producers.
2. That swarm **oversubscribes the physical cores**, so the OS scheduler cannot
   place even the dedicated raft-lane thread within the 3 s election window
   (`raft_election_timeout_min_ms = 3000`, `config.rs:117`) → CheckQuorum
   step-down.

PR #131 already removed the *inline* sync-on-async block; the residual vector is
**blocking-pool saturation + `Lane::Bulk` contention**, which is a *scheduling
and admission* problem — exactly what this feature solves. Adding more offload
threads makes it strictly worse.

There is **no** read-side query priority, class, or fair scheduler today
(confirmed absent).

## Design overview

Three ideas, layered like the Linux scheduler stack:

```
                    ┌───────────────────────────────────────────┐
   RESERVED (never  │  Consensus: raft runtime + per-peer lanes  │  cpu.max
   in the fair pool)│  Interactive: partition-key point reads    │  headroom
                    └───────────────────────────────────────────┘
                    ┌───────────────────────────────────────────┐
   FAIR POOL        │  Background class — bounded to cores−R      │
   (this feature)   │   hierarchical fair scheduling:            │
                    │     tenant-group rbtree (weighted)         │
                    │       └─ per-query rbtree (vruntime)       │
                    │   two resource dims: CPU-service + I/O      │
                    └───────────────────────────────────────────┘
```

1. **Scheduling classes (isolation).** Consensus and interactive point reads are
   *reserved* — outside the fair pool. Everything scan-shaped or background is in
   the fair pool, whose **aggregate concurrency is hard-capped** so the reserved
   classes always have a CPU (DR-5).
2. **Hierarchical fair scheduling (fairness).** Within the pool, pick the runnable
   unit with the smallest `vruntime`, computed first across tenant groups, then
   across queries (DR-4, DR-7). Classic CFS rbtree, two levels.
3. **Two resource dimensions (DR-2).** A schedulable chunk advances `vruntime` by
   its **CPU service time** and holds **I/O permits** (`Lane::Bulk`); an
   I/O-bound scan is throttled by permits even when it burns little CPU.

### Schedulable unit

The unit is a **chunk of work**, not a whole query: one page of a scan, one batch
of the k-way merge, one candidate-expansion window of an ANN walk, one repair
fetch chunk. A query is a *sequence* of chunks that re-enters the runqueue
between chunks. This is what makes long queries preemptible (DR-6).

## Components

### C1 — Classifier (admission)

At `router.rs:5082-5156`, map `ScanPlan` + predicates to a `SchedClass` and seed
weight:

| Query shape | Class | Seed weight (nice) |
|---|---|---|
| `PartitionKeyLookup` (full PK eq) | `Interactive` (reserved / bypass) | n/a (DD-2) |
| Bounded index read, small `LIMIT` | `Interactive` or light `Background` | 0 |
| `FullScan`, `ALLOW FILTERING`, no PK | `Background` | +10 |
| `VectorAnn` large `ef`, aggregates, `DISTINCT` scans | `Background` | +10 |
| Repair / compaction / index-build reads | `Background` (system group) | +15 |

The classifier produces a `SchedTicket` (query id, group = `TenantContext`,
class, seed weight) that travels with the request.

### C2 — Scheduler core (`ferrosa-sched`, new crate — DR-10)

Runtime-agnostic primitives, no storage/cluster/cql dependency:

- `SchedClass { Consensus, Interactive, Background }`.
- `Group` (tenant): weight, `min_vruntime`, a child rbtree of queries.
- `RunQueue`: two-level `BTreeMap<VRuntime, _>` (group → query). Pick-min = the
  next chunk to admit. `min_vruntime` floor on enqueue clamps both a returning
  long scan (can't hoard credit) and a fresh query (can't starve the scan) —
  fairness both directions (DR-7).
- `SchedTicket` — the per-query handle the executor calls between chunks:
  `ticket.reschedule(cpu_used, io_used).await` accounts service time, advances
  `vruntime`, and yields if a smaller-`vruntime` unit is waiting.
- `Reservation` — the hard cap: `background_permits = cores − reserved`; the pool
  never runs more than that many CPU-bound chunks at once, guaranteeing headroom
  for consensus + interactive.

### C3 — Bounded background executor pool (DR-9)

Replaces the unbounded storage `spawn_blocking` producers with a
**scheduler-owned, bounded** pool sized `cores − reserved`. Scan producers,
compaction merge steps, repair fetches, and index-build walks submit chunk
closures here; the pool admits by pick-min `vruntime`. This is the single most
important structural change and the whole of Phase 0's safety guarantee.

### C4 — Resource accounting (DR-2)

- **CPU dimension:** measure elapsed (or thread-CPU) time per chunk; advance
  `vruntime += cpu_time × weight_factor` (DD-1 pins the exact unit).
- **I/O dimension:** the pool holds a bounded set of `Lane::Bulk` permits
  (`ferrosa-net`); a chunk waiting on S3 holds a permit but consumes no CPU, so
  it is throttled by permit scarcity, and its `vruntime` also advances on I/O
  wait so I/O-bound scans don't get unlimited free turns.

### C5 — Consensus reservation (DR-5)

Phase 0: (a) set `max_blocking_threads` explicitly on the data/background
runtimes; (b) stop using the shared unbounded blocking pool for scan producers;
(c) size the background pool to leave `reserved` cores. Optional hardening:
thread-priority / core-pinning for the raft lane threads. A metric
`SCHED_CONSENSUS_HEADROOM_CORES` must stay > 0 under load.

## The refactor (key deliverable — DR-3, DR-6)

The user flagged this as central: the scheduler is only as good as the yield
points feeding it. Refactor targets, with grounding and effort:

| # | Target | File(s) | Current | Change | Effort |
|---|---|---|---|---|---|
| R1 | **Bound the scan producer pool** | `ferrosa-storage/src/store.rs:2747…` | unbounded `spawn_blocking`, `max_blocking_threads`=512 | submit to C3 bounded pool; page loop calls `ticket.reschedule()` | M |
| R2 | **Thread `SchedTicket` through the read path** | `ferrosa-cql/src/router.rs:5082…` → `ferrosa-cluster/src/write_path.rs:657` / `coordinator/range_read_stream.rs:1557` → `store.rs:2810/2703` | no ticket | classifier mints ticket at C1; passed to producer | M |
| R3 | **ANN/vector cooperative yield** | `ferrosa-index/src/vector/hnsw.rs:191/196/485`, `ivfflat.rs:256` | **synchronous, no yield, no chunk** | chunk candidate expansion; `reschedule()` every N candidates | M–L |
| R4 | **Compaction under group accounting** | `ferrosa-storage/src/compaction/executor.rs:143/856` | own threads + `CompactionGate` | keep threads, add `vruntime` accounting so compaction shares the system group fairly (replace/augment `CompactionGate`) | M |
| R5 | **Repair chunks account service** | `ferrosa-cluster/src/repair/executor.rs:376/177` | already chunked + `spawn_blocking` | add `reschedule()` at chunk boundary; system group | S |
| R6 | **Index build under scheduler** | `ferrosa-storage/src/index/scheduler.rs:355/638`, `ferrosa-index-builder/src/worker.rs:103` | own threads + semaphore | replace ad-hoc semaphore with C3 admission | M |
| R7 | **Hinted-handoff replay** | `ferrosa-cluster/src/raft_forward.rs`, `hints/` (loop unpinned — DD-3) | TBD | chunk + account after DD-3 | S–M |
| R8 | **Runtime config** | `ferrosa/src/runtime.rs`, `main.rs` | `max_blocking_threads` unset | set explicit ceilings; construct the scheduler + pool at boot | S |

**Shared refactor pattern (all of R1/R3/R4/R5/R6/R7):**

```text
loop over chunks {
    let chunk = next_chunk();              // page / candidate window / merge step
    do_work(chunk);                        // bounded by rows OR elapsed time (DR-6)
    ticket.reschedule(cpu_used, io_used).await;  // account + maybe yield
    if ticket.cancelled() { break; }       // deadline / client gone / drain
}
```

R2 (thread the ticket) + R1 (bounded pool) + R8 (runtime config) are the Phase 0
critical path and deliver the bug fix. R3 (ANN) is the biggest net-new yielding
work. R4/R6 also *simplify* by folding two bespoke throttles (`CompactionGate`,
index semaphore) into one scheduler — a genuine consolidation, not just additions.

## Configuration & observability

- **Config:** `reserved_cores` (default: `max(1, cores/4)`), per-class default
  nice, per-tenant `weight`/`shares`, chunk budget (rows + ms), background pool
  size. All via TOML (TOML-wins precedence per house config rules).
- **Metrics (Prometheus):** `SCHED_CONSENSUS_HEADROOM_CORES` (must stay > 0),
  per-class CPU%, background pool depth + admit-wait latency, per-group
  `vruntime` lag, demotions/sec, `Overloaded` rejects, I/O-permit saturation.
  These are the acceptance signals for the FMEA detection column.

## Phasing (→ `project-plan.md`)

- **Phase 0 — Reservation + bounded pool (fixes t_88223ad0).** R1, R2, R8, C3,
  C5; single class split (reserved vs background), no vruntime yet — background
  pool is simply bounded FIFO. Live regression under Fly 6.5%-CPU race fuzzer.
- **Phase 1 — Fair scheduling.** C2 vruntime rbtree, C1 classifier, seed weights,
  aging; interactive bypass (DD-2). Single (default) group.
- **Phase 2 — Two resource dimensions.** C4 I/O accounting, `Lane::Bulk` permit
  integration, `vruntime` on I/O wait. Pin DD-1 (vruntime unit) with a bench.
- **Phase 3 — Tenant groups + background unification.** DR-4 two-level rbtree;
  R4/R5/R6/R7 fold background jobs into the system group; per-tenant shares.

## Open questions

Tracked as `DD-1..4` in `decisions.md`. The two that most affect the design:
the `vruntime` unit (DD-1) and whether pure point reads bypass the scheduler
entirely (DD-2, leaning yes).

## Non-goals

- Preempting a synchronous storage/FFI call mid-execution (impossible; DR-6).
- Governing the write/apply path (DR-3; separate backpressure problem).
- Replacing openraft's internal heartbeat scheduling (we protect it, not
  reimplement it).
