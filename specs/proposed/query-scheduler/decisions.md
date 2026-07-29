---
title: "Query QoS / Fair Scheduler — Decision Records"
status: partially-implemented
component: ferrosa-sched + ferrosa-cql, ferrosa-cluster, ferrosa-storage, ferrosa-index
last_revised: 2026-07-20
executive_summary: >
  Locked design decisions for a CFS-inspired, multi-resource (CPU + I/O),
  multi-tenant fair scheduler that governs read scans and background jobs so
  that consensus and interactive queries keep their latency SLA under heavy
  analytical load. Delivered in phases; Phase 0 (consensus CPU reservation +
  bounded background pool) both ships first and fixes the raft-heartbeat
  starvation regression (t_88223ad0). Consensus is isolated by reservation,
  not fair share; cooperative yielding is the load-bearing refactor.
---

# Query QoS / Fair Scheduler — Decision Records

Each record is **locked** unless marked otherwise. `DR-1..4` come from the Phase 0
stakeholder grill; `DR-5..10` are baked-in consequences grounded in the code
reconnaissance (see `architecture.md` § "Current state").

## DR-1 — Ambition: full fair scheduler, delivered in phases (LOCKED)

Build the fair scheduler as a real, first-class subsystem, but land it in phases.
**Phase 0** (consensus CPU reservation + a bounded background execution pool)
ships first and, on its own, fixes the raft-heartbeat starvation
(`t_88223ad0`). Later phases add vruntime fairness, the second resource
dimension, and tenant groups.

- **Rationale:** value early (bug fixed in Phase 0), risk staged, and the end
  state is a genuine ferrosa differentiator vs Cassandra (which has no per-query
  QoS). Top-down-before-shipping leaves the P0 step-down unfixed for longer.
- **Rejected:** *minimal fix only* (not a feature; leaves analytical/OLTP
  interference unsolved) and *full scheduler top-down* (delays the bug fix).

## DR-2 — Govern both CPU and I/O (LOCKED)

The scheduler accounts for **two resource dimensions**: CPU service time
(deserialize + filter + merge) **and** I/O concurrency (outstanding S3/local
reads, expressed as `Lane::Bulk` permits). A query's `vruntime` advances on
whichever dimension it consumes.

- **Rationale:** ferrosa is S3-backed; a full scan is frequently I/O-bound, so
  CPU-only accounting would let an I/O-heavy scan escape throttling. The
  `Lane::Bulk` seam already exists (`ferrosa-net`), so the I/O dimension has a
  natural home.
- **Rejected:** *CPU only* (dead-ends on I/O-bound scans).

## DR-3 — Refactor scope: reads + background jobs (LOCKED)

v1 makes these cooperatively yield to the scheduler: **range scans, `ALLOW
FILTERING`, secondary-index reads, ANN/vector scans** (read side) **and
compaction reads, repair, hinted-handoff replay, index build** (background
side). The **write/apply path is out of v1** (writes already have admission
control and are latency-bounded, not scan-shaped).

- **Rationale:** unify all heavy, long-running work under one scheduler so no
  single class of background work can starve consensus or OLTP. The write path
  is a separate control problem (backpressure by bytes) already partly solved.
- **Consequence:** ANN/vector search is currently synchronous with **no yield
  points** (`ferrosa-index/src/vector/hnsw.rs`) — it is a required refactor
  target, not just an integration.

## DR-4 — Per-tenant group scheduling in v1 (LOCKED)

The scheduler is **hierarchical**: fairness is computed first across **tenant
groups** (weighted, cgroup/`cpu.shares` style), then across queries within a
group. Ships in v1, aligned with ferrosa-dbaas multi-tenancy.

- **Rationale:** in a shared cluster, one tenant's analytical scan must not
  starve another tenant's OLTP. This is the highest-value fairness axis for the
  DBaaS product and is cheap to design in from the start but expensive to retrofit.
- **Consequence:** the scheduler runqueue is a two-level structure
  (group rbtree → query rbtree), and `TenantContext` (already threaded
  everywhere) is the group key.

## DR-5 — Consensus is isolated by reservation, NOT fair share (LOCKED, baked)

Raft/consensus is **not** a high-weight participant in the fair pool. It keeps
its dedicated runtime (`runtime.rs:38`, 8 threads) and per-peer OS threads
(`lane_actor.rs:444`), and the scheduler **hard-caps aggregate background
concurrency** so those threads always get a CPU. Think Linux `SCHED_FIFO` above
`SCHED_OTHER`, plus a `cpu.max` bandwidth cap on the background class.

- **Rationale:** fairness ≠ isolation. A proportional share can still miss the
  3 s election deadline (`raft_election_timeout_min_ms = 3000`,
  `config.rs:117`). The recon proves this is the actual failure: raft *already*
  has a dedicated thread, yet unbounded storage `spawn_blocking` producers
  (`max_blocking_threads` unset → 512, `runtime.rs`) oversubscribe the cores and
  the OS can't schedule the raft thread → CheckQuorum step-down.
- **Consequence:** Phase 0 = set/enforce a bounded background execution pool
  sized `cores − reserved` and stop using the unbounded shared blocking pool for
  scan producers.

## DR-6 — Cooperative yielding is the preemption model (LOCKED, baked)

There is no mid-syscall preemption. Every long operation is decomposed into
**resumable chunks**; at each chunk boundary the worker calls the scheduler to
account service time and possibly yield. Chunk size is bounded by **rows OR
elapsed time** (whichever first), so a slow decode can't monopolize between
yield points.

- **Rationale:** you cannot preempt a synchronous storage/decode call in FFI.
  Cooperative yield points are mandatory; this is the load-bearing refactor.
- **Grounding:** the streaming scan already pages (`store.rs:2810 range_iter`,
  bounded mpsc `STREAM_BUFFER=4`), so read-side yield points mostly exist at page
  boundaries; ANN and some background loops do not yet.

## DR-7 — Aging over a-priori classification (LOCKED, baked)

Weight is *seeded* from query shape at admission (point read → interactive; full
scan / `ALLOW FILTERING` → background), but **accumulated service time
(`vruntime`) drives ongoing scheduling**, so a surprise-long "interactive"
query self-demotes without us predicting its cost.

- **Rationale:** classification alone is brittle; CFS's insight is that runtime
  accounting handles the unknown-length case for free.

## DR-8 — Admission at the `ScanPlan` seam (LOCKED, baked)

Classification and admission attach at the planner/router boundary
(`ferrosa-cql/src/planner.rs` `ScanPlan`; `router.rs:5082-5156`), where
partition-key presence, `ALLOW FILTERING`, `LIMIT`, `ORDER BY`, `DISTINCT`, and
aggregate shape are all known *before* any storage call.

- **Rationale:** the query shape is free here; no extra parsing. This is the one
  place that sees the whole query before dispatch.

## DR-9 — Cover the streaming seam, and bound the producer pool (LOCKED, baked)

The scheduler hooks the **streaming** read seams (`WritePath::range_read_stream_*`,
`ferrosa-cluster/src/coordinator/range_read_stream.rs`, `TableStore::range_iter` /
`range_iter_projected`, `store.rs:2703/2810`), not only `DataStore::read_range`
(which materializes). The unbounded `spawn_blocking` scan producers
(`store.rs:2747…`) are replaced by a **bounded, scheduler-owned executor pool**.

- **Rationale:** the recon shows streaming scans bypass `DataStore`; hooking only
  the materializing trait would miss the actual scan path and the actual
  starvation vector.

## DR-10 — New `ferrosa-sched` crate for the scheduler core (LOCKED, baked)

The scheduler primitives (classes, groups, runqueue, resource accounting, the
`SchedTicket` yield handle) live in a **new leaf crate `ferrosa-sched`** with no
dependency on storage/cluster/cql, so all of them can depend *on it* without a
cycle. Integration code (classifier, executor-pool wiring) lives in the
consuming crates.

- **Rationale:** keeps the DSM acyclic (storage, cluster, cql, index all consume
  the scheduler); mirrors how `ferrosa-common` is a shared leaf.
- **Open sub-question:** whether the bounded executor pool itself lives in
  `ferrosa-sched` or `ferrosa-storage` (it needs `spawn_blocking`); leaning
  `ferrosa-sched` owning a runtime-agnostic pool abstraction, storage supplying
  the closures. Resolve in Phase 1 design review.

## Deferred / open decisions

- **DD-1:** exact `vruntime` unit — wall-time vs thread-CPU-time vs a row/byte
  proxy. Leaning a hybrid: measured elapsed per chunk, weighted; revisit with a
  microbenchmark in Phase 2.
- **DD-2:** whether interactive point reads bypass the scheduler entirely (own
  fast lane) or enter with a large vruntime credit. Leaning **bypass** for pure
  partition-key lookups (zero scheduler overhead on the OLTP hot path).
- **DD-3:** hinted-handoff replay loop was not pinned in recon (`§7` gap) —
  confirm its main loop before wiring it into the background group.
- **DD-4:** admission-reject vs queue behavior when the background pool is
  saturated (fail-loud `Overloaded` like CQL backpressure, vs bounded queue with
  a deadline). Leaning bounded queue + deadline, `Overloaded` past the deadline.
