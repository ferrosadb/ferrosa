---
title: "Query QoS / Fair Scheduler — Project Plan"
status: partially-implemented
component: ferrosa-sched + integration crates
last_revised: 2026-07-28
executive_summary: >
  Four phases. Phase 0 (reservation + bounded background pool) is a shippable
  slice that fixes the raft-heartbeat starvation regression (t_88223ad0) with no
  vruntime machinery. Phases 1–3 add fair scheduling, the I/O resource
  dimension, and per-tenant groups plus the background-job unification refactor.
  Sequenced by FMEA risk (FM-1/FM-2 first) and by dependency: the bounded pool
  and the SchedTicket plumbing gate everything downstream.
---

# Query QoS / Fair Scheduler — Project Plan

> **Implementation status (2026-07-28).** This plan was authored before the work
> began and is preserved for its design rationale, FMEA-driven sequencing, and
> decision record — NOT as a description of unbuilt work. Much of it has since
> landed in `ferrosa-sched`; read the code as the source of truth and this plan
> as the "why".
>
> | Phase | Plan intent | On `main` today |
> |---|---|---|
> | 0 — reservation + bounded pool | fix raft starvation (t_88223ad0) | **Landed.** `SchedPool` bounds the `spawn_blocking` fan-out; `lib.rs` documents the reserved-headroom invariant. |
> | 1 — fair scheduling (vruntime) | vruntime run queue, chunk-budget yielding | **Largely landed.** `runqueue.rs` + `scheduler.rs` vruntime core; `SchedPool::submit_scan`/`ScanSlot` do cooperative re-acquire so one scan cannot monopolize the pool. |
> | 2 — two resource dimensions (CPU + I/O) | separate I/O accounting | **Partly landed.** `io_permits.rs` exists; check it against the plan's dimension model before assuming full coverage. |
> | 3 — tenant groups + background unification | per-tenant fair share | **Partly landed.** `group_runqueue.rs` + `fair_admit.rs` provide group-level admission; the background-job unification refactor is the piece to verify. |
>
> Consumers already wired: `ferrosa-cql` (planner) and `ferrosa-cluster`
> (coordinator). Before using this document to plan new work, diff each phase's
> task list against the modules above — some tasks are done, some were superseded,
> and the per-phase tables below have NOT been individually re-audited.

Priorities: **P1** = FMEA RPN ≥ 200 or blocks the bug fix; **P2** = high-risk /
dependency-unblocking; **P3** = completeness; **P4** = polish/tuning.

## Phase 0 — Reservation + bounded pool (fixes t_88223ad0)

**Goal:** stop the raft step-down under scan load. No vruntime; the background
pool is a bounded FIFO. Ship independently.

| Item | P | Refactor | FMEA | Deliverable |
|---|---|---|---|---|
| P0-1 Create `ferrosa-sched` crate (skeleton: `SchedClass`, `Reservation`, bounded pool, `SchedTicket` no-op) | P1 | R8 | — | new leaf crate, compiles, unit-tested pool |
| P0-2 Set explicit `max_blocking_threads` on data/background runtimes | P1 | R8 | FM-1 | `runtime.rs` ceilings; config field |
| P0-3 Route scan producers through the bounded pool | P1 | R1 | FM-1 | `store.rs:2747…` submits to C3, not raw `spawn_blocking` |
| P0-4 Thread a (no-op) `SchedTicket` router→coordinator→producer | P1 | R2 | — | plumbing seam for Phase 1 |
| P0-5 `SCHED_CONSENSUS_HEADROOM_CORES` + pool-depth metrics | P1 | — | FM-1 | Prometheus gauges |
| P0-6 **Live no-step-down regression** under Fly 6.5%-CPU fuzzer | P1 | — | FM-1 | reproduces old bug on baseline, green on fix |
| P0-7 RAII permit guard for pool slots | P2 | — | FM-8 | no manual release |

**Exit:** full-table `ALLOW FILTERING` fan-out under constrained CPU keeps raft
leader stable; headroom metric > 0 throughout; background throughput within an
accepted delta of pre-change baseline (FM-14 watch).

## Phase 1 — Fair scheduling (vruntime)

**Goal:** within the background pool, interleave chunks fairly; interactive
bypass.

| Item | P | Refactor | FMEA | Deliverable |
|---|---|---|---|---|
| P1-1 `RunQueue` (single group) vruntime rbtree + `min_vruntime` floor | P1 | — | FM-4, FM-5 | `ferrosa-sched` core |
| P1-2 `SchedTicket::reschedule(cpu,io)` real impl (account + yield) | P1 | R1 | FM-2 | chunk-boundary yield |
| P1-3 **Chunk-budget tripwire** (rows OR ms) inside `reschedule()` | P1 | — | FM-2 | assertion + `sched_max_chunk_ms` metric |
| P1-4 Classifier at `ScanPlan` seam; seed weights | P1 | R2 | FM-10 | `router.rs:5082…` |
| P1-5 Interactive point-read bypass (DD-2) | P2 | — | FM-6 | zero scheduler overhead on PK reads |
| P1-6 Fairness property tests (equal-weight convergence, aging) | P1 | — | FM-4, FM-5 | proptest suite |
| P1-7 No-lock-across-`reschedule` audit + inspection test | P1 | — | FM-3, FM-7 | review gate + test |

**Exit:** N equal-weight scans get equal service ±ε; a long scan makes monotonic
progress under interactive churn; p99 interactive latency under scan load within
SLA.

## Phase 2 — Two resource dimensions (CPU + I/O)

**Goal:** throttle I/O-bound scans, not just CPU-bound ones.

| Item | P | Refactor | FMEA | Deliverable |
|---|---|---|---|---|
| P2-1 I/O permit pool bound to `Lane::Bulk` | P1 | R1 | FM-8 | permit accounting |
| P2-2 `vruntime` advances on I/O wait | P1 | — | FM-4 | dual-dimension accounting |
| P2-3 Pin DD-1 (vruntime unit) with a microbenchmark | P2 | — | FM-4 | ADR + bench |
| P2-4 Permit-leak invariant test (panic-in-chunk) | P1 | — | FM-8 | RAII verified |

**Exit:** an I/O-bound scan is throttled to its fair share of `Lane::Bulk`; permit
count invariant holds across panics/cancels.

## Phase 3 — Tenant groups + background unification

**Goal:** per-tenant fairness; fold all background jobs under the scheduler.

| Item | P | Refactor | FMEA | Deliverable |
|---|---|---|---|---|
| P3-1 Two-level rbtree (group → query); `TenantContext` as group key | P1 | — | FM-12 | hierarchical fairness |
| P3-2 Per-tenant weights/shares (TOML) | P2 | — | FM-12 | config + tests |
| P3-3 ANN cooperative yield + **golden-recall guard** | P1 | R3 | FM-11 | chunked HNSW/IVF, identical top-k |
| P3-4 Compaction under group accounting (fold `CompactionGate`) | P2 | R4 | — | consolidation |
| P3-5 Repair chunk accounting | P2 | R5 | — | `repair/executor.rs:376` |
| P3-6 Index build under scheduler (fold semaphore) | P2 | R6 | — | consolidation |
| P3-7 Hinted-handoff replay (after DD-3 pin) | P3 | R7 | — | system group |
| P3-8 Bounded runqueue + `Overloaded` reject (DD-4) | P2 | — | FM-13 | no OOM under storm |
| P3-9 Tenant-gaming fairness test (1×100 vs 1×1) | P1 | — | FM-12 | group-level share proven |

**Exit:** tenant A's scan cannot starve tenant B's OLTP; ANN recall unchanged;
`CompactionGate` and the index semaphore are gone (replaced by the scheduler).

## Cross-phase risks

- **R3/ANN (FM-11)** is the highest-uncertainty refactor (correctness, not just
  perf) — golden-recall guard is non-negotiable.
- **FM-2 yield-gap** is the systemic risk: the whole feature silently regresses if
  a refactored loop forgets to `reschedule()`. The Phase 1 tripwire + inspection
  test must land before the Phase 3 background refactors.
- **DSM:** `ferrosa-sched` must stay a leaf (no storage/cluster/cql deps) or it
  creates a cycle — enforce in Phase 0.

## Sequencing rationale

Phase 0 ships the bug fix with minimal surface. `SchedTicket` plumbing (P0-4) is
laid down as a no-op so Phase 1 activates fairness without re-touching every call
site. The I/O dimension (Phase 2) needs the vruntime core (Phase 1) to advance
on. Tenant groups + background unification (Phase 3) are last because they are
additive on a proven single-group scheduler and carry the correctness-sensitive
ANN refactor.
