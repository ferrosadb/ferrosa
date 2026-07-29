---
title: "Query QoS / Fair Scheduler — FMEA"
status: partially-implemented
component: ferrosa-sched + integration crates
last_revised: 2026-07-20
executive_summary: >
  Failure-mode analysis for the fair scheduler. RPN = Severity × Occurrence ×
  Detection (1–10 each). The dominant risks are (a) consensus still starving if
  the reservation is wrong or a yield point is missing, and (b) yield-gap
  regressions where a refactored loop forgets to reschedule and reintroduces the
  current bug. Both are RPN ≥ 200 and become Sprint-1 work items with mandatory
  live regressions.
---

# Query QoS / Fair Scheduler — FMEA

Severity/Occurrence/Detection each 1–10 (higher = worse / more frequent / harder
to detect). RPN = S×O×D. Test cases required for RPN ≥ 50; work items
(`source: fmea`) for RPN ≥ 200.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / detection |
|---|---|---|---|---|---|---|---|
| FM-1 | **Consensus still starves** — reservation too small, pool cap too high, or a non-scan CPU hog escapes the cap | Raft step-down, cluster unavailability (the exact `t_88223ad0` regression) | 10 | 4 | 5 | **200** | Hard cap `cores−reserved`; `SCHED_CONSENSUS_HEADROOM_CORES` metric alerts if ≤0; live Fly 6.5%-CPU race-fuzzer regression asserting no step-down under scan load |
| FM-2 | **Yield-point gap** — a refactored loop (esp. R3 ANN, R4 compaction) misses a `reschedule()` and runs a chunk unbounded | Localized starvation returns; one query monopolizes a core | 9 | 6 | 6 | **324** | Chunk-budget assertion (rows OR elapsed ms) enforced *inside* `reschedule()`; a `sched_max_chunk_ms` tripwire metric; source-inspection test that every submit-to-pool loop calls `reschedule()`; watchdog logs a chunk exceeding budget |
| FM-3 | **Priority inversion** — interactive request blocked behind a background chunk holding a shared lock/permit | OLTP latency spike despite reservation | 8 | 5 | 6 | **240** | Never hold a storage/index lock across `reschedule()`/`.await` (mirrors the Accord dep-wait deadlock-safety rule); priority-inheritance on the few unavoidable shared locks; p99 interactive-latency-under-scan test |
| FM-4 | **vruntime accounting drift** — bad time source or weight math → one query/group hogs | Unfair scheduling, effective starvation of a tenant | 7 | 5 | 7 | **245** | `min_vruntime` floor clamp on enqueue; property test: N queries of equal weight converge to equal service ±ε; monotonic-vruntime invariant assertion |
| FM-5 | **Background starvation** — long scan never finishes because fresh queries keep preempting | Analytical queries hang / time out | 6 | 4 | 5 | 120 | `min_vruntime` floor prevents infinite deferral; a fresh query cannot get more than one slice of credit; aging test that a long scan makes monotonic progress under interactive churn |
| FM-6 | **Scheduler is the bottleneck** — runqueue lock contention on the hot path | Throughput collapse; scheduler overhead > work | 7 | 3 | 5 | 105 | Per-shard runqueues (like `memtable/sharded`); interactive point reads bypass entirely (DD-2); benchmark scheduler overhead < 2% at target QPS |
| FM-7 | **Reschedule deadlock** — chunk awaits `reschedule()` while holding a resource the pool needs to admit the next chunk | Pool wedges, all scans hang | 9 | 2 | 7 | 126 | Release all permits/locks before `reschedule().await`; loom/`cargo miri` on the pool admit path; deadlock watchdog on pool admit latency |
| FM-8 | **I/O permit leak** — a cancelled/panicked chunk drops a `Lane::Bulk` permit without returning it | Slow permit exhaustion → scans stall | 7 | 4 | 6 | 168 | RAII permit guard (like `CompactionPermit`); permit-count invariant metric; panic-in-chunk test asserts permit returned |
| FM-9 | **Cancellation/deadline ignored** — a drained/timed-out query keeps consuming (ties to the viz idle-drain class) | Wasted capacity, zombie scans | 6 | 4 | 5 | 120 | `ticket.cancelled()` checked every chunk; deadline propagated from CQL request; test: dropping the client stops the scan within one chunk budget |
| FM-10 | **Misclassification** — point read tagged Background (latency spike) or big scan tagged Interactive (bypasses cap → starvation) | Either OLTP latency or FM-1 recurrence | 8 | 3 | 4 | 96 | Classifier unit tests over the full `ScanPlan` matrix; a scan that *escalates* (exceeds interactive service budget) auto-demotes (DR-7 aging) — a misclassified scan self-corrects |
| FM-11 | **ANN chunking changes results** — breaking the HNSW walk into chunks alters recall/ordering | Wrong query answers (silent correctness bug) | 9 | 3 | 8 | **216** | Golden-recall test: chunked vs unchunked ANN return identical top-k on fixtures; chunk only at safe frontier boundaries (no partial-layer state loss) |
| FM-12 | **Tenant gaming** — a tenant issues many tiny queries to grab more aggregate share than its weight | Cross-tenant unfairness | 5 | 4 | 6 | 120 | Fairness is at the *group* level (weight per tenant), not per-query — many small queries share one group's slice; test: 1 tenant × 100 queries gets same share as 1 tenant × 1 query |
| FM-13 | **Runqueue unbounded growth** — query storm enqueues faster than drain | Memory blowup / OOM (violates bounded-collection rule) | 8 | 3 | 5 | 120 | Bounded runqueue + `Overloaded` reject past a depth/deadline (DD-4); depth metric; load test at 10× target admission rate |
| FM-14 | **Reservation starves throughput** — `reserved_cores` too large, background pool too small | Analytical throughput tanks; scans crawl | 5 | 4 | 4 | 80 | Default `reserved = max(1, cores/4)`; tunable; background-throughput regression vs pre-scheduler baseline |

## Work items (RPN ≥ 200) → `todo/`

- **FM-1** (200): consensus reservation correctness + live no-step-down regression. Sprint 1 / Phase 0.
- **FM-2** (324): yield-gap tripwire (budget assertion inside `reschedule()` + source-inspection test). Sprint 1 — this is the guard that stops the whole feature from silently regressing.
- **FM-4** (245): vruntime fairness property tests. Sprint 2 / Phase 1.
- **FM-3** (240): priority-inversion / no-lock-across-await audit. Sprint 2.
- **FM-11** (216): ANN chunking golden-recall guard. Sprint (Phase where R3 lands).

## Cross-cutting test themes

1. **No lock across `reschedule().await`** — a source-inspection + review gate, mirroring the Accord dep-wait deadlock-safety comment (`handlers.rs`).
2. **Every submit loop calls `reschedule()`** — inspection test (like the viz-drain `progress.truncations.push` guard we just added).
3. **Live starvation regression under constrained CPU** — the Fly 6.5%-CPU fuzzer is the canonical harness (per house memory).
4. **RAII for every permit** — no manual permit release anywhere.
