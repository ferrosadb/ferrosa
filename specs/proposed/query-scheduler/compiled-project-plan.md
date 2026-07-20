---
title: "Query QoS / Fair Scheduler — Compiled Project Plan"
status: proposed
component: ferrosa-sched + integration crates
last_revised: 2026-07-20
executive_summary: >
  Agent-executable compilation of the four-phase plan: a task DAG, parallel
  execution batches, and three-tier verification (unit acceptance → integration
  → live/system) per task. Batch B0 delivers the shippable Phase-0 bug fix.
  Each task names its files, its refactor id (R#), its FMEA guard, and the exact
  commands that prove it done. Follow TDD (red→green→refactor) per task.
---

# Query QoS / Fair Scheduler — Compiled Project Plan

**Conventions.** Every task is TDD (write the failing test first). Verification
has three tiers: **T1** unit/acceptance (crate `cargo test`), **T2** integration
(cross-crate / `MockStorage`), **T3** live/system (real cluster, Fly race-fuzzer,
or load harness). A task is *done* only when its listed tier(s) pass. Repo:
`ferrosa`; scheduler core crate: `ferrosa-sched` (new). No `#[ignore]`, no silent
returns, RAII for every permit, no lock held across `.await` in the pool path.

## Task DAG

```
B0 (Phase 0 — ships the fix)
  T0.1 sched-crate ──┬─ T0.3 route-producers-through-pool ─┬─ T0.5 headroom-metrics ─ T0.6 live-regression*
  T0.2 blocking-cap ─┘   T0.4 ticket-plumbing(no-op) ──────┘   T0.7 pool-RAII

B1 (Phase 1 — fairness)      needs B0
  T1.1 runqueue ─ T1.2 reschedule-impl ─┬─ T1.3 chunk-tripwire*
                                        ├─ T1.4 classifier
                                        ├─ T1.5 interactive-bypass
                                        └─ T1.6 fairness-proptests ─ T1.7 no-lock-audit

B2 (Phase 2 — I/O dim)       needs B1
  T2.1 io-permits ─ T2.2 vruntime-on-io ─ T2.3 vruntime-unit-bench ─ T2.4 permit-leak-test

B3 (Phase 3 — groups + bg)   needs B1 (T3.3 also needs B2 for I/O-heavy ANN)
  T3.1 two-level-rbtree ─┬─ T3.2 tenant-weights ─ T3.9 gaming-test
                         ├─ T3.3 ann-yield + recall-guard*
                         ├─ T3.4 compaction-fold
                         ├─ T3.5 repair-accounting
                         ├─ T3.6 indexbuild-fold
                         ├─ T3.7 hints-replay (needs DD-3)
                         └─ T3.8 bounded-runqueue
```
`*` = FMEA RPN ≥ 200 guard; must pass before dependents merge.

## Parallel batches

- **B0** is the critical path to the bug fix. Within B0: T0.1 ‖ T0.2 first, then
  T0.3 ‖ T0.4, then T0.5 ‖ T0.7, then T0.6.
- **B1** T1.4/T1.5/T1.6 parallelize after T1.2.
- **B3** T3.3/T3.4/T3.5/T3.6 parallelize (different subsystems, use worktree
  isolation to avoid file conflicts).

---

## B0 — Reservation + bounded pool (Phase 0, shippable)

### T0.1 — `ferrosa-sched` crate skeleton
- **Deps:** none. **Refactor:** R10/R8.
- **Files:** new `ferrosa-sched/{Cargo.toml,src/lib.rs}`; workspace `Cargo.toml`.
- **Action:** `SchedClass`, `Reservation { cores, reserved }`, a bounded
  execution pool (`submit(closure) -> JoinHandle`, admits ≤ `cores−reserved`
  CPU-bound at once), no-op `SchedTicket`. Leaf crate — depends only on
  `ferrosa-common` + tokio.
- **T1:** `cargo test -p ferrosa-sched` — pool admits ≤ cap concurrently
  (counter never exceeds); RAII slot returned on drop/panic.
- **Guard:** DSM — `ferrosa-sched` has no storage/cluster/cql dep.

### T0.2 — Explicit `max_blocking_threads`
- **Deps:** none. **Refactor:** R8. **FMEA:** FM-1.
- **Files:** `ferrosa/src/runtime.rs`, `ferrosa/src/main.rs`, config.
- **Action:** set `max_blocking_threads` on data/background runtimes (default
  derived from cores); expose `FERROSA_*_MAX_BLOCKING` + TOML.
- **T1:** config precedence test (TOML-wins) for the new field.

### T0.3 — Route scan producers through the bounded pool
- **Deps:** T0.1, T0.2. **Refactor:** R1. **FMEA:** FM-1.
- **Files:** `ferrosa-storage/src/store.rs:2703/2810/2747/2844/2937/3017`.
- **Action:** replace raw `spawn_blocking` scan producers with
  `sched_pool.submit(...)`. Page loop unchanged for now (FIFO admit).
- **T1:** streaming range read still returns identical rows (existing
  `range_iter` tests green). **T2:** concurrent-scan test asserts ≤ cap producers
  run at once (new gauge).

### T0.4 — Thread no-op `SchedTicket` router→coordinator→producer
- **Deps:** T0.1. **Refactor:** R2.
- **Files:** `ferrosa-cql/src/router.rs:5082…`, `ferrosa-cluster/src/write_path.rs:657`,
  `coordinator/range_read_stream.rs:1557`, `store.rs:2810`.
- **Action:** mint a `SchedTicket` at the router, pass it down to the producer
  (unused in B0). Establishes the seam so B1 activates fairness without
  re-touching call sites.
- **T1:** compiles; ticket reaches the producer (trace/assert in a test).

### T0.5 — Consensus-headroom + pool metrics
- **Deps:** T0.3. **FMEA:** FM-1.
- **Files:** `ferrosa-sched/src/metrics.rs`, Prometheus registry.
- **Action:** `SCHED_CONSENSUS_HEADROOM_CORES`, pool depth, admit-wait latency.
- **T1:** metric reflects `cores − active_background`.

### T0.6 — Live no-step-down regression ★
- **Deps:** T0.3, T0.5. **FMEA:** FM-1.
- **Files:** `ferrosa-jepsen` or a scan-storm harness + Fly runbook.
- **Action:** reproduce `t_88223ad0` on the *pre-fix* build (raft step-down under
  full-table `ALLOW FILTERING` on Fly 6.5%-CPU), then assert *green* on the fix.
- **T3:** leader term stable, `ELECTION_STORM_TERM_JUMPS_TOTAL` == 0, headroom > 0
  throughout the scan storm.

### T0.7 — Pool-slot RAII
- **Deps:** T0.1. **FMEA:** FM-8.
- **T1:** panic-in-closure test asserts the slot is returned (no leak).

**B0 exit:** T0.6 green → the bug fix ships. Tag as the Phase-0 deliverable.

---

## B1 — Fair scheduling (Phase 1)

### T1.1 — `RunQueue` (single group) + `min_vruntime` floor
- **Deps:** B0. **FMEA:** FM-4, FM-5.
- **Files:** `ferrosa-sched/src/runqueue.rs`.
- **T1:** pick-min ordering; enqueue clamps to `max(self, min_vruntime − boost)`;
  monotonic-`min_vruntime` invariant.

### T1.2 — `SchedTicket::reschedule(cpu, io)` real impl
- **Deps:** T1.1. **Refactor:** R1. **FMEA:** FM-2.
- **Files:** `ferrosa-sched/src/ticket.rs`, `store.rs` page loop.
- **Action:** account service time, advance `vruntime`, yield if a smaller-vruntime
  unit waits. Page loop calls it per page.
- **T1:** two scans interleave (neither monopolizes); **T2:** `MockStorage`
  multi-scan fairness.

### T1.3 — Chunk-budget tripwire ★
- **Deps:** T1.2. **FMEA:** FM-2.
- **Action:** inside `reschedule()`, assert chunk consumed ≤ budget (rows OR ms);
  emit `sched_max_chunk_ms`; source-inspection test that every pool-submit loop
  calls `reschedule()` (pattern like the viz-drain `truncations.push` guard).
- **T1:** over-budget chunk trips the assertion/metric; inspection test fails if a
  submit loop omits `reschedule()`.

### T1.4 — Classifier at `ScanPlan`
- **Deps:** T1.2. **FMEA:** FM-10.
- **Files:** `ferrosa-cql/src/router.rs:5082-5156`, `planner.rs`.
- **T1:** full `ScanPlan` matrix → expected class + seed weight (point→Interactive,
  FullScan/ALLOW FILTERING→Background, etc.).

### T1.5 — Interactive point-read bypass (DD-2)
- **Deps:** T1.4. **FMEA:** FM-6.
- **T2:** `PartitionKeyLookup` path has zero scheduler calls (overhead ~0).

### T1.6 — Fairness property tests
- **Deps:** T1.2. **FMEA:** FM-4, FM-5.
- **T1:** proptest — equal-weight → equal service ±ε; long scan makes monotonic
  progress under interactive churn (aging).

### T1.7 — No-lock-across-`reschedule` audit
- **Deps:** T1.2. **FMEA:** FM-3, FM-7.
- **T1:** source-inspection + review gate: no storage/index lock held across
  `reschedule().await` (mirror Accord `handlers.rs` deadlock-safety doc).

**B1 exit:** T1.3, T1.6, T1.7 green; p99 interactive-under-scan within SLA (T3).

---

## B2 — Two resource dimensions (Phase 2)

### T2.1 — I/O permit pool on `Lane::Bulk`
- **Deps:** B1. **FMEA:** FM-8. **Files:** `ferrosa-sched`, `ferrosa-net` lane wiring.
- **T1:** permit acquire/release RAII; bound respected.

### T2.2 — `vruntime` advances on I/O wait
- **Deps:** T2.1. **FMEA:** FM-4.
- **T2:** an I/O-bound scan (mock high-latency S3) is throttled to fair share
  despite low CPU.

### T2.3 — Pin DD-1 (vruntime unit) via bench
- **Deps:** T2.2. **Action:** microbench wall vs thread-CPU vs proxy; write ADR.

### T2.4 — Permit-leak invariant
- **Deps:** T2.1. **FMEA:** FM-8. **T1:** panic/cancel in chunk → permit returned.

---

## B3 — Tenant groups + background unification (Phase 3)

### T3.1 — Two-level rbtree (group→query)
- **Deps:** B1. **FMEA:** FM-12. **Files:** `ferrosa-sched/src/runqueue.rs`.
- **T1:** group-then-query pick-min; `TenantContext` as group key.

### T3.2 — Per-tenant weights (TOML) — **Deps:** T3.1. **T1:** share ∝ weight.

### T3.3 — ANN cooperative yield + recall guard ★
- **Deps:** T1.2 (+ B2 for I/O-heavy). **Refactor:** R3. **FMEA:** FM-11.
- **Files:** `ferrosa-index/src/vector/hnsw.rs:191/196/485`, `ivfflat.rs:256`.
- **Action:** chunk candidate expansion at safe frontier boundaries; `reschedule()`
  every N candidates.
- **T1:** **golden-recall** — chunked vs unchunked return identical top-k on
  fixtures; **T2:** ANN under scan load yields (no monopolization).

### T3.4 — Compaction under group accounting
- **Deps:** T3.1. **Refactor:** R4. **Files:** `compaction/executor.rs:143/856`.
- **Action:** fold `CompactionGate` into scheduler system-group accounting.
- **T1:** compaction shares system-group fairly; existing compaction tests green.

### T3.5 — Repair chunk accounting — **Deps:** T3.1. **Refactor:** R5.
- **Files:** `repair/executor.rs:376/177`. **T2:** repair yields between chunks.

### T3.6 — Index build under scheduler — **Deps:** T3.1. **Refactor:** R6.
- **Files:** `index/scheduler.rs`, `ferrosa-index-builder/src/worker.rs:103`.
- **Action:** replace bespoke semaphore with scheduler admission.

### T3.7 — Hinted-handoff replay — **Deps:** DD-3 pinned. **Refactor:** R7.

### T3.8 — Bounded runqueue + `Overloaded` (DD-4) — **FMEA:** FM-13.
- **T3:** 10× admission-rate load storm → bounded memory, clean rejects.

### T3.9 — Tenant-gaming fairness — **Deps:** T3.1. **FMEA:** FM-12.
- **T2:** 1 tenant × 100 queries == 1 tenant × 1 query in aggregate share.

**B3 exit:** cross-tenant isolation proven; ANN recall unchanged; `CompactionGate`
and index semaphore removed (consolidation complete).

---

## Global acceptance (feature done)

- **T3-A:** under full-table `ALLOW FILTERING` + repair + compaction concurrently,
  on constrained CPU: raft leader stable, interactive p99 within SLA, each tenant
  gets its weighted share.
- **T3-B:** background throughput ≥ accepted delta vs pre-scheduler baseline
  (FM-14) — the reservation didn't cripple analytics.
- **Docs:** per-crate README/specs updated for `ferrosa-sched` and every touched
  crate (house rule); this proposal promoted out of `specs/proposed/`.
- **Gates:** `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D
  warnings`, `cargo test --workspace`, and the `p0-oom-audit` all green.
