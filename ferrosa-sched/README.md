# ferrosa-sched

Query QoS scheduler core ([epic](../specs/proposed/query-scheduler/)): a bounded
execution pool that reserves CPU headroom for consensus (**Phase 0 / B0**), plus
CFS-inspired fair-share scheduling so concurrent scans share that pool without
one monopolizing it (**B1**).

## Why

ferrosa already isolates raft at the *runtime* level (a dedicated multi-thread
runtime + per-peer OS threads). The residual starvation (`t_88223ad0`) is the
**blocking pool**: storage scan producers `spawn_blocking` onto tokio's default
512-thread pool, so a full-table `ALLOW FILTERING` scan admits hundreds of
blocking producers that oversubscribe the cores and starve raft heartbeats into
a CheckQuorum leader step-down.

## What

- `Reservation { cores, reserved }` — `available() = cores − reserved` (floored at 1).
- `SchedPool` — a semaphore-bounded pool. `submit(closure)` admits at most
  `available()` CPU-bound closures at once and runs them on tokio's blocking
  threads; the admission permit is moved into the task, so a slot is released on
  completion **or panic** (RAII).
- `SchedPool::submit_scan(chunk_budget, f)` + `ScanSlot` (**B1 T1.2**) — a
  cooperative-yield scan entry: the producer calls `slot.tick()` after each
  produced chunk and, every `chunk_budget` chunks, the pool permit is released
  and fairly (FIFO) re-acquired, so a long full-table scan cedes the slot to
  waiting scans. Deadlock-free (release before re-acquire) and no CPU
  oversubscription (parked re-acquires don't run).
- `SchedClass { Foreground, Bulk }` + `runqueue::{RunQueue, SchedEntity,
  weight_for_class}` + `scheduler::{Scheduler, SchedTicket, advance_vruntime}`
  (**B1 T1.1/T1.2**) — the CFS-inspired vruntime fair-share core: pick-min run
  queue with a monotonic `min_vruntime` floor, `SchedTicket::reschedule` service
  accounting, and weighted fairness (Foreground 1024 : Bulk 256).

## Dependencies / dependents

- **Depends on:** `tokio` only (a leaf crate — the DSM guard keeps
  storage/cluster/cql out).
- **Dependents:** `ferrosa` (constructs the pool at boot), `ferrosa-storage`
  (routes scan producers through `submit_scan`), `ferrosa-cql`
  (`ScanPlan::sched_class` — B1 T1.4 classifier).

## Status

- **B0 (shipped, PR #286):** T0.1 pool, T0.2 `max_blocking_threads`, T0.3 route
  scan producers, T0.5 headroom metrics, T0.6 live no-step-down regression.
- **B1 (in progress, PR #287):** T1.1 run queue, T1.2 `Scheduler`/`SchedTicket`
  + `submit_scan` cooperative yield wired into the `store.rs` producers, T1.4
  `ScanPlan` classifier. Pending: T1.3 chunk-budget tripwire, T1.5 interactive
  bypass, T1.6 fairness proptests, T1.7 no-lock audit, weighted per-scan budget.
