# ferrosa-sched

Query QoS scheduler core. **Phase 0** of the fair scheduler ([epic](../specs/proposed/query-scheduler/)):
a bounded execution pool that reserves CPU headroom for consensus.

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
- `SchedClass { Foreground, Bulk }` and a no-op `SchedTicket` — the seam Phase 1
  (fair scheduling) activates without re-touching call sites.

## Dependencies / dependents

- **Depends on:** `tokio` only (a leaf crate — the DSM guard keeps
  storage/cluster/cql out).
- **Dependents:** `ferrosa` (constructs the pool at boot), `ferrosa-storage`
  (routes scan producers through it — Phase 0 T0.3).

## Status

Phase 0 in progress: T0.1 (this crate) + T0.2 (explicit `max_blocking_threads`
in `ferrosa/src/runtime.rs`) done. T0.3 (route `store.rs` scan producers through
`SchedPool`), T0.5 (headroom metrics), T0.6 (live no-step-down regression) pending.
