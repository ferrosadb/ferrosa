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
- `fair_admit::FairAdmit` (**B1.5**) — the live admission authority. At most
  `available()` scans hold a slot at once, and a freed slot is granted to the
  least-`vruntime` waiter, **weighted by `SchedClass`**: a Foreground scan (1024)
  advances `vruntime` 4x slower than a Bulk scan (256), so under contention it
  gets ~4x the slot turns. Admission is async (a waiter is a cheap task, never a
  parked blocking thread — the B0 property); a scan's mid-scan re-competes block
  its own blocking thread. Deadlock-free and slot-leak-free (RAII).
  `admit(class, cancel)` returns `Admitted::{Slot, Overloaded, Cancelled}`:
  **cancellable** — if the `cancel` future fires (or the future is dropped)
  before a slot is granted, a `WaitGuard` vacates the queue/waiter entry so a
  scan whose consumer went away never occupies a slot; and **bounded** — with no
  free slot and the waiter queue at `max_waiters`, admission is shed as
  `Overloaded` (metric `ferrosa_sched_admissions_rejected_overload_total`)
  rather than piling up. `submit_scan` takes the cancel signal and returns
  `ScanOutcome`; storage's `range_iter*` producers route through
  `spawn_bounded_range_scan`, passing the consumer channel's `closed()` as the
  cancel signal and failing loud on overload (never a silent empty stream).
- `SchedPool` — wraps `FairAdmit`. `submit_scan(class, chunk_budget, f)` + a
  `ScanSlot`: the producer calls `slot.tick()` per produced chunk and every
  `chunk_budget` chunks re-competes for its slot in vruntime order, so a long
  full-table scan cedes to more-deserving scans. `submit`/`submit_blocking` are
  the generic (Bulk-weight) entries.
- `runqueue::{RunQueue, SchedEntity, weight_for_class}` +
  `scheduler::{advance_vruntime, should_switch}` — the pure vruntime primitives
  `FairAdmit` is built from: a pick-min run queue with a monotonic `min_vruntime`
  floor, service charged inversely to weight, and the strictly-more-deserving
  yield rule.
- `group_runqueue::GroupRunQueue` (**B3**) — the **hierarchical** (per-tenant)
  dimension. A two-level fair queue: the outer level schedules groups (tenants)
  by group `vruntime`, the inner level schedules a group's queries (a nested
  `RunQueue`). A group's `vruntime` advances by the service of *any* of its
  queries, so a tenant's aggregate share is independent of how many queries it
  runs (`GroupId` is an opaque `u64` — the caller maps `TenantContext` → id,
  keeping the crate leaf).
- `io_permits::IoPermits` (**B2**) — the **I/O** resource dimension. Where
  `FairAdmit` bounds concurrent scan *compute*, this bounds concurrent bulk
  *I/O* (`Lane::Bulk`): at most `capacity` `IoPermit`s are held at once, so a
  fan-out of S3-reading scans cannot saturate the shared I/O path and starve the
  reserved lanes. Async acquire (an I/O-bound waiter is a cheap task, the B0
  property); RAII permits returned on drop, including panic/cancel unwind.

## Dependencies / dependents

- **Depends on:** `tokio` only (a leaf crate — the DSM guard keeps
  storage/cluster/cql out).
- **Dependents:** `ferrosa` (constructs the pool at boot), `ferrosa-storage`
  (routes scan producers through `submit_scan`), `ferrosa-cql`
  (`ScanPlan::sched_class` — B1 T1.4 classifier).

## Status

- **B0 (PR #286):** T0.1 pool, T0.2 `max_blocking_threads`, T0.3 route scan
  producers, T0.5 headroom metrics, T0.6 live no-step-down regression.
- **B1 (PR #287):** T1.1 run queue, T1.2 cooperative yield in the `store.rs`
  producers, T1.3 chunk-budget tripwire, T1.4 `ScanPlan` classifier, T1.5
  interactive bypass, T1.6 fairness proptests, T1.7 no-lock audit.
- **B1.5 (PR #287):** `FairAdmit` — the vruntime scheduler is now the **live
  admission authority** (replacing the FIFO semaphore); weighting is real and
  proven by test (`foreground_gets_roughly_four_to_one_over_bulk`). The
  standalone `Scheduler`/`SchedTicket` abstraction was removed as superseded.
  Every scan through the pool is currently a full-table `Bulk` scan (Foreground
  reads bypass — T1.5), so the 4:1 weighting is latent until B3 folds
  mixed-weight background work (compaction/repair/ANN) into the pool.
  Admission is cancellable + bounded (`Overloaded` backpressure), and range-scan
  readers open only after a slot is granted.
- **B2 (PR #288):** the I/O dimension.
  - T2.1 — `io_permits::IoPermits` bounded bulk-I/O permit pool, now a
    first-class `SchedPool` member: an admitted scan holds one permit for its
    producing life (the `Lane::Bulk` reservation, sized to CPU capacity by
    default, lower it to reserve I/O headroom). Gauges
    `ferrosa_sched_io_permits_{capacity,in_flight,acquired_total}`.
  - T2.2 — `vruntime` advances on **I/O wait**: `ScanSlot::tick` charges
    `vruntime` by the chunk window's measured **wall-elapsed µs** (CPU + I/O),
    not a chunk count, so an I/O-bound scan is throttled to equal *total
    service* (test `service_time_accounting_equalizes_io_bound_and_cpu_light_scans`).
  - T2.3 — DD-1 pinned to weighted wall-elapsed (`ADR-022`) with a microbench
    (`examples/vruntime_unit_bench.rs`; ~30 ns/chunk).
  - T2.4 — permit-leak invariant (RAII returns the permit on panic/cancel).
  - Remaining refinement: acquire the I/O permit at the *per-I/O operation*
    (release the CPU slot while blocked on S3) rather than per-scan — the deeper
    `ferrosa-net`/storage seam integration.
- **B3 (in progress):** `group_runqueue::GroupRunQueue` — T3.1 two-level
  (group→query) fair queue with the T3.9 anti-gaming property (1 tenant × 100
  queries == 1 tenant × 1 query) and a T3.2 weight preview (share ∝ weight),
  plus an `advance_vruntime` floor so a high-weight group can't monopolize via
  integer-division rounding. T3.8 (bounded runqueue + `Overloaded`) already
  shipped in B1.5. Next: make it live in `FairAdmit` (thread the tenant group
  through `submit_scan`), per-tenant weights from TOML (T3.2), and fold the
  background work — compaction/repair/index-build/ANN — into the pool as the
  `system` group (T3.3–T3.6), where the weighting finally arbitrates.
