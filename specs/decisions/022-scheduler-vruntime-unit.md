# ADR-022: Scheduler `vruntime` accounting unit — measured wall-elapsed, weighted

> Date: 2026-07-22
> Status: Accepted
> Scope: `ferrosa-sched` — `ScanSlot::tick` (the per-chunk accounting seam) and
> `scheduler::advance_vruntime`. Resolves **DD-1** from the query-scheduler
> blueprint (`specs/proposed/query-scheduler/decisions.md`).
> Non-goal: the per-tenant group weights (B3) or the I/O permit sizing.

## Context

The fair scheduler orders scan admission by `vruntime` — virtual runtime,
service time scaled by the inverse of a class weight. B2 makes scheduling
**two-dimensional** (CPU compute + I/O wait): an I/O-bound scan that blocks on
S3 burns little CPU, and must still be throttled to its fair share rather than
getting unlimited free turns. That requires deciding **what unit `vruntime`
advances in** (DD-1). Three candidates:

1. **Count proxy** — advance by a fixed amount per chunk (the pre-T2.2 unit:
   `service = chunk_budget`). Cheapest, but blind to how long a chunk took, so
   an I/O-bound scan and a CPU-light scan accrue `vruntime` at the same rate per
   chunk — the I/O-bound scan gets *equal turns*, i.e. more wall-time. Unfair.
2. **Thread-CPU time** (`CLOCK_THREAD_CPUTIME_ID`) — advance by CPU cycles
   actually consumed. Excludes I/O wait *by construction*, which is exactly the
   signal the I/O dimension needs; also not in `std` (per-platform syscall,
   heavier than `Instant::now`).
3. **Measured wall-elapsed** — advance by the wall-clock time the chunk took,
   which spans both CPU compute and I/O wait.

## Decision

`vruntime` advances by **measured wall-elapsed microseconds per chunk window,
weighted by `SchedClass`**. `ScanSlot::tick` accumulates `last_tick.elapsed()`
into `window_micros`, and at each `chunk_budget` boundary charges that window's
elapsed µs as the service passed to `advance_vruntime` (which divides by the
class weight). Floor at 1 µs so a sub-microsecond window still advances the
clock.

## Rationale

- **Captures the I/O dimension for free.** Wall-elapsed includes S3 wait, so an
  I/O-bound scan (slow chunks) accrues `vruntime` proportional to the wall-time
  it holds the pool — and is throttled to equal *total service*, not equal
  turns (test: `service_time_accounting_equalizes_io_bound_and_cpu_light_scans`).
  This is the "vruntime advances on I/O wait" requirement (T2.2).
- **Cheap.** Microbench (`examples/vruntime_unit_bench.rs`,
  `cargo run --release --example vruntime_unit_bench -p ferrosa-sched`):
  `Instant::now() + .elapsed()` costs **~30 ns/chunk**. A chunk is 64 partitions
  and a partition decode is microseconds to milliseconds, so the overhead is
  well under 0.01% of chunk time.
- **Portable and simple.** `std::time::Instant` only — no platform syscall, no
  new dependency, consistent with the crate's leaf (tokio-only) constraint.
- **Self-correcting.** Wall-time includes scheduler-preemption noise, but the
  run queue's `min_vruntime` floor and the pick-min rule average it out over
  windows; a single noisy chunk cannot let a scan monopolize or be starved.

## Consequences

- I/O-bound scans are throttled to their fair share despite low CPU (the B2 goal)
  with no separate I/O-time accounting path — one elapsed measurement serves both
  dimensions.
- The absolute `vruntime` magnitude is now in microseconds, not chunk counts;
  nothing external depends on the magnitude (it is only compared *between* queued
  scans), so this is internal.
- The bulk-I/O **permit** pool (`IoPermits`, T2.1) remains the complementary hard
  *concurrency* cap on the I/O dimension; this ADR governs only the fair-share
  *accounting* unit.

## Alternatives considered

- **Count proxy** — rejected: blind to I/O wait (the failure this ADR fixes).
- **Thread-CPU time** — rejected: excludes I/O wait, non-portable, heavier per
  measurement; revisit only if wall-time preemption noise is ever shown to cause
  measurable unfairness (none observed).
- **Row/byte proxy** (advance by rows or bytes produced) — rejected: correlates
  with neither CPU nor I/O reliably (a wide-row decode and a cheap tombstone skip
  differ by orders of magnitude in cost but not in row count).
