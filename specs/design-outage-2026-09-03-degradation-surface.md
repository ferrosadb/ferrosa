---
title: One degradation surface for storage faults
status: design
date: 2026-09-03
executive_summary: >
  The 2026-09-03 control-cluster outage repeated the shape of the 2026-08-20
  outage through a different axis. Readiness was taught about consensus health
  and never about storage health, so a node that had not written an SSTable in
  five days answered {"ready":true} throughout. This design routes writability,
  sustained flush failure and ring membership through the health surface that
  already exists in ferrosa-storage, edge-triggered, so one fault produces one
  log line, one Sentry event, one metric transition and a 503.
---

# One degradation surface for storage faults

## The finding that shapes this design

`ferrosa/src/web/readiness.rs` carries a comment written after the 2026-08-20
outage:

> A degraded CLUSTER is not ready, and grouping it with the pair modes above is
> what made the 2026-08-20 outage invisible. node1 sat outside the cluster for
> hours ... while this endpoint returned 200 `{"ready":true}` throughout. Every
> health check believed it, so nothing routed away and nobody was paged; it was
> found by a person noticing their task board was down.

On 2026-09-03 the same sentence was true again, with one word changed: it was
found by a person noticing they could not sign in from their phone. The
difference is the axis. `readyz_handler` takes `State<Arc<ModeController>>` and
nothing else — it can only ask questions about consensus. A node whose flushes
had failed with `EACCES` on every attempt for five days satisfied every
consensus condition and was therefore "ready".

**So the fix is not another special case in the match arm.** The 2026-08-20 fix
added one, and the next fault arrived through the axis that still had none. The
fix is to give readiness a second input.

## Current state

| Piece | Where | What it knows |
|---|---|---|
| `readyz_handler` | `ferrosa/src/web/readiness.rs` | Deployment mode, consensus supervision, Raft leader |
| Self-heal health surface | `ferrosa-storage/src/self_heal/metrics.rs` | Corrupt SSTables, a `DEGRADED` flag, `HealthEntry` lines |
| Flush / compaction | `ferrosa-storage/src/flush.rs`, `compaction/executor.rs` | Logs `ERROR` per failed attempt; reports nowhere |
| Data directory | opened implicitly on first write | Nothing checks it is writable |

The health surface already exists and its module doc already states the
intended role:

> The health surface is a small mutexed snapshot the web/metrics endpoint reads
> to answer "does this node have a data issue, and what is the controller doing
> about it?".

It has `set_degraded`, a `DEGRADED` gauge, and `HealthEntry`. It is currently
written only by self-heal. **This design widens its writers, not its concept.**

## Design

### A single degradation registry

Promote the self-heal health surface to a crate-level `ferrosa_storage::health`
with one reason type. Self-heal keeps its existing entries; two new writers join
it.

```rust
pub enum Degradation {
    /// A data-dir subpath the engine must write is not writable by this uid.
    DataDirUnwritable { path: PathBuf, uid: u32, owner: String },
    /// Flush or compaction has failed continuously for a table.
    FlushFailing { table: TableId, since: SystemTime, consecutive: u32 },
    /// Existing self-heal escalation.
    CorruptSSTable { table: TableId, reason: EscalateReason },
}
```

### Edge-triggered, because the signal already fired 14,000 times

The flush failure was logged on every attempt for five days. The signal was
never missing; it was indistinguishable from background noise, and it buried
everything else. Per the standing order, transitions are the event:

- **Entering** degradation: one `tracing::error!` naming the reason. This is the
  one line that becomes the Sentry event, and the one an operator greps for.
- **Leaving** it: one `tracing::info!` recording recovery and how long it lasted.
- **While** degraded: nothing. The gauge and `/readyz` carry the state.

Two lines per outage, however long it lasts.

### Readiness consults both axes

`readyz_handler` gains a storage-health input beside `ModeController`. Storage
degradation returns 503 with the reason named, in the existing fail-loud body
shape:

```json
{"ready":false,"waiting_for":"storage","detail":"data dir not writable: /var/lib/ferrosa/compaction owned by root:root, process uid 10001"}
```

Consensus rules are unchanged. A node is ready when **both** axes are healthy.

### Boot-time refusal

Precedent exists and was proven in this incident: the engine already refuses to
start on an unreadable `schema.json`, which is what surfaced the legacy-format
problem on node2 and node3 during the roll. `preflight::assert_data_dir_writable`
follows the same contract — create and unlink a probe file in each subpath the
engine writes, and refuse to start naming the path, the expected uid and the
actual owner.

The periodic re-check matters as much as the boot check: **the daemon here had
been running since Aug 28 and the directory went root-owned on Aug 29.** A
boot-only assertion would not have caught this incident at all.

## Component boundaries

| Item | Crate | Module | Depends on |
|---|---|---|---|
| Degradation registry | `ferrosa-storage` | `health` (from `self_heal::metrics`) | — |
| Writability preflight | `ferrosa-storage` | `preflight` | registry |
| Flush failure edges | `ferrosa-storage` | `flush`, `compaction::executor` | registry |
| Readiness second input | `ferrosa` | `web::readiness` | registry |
| Ring membership | `ferrosa-cluster` | `controller` | registry (or its own) |
| Sentry confirmation | `ferrosa` | `sentry_reporting`, `main` | — |

## Implementation order

1. **Sentry confirmation** (independent, ~20 lines). No dependencies; unblocks
   verifying everything below actually reports.
2. **Degradation registry** — the shared seam. Everything else depends on it.
3. **Writability preflight** (boot + periodic) — `t_af50874b`, the highest-value
   guard because it works regardless of *how* ownership went wrong.
4. **Flush failure edges** — `t_34748319`.
5. **Readiness second input** — consumes 2–4. Do this after its writers exist,
   or readiness gates on a surface nothing writes.
6. **Ring membership** — `t_86087eae`, but see below.

## What should NOT be built

- **No new TOML keys.** This project has been bitten by config nothing reads
  (`[s3]` endpoint/bucket/region are read by nothing). Thresholds go in code with
  named constants, or an env var that is tested.
- **No separate alerting daemon.** With `FERROSA_SENTRY_DSN` now set, an
  edge-triggered `tracing::error!` already becomes a Sentry event. Building a
  second notification path would be a third thing to keep alive.
- **No readiness flapping.** A single failed flush is not degradation. Require a
  sustained condition (consecutive failures or a duration) before flipping, and
  do not clear on one success.
- **Do not weaken the existing self-heal escalation** to fit the new enum. Its
  `NoHealthyReplica` refusal is a correct guard that fired truthfully throughout
  this incident.
- **`t_86087eae` (ring membership) may already be mostly covered.** `afb15310`
  now makes a degraded cluster member report 503, and Sentry is live. Verify what
  is still missing before building — the remaining gap may be only "someone is
  told", which is now Sentry's job, not new code's.

## Test strategy

| Item | Layer | Test |
|---|---|---|
| Preflight | unit | A dir the process cannot write is detected; a writable one passes; the error names path, uid and owner |
| Preflight | unit | Runs as a non-root uid in CI — a root-run test passes vacuously because root bypasses mode bits |
| Registry | unit | Entering logs once; staying degraded logs nothing further; leaving logs once with duration |
| Flush edges | unit | N-1 consecutive failures do not degrade; N does; one success clears only after the hysteresis rule |
| Readiness | unit | Storage-degraded returns 503 with the reason named, in every consensus mode that would otherwise be 200 |
| Readiness | unit | Consensus-healthy + storage-healthy is the only 200 |

The preflight root caveat is the sharp one: **a test that runs as root passes
vacuously**, because root ignores permission bits. That is the same reason the
production daemon runs as uid 10001 and would have caught this. The test must
assert it is not running as root, or skip loudly.
