# Write-Path Memtable Backpressure

> Last updated: 2026-05-19
> Status: Implemented (PR #50)

## Motivation

Sustained-write workloads — anti-entropy repair's apply phase, bulk
load, PITR restore, raft state-machine catch-up — can drive the active
memtable past the soft `flush_threshold_bytes` boundary much faster than
the maintenance loop can drain it. The soft threshold only schedules
an *async* flush via `flush_if_needed`; if writes keep arriving, the
active memtable keeps growing and the flushing memtable (held in RSS
until the S3 upload confirms) compounds the resident set.

The forcing observation came from the fmem cluster on 2026-05-17, where a
repair apply session pushed node1's resident set to **1.4 GiB** inside a
single chunked Apply RPC and tripped the 2 GiB cgroup oom-kill. The
maintenance loop ran on a one-second tick; the writer outpaced it by
orders of magnitude.

The fix is a hard, synchronous, in-line flush trigger inside the write
path itself. When the active memtable crosses
`memtable_backpressure_bytes`, `StorageEngine::write` calls
`self.flush(table_id)` synchronously before returning, blocking the
writer until the in-progress flush completes. The store's `flush_guard`
(a `parking_lot::Mutex`) serialises concurrent flushes, so a runaway
writer waits for the in-progress flush instead of growing a second
unbounded memtable in parallel.

## Mechanism

Source: `ferrosa-storage/src/engine.rs`, in `StorageEngine::write`
after the commit-log append + memtable insert succeed.

```
let mt_size = state.store.memtable_size() as u64;
let needs_flush = mt_size >= self.config.memtable_backpressure_bytes;
drop(tables);

if needs_flush {
    self.flush(table_id)?;
}
```

The check is done **after** the write has been appended to the commit
log and inserted into the memtable, so the trigger is "we just made the
memtable too big" rather than "if we wrote now we would make it too
big". This keeps the write itself optimistically lock-free; the
backpressure cost is paid only by the writer who tipped the memtable
over the line.

The default behaviour is a no-op: the trigger only fires when memtable
size reaches `memtable_backpressure_bytes`, which is set high enough
that routine writes never hit it. Only sustained-write scenarios — where
the maintenance loop falls behind — drive the memtable into the
backpressure regime.

`memtable_backpressure_bytes` is distinct from `flush_threshold_bytes`.
The two thresholds form a soft / hard pair:

| Threshold | Behaviour | Trigger |
|-----------|-----------|---------|
| `flush_threshold_bytes` | schedule async flush | maintenance loop's periodic `flush_if_needed` |
| `memtable_backpressure_bytes` | synchronous in-line flush | in `StorageEngine::write` |

The async path handles routine threshold crossings without per-write
cost. The synchronous path catches the case where the async path can't
keep up.

## Configuration

`StorageEngineConfig::memtable_backpressure_bytes: u64` is the single
knob. Sources, in order:

1. `FERROSA_MEMTABLE_BACKPRESSURE_BYTES` environment variable (parsed
   via `StorageEngineConfig::from_env`).
1. Default: `max(flush_threshold_bytes * 4, 64 MB)`. With the default
   `flush_threshold_bytes = 64 MB`, this lands at **256 MB**. Tests
   with intentionally tiny thresholds (4 KB in `test_config`) don't
   trip the production backpressure path because the floor is 64 MB.

The factor-of-four spread between the soft and hard thresholds is
deliberate: it gives the async flush room to drain through one or two
maintenance ticks before the synchronous path kicks in. In a
well-behaved workload the active memtable hits `flush_threshold_bytes`,
the maintenance loop schedules an async flush within ~1s, and the
memtable never approaches the backpressure floor.

## Test posture

`StorageEngineConfig::test_config` sets `memtable_backpressure_bytes:
u64::MAX`, effectively disabling backpressure in every test by default.
This is required: many flush-behaviour tests pick intentionally tiny
`flush_threshold_bytes` values (1 byte, 4 KB) to make a single write
trip the threshold; if backpressure also fired at that size every one
of those tests would deadlock against the concurrent-flush test harness.

One regression test opts in explicitly to pin the contract:
`write_triggers_inline_flush_when_backpressure_exceeded` in
`ferrosa-storage/src/engine.rs`. It sets
`config.memtable_backpressure_bytes = 1`, performs a single write, and
asserts that `engine.sstable_count(&tid) == 1` *without* any explicit
`flush_if_needed` / `flush_all` / maintenance-tick call. That assertion
is what production-side sustained-write workloads (repair apply, bulk
load, PITR restore) rely on.

Tests that need backpressure to fire must follow the same opt-in
pattern: build a `test_config`, then mutate
`config.memtable_backpressure_bytes` before calling `StorageEngine::new`.

## Interaction with repair apply

The Apply phase of anti-entropy repair
([anti-entropy-repair-architecture.md](anti-entropy-repair-architecture.md))
is the canonical example of a sustained-write workload that needs
backpressure. Repair's chunked Apply flushes 64 partitions per RPC; on
a table with multi-MB partitions (the fmem `entity_store` has a
768-dim embedding column plus arbitrary text), a single Apply chunk
lands hundreds of MB of writes in milliseconds. The maintenance loop
cannot keep up, the soft threshold has no synchronous lever, and
without backpressure the memtable grows unboundedly until the cgroup
kills the node.

With backpressure on at the default `max(flush_threshold_bytes * 4, 64
MB)`:

1. The first Apply chunk lands in the memtable, pushing it past 256 MB.
1. The writer (`RepairApplyHandler::handle`) calls
   `storage.write(...)` on the row whose insert finally tips the
   memtable over.
1. That `write` call observes `mt_size >= memtable_backpressure_bytes`
   and invokes `self.flush(table_id)` synchronously.
1. `flush_guard` serialises with any in-flight async flush; the writer
   blocks until the active-memtable contents are durable on disk.
1. The synchronous flush returns, the writer returns success, and the
   Apply handler moves to the next row.

The net effect is that the Apply phase is *naturally* paced by the
receiver's flush throughput, not by the network. Repair sessions take
longer when the receiver is slow to flush, but the receiver does not
OOM.

## Limits

- The backpressure trigger is a hard threshold, not a soft gradient.
  Throughput at the threshold drops from "memtable-rate" to
  "flush-rate" in one step. A future refinement could introduce a
  gradual slowdown (e.g. inject a small sleep proportional to how far
  past the threshold the memtable is) so the throughput profile is
  smoother under sustained pressure.
- There is no per-table override. The same
  `memtable_backpressure_bytes` applies to every registered table on
  the engine. A high-write small-row table and a low-write large-row
  table cannot use different thresholds today.
- The threshold is checked on `memtable_size()`, which sums the active
  shard sizes. With `memtable_num_shards = 64`, one runaway shard can
  trip the threshold even when 63 other shards are nearly empty. The
  alternative — per-shard thresholds — is open work.

## Related Specs

- [Storage](storage.md) — `StorageEngineConfig`, flush threshold,
  memtable architecture
- [Anti-Entropy Repair Architecture](anti-entropy-repair-architecture.md) —
  the apply-phase workload that motivated this knob
- [`specs/archive/bugs-verified/refactor-cluster-recovery-storage-oom-seams.md`](archive/bugs-verified/refactor-cluster-recovery-storage-oom-seams.md) —
  the broader recovery / OOM seam refactor that this fix completes
