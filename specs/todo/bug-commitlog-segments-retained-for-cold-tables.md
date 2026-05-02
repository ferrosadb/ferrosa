---
type: todo
priority: P1
status: triage
created: 2026-04-28
updated: 2026-04-28
---

# Bug: commit log segments retained indefinitely for never-flushed tables

## Why this is a Ferrosa bug

The commit log is a write-ahead log: segments exist to make memtable
mutations durable until the corresponding memtables are flushed to SSTables.
Once flushed, the segments serve no purpose and must be reclaimed. Today,
a segment is reclaimed only when **every** table that wrote to it has
flushed past its position in that segment. A single table that never flushes
(e.g., infrequent writes, small memtable, or a system table whose size
never crosses the flush threshold) keeps every segment it has ever touched
alive on disk forever.

This causes:

- Disk space leak proportional to uptime and write volume.
- A growing replay surface that increases time to recovery linearly.
- A direct trigger for the P0 OOM bug
  (`bug-commitlog-replay-oom-on-large-log.md`) once the log size exceeds
  container RAM.

## Observed on

- Ferrosa: `ferrosa-storage/src/commitlog/mod.rs::discard_completed` as of
  2026-04-28 (HEAD).
- `ferrosa-memory` 3-node podman cluster, `node1`:
  - 100 commit log segments retained (`commitlog-186.log` … `commitlog-284.log`),
    3.1 GB total.
  - Checkpoint `commitlog_checkpoint.json` records flushed positions for
    13 tables; max segment id seen is 186. All 98 segments after 186 are
    retained because some table that wrote to them has never recorded a
    flushed position. Tables present in segments but absent from the
    checkpoint include (inferred from segment contents and schema):
    - `agent_memory.episodes`
    - `agent_memory.relations`
    - several `system_*` tables
    that have at most a few hundred rows and never trigger a memtable
    flush.

## Symptom

- `du -sh ~/data/ferrosa-memory/node1/commitlog` grows monotonically across
  weeks of normal operation, never shrinking.
- `discard_completed` is called on every flush of a hot table but produces
  no segment deletions because cold tables hold the gate closed.
- Eventually the log exceeds container memory and the node enters the
  OOM boot loop documented in
  `bug-commitlog-replay-oom-on-large-log.md`.

## Root cause

`CommitLog::discard_completed` (`ferrosa-storage/src/commitlog/mod.rs:273`)
deletes a segment only when its `tables` map is empty:

```rust
if let Some(table_pos) = tables.get(table_id) {
    if *table_pos <= position {
        tables.remove(table_id);
    }
}
if tables.is_empty() {
    // ... delete segment file
}
```

There is no mechanism to force a flush for tables whose entries are
holding a segment alive, and no upper bound on how long a segment may
remain on disk waiting for the laggard table to flush.

## Proposed fix

Two complementary mechanisms:

1. **Force-flush-on-retention-threshold.** When the number of retained
   segments (or total commit log bytes) exceeds a configurable threshold,
   identify the oldest segment and force a flush of every dirty table
   that has entries in it. This bounds replay surface and disk use to
   `threshold * segment_size`.

2. **Periodic flush of cold tables.** A background tick (every 30 s, say)
   that asks each table whose dirty position in the oldest live segment
   is older than `max_segment_age_secs` (default 5 min) to flush. This
   handles the steady-state "never-flushed" case without needing the
   threshold to trip.

Both should be opt-out-able via config for tests. Force-flush should record
a metric (`COMMITLOG_FORCED_FLUSHES_TOTAL`) so operators can see when it
fires.

## Test plan

- Existing test `discard_completed_only_deletes_when_all_tables_flush`
  documents current (buggy) behaviour; keep it but rename to
  `discard_completed_waits_for_dirtiest_table_without_force_flush`.
- New: `force_flush_unblocks_segment_when_threshold_exceeded` —
  configure threshold = 3 segments, write to two tables A (hot) and
  B (cold), flush A repeatedly, verify B gets force-flushed when
  retention crosses 3 segments and the oldest segment is deleted.
- New: `cold_table_flushes_after_max_segment_age` — write to A and B,
  flush A, advance time past `max_segment_age_secs`, verify B's memtable
  flushes and the oldest segment becomes eligible for deletion.

## Related

- `bug-commitlog-replay-oom-on-large-log.md` — P0 boot-loop bug that this
  retention leak feeds.
