---
type: todo
priority: P0
status: triage
created: 2026-04-28
updated: 2026-04-28
---

# Bug: commit log replay OOMs on large log (eager Vec accumulation)

## Why this is a Ferrosa bug

Crash recovery must be bounded in memory. A node whose commit log has grown
beyond the container memory limit must still be able to come back up — that is
the entire point of having a WAL. Today, replay loads every undominated
mutation from every segment into a single `Vec<Mutation>` before the engine
applies anything, so a node with a 3 GB log on a 2 GB container is
unrecoverable: it crashes mid-replay, restarts, replays the same logs, OOMs
again, and stays in a boot loop until an operator deletes data.

The on-disk segments are bounded (~32 MB each). RAM use during replay should
also be bounded — by one segment, not by the whole log.

## Observed on

- Ferrosa: workspace at `/Users/bkearns/src/ferrosa-suite/ferrosa`,
  `ferrosa-storage/src/commitlog/mod.rs::open_and_replay` as of 2026-04-28
  (HEAD).
- Cluster: `ferrosa-memory` 3-node podman cluster
  (`/Users/bkearns/src/ferrosa-suite/ferrosa-memory/docker-compose.yml`).
- Symptom on `node1`:
  - `~/data/ferrosa-memory/node1/commitlog`: 3.1 GB, 100 segment files
    (`commitlog-186.log` … `commitlog-284.log`), ~32 MB each.
  - Container `mem_limit: 2g` (`docker-compose.yml`).
  - Container exits 137 (`OOMKilled = true`) ~30 s after the
    `existing commit log segments found — replaying for crash recovery` line.
  - Restart loop — every retry redoes the same work and dies the same way.
- `node2` and `node3` are unaffected (commit logs are 7 MB / 42 MB).

## Symptom

```
INFO ferrosa: ferrosa starting
INFO ferrosa: existing commit log segments found — replaying for crash recovery
[~30s pass — no further log lines]
<container exits 137, OOMKilled>
```

Followed by the libpod restart cycle, repeating indefinitely.

## Root cause

`CommitLog::open_and_replay` (in `ferrosa-storage/src/commitlog/mod.rs:136`)
iterates every segment file, calls `SegmentReader::read_all()` per segment,
and pushes every undominated mutation into a single `Vec<Mutation>`:

```rust
let mut mutations = Vec::new();
for (_, path) in &segment_files {
    let mut reader = SegmentReader::open(path)?;
    let entries = reader.read_all()?;
    for (pos, mutation) in entries {
        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
        let dominated = checkpoint.get(&table_id).is_some_and(|cp| pos <= *cp);
        if !dominated {
            mutations.push(mutation);
        }
    }
}
// Files deleted here, before mutations are applied.
for (_, path) in &segment_files {
    let _ = fs::remove_file(path);
}
```

The Vec is then handed back to `main.rs`, held while `register_system_tables`
runs, and finally consumed by `StorageEngine::replay_mutations` — which builds
an additional `HashSet<[u8;16]>` over every mutation_id for dedup. Peak RSS
during replay is therefore at least:

```
sum(undominated entries) * (Mutation in-memory size)
  + sum(undominated entries) * 16 bytes (dedup HashSet)
  + one segment's raw bytes (~32 MB) held in SegmentReader.data
```

For the observed 3.1 GB log this comfortably exceeds the 2 GB container
limit. The 32 MB segment cap does not help because the loop never drops
prior segments' decoded mutations.

## Why the log got this big in the first place

This bug is the **immediate boot-loop trigger**. The **upstream cause** —
why the log was 3 GB instead of a few hundred MB — is that
`discard_completed` only deletes a segment once **every** dirty table in it
has flushed past its position. Tables that never flush (no rows, infrequent
writes, or memtable too small to hit the flush threshold) keep their
segments alive forever. Filed separately as
`bug-commitlog-segments-retained-for-cold-tables.md`.

Both bugs need fixing. Even with the retention bug fixed, replay must remain
bounded in memory so a one-time spike (heavy ingest then crash) doesn't
trap the node in a boot loop.

## Proposed fix (TDD)

Replace the eager-Vec accumulation in `open_and_replay` with streaming
per-segment apply. Two parts:

1. **Don't delete segment files inside `open_and_replay`.** Move that to a
   new `commit_log.discard_replayed_segments(&[u64])` called from the engine
   *after* `replay_mutations` has succeeded. This lets us re-read segments
   incrementally without holding the whole log in RAM at once.

2. **Make replay a streaming iterator.** Either:
   - (a) Add `CommitLog::replay_segment(segment_id, callback: impl FnMut(Mutation))`
     that opens one segment, decodes entries, calls the callback for each
     non-dominated mutation, and frees the segment's bytes before returning.
     The engine calls this once per segment and applies/defers each mutation
     immediately. Peak RAM = one segment + one mutation + dedup set.
   - (b) Or, expose `SegmentReader::iter_entries()` (a streaming iterator
     over `(CommitLogPosition, Mutation)`), then have the engine drive the
     loop and call `apply_replay_mutation_if_registered` per entry.

Either approach caps replay memory at `O(segment_size + |dedup_set|)`.

The dedup set itself can become large for very long logs — a follow-up could
hash mutation_ids into a Bloom filter or scope the set to the segment being
replayed, since within a single segment IDs are already unique. Out of
scope for the P0 fix.

## Test plan (red first)

Add `ferrosa-storage/src/commitlog/mod.rs::tests::open_and_replay_is_bounded_memory`:

- Build a synthetic commit log directory with N=200 segments of ~1 MB each
  (~200 MB total), mutations targeting a table that has no checkpoint entry
  (so all are undominated).
- Wrap the allocator (or use `tikv-jemallocator` stats / a simple
  `peak_alloc` shim already present in the workspace if any) to capture peak
  resident memory during `open_and_replay`.
- Assert peak alloc is under, e.g., 32 MB (one segment) + small constant.

Currently this should fail because peak alloc grows linearly with N.

After the fix the test passes.

Add a second test
`open_and_replay_continues_after_partial_apply_failure` that simulates a
crash between segment 5 and segment 6 (e.g., an injected error in the apply
callback for segment 6) and verifies that on the next call to
`open_and_replay`, segments 1–5 have been deleted (because the engine called
`discard_replayed_segments` for them) but segment 6+ are still present and
get retried.

## Operational mitigation while the fix lands

Until the fix ships, an operator can recover a stuck node by raising the
container's `mem_limit` past the size of its commit log. **Do not delete
the commit log to escape the loop** — that drops any unflushed writes.
For the local dev cluster, raising `node1`'s `mem_limit` to 6 GB in
`docker-compose.yml` is enough to absorb a 3 GB replay.

## Related

- `archive/bug-commitlog-replay-panics-on-torn-tail.md` — earlier replay
  bug; same code path, different failure mode (panic vs OOM).
- `bug-commitlog-segments-retained-for-cold-tables.md` — upstream cause
  (steady-state bloat) that made this OOM reachable in normal operation.
