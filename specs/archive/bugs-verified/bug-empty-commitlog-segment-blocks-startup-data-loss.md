---
type: todo
priority: P0
status: draft
created: 2026-04-27
updated: 2026-04-27
---

# Bug: zero-byte commit-log segment causes refuse-to-start (effective data loss)

## Why this is a Ferrosa P0

A Ferrosa node refuses to start if any file in its `commitlog/` directory
is shorter than the segment header (currently 0 bytes is the trivial case).
The error is hard-fatal:

```
Error: InvalidFormat("segment file too short: 0 bytes (need at least 17)")
```

The node then enters a restart loop and the cluster cannot recover without
operator intervention (deleting the offending file). On a multi-node cluster
where multiple nodes hit the same condition simultaneously (e.g. host reboot,
podman VM OOM, simultaneous SIGKILL), the entire cluster is unrecoverable
without a human in the loop.

**Data integrity guarantee violated.** A storage engine MUST be able to come
back up against its own on-disk state without an operator needing to wipe
or surgically delete files. Today's behaviour means a routine kill-during-roll
is functionally indistinguishable from data corruption — the operator is
forced to choose between (a) deleting the segment (silent data loss if the
segment had been written to) and (b) leaving the cluster down.

## Observed on

- Ferrosa commit: `00915b2` (origin/main, fetched 2026-04-27)
- Cluster: 3-node ferrosa-memory dev cluster (image
  `localhost/ferrosa-memory-node:latest`, sha `bd1dfbc213ab`)
- Two of three nodes (node1, node3) refused to start. node2 started
  cleanly. Both failing nodes had a single 0-byte segment with mtime
  2026-04-27T08:29 — created at the moment of the previous shutdown.

## Forensic snapshot (preserved at /tmp/ferrosa-node1-forensics-*)

```
$ ls -la ~/data/ferrosa-memory/node1/commitlog/
-rw-r--r--  1 bkearns staff  1286 Apr 26 02:58 commitlog_checkpoint.json
-rw-r--r--  1 bkearns staff     0 Apr 27 08:29 commitlog-185.log
```

`commitlog_checkpoint.json` references `segment_id: 185` with non-zero
offsets for several tables — meaning segment 185 had previously been
flushed at a non-zero offset. The 0-byte file with the same name was
created at the moment of crash/shutdown and never written. (Whether
this is a roll-to-new-segment race or a mid-fsync truncation is the
follow-up question; the fact that startup hard-fails on it is the P0.)

## Reproduction (synthetic)

```bash
mkdir -p /tmp/ferrosa-empty-seg-repro/commitlog
touch /tmp/ferrosa-empty-seg-repro/commitlog/commitlog-1.log
# point a node at this dir → fails with InvalidFormat at startup
```

## Root cause (reader-side, the P0 part)

`ferrosa-storage/src/commitlog/reader.rs:49` hard-errors if a segment
file is smaller than `HEADER_SIZE`. `ferrosa-storage/src/commitlog/mod.rs::open_and_replay`
at the `?` on `SegmentReader::open(path)?` propagates that error and
aborts replay — the engine never starts.

A 0-byte segment carries no records by construction (the writer cannot
have logged anything before the header was written). Treating it as an
unrecoverable corruption is incorrect: it's a known torn-create state.

## Root cause (writer-side, the durability follow-up)

The fact that 0-byte segments can land on disk under their canonical name
is a separate, lower-priority issue. The fix is a write-then-rename or
`O_TMPFILE`-then-linkat pattern so that no segment ever appears under its
final name without at least the header durably synced. That fix is more
invasive and is **not** required to close the P0 — it is filed as a
follow-up.

## Fix (P0, this bug)

In `ferrosa-storage/src/commitlog/mod.rs::open_and_replay` (and the symmetric
path in `replay_from`), tolerate an empty segment file:

- If the file is 0 bytes: log a `WARN` with the path and segment id, skip
  it, and continue. The segment-cleanup loop already deletes the file at
  the end of replay.
- If the file is `>0` and `< HEADER_SIZE`: this *is* corruption (writer
  wrote bytes but didn't finish the header). Today's hard error is
  appropriate but should still surface a clearer message and a metric.
  Do not silently extend tolerance into the partial-header case.

A new metric `FERROSA_COMMITLOG_EMPTY_SEGMENT_SKIPPED_TOTAL` should
increment whenever a 0-byte segment is skipped, so a runaway producer
of empty segments is loud in observability.

## Acceptance criteria

1. Synthetic repro (a fresh `commitlog/` containing a single 0-byte
   `commitlog-N.log` and a valid `commitlog_checkpoint.json`) starts
   the engine successfully.
2. The node1 + node3 forensics directories under
   `/tmp/ferrosa-node1-forensics-*` (preserved as the regression
   fixture) start cleanly with all SSTable / Raft / Accord state intact.
3. A unit test in `ferrosa-storage/src/commitlog/reader.rs` or
   `mod.rs` covers both branches: 0-byte (tolerated, skipped) and
   `0 < n < HEADER_SIZE` (still errors).
4. `ELECTION_STORM_TERM_JUMPS_TOTAL` and the new
   `FERROSA_COMMITLOG_EMPTY_SEGMENT_SKIPPED_TOTAL` are surfaced in the
   metrics endpoint and alertable.

## Out of scope (filed as follow-ups)

- Writer-side prevention (write-then-rename / `O_TMPFILE`).
- Recovery semantics for the partial-header (`0 < n < HEADER_SIZE`)
  case — current behaviour is correct (hard-fail) but the operator UX
  needs better breadcrumbs.

## Forensic analysis: how the 0-byte segment-185 was produced

Reviewed `/tmp/ferrosa-node1-forensics-1777318131/`:

- `commitlog_checkpoint.json` mtime = 2026-04-26 02:58 PDT, in-file
  `timestamp = 2026-04-26T09:58:11Z`. References `segment_id: 185` at
  non-trivial offsets across many tables (e.g. `system_graph_agent_memory.adjacency`
  at offset 7 487 860, `system_auth.role_permissions` at 7 490 967).
- `commitlog-185.log` mtime = 2026-04-27 08:29 PDT, length 0.
- ~30 hours separate the checkpoint write from the file's last mtime.
  The file's mtime equals the moment of the previous (killed) shutdown.

The writer creates the segment file lazily — `Segment::new`
(`ferrosa-storage/src/commitlog/segment.rs:135`) only computes the path;
no I/O. The file appears on disk on the *first* `flush_to_disk`
(`segment.rs:357`), which uses:

```rust
fs::OpenOptions::new()
    .create(true)
    .truncate(true)
    .write(true)
    .open(&self.path)?;          // (A) — file exists at 0 bytes
file.write_all(&buf[..pos])?;     // (B)
file.sync_all()?;                 // (C) — bytes durably synced
```

Between (A) and (C), the on-disk file has length 0. A SIGKILL or host
power-loss in that window leaves a 0-byte segment with the canonical
name. `force_full_flush` (`segment.rs:419`) uses the same
`create+truncate+write+sync` pattern — and importantly, it runs
*unconditionally* (every catch-up replay, not just first-flush), so it
re-opens existing populated segments with `O_TRUNC` and is a *second*
crash window where a previously-7.5 MB file collapses to 0 bytes.

In this incident the most plausible sequence is:

1. R1 ran fine through 2026-04-26 02:58 PDT, flushed many tables,
   committed checkpoint with segment 185 at offset ~7.5 MB.
2. R1 (or a subsequent run) called `force_full_flush` — used by
   `engine.rs:2282` (`force_sync` path) — on segment 185 again.
3. The container/process was killed between the `O_TRUNC` open and the
   following `sync_all`, leaving commitlog-185.log at 0 bytes. (Mtime
   2026-04-27 08:29 confirms the kill moment.)
4. The next start (R2) hit `SegmentReader::open` on the 0-byte file →
   `InvalidFormat` → refused to start → preserved by operator into
   `/tmp/ferrosa-node1-forensics-*` and the cluster ran 2/3 until the
   fix landed.

This matches the spec's observation that node1 and node3 both crashed
at the same wall-clock moment with the same shape — a host-level event
(podman VM OOM, host reboot, simultaneous SIGKILL) caught both nodes
mid-flush. node2 happened not to be flushing.

**Ferrosa-side conclusion**: the writer's `O_TRUNC` rewrite path
(`force_full_flush`, and the first-flush path of `flush_to_disk`) is
not crash-safe — any kill between truncate and `sync_all` produces
exactly the observed 0-byte segment. The reader-side fix in this PR
makes that crash window survivable; the writer-side fix
(write-temp-then-`renameat`) remains tracked as the follow-up.

## Operator runbook (until fix lands)

1. Stop the failing node.
2. Identify the 0-byte segment:
   `find ~/data/ferrosa-memory/<node>/commitlog -size 0c -name '*.log'`
3. Snapshot for forensics: `cp commitlog-N.log /tmp/forensics/`
4. Delete the 0-byte file.
5. Restart the node.

This runbook *should not be needed* once the fix is in.
