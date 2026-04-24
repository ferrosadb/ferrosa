---
type: todo
priority: P0
status: fix-applied-pending-image-rebuild
created: 2026-04-23
updated: 2026-04-23
---

# Bug: commit log replay panics on torn-tail segments

## Why this is a Ferrosa bug

Commit log replay runs on every startup after a crash. It must not
panic — a panic turns a recoverable node into a crash loop, which is
what happened to two of the three nodes in the local ferrosa-memory
dev cluster today. Torn writes at segment tails are a routine consequence
of SIGKILL, host reboot, or the container runtime killing a process
mid-write. The reader already has a "skip to next sync marker" code path
for CRC failures; a torn tail must fall into the same graceful-skip path,
not a slice-bounds panic.

## Observed on

- Ferrosa commit: `c47bfa8` (branch `fix/mixed-client-topology-and-typed-edge-bugs`)
- Cluster: local 3-node podman cluster from
  `/Users/bkearns/src/ferrosa-memory/docker-compose.yml`
- Two of three nodes crash-looped on startup:
  - `node1`: segment `commitlog-173.log`, 786432 bytes
  - `node3`: segment `commitlog-101.log`, 655360 bytes

## Symptom

```
INFO ferrosa: existing commit log segments found — replaying for crash recovery

thread 'main' (1) panicked at ferrosa-storage/src/commitlog/reader.rs:154:41:
range end index 786571 out of range for slice of length 786432
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
```

`node3` shows the same panic with different numbers (`655361 out of range
for slice of length 655360`). Both nodes are stuck in `restart:on-failure`
loops, visible in podman `State.ExitCode=101` and
`/tmp/ferrosa-memory-watchdog.log`.

## Root cause

`SegmentReader::read_all` in `ferrosa-storage/src/commitlog/reader.rs`
computes `section_end` from the sync marker's `next_marker_offset`
without clamping to the actual file length:

```rust
let section_end = if next_marker_offset == 0 {
    self.data.len()
} else {
    next_marker_offset as usize     // <-- unbounded
};
```

The entry-parse loop then relies on `entry_end > section_end` as its
bounds guard. If a torn write leaves a valid sync marker on disk with a
`next_marker_offset` that points past the physical end of the file
(because the bytes past it never reached disk), `section_end` exceeds
`data.len()`. The guard passes, execution reaches
`&self.data[payload_start..payload_end]` on line 154, and the slice
panics.

The sync marker itself is valid — its CRC check passes because the
length and CRC fields are the last 8 bytes of the flushed prefix. What's
missing are the bytes the marker points *to*.

## Fix

Clamp `section_end` to `self.data.len()`:

```rust
(next_marker_offset as usize).min(self.data.len())
```

With the clamp, the existing `entry_end > section_end` check at line 148
fires on the torn entry and the reader `break`s cleanly out of the
section — matching the graceful-skip behavior for CRC failures.

## Regression test

`torn_tail_past_sync_marker_does_not_panic` in
`ferrosa-storage/src/commitlog/reader.rs`. Constructs a segment with a
valid entry, truncates the file mid-payload, and patches the initial
sync marker so `next_marker_offset` points past EOF with a valid CRC.
Without the fix the test panics with the same "range end index out of
range" signature; with the fix it returns zero entries.

## Status

- [x] Fix applied (`reader.rs` lines 108–116)
- [x] Regression test added and passing
- [ ] Node container image rebuilt
- [ ] `node1` and `node3` restarted cleanly

## Recovery for the affected cluster

With the fix in place, the torn segments replay correctly on restart:
entries before the torn record are recovered, the torn record and
anything after it in that section are dropped. Entries in earlier
intact segments on `node3` (three ~32MB files from the prior day) are
untouched.

Data that was acked at CL=QUORUM and not yet flushed to SSTable on
either of these nodes is still durable on `node2` (which has been
healthy throughout) and will re-replicate via hinted handoff. Writes
that were acked only at CL=ONE to `node1` or `node3` and had not yet
replicated may be lost; per ferrosa-memory defaults this should be a
small or empty set.
