# BUG (P0): SSTable writes are not crash-atomic — a kill mid-flush corrupts committed data and loses the WAL copy

**Status**: Open — fix on branch `fix/sstable-crash-atomic-writes`.
**Severity**: P0 — durable data corruption + data loss from an ordinary process kill. Violates the project "crash over corruption" failure philosophy.
**Component**: `ferrosa-storage` (`flush.rs`, `engine.rs`, `compaction/executor.rs`), with a defense-in-depth touch in `ferrosa-sstable` (`reader.rs`).
**Found**: 2026-06-05, root-causing ferrosa-memory SSTable corruption after cgroup OOM-kills.

## Observed

After nodes were OOM-killed (SIGKILL) mid-write, many *committed* SSTables had a `Data.db`
shorter than their own index/Statistics claim:

```
read_exact_at: wanted 40245 bytes, got 474
corrupted DeletionTime flags: 0xa1
```

Rows in those generations were permanently lost (effective RF=1, no replica to heal from).
Tables gutted: `temporal_edges` (→10 rows), `context_segments` (→13); 242 `entity_store`
SSTables quarantined.

## Root cause — two gaps, both from a missing fsync barrier

The flush path (`FileFlushTarget::flush_files` / `flush`, `ferrosa-storage/src/flush.rs:886-1045`)
writes components to `*.tmp` then `std::fs::rename` to final `{gen}-*.db`. It does **temp+rename
but never fsyncs**:

- **No `fsync` of the component files** (only the *staging* Data.db is synced in
  `ferrosa-sstable/src/writer.rs:155,1299,1313`; Partitions/Statistics/Filter/Rows/TOC are
  written with plain `std::fs::write` and never synced).
- **No `fsync` of the containing directory** — the renames that create the final directory
  entries are never made durable.

### Gap 1 — corruption
A crash after the rename to final names but before page-cache writeback leaves a final-named
`{gen}-Data.db` whose bytes are partly lost → truncated committed SSTable. `cleanup_stale_tmp_files`
(`flush.rs:596`) ignores it (not `*.tmp`). Startup (`load_existing_sstables_and_sidecars_with_repair_mode`,
`engine.rs:2849`) discovers generations purely by presence of `{gen}-Data.db`, quarantines only
*zero-byte* critical components (`engine.rs:2909-2951`), and `SSTableReader::open` (`reader.rs:222`)
reads only small components — it never compares `Data.db` length to the index's claimed extent. So a
truncated-but-nonzero Data.db loads as a valid generation and fails only at query time.

The in-code comment at `flush.rs:902-904` ("If the process crashes mid-flush, only .tmp files exist —
no corrupt final SSTables") is **false without fsync**: data-block durability and rename durability are
independent.

### Gap 2 — WAL discarded before durability (data loss)
`StorageEngine::flush` (`engine.rs:5196`) calls `commit_log.discard_completed(table_id, pos)`
(`engine.rs:5265`) immediately after `store.flush()` returns (`:5213`) — but `store.flush()` returns
*before any fsync*. The WAL checkpoint is advanced and segments deleted while the SSTable is still only
in page cache. If the flush tore (Gap 1), the only other copy of those rows is already gone; replay
starts after the discarded position and cannot rebuild them.

### Compaction has the same gap
`compaction/executor.rs:870` promotes its output via the same `flush_files` path. The manifest swap /
input deletion in `compaction/finalize.rs` is gated on S3 confirmation, but the **local** output SSTable
has the identical non-durable rename.

## Fix

Apply temp → fsync-file → rename → fsync-dir, and only then signal "committed":

1. **`flush.rs` `flush` and `flush_files`**: fsync **every** promoted component file (Data, Partitions,
   Rows, Filter, Statistics, TOC, CompressionInfo) before/at rename, then **open `base_dir` and
   `sync_all()` once** after all renames. Return Ok only after the directory fsync. (The fsync helper
   pattern already exists — `writer.rs` `sync_data`, `quarantine.rs:194`, the commit log, `accord/sync_writer.rs`.)
2. **`engine.rs::flush`**: ensure `commit_log.discard_completed` runs only after the SSTable is durably
   fsynced (fix #1 makes `store.flush()` return post-fsync, making the existing order correct; add an
   explicit barrier/comment so it cannot regress).
3. **Compaction** (`compaction/executor.rs:870`): inherits #1; confirm the manifest swap / input deletion
   is sequenced after the now-durable local promote.
4. **Defense-in-depth** (`reader.rs:222` open, or the startup critical-component check `engine.rs:2909`):
   reject/quarantine when `Data.db` length < the maximum extent implied by the partition index +
   Statistics, converting silent query-time truncation into a loud load-time rejection regardless of
   repair mode.

## Tests

- A flush/compaction test that injects a crash *after rename, before fsync* (or asserts fsync is invoked
  on every component + the directory) — the existing `flush_uses_atomic_rename` / `flush_does_not_leave_final_files_if_interrupted`
  tests assert intent but never assert durability.
- A load-time test: a truncated (nonzero) `Data.db` is rejected/quarantined at startup, not at first query.
- A WAL-ordering test: WAL discard does not advance until the SSTable fsync completes.

## Related

- The OOM that triggered the kills: `p0-unbounded-sstable-reader-memory-oom.md` (in-progress on
  `fix/bounded-sstable-reader-pool`). That bounds memory so kills stop happening; THIS bug ensures a kill
  (from any cause) never corrupts. Both are needed.
