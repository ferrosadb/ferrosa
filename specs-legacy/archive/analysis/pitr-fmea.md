# FMEA — Point-in-Time Restoration

> Last updated: 2026-03-20
> Status: Implemented

## Scoring Criteria

| Score | Severity (S) | Occurrence (O) | Detection (D) |
|-------|-------------|----------------|----------------|
| 1 | Negligible | Almost never | Always detected before impact |
| 2-3 | Minor degradation | Rare | Usually detected |
| 4-6 | Significant impact | Occasional | Sometimes detected |
| 7-8 | Major failure | Frequent | Rarely detected |
| 9-10 | Catastrophic / data loss | Very frequent | Undetectable |

RPN = Severity x Occurrence x Detection. **Action required for RPN >= 50.**

## Failure Mode Table

| ID | Component | Failure Mode | Effect | S | O | D | RPN | Mitigation | Test Case |
|----|-----------|-------------|--------|---|---|---|-----|------------|-----------|
| FM1 | CommitLogArchiver | S3 upload fails for segment | Gap in archive; PITR window has hole | 8 | 4 | 3 | **96** | Retry with exponential backoff (5 attempts); keep local segment until confirmed; alert on failure | T1: Inject S3 error, verify retry succeeds; T2: Verify segment retained on disk after failed upload |
| FM2 | CommitLogArchiver | Archiver falls behind write rate | Segments accumulate on disk; disk pressure | 7 | 5 | 3 | **105** | Monitor unarchived segment count; backpressure via disk usage threshold; alert at 10 unarchived segments | T3: Write at high rate with slow S3 mock, verify segment count monitored; T4: Verify disk pressure alert fires |
| FM3 | CommitLogArchiver | Archive manifest CAS conflict | Segment archived but not indexed | 5 | 3 | 4 | **60** | CAS retry (3 attempts); fallback S3 prefix scan during restore | T5: Simulate concurrent archive manifest updates, verify retry succeeds |
| FM4 | CommitLogArchiver | Segment file deleted before archive | Permanent gap in archive | 9 | 2 | 2 | 36 | Never delete segment until archiver confirms; add `archived` flag to segment tracker | T6: Verify discard_completed() skips unarchived segments |
| FM5 | SnapshotManager | Memtable flush fails during snapshot | Snapshot captures stale manifest (missing recent flushes) | 8 | 2 | 3 | 48 | Snapshot creation is atomic: all flushes succeed or snapshot fails; return error, don't create partial snapshot | T7: Inject flush error, verify snapshot creation returns error |
| FM6 | SnapshotManager | S3 PUT fails for manifest copy | Partial snapshot in S3 (metadata without manifest) | 7 | 3 | 3 | **63** | Atomic snapshot: write manifest, schema, metadata in order; validate on load; delete partial on error | T8: Inject S3 error during snapshot, verify cleanup of partial files |
| FM7 | SnapshotManager | Snapshot references compacted SSTables | SSTables may have been replaced by compaction between flush and snapshot | 6 | 3 | 5 | **90** | Snapshot copies manifest atomically — if compaction runs concurrently, the manifest is a consistent snapshot of either pre- or post-compaction state | T9: Run compaction concurrently with snapshot creation, verify manifest is internally consistent |
| FM8 | RestoreManager | Archived segment checksum mismatch | Segment corrupted in S3; replay produces wrong state | 9 | 1 | 1 | 9 | SHA-256 verify on download; abort restore with clear error | T10: Corrupt archived segment in S3, verify restore aborts with checksum error |
| FM9 | RestoreManager | Missing segment in archive (gap) | Cannot replay continuously from snapshot position | 9 | 2 | 2 | 36 | Validate segment continuity before replay; abort if gap detected; report which segments are missing | T11: Delete one archived segment, verify restore detects gap and reports it |
| FM10 | RestoreManager | Timestamp filter off-by-one | Mutations at exact boundary included/excluded incorrectly | 6 | 3 | 6 | **108** | Use `<=` (inclusive) for restore timestamp; document boundary behavior; test with exact-match timestamps | T12: Create mutations at t=100, restore to t=100, verify they are included |
| FM11 | RestoreManager | Restore from wrong node's snapshot | Token ranges don't match; data placed in wrong partitions | 9 | 2 | 4 | **72** | Validate node_id in metadata; require `--force` for cross-node restore; warn operator | T13: Restore with mismatched node_id, verify warning and require --force |
| FM12 | RestoreManager | Schema mismatch between snapshot and segments | Replay applies mutations to tables that don't exist or have different schema | 7 | 2 | 4 | **56** | Restore loads schema from snapshot first; validates mutation table/keyspace against schema before replay; skip unknown tables with warning | T14: Archive segments with table "foo", delete "foo" from schema snapshot, verify restore warns and skips |
| FM13 | SSTable GC | GC deletes SSTables still referenced by snapshot | Snapshot becomes unrestorable | 10 | 3 | 5 | **150** | GC must scan all snapshot manifests; SSTable deleted only when zero references across all manifests | T15: Create snapshot, compact, verify GC doesn't delete snapshot-referenced SSTables |
| FM14 | Archive Retention | Retention cleanup deletes segments needed for oldest snapshot | Oldest snapshot becomes unrestorable | 9 | 3 | 3 | **81** | Retention policy respects snapshot boundaries: never delete segments newer than oldest snapshot's commit log position | T16: Create snapshot, advance retention, verify segments after snapshot position are retained |
| FM15 | CommitLogArchiver | Node crash during segment archive | Segment partially uploaded to S3 | 5 | 3 | 2 | 30 | Archiver uploads atomically (single PUT); partial PUT is not visible in S3; on restart, re-archive the segment | T17: Kill archiver mid-upload, restart, verify segment re-archived |
| FM16 | RestoreManager | S3 download fails during restore | Restore incomplete; node cannot start | 7 | 3 | 2 | 42 | Retry downloads with backoff; restore is idempotent (can restart from beginning); clear error messages | T18: Inject S3 error during SSTable download, verify retry; verify restart works |

## Risk Priority Summary

| Priority | ID | RPN | Failure Mode |
|----------|----|-----|-------------|
| 1 | FM13 | 150 | GC deletes snapshot-referenced SSTables |
| 2 | FM10 | 108 | Timestamp filter off-by-one |
| 3 | FM2 | 105 | Archiver falls behind write rate |
| 4 | FM1 | 96 | S3 upload fails for segment |
| 5 | FM7 | 90 | Snapshot references compacted SSTables |
| 6 | FM14 | 81 | Retention cleanup deletes snapshot-needed segments |
| 7 | FM11 | 72 | Restore from wrong node's snapshot |
| 8 | FM6 | 63 | S3 PUT fails during snapshot creation |
| 9 | FM3 | 60 | Archive manifest CAS conflict |
| 10 | FM12 | 56 | Schema mismatch between snapshot and segments |

## Implementation Status

> Updated: 2026-03-20

| ID | RPN | Mitigation Status | Sprint | Evidence |
|----|-----|-------------------|--------|----------|
| FM1 | 96 | **Implemented** — Exponential backoff retry (5 attempts), local segment retained until confirmed | P-1 | `6dd71e5` |
| FM2 | 105 | **Implemented** — archive_status virtual table monitors unarchived segments, lag metrics | P-4 | `faf52e6` |
| FM3 | 60 | **Implemented** — Archive manifest CAS retry, hex-prefix paths for throughput | P-1 | `6dd71e5` |
| FM4 | 36 | **Implemented** — `archived` flag on segment tracker prevents premature deletion | P-1 | `1e73f66` |
| FM5 | 48 | **Implemented** — Snapshot creation atomic: all flushes succeed or fail | P-2 | `bf0afdb` |
| FM6 | 63 | **Implemented** — Atomic snapshot write with cleanup on error | P-2 | `ed0df9d` |
| FM7 | 90 | **Implemented** — Snapshot copies manifest atomically; concurrent compaction safe | P-2 | `31ad2a4` |
| FM8 | 9 | Low RPN — SHA-256 verify on download planned for full restore path | — | Deferred |
| FM9 | 36 | **Implemented** — Segment continuity validation before replay | P-3 | `abf30dc` |
| FM10 | 108 | **Implemented** — Inclusive `<=` boundary, timestamp filtering during replay | P-3 | `abf30dc` |
| FM11 | 72 | **Implemented** — Node-id validation, `--force` required for cross-node restore | P-3 | `9c549ee` |
| FM12 | 56 | **Implemented** — Schema loaded from snapshot, unknown table mutations skipped with warning | P-3 | `abf30dc` |
| FM13 | 150 | **Implemented** — GC scans all snapshot manifests before SSTable deletion | P-2 | `a0160de` |
| FM14 | 81 | **Implemented** — Retention respects snapshot boundaries | P-2 | `1e73f66` |
| FM15 | 30 | Low RPN — Atomic S3 PUT prevents partial upload visibility | — | By design |
| FM16 | 42 | Low RPN — Idempotent restore with retry planned | — | Deferred |

## Test Plan

### Unit Tests

| ID | Test | Component | Validates |
|----|------|-----------|-----------|
| T1 | Archiver retries on S3 error | CommitLogArchiver | FM1 retry logic |
| T2 | Segment retained after failed upload | CommitLogArchiver | FM1 local retention |
| T4 | Disk pressure alert on segment accumulation | CommitLogArchiver | FM2 monitoring |
| T5 | Archive manifest CAS retry | CommitLogArchiver | FM3 conflict handling |
| T6 | discard_completed skips unarchived | CommitLog | FM4 safety interlock |
| T7 | Snapshot fails on flush error | SnapshotManager | FM5 atomicity |
| T8 | Partial snapshot cleaned up | SnapshotManager | FM6 cleanup |
| T10 | Checksum mismatch aborts restore | RestoreManager | FM8 integrity |
| T12 | Timestamp boundary inclusive | RestoreManager | FM10 correctness |
| T13 | Cross-node restore requires --force | RestoreManager | FM11 safety |
| T14 | Unknown table mutations skipped | RestoreManager | FM12 schema compat |

### Integration Tests

| ID | Test | Components | Validates |
|----|------|------------|-----------|
| T3 | High write rate with slow S3 | Archiver + CommitLog | FM2 backpressure |
| T9 | Concurrent compaction + snapshot | SnapshotManager + CompactionExecutor | FM7 consistency |
| T11 | Restore detects archive gap | RestoreManager + ArchiveManifest | FM9 continuity |
| T15 | GC respects snapshot manifests | GC + SnapshotManager | FM13 safety |
| T16 | Retention respects snapshot boundaries | Archiver + SnapshotManager | FM14 coordination |
| T17 | Crash recovery during archive | CommitLogArchiver | FM15 idempotency |
| T18 | Restore retry on S3 error | RestoreManager | FM16 resilience |

### End-to-End Tests

| ID | Test | Validates |
|----|------|-----------|
| E1 | Full PITR cycle: write, snapshot, write more, restore to midpoint | All components — the happy path |
| E2 | Restore after compaction: snapshot, compact, more writes, restore | FM7 + FM13 — compaction doesn't break restore |
| E3 | Multi-table restore: writes to 3 tables, snapshot, more writes, PITR | Schema handling across tables |

## Related Specs

- [PITR](pitr.md) — architecture
- [Threat Model — PITR](threat-model-pitr.md) — STRIDE analysis
- [Storage](storage.md) — storage engine (commit log, manifest, GC)
