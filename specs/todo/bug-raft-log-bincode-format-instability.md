---
type: todo
priority: P1
status: draft
created: 2026-06-12
affected-versions: all (no raft log format versioning exists)
---

# Bug: persisted Raft log entries have no format versioning — build drift bricks the metadata plane silently

## Observed (fmem 3-node cluster, 2026-06-12)

All three nodes fail Raft initialization at boot with an identical fatal:

```
ERROR ferrosa_cluster::controller::cluster: raft initialization failed (Fatal)
  fatal=when Read Logs: alloc::boxed::Box<bincode::error::ErrorKind>:
  invalid value: integer `1635017060`, expected variant index 0 <= i < 26
```

- Snapshot at `last_applied=5999` decodes fine on every build tried
  (`recovered raft topology from persisted state machine snapshot
  member_count=3 token_count=768`).
- Log entries `[6000, 6121]` are unreadable by **both** the current build
  and the previous deployed build (`ferrosa-memory-node:rowpage-prev`) —
  they were written by an older build whose `RaftCommand`/`RaftOp` bincode
  layout differed (variant added/reordered or embedded struct field change).
  `1635017060` is ASCII bytes being misread as an enum tag — framing slip,
  not random corruption: byte-identical error at the same offset on three
  independently-written node logs.

## Why this is severe

1. **The failure is silent in operation.** Raft init failure is non-fatal
   to the process: the node recovers topology optimistically and keeps
   serving CQL. The fmem cluster ran **10+ hours with a dead metadata
   plane** (no DDL replication, no membership changes possible) while
   TCP-based healthchecks reported `healthy`. Only the new `/readyz`
   leader-gated probe surfaced it (`{"ready":false,"waiting_for":"raft_leader"}`).
2. **No build can repair it.** Old and new builds both fail to decode;
   there is no `--skip-bad-entries` or log-truncate tooling.
3. **Every release upgrade is exposed.** Any commit that touches the
   `RaftOp` enum (or any type embedded in `RaftCommand`) changes the
   on-disk log format with no version tag, no compat test, and no error
   message that names the real cause.

## Fix shape

- Version-tag persisted raft log entries (envelope with a format version,
  or switch the log codec to a self-describing/tagged encoding).
- CI gate: golden-file decode test over raft log entries written by the
  previous release tag (extend the driver-conformance pattern to the
  internode/persistence layer).
- `ferrosa-ctl raft-log inspect/truncate` operator tooling: decode what is
  readable, report the first bad entry, optionally truncate to the last
  snapshot-covered index after explicit confirmation.
- Startup should distinguish "log entry newer than snapshot is unreadable"
  and name the recovery procedure in the error.

## Recovery procedure for the fmem cluster (pending operator approval)

The snapshot at 5999 is readable everywhere; entries 6000–6121 (122
metadata ops) are not. Purging log segments beyond the snapshot on all
three nodes and restarting re-forms Raft from the snapshot. Cost: those
122 committed metadata ops are discarded — acceptable only because
membership (3 nodes) and token ring (768) are stable and CQL schema is
persisted separately in `schema.json`. Requires explicit sign-off.

## Related

- `specs/todo/bug-slow-raft-cold-start-after-graceful-shutdown.md`
- `/readyz` probe (`ferrosa/src/web/readiness.rs`) — detection path
