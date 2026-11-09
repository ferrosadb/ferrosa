# TODO: 21 p0-oom-audit allowlist entries expire on 2026-11-15

**Severity:** High (the Clippy gate fails on every branch from that date)
**Component:** p0-oom-guard (`xtask p0-oom-audit --enforce`)
**Files:** `specs/p0-oom-guard/oom-audit-allow.toml`

## Issue

`p0-oom-audit --enforce` warns that 21 allowlist entries expire on 2026-11-15. Owners are
`storage` and `bkearns`. Rules and files:

- server-side-result-cap: `ferrosa-sstable/src/reader.rs`, `ferrosa-sstable/src/trie/walker.rs`,
  `ferrosa-sparql/src/executor.rs`, `ferrosa-storage/src/store.rs`, `ferrosa-storage/src/engine.rs`,
  `ferrosa-cluster/src/controller/bootstrap/bootstrap_stream.rs`
- stream-push-accumulation: `ferrosa-graph/src/executor/expand.rs`, `ferrosa-graph/src/executor/stream.rs`,
  `ferrosa-sparql/src/property_path.rs`
- cloned-stream-elements: `ferrosa-cql/src/router.rs`, `ferrosa-cluster/src/repair/rpc.rs`,
  `ferrosa-cluster/src/repair/mod.rs`, `ferrosa-cluster/src/rebalance.rs`,
  `ferrosa-cluster/src/coordinator/read.rs`, `ferrosa-cluster/src/controller/membership.rs`,
  `ferrosa-cluster/src/controller/cluster.rs`
- the remaining entries: `with-capacity-limit`, `returns-vec-partition-or-row` and `copies-row-data-arg` on
  `ferrosa-sstable/src/reader.rs`; `cqlvalue-row-accumulation` on `ferrosa-flight/src/convert.rs`;
  `clone-on-row-data` on `ferrosa-graph/src/executor/expand.rs`

Once they expire, each entry becomes an `expired-allow-entry` finding, and Clippy fails on every
branch.

## What closes it

Land the streaming fixes, which is preferred (tracked under the materialization epic,
t_110dd8a5 / t_eb344b9c). Renewing an entry is a waiver: `scripts/check-no-new-waivers.py`
requires a `Waiver-Approved-By: Ben Kearns` trailer on the commit, and Ben's rule is that a
finding gets fixed rather than waived.

Recorded 2026-11-09 while consolidating PR 656, because the forge task board was unreachable.
