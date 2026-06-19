# Bug: DDL Forward Handler Fails to Deserialize — "unknown variant `op`"

**Severity:** Critical (blocks all schema creation on fresh clusters)
**Component:** ferrosa-cluster/ddl_path
**Branch:** ALL branches (confirmed on both feat/sparql-endpoint and feat/pitr-raft-fixes)
**Note:** This is NOT a SPARQL branch regression. It's a pre-existing bug that only manifests on truly fresh clusters with no prior schema.json files.

## Issue

On fresh 3-node cluster formation, the `ClusterDdlForwardHandler` fails to deserialize DDL operations:

```
ERROR ferrosa_cluster::ddl_path: ClusterDdlForwardHandler: failed to decode op: 
internal: DdlOperation deserialize: unknown variant `op`, expected one of 
`CreateKeyspace`, `DropKeyspace`, `CreateTable`, `DropTable`, `AlterKeyspace`, 
`AlterTable`, `CreateRole`, `AlterRole`, `DropRole`, `Grant`, `Revoke`, 
`CreateIndex`, `DropIndex`, `CreateType`, `DropType`, `CreateFunction`, 
`DropFunction`, `CreateAggregate`, `DropAggregate` at line 1 column 5
```

## Impact

- Keyspaces and tables are never created on non-leader nodes
- CQL queries against user keyspaces timeout (tables don't exist)
- Restore scripts fail indefinitely
- MCP server can't connect
- Cluster appears healthy (TCP, Raft) but is functionally broken for DDL

## Root Cause

The DDL forward message is being serialized with a wrapper `{"op": {...}}` instead of the enum variant directly. The `DdlOperation` enum expects internally tagged variants like `{"CreateKeyspace": {...}}` but receives `{"op": {"CreateKeyspace": {...}}}`.

This is likely a serde serialization mismatch between the sender (leader) and receiver (follower). The leader may be wrapping the operation in an extra layer.

## Reproduction

```bash
# 1. Clean cluster start
podman compose down
rm -rf ~/data/ferrosa-memory/node{1,2,3}/*
podman compose up -d

# 2. Wait for Raft leader election (~30s)
# 3. Check logs:
podman logs ferrosa-memory_node1_1 2>&1 | grep "failed to decode"
# → ERROR: unknown variant `op`

# 4. Try any CQL query against user keyspace:
# → OperationTimedOut (table doesn't exist)
```

## Proposed Fix

Check `ferrosa-cluster/src/ddl_path.rs` for the serialization/deserialization of `DdlOperation`. Either:
1. The sender wraps in `{"op": ...}` — remove the wrapper
2. The receiver expects unwrapped — add `#[serde(untagged)]` or adjust the struct
3. Version mismatch between the DDL message format on different code paths

## Workaround

The schema propagates via `schema.json` snapshot (which works — the log shows "schema snapshot sent to rejoined peer"). But the DDL forward path for new operations is broken.

If the `schema.json` from the leader already contains all tables, follower nodes load it on startup. The issue is that new DDL operations (e.g., creating the `agent_memory` keyspace on a fresh cluster) can't propagate.

## Verification (2026-04-05, branch fix/p0-compaction-ddl-readiness, commit 5330968)

Fresh 3-node cluster started with empty data dirs:
- No "unknown variant `op`" errors in logs
- Schema applied: 5 keyspaces, 29 tables
- Raft leader elected
- SPARQL endpoint healthy
- CQL queries work immediately
- **Status: VERIFIED FIXED**
