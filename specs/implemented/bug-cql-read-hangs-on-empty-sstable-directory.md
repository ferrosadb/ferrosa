---
type: bug
priority: P1
reported-by: agent
implemented-by: ""
verified-by: ""
created: 2026-04-06
updated: 2026-04-06
source: ferrosa-memory MCP startup hang
branch: "main @ cffc67f"
---

# CQL SELECT hangs indefinitely on tables with empty SSTable directories

## Description

When a table's SSTable directory exists but contains no files (no Data.db), a `SELECT` query against that table hangs indefinitely instead of returning an empty result set. This blocks ferrosa-memory MCP startup because `load_entity_types()` and `load_edge_types()` query these tables during initialization.

## Reproduction

```bash
# Fresh cluster from cffc67f
rm -rf ~/data/ferrosa-memory/node{1,2,3}/*
podman compose up -d
# Wait for healthy

# Ingest data (creates entity_types/edge_types directories but no data)
frg ingest --cql localhost:19042 /path/to/repo

# Restart cluster
podman compose stop && podman compose up -d

# This hangs:
python3 -c "
from cassandra.cluster import Cluster
c = Cluster(['localhost'], port=19042, protocol_version=4)
s = c.connect('agent_memory')
print(list(s.execute('SELECT type_name FROM agent_memory.entity_types')))
"
# Times out after 10s (default client timeout)
```

## Evidence

```
# SSTable directory exists but is empty:
$ podman exec node1 ls -la /var/lib/ferrosa/sstables/agent_memory.entity_types/
total 0
drwxr-xr-x 2 root root 64 .
drwxr-xr-x 37 root root 1184 ..

# Query hangs:
entity_types error in 10.01s: Client request timeout
edge_types error in 10.01s: Client request timeout

# Other tables with actual SSTable files work fine:
entity_store: 13892 entities returned in <1s
```

## Impact

- **P1**: Blocks MCP server startup — `ferrosa-memory-mcp` hangs during `load_entity_types()` and never responds to `initialize`
- Affects any table that has been created (DDL) but never written to, or whose SSTables were lost
- The `entity_types` and `edge_types` tables are type registry tables that are queried at startup

## Workaround

Insert at least one row into the affected tables before restarting:
```cql
INSERT INTO agent_memory.entity_types (type_name) VALUES ('concept');
INSERT INTO agent_memory.edge_types (type_name) VALUES ('related_to');
```

## Root Cause Hypothesis

The storage engine's read path likely opens the SSTable directory, finds no files, and enters a code path that either:
1. Waits for SSTables to appear (polling/blocking)
2. Tries to read a manifest that doesn't exist and blocks on I/O
3. Has an iterator that never terminates because there are no SSTables to iterate

Tables with no SSTable directory at all (never created) would skip the SSTable read path entirely. The bug is specific to directories that exist but are empty.

## Implementation Notes

Resolved on PR #109. Verified 2026-04-18 against the live 3-node
ferrosa-memory cluster running the PR #109 image:

- `SELECT * FROM agent_memory.{entity_types, edge_types, audit_log,
  tool_usage_log}` all return in < 0.05s with the correct result set —
  including tables with only a subset of expected SSTables and tables
  with zero matching rows.
- Direct repro: `CREATE KEYSPACE + CREATE TABLE + SELECT * + SELECT …
  WHERE k=1` against a brand-new table with no writes returns `0 rows`
  in 0.01s. No hang.

Most likely carried by the cluster-formation/schema-replay commits
(`8d18faa`, `9bcd7ea`) which ensure new tables are registered on every
node before the first read. The startup-compaction/load-existing-SSTable
changes (`c6209a8`, `9441e48`) also closed the empty-dir case by
treating missing SSTable sets as "empty result" rather than blocking.
