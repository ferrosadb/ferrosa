---
type: bug
priority: P2
reported-by: agent
implemented-by: ""
verified-by: ""
created: 2026-04-06
updated: 2026-04-06
source: ferrosa-memory DIKW pipeline test
source-location: "ferrosa-memory/scripts/test-dikw-pipeline.sh"
branch: "fix/compaction-data-loss @ e9703f8"
---

# Small SSTable index corruption on tool_usage_log and audit_log

## Description

After the P0 cell sorting fix (`e9703f8`), the major entity_store corruption is resolved. However, small SSTables (under 1KB) for `tool_usage_log` and `audit_log` still produce index mismatches on read. These do not cause visible data loss (entity_store and typed_edges are intact, canaries survive) but generate WARN-level log noise on every restart.

## Evidence

From `test-dikw-pipeline.sh` (all phases PASS, but DB trust issues flagged):

```
node1: 9 corruption errors in logs
node2: 7 corruption errors in logs
node3: 7 corruption errors in logs
```

All errors are small byte mismatches:
```
wanted 97 bytes, got 34
wanted 111 bytes, got 99
wanted 1 bytes, got 0
wanted 118 bytes, got 40
wanted 93 bytes, got 10
wanted 114 bytes, got 111
wanted 30 bytes, got 16
wanted 7524 bytes, got 241
```

## Affected SSTables

On node1, the smallest SSTable data files correlate with the error sizes:

```
 97 bytes  agent_memory.tool_usage_log  (gen 1775514539507420)
127 bytes  agent_memory.audit_log       (gen 1775516318722468)
151 bytes  agent_memory.tool_usage_log  (gen 1775516318752259)
161 bytes  agent_memory.tool_usage_log  (gen 1775516125991679)
181 bytes  agent_memory.tool_usage_log  (gen 1775515598784714)
187 bytes  agent_memory.entity_store    (gen 1775516318736632)
```

The `wanted 97 bytes, got 34` error matches the 97-byte tool_usage_log SSTable exactly — the index records the full file size (97) but the reader only gets 34 bytes at the requested offset, suggesting the index offset is wrong.

## Reproduction

1. Start fresh 3-node cluster from `e9703f8`
2. Ingest ferrosa-memory and ferrosa via `frg ingest`
3. Stop and restart cluster (`podman compose stop && podman compose up -d`)
4. Check logs: `podman logs ferrosa-memory_node1_1 2>&1 | grep 'skipping corrupted'`

Corruption appears after every restart. Same errors on all 3 nodes.

## Root Cause Hypothesis

The cell sorting fix in `e9703f8` corrected tables with multiple regular columns (like entity_store with ~10 columns). However, tables with very few columns (tool_usage_log, audit_log) may still have an off-by-one or prefix-related issue in the SSTable index. The small byte deltas (97→34, 111→99) suggest the index is off by a fixed amount per row rather than the massive 150x mismatch seen before the fix.

Alternatively, these tiny SSTables may be written during graceful shutdown (543c80b) with an incomplete final row that gets indexed but not fully flushed.

## Impact

- **P2**: No data loss observed — entity_store, typed_edges, and all canaries survive
- **Log noise**: 7-9 WARN lines per node per restart
- **Silent data loss risk**: If tool_usage_log or audit_log data is queried, results may be incomplete
- **Same class of bug** as the P0 entity_store corruption, just on smaller tables

## Suggested Investigation

1. Check if `tool_usage_log` and `audit_log` have a different column layout (e.g., fewer regular columns, different clustering key structure) that the cell sorting fix doesn't cover
2. Verify the tiny SSTables (97 bytes) are complete — a 97-byte Data.db for a table with multiple columns seems suspiciously small and may be a truncated flush
3. Add post-flush index validation for all tables, not just entity_store
