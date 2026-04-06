---
type: bug
priority: P0
reported-by: human+agent
implemented-by: ""
verified-by: ""
created: 2026-04-06
updated: 2026-04-06
source: ferrosa-memory DIKW pipeline test
source-location: "ferrosa-memory/scripts/test-dikw-pipeline.sh"
related: "specs/verified/bug-large-write-causes-data-loss-in-partition.md"
branch: "fix/compaction-data-loss @ e9703f8"
---

# SSTable corruption persists after 689e404 — data lost on restart

## Implementation Notes

### Root Cause (confirmed via CQL integration test)

`build_row()` in `ferrosa-cql/src/bridge.rs` constructed `Row.cells` in INSERT-statement column order (e.g., `entity_name` before `confidence`), but the SSTable reader reads cells in column-index order from the bitmap. When columns are listed in non-schema order AND have different value sizes (float=4 bytes vs text=10+ bytes), the reader misinterprets cell boundaries, causing parse drift that corrupts every subsequent row and partition.

**Why entity_store was affected but typed_edges wasn't:** entity_store has 5+ regular columns with mixed types. The INSERT statements list columns out of schema order. typed_edges has fewer columns and happened to list them in order.

### Fix (e9703f8)

One line in `bridge.rs:build_row()`: `cells.sort_by_key(|(idx, _)| *idx);`

### Test

`build_row_sorts_cells_by_column_index` — passes cells `[(3, text), (0, float), (1, text)]`, asserts output is sorted `[0, 1, 3]`.

### All fixes in this branch

| Commit | Fix |
|--------|-----|
| `692a96b` | coordinate_range_read: propagate errors, 120s timeout |
| `c948aae` | Multi-column CK per-component BTI serialization |
| `7c70c42` | Manifest save_with_retry carries pending removals |
| `689e404` | Strip CQL bridge u16 CK prefixes before BTI write |
| `543c80b` | Handle SIGTERM for graceful shutdown flush |
| `e9703f8` | **Sort cells by column index in build_row** |

## Description

After applying all fixes on `fix/compaction-data-loss` through `7c70c42` (manifest save_with_retry, multi-column serialization fix, coordinator read bypass fix, compaction directory collision fix), SSTable corruption still occurs. Data appears intact when reading from memtables but is lost when the cluster is restarted and reads come from SSTables.

## Critical Finding: test-data-loss.sh gives false PASS

The existing `test-data-loss.sh` never restarts the cluster — it reads within 90s of writes, hitting memtables. It consistently passes even though the underlying SSTables are corrupt.

The new `test-dikw-pipeline.sh` forces a `podman compose stop/up` between writes and reads, which flushes memtables to SSTables and restarts. This reveals the corruption.

## Reproduction (clean cluster, 7c70c42)

1. Wipe all node data: `rm -rf ~/data/ferrosa-memory/node{1,2,3}/*`
2. Start cluster: `podman compose up -d`
3. Run `scripts/test-data-loss.sh` → **PASS** (false positive — memtable reads)
4. Run `scripts/test-dikw-pipeline.sh --skip-ingest` → **FAIL** (forces restart, reads from SSTables)

## Evidence

### Pre-restart (memtable reads): 13,437 entities, 100/100 canaries

```
test-data-loss.sh:
  After canary insert: 100 entities, canaries: 100/100
  After ferrosa-memory ingest: 2902 entities, canaries: 100/100
  After ferrosa ingest (immediate): 13437 entities, canaries: 100/100
  After 30s wait (post-compaction): 13437 entities, canaries: 100/100
  After 90s total wait: 13437 entities, canaries: 100/100
  RESULT: PASS
```

### Post-restart (SSTable reads): 0/50 canaries on node1

```
test-dikw-pipeline.sh (after force_flush_restart):
  CANARY LOSS: 0/50 (post-consolidation)
  node1: 2 corruption errors
  node2: 4 corruption errors (canaries: 50/50)
  node3: 4 corruption errors (canaries: 50/50)
```

### Asymmetric corruption — node1 loses data, nodes 2-3 retain it

```
node1: wanted 12562695 bytes, got 2122115 (6x mismatch)
node1: wanted 40 bytes, got 4

node2: corrupted DeletionTime flags: 0xdf, 0xc2
node2: wanted 1509384 bytes, got 284752

node3: wanted 17975966 bytes, got 4177811
node3: wanted 4714974 bytes, got 901195
```

All 3 nodes have corrupt SSTables, but node1 is worst affected — loses all entity_store data. Nodes 2-3 retain canaries despite their own corruption, suggesting the corrupt SSTables on those nodes are for different tables or partitions.

## Root Cause Analysis

The pattern is consistent across multiple test runs:
- **Flush verification passes** (data file size correct on disk at flush time)
- **Index records impossible offsets** (wanting 12MB from a 2MB file)
- **Corruption is per-node** (each node corrupts independently, different patterns)

This rules out replication bugs. The issue is in the SSTable writer or index builder on each individual node.

Hypothesis: the SSTable index is built from cumulative offsets that include data from other partitions or other SSTables in the same directory. When the index records `offset=X, length=Y`, the `X+Y` exceeds the actual file length because the offset was computed against a conceptual "concatenated" view of multiple SSTables rather than the individual Data.db file.

## Suggested Fix

1. Add a post-flush validation: after writing Data.db + Index.db, verify that every partition offset in the index is within bounds of the data file
2. If validation fails, log the exact index entry and abort the flush rather than persisting a corrupt SSTable
3. Update `test-data-loss.sh` to include a cluster restart step so it catches this class of bug

## Update: 689e404 (CQL bridge u16 CK prefix stripping)

Still fails. Same corruption pattern, now even worse — all 3 nodes lose all canaries:

```
689e404 results (test-dikw-pipeline.sh):
  Phase 1: 2852 pre-flush -> 52 post-flush (98% loss, delta: -2800)
  Phase 2: 5107 pre-flush -> 163 post-flush (97% loss, delta: -4944)
  entity_store: wanted 154239971 bytes, got 1028505 (150x mismatch)
  node1: 262 corruption errors, 0/50 canaries
  node2: 7 corruption errors, 0/50 canaries
  node3: 2 corruption errors, 0/50 canaries
```

New observation: **typed_edges table survived** (62,997 edges persisted across restart) while entity_store was destroyed. The corruption targets entity_store specifically. Both tables use the same (tenant_id, session_id) partition key but different clustering keys — entity_store has a single UUID clustering key while typed_edges has (src_id, edge_type, dst_id). The fix in 689e404 addressed multi-column CK serialization but entity_store's single-column CK may have a different serialization path that is still broken.

## Investigation: unit tests PASS — corruption is runtime-only

Two restart roundtrip tests were added and BOTH PASS:

1. `flush_restart_roundtrip_entity_store_schema` — 200 rows, entity_store schema (CompositeType PK, UUID CK, 2 regular columns). Flush → drop engine → re-open from disk → all 200 rows survive.

2. `multi_flush_restart_roundtrip_preserves_all_data` — 500 rows across 5 flushes, entity_store schema. Multiple SSTables → compaction → restart → all 500 rows survive.

**This means the serialization/deserialization is correct.** The corruption is caused by something in the runtime environment that unit tests don't exercise:

- **Concurrent flushes/compaction**: Multiple threads writing to the same table directory simultaneously
- **CQL layer cell ordering**: The CQL INSERT path might produce rows with cells in non-schema order (causes value swaps but shouldn't cause parse drift)
- **Auto-flush racing with explicit flush**: Two flushes triggered concurrently could produce SSTables that overwrite each other
- **File-level corruption**: A concurrent process (compaction) modifying files while the flush writes

### Next steps to isolate:
1. Add diagnostic: log the total bytes written by `serialize_partition` vs the expected bytes from the varint decoded during read — find the EXACT partition where drift starts
2. Write a CQL-level roundtrip test that goes through the full INSERT → flush → restart → SELECT path (not just the storage engine)
3. Check for concurrent flush/compaction races in the flush guard logic

## Impact

- P0: 97-100% entity_store data loss on all nodes after restart
- typed_edges survives (different clustering key structure)
- All prior "PASS" results from test-data-loss.sh are unreliable (never restarts)
- The ferrosa-memory cluster cannot be restarted without data loss
