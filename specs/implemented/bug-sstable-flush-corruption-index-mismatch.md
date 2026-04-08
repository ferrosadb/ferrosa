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
tested-commits: "bf65f33, 7c70c42, 689e404, 543c80b, e9703f8"
---

# SSTable index corruption — data lost on restart across all fix attempts

## Description

SSTables written during memtable flush have corrupt indices. The index records partition byte ranges that don't match the actual data file. On restart, ferrosa skips these SSTables as corrupt, silently losing all data they contain.

**This bug persists through 4 fix attempts** on `fix/compaction-data-loss`:
- `bf65f33` — coordinator read bypass fix
- `7c70c42` — manifest save_with_retry
- `689e404` — CQL bridge u16 CK prefix stripping
- `543c80b` — graceful SIGTERM shutdown with memtable flush

## Critical: test-data-loss.sh gives false PASS

The existing `test-data-loss.sh` never restarts the cluster. It reads within 90s of writes, hitting memtables. It consistently **passes** even though the SSTables are corrupt.

The new `test-dikw-pipeline.sh` forces `podman compose stop/up` between writes and reads, flushing memtables and restarting. This reveals the corruption every time.

## Reproduction

```bash
# Clean cluster with any of the tested commits
rm -rf ~/data/ferrosa-memory/node{1,2,3}/*
podman compose up -d
# Wait for healthy, then:
bash scripts/test-dikw-pipeline.sh
```

## Test Results Across Fix Attempts

### bf65f33 (coordinator read bypass)

```
Pre-restart:  14,022 entities, 100/100 canaries — PASS (memtable reads)
Post-restart: 186 entities, 0/50 canaries — 99% loss
node1: corrupted DeletionTime flags 0x99, 0xf1
       wanted 209317376 bytes, got 2915209
```

### 7c70c42 (manifest save_with_retry)

```
Pre-restart:  13,437 entities, 50/50 canaries — PASS (memtable reads)
Post-restart: 0/50 canaries on node1 (nodes 2-3 retained canaries)
node1: wanted 12562695 bytes, got 2122115
node2: corrupted DeletionTime flags 0xdf, 0xc2
node3: wanted 17975966 bytes, got 4177811
```

### 689e404 (u16 CK prefix stripping)

```
Phase 1: 2852 pre-flush -> 52 post-flush (98% loss, delta: -2800)
Phase 2: 5107 pre-flush -> 163 post-flush (97% loss, delta: -4944)
All 3 nodes: 0/50 canaries, 262 corruption errors on node1
entity_store: wanted 154239971 bytes, got 1028505 (150x mismatch)
typed_edges: SURVIVED (62,997 edges persisted)
```

### 543c80b (graceful SIGTERM shutdown)

```
Phase 1: 2852 entities -> 75 post-flush (97% loss)
0/50 canaries after first restart
```

## Key Observations

1. **typed_edges survives, entity_store doesn't** (689e404 test): Both use the same `(tenant_id, session_id)` partition key but different clustering keys. entity_store has a single UUID CK, typed_edges has `(src_id, edge_type, dst_id)`. The corruption specifically affects entity_store's index.

2. **The index byte range is wildly wrong**: Consistently wants 100-200MB from files that are 1-5MB. The ratio varies per run but the pattern is the same — the index thinks partitions are ~100x larger than they are.

3. **Flush verification passes**: The `[flush] VERIFIED: Data.db=N bytes on disk` logs show correct file sizes at write time. The data file is written correctly — the **index** is wrong.

4. **Corruption is independent per node**: All 3 nodes corrupt their own SSTables independently with different error patterns (different DeletionTime flag values, different byte counts).

5. **Graceful shutdown doesn't help** (543c80b): Even with proper SIGTERM handling and memtable flush before exit, the SSTables written during normal operation are already corrupt.

## Root Cause Hypothesis

The SSTable **partition index** (`Index.db` or BTI row index) records cumulative byte offsets that don't correspond to the actual `Data.db` layout.

The most likely mechanism: **the index offset calculation includes the u16 CQL serialization length prefixes** that are stripped from the data during BTI serialization. The data file is written with BTI-format cells (no CQL prefixes), but the index records offsets computed from the CQL-format sizes (with prefixes). This creates a growing offset drift that makes the index think each partition starts further into the file than it actually does.

This would explain:
- Why entity_store (single UUID CK, small rows) is more affected — the prefix overhead is proportionally larger
- Why typed_edges (multi-column CK, larger rows) survives — the prefix overhead is proportionally smaller, or the 689e404 fix correctly handles the multi-column case
- Why the wanted/got ratio is consistent (~150x for entity_store)

## Commits Tested (all on fix/compaction-data-loss)

| Commit | Description | Result |
|--------|-------------|--------|
| `d62fa95` | fix: P0 compaction output directory collision | Untested with restart |
| `9a11092` | fix: P0 CQL reads bypass cluster coordinator | Untested with restart |
| `bf65f33` | fix: P0 coordinate_range_read silent data loss | FAIL: 14022→186 entities |
| `7c70c42` | fix: manifest save_with_retry pending removals | FAIL: 0/50 canaries node1 |
| `689e404` | fix: P0 SSTable writer strip u16 CK prefixes | FAIL: 2852→52, 5107→163 |
| `543c80b` | fix: P0 SIGTERM graceful shutdown flush memtables | FAIL: 2852→75, 5166→183 |
| `e9703f8` | fix: P0 sort cells by column index in build_row | **PASS**: 13,759 entities survive 3 restarts, 50/50 canaries all nodes |

## Test Scripts Used

- `scripts/test-data-loss.sh` — canary survival test **without** restart. Always passes (false positive).
- `scripts/test-dikw-pipeline.sh` — DIKW pipeline test **with** forced restart between writes and reads. Catches the bug every time. Also validates consolidation, datalog, warmth/pagerank, and search quality.
- `scripts/mcp_helper.py` — JSON-RPC stdio helper for calling MCP tools from bash.

## Suggested Investigation

1. **Instrument the index writer**: In the SSTable flush path, log the byte offset being recorded in the index entry alongside the actual write position in Data.db. If they diverge, the index is wrong at write time.

2. **Compare single-CK vs multi-CK serialization**: The entity_store (single UUID CK) and typed_edges (3-column CK) use different code paths. Check if the fix in 689e404 only applies to the multi-column case.

3. **Byte-level comparison**: Dump the raw Index.db entries for a small flush (10 entities) and compare the recorded offsets against the actual partition boundaries in Data.db.

## Impact

- **P0**: 97-100% entity_store data loss on all nodes after any restart
- **Silent**: Data appears intact in memtables, corrupt only after flush to SSTable
- **test-data-loss.sh unreliable**: Never restarts, always gives false PASS
- **Cluster cannot be safely restarted** without losing all entity_store data
- **typed_edges partially spared**: Different CK structure may use a different (working) code path
