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
branch: "fix/compaction-data-loss @ 7c70c42"
---

# SSTable corruption persists after 7c70c42 — data lost on restart

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

## Impact

- P0: 100% data loss on node1 after restart
- All prior "PASS" results from test-data-loss.sh are unreliable
- The ferrosa-memory cluster cannot be restarted without data loss
