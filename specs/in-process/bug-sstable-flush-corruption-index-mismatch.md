---
type: bug
priority: P0
reported-by: human+agent
implemented-by: ""
verified-by: ""
created: 2026-04-06
updated: 2026-04-06
status: implemented
source: ferrosa-memory cluster testing
source-location: "ferrosa-memory/scripts/test-data-loss.sh"
related: "specs/verified/bug-large-write-causes-data-loss-in-partition.md"
---

# SSTable flush produces corrupt files — index records wrong partition size

## Description

SSTables written during memtable flush are internally corrupt. The index records partition sizes that don't match the actual data file. On subsequent reads (after restart or compaction), ferrosa skips these SSTables as corrupt, silently losing all data they contain.

This is distinct from the compaction directory collision bug (fixed in `d62fa95`) and the coordinator read bypass (fixed in `9a11092`/`bf65f33`). Those fixes prevent some loss paths but **this bug continues to produce corrupt SSTables during normal flush operations**.

## Reproduction

1. Start 3-node ferrosa cluster from `fix/compaction-data-loss` branch @ `bf65f33`
2. Insert 100 canary entities
3. Run `skilltools ingest` for ferrosa-memory (~2,800 entities)
4. Run `skilltools ingest` for ferrosa (~11,000 entities)
5. Verify all 14,022 entities present immediately (reads from memtables — **PASSES**)
6. Wait for flush + compaction (or restart cluster)
7. Query entity count: **186 entities survive** — 99% data loss

## Evidence

### Flush verification logs show correct sizes at write time

```
[flush] gen=1775503113545471 data_size=2915272 partitions_size=215 dir="agent_memory.typed_edges"
[flush] gen=1775503113545471 VERIFIED: Data.db=2915272 bytes on disk
[flush] gen=1775503113578936 data_size=4420295 partitions_size=215 dir="agent_memory.entity_store"
[flush] gen=1775503113578936 VERIFIED: Data.db=4420295 bytes on disk
```

The flush code verifies the file size matches on disk. The data IS written correctly.

### But reads fail with impossible size expectations

```
[READ ERROR] SSTable id=1775503113545471 path="agent_memory.typed_edges":
  error=I/O error: read_exact_at: wanted 209317376 bytes, got 2915209
  data_file_len=2915272, sstable_count=4

read_range: skipping corrupted SSTable:
  I/O error: cell value length 28655383987 exceeds maximum (268435456), likely corrupt SSTable

read_range: skipping corrupted SSTable:
  invalid data: corrupted DeletionTime flags: 0x99
```

Key observation: the reader **wants 209MB** from a file that is **2.9MB**. The data file size is correct (verified at flush), but the **index is recording a wrong partition byte range**.

### Compaction also fails on missing files

```
[compaction] starting task for agent_memory.entity_store: 4 inputs
[compaction] reading input SSTable 4: Data.db=0 bytes, path="agent_memory.entity_store/4-Data.db"
[compaction] task failed: aborting compaction: SSTable 4 Data.db missing: No such file or directory (os error 2)
```

SSTable ID `4` suggests pre-existing SSTables (low generation numbers) that were either not properly migrated or their files were deleted without updating the manifest.

### S3 sync broken

```
S3 SSTable sync failed e=invalid format: failed to save manifest: Operation not yet implemented.
```

This means no SSTable data is persisted to S3. Combined with local file corruption, this is a total data loss path.

### Corruption is cluster-wide

All 3 nodes show the same corruption patterns with different DeletionTime flag values (0x99, 0xf1, 0xb8, 0xe0), indicating each node independently produces corrupt SSTables rather than replicating corrupt data from a single source.

## Root Cause Hypothesis

The SSTable **index** (partition offsets within Data.db) does not match the actual data layout. Two possible mechanisms:

1. **Index offset calculation bug**: The index records cumulative byte offsets that include data from previous flushes or use a stale base offset, causing `read_exact_at` to request far more bytes than the partition actually occupies.

2. **Generation reuse after restart**: SSTable generation IDs may collide with pre-existing files from before the restart. The manifest loads the old index entries but the data files were overwritten with new (shorter) content. Evidence: the `wanted 209317376 bytes, got 2915209` pattern — the wanted size could be from a prior incarnation's index.

3. **Manifest desync**: The in-memory SSTable registry may include entries from SSTables that were deleted during a prior compaction run, or whose files were not fully synced to disk before a crash/restart.

## Suggested Investigation

1. **Compare index vs data at read time**: In `ferrosa-storage/src/store.rs` `read_range`, log the index entry's recorded offset + size alongside the actual Data.db file length before attempting the read.

2. **Verify manifest after restart**: After loading the manifest on startup, verify that every SSTable ID in the manifest has a corresponding Data.db file on disk with a size >= the max offset recorded in its index.

3. **Check generation assignment**: Verify that flush generation IDs are strictly monotonically increasing across restarts and never reuse an ID from a prior incarnation.

4. **Fix S3 manifest save**: The `Operation not yet implemented` error on manifest save to S3 means the persistence layer has a regression or unimplemented codepath. This blocks durability entirely.

## Impact

- **Data loss**: 99% of entities lost after memtable flush
- **Silent**: Data appears present when in memtables, disappears after flush/restart
- **The data loss test script gives a false PASS** because it reads within the memtable lifetime window
- **Affects all tables**: entity_store, typed_edges, derived_cache, etc.
- **Cluster-wide**: All 3 nodes independently corrupt

## Test Script Fix

The current `test-data-loss.sh` should be updated to force a flush or restart the cluster between writes and reads to ensure it tests post-flush durability, not just memtable reads.
