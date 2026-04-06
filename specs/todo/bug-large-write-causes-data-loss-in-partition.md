
## Re-test After Fix 67021ac (manifest removals + executor skip corrupt)

**Result: FIX DID NOT RESOLVE THE BUG.**

```
IMMEDIATE:  15,447 entities, 69,994 edges, canaries: 100/100 ✓
AFTER 90s:   2,197 entities, 32,923 edges, canaries: 3/100   ✗
```

Same pattern: data present immediately, lost after compaction runs. 12,250 entities and 37,071 edges lost. Canaries dropped from 100 to 3.

NOTE: Initial connection failure was caused by stale MCP/skilltools processes holding CQL connections, not a protocol bug. After killing stale processes, connections worked fine.

Fix attempt count: 4 (all failed). The root cause remains in the compaction path.

## Log Evidence from Fix 67021ac

Still seeing corrupt SSTables:
```
skipping corrupt SSTable: wanted 209,317,376 bytes, got 2,901,290  (97% short!)
skipping corrupted SSTable: corrupted DeletionTime flags: 0x81
skipping corrupted SSTable: corrupted DeletionTime flags: 0xae
skipping corrupted SSTable: wanted 9,381,633 bytes, got 4,235,743  (55% short!)
```

**Key observation: the SSTables are TRUNCATED, not malformed.** The file was supposed to be 209MB but only 2.9MB was written. This is NOT a serialization bug — it's a **premature file close** or **concurrent read during write**.

The flush writes an SSTable file but either:
1. The file is read by compaction/query BEFORE the flush write completes
2. The file handle is closed before all data is fsynced
3. A concurrent flush replaces the file while it's still being written

This explains why DeletionTime flags look "corrupt" — we're reading partially-written data, not incorrectly-serialized data.

## Re-test After Fix d62fa95 (compaction output directory collision)

**Result: FIX DID NOT RESOLVE THE BUG. Fix attempt #5.**

```
IMMEDIATE: 12,424 entities, canaries: 3/100  ← LOST DURING INGEST, not after compaction
AFTER 90s:  2,210 entities, canaries: 3/100
```

WORSE: canaries are already gone IMMEDIATELY after ingest — not waiting for compaction. The truncation is happening during concurrent flush, not during compaction.

Truncated SSTables:
```
wanted 209,317,376 bytes, got 3,830,406  (SSTable index 0)
wanted 1,315,072 bytes, got 318,295     (SSTable index 1)
```

The directory collision fix addresses compaction output, but the truncation is happening in the FLUSH path — concurrent flushes writing to the same directory overwrite each other.

## Re-test After Fixes 6cfd77b + 3219876 + 9730cf2 (batch, hint, gen collision)

**Result: FIX DID NOT RESOLVE THE BUG. Fix attempts #6-8.**

```
IMMEDIATE: 15,560 entities, 70,423 edges, canaries: 100/100 ✓
AFTER 120s: 2,200 entities, 33,487 edges, canaries: 3/100   ✗
```

Same truncated SSTable: wanted 209,317,376 got 2,891,571. This EXACT 209MB number appears in every test run — it's the same SSTable being truncated every time. This suggests a deterministic corruption based on data volume, not a race condition.

Hypothesis: the SSTable writer has a 209MB expected size from the index, but the data write only produces ~3MB. The index is computed BEFORE the data is written, and something causes the data portion to be truncated (perhaps a buffer flush that doesn't write all pages, or an off-by-one in the data file offset calculation).
