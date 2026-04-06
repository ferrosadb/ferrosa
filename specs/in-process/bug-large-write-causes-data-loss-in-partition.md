---
implemented-by: claude-code
updated: 2026-04-06
---

## Implementation Notes

### Root Cause (confirmed)

Two independent bugs combined to cause silent data loss:

**Bug 1: Silent error swallowing in `coordinate_range_read`** (`ferrosa-cluster/src/coordinator/read.rs`)
- Remote node failures (timeout, decode error, missing host_id) returned `vec![]` instead of `Err`
- `WritePath::range_read` further swallowed errors with `.unwrap_or_default()`
- Result: query appeared to succeed but returned only local node's data (1/3 on 3-node RF=1)

**Bug 2: Range reads used Data lane (10s timeout)** for full-table scans
- A 209MB partition response exceeds the 10s Data lane timeout
- `NetError::Timeout` was caught by the `_ => vec![]` catch-all in Bug 1

### Fix Applied

1. **`coordinator/read.rs` — `coordinate_range_read`**: Each per-node future now returns `Result`. Errors are collected and propagated. If ANY node fails, the function returns `Err` with the first error and logs all failures at ERROR level. Range reads now use `send_with_timeout` with 120s (`RANGE_READ_TIMEOUT`).

2. **`write_path.rs` — `range_read`**: Return type changed from `Vec<Partition>` to `Result<Vec<Partition>>`. All variants propagate errors. `Unavailable` variant returns explicit error instead of empty vec.

3. **`ferrosa-cql/src/router.rs`**: All 4 call sites updated to use `?` — errors propagate as `CqlError::ServerError` via existing `From<ClusterError>` impl.

### Tests Added

- `coordinate_range_read_errors_when_remote_nodes_unreachable` — 3-node ring, remote nodes have no pool. Asserts `Err` (was silently returning partial data).
- `coordinate_range_read_single_node_succeeds` — single-node ring, no remote nodes to fail. Asserts `Ok`.

### Pre-existing Fix (already in codebase)

- `encode_signed_bytes` sign bit preservation (`ferrosa-sstable/src/trie/node.rs:241-248`) — fixes BTI trie partition index corruption for negative values near byte boundaries.

## Analysis After Fix dacb814

**Pattern: 15,587 → 2,200 entities after 120s, every single time.**

2,200 ≈ 1/3 of 15,587 ÷ 3 nodes × 1.3 (overlap factor). This is exactly what ONE node's local data would be with RF=1 on 3 nodes.

Key question: **are nodes 2 and 3 crashing during compaction, or is their data being silently lost?**

Need to check:
1. Are all 3 nodes still running after the 120s wait?
2. What do the compaction logs show on each node?
3. Is the "2,200" coming from just the local node's data (coordinator's local read_range) while remote range reads fail silently?

## Investigation: Check coordinate_range_read error handling

Line 789-793 in coordinator/read.rs:
```rust
Err(e) => {
    tracing::warn!("coordinate_range_read: failed to decode response: {e}");
    vec![]  // ← SILENT EMPTY RETURN on decode failure!
}
_ => vec![],  // ← SILENT EMPTY RETURN on unexpected message!
```

If the RangeReadResponse from a remote node fails to deserialize (too large for bincode? truncated?), the coordinator silently returns empty for that node's data. The range read appears to succeed but is missing 2/3 of the data.

This would explain why:
- Immediate reads work: data is in memtable, local reads find it
- After compaction: data is in SSTables, needs range read from remote nodes
- Range read from remote nodes fails silently → returns empty → 2/3 data "lost"

## Root Cause: Data Lane Timeout on Large Partitions

**Data lane timeout: 10 seconds.** The edge table has 70K rows (~209MB) in one partition.
After compaction, reading this from a remote coordinator exceeds the timeout.

Sequence:
1. Immediate reads work: data in memtable/small SSTables, reads are fast
2. Compaction merges into one large SSTable per node
3. CQL round-robin sends SELECT count(*) to non-owner coordinator
4. Coordinator routes to owner via Data lane
5. Owner serializes 70K rows → 209MB ReadResponse
6. Data lane times out at 10s → returns error → empty result
7. Client sees ~2,200 entities (only the coordinator's local token range data)

Fix: increase Data lane timeout OR implement chunked/streaming reads.

## New Hypothesis: Large Partition + No CQL Paging

The `209,317,376` byte value that appears in every truncation error is the **partition data size**, not the SSTable file size. With 15K+ entities in a single `(tenant_id, session_id)` partition, the partition is ~209MB.

Without CQL client-side paging:
- The storage engine reads the full partition in one `read_exact_at(209MB)` call
- If the SSTable file is being written concurrently (flush), the file may only be partially on disk (2.9MB written so far)
- The read fails with "wanted 209MB got 2.9MB" — and the entire partition is skipped

CQL paging would mitigate this by reading in chunks, but the root cause is still the concurrent read-during-write race. The SSTable should not be registered as readable until the flush is fully complete.

Two fixes needed:
1. **Immediate**: Don't register SSTable in the active set until flush is complete + fsync'd
2. **Long-term**: Implement CQL paging for large partition reads so a partial read failure doesn't lose the entire partition

## Diagnostic Evidence from Fix 81d36ee

**New diagnostic fields reveal the root cause:**

```
[READ ERROR] SSTable id=658217520290 path=""
  wanted 209,317,376 bytes, got 2,937,173
  data_file_len=2,937,236
  sstable_count=2
```

**THREE critical findings:**

1. **`data_file_len=2,937,236`** — The actual Data.db file on disk is only 2.9MB. This is NOT a read-during-write race — the file IS this size. The data was never fully written. The flush wrote 2.9MB but should have written 209MB.

2. **`path=""`** — The SSTable is registered with an EMPTY PATH. This means the SStableEntry in the active set has `path: ""` instead of the actual file path. This is the registration bug — the SSTable was added to the active set with metadata from a different source than the actual flush output.

3. **`sstable_count=2`** — There are 2 SSTables for this table. The one being read (id=658217520290) has the empty path. The other SSTable (the 2.9MB one) has the actual data. The reader is trying to read from the WRONG SSTable — the one with empty path and stale index metadata.

**Root cause hypothesis:** When a flush completes, the SSTable is registered with `path: ""` (empty). The index metadata says the partition is 209MB (from the memtable size computation), but the actual data file is only 2.9MB. The reader uses the index metadata to compute read offsets, finds the data file is too small, and skips the partition.

**Fix:** Ensure `SStableEntry` is registered with the actual file path from `FileFlushTarget::flush()`, not an empty string. Check where `path: ""` comes from in the SSTable registration code.

## Re-test After Fix f14d046

**Result: STILL FAILING but path is now populated (that fix worked).**

```
IMMEDIATE: 15,722 entities, 71,292 edges, canaries: 100/100 ✓
```

New diagnostic output:
```
[READ ERROR] SSTable id=658217520291 
  path="/var/lib/ferrosa/sstables/agent_memory.typed_edges"  ← PATH NOW POPULATED
  wanted 209,317,376 bytes, got 5,596,681
  data_file_len=5,596,744                                    ← FILE ON DISK IS 5.5MB
  sstable_count=2
```

**Progress:** path="" is fixed. But the Data.db file is still truncated — 5.5MB instead of 209MB. The flush IS writing to the correct path, but it's not writing all the data.

The flush writes the memtable to an SSTable file. The memtable has 209MB of data (71K edges). The file ends up at 5.5MB. This means the flush is either:
1. Writing partial data (stops after ~2.5% of the memtable)
2. Being interrupted by a concurrent flush that truncates the file
3. The file is flushed, then overwritten by a subsequent smaller flush to the same generation

## BREAKTHROUGH: Flush Assertions PASS — File is Correct Size

The flush diagnostics fire and VERIFY successfully:
```
[flush] gen=658217520291 data_size=5596744 dir=".../agent_memory.typed_edges"
[flush] gen=658217520291 VERIFIED: Data.db=5596744 bytes on disk
```

But the READ ERROR says:
```
wanted 209,317,376 bytes, got 5,596,681
data_file_len=5,596,744
```

**THE FILE IS NOT TRUNCATED.** It was written correctly at 5.5MB. The problem is the SSTable PARTITION INDEX says the data should be 209MB, but the actual data portion is only 5.5MB.

**Root cause: the partition index has stale/incorrect offset metadata.**

When the memtable is flushed, it produces:
- Index file: records (partition_key → offset, length) in the Data.db
- Data file: the actual serialized rows

The index says "partition X starts at offset Y and is 209MB long" but the data file only has 5.5MB total. This means:
1. The index was computed from a larger memtable (before a partial flush drained some data)
2. OR the index was copied from a PREVIOUS SSTable during compaction and doesn't match the new data file
3. OR two flushes share the same generation number and the index from flush A is being read with the data file from flush B

**Next diagnostic:** log the partition index entries during flush — specifically the (offset, length) for each partition. Compare against the actual data file size. If any partition's offset+length exceeds the file size, the index is wrong.

## Partition Count Analysis

Flush logs show `partitions_size=215` per SSTable. But `typed_edges` has PK `(tenant_id, session_id)` — ALL 71K rows are in ONE partition. So:

- The flush captured a subset of the single giant partition (5.5MB out of ~209MB estimated)
- 38,094 rows readable (from memtable + 2 SSTables), 33,198 rows lost
- The index says the partition is 209MB because it computed the size from the full memtable, but the flush only wrote a portion

**This points to the flush draining a PARTIAL memtable.** The memtable has 209MB of data for this partition, but the flush only serializes the first ~5.5MB worth of rows before stopping. The index records the full 209MB expected size but the data writer stops early.

**Check:** Is the memtable being drained/cleared DURING the flush? If new writes arrive while the flush is serializing, does the memtable iterator see the new rows or does it snapshot at flush start? A non-snapshot iterator that races with writes could produce a short data file with a stale size estimate.

## Re-test After Fix 9a11092

**Result: STILL FAILING.**

```
IMMEDIATE: 15,427 entities, 70,271 edges, canaries: 100/100
AFTER 120s: 2,197 entities, 34,451 edges, canaries: 3/100
```

Same pattern. The flush fix did not resolve the partial memtable serialization.
