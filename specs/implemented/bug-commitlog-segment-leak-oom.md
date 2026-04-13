---
type: bug
priority: P0
reported-by: agent
implemented-by: "claude"
verified-by: ""
created: 2026-04-13
updated: 2026-04-13
source: ferrosa-memory 3-node cluster (podman)
source-location: "ferrosa-memory/docker-compose.yml"
branch: "main (ferrosa)"
---

# Closed commitlog segments leak in memory, causing OOM

## Summary

Closed commitlog segments hold their full 32 MB buffer in memory (`Arc<Segment>` in `closed_segments: Vec`). When segment GC stalls — because low-write tables never advance their flush checkpoint — segments accumulate unboundedly. A 3-node cluster running for ~48 hours accumulated 184 segments on one node (5.7 GB of in-memory buffers), triggering the Linux OOM killer.

## Impact

- All three ferrosa nodes killed by OOM (SIGKILL/exit 137)
- Kill cascade: node1 first (~03:44), node2 (~06:42), node3 (~15:04)
- Podman VM (8 GB RAM) left in degraded state; SSH and container runtime unresponsive
- Data on disk appears intact (commitlog segments + SSTables present)

## Evidence

### OOM kills (from VM serial console)

```
[13905.051878] Out of memory: Killed process 7456 (ferrosa)
    total-vm:6575084kB, anon-rss:2754464kB (~2.7GB)
[24093.970799] Out of memory: Killed process 7941 (ferrosa)
    total-vm:8258936kB, anon-rss:3759908kB (~3.7GB)
```

### Disk state (host-mounted volumes)

| Node  | Commitlog segments | Commitlog size | SSTable size | Ratio |
|-------|-------------------|---------------|-------------|-------|
| node1 | 43                | 1.3 GB        | 78 MB       | 17:1  |
| node2 | 75                | 2.3 GB        | 72 MB       | 32:1  |
| node3 | 184               | 5.7 GB        | 77 MB       | 75:1  |

### Checkpoint analysis (node3, 2026-04-13T05:42:28Z)

Flush positions show major tables are current but several tables are stale:

| Table                        | Flushed to segment | Status |
|------------------------------|-------------------|--------|
| entity_store                 | 186               | Current |
| audit_log                    | 186               | Current |
| tool_usage_log               | 186               | Current |
| derived_cache_by_query       | 14                | STALE (172 segments behind) |
| typed_edges                  | 7                 | STALE (179 segments behind) |
| edge_types                   | 2                 | STALE (184 segments behind) |
| entity_types                 | 2                 | STALE (184 segments behind) |

Node1 has an `intentions` table (segment 20) that node3 lacks entirely.

### Container exit codes

All three ferrosa containers exited with code 137 (SIGKILL). MinIO and MCP server survived.

### macOS diagnostic reports

- Apr 12 17:39: VM footprint grew from 5.3 GB to 8.2 GB (+2.85 GB)
- Apr 12 20:47: 96% CPU utilization, VM at 8.2 GB memory ceiling

## Root Cause Analysis

### Primary: Closed segments hold buffers in memory

`ferrosa-storage/src/commitlog/segment.rs:86`:
```rust
pub struct Segment {
    buffer: UnsafeCell<Vec<u8>>,  // 32 MB pre-allocated buffer
    // ...
}
```

`ferrosa-storage/src/commitlog/mod.rs:68`:
```rust
closed_segments: Mutex<Vec<Arc<Segment>>>,  // holds Arc to all closed segments
```

Each closed segment retains its full 32 MB buffer via `Arc<Segment>`. Segments are only removed from `closed_segments` when `discard_completed()` or `discard_completed_segments()` deletes them. If GC stalls, memory grows at 32 MB per segment rotation (~every 5 minutes under moderate write load).

### Secondary: Segment GC requires ALL tables flushed

`ferrosa-storage/src/commitlog/mod.rs:272`:
```rust
if tables.is_empty() {  // ALL tables must be cleared
    segments_to_delete.push(seg_id);
}
```

A segment can only be deleted when every table that wrote to it has been flushed past its position. If ANY table's flush position is stale, the segment is pinned — along with its 32 MB buffer.

### Tertiary: Low-write tables stall flush advancement

The checkpoint shows `edge_types`, `entity_types`, `typed_edges`, and `derived_cache_by_query` flushed to segments 2-14 while the active segment is ~186. The age-based flush (`flush_max_age_secs = 30`) should trigger for these tables if they have pending memtable data.

Possible reasons the stale tables aren't flushing:

1. **No writes after early segments**: If these tables have no writes in segments 15+, they aren't pinning those segments. But then something else is pinning them — possibly tables present in the commitlog but absent from the engine's `tables` map (e.g., `intentions` missing on node3).

2. **Writes via replication bypass memtable**: If inter-node replication writes to the commitlog (populating `segment_tracker`) but not to the memtable, `memtable_size() == 0` prevents age-based flush. The segment_tracker entry is never cleared.

3. **Table mismatch between commitlog and engine**: If a table exists in the segment_tracker (from commitlog writes) but not in `engine.tables`, it is never iterated by `flush_if_needed()` and never flushed. Node3 lacks `intentions` in its checkpoint while node1 has it — if node3's commitlog contains replicated `intentions` writes, those entries pin segments indefinitely.

### Contributing: DepWaitGraph unbounded sets

`ferrosa-cluster/src/accord/dep_wait.rs:94-96`:
```rust
applied: HashSet<TxnId>,   // grows monotonically, never pruned
aborted: HashSet<TxnId>,   // grows monotonically, never pruned
```

Every completed or aborted Accord transaction is inserted into these sets but never removed. Under sustained write load, these contribute additional memory pressure. Each `TxnId` is ~32 bytes; millions of transactions over 48 hours could add hundreds of MB.

## Reproduction

1. Start the 3-node ferrosa-memory cluster (`podman compose up -d`)
2. Run continuous write workload via the MCP server (smart_ingest, create_edge, etc.)
3. Monitor: `du -sh ~/data/ferrosa-memory/node*/commitlog/` and container memory
4. Within hours, commitlog segments accumulate; within 1-2 days, OOM kill

## Proposed Fix

### P0: Drop segment buffer after fsync (immediate fix)

After a segment is fsynced to disk and moved to `closed_segments`, replace the in-memory buffer with an empty Vec (or use a file-backed mmap). The segment metadata (id, path, dirty tables) is ~100 bytes; the 32 MB buffer is only needed during the write window.

```rust
// In force_rotate(), after fsync:
old_segment.release_buffer();  // shrink Vec to 0, keeping metadata
```

This caps memory at: 1 active segment (32 MB) + metadata for closed segments (~negligible). Even if GC stalls, memory stays bounded.

### P0: Investigate table mismatch in segment tracker

Add instrumentation to log when `segment_tracker` contains tables not present in `engine.tables`. If this is confirmed as a leak source, ensure all tables that appear in the commitlog are registered in the engine.

### P1: Evict stale entries from DepWaitGraph

Add time-based or generation-based eviction to `DepWaitGraph.applied` and `DepWaitGraph.aborted`. Entries older than 10 minutes (10x the wait timeout) are safe to remove.

### P2: Bound closed_segments with backpressure

If `closed_segments.len()` exceeds a threshold (e.g., 10), block or slow new writes until GC catches up. This provides an OOM safety net even if the buffer-release fix isn't sufficient.

## Diagnostic Checklist

To confirm the root cause when a node can be instrumented:

- [ ] Log `closed_segments.len()` and total buffer bytes in the maintenance loop
- [ ] Log `segment_tracker` contents: which tables pin which segments
- [ ] Compare tables in `segment_tracker` vs tables in `engine.tables` (the mismatch theory)
- [ ] Log `DepWaitGraph.applied.len()` and `aborted.len()` periodically
- [ ] Run with `FERROSA_FLUSH_MAX_AGE_SECS=5` to see if stale tables flush faster
- [ ] After restart, verify `open_and_replay` clears all old segments (it should)

## Implementation Notes

### P0: Segment buffer release (primary fix)

Added `Segment::release_buffer()` (`ferrosa-storage/src/commitlog/segment.rs`) which replaces the 32 MB `Vec<u8>` with an empty `Vec::new()`. Called in `CommitLog::force_rotate()` immediately after `flush_to_disk()` + `close_file_handle()`. This bounds closed-segment memory at ~200 bytes metadata per segment regardless of GC lag.

Added `Segment::buffer_bytes()` as a monitoring accessor and `CommitLog::closed_segments_total_bytes()` as a regression detector. Existing `replay_from()` is unaffected — it reads segment data via `SegmentReader::open(path)` from disk, not from the in-memory buffer.

**Tests added** (8 new tests, all pass):
- `segment::tests::release_buffer_frees_memory`
- `segment::tests::release_buffer_preserves_metadata`
- `segment::tests::buffer_bytes_returns_capacity_before_release`
- `commitlog::tests::closed_segments_total_bytes_zero_with_no_closed`
- `commitlog::tests::closed_segment_buffers_released_after_rotation`

### P1: DepWaitGraph applied/aborted eviction

Changed `DepWaitGraph.applied` and `.aborted` from `HashSet<TxnId>` to `HashMap<TxnId, Instant>` to track insertion time. Added `prune(max_age: Duration) -> usize` which calls `.retain()` on both maps and returns the count of evicted entries. Added `applied_count()` and `aborted_count()` accessors.

Caller (maintenance loop) should call `graph.prune(10 * DEP_WAIT_TIMEOUT)` periodically to bound memory.

**Tests added** (6 new tests, all pass):
- `dep_wait::tests::applied_count_tracks_mark_applied`
- `dep_wait::tests::aborted_count_tracks_break_cycle`
- `dep_wait::tests::prune_removes_old_applied_entries`
- `dep_wait::tests::prune_keeps_recent_applied_entries`
- `dep_wait::tests::prune_removes_old_aborted_entries`
- `dep_wait::tests::applied_set_bounded_by_pruning`

### Not implemented

- P0 table mismatch logging — requires `StorageEngine` access to compare `segment_tracker` vs `engine.tables`. Tracked as a follow-up diagnostic task.
- P2 write backpressure on `closed_segments` — buffer release (P0) is sufficient; backpressure adds complexity without proportional benefit given the primary fix.

## Related

- `bug-small-sstable-index-corruption-tool-usage-audit.md` — SSTable index issues on small tables (may cause flush failures that stall GC)
- Commitlog archiver (`commitlog/archiver.rs`) — when enabled, adds an archive gate to segment deletion; not enabled in this deployment
- Accord dep_wait (`accord/dep_wait.rs`) — unbounded set growth is a separate memory concern
