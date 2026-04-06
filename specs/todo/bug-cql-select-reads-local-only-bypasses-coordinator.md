# BUG: CQL SELECT Reads Local Storage Only — Bypasses Cluster Read Coordinator

**Severity:** P0 (data loss — reads return empty for data that exists on other nodes)
**Component:** ferrosa-cql

## Issue

`route_select_user_table` in `router.rs` calls `state.engine.read()` which reads
from LOCAL storage only. It does NOT use `ClusterCoordinator::coordinate_read()`
to route to the correct replica.

With RF=1 on a 3-node cluster:
- Writes correctly route to the token owner via `coordinate_write`
- Reads go to LOCAL storage via `engine.read` — if this node isn't the owner, returns empty
- CQL driver round-robin means 2/3 of reads miss on a 3-node cluster

This is the root cause of the persistent data loss bug (specs/todo/bug-large-write-causes-data-loss-in-partition.md).

## Evidence

- Data present immediately after ingest (100% canaries): writes are routed correctly
- Data "lost" after 90-120s: background compaction flushes memtable, reads from wrong node return empty
- No SSTable corruption in latest run: the data EXISTS on the correct node, it's just not being READ from there
- Same pattern every run: 15K entities → 2.2K after compaction (only data owned by the coordinator's local token range survives)

## Affected Code

All `state.engine.read()` calls in router.rs:
- Line 1031: FTS query result fetch
- Line 1823: IF NOT EXISTS check
- Line 2024: SELECT partition read  
- Line 4256: helper function

## Fix

Replace `state.engine.read()` with `state.coordinator.coordinate_read()` in the SELECT path.
This routes the read to the correct replica via the token ring, matching the write path.

For pair mode (2-node), reads can stay local since both nodes have all data.
For cluster mode, reads MUST go through the coordinator.
