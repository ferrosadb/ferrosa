# TODO: Manifest Generation ID Collision on Multi-Node Flush

**Severity:** Critical (data loss when two nodes flush with same gen ID)
**Component:** ferrosa-storage

## Issue

Flush generation numbers are allocated per-node from local counters. Two nodes can independently produce SSTables with the same generation ID (e.g., both gen=5). When both update the manifest via save_with_retry:

1. Node A saves manifest with gen=5 (its SSTable)
2. Node B's save conflicts, reloads, merge_into sees gen=5 already present
3. merge_into REPLACES Node A's entry with Node B's (same ID, different data)
4. Node A's SSTable is orphaned in S3 — its data is lost from the manifest

## Fix

Use globally unique SSTable IDs:
- UUID-based IDs instead of sequential generation numbers
- Or prefix with node_id: `{node_uuid}_{gen}`
- Or use Raft to allocate global sequence numbers
