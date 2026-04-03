# BUG: Partition key lookup returns empty on replica nodes despite RF=3

**Severity:** P0 — data appears lost on 2 of 3 nodes for partition-key queries
**Branch:** `fix/standalone-progressive-join` (commits up to `1456778`)
**Found:** 2026-04-02
**Reporter:** ferrosa-memory-mcp
**Status:** ALL FIXES COMPLETE — Bug A (keyspace_rf → ReplicationStrategy), Bug B (WritePath NTS dispatch), Bug C (datacenter default) all fixed, 575+512 tests green, clippy clean.

## Summary

Data written to a 3-node cluster with `NetworkTopologyStrategy` RF=3 is only queryable via partition key on the coordinator node that received the write. The other two nodes return 0 rows for the same partition key query, even though a full table scan (`SELECT COUNT(*)`) shows all rows present on every node.

## Reproduction

### Setup

3-node cluster via docker-compose, `agent_memory` keyspace with NTS RF=3:

```
keyspace=agent_memory, replication={'datacenter1': '3', 'class': 'NetworkTopologyStrategy'}
```

Nodes report datacenter as `dc1` (from `system.local`/`system.peers`), but the keyspace replication config references `datacenter1`. This mismatch may be the root cause.

### Write data to node1

```bash
# Restore 2039 entities via python cassandra-driver on port 19042 (node1)
./scripts/restore-memory.sh ~/data/ferrosa-memory/backup-golden-20260402
```

### Verify full table scan works on all nodes

```python
for port in [19042, 19043, 19044]:
    # SELECT COUNT(*) FROM agent_memory.entity_store
    # Result: 2039 on all 3 nodes ✓
```

### Partition key lookup fails on node2 and node3

```python
tenant = uuid.UUID('9a5f8fbf-d842-4d30-8ea5-1aa931e618a8')
session = uuid.UUID('00000000-0000-0000-0000-000000000000')

for port in [19042, 19043, 19044]:
    # SELECT entity_id FROM entity_store WHERE tenant_id = ? AND session_id = ?
    # port 19042: entities=2038, typed_edges=4964  ✓
    # port 19043: entities=0, typed_edges=0         ✗
    # port 19044: entities=0, typed_edges=0         ✗
```

### Observed behavior in production

cdrs-tokio uses `RoundRobinLoadBalancingStrategy` across all 3 nodes. Queries succeed ~1/3 of the time (when routed to node1) and return empty ~2/3 of the time (when routed to node2 or node3).

## Root Cause Analysis (confirmed 2026-04-02)

Three converging bugs, all in the CQL router → coordinator wiring:

### Bug A: `keyspace_rf()` ignores NTS per-DC replication (PRIMARY)

**File:** `ferrosa-cql/src/router.rs:3889-3896`

```rust
fn keyspace_rf(schema: &Schema, ks: &str) -> usize {
    snap.keyspaces.get(ks)
        .and_then(|km| km.replication.options.get("replication_factor"))  // ← only SS key
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1)  // ← returns 1 for NTS keyspaces!
}
```

For NTS keyspaces, options contain `{"datacenter1": "3"}` — no `"replication_factor"` key. Function returns **RF=1** instead of 3. Called at 4 sites: INSERT (line 1769), UPDATE (2149), DELETE (2332), BATCH (2464).

**Result:** Writes only go to 1 node (the coordinator). Other nodes never receive the data.

### Bug B: CQL router never dispatches to NTS-aware coordinator methods

`coordinate_write_nts()` (write.rs:194) and `coordinate_read_nts()` (read.rs:636) are fully implemented with per-DC ACK tracking and LOCAL_QUORUM support, but the router always calls SimpleStrategy methods. A full implementation plan exists at `superpowers/plans/2026-03-20-c1-network-topology-strategy.md` but was never wired into the router.

### Bug C: Datacenter name mismatch (`dc1` vs `datacenter1`)

**File:** `ferrosa-cluster/src/config.rs:54-58` — default datacenter is `"dc1"`

Nodes report `dc1` in system.local/system.peers, but Cassandra convention (and user keyspaces) use `datacenter1`. Even if RF extraction were fixed, `nts_replicas()` (ring/mod.rs:190) would return empty because no node's `data_center` matches the keyspace's DC name.

### Why full scans work

`SELECT COUNT(*)` uses `read_range()` which iterates all local partitions sequentially without token-based lookup. The coordinator node has the data locally (it wrote it), so full scans return correct counts from that node. Partition key lookups use `engine.read(&table_id, &decorated_key)` which requires the data to be in local storage — it's not, because it was never replicated.

## Impact

- Any multi-node deployment with NTS keyspaces and partition-key queries returns intermittent empty results
- Token-aware drivers route to the "correct" node based on partition key hash, which may not be the coordinator, causing consistent failures
- Round-robin drivers see ~1/N success rate where N is the cluster size
- **This makes multi-node clusters unusable for production workloads**

## Fix Plan

### Fix 1: NTS-aware RF extraction (S, blocks everything)

Replace `keyspace_rf()` with a function that returns the replication strategy, not just RF:

```rust
fn keyspace_replication(schema: &Schema, ks: &str) -> ReplicationInfo {
    let snap = schema.snapshot();
    let km = snap.keyspaces.get(ks);
    match km.replication.strategy.as_str() {
        "NetworkTopologyStrategy" => ReplicationInfo::NTS(km.replication.options.clone()),
        _ => ReplicationInfo::Simple(keyspace_rf(schema, ks)),
    }
}
```

### Fix 2: Router dispatches to NTS coordinator (M)

At each of the 4 call sites (INSERT, UPDATE, DELETE, BATCH), dispatch to `coordinate_write_nts()` when the keyspace uses NTS. Same for reads.

### Fix 3: Validate DC name at keyspace creation (S)

When `CREATE KEYSPACE ... WITH REPLICATION = {'class': 'NTS', 'datacenter1': '3'}`, check that `datacenter1` matches at least one node's `data_center`. Warn or reject if no match.

### Fix 4: Default datacenter should be `datacenter1` (S)

Change default from `"dc1"` to `"datacenter1"` to match Cassandra convention.

## Expected behavior

With RF=3 and 3 nodes, every node should return the same results for any partition key query at CL=ONE.
