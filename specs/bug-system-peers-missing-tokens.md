# BUG: `FERROSA_CQL_BROADCAST` not propagated to `system.peers` on other nodes

**Severity:** P1 — blocks token-aware CQL drivers from connecting to multi-node clusters behind port-mapped containers
**Branch:** `fix/standalone-progressive-join` (commits up to `c3e269b`)
**Found:** 2026-04-02
**Reporter:** ferrosa-memory-mcp (cdrs-tokio client)

## Summary

`c3e269b` added `FERROSA_CQL_BROADCAST` support, which correctly sets `system.local.rpc_address` on the node that owns the env var. However, the broadcast address is **not propagated through Raft** to other nodes' `system.peers` tables. When a client queries `system.peers` on node1, it still sees container-internal IPs for node2 and node3, even though node2 and node3 have `FERROSA_CQL_BROADCAST` set and their own `system.local` reflects it.

CQL drivers discover peer nodes via `system.peers` and attempt direct connections to the advertised `native_address`. When those are unreachable container IPs, the driver hangs.

## Reproduction

```yaml
# docker-compose.yml — all three nodes have FERROSA_CQL_BROADCAST set:
node1:
  environment:
    FERROSA_CQL_BROADCAST: "127.0.0.1:19042"
node2:
  environment:
    FERROSA_CQL_BROADCAST: "127.0.0.1:19043"
node3:
  environment:
    FERROSA_CQL_BROADCAST: "127.0.0.1:19044"
```

```
-- system.local on node1 is correct:
cqlsh node1> SELECT rpc_address FROM system.local;
  127.0.0.1   ✓

-- system.peers on node1 still shows container IPs:
cqlsh node1> SELECT peer, native_address, native_port FROM system.peers;
  10.89.0.217 | 10.89.0.217 | 9042    ✗ (should be 127.0.0.1:19043)
  10.89.0.220 | 10.89.0.220 | 9042    ✗ (should be 127.0.0.1:19044)
```

## Root cause

The `cql_broadcast` field is added to `NodeInfo` in `ferrosa-cluster/src/state.rs` and used when building the peers list. But when building `system.peers` rows for remote nodes, the code falls back to the internode IP rather than using the peer's `cql_broadcast` from the Raft ring state.

The test `raft_cluster_state_uses_cql_broadcast_for_native_address` in `state.rs:154` passes because it sets `cql_broadcast` directly on the `NodeInfo` struct — but in production, the broadcast value from node2's config needs to be propagated through the Raft ring to node1's `system.peers` view.

## Additional issue: hostname support

`FERROSA_CQL_BROADCAST` only accepts IP:port format. Setting `host.containers.internal:19043` logs:

```
WARN ferrosa: FERROSA_CQL_BROADCAST=host.containers.internal:19043 is not a valid address, falling back to 127.0.0.1
```

Should resolve hostnames or at minimum document IP-only requirement.

## cdrs-tokio behavior

```
DEBUG cdrs_tokio: Copying contact point. broadcast_rpc_address: [::1]:19042   ← correct (from system.local)
DEBUG cdrs_tokio: Adding new node. broadcast_rpc_address: 10.89.0.217:9042    ← wrong (from system.peers)
DEBUG cdrs_tokio: Adding new node. broadcast_rpc_address: 10.89.0.220:9042    ← wrong (from system.peers)
DEBUG cdrs_tokio: Creating connection pool host_id=22222222-...
# hangs — 10.89.0.217:9042 is unreachable from host
```

## Impact

- Any CQL driver with peer auto-discovery fails against port-mapped clusters
- `system.local` fix alone is insufficient — drivers use `system.peers` to discover and connect to all nodes
- The `d7a951e` tokens fix unmasked this: previously cdrs-tokio failed before reaching peer discovery

## Expected behavior

When node2 has `FERROSA_CQL_BROADCAST=127.0.0.1:19043`, querying `system.peers` on **any other node** should return `native_address=127.0.0.1, native_port=19043` for node2's row.
