# BUG: Raft internode transport timeout prevents cluster formation

**Status:** PARTIALLY RESOLVED — `808b72b` moved ClusterInvite to Data lane; `30768c0` delivers synchronously before Raft init. Root Raft timeout may still occur if peers don't transition to cluster mode (see BUG-RAFT-HANDLER-RACE.md).

## Symptom

3-node ferrosa cluster via podman compose. All nodes healthy (CQL port 9042 open). TCP connectivity between containers works (`/dev/tcp/node2/7000` succeeds). But Raft elections fail with:

```
ERROR openraft::core::raft_core: timeout error=timeout after 1s when Vote N1->N2
ERROR openraft::core::raft_core: while requesting vote error=Unreachable node: ferrosa_net::error::NetError: timeout: Raft lane timeout
```

CQL returns `unpack requires a buffer of 2 bytes` — the port is open but CQL is blocked waiting for Raft leader election.

## Environment

- ferrosa branch: `fix/standalone-progressive-join` (ba7599a)
- Also reproduced on merged `main` (d79d74b) 
- docker-compose.yml in ferrosa-memory
- `FERROSA_MODE=dev` set on all nodes
- `FERROSA_CLUSTER_MODE` removed (auto-detect)
- Raft state wiped (`rm -rf ~/data/ferrosa-memory/node{1,2,3}/raft`)
- Fresh start — same result

## What works

- TCP: `bash -c 'echo > /dev/tcp/node2/7000'` succeeds from node1
- Healthcheck: all 3 nodes report healthy (TCP 9042 open)
- Node1 can vote for itself (self-vote succeeds)

## What fails

- Raft lane transport: `ferrosa_net::error::NetError: timeout: Raft lane timeout`
- Node1 cannot reach node2 (id 2459565876494606882) or node3 (id 3689348814741910323)
- Node IDs derived from FERROSA_HOST_ID UUIDs

## Likely cause

The Raft lane in `ferrosa-net` uses a multiplexed connection (separate from raw TCP). Either:
1. The Raft lane handshake fails silently (TLS? auth? protocol version mismatch?)
2. The connection pool doesn't resolve `node2`/`node3` hostnames correctly at the Raft layer
3. The lane send buffer fills up and times out because the receiving end isn't draining

## Files to investigate

- `ferrosa-net/src/` — RpcClient, lane management, connection pooling
- `ferrosa-cluster/src/raft/` — how Raft messages are sent via the net layer
- The `Raft lane timeout` error message origin in ferrosa-net
