---
type: todo
priority: P1
status: draft
created: 2026-05-16
updated: 2026-05-16
affected-versions: ferrosa v0.10.0 and earlier (pre-existing; v0.10.0 timeouts are smaller/different but still present)
---

# Bug: Cross-node coordinated reads time out on the Bulk lane

## Why this is a Ferrosa bug

Ferrosa advertises CQL wire-protocol compatibility and a 3-node clustered
deployment. CQL range reads, secondary-index reads, and the
`/workbench/api/summary` endpoint all coordinate across replicas via the
`Bulk` internode lane. Every coordinated read in the 3-node
`ferrosa-memory` cluster fails with `net: timeout: Bulk lane send timeout`
on at least one peer — bidirectionally, across all pairs — even when:

- TCP peer connections are established (`peer connected` events fire
  promptly at startup).
- Reverse connections are established.
- ClusterInvites flow across the same lane set.
- Raft eventually elects a leader (after a ~3min cold-start settle).
- Local CQL reads on each node work fine.

Clients that only execute single-partition reads against the
local coordinator (e.g. `cqlsh ... SELECT COUNT(*) FROM <local-replica
table>`) succeed. Clients that issue range scans, multi-replica reads, or
hit `workbench/api/*` see partial-result warnings or 500-level failures.

Observed both on the v0.9.0 image (user-reported pre-existing timeouts)
and the v0.10.0 image rebuilt 2026-05-16. v0.10.0 appears better than
v0.9.0 in this user's testing — fewer queries time out overall — but the
underlying coordinator timeout still reproduces deterministically on
range reads.

## Observed on

- Ferrosa: `ferrosa-memory-node:v0.10.0` (image sha
  `badbc54253b0`), built from main HEAD at commit `b7eb20c`
  (`chore: release v0.10.0 (#40)`).
- Cluster: `ferrosa-memory/docker-compose.yml` (3 nodes + ferrosa-memory-mcp
  + MinIO). Auth enabled. `host_id` 11111111-…, 22222222-…, 33333333-….
- Bridge network `172.20.0.0/16`, internode 7000/tcp.
- Date: 2026-05-16.

## Symptom

After the cluster comes up healthy (all 5 containers
`docker compose ps` healthy, Raft leader elected on node3, schema visible
on every node), curl the workbench summary endpoint:

```
$ curl -sS -u ferrosa_user:ferrosa_user http://127.0.0.1:18765/workbench/api/summary
{
  "derived_fact_count": 1924,
  "edge_count": 0,
  "error": "Database returned an error: Internal server error. ...
            Error message: server error: cluster error: internal:
            index read from node 1229782938247303441 (11111111-1111-1111-1111-111111111111)
            via 172.20.0.3:7000: net: timeout: Bulk lane send timeout",
  "node_count": 0,
  "rule_count": 0,
  "status": "not_ready"
}
```

Node-side logs show the timeout on every cross-replica read:

```
ERROR cql.request{cql.opcode=Query client.address=172.20.0.1:49730}:
  ferrosa_cluster::coordinator::read:
  coordinate_range_read: internal: range read from node 3689348814741910323
  (33333333-3333-3333-3333-333333333333) via 172.20.0.5:7000:
  net: timeout: Bulk lane send timeout

WARN  cql.request{...}:
  coordinate_range_read: 2 node(s) failed, returning partial results
  from 1 node(s) failed_nodes=2 partitions_received=34
```

Raft replication is also affected:

```
WARN  openraft::replication: error replication to target=1229782938247303441
  error=timeout after 600ms when AppendEntries 3689348814741910323->1229782938247303441
```

Local single-replica CQL queries succeed:

```
$ cqlsh 127.0.0.1 19042 -u ferrosa_admin -p ferrosa_admin
  -e "SELECT COUNT(*) FROM agent_memory.entity_store;"
 count
-------
  9774
```

Same query against node2 (19043) returns 9774; node3 (19044) returns
10800 (replica skew, likely separate).

## Performance baseline (2026-05-16)

Three back-to-back `SELECT COUNT(*) FROM agent_memory.entity_store` runs
on the live cluster (9 774 partitions, RF=3, 3 nodes, no concurrent
load) under the v0.10.0 image:

| Run | Wall-clock | Notes                                          |
|-----|-----------|------------------------------------------------|
| 1   | 7.28 s    | survives because cqlsh `--request-timeout=60`  |
| 2   | 37.15 s   | same query — variance is intrinsic, not noise  |
| 3   | 7.32 s    | reproducible                                   |

A bounded `SELECT … LIMIT 5` against the same table returns in 0.65 s
because the coordinator does not need to fan out a full range scan.

On-disk size: `~/data/ferrosa-memory/minio/` is 3.6 GB; node1's
`/var/lib/ferrosa` working set is 6.1 GB.

## Why tweaking the timeout is the wrong axis

- The current `BULK_READ_TIMEOUT = 3 s` already fires; the proposed bump
  to 8 s (PR #41) still loses Run 2 (37 s). No fixed wall-clock value is
  correct for both the steady-state (~7 s) and the outlier (~37 s)
  observed *today* at 9 K partitions, let alone for production-scale
  tables with millions or billions of partitions.
- Magic-number caps are antipatterns. The existing
  `RANGE_READ_MATERIALIZATION_CAP = 10_000` in `ferrosa-storage` is a
  declared antipattern — its own error message says
  `"use a paged/streaming read path"`. Adding a hardcoded 8 s timeout
  alongside it compounds the problem.
- The Bulk lane already has a 60 s envelope timeout
  (`Lane::Bulk.timeout()` in `ferrosa-net/src/codec.rs`); the
  coordinator's 3 s/8 s wall-clock cap on top of that is the
  contradiction — Bulk lane is sized for high-throughput
  latency-tolerant transfers, but the coordinator denies it the time
  to actually transfer.

The correct architecture replaces the single-shot RPC with a streaming
response gated by an **idle-timeout watchdog** that resets every time a
chunk or heartbeat arrives. See
[[020-streaming-internode-range-read]] for the design.

## Suspected scope

- `ferrosa_cluster::coordinator::read::coordinate_range_read` and
  `coordinate_index_read` push work over the `Bulk` lane defined in
  `ferrosa-net`.
- Symptom is `Bulk lane send timeout` — the send side gives up before
  the receiver responds. The new envelope framing gate in PR #39
  (`Add CapnProto internode envelope framing gate`,
  `Add CapnProto cluster recovery adapters`) touched lane dispatch.
  Worth checking whether `Bulk` lane handler is still wired up the same
  way after the gate landed, and whether the gate's default state
  exposes a bug in the legacy path.
- Worth also checking: per-lane queue depth limits, the
  reconnect/backoff loop when one peer goes down briefly during cluster
  bring-up, and whether the openraft 0.9 `loosen-follower-log-revert`
  feature interacts.

## Repro

1. `cd ferrosa-suite/ferrosa-memory`
2. `docker compose down && docker compose up -d`
3. Wait ~3min for Raft to converge.
4. `curl -sS -u ferrosa_user:ferrosa_user http://127.0.0.1:18765/workbench/api/summary | jq .`
   → `status: not_ready`, error contains `Bulk lane send timeout`.
5. `cqlsh 127.0.0.1 19042 -u ferrosa_admin -p ferrosa_admin -e "SELECT * FROM agent_memory.entity_store LIMIT 10;"`
   → succeeds (local-replica, no coordinator fan-out).

## Why it's load-bearing

The `not_ready` summary status is the gate `smoke-18765.sh` checks before
declaring the cluster healthy. Without coordinated reads, the
workbench/MCP "knowledge graph console" cannot present aggregate stats;
clients that query across partitions degrade to partial results; and
Raft heartbeats time out at 600ms — within tolerance for leader stability
but indicative that the same lane path that breaks reads also slows
consensus.

## Validation steps before declaring fixed

- `smoke-18765.sh` runs to completion (passes `summary status is ready`).
- `cqlsh -e "SELECT COUNT(*) FROM agent_memory.<table>"` against any
  table where ownership crosses nodes succeeds without
  `partial results from N node(s)` warnings.
- 60s sample of node logs after a cold restart contains zero
  `Bulk lane send timeout` lines.
- Raft `AppendEntries` heartbeats stay under the 600ms timeout.

Related: [[refactor-cluster-recovery-storage-oom-seams]],
[[019-capnproto-internode-protocol]].
