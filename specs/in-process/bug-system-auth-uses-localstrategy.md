---
type: todo
priority: P1
status: draft
created: 2026-04-23
updated: 2026-04-23
---

# Bug: `system_auth` keyspace uses `LocalStrategy`, so GRANTs and role changes don't replicate

## Why this is a Ferrosa bug

`system_auth` holds roles, password hashes, and permission grants. On a
multi-node cluster this state must be visible from every coordinator, or
authentication and authorization become non-deterministic: a given query
will succeed when it lands on a node that knows about the role/grant and
fail when round-robin routes it to one that doesn't.

Cassandra's own guidance is that `system_auth` must use
`NetworkTopologyStrategy` with an RF equal to the number of nodes in the
datacenter (or at least a quorum). Shipping it as `LocalStrategy` means a
working 3-node Ferrosa cluster with auth enabled is broken by default the
moment anyone issues a second GRANT.

## Observed on

- Ferrosa commit: `c47bfa8` (branch `fix/mixed-client-topology-and-typed-edge-bugs`)
- Cluster: local 3-node podman cluster from
  `/Users/bkearns/src/ferrosa-memory/docker-compose.yml`, auth enabled

## Symptom

Reproduced today while wiring the viz dashboard in `ferrosa-memory`. The
in-pod MCP runs the runtime CQL session as `ferrosa_user` and needed
`SELECT ON KEYSPACE agent_memory`. The GRANT was issued once via cqlsh
against `node1`, returned no error, and took effect on node1 — but
round-robin queries continued to fail about a third of the time with:

```
unauthorized: ferrosa_user lacks SELECT on table agent_memory.entity_store
```

A look at `system_schema.keyspaces` explained it:

```
 system_auth              | {'class': 'LocalStrategy'}
 agent_memory             | {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}
```

`agent_memory` is correctly replicated; `system_auth` is not. Each node
keeps its own copy of the auth tables.

## Repro

1. Start the 3-node cluster with auth enabled.
2. `cqlsh node1 9042 -u ferrosa_admin -p ferrosa_admin -e "GRANT SELECT ON KEYSPACE agent_memory TO ferrosa_user;"`.
3. From any CQL client that connects to the cluster with round-robin LB:
   `SELECT * FROM agent_memory.entity_store LIMIT 1` as `ferrosa_user`.
4. Observe that the query alternately succeeds and fails with the
   "unauthorized" error depending on which node it lands on.

## Expected

`system_auth` is replicated across the cluster so a single GRANT is
visible from every coordinator immediately after it returns.

## Fix direction

Either:

1. Change the default replication strategy for `system_auth` during
   cluster bootstrap to `NetworkTopologyStrategy` with RF = min(3, node
   count per DC). On existing clusters, `ALTER KEYSPACE system_auth
   WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}`
   + `nodetool repair system_auth` would migrate them.
2. Add a first-class CQL mechanism to replicate auth writes cluster-wide
   regardless of the replication strategy (for parity with Scylla's
   approach, which uses RAFT-replicated auth).

Option 1 matches Cassandra's conventional setup and is the least
invasive. The bootstrap DDL in `ferrosa-memory` currently uses the
cluster defaults for `system_auth` — if Ferrosa fixes the default, no
`ferrosa-memory` changes are needed.

## Workaround on the affected cluster

For today's incident, switched the `ferrosa-memory` in-pod MCP runtime
user from `ferrosa_user` to `ferrosa_admin` (matching what the host-side
launchctl agent was already doing). Viz now consistently loads. The
clean split (`ferrosa_user` for runtime + explicit GRANTs + `ferrosa_admin`
only for DDL) is blocked on this bug and on the node3 handshake
regression filed separately.
