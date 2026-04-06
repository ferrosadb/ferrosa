# Bug: CQL Port Accepts Connections Before Schema Is Ready to Serve Queries

**Severity:** High (user-facing)
**Component:** ferrosa-cql / cluster formation

## Issue

After cluster formation, the CQL port (9042) starts accepting TCP connections and the health check passes, but queries against user keyspaces timeout because DDL propagation hasn't completed.

A client connects successfully, but `SELECT COUNT(*) FROM agent_memory.entity_store` times out. The client has no way to know the server isn't ready — it just looks like a broken cluster.

## Expected Behavior

Either:
- **Option A:** CQL port should not accept connections until the schema is fully propagated and queries can be served. The health check TCP probe would then naturally gate readiness.
- **Option B:** CQL server returns a proper error (`SERVER_ERROR` or `UNAVAILABLE`) with a message like "schema not yet ready" instead of silently timing out.
- **Option C:** Add a readiness probe separate from liveness (`/ready` endpoint that checks schema propagation status).

## Current Behavior

1. Cluster starts, CQL port opens immediately
2. Health check passes (TCP connect succeeds)
3. Client connects, sends query
4. Query times out after 10s (default client timeout)
5. Client sees `cassandra.OperationTimedOut` — indistinguishable from a real timeout
6. After ~30-60 seconds, schema propagates and queries work

## Impact

- Restore scripts fail on first attempt, requiring manual retry
- MCP server reconnection fails, requiring restart
- Monitoring systems get false "cluster unhealthy" alerts during the ready window
- Users think the cluster is broken

## Reproduction

```bash
podman compose down
rm -rf ~/data/ferrosa-memory/node{1,2,3}/*
podman compose up -d
# Immediately after health checks pass:
cqlsh -e "SELECT COUNT(*) FROM agent_memory.entity_store" 127.0.0.1 19042
# → OperationTimedOut
# Wait 30-60 seconds:
cqlsh -e "SELECT COUNT(*) FROM agent_memory.entity_store" 127.0.0.1 19042
# → (count 0) — works
```

## Proposed Fix

Option B is the least disruptive: in the CQL request handler, check if the requested keyspace exists in the local schema registry before executing the query. If not yet propagated, return `SERVER_ERROR` with "keyspace not ready" instead of attempting to read from non-existent tables (which hangs waiting for Raft DDL).

This gives clients an immediate, actionable error instead of a silent timeout.

## Workaround

Add a retry loop to restore scripts and MCP server connection:
```python
for attempt in range(30):
    try:
        session.execute('SELECT now() FROM system.local')
        break
    except OperationTimedOut:
        time.sleep(2)
```

## Verification (2026-04-05, commit 9e74cd5)

Fresh cluster, immediate CQL query: `SELECT COUNT(*) FROM agent_memory.entity_store` returns count=0 instantly. No timeout.
- **Status: VERIFIED FIXED**
