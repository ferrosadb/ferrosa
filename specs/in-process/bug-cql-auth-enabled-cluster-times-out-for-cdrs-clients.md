---
type: todo
priority: P1
status: draft
created: 2026-04-20
updated: 2026-04-20
---

# Bug: auth-enabled Ferrosa cluster times out for `cdrs-tokio` CQL clients

## Why this is a Ferrosa bug

`ferrosa-memory` is using the public Cassandra CQL wire protocol on
port `9042`. That is a public compatibility surface. If a standard Rust
CQL client that previously worked can no longer complete session
establishment against an auth-enabled Ferrosa cluster, that is a server
compatibility bug, not something the client should paper over.

The failure mode here is not a clean auth error. The client stalls until
timeout while the transport logs repeated:

- `IO error: failed to fill whole buffer`

That points to a broken or incomplete wire-level handshake/response
path.

## Observed on

- Ferrosa commit: `6fce814`
- Cluster: local 3-node podman cluster from
  `/Users/bkearns/src/ferrosa-memory/docker-compose.yml`
- Auth: enabled
- Client: `ferrosa-memory` via `cdrs-tokio`
- Credentials used by the failing client:
  - username: `ferrosa_admin`
  - password: `ferrosa_admin`

## Repro

1. Build Ferrosa from `6fce814`.
2. Start the auth-enabled 3-node cluster from
   `/Users/bkearns/src/ferrosa-memory/docker-compose.yml`.
3. Start `ferrosa-memory-mcp` with:

```bash
FERROSA_MEMORY_CONFIG=/tmp/ferrosa-memory-smoke-28765/ferrosa-memory-http.toml \
target/debug/ferrosa-memory-mcp
```

4. Wait for startup.

## Actual

- Graph HTTP connects successfully.
- SPARQL passthrough works.
- CQL never becomes ready.
- `ferrosa-memory` stays in reconnect mode.
- `/healthz/ready` reports `not ready`.
- Workbench CQL and Datalog surfaces return:
  - `{"error":"CQL connection not yet established, retrying in background..."}`

Relevant client log lines:

- `CQL connection failed (CQL session build timed out (10s) — is Ferrosa running?)`
- repeated `cdrs_tokio::transport: IO error: failed to fill whole buffer`

## Expected

- A public CQL client using valid credentials should either:
  - connect successfully, or
  - fail immediately with a protocol-correct auth/permission error

It should not hang until timeout on session establishment.

## Impact

- `ferrosa-memory` cannot become ready against the auth-enabled cluster
  even though graph and SPARQL are up.
- All CQL-backed `ferrosa-memory` features are blocked:
  - workbench summary counts
  - CQL explorer
  - local Datalog evaluation
  - most MCP tool paths

## Acceptance

- `cdrs-tokio` clients can establish a session against the auth-enabled
  cluster using `ferrosa_admin`.
- `ferrosa-memory-mcp` reaches ready state without reconnect churn.
- `POST /workbench/api/cql/query` succeeds for a simple probe such as:

```sql
SELECT * FROM agent_memory.entity_store LIMIT 1
```

