---
type: todo
priority: P1
status: draft
created: 2026-04-20
updated: 2026-04-21
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

- Ferrosa commit: `fe11d50` (re-verified), previously `2faab48`, `3f868db`, and `6fce814`
- Cluster: local 3-node podman cluster from
  `/Users/bkearns/src/ferrosa-suite/ferrosa-memory/docker-compose.yml`
- Auth: enabled
- Client: `ferrosa-memory` via `cdrs-tokio`
- Credentials used by the failing client:
  - username: `ferrosa_admin`
  - password: `ferrosa_admin`

## Repro

1. Build Ferrosa from `6fce814`.
2. Start the auth-enabled 3-node cluster from
   `/Users/bkearns/src/ferrosa-suite/ferrosa-memory/docker-compose.yml`.
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

## Investigation (2026-04-20)

Reproduction was **attempted against HEAD via four new regression tests
in `ferrosa-cql/tests/handshake.rs`** and none of them fail:

- `seeded_ferrosa_admin_can_authenticate_over_v4_tcp` — full STARTUP →
  AUTHENTICATE → AUTH_RESPONSE → AUTH_SUCCESS with seeded roles.
- `seeded_ferrosa_user_can_authenticate_over_v4_tcp` — same for the
  other seeded role.
- `cdrs_tokio_shaped_handshake_options_then_startup_then_auth` — adds
  the OPTIONS → SUPPORTED prelude cdrs-tokio sends before STARTUP.
- `cdrs_tokio_startup_with_lz4_compression_completes_handshake` —
  STARTUP advertises `COMPRESSION=lz4`, asserts AUTH_SUCCESS goes out
  UNCOMPRESSED (compression flip only happens after AUTH_SUCCESS per
  CQL spec, so this pins server behavior).

All four pass. Handshake is correct for both v4 raw TCP and the
cdrs-tokio-shaped sequence, including LZ4 negotiation.

**Running cluster image is stale.** The podman image the bug reproduced
against was built at `2026-04-19 11:05 PDT` from a working tree that
predates:

- `6fce814` — writer Gate A + Gate B (restored from next-writervalidate image)
- `7f8c98f` — adjacency keyspace/table registered before observer starts
- `ddb4eba` — renamed seeded role `app_reader` → `ferrosa_user`

Before re-filing, **rebuild the cluster image from current HEAD** and
repro against that. If the bug persists, the next most likely suspects:

1. Cluster-mode-specific auth path the standalone harness does not
   exercise (Raft-mediated schema read on auth, if any).
2. `SELECT ... FROM system.local / system.peers` right after
   AUTH_SUCCESS — cdrs-tokio runs this during session-build. Add a
   test that issues these under `ferrosa_admin` and confirms no hang.
3. Frame compression state getting out-of-sync between client and
   server at the compression-flip point (after AUTH_SUCCESS).

## Root cause (2026-04-21)

`system.local.rpc_port` was the **container bind port (9042)**, not the
**host-reachable broadcast port (19042)**. `main.rs` parsed
`FERROSA_CQL_BROADCAST=127.0.0.1:19042` for its IP but then used
`cql_bind.port()` (9042) for the port:

```rust
// Before:
let node_config = ferrosa_schema::NodeConfig {
    rpc_address: cql_broadcast_addr,   // 127.0.0.1 ✓
    rpc_port: cql_bind.port(),          // 9042 ✗  (must be 19042)
    ...
};
```

After AUTH_SUCCESS, cdrs-tokio queries `system.local` and tries to
reconcile the contact point `127.0.0.1:19042` against the advertised
`(rpc_address, rpc_port) = (127.0.0.1, 9042)`. The local endpoint
appears to be a *different* node at an unreachable port; cdrs-tokio
opens a new connection to `127.0.0.1:9042` and hangs on
`read_exact` — the exact "failed to fill whole buffer" symptom.

The handshake tests passed because they skip `system.local` peer
discovery. The multi-contact-point tests in
`ferrosa-memory-core/tests/cql_live.rs` hit it because cdrs-tokio's
`RoundRobinLoadBalancingStrategy` does full peer discovery.

## Fix

Extracted the broadcast parser to
`ferrosa/src/cql_broadcast.rs::parse_cql_broadcast(raw, fallback_port)`
returning `(IpAddr, u16)`. Handles:

- `"127.0.0.1:19042"` → `(127.0.0.1, 19042)` — the fix
- `"127.0.0.1"` → `(127.0.0.1, fallback)` — pure-IP unchanged
- `"host.containers.internal:19043"` → DNS-resolved IP + 19043
- Garbage → `(127.0.0.1, fallback)`

5 unit tests pin these behaviors, including one named
`port_mapped_container_advertises_host_port_not_bind_port` that
explicitly asserts the scenario behind this bug cannot regress.

`main.rs` now destructures `(cql_broadcast_addr, cql_broadcast_port)`
from the helper and uses BOTH for `NodeConfig`. Also added an INFO log
at startup so the advertised address is visible on the operator side.

## Implementation Notes

Regression coverage lives at:
- `ferrosa/src/cql_broadcast.rs::tests` — 5 parser tests
- `ferrosa-cql/tests/handshake.rs` — 4 handshake tests (OPTIONS →
  STARTUP → AUTH_RESPONSE → AUTH_SUCCESS, plus LZ4 negotiation)

The live-cluster repros in
`ferrosa-memory-core/tests/cql_live.rs` (gated on
`FERROSA_TEST_CONTAINERS=1`) should now pass after a cluster rebuild
from the commit that includes this fix.

## Better repro (2026-04-21)

The standalone handshake tests are too narrow. They prove:

- OPTIONS → STARTUP → AUTH_RESPONSE → AUTH_SUCCESS is correct
- seeded-role auth works at the raw protocol level
- the LZ4 negotiation point is not obviously broken

They do **not** reproduce the actual `ferrosa-memory` failure shape.

### Stronger repro now added in `ferrosa-memory`

Two live tests in
`/Users/bkearns/src/ferrosa-suite/ferrosa-memory/crates/ferrosa-memory-core/tests/cql_live.rs`
match the real failing path much more closely:

1. `auth_enabled_multipoint_cdrs_session_build_succeeds`
   - uses `StaticPasswordAuthenticatorProvider`
   - uses **all three** contact points:
     - `127.0.0.1:19042`
     - `127.0.0.1:19043`
     - `127.0.0.1:19044`
   - uses `RoundRobinLoadBalancingStrategy`
   - uses the same 10s timeout envelope as `ferrosa-memory`
   - prepares a real `agent_memory.memo_cache` statement after session build

2. `auth_enabled_multipoint_cql_storage_connect_matches_fmem_runtime_path`
   - calls `CqlStorage::connect()` directly
   - uses the same config shape as the running `ferrosa-memory-mcp`
   - immediately exercises the prepared-statement path that `fmem` uses

### Result on rebuilt cluster

Rebuilt local cluster from Ferrosa commit `fe11d50`, then ran:

```bash
FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-memory-core --test cql_live \
  auth_enabled_multipoint_cdrs_session_build_succeeds -- --ignored --nocapture
```

Result:

- panics after 10s with `session build timed out: Elapsed(())`

Then ran:

```bash
FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-memory-core --test cql_live \
  auth_enabled_multipoint_cql_storage_connect_matches_fmem_runtime_path \
  -- --ignored --nocapture
```

Result:

- panics after 10s with:
  `CqlStorage::connect should succeed on the auth-enabled local cluster: \
  CQL session build timed out (10s) — is Ferrosa running?`

### Why this is a better repro

This narrows the bug far better than the earlier handshake-only tests:

- if handshake-only tests pass, but authenticated multi-contact-point
  session build still times out, the bug is likely in the **post-auth
  session bootstrap path**, not the raw auth frames
- likely areas:
  - peer discovery / topology reads during session build
  - authenticated `system.local` / `system.peers` queries
  - multi-node contact-point handling after AUTH_SUCCESS
  - server behavior on the first normal request after auth, not auth itself

### Updated hypothesis

The current best hypothesis is:

- raw auth handshake is fixed
- authenticated cluster-mode session bootstrap for `cdrs-tokio`
  remains broken

That keeps this issue open even though the four handshake regressions
are green.

## Workbench confirmation on `fe11d50`

With `ferrosa-memory-mcp` restarted against the rebuilt local cluster:

- `GET https://127.0.0.1:28765/healthz/ready` => `not ready`
- `GET http://127.0.0.1:28766/workbench/api/summary` =>
  `{"status":"not_ready","error":"CQL connection not yet established, retrying in background...",...}`
- `POST /workbench/api/cql/query` =>
  `{"error":"CQL connection not yet established, retrying in background..."}`
- `POST /workbench/api/sparql/query` still succeeds

So the live application symptom and the focused repro tests still line up on
`fe11d50`.

## Second root cause + fix (2026-04-21)

The `rpc_port` fix (5061f13) was necessary but not sufficient. After
the contact-point handshake completes, cdrs-tokio's
`cluster_metadata_manager::refresh_node_infos` runs `is_peer_row_valid`
on every row of `system.peers`. That validator (cdrs-tokio's
`cluster_metadata_manager.rs:210-222`) requires the `tokens` column to
be NON-empty:

```rust
fn is_peer_row_valid(row: &Row) -> bool {
    let has_rpc_address = ...;
    has_rpc_address
        && !row.is_empty_by_name("host_id")
        && !row.is_empty_by_name("data_center")
        && !row.is_empty_by_name("rack")
        && !row.is_empty_by_name("tokens")        // <-- the problem
        && !row.is_empty_by_name("schema_version")
}
```

Ferrosa's `RaftClusterState::peers()` was returning `tokens: vec![]`
for every peer regardless of how many tokens the ring had assigned to
that peer. cdrs-tokio's `filter_map` therefore dropped every peer row,
leaving the topology pool effectively single-node and driving an
internal state where session-build never converges. On the wire side
this manifested as `failed to fill whole buffer` retries until the
10s session-build timer fired.

Fix: populate `tokens` in `RaftClusterState::peers()` from the live
`TokenRing` via `ring.tokens_for_node(id)`, formatted as decimal
strings (matching Cassandra's `set<text>` shape on `system.peers`).

Regression coverage:
- `ferrosa-cluster::state::tests::raft_cluster_state_populates_tokens_from_ring_for_peers`
  pins that any peer with ring-assigned tokens reports a non-empty
  `tokens` field.
- `ferrosa-cql::tests::handshake::cdrs_tokio_session_bootstrap_queries_return_well_formed_results`
  pins that the four exact queries cdrs-tokio's
  `cluster_metadata_manager` runs (system.local, system.peers,
  system.peers_v2, `SELECT keyspace_name, toJson(replication) AS
  replication FROM system_schema.keyspaces`) all return well-formed
  Rows results under authenticated context.
- `ferrosa-cql::tests::handshake::auth_enabled_post_auth_introspection_does_not_hang`
  pins the broader cqlsh-style introspection sequence under auth.
