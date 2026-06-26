---
title: bug-cql-system-peers-regresses-to-unreachable-podman-rpc-addresses
status: in-process
reported-by: ferrosa-memory live host repro
date: 2026-04-20
commit-observed: fe11d50
---

# Bug: host CQL clients discover podman-internal peer RPC addresses

## Summary

On the rebuilt 3-node local podman cluster, a host-side CQL client can now
complete the initial authenticated control connection, but full topology-based
session/bootstrap still breaks because Ferrosa advertises peer
`broadcast_rpc_address` values on the podman-internal network
(`10.89.x.x:9042`) instead of host-reachable addresses (`127.0.0.1:19043`,
`127.0.0.1:19044`).

This is visible in `cdrs-tokio` debug logs and reproduces in the real
`ferrosa-memory` MCP connect path. The symptom is a hang or repeated reconnect
while the client tries to open pools to unreachable peer addresses.

## Why this matters

`ferrosa-memory` is a host-side client. It should not need any special
workaround to reinterpret topology metadata. If Ferrosa exposes CQL on the host
ports, then the peer topology it returns to public CQL clients must also be
host-reachable.

This is not an `fmem` abstraction bug. The server is advertising endpoints that
its host clients cannot dial.

## Repro

Environment:

- local 3-node podman cluster from `../ferrosa`
- auth enabled
- seeded roles `ferrosa_admin / ferrosa_admin`
- host-side client using three contact points:
  - `127.0.0.1:19042`
  - `127.0.0.1:19043`
  - `127.0.0.1:19044`

From `ferrosa-memory`:

```bash
FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-memory-core --test cql_live \
  auth_enabled_multipoint_cql_storage_connect_matches_fmem_runtime_path \
  -- --ignored --nocapture
```

Observed logs:

```text
DEBUG cdrs_tokio::cluster::control_connection: Established new control connection.
DEBUG cdrs_tokio::cluster::metadata_builder: Copying contact point. node_info=NodeInfo { ..., broadcast_rpc_address: 127.0.0.1:19042, ... }
DEBUG cdrs_tokio::cluster::metadata_builder: Adding new node. node_info=NodeInfo { ..., broadcast_rpc_address: 10.89.1.44:9042, ... }
DEBUG cdrs_tokio::cluster::topology::node: Creating connection pool self.host_id=Some(22222222-2222-2222-2222-222222222222)
```

At the same time, the live `ferrosa-memory-mcp` process shows a SYN attempt to
the podman-internal address instead of the host-exposed CQL port:

```text
TCP 192.168.202.63:64708->10.89.1.44:9042 (SYN_SENT)
```

## Expected

For a host client connected to `127.0.0.1:19042/19043/19044`, peer metadata
returned through `system.peers` / topology discovery should resolve to the
corresponding host-reachable CQL addresses, not podman-internal addresses.

## Actual

Peer discovery returns an internal container-network RPC address for at least
one peer, causing host-side cluster session bootstrap to hang or reconnect.

## Scope

- initial single-node/authenticated control connection: now works
- full multi-node host-side session bootstrap: still broken
- `ferrosa-memory-mcp` stays blocked before becoming ready when it follows the
  full MCP/storage connect path

## Related history

This looks like a regression or incomplete fix in the same area as:

- `specs/archive/bugs-verified/bug-system-peers-missing-tokens.md`

That older bug showed the same failure mode: host clients received internal
peer addresses from topology metadata and then hung trying to connect.

## Acceptance

- host-side multi-contact-point CQL clients can connect and prepare statements
  without attempting podman-internal peer addresses
- `FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-memory-core --test cql_live \
  auth_enabled_multipoint_cql_storage_connect_matches_fmem_runtime_path \
  -- --ignored --nocapture`
  passes
- `ferrosa-memory-mcp` becomes `ready` on `28765` against the rebuilt auth
  cluster without client-side topology hacks
