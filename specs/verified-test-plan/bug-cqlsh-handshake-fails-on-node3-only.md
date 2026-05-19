---
> Docs audit note: moved from `specs/in-process/` because the item needs a fresh live reproduction before it can be marked fixed.

type: todo
priority: P2
status: draft
created: 2026-04-23
updated: 2026-04-23
---

# Bug: cqlsh 4.1 handshake fails against one cluster node while identical nodes succeed

## Why this is a Ferrosa bug

Ferrosa advertises itself as Cassandra-compatible (`cqlsh ... was built
against 4.1.11, but this server is 5.1.0`). The standard Python
`cassandra-driver` inside cqlsh is the reference client for that
compatibility surface. If cqlsh connects to some nodes in the cluster
but consistently fails to connect to others — with identical config,
identical image, identical role set — that is a Ferrosa wire-protocol
regression on the failing node. The symptom is a *client-side* parse
error, which means the server sent a response the reference driver
can't decode.

This masks operator workflows: GRANT, ALTER ROLE, node-specific
diagnostics, schema introspection via cqlsh. `cdrs-tokio` works around
whatever node3 is sending, so application traffic is unaffected and the
bug is invisible until someone reaches for cqlsh.

## Observed on

- Ferrosa commit: `c47bfa8` (branch `fix/mixed-client-topology-and-typed-edge-bugs`)
- Cluster: local 3-node podman cluster from
  `/Users/bkearns/src/ferrosa-suite/ferrosa-memory/docker-compose.yml`, auth enabled
- Client: official `docker.io/library/cassandra:4.1` image,
  `cqlsh` (Python cassandra-driver)

All three nodes are:

- Up and healthy per podman.
- Responding to `cdrs-tokio` from `ferrosa-memory-mcp` (SELECT, INSERT,
  CREATE TABLE all work).
- Accepting TCP on port 9042 (the driver reports `Unable to connect`
  but the underlying error is a decode failure, not a refused connect).

## Symptom

```
$ cqlsh node1 9042 -u ferrosa_admin -p ferrosa_admin -e "SELECT now() FROM system.local;"
# works

$ cqlsh node2 9042 -u ferrosa_admin -p ferrosa_admin -e "SELECT now() FROM system.local;"
# works

$ cqlsh node3 9042 -u ferrosa_admin -p ferrosa_admin -e "SELECT now() FROM system.local;"
Connection error: ('Unable to connect to any servers',
  {'10.89.1.57:9042': error('unpack requires a buffer of 2 bytes')})
```

The same error reproduces:

- Unauthenticated (`cqlsh node3 9042 -e "SELECT key FROM system.local;"`).
- With `--protocol-version=3`, `--protocol-version=4`, and
  `--protocol-version=5`.
- Via the host-mapped port (`cqlsh host.containers.internal 19044 ...`).
- On repeated attempts.

The `unpack requires a buffer of 2 bytes` message is raised inside
`cassandra-driver`'s frame decoder when a length-prefixed field claims
more bytes than arrived — typical cause is an options/SUPPORTED or
STARTUP response whose advertised length doesn't match the payload.

## Repro

1. Start the 3-node cluster with auth enabled.
2. From any host/pod with podman and network access to the cluster:
   ```
   podman run --rm --network ferrosa-memory_default docker.io/library/cassandra:4.1 \
     cqlsh node3 9042 -u ferrosa_admin -p ferrosa_admin -e "SELECT key FROM system.local;"
   ```
3. Repeat against `node1` and `node2` — those succeed.

## Next diagnostic step

Capture the server's first response on each node:

```
tshark -i any -f "tcp port 9042" -O cassandra -w /tmp/node1.pcap   # for each node
```

Compare the SUPPORTED / AUTHENTICATE frame from node3 against node1 and
look for a truncated length field, a duplicate key in the options map,
or an unexpected UTF-8 in a server identifier. Whatever `cdrs-tokio`
tolerates that the Python driver doesn't is the wire deviation to fix.

## Workaround

None needed for application traffic. For operator tasks:

- Issue cluster-wide DDL/GRANTs against `node1` or `node2` only.
- Use a `cdrs-tokio`-backed tool (e.g. a small Rust one-shot or
  `ferrosa-memory-batch`) instead of cqlsh when node3 is involved.

## Relation to the `system_auth` replication bug

This bug compounds the `system_auth = LocalStrategy` bug filed in
`bug-system-auth-uses-localstrategy.md`: because auth is not replicated,
operators must issue GRANTs against *every* node, but node3 can't be
reached by the reference CQL client.
