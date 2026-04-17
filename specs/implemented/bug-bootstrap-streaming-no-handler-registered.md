---
type: bug
priority: P1
reported-by: ferrosa-memory launch deployment
implemented-by: ""
verified-by: ""
created: 2026-04-16
updated: 2026-04-16
---

# Bootstrap streaming fails: `no handler registered msg_type=StreamStart`

## Observed

After a fresh cluster bootstrap (ferrosa-memory 3-node podman cluster following
the empty-membership workaround), the elected leader (node1) attempts to
bootstrap-stream existing table data to node3 and every stream attempt times
out on the Bulk lane. Receiver logs:

```
WARN ferrosa_net::rpc::handler: no handler registered msg_type=StreamStart
```

Sender (node1, the leader) logs:

```
WARN ferrosa_cluster::controller::cluster: bootstrap streaming failed for
    agent_memory.typed_edges  e=net: timeout: Bulk lane timeout
    target=3689348814741910323
WARN ferrosa_cluster::controller::cluster: bootstrap streaming failed for
    agent_memory.temporal_events  e=net: timeout: Bulk lane timeout
    target=3689348814741910323
```

Downstream effect: node1's CQL port (19042) becomes unresponsive (every query
times out), DDLs forwarded to the leader time out, and any client trying to
apply schema migrations through node1 fails. The ferrosa-memory MCP binary
cannot complete its `schema_version` table setup:

```
Error: schema migration failed, aborting startup: schema_version table setup
failed: Server 127.0.0.1:19043 error: ErrorBody { message: "server error:
cluster error: net: timeout: Data lane timeout", ty: Server }
```

CQL reads/writes against node2 and node3 (SSTable path, not DDL) still work.

## Root Cause

`ferrosa-cluster/src/controller/cluster.rs:370-409` registers RPC handlers at
cluster init for Raft messages (`RaftAppendEntries`, `RaftVote`,
`RaftInstallSnapshot`), repair (`RepairWrite`), reads (`ReadRequest`,
`RangeReadRequest`, `IndexReadRequest`), and (later) `ClusterInvite` /
`PairDdlForward`. It does **not** register a handler for any of the streaming
message types:

- `MsgType::StreamStart`
- `MsgType::StreamChunk`
- `MsgType::StreamEnd`
- `MsgType::SstableStreamStart`
- `MsgType::SstableStreamChunk`
- `MsgType::SstableStreamEnd`

`ferrosa-cluster/src/streaming/receiver.rs` *defines* the receiver logic
(`StreamReceiver::begin`, `SstableStreamSession`, etc.), but nothing in the
cluster controller startup path wires it into the RPC registry.
`StreamSender::send_stream` (called from `cluster.rs:931`) happily emits
`StreamStart` onto the wire, and the receiver returns "no handler registered"
for every frame, causing the Bulk lane to time out.

This is a cluster-formation regression: whatever wiring used to register the
streaming handlers (probably during an earlier refactor into the
`streaming/` module) was dropped.

Grep confirming the gap:

```
$ rg 'MsgType::(StreamStart|StreamChunk|StreamEnd|SstableStreamStart)' \
    ferrosa-net ferrosa-cluster ferrosa
# Only matches in message.rs / codec.rs / accord_messages.rs — never in a
# registry.register() call anywhere in the tree.
```

## Repro

1. Start a fresh 3-node ferrosa-memory cluster with existing SSTable data on
   one node (or wipe `raft/` directories to force a re-bootstrap with data).
2. Observe on the seed/leader: `bootstrap streaming failed for <ks>.<tbl>
   e=net: timeout: Bulk lane timeout`.
3. Observe on the joining node: `no handler registered msg_type=StreamStart`.
4. CQL DDL against the leader hangs; every schema-migration client fails to
   start.

## Expected

The cluster controller should register receivers for all streaming message
types before (or as part of) enabling bootstrap streaming. Bootstrap streaming
should complete within the streaming-latency budget and not wedge the CQL /
Bulk lane on the sender.

## Proposed Fix Direction

In `ferrosa-cluster/src/controller/cluster.rs` around lines 376–409, alongside
the Raft handler registration block, add:

```rust
let stream_receiver_handler = Arc::new(StreamReceiverHandler::new(/* ... */));
self.registry
    .register(MsgType::StreamStart, stream_receiver_handler.clone());
self.registry
    .register(MsgType::StreamChunk, stream_receiver_handler.clone());
self.registry
    .register(MsgType::StreamEnd, stream_receiver_handler.clone());

let sstable_stream_handler = Arc::new(SstableStreamReceiverHandler::new(/* ... */));
self.registry
    .register(MsgType::SstableStreamStart, sstable_stream_handler.clone());
self.registry
    .register(MsgType::SstableStreamChunk, sstable_stream_handler.clone());
self.registry
    .register(MsgType::SstableStreamEnd, sstable_stream_handler.clone());
```

(The exact handler names may differ; this is the wiring shape. If no
`StreamReceiverHandler` / `SstableStreamReceiverHandler` exists today, create
one that bridges incoming frames into `StreamReceiver::begin` /
`SstableStreamSession::begin` per session id.)

Also register these handlers on the cluster's **data runtime** — `runtime.rs`
comment says "Internode data path: read/write forwarding, bootstrap streaming,
repair" — so the handler registration should go on the data runtime registry,
not the raft runtime registry, to match the separation the comment documents.

## Fail-Loud Improvement (Nice to Have)

`no handler registered` is currently a WARN. Upgrade to ERROR (or at least
log once-with-context) when a request from a trusted internode peer hits an
unregistered handler — silently WARN-ing per message means this bug went
undetected.

## Acceptance Criteria

- [ ] Grep finds `register(MsgType::StreamStart, ...)` (and the other five
      streaming msg types) inside `ferrosa-cluster/src/controller/cluster.rs`
      (or wherever cluster-mode handlers are wired).
- [ ] Bootstrap-streaming test: 3-node cluster with pre-populated data on
      node1, wipe raft/ on all nodes, restart — node3 receives
      `bootstrap streaming complete` without Bulk lane timeouts.
- [ ] No `no handler registered msg_type=Stream*` log lines on any node after
      normal cluster formation.
- [ ] Node1 CQL/19042 remains responsive while streaming is in progress
      (sender should not block the CQL listener).

## Implementation Notes

Created `ferrosa-cluster/src/streaming/handler.rs` with two handler structs:

- **`StreamHandler`** — handles `StreamStart`, `StreamChunk`, `StreamEnd`. Manages in-flight sessions in a `DashMap<u64, StreamSession>` keyed by `session_id`. On `StreamStart`, creates a session via `StreamReceiver::begin()`. On `StreamChunk`, accumulates mutations. On `StreamEnd`, validates checksum and applies all mutations to storage via `StreamSession::finish()`.

- **`SstableStreamHandler`** — handles `SstableStreamStart`, `SstableStreamChunk`, `SstableStreamEnd`. Same session-store pattern with `DashMap<u64, SstableStreamSession>`. Writes SSTable component files to `{data_dir}/sstables/{ks}.{tbl}/{id}/`.

Both handlers are registered in `controller/cluster.rs` alongside the existing Raft/repair/read handlers, before the init task is spawned. Each handler is registered for all three message types of its protocol (using `Arc::clone`).

Files changed:
- `ferrosa-cluster/src/streaming/handler.rs` — new file
- `ferrosa-cluster/src/streaming/mod.rs` — added `pub mod handler` + re-exports
- `ferrosa-cluster/src/controller/cluster.rs` — 6 handler registrations
- `ferrosa-cluster/Cargo.toml` — added `dashmap = "6"`

## Related

- `specs/implemented/bug-raft-startup-fails-after-oom-purged-log.md` — fixes
  the pre-condition crash that hid this bug.
- `specs/todo/bug-raft-empty-membership-after-recovery.md` — membership
  wipe-on-restart exposed this bug by forcing a bootstrap-streaming cycle.
- `specs/hazards-cluster-formation.md` — existing catalogue of cluster
  formation hazards; this one is not yet listed.
