---
crate: ferrosa-net
status: implemented
last_updated: 2026-07-01
executive_summary: >
  The internode transport for ferrosa: a custom framed TCP wire protocol with a
  44-byte header, a PSK-HMAC handshake, three priority lanes (Raft/Data/Bulk) per
  peer, and a cancel-safe actor-based connection pool with backoff/dormancy
  reconnect. Higher layers register typed RPC handlers; this crate moves bytes,
  authenticates peers, multiplexes by priority, propagates trace context, and
  keeps connections alive across peer IP churn. Near-leaf: depends only on
  ferrosa-common (for TaskPool).
---

# ferrosa-net — Architecture Overview

## Purpose & boundary

`ferrosa-net` is the **node-to-node transport substrate**. Its boundary is the
wire: it knows how to frame, authenticate, prioritise, and deliver `Message`s
between peers, and how to keep those connections alive — and nothing about Raft
semantics, query planning, storage, or cluster membership policy. Those live in
`ferrosa-cluster` and above, which plug in via the `HandlerRegistry` /
`RpcHandler` seam.

It is deliberately a **near-leaf**: its only in-workspace dependency is
`ferrosa-common`, from which it re-exports `task_pool::TaskPool`. (An internal
`ARCHITECTURE.md` reference doc states it has *no* `ferrosa-common` dependency;
that is wrong — see `Cargo.toml` and `src/task_pool.rs`.)

## Module map

| Module | Responsibility |
|--------|----------------|
| `codec` | Frame model: `FrameHeader` (44-byte), `Frame`, `Lane`, `MsgType`, `TraceContext`, `InternodeCodec` (tokio-util `Encoder`/`Decoder`), `WireFrameFormat` |
| `message` | `Message` enum + hand-rolled length-prefixed encode/decode for all `MsgType`s |
| `protocol` | Generated Cap'n Proto envelope (v2) + adapter types, capability/feature negotiation, `encode/decode_message_envelope` |
| `accord_messages` | `AccordMessageType` discriminants (Accord payloads carried as opaque `Bytes`) |
| `handshake` | PSK-HMAC handshake (`initiate_handshake` / `accept_handshake`), `compute/verify_auth_token`, `HandshakePeer` |
| `pool` | `PriorityPool`: 3 lanes per peer, connect/send/fire/shutdown, reconnect-host selection |
| `lane_actor` | Per-lane actor task, `LaneHandle`, `LaneCommand`, stream-window dispatch, reconnect/dormant driving |
| `reconnect` | `LaneState`, backoff constants, `connect_with_retry_cancelable`, alive watcher, dormant counters |
| `rpc/server` | `RpcServer`: accept loop, acceptor handshake, dispatch, graceful drain, TLS acceptor |
| `rpc/client` | `RpcClient`: outbound connection, frame reader/writer loops, bandwidth metrics |
| `rpc/handler` | `HandlerRegistry` (`MsgType`→handler, dynamic), `RpcHandler` trait, `PingHandler` |
| `tls` | rustls (ring) `TlsAcceptor`/`TlsConnector` from PEM paths; `require_tls` enforcement |
| `stream_router` | Per-`request_id` dispatch for multi-message streaming RPCs; `is_registered` route-liveness predicate for callers' per-request state |
| `idle_timeout` | Producer-quiet watchdog for streaming consumers (heartbeats reset the deadline) |
| `peer` | Per-peer registry, liveness/heartbeat state |
| `skew` | Per-peer RTT + clock-skew tracking; feeds Accord `SkewMax` |
| `discovery` | `Discovery` trait + `SeedDiscovery` |
| `metrics` | Lane queue depth, in-flight RPCs, timeouts, dormant peers, bandwidth |
| `config` / `error` | `NetConfig` (env-driven), `NetError`, `bind_failure_diagnostic` |

## Wire frame layout

```
byte:  0      1      2      3      4..8        8..12      12..44
     ┌──────┬──────┬──────┬──────┬───────────┬──────────┬───────────────┐
     │ ver  │flags │ lane │ type │ stream_id │  length  │ trace_context │  body[length]
     └──────┴──────┴──────┴──────┴───────────┴──────────┴───────────────┘
       u8     u8     u8     u8      u32 BE      u32 BE       32 bytes
```

`HEADER_SIZE = 44`. `version` is `1` (Legacy) or `2` (Cap'n Proto envelope);
the codec is constructed per-connection with the negotiated `WireFrameFormat`
and rejects frames whose version does not match. `length` is bounded by
`max_frame_body_size` (default 256 MiB).

## Data flow (summary)

- **Connect**: `PriorityPool::connect` resolves the peer, builds a shared TLS
  connector, opens 3 `RpcClient`s (one per lane), each running its handshake,
  then spawns a `lane_actor` per lane. See [data-flow.md](data-flow.md).
- **Send**: a caller calls `pool.send(msg, lane)`; the `LaneHandle` reserves an
  mpsc slot (cancel-safe), the actor enforces the per-lane stream window and the
  process-wide Data-lane in-flight cap, then dispatches on the `RpcClient`.
- **Receive**: `RpcServer` accepts, runs `accept_handshake`, then reads frames
  and routes each through `HandlerRegistry::dispatch`; streaming responses are
  fanned out by `StreamRouter` keyed on `request_id`.

## Key invariants

1. **No `Mutex` held across network `await`.** Lane state is owned exclusively by
   a single actor task; callers interact only via `LaneHandle` (mpsc). This is
   the crate's central cancel-safety guarantee (`lane_actor`).
2. **Frame version matches the negotiated format.** `InternodeCodec::decode`
   rejects a legacy frame on a Cap'n Proto connection and vice versa, rather than
   misparsing.
3. **`MsgType` byte tags must round-trip.** Every serialised discriminant must
   decode via `TryFrom<u8>`; a missing arm makes peers silently drop frames
   (regression-tested for the repair RPC tags `0x29..=0x2E`).
4. **Ordered streaming responses dispatch in wire order.** Chunk/heartbeat/done
   frames for one stream are processed in order; out-of-order dispatch trips the
   coordinator's contiguous-`seq` check (`MsgType::is_ordered_stream_response`).
5. **Reconnect targets a re-resolvable hostname, not a frozen IP.** Lanes prefer
   the peer's advertised internode-broadcast hostname so a peer that restarts on
   a new container IP is reconnected (P3 fix in `pool::pick_reconnect_host`).
6. **Fail loud on bad auth / require_tls.** PSK mismatch rejects the handshake;
   `require_tls` with no cert errors at startup rather than silently running
   plaintext.

## Position in the dependency graph

Near-leaf. **Calls:** `ferrosa-common` (only). **Called by:** `ferrosa`,
`ferrosa-cluster`, `ferrosa-cql`, `ferrosa-graph`, `ferrosa-session`. See the
[root crate index](../../specs/crates.md) for the full graph.
