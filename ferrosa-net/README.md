# ferrosa-net

> The internode transport: custom framed TCP wire protocol, PSK-HMAC handshake,
> three priority lanes per peer, and a cancel-safe actor-based connection pool.

## What this crate is

`ferrosa-net` owns the **wire protocol and peer lifecycle** for ferrosa's
node-to-node communication. It is *not* Cassandra wire-compatible — it is a
purpose-built internode protocol. Higher layers (`ferrosa-cluster`,
`ferrosa-cql`, `ferrosa-graph`, `ferrosa-session`, and the `ferrosa` binary)
register typed message handlers and send messages; this crate moves the bytes,
authenticates peers, multiplexes by priority, and keeps connections alive across
peer restarts.

It is a near-leaf in the dependency graph: it depends only on `ferrosa-common`
(for the shared `TaskPool`), not on any cluster/storage/query crate.

## What's implemented

- **Framed wire protocol** (`codec`) — a fixed-size frame header
  (`HEADER_SIZE = 44` bytes: `version(1) + flags(1) + lane(1) + msg_type(1) +
  stream_id(4) + length(4) + trace_context(32)`) followed by a length-prefixed
  body. `InternodeCodec` implements tokio-util `Encoder`/`Decoder`; it rejects
  oversized frames (`FrameTooLarge`), unknown lanes/message types, and
  version-mismatched frames.
- **Distributed trace propagation** — every frame carries a 32-byte
  `TraceContext` (`trace_id(16) + span_id(8) + flags(8)`); all-zero means no
  active trace.
- **Two frame formats** — `WireFrameFormat::Legacy` (v1) and
  `CapnpEnvelope` (v2). The Cap'n Proto envelope (`protocol`) carries typed
  cluster-control, recovery, bootstrap, and stream payloads plus a versioned
  capability/feature negotiation; legacy message bodies ride as an append-only
  `LegacyPayload` until peers negotiate the Cap'n Proto format.
- **Message model** (`message`) — the `Message` enum with hand-rolled
  length-prefixed encode/decode for lifecycle, Raft, mutation/read, repair,
  streaming, pair-mode, batchlog, index (incl. full-text scatter-gather), Accord
  (incl. the additive multi-key `AccordPreAcceptV2` `0x7B` / `AccordApplyV2` `0x7C`
  codes — bincode is not self-describing, so multi-key transactions get new codes
  rather than extending the single-key payloads), and bootstrap message types
  (`MsgType` discriminants `0x01`..=`0x83`). Optional trailing fields decode to
  `None` on pre-extension peers for backward compatibility.
- **PSK-HMAC handshake** (`handshake`) — `initiate_handshake` /
  `accept_handshake` exchange `Handshake`/`HandshakeAck`, verifying cluster name,
  protocol version, and an `HMAC-SHA256(psk, cluster_name|host_id|nonce)` auth
  token via the `hmac` crate's constant-time `verify_slice`. The handshake also
  exchanges CQL- and internode-broadcast addresses.
- **Priority lanes + actor pool** (`pool`, `lane_actor`) — `PriorityPool` holds
  three TCP connections per peer, one per `Lane` (`Raft`, `Data`, `Bulk`). Each
  lane is owned by a dedicated actor task that processes `LaneCommand`s
  sequentially over an mpsc channel — eliminating the cancel-safety hazard of
  holding a `tokio::Mutex` across network `await`s. The Raft lane can run on its
  own OS thread/runtime so heartbeats are never starved by data-path saturation.
- **Reconnect / dormancy lifecycle** (`reconnect`, `lane_actor`) — on disconnect
  a lane retries with exponential backoff (`connect_with_retry_cancelable`); after
  `MAX_RECONNECT_ATTEMPTS` it counts an exhaustion, and after
  `DORMANT_AFTER_EXHAUSTIONS` it goes `Dormant`, probing once per
  `DORMANT_PROBE_INTERVAL`. Reconnects re-resolve the peer's advertised hostname
  so container IP churn is handled automatically.
- **RPC server + handler registry** (`rpc`) — `RpcServer` accepts inbound
  connections, runs the acceptor handshake, and dispatches frames through a
  thread-safe `HandlerRegistry` (`MsgType` → `Arc<dyn RpcHandler>`) that supports
  dynamic registration after start. Graceful drain via `CancellationToken` with a
  bounded wait.
- **TLS** (`tls`) — optional rustls (ring provider) `TlsAcceptor`/`TlsConnector`
  built from PEM cert/key/CA paths; `require_tls` fails startup loudly when no
  cert is configured.
- **Streaming support** (`stream_router`, `idle_timeout`) — `StreamRouter`
  dispatches multi-message streaming RPCs keyed by `request_id`; the idle-timeout
  watchdog aborts a consumer only after the producer is quiet for longer than the
  timeout (heartbeats reset the deadline). `is_registered(request_id)` exposes
  route liveness as the lifecycle predicate for callers' per-request companion
  state (ferrosa-cluster's stream seq tracking keys create/drop off it: ids are
  monotonic and never reused, and a route is always registered before its
  request fires, so "no route" is terminal for that id).
- **Failure detection + skew** (`peer`, `skew`) — per-peer RTT and clock-skew
  tracking derived from heartbeats; the Accord protocol consumes `SkewMax`.
- **Discovery** (`discovery`) — `SeedDiscovery` over a `Discovery` trait.
- **Metrics** (`metrics`) — lane queue depth, in-flight RPCs, timeouts, dormant
  peer counts, bandwidth.

## Public API (key entry points)

| Area | Types / functions |
|------|-------------------|
| Framing | `InternodeCodec`, `Frame`, `FrameHeader`, `Lane`, `MsgType`, `TraceContext`, `WireFrameFormat`, `HEADER_SIZE` |
| Messages | `Message`, `accord_messages::AccordMessageType` |
| Cap'n Proto envelope | `CapnpEnvelope`, `encode_message_envelope`, `decode_message_envelope`, `negotiate_capnp_capabilities` |
| Handshake | `initiate_handshake`, `accept_handshake`, `compute_auth_token`, `verify_auth_token`, `HandshakePeer` |
| Pool / lanes | `PriorityPool`, `LaneHandle`, `LaneOutcome`, `LaneStatusReport`, `spawn_lane_actor` |
| RPC | `RpcServer`, `RpcClient`, `HandlerRegistry`, `RpcHandler`, `PeerId`, `InboundPeerCallback` |
| Config / errors | `NetConfig`, `NetError`, `bind_failure_diagnostic` |
| TLS | `tls::build_tls_acceptor`, `tls::build_tls_connector` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — re-exports `ferrosa_common::task_pool::TaskPool` as the
  crate's `TaskPool` (`src/task_pool.rs`). This is the *only* in-workspace
  dependency.

External: `tokio`, `tokio-util`, `bytes`, `capnp`/`capnpc`, `rustls` +
`tokio-rustls` + `rustls-pemfile`, `hmac` + `sha2`, `dashmap`, `parking_lot`,
`arc-swap`, `futures`, `uuid`, `rand`, `lz4_flex`, `snap`, `tracing`.

**Called by** (crates that depend on this):

- **`ferrosa`** — runs the `RpcServer`, builds `NetConfig`, wires runtimes.
- **`ferrosa-cluster`** — registers Raft/repair/Accord handlers; reacts to peer
  events; uses `PriorityPool` for outbound RPC.
- **`ferrosa-cql`**, **`ferrosa-graph`**, **`ferrosa-session`** — send/receive
  internode messages via the pool and handler registry.

> Note: an internal `ARCHITECTURE.md` reference doc claimed `ferrosa-net` has **no**
> `ferrosa-common` dependency. That is **incorrect** — `Cargo.toml` and
> `src/task_pool.rs` show a real dependency on `ferrosa_common::task_pool`. The
> truth is documented here; the reference doc should be corrected.

## Tests

~138 in-crate unit tests across the modules, plus ~27 integration tests in
`tests/` (Cap'n Proto adapters/conformance/envelope framing/protocol,
end-to-end `integration.rs`, and `reconnect_backoff.rs`). No `#[ignore]`, no
`TODO`/`FIXME`/`unimplemented!` in `src/`.

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, position
- [Data flow](specs/data-flow.md) — frame / handshake / lane sequence diagram
- [FMEA / known issues](specs/fmea.md) — failure modes + gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
