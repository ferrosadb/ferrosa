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
  streaming, pair-mode, batchlog, index (incl. full-text scatter-gather and
  the keyed `IndexReadInPartition{Request,Response}` `0x66`/`0x67` pair,
  t_430c4188, and the streaming fulltext family
  `FulltextSearchStream{Request,Chunk,Heartbeat,Done,Cancel}` `0x3B`..=`0x3F`,
  t_4ae47a9f — the `fts_match` twin of the ADR-020 range-read stream whose
  Chunk/Heartbeat/Done frames are `is_ordered_stream_response`), Accord
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
  a lane immediately enters `Reconnecting` so new work is not dispatched to the
  dead RPC client, then retries with exponential backoff
  (`connect_with_retry_cancelable`, the fast phase); after
  `reconnect_fast_attempts()` (default `MAX_RECONNECT_ATTEMPTS` = 10) it counts
  an exhaustion, and after `DORMANT_AFTER_EXHAUSTIONS` it drops into the
  indefinite slow-retry phase (`Dormant`): one single-attempt probe per
  `slow_retry_interval()` (default 30 s, plus up to 25% jitter) until the peer
  returns or the lane is shut down, for an outage of any length. Both bounds are
  env-tunable (`FERROSA_NET_RECONNECT_FAST_ATTEMPTS`,
  `FERROSA_NET_RECONNECT_SLOW_INTERVAL_MS`; see `PROFILE.md`). Logging is
  edge-only: one line on losing the connection, one on entering slow-retry, one
  on recovery; per-attempt detail is DEBUG and counted in
  `total_reconnect_attempts()`. A lane remembers the node it was opened to and
  refuses a reconnect answered by a different host id (logged once per episode).
  The client records its death with `send_replace`, and the alive watcher checks
  the current value before waiting, so a connection that dies before the lane
  subscribes is not missed. Reconnects re-resolve the peer's advertised hostname
  so container IP churn is handled automatically. `NetError::LaneShutdown`
  means a pool's actors have exited (peer connection replaced); it is not
  reconnect exhaustion. A listener that owns its `PeerManager` (the cluster
  `ModeController`) is passed through `PeerManager::with_weak_listener` and
  held weakly; `PeerManager::new` holds its listener strongly, for listeners
  nothing else owns. Events for a dropped owner are logged and discarded.
  `PeerManager` re-issues a request once on the current
  pool when the one it resolved was replaced mid-request. If the registered
  pool itself is dead, `PeerManager` deregisters it, re-dials once via
  `ensure_peer` (identity-checked), logs the failure and the recovery once
  each, and re-issues the request; if the dial fails the request errors and a
  pool-less placeholder keeps the peer's address, which the heartbeat loop
  re-dials (identity-checked, exponential backoff capped at the slow-retry
  interval, jittered) until the peer answers or is removed. `remove_peer` shuts
  the peer's pool down so its lane-actor tasks exit, and a re-dial that
  completes after removal is refused (`install_pool` guard).
- **RPC server + handler registry** (`rpc`) — `RpcServer` accepts inbound
  connections, runs the acceptor handshake, and dispatches frames through a
  thread-safe `HandlerRegistry` (`MsgType` → `Arc<dyn RpcHandler>`) that supports
  dynamic registration after start. Graceful drain via `CancellationToken` with a
  bounded wait.
- **TLS** (`tls`) — optional rustls `TlsAcceptor`/`TlsConnector` built from PEM
  cert/key/CA paths; `require_tls` fails startup loudly when no cert is
  configured (acceptor AND connector). This module is the process's single
  crypto-provider decision (`crypto_provider()`, currently `ring`): internode,
  CQL, PostgreSQL, Bolt, graph HTTP, SPARQL, the web console and Arrow Flight
  (ALPN `GRPC_ALPN` = `h2`) build their
  server config through `server_config_from_pem` / `optional_server_config`, so a
  provider swap (e.g. FIPS) is a one-line change here.
- **Streaming support** (`stream_router`, `idle_timeout`) — `StreamRouter`
  dispatches multi-message streaming RPCs keyed by `request_id`; the idle-timeout
  watchdog aborts a consumer only after the producer is quiet for longer than the
  timeout (heartbeats reset the deadline). `is_registered(request_id)` exposes
  route liveness as the lifecycle predicate for callers' per-request companion
  state (ferrosa-cluster's stream seq tracking keys create/drop off it: ids are
  monotonic and never reused, and a route is always registered before its
  request fires, so "no route" is terminal for that id). Data frames route
  fail-loud (`route` closes the route on a full buffer so a dropped chunk can
  never become a silent partial result); advisory frames route lossily
  (`route_lossy` drops the frame on a full buffer and KEEPS the route —
  heartbeats must never kill a healthy mid-window stream, t_a0f922a3).
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
| Config / errors | `NetConfig` (`from_lookup` → config + `ConfigIssue`s, `from_env`, `from_env_checked`), `NetError`, `bind_failure_diagnostic` |

`NetConfig` no longer drops a bad `FERROSA_*` value silently. Every rejected value is
a `ConfigIssue`: a typo (unparseable bind, non-numeric or zero timeout, a
non-boolean `FERROSA_INTERNODE_REQUIRE_TLS` — which used to read as `false` and
silently disable the TLS requirement) is **fatal** under `from_env_checked`, which
`ferrosa` uses at startup; a seed or broadcast hostname that does not resolve yet is
logged at WARN and startup continues (the binary retries seeds by name). An empty
value counts as unset.
| TLS | `tls::build_tls_acceptor`, `tls::build_tls_connector`, `tls::crypto_provider`, `tls::server_config_from_pem`, `tls::optional_server_config`, `tls::HTTP_ALPN`, `tls::GRPC_ALPN` |

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

~157 in-crate unit tests across the modules, plus ~27 integration tests in
`tests/` (Cap'n Proto adapters/conformance/envelope framing/protocol,
end-to-end `integration.rs`, and `reconnect_backoff.rs`). No `#[ignore]`, no
`TODO`/`FIXME`/`unimplemented!` in `src/`.

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, position
- [Data flow](specs/data-flow.md) — frame / handshake / lane sequence diagram
- [FMEA / known issues](specs/fmea.md) — failure modes + gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
