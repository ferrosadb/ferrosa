# ferrosa-flight

> Apache Arrow Flight (gRPC) query endpoint for Ferrosa. A client puts a CQL
> `SELECT` in the Flight ticket; `ferrosa-cql` executes it and the result is
> streamed back as Arrow record batches.

## What this crate is

`ferrosa-flight` exposes Ferrosa's data over the [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
gRPC protocol, so Arrow-native clients (pandas/Polars/DuckDB/Spark via
`arrow-flight`) can read and write columnar data without going through the CQL
native wire. The command carried by a Flight `Ticket`/`FlightDescriptor` is a
CQL string: a `SELECT` for reads, executed by `ferrosa_cql::router::route_select_raw`
and converted column-by-column to an Arrow [`RecordBatch`](src/convert.rs).

Transport is gRPC over **TLS** when a certificate is configured
(t_58db6320): `server::serve_service` terminates TLS with `tokio-rustls` on a
`rustls::ServerConfig` built by `ferrosa_net::tls` (the one crypto provider
every Ferrosa listener uses), ALPN `h2`, and hands finished handshakes to tonic
via `serve_with_incoming`. tonic's own `tls` feature is not enabled. With a
certificate the port speaks TLS only; a plaintext client's handshake fails and
the connection is closed (WARN with the peer address).

Authentication is **bearer-token, never anonymous** (decision **D4**):
`Handshake` validates CQL credentials and issues an HMAC-SHA256-signed token;
every other RPC requires `authorization: Bearer <token>` and derives its
`AuthContext` from the verified claims.

## What's implemented

- **`Handshake`** — validates `username\0password` via `Schema::authenticate`,
  returns a signed bearer token (`token.rs`, HMAC-SHA256, absolute expiry, key
  rotation overlap).
- **`DoGet`** — streams a CQL `SELECT` result **one page at a time** (1024 rows
  by default) via `futures::stream::unfold` over the CQL paging cursor, so peak
  memory is bounded to a single page. Non-`SELECT` tickets are rejected up front.
- **`GetFlightInfo`** — returns the Arrow schema plus Flight endpoints. On a
  cluster topology with a known table it emits **one endpoint per ring token
  range** (token-bounded `SELECT` ticket located on the owning replicas), so a
  client can read ranges in parallel (`plan.rs`, W-002). Standalone / unknown
  table falls back to a single self-endpoint.
- **`GetSchema`** — returns just the Arrow schema (IPC-encoded) for a command.
- **`DoPut`** — decodes inbound Arrow batches and writes each row into the
  `ferrosa-table` (`keyspace.table` metadata header) as a generated CQL `INSERT`
  routed through the normal write path; returns the rows-applied count. A value
  with no CQL literal (non-finite float, unsupported type) fails the put with
  `InvalidArgument` naming the column; each batch is rendered in full before any
  of its rows is written, so no row lands without a column (FL-T016).
- **`DoExchange`** — bidirectional **upsert with per-batch ack**: consumes the
  same inbound batches as `DoPut` and emits one `FlightData` ack (rows-applied
  count in `app_metadata`) per batch, strictly ordered. (This is an upsert
  channel, **not** a live CDC subscribe — see [FMEA](specs/fmea.md).)
- **`ListFlights`** — enumerates queryable (non-system) tables as `FlightInfo`,
  each with a `SELECT * FROM ks.t` ticket; optional `Criteria` prefix filter.
- **`PollFlightInfo`** — resolves a descriptor to its `FlightInfo` and reports
  it complete (`progress = 1.0`); synchronous (no long-running async poll yet).
- **`DoAction` / `ListActions`** — `server.info` (name + crate version) and
  `token.validate` (echo verified identity). The two stay in lock-step via
  `SUPPORTED_ACTIONS`.

Every RPC is implemented — there are **no `Unimplemented` stubs**.

## How it works

| Module | Responsibility |
|--------|----------------|
| `service` (`src/service.rs`) | `FerrosaFlight` — the `FlightService` impl: all RPCs, auth, `query_to_batch`, write-path `build_insert`/`write_batch`, endpoint assembly |
| `convert` (`src/convert.rs`) | CQL result ↔ Arrow: `rows_to_record_batch` (full CQL type coverage) and `record_batch_to_rows` (scalar Arrow → CQL, fail-loud on the rest) |
| `plan` (`src/plan.rs`) | Pure distributed-read planner: ring token ranges → token-bounded `SELECT` endpoints with replica locations (W-002) |
| `token` (`src/token.rs`) | HMAC-SHA256 signed bearer tokens: `issue` / `verify` / `verify_with_keys` (key rotation) |
| `server` (`src/server.rs`) | gRPC bootstrap: `flight_service` (mount on a `tonic` server), `FlightTlsConfig` (cert/key/require_tls → rustls config via `ferrosa_net::tls`, ALPN h2), `serve_service` / `serve_on_listener` (TLS-only when configured, plaintext otherwise), `serve` (plaintext convenience) |

## Public API (key entry points)

| Area | Items |
|------|-------|
| Service | `FerrosaFlight::new`, `.with_flight_advertise`, `.with_flight_port`, `.with_tls_locations`, `.with_previous_keys`, `.with_token_ttl`, `.with_page_size` |
| Server | `server::flight_service`, `server::FlightTlsConfig`, `server::serve_service`, `server::serve_on_listener`, `server::serve`, `server::ServeError`, `server::TlsConnection` |
| Convert | `convert::rows_to_record_batch`, `convert::record_batch_to_rows`, `convert::cql_type_to_arrow`, `convert::ConvertError` |
| Plan | `plan::distributed_endpoints`, `plan::ring_token_ranges`, `plan::token_bounded_select`, `plan::EndpointPlan` |
| Token | `token::issue`, `token::verify`, `token::verify_with_keys`, `token::Claims`, `token::TokenError` |

## Configuration

| Env var | Effect |
|---------|--------|
| `FERROSA_FLIGHT_BROADCAST` | This node's externally-reachable Flight address advertised for ranges it owns. Unset → self-owned ranges advertise **no** location (client falls back to the queried connection) rather than faking an address. |
| `FERROSA_FLIGHT_PORT` | Flight gRPC port combined with a remote replica's internode host to build its advertised location. Library default `service::DEFAULT_FLIGHT_PORT` = `8815`, the binary's default bind port (it was `50051`, which nothing listened on). The `ferrosa` binary advertises its own `[flight] bind` port unless this is set, and a malformed value stops startup. The scheme is `grpc+tls://` when the service is built `.with_tls_locations(true)` (the binary does this when the Flight listener has a certificate, its own or the node-wide `[tls]` one), else `grpc://`. |

TLS keys are read by the `ferrosa` binary — the node-wide `[tls] cert/key/
require` (env `FERROSA_TLS_*`), overridden by `[flight] tls_cert` / `tls_key` /
`require_tls` (env `FERROSA_FLIGHT_TLS_CERT` / `_TLS_KEY` / `_REQUIRE_TLS`) —
and passed in as a `FlightTlsConfig`. `require_tls` without a certificate, or only
one of cert/key, is an error naming "Arrow Flight", never a plaintext fallback.

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-cql`** — `router::route_select_raw` (read), `router::route` (write),
  the parser/AST, paging params, `SharedState`/`RequestContext`.
- **`ferrosa-cluster`** — `ConsistencyLevel`, the `TokenRing` and
  `ReplicationStrategy` used to plan per-range endpoints, `uuid_to_node_id`.
- **`ferrosa-schema`** — `AuthContext`, `Schema::authenticate`, `is_system_keyspace`,
  the schema snapshot used to enumerate tables and partition keys.
- **`ferrosa-common`** — `CqlValue` / `CqlType` (the value model converted to Arrow).
- **`ferrosa-net`** — `tls::optional_server_config` / `GRPC_ALPN` (the shared
  TLS builder and crypto provider).

External: `arrow` + `arrow-flight` (53), `tonic` (0.12, no `tls` feature),
`rustls` / `tokio-rustls` (TLS termination), `hmac`/`sha2`/`hex` (tokens),
`futures`, `tokio`, `tracing`.

**Called by** (crates that depend on this):

- **`ferrosa`** — the main binary mounts `flight_service` / calls `serve` to expose the endpoint.

## Tests

55 tests total — 25 unit (`convert` 11, `token` 7, `plan` 7) + 30 integration:

- `tests/read_path.rs` (4) — `DoGet` streams a `SELECT` result as Arrow, paging across multiple pages, bearer required, non-`SELECT` rejected.
- `tests/grpc_handshake.rs` (2) — full gRPC `Handshake` → `DoGet`; `DoPut` write then `DoGet` read-back over a real `tonic` channel.
- `tests/exchange_path.rs` (2) — `DoExchange` upserts each batch and acks; requires a valid bearer.
- `tests/minor_rpcs.rs` (11) — `ListFlights`, `PollFlightInfo`, `ListActions`, `DoAction` (`server.info` / `token.validate` / unknown / bearer).
- `tests/distributed_endpoints.rs` (3) — standalone single endpoint; multi-range topology one endpoint per range; `grpc+tls://` locations when serving TLS, default advertised port 8815, `with_flight_port` override.
End to end, `scripts/flight-tls-probe.sh <host:port> <ca>` (curl + python3)
checks a running node: plaintext gRPC refused, unauthenticated ListFlights
answered UNAUTHENTICATED over TLS, Handshake issues a token, authenticated
ListFlights succeeds. CI's secure-compose job runs it against a
`flight`-feature 3-node production cluster.

- `tests/tls_listener.rs` (8) — a TLS client (ALPN h2, same provider) completes `Handshake` + `ListFlights`; a plaintext gRPC client gets no RPC through and the server keeps serving TLS; an untrusted certificate is rejected; `require_tls` without a certificate / half a certificate is an error; ALPN is `h2` only; a missing certificate file stops `serve_service` before binding.

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, data flow
- [FMEA / known issues](specs/fmea.md) — failure modes + real gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
- [Data flow](specs/data-flow.md) — Handshake → token → DoGet → CQL exec → Arrow stream
