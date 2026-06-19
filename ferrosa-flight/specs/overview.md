---
crate: ferrosa-flight
status: implemented
last_updated: 2026-06-19
executive_summary: >
  Apache Arrow Flight (gRPC) query endpoint for Ferrosa. A client carries a CQL
  SELECT in the Flight ticket; ferrosa-cql executes it (route_select_raw) and the
  result is streamed back as Arrow record batches, paged so peak memory is bounded
  to one page. Bearer-token auth (decision D4): Handshake validates CQL credentials
  and issues an HMAC-SHA256 token; every other RPC requires it. DoPut/DoExchange
  write Arrow rows back as CQL INSERTs. GetFlightInfo plans one endpoint per ring
  token range for parallel distributed reads (W-002). All Flight RPCs are
  implemented — there are no Unimplemented stubs.
---

# ferrosa-flight — Architecture Overview

## Purpose & boundary

`ferrosa-flight` is the Arrow Flight adapter over Ferrosa's CQL execution layer.
Its boundary is deliberately thin: it owns gRPC framing, bearer-token auth, the
CQL↔Arrow conversion, and distributed-read endpoint planning — and **nothing
else**. Query execution, key/token derivation, consistency, and replication all
belong to `ferrosa-cql`'s router and the layers beneath it; this crate calls
`route_select_raw` (read) and `route` (write) and never reaches into storage
directly.

The command transported by a Flight `Ticket` / `FlightDescriptor.cmd` is a CQL
string. Reads require a `SELECT`; writes (`DoPut` / `DoExchange`) carry Arrow
batches plus a `ferrosa-table` (`keyspace.table`) metadata header and are
materialized as generated CQL `INSERT`s.

## Module map

| Module | Responsibility |
|--------|----------------|
| `service` (`src/service.rs`, ~880 LoC) | `FerrosaFlight`: the full `FlightService` impl, per-RPC auth, `query_to_batch`, write-path `build_insert` / `write_batch`, endpoint assembly |
| `convert` (`src/convert.rs`, ~980 LoC) | `rows_to_record_batch` (CQL result → Arrow, full type coverage incl. list/set/map/tuple/udt/vector/decimal) and `record_batch_to_rows` (Arrow → CQL, scalar subset, fail-loud) |
| `plan` (`src/plan.rs`, ~320 LoC) | Pure distributed-read planner: ring token ranges → token-bounded `SELECT` endpoints + replica Flight locations (W-002) |
| `token` (`src/token.rs`, ~230 LoC) | HMAC-SHA256 signed bearer tokens: `issue` / `verify` / `verify_with_keys` (rotation overlap) |
| `server` (`src/server.rs`) | gRPC bootstrap: wrap in `FlightServiceServer`, bind-and-serve |
| `lib` (`src/lib.rs`) | Module re-exports only |

## Data flow

**Auth (every session).** `Handshake` reads a `username\0password` payload,
calls `Schema::authenticate`, and returns an HMAC-SHA256 token whose payload is
`{expires_at}:{is_superuser}:{role}`. Every subsequent RPC calls
`authenticate(metadata)`, which strips `Bearer `, runs `verify_with_keys`
(constant-time signature check, then expiry), and builds the `AuthContext` from
the verified claims. There is no anonymous path.

**Read (`DoGet`).** Ticket bytes → UTF-8 CQL → parse, require `SELECT` →
`stream::unfold` over a paging cursor: each step calls `route_select_raw` with
`page_size = 1024` and the prior `paging_state`, converts the page's
`column_types` + `Vec<Vec<Option<CqlValue>>>` to a `RecordBatch` via
`rows_to_record_batch`, yields it, and advances the cursor until the CQL layer
reports no continuation. The batches feed a `FlightDataEncoderBuilder` so the
schema is taken from the first (always-emitted) batch. Peak memory is one page.

**Discovery (`GetFlightInfo` / `GetSchema` / `ListFlights`).** These call
`query_to_batch`, which executes the `SELECT` with `page_size = 1` purely to
derive the Arrow schema. `GetFlightInfo` then asks `plan::distributed_endpoints`
for one endpoint per ring token range (each ticket a `token(pk) > s AND
token(pk) <= e` bounded `SELECT`, located on the owning replicas); a single
whole-ring plan collapses to the standalone single self-endpoint.

**Write (`DoPut` / `DoExchange`).** Inbound `FlightData` → `RecordBatch`es →
`record_batch_to_rows` → for each row, `build_insert` emits
`INSERT INTO ks.t (...) VALUES (...)` (column identifiers validated, string/blob
literals escaped) → re-parsed and routed via `ferrosa_cql::router::route`.
`DoPut` returns one `PutResult` with the total count; `DoExchange` emits one ack
per batch, strictly ordered, stopping on the first error.

## Key invariants

1. **No anonymous access (D4).** Every RPC except `Handshake` calls
   `authenticate` first; a missing/forged/expired token is `Unauthenticated`.
2. **Bounded `DoGet` memory.** The result is paged through the CQL cursor; the
   server never materializes a whole result set in one `RecordBatch`.
3. **Fail loud on conversion.** An unsupported CQL type, an Arrow type with no
   CQL mapping, or a value/column type mismatch returns a `ConvertError` (→
   `Status`) — never silently-wrong data, never a dropped batch.
4. **Never fabricate a Flight address.** When a replica's advertised Flight
   address is unknown the endpoint is still emitted with *no* location (client
   falls back to the queried connection); the planner does not invent a host.
5. **Write injection guard.** Column/table identifiers interpolated into
   generated `INSERT`s must pass `valid_ident`; values are escaped literals.

## Position in the dependency graph

A thin top-of-stack adapter: depends on `ferrosa-cql`, `ferrosa-cluster`,
`ferrosa-schema`, `ferrosa-common`; depended on only by the `ferrosa` binary.
See the [root crate index](../../specs/crates.md) for the full graph.
