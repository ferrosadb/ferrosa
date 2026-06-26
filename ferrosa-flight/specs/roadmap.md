---
crate: ferrosa-flight
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-flight — Roadmap

Sourced from the in-code design notes (`service.rs` / `plan.rs` doc comments),
the FMEA gaps ([fmea.md](fmea.md)), and the dependency/usage review. The crate
implements every Flight RPC today; the roadmap is about depth and hardening, not
filling stubs.

## Now (highest value)

- **Typed write path for `DoPut` / `DoExchange` (FMEA FL-1).** Replace
  per-row `INSERT` text interpolation (`build_insert` + `cql_literal`) with a
  typed prepared-statement or direct-mutation path. Today NULL, non-finite
  floats, and any non-scalar `CqlValue` are *silently omitted* from the
  generated INSERT — a successful `DoPut` can drop a column. The write path must
  carry NULLs and the full CQL type set, fail-loud on the genuinely
  unrepresentable.
- **Derive schema without executing the query (FMEA FL-4).** `GetSchema`,
  `GetFlightInfo`, and `ListFlights` currently run the user's `SELECT` (page
  size 1) just to learn its Arrow schema. Build the schema from the table's
  declared column types in the schema snapshot so metadata RPCs do not execute
  live queries.

## Next

- **Widen the reverse Arrow→CQL type matrix (FMEA FL-2).** `record_batch_to_rows`
  covers ~9 scalar Arrow types; a batch produced by the forward path (Date32,
  Time64, lists, maps, structs, intervals, decimal-as-text) cannot be written
  back via `DoPut`. Extend the decoder to match the forward coverage so a
  `DoGet` → `DoPut` round-trip works for the full type set.
- **TLS for the convenience server (FMEA FL-7).** Document clearly that
  `server::serve` is plaintext and production must wrap `flight_service` with
  TLS; optionally add a `serve_tls` helper so the common case is secure by
  default.
- **`FixedSizeBinary(16)` for `uuid`/`timeuuid`.** The forward path emits UUIDs
  as canonical text (`cql_type_to_arrow` notes this is a refinement target);
  fixed-size binary is the more Arrow-native, compact representation.

## Later

- **Live CDC / subscribe channel over `DoExchange` (FMEA FL-3).** Today
  `DoExchange` is an upsert-with-ack; a real change-feed (server pushes committed
  changes to a subscribed client) is explicitly post-v1. Wire it to the CDC
  source when that lands.
- **Genuine async `PollFlightInfo` (FMEA FL-8, W-002).** Replace the synchronous
  `progress = 1.0` response with real long-running query progress + a
  continuation descriptor.
- **Byte-budgeted `DoGet` paging (FMEA FL-5).** Bound a page by serialized bytes,
  not just row count, so a page of wide blob/collection rows stays small.

## Non-goals

- Query planning / execution, key/token derivation, consistency, replication —
  these belong to `ferrosa-cql` and the layers beneath it. This crate is the
  Flight adapter: gRPC framing, auth, CQL↔Arrow conversion, and endpoint planning.
- A second CQL value encoder. Conversion lives in `convert.rs`; the engine's row
  codec lives in `ferrosa-row-bridge` / `ferrosa-cql`.
