---
crate: ferrosa-flight
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-flight — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). This crate is a network-facing read/write endpoint on the query
path, so confidentiality and silent-wrong-data severities dominate.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| FL-1 | `DoPut` materializes each row to a CQL `INSERT` text string, re-parsed per row | Slow, lossy write path: only `cql_literal`-representable scalar values survive; NULL / non-finite floats / rich types are silently *omitted* from the INSERT (the column simply isn't written) | 7 | 5 | 6 | 210 | **Open gap.** `cql_literal` returns `None` (column dropped) for NULL, non-finite f32/f64, and any non-scalar `CqlValue`. Write a typed prepared-statement / direct-mutation path that carries NULLs and rich types instead of string interpolation. |
| FL-2 | `record_batch_to_rows` (the `DoPut`/`DoExchange` reverse path) only decodes ~9 scalar Arrow types | An inbound batch containing List/Map/Struct/Date32/Time64/Interval/Decimal columns fails the whole write with `UnsupportedArrow` | 6 | 5 | 3 | 90 | **Partial / by design.** Fails loud (no silent drop) but the reverse type matrix is far narrower than the forward path — a `DoGet` batch (Date32, lists, structs) cannot be round-tripped back through `DoPut`. Track the supported-Arrow matrix; extend to match the forward path. |
| FL-3 | `DoExchange` is an upsert-with-ack channel, **not** a live CDC / subscribe stream | Clients expecting a change-feed over `DoExchange` get only per-batch write acks; there is no server-initiated push of committed changes | 4 | 4 | 4 | 64 | **By design (post-v1).** Documented in `service.rs` ("Out of scope (post-v1): a live-subscribe channel over `DoExchange`"). A real CDC `DoExchange` is unwired — see roadmap. |
| FL-4 | `GetFlightInfo` / `GetSchema` / `ListFlights` execute the real `SELECT` (page_size 1) just to learn the schema | A schema lookup runs a live single-row query: cost on an expensive predicate, and side effects of executing user CQL for metadata; `ListFlights` does this once per table | 5 | 4 | 5 | 100 | **Open gap.** Schema is derived by executing `query_to_batch` rather than from table metadata. Derive the Arrow schema from the schema snapshot (column types) without running the query. |
| FL-5 | Whole result paged but a single page can still be large | Peak memory is one page (default 1024 rows); a row with huge blobs/collections makes one page heavy | 5 | 3 | 4 | 60 | **Mitigated.** Paged `DoGet` via the CQL cursor bounds memory to one page (vs. whole-set materialization). Page size is configurable (`with_page_size`); byte-budgeted paging is a refinement. |
| FL-6 | Token signing key is process-held and supplied by the caller; no built-in rotation scheduler | A long-lived key compromise validates forged tokens until manually rotated | 7 | 2 | 5 | 70 | **Partial.** `verify_with_keys` supports a rotation overlap window (current + retired keys), HMAC-SHA256, constant-time verify, absolute expiry (default 1h). Key *provisioning/rotation cadence* is the embedder's responsibility (`ferrosa` binary). |
| FL-7 | `serve` builds a plaintext `tonic` server with no TLS | Bearer tokens and query data traverse the wire in cleartext if the embedder does not add transport security | 8 | 3 | 4 | 96 | **By design / deferred to caller.** `flight_service` returns the bare service so the embedder can wrap it with TLS + their own incoming/shutdown wiring; the convenience `serve` is plaintext. Document that production must terminate TLS. |
| FL-8 | `PollFlightInfo` is synchronous — always returns `progress = 1.0`, no continuation | A client polling a long-running query gets an immediate "complete" with the full info, with no genuine async progress | 3 | 3 | 5 | 45 | **By design (v1).** Results are ready synchronously; real long-running async polling is W-002 follow-up. |
| FL-9 | Distributed endpoint planning needs explicit `FERROSA_FLIGHT_BROADCAST` per node | If self-broadcast is unset, self-owned ranges advertise no location; a non-co-located client must already be on the right connection | 4 | 4 | 3 | 48 | **Mitigated (fail-honest).** Unresolvable addresses are omitted, never faked; the endpoint is still emitted so data stays reachable via the queried connection. Operationally requires each node to set its broadcast addr. |

## Top risks to act on

1. **FL-1 (RPN 210)** — the write path stringifies rows into CQL `INSERT`s and
   *silently omits* any column it cannot render (NULL, non-finite float, rich
   types). A client writing such a column sees a successful `DoPut` with that
   column missing. Replace text interpolation with a typed write path that
   carries NULLs and the full type set.
2. **FL-4 (RPN 100)** — schema discovery executes the user's query. Derive the
   Arrow schema from table metadata so `GetSchema`/`GetFlightInfo`/`ListFlights`
   do not run live queries.
3. **FL-7 (RPN 96)** — the convenience `serve` is plaintext; production
   deployments must wrap `flight_service` with TLS.

## Detection assets

- `tests/grpc_handshake.rs` — full gRPC `Handshake` → `DoGet` and a `DoPut`
  write / `DoGet` read-back over a real `tonic` channel.
- `tests/read_path.rs` — multi-page `DoGet`, bearer enforcement, non-`SELECT`
  rejection.
- `tests/exchange_path.rs` — `DoExchange` per-batch ack + bearer enforcement.
- `convert.rs` unit tests — full forward type coverage and the fail-loud cases
  (`TypeMismatch`, `UnsupportedArrow`).
- `token.rs` unit tests — tamper/expiry/wrong-key/rotation precedence.
- `plan.rs` unit tests — per-range tickets, RF>1 multi-location, unresolvable
  address omitted (not faked).
