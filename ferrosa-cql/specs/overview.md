---
crate: ferrosa-cql
status: implemented
last_updated: 2026-06-27
executive_summary: >
  The CQL native-protocol (v3/v4/v5) server and the largest, most central crate in
  the workspace (~54k LoC). It owns the full client path — TCP accept, frame
  codec, SASL auth, lexer/parser, query routing into schema and storage, result
  encoding, prepared statements, pagination, LWT over Accord, and the streaming
  SUBSCRIBE/CDC extension. It re-exports the byte-identical row codec from
  ferrosa-row-bridge (D10) so the Postgres front-end shares exactly one encoder.
---

# ferrosa-cql — Architecture Overview

## Purpose & boundary

`ferrosa-cql` is the **client-facing front-end** of Ferrosa. Its boundary spans
from the raw TCP socket to the storage engine: it decodes CQL binary frames,
authenticates, parses CQL text into an AST, routes statements through the schema
and storage engines, and encodes result sets back onto the wire. It speaks the
Cassandra native protocol (v3/v4 fully, v5 accepted for explicit conformance
testing but not yet advertised in `SUPPORTED`, v6+ rejected with a
protocol-version mismatch that advertises supported v5) so unmodified CQL drivers
connect to it.

It is deliberately the **integration hub** rather than a leaf: it depends on
eleven sibling crates and turns a wire frame into a storage mutation/read. The
one piece it does *not* own is the row codec — that lives in `ferrosa-row-bridge`
and is re-exported here (decision **D10**) so `ferrosa-postgres` reuses the exact
same encode/decode without depending on this large crate.

## Module map

| Module | LoC | Responsibility |
|--------|-----|----------------|
| `router` | ~22.4k | Central `route()` dispatch: permission checks, DML/DDL handlers, prepared fast paths, ORDER BY planning, `SharedState` |
| `parser` | ~5.9k | Recursive-descent CQL parser → `Statement` AST |
| `connection` | ~4.0k | Per-connection state machine, handshake, per-opcode dispatch, subscription pump |
| `bridge` | ~2.7k | `Term`↔`CqlValue`↔storage conversions, server-side fn eval, **row-codec re-export** |
| `frame` | ~1.4k | CQL header/body codec, opcodes, LZ4/Snappy compression, streaming flag |
| `lexer` | ~1.4k | Hand-written CQL tokenizer |
| `accord_router` | ~1.2k | LWT-on-Accord routing decisions + CAS execute-phase logic |
| `subscribe` | ~1.2k | Per-connection streaming subscriptions, dual-timestamp events |
| `prometheus` | ~1.1k | Prometheus text rendering of virtual-table + runtime metrics |
| `server` | ~1.1k | TCP accept loop, TLS, connection caps, `auth_disabled` resolution |
| `client` | ~1.0k | Thin CQL client (used by ferrosa-ctl) |
| `result` | ~0.9k | RESULT-frame encoder |
| `ast` | ~0.9k | Statement/expression AST |
| `types` | ~0.6k | 16-bit CQL type system, codec re-export |
| `transaction_keys` / `transaction_limits` | ~1.0k | Accord partition-key extraction, per-connection txn limits |
| `planner` | ~0.6k | Scan planning |
| `error` / `paging` / `duration` / `session` / `topology` / `event` / `observability` / `prepared` | — | Error type + `From<RowBridgeError>`, paging cursor, duration type, session, topology policy, EVENT, metrics, prepared cache |
| `virtual_tables/` | ~4.5k | `system_observability.*` runtime introspection tables |

## Concurrency model

Each TCP connection gets its own Tokio task owning a `Framed<TcpStream, CqlCodec>`.
Hot paths are lock-free: schema reads via `ArcSwap::load()`, prepared-statement
lookups via `moka` (W-TinyLFU), storage via `Arc<StorageEngine>`. `SharedState`
holds the shared engine state (`Arc<SessionCore>` via `Deref`) plus the trackers,
metrics, event broadcast channel, and prepared cache. A per-connection in-flight
semaphore (default 128) returns `Overloaded` rather than unbounded queueing.

## Data flow

**Write (INSERT/UPDATE/DELETE).** Frame bytes → `CqlCodec::decode` → opcode
dispatch in `connection` → `parser` → `Statement` → `router::route()`. The router
checks permissions (M8), converts `Term`s to `CqlValue`s via `bridge`, builds the
`DecoratedKey` + storage `Row` via the re-exported `ferrosa-row-bridge` builders,
and applies through `SessionCore`'s write path / `StorageEngine`. LWT statements
(serial consistency set, cluster mode) detour through `accord_router` →
`route_lwt_via_accord`. A void/applied RESULT frame is encoded back.

**Read (SELECT).** Same front half; `router::route_select` resolves the table,
plans the scan (`planner`, ORDER BY classification), reads `Partition`s from
storage, decomposes them to rows via the re-exported
`partition_to_rows_with_storage_mapping` (tombstone/TTL skipping, storage→table
column mapping), applies projection/LIMIT/paging, and `result.rs` encodes the
Rows RESULT frame (with `paging_state` when more pages remain).

Full-text predicates (`WHERE col = fts_match('...')`) take a dedicated branch:
it resolves the matching partition keys through the cluster write path
(`WritePath::fulltext_search`) — which scatter-gathers across every node's local
FTI and unions the keys — then fetches and post-filters those partitions. A
coordinator-local index lookup previously made `fts_match` non-deterministic on a
cluster (BUG-F-007); standalone/pair still resolve locally.

See [data-flow.md](data-flow.md) for the sequence diagrams.

## Key invariants

1. **One row encoder.** Encode/decode is the re-export from `ferrosa-row-bridge`;
   no second encoder exists in this crate (D10). A divergent copy would silently
   corrupt cross-front-end reads.
2. **Permission check on every route.** Each `route_*` function calls
   `Schema::check_permission` (M8); warn-mode logs+counts denials but proceeds.
3. **Batch size capped.** `MAX_BATCH_STATEMENTS` (default 500, M12) bounds BATCH.
4. **Range-checked narrowing.** `bridge` range-checks all narrowing integer
   conversions (M5); no `unwrap()` on user data (M4).
5. **Fail loud on the Accord gap.** LWT routing in standalone/pair mode returns a
   clear `ServerError` rather than silently falling back to a non-linearizable
   local path (p0-03 policy).
6. **16-byte TimeUUID from `now()`.** `eval_now()` guarantees a 16-byte encoding —
   a short one wedges TimeUUID-clustered tables at flush.

## Native protocol version behavior

- The wire codec accepts **v3, v4, and v5** request bytes and replies with the
  negotiated response byte (`0x83`, `0x84`, or `0x85`).
- **v6+ is rejected** with a `ProtocolVersionMismatch` error that advertises v5
  as the greatest supported version.
- The `SUPPORTED` response currently advertises **v3 and v4** in
  `PROTOCOL_VERSIONS`. Production drivers therefore negotiate v4, which is fully
  conformant today.
- **v5 handshake and modern framing are implemented**, and the server emits the
  v5 `result_metadata_id` in PREPARE responses. v5 can be used explicitly for
  conformance testing and future driver support.
- Remaining v5-only features not yet complete are tracked in
  [roadmap.md](roadmap.md).

## Position in the dependency graph

A hub, not a leaf. Depends on eleven sibling crates (`ferrosa-cdc`,
`-cluster`, `-common`, `-index`, `-net`, `-row-bridge`, `-schema`, `-session`,
`-sstable`, `-storage`, `-udf`); depended on by `ferrosa`, `ferrosa-ctl`,
`ferrosa-flight`, `ferrosa-loadgen`. See the
[root crate index](../../specs/crates.md) for the full graph.
