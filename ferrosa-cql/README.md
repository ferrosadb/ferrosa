# ferrosa-cql

> The CQL native-protocol (v4/v5) server — the client-facing front-end to the
> Ferrosa database. The largest, most central crate in the workspace (~54k LoC).

## What this crate is

`ferrosa-cql` implements the Cassandra-compatible **CQL native binary protocol**
end to end: TCP accept loop, per-connection framing, SASL auth, a hand-written
lexer/parser, query routing into schema and storage, the result-set encoder,
prepared statements, pagination, lightweight transactions (LWT) over Accord, and
the streaming `SUBSCRIBE`/CDC extension. Clients are standard CQL drivers
(scylla-rust-driver, cdrs-tokio, the DataStax Java driver, NoSQLBench).

It is the integration hub of the storage stack: it depends on eleven sibling
crates and is the place where a wire frame becomes a storage mutation or a read.
The companion SQL front-end (`ferrosa-postgres`) shares this crate's *row codec*
— that logic was extracted to `ferrosa-row-bridge` (decision **D10**) and is
**re-exported** here at its original public paths so in-crate callers are
unaffected (see [Bridge re-export](#bridge-re-export-d10)).

## What's implemented

- **TCP server** (`server.rs`) — accept loop, per-connection Tokio task, optional
  rustls TLS, per-IP and global connection caps, per-connection in-flight
  semaphore returning `Overloaded` on saturation, `auth_disabled` resolution.
- **Frame codec** (`frame.rs`) — `tokio_util::codec` `Framed` decoder/encoder for
  the 9-byte CQL header + body, opcode table, LZ4/Snappy body compression, and a
  custom `STREAMING_FLAG` (bit 0x10) for SUBSCRIBE response frames.
- **Connection state machine** (`connection.rs`, ~4k LoC) — STARTUP → (AUTH) →
  READY handshake, per-opcode dispatch (QUERY / PREPARE / EXECUTE / BATCH /
  REGISTER / OPTIONS), bind-marker counting, subscription push pump.
- **Lexer + parser** (`lexer.rs`, `parser.rs` ~5.9k LoC, `ast.rs`) — hand-written
  tokenizer and recursive-descent parser producing a `Statement` AST: full DML,
  DDL (keyspace/table/index/type/role/function/aggregate), BATCH, LWT `IF`
  clauses, `USING TIMESTAMP/TTL`, ANN/geo `SELECT` extensions.
- **Router** (`router.rs`, ~22k LoC) — the central dispatch: `route()` classifies
  a `Statement`, tracks it for observability, checks permissions (M8), and
  delegates to `route_select`/`route_insert`/`route_update`/`route_delete`/
  `route_batch` and the DDL/role handlers. Fast paths exist for prepared
  SELECT/INSERT. ORDER BY classification picks an inline vs. spillable temp-sort
  plan. Carries the security mitigations (M8 permissions, M12 batch cap).
- **Bridge** (`bridge.rs`) — parser `Term` → wire `CqlValue` → storage
  `CellValue`/`Row` conversions, server-side function eval (`now()`,
  `toTimestamp()`), and the **re-export** of the row codec from
  `ferrosa-row-bridge`.
- **Result encoding** (`result.rs`, `types.rs`) — CQL RESULT-frame encoder, the
  16-bit type system, and the re-exported `encode_value`/`decode_value` codec.
- **Prepared statements** (`prepared.rs`) — `moka` W-TinyLFU cache keyed by the
  MD5 of the query text, weight-bounded.
- **Pagination** (`paging.rs`) — opaque `paging_state` cursor (pk + ck +
  remaining-in-partition flag) for CQL v5 paging.
- **LWT / transactions** (`accord_router.rs`, `transaction_keys.rs`,
  `transaction_limits.rs`) — routing decision (Accord in cluster mode, local in
  standalone), `IF [NOT] EXISTS` / `IF <cond>` CAS semantics with the `[applied]`
  result column, partition-key extraction for Accord, and per-connection
  transaction limits (concurrency / timeout / key count).
- **SUBSCRIBE / CDC** (`subscribe.rs`, `event.rs`) — per-connection streaming
  subscriptions that re-run an inner SELECT on an interval and push delta frames;
  dual-timestamp (Accord ts + apply ts) events; CQL `EVENT` push via a broadcast
  channel.
- **Virtual tables** (`virtual_tables/`) — `system_observability.*` runtime
  introspection tables (active_queries, connections, billing, index_usage,
  full_scan_reasons, materialization queues, alerts, query_fingerprints, …) plus
  the Cassandra-compatible `system.peers_v2` topology table.
- **Observability** (`observability.rs`, `prometheus.rs`) — per-opcode CQL
  metrics and a Prometheus text renderer.
- **Topology** (`topology.rs`) — public-vs-internal address policy for
  `system.local` / `system.peers_v2`.
- **Client** (`client.rs`) — a thin CQL client reusing `CqlCodec`, used by
  `ferrosa-ctl`.

## Bridge re-export (D10)

The byte-for-byte CQL row codec and `Partition`→row decomposition do **not** live
here — they were extracted into the dependency-light `ferrosa-row-bridge` crate so
`ferrosa-postgres` can reuse the *identical* encoder/decoder without depending on
this ~54k-LoC crate. `ferrosa-cql` re-exports them at their original paths:

- `ferrosa_cql::types::{encode_value, decode_value}` ← `ferrosa_row_bridge`
- `ferrosa_cql::bridge::{build_decorated_key, build_row, build_delete_row,
  encode_clustering, decode_pk, decode_clustering, partition_to_rows*, …}`
- `ferrosa_cql::bridge::{parse_cql_type, parse_cql_type_in_keyspace}`

`error.rs` provides `From<RowBridgeError> for CqlError` so the hundreds of
in-crate callers see no behavioural change. The rule: **there is exactly one row
encoder, and it lives in `ferrosa-row-bridge`** — a divergent copy is the top
SQL-front-end FMEA risk.

## Public API (key entry points)

| Area | Entry points |
|------|--------------|
| Server | `server::{CqlServer, ServerConfig, resolve_auth_disabled}` |
| Framing | `frame::{CqlCodec, CqlFrame, FrameHeader, Opcode, Compression}` |
| Routing | `router::{route, SharedState, RequestContext, RouteResult}` |
| LWT/Accord | `accord_router::{route_decision, RoutingMode, RouteDecision}` |
| Prepared | `prepared::{PreparedCache, PreparedPlan}` |
| Paging | `paging::PagingState` |
| Subscribe | `subscribe::{SubscriptionHandle, SubscriptionEvent, run_subscription_poll}` |
| Types/codec | `types::{CqlType, CqlValue, encode_value, decode_value}` (codec re-exported) |
| Client | `client::{CqlClient, ResultRow}` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- `ferrosa-cdc` — change-data-capture feed for SUBSCRIBE/CDC.
- `ferrosa-cluster` — consistency levels, Accord/LWT routing, DDL path, peers.
- `ferrosa-common` — `CqlType`, `CqlValue`, `Token`, `DecoratedKey`, `CellValue`.
- `ferrosa-index` — `IndexType` and secondary-index query support.
- `ferrosa-net` — internode `TaskPool`, framing helpers, graceful drain.
- `ferrosa-row-bridge` — **the re-exported row codec** (D10).
- `ferrosa-schema` — keyspaces/tables/roles, `AuthContext`, permissions, virtual tables.
- `ferrosa-session` — `SessionCore`, the protocol-agnostic engine state.
- `ferrosa-sstable` — `Partition`/`Row` shapes consumed on the read path.
- `ferrosa-storage` — `StorageEngine`, temp-sort reservations, table IDs.
- `ferrosa-udf` — user-defined function/aggregate execution.

**Called by** (crates that depend on this):

- `ferrosa` — the main binary wires up and runs the CQL server.
- `ferrosa-ctl` — uses the thin `client` for cluster management.
- `ferrosa-flight` — Arrow Flight endpoint reuses CQL parsing/routing.
- `ferrosa-loadgen` — load testing against the CQL layer.

## Tests

~945 in-crate test functions (heaviest: `router.rs` 253, `parser.rs` 158,
`bridge.rs` 121, `connection.rs` 43) plus integration tests under `tests/`
(`handshake`, `auth_integration`, `auth_warn_mode`, `bolt_transaction_state`,
`cassandra_cql_examples`). The ignored live-cluster test `fts_live_cluster`
runs in the CI cluster-integration job and asserts native `fts_match` returns a
stable flushed row from every 3-node coordinator. In-code TODO/FIXME density is
very low (1 marker); the real gaps are tracked structurally — see the FMEA.

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, position
- [Data flow](specs/data-flow.md) — INSERT/SELECT through frame→parse→route→storage + the bridge re-export
- [FMEA / known issues](specs/fmea.md) — failure modes + real gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
- Topic reference: [`specs/reference/cql.md`](../specs/reference/cql.md)
