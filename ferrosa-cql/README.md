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
  REGISTER / OPTIONS), bind-marker counting, subscription push pump. PREPARE
  metadata preserves bind order and includes synthetic typed specs for
  non-column parameters such as `LIMIT ?` (`[limit] : int`), so strict drivers
  receive one variable specification per placeholder.
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
  The `DEFAULT_RANGE_READ_LIMIT` (10_000) result cap is removed for the
  O(1)-streamable full-scan shapes, which are bounded only by the query's own
  `LIMIT` — never a server-side row cap: projected scans (e.g. `SELECT DISTINCT
  <partition-key column>`) stream through `range_read_projected_stream_all_with`;
  scalar aggregates (`SUM`/`MIN`/`MAX`/`AVG`) fold through an O(1) streaming
  accumulator (`stream_builtin_aggregates`) over the uncapped
  `range_read_stream_all_with` (exact over the whole table, no `all_rows`
  materialization); a user `LIMIT N` above the storage OOM guard streams
  (take-`N`) instead of a `Vec` materialization. The unbounded `ORDER BY` (no
  `LIMIT`) global sort now **spills** (step 5): it streams the uncapped scan
  through `sort_rows_from_partition_stream_spilling` → `ferrosa_storage::ExternalSorter`
  (bounded-memory external merge sort, cascade k-way merge), returning the fully,
  correctly ordered result with no cap and memory bounded by the spill threshold
  (`FERROSA_RANGE_SPILL_THRESHOLD_{PCT,BYTES}`). `DISTINCT`/aggregate/function-projection
  keep their `range_read_limited_rows_checked` fail-loud cap
  (spec: `specs/proposed/streaming-range-reads-no-cap.md`).
- **Scan planner** (`planner.rs`) — rule-based `ScanPlan` selection for SELECT:
  `PartitionKeyLookup` (full PK), `PartitionIndexLookup` (full PK **plus** an
  indexed residual `=` predicate — t_430c4188: keyed secondary-index consult
  restricted to the partition, O(matching rows) instead of O(partition rows),
  routed to the partition's replicas, no ALLOW FILTERING needed). Empty keyed
  consults rescan the one partition only while storage reports the index is not
  current; once `IndexStateTracker` is `Current`, an empty consult is accepted as
  a real miss. Ordinary equality plans admit only scalar indexes (B-tree, hash,
  composite, phonetic, and filtered); full-text, vector, and geo indexes have
  dedicated operators and cannot be selected for `column = value`,
  `SingleIndex` / `IndexScanWithFilter` / `IndexIntersection` (global index
  scatter-gathers), `VectorAnn` / `GeoIndex` / `FullTextIndex` (dedicated
  branches), `FullScan`. `EXPLAIN SELECT …` renders the same plan the router
  executes. `CREATE INDEX` on a CLUSTERING column wires the storage engine's
  clustering-component build path (previously a silent schema-only no-op).
- **Bridge** (`bridge.rs`) — parser `Term` → wire `CqlValue` → storage
  `CellValue`/`Row` conversions, server-side function eval (`now()`,
  `toTimestamp()`), and the **re-export** of the row codec from
  `ferrosa-row-bridge`.
  **Timestamp bounds validation (Bug C, t_a0f922a3)**: `validate_timestamp_ms`
  rejects any `timestamp` cell outside `[TIMESTAMP_MIN_MS, TIMESTAMP_MAX_MS]`
  (chrono `MIN_UTC`/`MAX_UTC` millis) at the **write** boundary — integer-literal,
  string, and bound 8-byte-blob paths alike — so an out-of-range value can never
  be persisted into a cell whose date the driver would fail to decode. On the
  **read** side `cell_to_cql_value` fails loud (`ServerError` naming the offending
  millis) on an already-corrupt on-disk timestamp instead of emitting an
  undecodable value that would crash `SELECT *` for the whole partition. See
  FMEA `CQL-12`.
- **Result encoding** (`result.rs`, `types.rs`) — CQL RESULT-frame encoder, the
  16-bit type system, and the re-exported `encode_value`/`decode_value` codec.
- **Prepared statements** (`prepared.rs`) — `moka` W-TinyLFU cache keyed by the
  MD5 of the query text, weight-bounded.
- **Pagination** (`paging.rs`) — opaque `paging_state` cursor (pk + ck +
  remaining-in-partition flag, HMAC-signed) for CQL v5 paging. Paged full-table
  scans resume WITHIN a wide partition (t_a0f922a3): the router decodes the
  cursor into `ferrosa_cluster::write_path::ScanResume { key, clustering }` so
  every producer (local iterator and each remote replica) skips the delivered
  prefix instead of re-streaming it, and the streaming collectors
  (`collect_page_from_partition_stream` / `collect_filtered_page_...`) apply
  the same skip-≤-last as an idempotent second layer. Page-advance +
  exact-union tests (hard per-page timeouts — a stall or cycling cursor FAILS,
  never hangs): `wide_partition_spanning_pages_terminates_exactly`,
  `mixed_wide_and_narrow_partitions_page_exactly`,
  `wide_partition_multi_text_clustering_pages_exactly_after_flush`,
  `many_small_partitions_pk_projection_pages_without_stalling`.
  **Wire ingress (`connection.rs`, `decode_query_params`)** — the QUERY/EXECUTE
  `<query_parameters>` section (§4.1.4) is decoded IN ORDER — flags → values →
  `page_size` (flag 0x04) → `paging_state` (flag 0x08) — into `PagingParams`,
  which `build_request_context` threads onto every `RequestContext`. Before the
  t_a0f922a3 LIVE fix these two fields were never parsed: the handlers built
  `PagingParams::default()`, so a driver's `fetch_size` resolved to the server
  default page and the client-echoed cursor was dropped — every page re-served
  page 1 (`has_more` stuck True) regardless of a correct router/coordinator
  paging path. The regression is pinned end-to-end WITHOUT hand-building
  `ctx.paging`: `live_wire_paged_scan_advances_and_terminates_exactly` serializes
  `fetch_size` + the echoed cursor to real wire bytes and re-derives them through
  `decode_query_paging` on every page (3×5000-row clustered table, projected
  `SELECT pk, ck`), asserting each page ≤ fetch_size, strict advance, exact
  15k-row union, and `has_more=false` at exhaustion — plus `query_params_decode_*`
  unit tests over the raw v4/v5 payloads.
- **LWT / transactions** (`accord_router.rs`, `transaction_keys.rs`,
  `transaction_limits.rs`) — routing decision (Accord in cluster mode, local in
  standalone), `IF [NOT] EXISTS` / `IF <cond>` CAS semantics with the `[applied]`
  result column, partition-key extraction for Accord, and per-connection
  transaction limits (concurrency / timeout / key count).
- **SUBSCRIBE / CDC** (`subscribe.rs`, `event.rs`) — per-connection streaming
  subscriptions that re-run an inner SELECT on an interval and push delta frames;
  dual-timestamp (Accord ts + apply ts) events; CQL `EVENT` push via a broadcast
  channel. A reconnecting control connection receives a retained schema-change
  event at most once, avoiding duplicate driver metadata refreshes after DDL.
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

~985 in-crate test functions, zero `#[ignore]`d (heaviest: `router.rs` ~280,
`parser.rs` 158, `bridge.rs` 121, `connection.rs` 43) plus integration tests under `tests/`
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
