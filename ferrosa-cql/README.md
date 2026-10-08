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
  On a v5 connection the envelope is additionally wrapped in the modern frame
  format (3-byte length/flag header + CRC24, payload, CRC32).
  Auth-enabled connections switch at the `AUTHENTICATE` response boundary, so
  `AUTH_RESPONSE` and `AUTH_SUCCESS` use checksummed v5 frames as clients expect.
  A payload is capped at `V5_MAX_PAYLOAD` (2^17−1 = 128 KiB) by the 17-bit
  length field, so
  **an envelope larger than that is split across consecutive frames, every one
  of them marked `isSelfContained=0`**; the receiver reassembles by reading the
  envelope header's own length and accumulating until it is satisfied, since a
  multi-frame envelope has no terminating marker. Both directions are
  implemented and round-tripped in tests. (This path previously asserted on
  oversize payloads, which panicked the CQL runtime thread and dropped the
  connection for any response page over 128 KiB.)
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
  Standalone role create/alter/drop handlers also write the authoritative
  `system_auth.roles` row before acknowledging success, matching the
  pair/cluster persistence contract.
  The `DEFAULT_RANGE_READ_LIMIT` (10_000) result cap is removed for the
  O(1)-streamable full-scan shapes, which are bounded only by the query's own
  `LIMIT` — never a server-side row cap: projected scans (e.g. `SELECT DISTINCT
  <partition-key column>`) stream through `range_read_projected_stream_all_with`;
  paged `SELECT DISTINCT` over the complete partition key emits exactly one row
  per physical partition and resumes at partition boundaries, so clustering-row
  multiplicity cannot consume page capacity or skip a partition;
  scalar aggregates (`SUM`/`MIN`/`MAX`/`AVG`) fold through an O(1) streaming
  accumulator (`stream_builtin_aggregates`) over the uncapped
  `range_read_stream_all_with` (exact over the whole table, no `all_rows`
  materialization); a user `LIMIT N` above the storage OOM guard streams
  (take-`N`) instead of a `Vec` materialization.
  **Result cursors** (`result_cursor.rs`). A full-table SELECT whose result
  cannot be produced in scan order — `ORDER BY` (with or without `LIMIT`),
  `DISTINCT` over an arbitrary projection, or a non-aggregate function
  projection — is answered from a server-side `ResultCursor`
  (`serve_result_cursor` / `build_result_cursor`). The first request scans the
  table ONCE: with `ORDER BY`, filtered rows go into a spilling
  `ferrosa_storage::ExternalSorter`; otherwise rows are projected,
  de-duplicated for `DISTINCT` (`SpillingDedup`, resident keys
  `FERROSA_CQL_DISTINCT_RESIDENT_KEYS`) and spooled in arrival order, and a
  `LIMIT` stops the scan. Each response reads ONE page from the cursor; if rows
  remain, the cursor is parked in the node's `ResultCursorRegistry` and the
  client gets a signed, versioned cursor token (`CursorToken`) as its
  `paging_state`. Paging is O(page) per page and O(table) in total; the result
  is never collected. A parked cursor holds files and a merge head only — no
  storage scan, scan-pool slot or thread — and is deleted with its spill
  directory when idle past `FERROSA_CQL_RESULT_CURSOR_TTL_SECS` (default 300),
  `FERROSA_CQL_RESULT_CURSOR_CLOSE_GRACE_SECS` (default 30) after its
  connection closes unless a page is read meanwhile, when its last page is
  read, or when the request reading it is cancelled. A node holds at most `FERROSA_CQL_RESULT_CURSOR_MAX`
  (default 256) open cursors and refuses more with `Overloaded` before
  scanning. A token for an expired/closed cursor, from another node or a
  restarted one, for another query or role, a stale page, or a pre-cursor
  (offset or scan-position) paging state is a clear `Invalid` error — never a
  silent restart or a partial result. A cursor lives on the node that built
  it; the token names that node, and any other coordinator forwards the page
  request to it over internode (`MsgType::ResultCursorPage`,
  `result_cursor::forward_page`, Data lane deadline) and relays the reply, so
  a driver may send the next page anywhere (scylla-rust-driver retries the
  remaining pages on another node after a broken connection). It forwards only
  to a peer that advertised `CAP_RESULT_CURSOR_PAGE` in its handshake; an older
  node, an unreachable owner, or an owner that restarted gets a named error.
  The stored rows are final result rows prefixed by their sort key, so the
  owner serves a page with no statement context.
  An UNPAGED request (no page size) still receives the whole result in one
  frame, as the protocol requires, so its heap is O(result).
  Results are bounded only by the query's own `LIMIT`, never a server-side row
  cap (spec: `specs/proposed/streaming-range-reads-no-cap.md`).
  Global secondary-index reads (`SingleIndex`, `IndexScanWithFilter`,
  `IndexIntersection`) stream in row order — `(partition key, clustering)` —
  through `WritePath::index_read_stream(.., after)`, and hold O(sources) at
  every layer (t_50c8bc7d): each node merges its memtable and sidecar posting
  lists, the coordinator merges the nodes and drops replica copies by
  adjacency, and an intersection is a partition-level merge-join with one head
  per index. A plain projection is served one bounded page at a time — the
  client's page size, or `default_scan_page_size()` when unpaged — through
  `collect_filtered_page_from_partition_stream`, and the next page resumes
  strictly after the `(pk, ck)` cursor. Builtin aggregates over an index fold
  as rows stream. `ORDER BY`, `DISTINCT` and non-builtin function projections
  over an index still collect the match set (`collect_index_rows_with_limit`).
- **Scan planner** (`planner.rs`) — rule-based `ScanPlan` selection for SELECT:
  `PartitionKeyLookup` (full PK), `PartitionIndexLookup` (full PK **plus** an
  indexed residual `=` predicate — t_430c4188: keyed secondary-index consult
  restricted to the partition, O(matching rows) instead of O(partition rows),
  routed to the partition's replicas, no ALLOW FILTERING needed). The request's
  consistency level is propagated to that consult (t_2f174c97), so CL ONE
  returns after one successful replica instead of waiting for all replicas.
  Empty keyed
  consults rescan the one partition only while storage reports the index is not
  current; once `IndexStateTracker` is `Current`, an empty consult is accepted as
  a real miss. Ordinary equality plans admit only scalar indexes (B-tree, hash,
  composite, phonetic, and filtered); full-text, vector, and geo indexes have
  dedicated operators and cannot be selected for `column = value`,
  compound clustering cursors such as `(recorded_at, entity_id) > (?, ?)` are
  parsed as ordered row-value restrictions and evaluated lexicographically
  (t_4d8925f4), so ties in the leading component neither skip nor duplicate rows,
  `SingleIndex` / `IndexScanWithFilter` / `IndexIntersection` (global index
  scatter-gathers), `VectorAnn` / `GeoIndex` / `FullTextIndex` (dedicated
  branches). Full-text resolution filters for `FullTextIndex`, so an earlier
  phonetic or scalar index on the same column cannot hijack `fts_match`.
  `FullScan`. `EXPLAIN SELECT …` renders the same plan the router executes.
  **Unselective index keys scan:** with `ALLOW FILTERING`, a `SingleIndex` /
  `IndexScanWithFilter` plan whose key matches at least a tenth of the table's
  partitions (and at least 50 rows) runs as `FullScan` with post-filter instead
  (`scan_instead_of_unselective_index`, backed by
  `TableStore::index_key_is_unselective`, which counts postings only up to that
  threshold). Serving such a key point-reads every row it names; ferrosa-memory's
  single-tenant edge count took 87 s that way. Without `ALLOW FILTERING` the plan
  never switches.
  `CREATE INDEX` on a CLUSTERING column wires the storage engine's
  clustering-component build path (previously a silent schema-only no-op).
  Scalar indexes created after writes synchronously stream pre-existing active
  and flushing memtable rows into the index before indexed SELECTs can use it.
  Rows already in SSTables are backfilled asynchronously. One rule covers every
  index that cannot completely answer on this node — absent from the local
  table though the schema lists it (t_50c8bc7d), or with `IndexStateTracker`
  reporting its build unfinished (t_edd3be70): it is **withheld from the
  planner**, so the query takes the scan its `ALLOW FILTERING` licenses and says
  so at WARN, and is refused — naming each index and why — only when it licensed
  no scan. A partial index never answers as though it were complete, and a
  correct slow answer beats a server error: refusing the licensed case turned
  ferrosa-memory's entity streams into 500s across `main` and every open PR
  (t_12457d3e). Once the index is current, an empty global lookup is a real miss
  and never falls back to a scan.
  **The scan WARN is once per plan (DT-16):** a `FullScan` states itself at WARN
  the first time its `(keyspace, table, predicate column, operator)` plan is seen
  on this node, and at DEBUG after that (`FullScanTracker::scan_warn_is_first`).
  A recurring scan used to write a near-identical line every time — 93 lines from
  one legitimate multi-scan workload — which buried every other WARN. This
  demotes the **log line only**: `FullScanTracker::record` still runs on every
  scan, so `system_observability.full_scan_reasons` (and the
  `/api/observability/full_scan_reasons` endpoint) still counts each occurrence
  and remains the surface to alert on.
  Virtual `system_schema` reads share one projection/aggregate encoder: ordinary
  projections expose exactly the requested metadata, and `count(*)` returns one
  `bigint` row instead of a zero-column frame that standard drivers cannot
  decode.
- **Bridge** (`bridge.rs`) — parser `Term` → wire `CqlValue` → storage
  `CellValue`/`Row` conversions, server-side function eval (`now()`,
  `toTimestamp()`), and the **re-export** of the row codec from
  `ferrosa-row-bridge`.
  Map element assignments (`map[key] = value`) are emitted as complex cells
  whose path is the encoded key and whose value is the encoded map value. They
  therefore compose safely with whole-map inserts and later key removals.
  **Timestamp bounds validation (Bug C, t_a0f922a3)**: `validate_timestamp_ms`
  rejects any `timestamp` cell outside `[TIMESTAMP_MIN_MS, TIMESTAMP_MAX_MS]`
  (chrono `MIN_UTC`/`MAX_UTC` millis) at the **write** boundary — integer-literal,
  string, and bound 8-byte-blob paths alike — so an out-of-range value can never
  be persisted into a cell whose date the driver would fail to decode. On the
  **read** side `cell_to_cql_value` fails loud (`ServerError` naming the offending
  millis) on an already-corrupt on-disk timestamp instead of emitting an
  undecodable value that would crash `SELECT *` for the whole partition. See
  FMEA `CQL-12`.
  **Corrupt cells fail the read (t_cf7ca2cc)**: the row-bridge decomposition
  returns `RowDecodeError`, surfaced as `CqlError::CorruptCell` (server error
  0x0000, message names `keyspace.table`, column and partition key) instead of a
  row with a NULL. See FMEA `CQL-Tcf7ca2cc`.
- **Result encoding** (`result.rs`, `types.rs`) — CQL RESULT-frame encoder, the
  16-bit type system, and the re-exported `encode_value`/`decode_value` codec.
- **Prepared statements** (`prepared.rs`) — `moka` W-TinyLFU cache keyed by the
  MD5 of the query text, weight-bounded. EXECUTE preserves the exact wire
  encoding for scalar types that lack lossless parser literals (`date`, `time`,
  `duration`, `decimal`, and arbitrarily large `varint`) until the value is
  decoded against its prepared column type.
- **Pagination** (`paging.rs`) — opaque `paging_state` cursor (pk + ck +
  remaining-in-partition flag, HMAC-signed) for CQL v5 paging. Queries served
  from a result cursor carry a different token instead (`result_cursor::CursorToken`:
  magic `FF 52 43`, version byte (2), owner host id, node epoch, cursor id,
  page sequence, query fingerprint; signed with the same key); each decoder refuses the other's
  token by name. Paged full-table
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
  result column (conditional statements require SELECT as well as MODIFY and
  fail closed with `Unauthorized` before the condition is evaluated, in CQL,
  batches and transaction blocks alike), partition-key extraction for Accord, and per-connection
  transaction limits (concurrency / timeout / key count). Both separately sent
  `BEGIN` / body / `COMMIT` statements and the documented single-query
  `BEGIN TRANSACTION; ...; COMMIT TRANSACTION;` block form use the same
  registry-backed Accord path; body errors roll the block back immediately.
  **Every conditional write is evaluated, never dropped (t_cd5142b5).**
  Standalone `UPDATE`/`DELETE ... IF <cond>`, `IF EXISTS` and `INSERT ... IF NOT
  EXISTS` read the current row and evaluate the clause with the same
  `accord_router::eval_if_conditions` the cluster path gates on, atomically
  under a per-partition lock (`local_lwt.rs`) held until the write lands; the
  reply is the standard `[applied]` row (plus current values when not applied).
  Conditions inside `BEGIN TRANSACTION` blocks and inside any `BATCH`
  (logged/unlogged/counter) are rejected with the typed
  `CqlError::ConditionalUnsupported` (code 0x2200) before anything is written.
  **In cluster mode every condition, `IF NOT EXISTS` included, is evaluated on
  the statement's own row (t_7a0acbc8).** Replicas read the statement's row of
  the target table at the agreed `t` (`ReadPredicate::ReadClusteringRow`, which
  returns that one row, t_5504f601); the coordinator evaluates the condition on
  the row at the statement's clustering and gates the write with
  `eval_lwt_for_statement`. A row with the same key in another table, or another
  row of the partition, does not satisfy or fail the condition.
- **SUBSCRIBE / CDC** (`subscribe.rs`, `event.rs`) — per-connection streaming
  subscriptions that re-run an inner SELECT on an interval and push delta frames;
  dual-timestamp (Accord ts + apply ts) events; CQL `EVENT` push via a broadcast
  channel. A reconnecting control connection receives a retained schema-change
  event at most once, avoiding duplicate driver metadata refreshes after DDL.
  The DataStax Java v5 smoke suite exercises the resulting CREATE INDEX → DROP
  KEYSPACE schema-agreement path and passes all 38 checks.
- **Virtual tables** (`virtual_tables/`) — `system_observability.*` runtime
  introspection tables (active_queries, connections, billing, index_usage,
  full_scan_reasons, materialization queues, alerts, query_fingerprints, …) plus
  the Cassandra-compatible `system.peers_v2` topology table.
- **Observability** (`observability.rs`, `prometheus.rs`) — per-opcode CQL
  metrics and a Prometheus text renderer.
- **Request metrics** (`request_metrics.rs`) — client load on `/metrics`:
  `ferrosa_cql_requests_total{kind,outcome}` (kind = query/prepare/execute/batch,
  outcome = ok/error/cancelled), a cumulative latency histogram
  `ferrosa_cql_request_duration_seconds{kind}` (0.5 ms to 10 s + `+Inf`), and
  `ferrosa_cql_requests_in_flight{kind}`. Recorded at both dispatch points in
  `connection.rs` (QUERY runs inline; PREPARE/EXECUTE/BATCH run on spawned tasks). A
  request whose task is dropped before it finishes is counted as `cancelled`, not
  lost. Every series is rendered before its first sample so dashboards see stable
  names. `/metrics` had no request-rate or latency series before.
- **Topology** (`topology.rs`) — public-vs-internal address policy for
  `system.local` / `system.peers_v2`.
- **Client** (`client.rs`) — a thin CQL client reusing `CqlCodec`, used by
  `ferrosa-ctl`.

## CQL Accord transaction bounds

The connection-independent `BEGIN TRANSACTION` registry has per-node runtime
bounds loaded at startup. Environment variables override these defaults without
rebuilding Ferrosa:

| Environment variable | Default | Bound |
|---|---:|---|
| `FERROSA_CQL_TRANSACTION_MAX_OPEN` | `10000` | Open transactions in the node registry |
| `FERROSA_CQL_TRANSACTION_MAX_STATEMENTS` | `10000` | Staged reads and writes combined per transaction |
| `FERROSA_CQL_TRANSACTION_DEFAULT_TIMEOUT_MS` | `10000` | Default open transaction lifetime in milliseconds |
| `FERROSA_CQL_TRANSACTION_MAX_TIMEOUT_MS` | `600000` | Largest accepted `USING TIMEOUT` value in milliseconds |
| `FERROSA_CQL_TRANSACTION_REAPER_INTERVAL_MS` | `1000` | Expiration sweep cadence in milliseconds |

All values must be positive, and the default timeout must not exceed the maximum.
Malformed or inconsistent overrides log an error and use the defaults without
stopping startup. The timeout evicts abandoned staged state; the registry and
per-transaction statement caps bound concurrent memory use. See the public
[`PROFILE.md`](../PROFILE.md) for the operator tuning guide.

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

## USING TIMESTAMP range (t_cf637b6e)

`USING TIMESTAMP` (INSERT, UPDATE, DELETE, BATCH) at or above 1e18 is refused
with an invalid-request error: storage reads that range as legacy nanosecond
Accord stamps and divides it by 1000. This is a deliberate Cassandra
incompatibility (Cassandra accepts any `long`); 1e18 microseconds is the year
33658. `SELECT writetime()`/`TTL()` are not wired to cell metadata yet and
return null (t_7987e84c).

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

Three slow `router.rs` paging/limit tests live in a `mod slow` gated behind the
`slow-tests` feature instead of `#[ignore]`. PR CI compiles them
(`--all-features`) but skips running them (`--skip ::slow::`);
`nightly-slow-tests.yml` is where they run.

### Write backpressure errors

Storage's typed `Error::Overloaded { reason, table }` maps directly to CQL
`OVERLOADED` (`0x1001`). The legacy string-detected backpressure path remains
for older storage/cluster errors and logs when it is used.

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, position
- [Data flow](specs/data-flow.md) — INSERT/SELECT through frame→parse→route→storage + the bridge re-export
- [FMEA / known issues](specs/fmea.md) — failure modes + real gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
- Topic reference: [`specs/reference/cql.md`](../specs/reference/cql.md)

> T-022: `bridge::{resolve_builtin_type, cql_type_name, cql_type_display_name}` delegate to `ferrosa_common::cql_type::names`.

## jsonb (T-150)

jsonb has no CQL literal binding yet: `term_to_cql_value` refuses every literal into a jsonb target with a "not yet supported (T-170/T-171)" error, and `CONTAINS` over `list<jsonb>` errors instead of matching nothing. Results follow D6a: the wire type is varchar and the cell is the JSON text; `toJson` prints the document. LWT `IF` orders jsonb by D18 (T-150).

### jsonb placement (T-154a)

`CREATE TABLE` and `ALTER TABLE ADD` refuse jsonb in a partition or clustering
key and `set<jsonb>`, `map<jsonb, _>`, `vector<jsonb, n>` as InvalidRequest
(0x2200), naming the column and the rule. The check runs before the direct,
pair or Raft path sees the statement.

### jsonb DDL is standalone-only for now (T-300)

`CREATE TABLE`, `ALTER TABLE ADD`, `CREATE TYPE` and `ALTER TYPE ADD` that
create or add a jsonb column or field (top level, nested, or through a UDT) are
InvalidRequest (0x2200) on a node that is not standalone. The message names the
mode and says the capability ledger (D15a) is required; there is no bypass.
