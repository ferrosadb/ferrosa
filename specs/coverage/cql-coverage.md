---
scope: ferrosa-cql/
excludes: src/accord_router.rs (Accord agent)
updated: 2026-04-18
---

# CQL Protocol & Query Layer — Coverage Review

## 1. Feature Inventory

### Frame Layer — `frame.rs`

| Feature | File:Line | Notes |
|---|---|---|
| 9-byte envelope header encode/decode | `frame.rs:105–167` | Version, flags, stream_id, opcode, length |
| Opcode enum (all 16 opcodes) | `frame.rs:57–75` | ERROR…AUTH_SUCCESS |
| CqlCodec (tokio Encoder/Decoder) | `frame.rs:190–461` | Framed<TcpStream, CqlCodec> |
| Max frame size enforcement (default 256 MiB) | `frame.rs:202` | Configurable |
| LZ4 compression | `frame.rs:469–475` | 4-byte uncompressed-len prefix + lz4 block |
| Snappy compression | `frame.rs:494–508` | Raw snap encoding |
| CRC24 (v5 frame header) | `frame.rs:519–545` | `crc24_public()` |
| CRC32 (v5 frame payload) | `frame.rs:548–561` | `crc32_castagnoli()` |
| V5 segmented frame encode | `frame.rs:567–596` | `encode_v5_frame()` |
| V5 segmented frame decode | `frame.rs:281–375` | `decode_v5_frame()` |
| V4 envelope decode | `frame.rs:235–272` | `decode_v4_envelope()` |
| STREAMING flag (0x10) | `frame.rs:23` | Used by SUBSCRIBE push frames |
| COMPRESSION flag (0x01) | `frame.rs:26` | |
| 33 unit tests for frame codec | `frame.rs:599–1135` | Round-trip, truncated, oversized |

### Connection State Machine — `connection.rs`

| Feature | File:Line | Notes |
|---|---|---|
| STARTUP handling | `connection.rs:539–614` | CQL_VERSION + COMPRESSION parse |
| AUTHENTICATE response (auth enabled) | `connection.rs:608–613` | Returns `org.apache.cassandra.auth.PasswordAuthenticator` |
| AUTH_RESPONSE / SASL PLAIN | `connection.rs:650–689` | `parse_sasl_plain()`, schema.authenticate() |
| AUTH_SUCCESS | `connection.rs:676` | On valid credentials |
| Max 3 auth attempts | `connection.rs:691–700` | Closes connection on exceeded |
| READY (auth disabled path) | `connection.rs:604–607` | `auth_disabled` flag |
| OPTIONS → SUPPORTED | `connection.rs:618–646` | CQL_VERSION + COMPRESSION multimap |
| QUERY handling | `connection.rs:704–799` | Parse + route |
| PREPARE handling | `connection.rs:805` | Parse + cache + metadata |
| EXECUTE handling | `connection.rs:880–945` | Lookup prepared, bind, re-route |
| BATCH handling | `connection.rs:949–1076` | Mixed INSERT/UPDATE/DELETE |
| REGISTER handling | `connection.rs:1080–1083` | Returns READY; **event push not wired** (see Gaps) |
| USE keyspace (connection-local) | `connection.rs:782, 935, 1065` | `current_keyspace` updated on SetKeyspace result |
| Idle timeout (300 s) | `connection.rs:43, 129` | `IDLE_TIMEOUT` |
| In-flight semaphore (backpressure) | `connection.rs:123` | `max_in_flight` |
| V5 framing negotiation (USE_BETA flag) | `connection.rs:303–306` | `enable_v5_framing()` after READY/AUTH_SUCCESS |
| Compression enable after handshake | `connection.rs:290–295` | `set_compression()` after READY/AUTH_SUCCESS |
| ConnectionTracker register/deregister | `connection.rs:92–107` | RAII guard |
| Default superuser AuthContext when auth disabled | `connection.rs:1094–1101` | `cassandra` / superuser=true |
| 18 unit tests | `connection.rs:1548–2216` | |

### Auth — `auth.rs`

| Feature | File:Line | Notes |
|---|---|---|
| SASL PLAIN parse (`\0user\0pass`) | `auth.rs:22–42` | |
| AUTHENTICATE encode | `auth.rs:47–52` | |
| AUTH_SUCCESS encode | `auth.rs:55–58` | |
| AUTH_CHALLENGE | — | **Not implemented** (see Gaps) |

### Parser — `parser.rs` (4209 lines, 105 unit tests)

| Feature | File:Line |
|---|---|
| SELECT (DISTINCT, ALLOW FILTERING, ANN ORDER BY, token(), fts_match) | `parser.rs:143–212` |
| INSERT (IF NOT EXISTS, IF conditions, USING TTL/TIMESTAMP) | `parser.rs:314–347` |
| UPDATE (counter ops, collection +/-, IF conditions) | `parser.rs:353–436` |
| DELETE (map element, IF conditions) | `parser.rs:442–495` |
| BATCH (BEGIN BATCH … APPLY BATCH, LOGGED/UNLOGGED/COUNTER) | `parser.rs:529–580` |
| BEGIN TRANSACTION / COMMIT / ROLLBACK | `parser.rs:501–522` |
| CREATE TABLE (composite PK, clustering, TTL, WITH options) | `parser.rs:626–769` |
| CREATE KEYSPACE | `parser.rs:798–838` |
| ALTER TABLE (ADD/DROP/RENAME/WITH) | `parser.rs:966–1022` |
| DROP TABLE / KEYSPACE / INDEX / TYPE / FUNCTION / AGGREGATE | `parser.rs:1028–1078` |
| CREATE/ALTER/DROP TYPE (UDT) | `parser.rs:1084–1165` |
| CREATE/DROP FUNCTION | `parser.rs:1171–1272` |
| CREATE/DROP AGGREGATE | `parser.rs:1274–1366` |
| CREATE INDEX (USING type, WITH OPTIONS) | `parser.rs:1372–1433` |
| CREATE ROLE / ALTER ROLE / DROP ROLE | `parser.rs:896–964, 1059–1065` |
| GRANT / REVOKE | `parser.rs:1483–1511` |
| USE | `parser.rs:1461–1465` |
| TRUNCATE | `parser.rs:1471–1477` |
| SUBSCRIBE / UNSUBSCRIBE | `parser.rs:1611–1663` |
| EXPLAIN | `parser.rs:1665–1677` |
| Type system (scalar, collection, frozen, UDT, vector) | `parser.rs:1991–2081` |
| Nesting depth guard (MAX 32) | `parser.rs:19, 91–105` |
| Max collection elements (65 536) | `parser.rs:27` |
| Cassandra doc example corpus test | `tests/cassandra_cql_examples.rs` | Parses upstream CQL files |

### CQL Type System — `types.rs` (43 property tests)

| Feature | File:Line |
|---|---|
| All scalar types encode/decode | `types.rs:21–351` |
| Collection types (list, set, map) | `types.rs:21–351` |
| Tuple, frozen UDT | `types.rs:21–351` |
| varint / decimal | `types.rs:355–392` |
| vector<float, N> | `types.rs:21–351` |
| inet (IPv4 + IPv6) | `types.rs:21–351` |

### CQL Bridge — `bridge.rs` (112 unit tests)

| Feature | File:Line |
|---|---|
| term_to_cql_value (AST → CqlValue) | `bridge.rs:31–381` |
| CQL type name resolution (schema-aware) | `bridge.rs:719–804` |
| Composite partition key encoding | `bridge.rs:814–841` |
| Row building from CqlValue | `bridge.rs:853–959` |
| partition_to_rows (storage → result rows) | `bridge.rs:969–1191` |
| cql_value_to_json (toJson() function) | `bridge.rs:1261–1485` |

### Prepared Statements — `prepared.rs`

| Feature | File:Line |
|---|---|
| PreparedCache (moka W-TinyLFU, weight-based) | `prepared.rs:27–58` |
| MD5-based prepared statement ID | `prepared.rs:61–68` |
| Schema invalidation (invalidate_all) | `prepared.rs:56–58` |
| pk_count in PREPARE response | `result.rs:155–220` |

### Result Encoding — `result.rs` (12 unit tests)

| Feature | File:Line |
|---|---|
| VOID result | `result.rs:21–25` |
| SET_KEYSPACE result | `result.rs:28–33` |
| SCHEMA_CHANGE result | `result.rs:42–72` |
| ROWS result (with metadata) | `result.rs:78–182` |
| Paged ROWS result + paging_state | `result.rs:94–125` |
| PREPARED result + bind metadata | `result.rs:138–230` |
| Type encoding (all CQL types) | `result.rs:286–330` |

### Paging — `paging.rs` (14 unit tests)

| Feature | File:Line |
|---|---|
| PagingState encode/decode | `paging.rs:20–92` |
| apply_pagination (offset + limit) | `paging.rs:123–179` |
| Page size + paging_state token | `paging.rs:97–111` |

### SUBSCRIBE / UNSUBSCRIBE — `subscribe.rs` (9 unit tests)

| Feature | File:Line |
|---|---|
| SUBSCRIBE EVERY <n>s | `subscribe.rs:155–212` | Poll-based |
| SUBSCRIBE DELTA | `subscribe.rs:26–74` | Diff delivery |
| UNSUBSCRIBE (by stream_id or all) | `subscribe.rs:119–138` |
| SubscriptionState (max 8 per connection) | `subscribe.rs:93–138` |
| STREAMING flag on push frames | `frame.rs:23` |
| Change-driven SUBSCRIBE (no EVERY) | `connection.rs:317–323` | **Returns error — not yet implemented** |

### Events — `event.rs` (4 unit tests)

| Feature | File:Line |
|---|---|
| CqlEvent encode (SchemaChange, TopologyChange, StatusChange) | `event.rs:74–127` |
| EventType parse | `event.rs:17–24` |
| Broadcast channel infrastructure | `router.rs:152–153` | Present but not connected to REGISTER |

### Virtual Tables — `virtual_tables/`

| Feature | File:Line |
|---|---|
| connections table | `virtual_tables/connections.rs` |
| active_queries table | `virtual_tables/active_queries.rs` |
| alerts, billing, full_scan_reasons, query_fingerprints, table_access | `virtual_tables/stubs.rs` |
| VirtualTableRegistry lookup | `virtual_tables/mod.rs` |

### Observability — `observability.rs`, `prometheus.rs`

| Feature | File:Line |
|---|---|
| CqlMetrics (per-operation counters) | `observability.rs` |
| Prometheus text exposition | `prometheus.rs` |

---

## 2. Spec Coverage Matrix

| Spec Feature | spec/cql.md | ADR-002 | ADR-006 | Implemented | Mismatch |
|---|---|---|---|---|---|
| CQL v4 framing | Yes | — | — | Yes | None |
| CQL v5 framing + CRC24/CRC32 | Yes | — | — | Yes | None |
| LZ4 / Snappy compression | Yes | — | — | Yes | None |
| All 16 opcodes defined | Yes | — | — | Yes | None |
| STARTUP / READY / AUTHENTICATE flow | Yes | — | — | Yes | None |
| AUTH_CHALLENGE opcode | Yes (opcode table) | — | — | No | **Gap**: opcode in table, encode fn absent |
| SASL PLAIN auth | Yes | — | — | Yes | None |
| SUPPORTED / OPTIONS | Yes | — | — | Yes | None |
| QUERY / PREPARE / EXECUTE / BATCH | Yes | — | — | Yes | None |
| REGISTER / EVENT | Yes | — | — | Partial | REGISTER accepted; events never pushed to registered client |
| SELECT (full feature set) | Yes | — | — | Yes | None |
| INSERT / UPDATE / DELETE / LWT | Yes | — | — | Yes | None |
| DDL (CREATE/ALTER/DROP all objects) | Yes | — | — | Yes | None |
| GRANT / REVOKE / CREATE ROLE | Yes | — | — | Parsed only; enforcement partial | Permission callsites in router.rs but auth disabled globally |
| SUBSCRIBE / UNSUBSCRIBE | Yes | — | — | Yes (EVERY); No (change-driven) | Change-driven path returns error |
| USE keyspace | Yes | — | — | Yes | None |
| Pagination (paging_state) | Yes | — | — | Yes | None |
| Prepared cache (moka, MD5) | Yes | — | — | Yes | None |
| pk_count in PREPARE response | Yes | — | — | Yes | None |
| ALLOW FILTERING | Yes (supported) | ADR-006 says reject | — | **Supported** | ADR-006 is outdated — code accepts it; spec says supported |
| Virtual tables (system_observability) | Yes | — | — | Partial (stubs present) | billing, alerts, full_scan_reasons are stubs, not live data |
| CQL type system (all types) | Yes | — | — | Yes | None |
| vector<float, N> | Yes | — | — | Yes | None |
| CqlValue ↔ CellValue bridge | Yes | — | — | Yes | None |
| CQL client module (client.rs) | Yes | — | — | Yes | None |
| Auth disabled → anonymous superuser | Yes | — | — | Yes | None |
| FERROSA_AUTH_DISABLED bypass | Implicit | — | — | Yes (auth_disabled flag) | See auth gap below |
| CQL role auth plumbing | design-cql-role-auth-rollout.md | — | — | No | AUTHENTICATE wired but `auth_disabled=true` bypasses it; Sprint A pending |

**ADR-006 mismatch**: ADR-006 §3 states `ALLOW FILTERING` should return `ERROR(Invalid)`. The current router and spec (`cql.md`) both support it. ADR-006 is stale and should be updated, but this is a documentation gap, not a code bug.

---

## 3. Test Coverage

### Handshake & Protocol Tests — `tests/handshake.rs` (26 tokio tests)

| Test | What it covers |
|---|---|
| `startup_then_authenticate_then_auth_success` | Full SASL handshake |
| `malformed_sasl_payload_returns_bad_credentials` | Auth error path |
| `three_failed_auth_attempts_closes_connection` | Max attempt enforcement |
| `auth_disabled_startup_returns_ready` | Dev-mode no-auth path |
| `query_creates_keyspace_and_table_and_inserts_and_selects` | Full DML round-trip |
| `prepare_and_execute` | Prepared statement flow |
| `cqlsh_introspection_queries_all_succeed` | system.local / system.peers |
| `cqlsh_system_local_has_tokens_column` | Token metadata |
| `cqlsh_full_workflow` | Realistic driver handshake sequence |
| `query_before_startup_returns_protocol_error` | Phase guard |
| `stream_id_preserved` | Multiplexing |
| `subscribe_every_receives_streaming_frames` | SUBSCRIBE EVERY polling |
| `subscribe_without_every_returns_error` | Change-driven guard |
| `unsubscribe_returns_void_result` | UNSUBSCRIBE |
| `subscribe_max_subscriptions_enforced` | Limit enforcement |
| `create_type_and_use_in_table` | UDT DDL + usage |
| `create_type_if_not_exists` / `drop_type_if_exists` / `alter_type_add_field` | UDT lifecycle |
| `system_schema_types_queryable` | system_schema virtual table |
| `query_with_bind_values_*` | Bound values, paging, ALLOW FILTERING |
| `v5_startup_and_query_over_framed_transport` | V5 CRC framing end-to-end |
| `v4_and_v5_coexist_on_same_server` | Protocol coexistence |

### Parser Tests — `parser.rs` (105 unit tests)

Every statement type has at least one parser round-trip test. Edge cases include: composite keys, IF conditions, collection mutations, ANN ORDER BY, SUBSCRIBE/UNSUBSCRIBE, EXPLAIN, duration parsing.

### Type System Tests — `types.rs` (43 property tests)

PropTest round-trips for all scalar and collection types. `arb_scalar_value()` strategy covers the full CqlType/CqlValue space.

### Bridge Tests — `bridge.rs` (112 unit tests)

Type coercion, partition key construction, JSON serialization.

### Cassandra CQL Example Corpus — `tests/cassandra_cql_examples.rs`

Parses every `.cql` file from the Cassandra 5.1 documentation submodule and reports pass/fail per statement. CQLSH-only commands (`SOURCE`, `DESCRIBE`, etc.) are explicitly filtered out.

### Gaps in Test Coverage

- No test for compressed frames in handshake (`COMPRESSION=lz4` STARTUP path)
- No test for AUTH_RESPONSE after auth-enabled server startup via the `cdrs-tokio` driver
- No test for EVENT push to registered client (REGISTER flow)
- No test for tracing flag (`0x02`) or warning flag (`0x08`) frame headers
- No integration test for change-driven SUBSCRIBE (blocked by implementation gap)

---

## 4. Gaps

### P0 — Blocking for correctness / driver compat

**GAP-CQL-1: EVENT push not wired to REGISTER**

`handle_register()` returns READY but never subscribes the connection to `event_sender`. The broadcast channel (`SharedState::event_sender`) exists and `CqlEvent` encodes correctly, but no code in `connection.rs` subscribes after REGISTER and pushes EVENT frames. Drivers that rely on schema-change events (e.g., to invalidate prepared statements) will silently fail.

- File: `connection.rs:1080–1083`, `router.rs:152`
- Action: After REGISTER, subscribe to `event_sender`, store event types requested, and push EVENT frames in the `select!` loop alongside subscription pushes.

**GAP-CQL-2: AUTH_CHALLENGE not implemented**

The opcode (0x0E) is defined in the opcode table and the spec, but there is no `encode_auth_challenge()` function in `auth.rs` and no code path that emits it. SASL mechanisms that require a server challenge step (e.g., future SCRAM-SHA) will not be expressible. Currently only SASL PLAIN is used (no challenge needed), but the absence is a forward compatibility gap.

- File: `auth.rs` (missing), `connection.rs` (no handler)
- Action: Add `encode_auth_challenge(data: &[u8])` helper and a challenge path in `handle_auth_response()`. Not needed until SCRAM support is added.

### P1 — Important for production readiness

**GAP-CQL-3: CQL-layer AUTHENTICATE plumbing disabled end-to-end**

`FERROSA_AUTH_DISABLED` short-circuits auth at two layers: (1) the web middleware (`ferrosa/src/web/auth.rs::auth_middleware`) and (2) the CQL layer (`connection.rs:84, 604`). The CQL AUTHENTICATE→AUTH_RESPONSE path exists and is tested in isolation, but the deployed configuration never exercises it because `auth_disabled=true` is always set.

A live agent is implementing the full rollout. Design is at `specs/decisions/design-cql-role-auth-rollout.md`. The todo is `specs/todo/todo-enable-cql-role-auth-for-graph-table-isolation.md`. Until Sprint A of that plan is complete and `FERROSA_AUTH_DISABLED` is removed from the deploy config, any process on the internal network can write to any table.

- File: `connection.rs:84`, `ferrosa/src/web/auth.rs`
- Action: Follow design-cql-role-auth-rollout.md Sprint A. Do not touch `ferrosa-cql/src/auth.rs`, `server.rs`, or `router.rs` — live agents are editing those.

### P2 — Quality / completeness

**GAP-CQL-4: Several `system_observability` virtual tables are stubs**

`billing.rs`, `alerts.rs`, `full_scan_reasons.rs`, `query_fingerprints.rs`, and `table_access.rs` exist in `virtual_tables/` but return static or empty rows. `cql.md` documents only `connections`, `active_queries`, and `storage_stats`; the stub tables are undocumented. Either promote them to live data sources or remove the stubs to avoid confusion.

- File: `virtual_tables/stubs.rs`, `virtual_tables/billing.rs`, etc.
- Action: Audit each stub. Implement or remove; update `cql.md` table list.

**GAP-CQL-5: ADR-006 §3 (ALLOW FILTERING rejection) is stale**

ADR-006 states `ALLOW FILTERING` should return `ERROR(Invalid)`. Both `cql.md` and `router.rs` explicitly support it. The ADR needs a superseding note or an update to §3 to reflect the current decision to support full-scan queries when explicitly requested.

- File: `specs/decisions/006-cql-architecture.md`
- Action: Add an addendum to ADR-006 §3 noting the reversal and the rationale (secondary index + ALLOW FILTERING is now implemented).

**GAP-CQL-6: No compression test in handshake integration tests**

`COMPRESSION=lz4` and `COMPRESSION=snappy` STARTUP paths are exercised only by unit tests inside `frame.rs`. The `handshake.rs` integration tests do not negotiate compression, so codec + server interaction under compression is untested at the TCP level.

- File: `tests/handshake.rs`
- Action: Add `negotiate_lz4_compression_and_query` and `negotiate_snappy_compression_and_query` tests.

---

## 5. Recommendations

1. **Wire EVENT push after REGISTER (GAP-CQL-1 — P0).** The infrastructure is fully built (`event_sender` broadcast, `CqlEvent` encoding, `select!` loop). The only missing piece is a receiver subscription and per-connection event filter. This is a small addition (~30 lines) with high driver-compat value.

2. **Complete auth rollout per design-cql-role-auth-rollout.md (GAP-CQL-3 — P1).** The SASL PLAIN path is correctly implemented and tested in isolation. The only blocker is the `auth_disabled` flag being set in the deploy config. Sprint A of the design doc is low-risk (2–3 days) and closes a real security gap.

3. **Add compression integration tests (GAP-CQL-6 — P2).** Two tests in `handshake.rs` would validate the negotiate-then-compress path end-to-end. The `CqlCodec` compression logic has unit tests but no TCP-level coverage.

4. **Audit and resolve virtual table stubs (GAP-CQL-4 — P2).** Stubs silently return empty data, violating the "fail loud" convention. Each stub should either be wired to a real data source or removed. Leaving dead stubs in place risks a future agent treating them as authoritative.

5. **Update ADR-006 to reflect the ALLOW FILTERING reversal (GAP-CQL-5 — P2).** Stale ADRs cause confusion when a new agent reads ADR-006 and tries to enforce the reject behavior. A one-paragraph addendum is enough to close this.
