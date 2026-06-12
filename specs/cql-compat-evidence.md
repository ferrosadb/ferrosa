# CQL Compatibility Evidence Table

Source-of-truth for public CQL compatibility documentation at
`docs/database/cql-compatibility.html` and `docs/database/migration.html`.

Every public claim must have a row here. If verdict is **unverified**, the
claim must be absent from the public page. Audited 2026-06-11.

---

## Protocol Version

| Claim | Verdict | Evidence |
|-------|---------|----------|
| Server negotiates at v4 | **Confirmed** | `ferrosa-cql/src/frame.rs:396–404` — codec decoder checks `version_byte` and returns `ProtocolVersionMismatch { supported: 0x04 }` for any version ≥ 0x05. Test at line 828 (`codec_rejects_v5_use_beta_query`) and 1258 (`codec_rejects_v5_request_to_force_v4_fallback`) confirm v5 STARTUP is rejected with `supported=4`. |
| CLAUDE.md crate table says "CQL native protocol v5" for ferrosa-cql | **Internal label only** | `ferrosa-cql/src/frame.rs:1` module doc reads "CQL native protocol v5 frame encoding and decoding" — this refers to the codec's internal ability to parse/encode v5 frames (used on established v5 connections if `enable_v5_framing()` is called). The negotiation hard-cap to v4 is separate. Client-visible protocol is v4. Public docs correctly say "negotiation capped at v4". |
| VERSION_REQUEST = 0x05 constant | **Internal** | `frame.rs:12` — this constant is for the codec's internal v5 path, not what is sent to clients. Responses always use `VERSION_RESPONSE = 0x84` (v4 response byte). |

---

## GROUP BY

| Claim | Verdict | Evidence |
|-------|---------|----------|
| GROUP BY is not supported | **Confirmed missing** | Zero hits for `GROUP BY`, `group_by`, or `GroupBy` in `ferrosa-cql/src/ast.rs`, `parser.rs`, or `planner.rs`. `SelectStatement` struct has no `group_by` field. Parser has no GROUP BY token path. Router comment at `router.rs:3130` explicitly notes "no GROUP BY". |

---

## Aggregate Functions (COUNT / MIN / MAX / SUM / AVG)

| Claim | Verdict | Evidence |
|-------|---------|----------|
| Built-in aggregates supported | **Confirmed** | `ferrosa-cql/src/router.rs:4119–4156` — `has_builtin_agg` detection, `compute_builtin_aggregate()` call. COUNT, AVG, MIN, MAX, SUM are all handled as a single result row. |
| Aggregates without GROUP BY work (whole-table aggregate) | **Confirmed** | Same code path; all rows fetched then reduced. |
| Aggregates with GROUP BY | **Not supported** — see GROUP BY row above. |

---

## NULL Semantics

| Claim | Verdict | Evidence |
|-------|---------|----------|
| CQL NULL maps to `CellValue::Empty` internally | **Confirmed** | `specs/cql.md:164` — "Null handling: CQL null (length = -1) maps to `CellValue::Empty`". |
| NULL wire encoding is standard (-1 bytes) | **Confirmed** | `ferrosa-cql/src/result.rs:419–424` — `None` and `CqlValue::Null` both write `buf.put_i32(-1)`, matching CQL protocol spec. No driver-visible divergence on read. |
| Explicit NULL INSERT writes a tombstone (matches Cassandra) | **Confirmed** | `bridge.rs:2334–2344` test `build_row_null_value_is_a_tombstone_not_an_empty_live_cell` asserts null → tombstone, not live empty cell. |
| Delete-vs-write tie on same timestamp diverges from Cassandra | **Confirmed behavioral difference** | `cql-compatibility.html` already documents: Ferrosa favors the write; Cassandra favors the delete. Evidence: `docs/database/cql-compatibility.html:461–463`. |

---

## Logged Batch Atomicity

| Claim | Verdict | Evidence |
|-------|---------|----------|
| Logged batch executes (does not error) | **Confirmed** | `router.rs:5662` routes `BatchType::Logged` to `route_logged_batch()`. |
| Single-node: atomic via commit log group write | **Confirmed** | `router.rs:5694–5772` — collects mutations, calls `write_path.write_batch()`. Comment at 5694 says "commit log provides crash recovery". |
| Cluster: full 3-phase batchlog protocol | **Confirmed in comment; not independently verified** | `router.rs:5700–5701` says "Delegates to `coordinate_logged_batch()` for the full 3-phase batchlog protocol." Not independently audited in this pass. |
| Old claim "not yet supported" on public page | **Incorrect** — partially supported; batchlog atomicity semantics are implemented at least for single-node path. |

---

## Query Tracing

| Claim | Verdict | Evidence |
|-------|---------|----------|
| Query tracing (Cassandra TRACING flag) not supported | **Confirmed missing** | No hits for `tracing_id`, `tracing_session`, `TRACING`, or `query_tracing` in `ferrosa-cql/src/` (excluding Rust tracing library calls which are structured logging, not CQL tracing). The QUERY/EXECUTE frame flag 0x02 (TRACING) is accepted by the frame decoder but not acted upon. |

---

## Materialized Views

| Claim | Verdict | Evidence |
|-------|---------|----------|
| Not supported | **Confirmed** | `ast.rs` has no `CreateMaterializedView` variant. Parser has no materialized view path. DDL table in compat page marks it "Not yet". |

---

## PER PARTITION LIMIT

| Claim | Verdict | Evidence |
|-------|---------|----------|
| PER PARTITION LIMIT not supported | **Confirmed missing** | Zero hits for `PER_PARTITION`, `per_partition_limit`, or `PerPartitionLimit` anywhere in `ferrosa-cql/src/`. `SelectStatement` has no such field. Parser has no keyword path. |

---

## ALLOW FILTERING

| Claim | Verdict | Evidence |
|-------|---------|----------|
| ALLOW FILTERING accepted (allows full-table scans) | **Confirmed** | `ast.rs:360` — `SelectStatement.allow_filtering: bool`. Parser sets it. Router at `router.rs:15257` test `allow_filtering_required_for_non_indexed_where_column`. |

---

## LIKE

| Claim | Verdict | Evidence |
|-------|---------|----------|
| LIKE supported as post-scan predicate | **Confirmed** | `ast.rs:232` — `ComparisonOp::Like`. Page documents `%` wildcard, case-sensitive, requires ALLOW FILTERING. |

---

## Secondary Index Semantics Differences from Cassandra SAI

| Claim | Verdict | Evidence |
|-------|---------|----------|
| Indexes are eventually consistent (not synchronous) | **Confirmed** | `cql-compatibility.html:699` already documents async build. "Indexes are eventually consistent — there's a brief window between write and index availability." This is a behavioral difference vs Cassandra SAI which also has this property but it warrants disclosure. |
| Index semantics use storage-attached model (not 2i or SAI protocol) | **Confirmed** | `CLAUDE.md:ferrosa-index` crate description. Not wire-compatible with Cassandra SAI query plans. |

---

## Counters

| Claim | Verdict | Evidence |
|-------|---------|----------|
| Counter type accepted and increment/decrement works | **Confirmed** | `router.rs:5312–5378` handles `CqlType::Counter` in UPDATE assignments. `ast.rs:478` has `BatchType::Counter`. |
| Counter semantics match Cassandra | **Partially verified** | Code reads current value and adds delta. Cross-replica counter idempotency (Cassandra uses sharded counters with reconciliation) not independently audited. Mark as "behavioral differences may exist in multi-node scenarios" if claiming full compatibility. |

---

## TTL

| Claim | Verdict | Evidence |
|-------|---------|----------|
| USING TTL accepted on INSERT and UPDATE | **Confirmed** | `ast.rs:382,438` — `using_ttl: Option<Term>` on both `InsertStatement` and `UpdateStatement`. Parser at `parser.rs:2869–2920`. |
| TTL enforced at read time (expired cells hidden) | **Confirmed** | `bridge.rs:1160–1169` — `ldt_is_expired()` function; `bridge.rs:1228,1325` — liveness TTL check. Test at `bridge.rs:2576` `partition_to_rows_skips_fully_expired_ttl_row`. |
| TTL function in SELECT (returns remaining TTL) | **Confirmed** | `router.rs:4096` — "ttl" in the built-in function skip list. `bridge.rs:1389,1484` — `CellMetadata.ttl` field populated. |

---

## UDFs / Aggregates (CREATE FUNCTION / CREATE AGGREGATE)

| Claim | Verdict | Evidence |
|-------|---------|----------|
| CREATE FUNCTION DDL supported | **Confirmed** | `ast.rs:77–88` — `Statement::CreateFunction`. |
| Only WASM (and AssemblyScript) UDF languages supported | **Confirmed** | `router.rs:9711–9714` — rejects any language not `wasm` or `assemblyscript` with "unsupported UDF language". Test at `router.rs:13000`. |
| Java UDFs not supported | **Confirmed** | Same — `router.rs:13018` test explicitly tries `LANGUAGE java` and expects rejection. |
| CREATE AGGREGATE DDL supported | **Confirmed** | `ast.rs:94–110` — `Statement::CreateAggregate`. |
| UDA execution in SELECT | **Confirmed** | `router.rs:4128–4130` — `ResolvedFunctionKind::Aggregate` detected and executed. |

---

## CDC (Change Data Capture)

| Claim | Verdict | Evidence |
|-------|---------|----------|
| Internal CDC reader exists in commit log | **Confirmed** | `ferrosa-storage/src/commitlog/cdc.rs` — `CdcReader` with checkpoint. `ferrosa-storage/src/lib.rs:49` exports it. |
| Cassandra CQL CDC flag on tables (ALTER TABLE WITH cdc=true) | **Unverified** | `router.rs:1818,1857` — `cdc` field in system_schema tables set to false by default (`engine.rs:8675`). Whether `WITH cdc = true` in CREATE TABLE/ALTER TABLE is parsed and respected is not confirmed in this audit. |

---

## Schema-Change EVENT push after REGISTER

| Claim | Verdict | Evidence |
|-------|---------|----------|
| REGISTER opcode accepted | **Confirmed** | `connection.rs:1062` routes `Opcode::Register` to `handle_register()`. |
| handle_register returns READY | **Confirmed** | `connection.rs:1803–1806` — returns `HandleResult::Reply(Opcode::Ready, BytesMut::new())`. Comment says "Event push is deferred." |
| EVENT frames are actually pushed to registered clients after schema changes | **Not implemented** | `event_sender` broadcast channel is created (`server.rs:554`) but `event_sender.send()` is never called anywhere in `ferrosa-cql/src/`. The broadcast receiver is never subscribed per-connection. REGISTER is accepted but schema-change / topology / status EVENT push is inert. This is the "wired-but-inert" issue from the prior audit. |

---

## Vnodes / Token Allocation

| Claim | Verdict | Evidence |
|-------|---------|----------|
| Murmur3Partitioner used | **Confirmed** | `CLAUDE.md`: "Partitioner: Murmur3Partitioner (Cassandra compatible)". |
| Vnode count / token assignment behavior | **Unverified** | No code audit performed. Cassandra defaults 16 vnodes per node (Cassandra 4.x) or 256 (Cassandra 2.x). Ferrosa may differ. Mark as unverified; do not claim compatibility. |

---

## Summary: Items That Must Appear on Public Page

### Items to add to "Not Yet Supported"
1. **GROUP BY** — parser and AST have no support; queries using GROUP BY will error.
2. **PER PARTITION LIMIT** — not parsed or planned.
3. **Query tracing** — TRACING flag accepted but ignored; no tracing response.
4. **Schema-change EVENT push** — REGISTER accepted, READY returned, but no EVENT frames are ever pushed. Drivers that depend on server-push schema invalidation (e.g. some DataStax driver versions) must poll instead.

### Items to add to "Behavioral Differences"
1. **Write-vs-delete tie on same timestamp** — Ferrosa favors the write; Cassandra favors the delete. (Already documented, confirm it stays.)
2. **LOGGED batch semantics** — single-node: atomic via commit log group write (no batchlog). In Cassandra, logged batches use a batchlog for cross-coordinator replay on failure. Ferrosa's single-node path omits the batchlog but provides equivalent crash-recovery via the commit log. Application behavior is the same for non-crashing scenarios.
3. **Java UDFs not supported** — Cassandra uses Java (and JavaScript) for `CREATE FUNCTION`. Ferrosa uses WebAssembly only. Existing Java UDFs must be recompiled to WASM.
4. **Secondary index eventual consistency window** — Already on the page; confirm it stays in the Behavioral Differences section.
5. **Counter multi-node semantics** — Cassandra uses distributed sharded counters with specific reconciliation. Ferrosa's counter implementation is not fully audited for multi-node edge cases.

### Items that are fine as-is
- TTL: fully implemented, write and read enforcement confirmed.
- Aggregates (COUNT/MIN/MAX/SUM/AVG): implemented.
- ALLOW FILTERING: implemented.
- LIKE: implemented.
- WASM UDFs: implemented and documented correctly.
- Protocol v4 negotiation: confirmed, page is accurate.
- Materialized views: correctly listed as not supported.

### Items that were overclaiming (now fixed)
- "Most applications migrate without code changes" — no evidence; replaced with specific verified driver test matrix claim.
- Logged batch listed as "not yet supported" — incorrect; it is implemented.
- "GROUP BY, PER PARTITION LIMIT, query tracing, schema-change EVENT push" — all missing but unlisted.
