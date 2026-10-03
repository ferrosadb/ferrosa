---
crate: ferrosa-postgres
doc: fmea
last_updated: 2026-09-27
---

# ferrosa-postgres — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). This is a **developer-preview** front-end on the client-facing
data path, so correctness/atomicity gaps dominate. The table is grounded in the
actual code (`src/query.rs`, `src/server.rs`, `src/storage_provider.rs`,
`src/connection.rs`).

## Supported surface (what works today)

- **DML/DQL:** `SELECT` (single `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`,
  `LIMIT`, aggregates), no-`FROM` scalar selects, and **single-row** `INSERT` /
  `UPDATE` / `DELETE` (key-equality `WHERE`, Cassandra-style blind
  upsert/tombstone).
- **Protocols:** simple (`Q`) and extended (`Parse`/`Bind`/`Describe`/`Execute`/
  `Sync`/`Close`); prepared `SELECT` **and parameterized `INSERT`/`UPDATE`/
  `DELETE`**; text + binary param/result formats; `$N` type inference for
  `SELECT` (from comparison columns) and DML (from each placeholder's target
  column).
- **Parameterized DML + `INSERT … RETURNING`:** `$N` bound at `Bind`, substituted
  at `Execute`; `INSERT … RETURNING col,…`/`*` echoes the just-written values.
  This is the `Ecto.Repo.insert/update/delete/all` path.
- **Auth:** SCRAM-SHA-256 against the live schema role store.
- **Catalog:** `pg_catalog` namespace/class/attribute/type projection.

## Not yet supported (fail-loud gaps)

- `UPDATE`/`DELETE … RETURNING` → `0A000` (only `INSERT … RETURNING` is wired).
- `ON CONFLICT` (upsert) → parse error; multi-row `INSERT … VALUES (…),(…)`.
- `= ANY($N)` / IN-list parameter expansion.
- `UPDATE`/`DELETE` with a non-key or range `WHERE` (only full-PK equality).
- **Full PostgreSQL strict-serializability scope is not yet verified** — the
  supported native-driver workload and one-paused-replica active-quorum schedule
  passed CI on PR #456 head `9ba735e72d1e117d7b64e574402ef7ab60fafad6` (run
  36329758452). General range predicates, resumed-replica catch-up, process
  crashes, and mixed CQL/PostgreSQL histories remain outside that evidence.
- `SET`/`RESET` session GUCs (simple-query path returns `0A000`).
- Function calls in DML `VALUES`; most scalar functions beyond
  `version()`/`current_database()`/`current_schema()`.
- Query cancellation (`BackendKeyData` is a `(0,0)` placeholder). TLS is
  supported (t_e1c819ad); mutual TLS / client certificates are not.
- Binary `numeric` result/param (rejected with an explicit unsupported-format error).
- CQL `Duration` + collections (`List`/`Set`/`Map`/`Tuple`/`Udt`/`Vector`) are
  unsupported and now fail a scan instead of being reported as NULL.

## Failure modes

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| PG-1 | **PostgreSQL transaction atomicity and snapshot isolation** — PG-owned MVCC commit path; CQL remains on Accord | A serializable driver sees its begin snapshot plus its own writes; commits are atomic or abort with `40001` | 9 | 3 | 4 | 108 | **Distributed wiring and targeted evidence implemented.** Snapshot, read-your-writes, write-skew, phantom, lost-update, real-time-order, rollback, protocol-parity, stable cross-node snapshot, cross-node predicate-conflict, restart, and atomic-visibility tests cover current behavior. The native-driver Jepsen gate passed transfer/register/fixed-point predicate/phantom/write-skew histories plus one paused-replica schedule on `9ba735e72d1e117d7b64e574402ef7ab60fafad6` (run 36329758452). Cluster validation uses a global PostgreSQL commit marker, so unrelated PG commits can cause false conflicts. Resumed-node catch-up and CQL/PostgreSQL interleavings are not covered. |
| PG-11 | **PostgreSQL strict-serializability recovery scope is incomplete** | A recovering replica or a mixed CQL/PostgreSQL history may diverge despite a valid PostgreSQL-only history on the active quorum | 10 | 3 | 8 | 240 | **Open boundary.** CI passed the PostgreSQL Jepsen checker with a single replica paused and checks history plus convergence only across the active quorum. The supplied read/write table set is not used for per-table Accord validation, so unrelated PostgreSQL writes can cause false conflicts. Add separate recovery/catch-up and mixed-protocol histories before making those broader claims. |
| PG-Tf348ba0b | **Result streaming: a mid-stream failure, a suspended portal, or a vanished client.** Rows now reach the socket as the executor yields them, so (a) an error can occur after rows were sent, (b) a portal suspended by `max_rows` keeps a running query and one blocking thread parked in a channel send, (c) the client can disappear mid-stream | (a) a truncated result reported as success; (b) leaked executor threads exhausting `max_blocking_threads`; (c) an executor scanning a table nobody reads | 8 | 3 | 3 | 72 | **Mitigated.** (a) `ErrorResponse` after the rows already sent, never `CommandComplete` (`result_stream::tests::a_mid_stream_error_follows_the_rows_it_interrupts`); a storage error outranks an executor error and turns a clean end-of-stream into a failure. (b) `Close`, rebind, `Sync` outside a block and session drop release the query (`extended::tests::*_releases_a_suspended_portals_query`, `sync_releases_suspended_portals_only_outside_a_transaction`). A suspended portal holds **no thread** (2026-10-03, missing-guards entry 8): the executor is an owned `RowCursor` pulled a batch at a time on a blocking thread, and the storage scan beneath it pauses and returns its thread after a 10 ms grace, keeping its position (ferrosa-storage ST-73). A client that stopped reading its socket holds none either: the socket write is async and no fetch waits on it. Before, each suspended portal parked two blocking threads and about `cores` idle clients exhausted the listener runtime's blocking pool. Evidence: `tests/pg_suspended_portals_hold_no_thread.rs` (16 portals beside `max_blocking_threads(4)`: all 4 threads free, another session's SELECT returns every row; red before at portal 3 of 16; plus 16 concurrent queries beside the same pool all complete), `tests/pg_stalled_reader_holds_no_thread.rs` (8 clients that stop reading mid-result: every thread free, another SELECT completes), `tests/pg_portal_resume.rs` (resumed across storage pauses and concurrent writes: every row once, in order), `result_stream::tests::a_suspended_stream_holds_no_blocking_thread` (red with a prefetch that waits on the consumer). Caps and idle expiry for suspended portals: PG-14, PG-15. (c) A failed socket write ends the connection and drops the stream; an in-flight fetch sees the cancel flag at its next row (`result_stream::tests::dropping_the_stream_stops_the_producer`). Bound proven end to end: `tests/pg_streaming_results.rs` (12 MiB result under a 4 MiB live-heap budget; `max_rows` resume without gap or duplicate). |
| PG-14 | **Unbounded suspended portals.** With no thread held, nothing limited how many portals a client (or all clients) could leave suspended, each holding buffered rows, its storage scan's open SSTable readers (which pin compacted-away files on disk) and any spilled sort runs | Memory, file descriptors and disk held by idle clients without limit | 7 | 3 | 3 | 63 | **Mitigated.** `portal_limits::SuspendedPortals` admits a fresh portal executed with `max_rows` only under `max_suspended_portals_per_connection` (64) and `max_suspended_portals` per node (2048), before it runs; past either it answers `53000` naming the limit before any `DataRow` (`server::txn_atomicity_tests::a_portal_over_the_suspension_limit_is_refused_before_any_row`, red when admission happened at suspension: a `DataRow` preceded the error). A portal that completes without suspending gives its place back. The node count is a lock-free CAS counter; each admitted portal holds an RAII `PortalSlot`, so every way a portal ends returns it. Refusals WARN on the edge (INFO on recovery) and count in `ferrosa_pg_suspended_portal_refusals_total`; `ferrosa_pg_suspended_portals` is the gauge. Configurable in `[postgres]` TOML or `FERROSA_POSTGRES_*`. Evidence: `portal_limits::tests::*`, `extended::tests::the_connection_limit_refuses_one_more_suspended_portal`, `tests/pg_suspended_portal_limits.rs` (third portal on a 2-portal connection and fourth on a 3-portal node refused with 53000; closing one frees its place; red with either check disabled). Residual: at the limit, an `Execute` with `max_rows` is refused even when its result would have fit and never suspended. |
| PG-15 | **A suspended portal left idle forever, or re-run silently.** A client that walks away without closing kept its portal's query until disconnect; and a portal whose query was released (expired, refused, or run to its end) would, on the next `Execute`, start its query over from the first row | Resources held indefinitely; duplicate rows returned to a client that resumes | 7 | 3 | 3 | 63 | **Mitigated.** The connection's read loop wakes at the next expiry (`suspended_portal_idle_timeout_ms`, default 10 min, the MVCC snapshot age) even if the client sends nothing, and closes portals idle that long, freeing the query, scans, spill files and node slot; a later `Execute` answers `57014` naming the timeout. A refused portal answers its `53000` again; a finished portal answers `SELECT 0`, as PostgreSQL does. Ending a transaction destroys its portals. Evidence: `extended::tests::an_idle_suspended_portal_expires_and_answers_an_error` (explicit clock), `tests/pg_suspended_portal_limits.rs::a_finished_portal_returns_no_more_rows`, `tests/pg_suspended_portal_release.rs` (idle expiry and disconnect each free the node slot, the blocking threads and the spill files). Residual: a DML portal executed twice still re-runs its statement. |
| PG-2 | **(resolved) `$N` params in DML** — parameterized `INSERT`/`UPDATE`/`DELETE` are now bound at `Bind` and substituted at `Execute` | — | 2 | 1 | 1 | 2 | **Done.** `substitute_param` fails loud `08P01` if a `$N` has no bound value; param OIDs inferred from each placeholder's target column. Covered by the `extended_parameterized_*` live tests. |
| PG-12 | **(resolved) Bind parameter decode turned bad values into NULL/text (jsonb FM-42, T-015)** | An unparseable int/uuid/timestamp bound as NULL or text, silently writing or filtering on the wrong value; an unmapped OID (json 114, jsonb 3802, timestamptz 1184) decoded as text | 9 | 3 | 3 | 81 | **Done.** `decode_param_checked` returns a typed `ParamError`: `22P02` for text-format parse failures (incl. non-UTF-8), `22P03` for malformed binary, `42704` for any non-zero OID without a mapping, `0A000` for binary numeric. OID 0 stays UTF-8 text. Numeric text now accepts exponents. Messages never echo the value. The lenient `decode_param` is gone. Covered by `pg_param_parse_failure_is_22p02_not_null` and `pg_param_unknown_oid_is_refused`. |
| PG-3 | **`INSERT … RETURNING` only; `UPDATE`/`DELETE … RETURNING` + `ON CONFLICT` unsupported** | `UPDATE`/`DELETE … RETURNING` and upsert ORM patterns fail | 5 | 5 | 2 | 50 | `INSERT … RETURNING` done (in-memory row, no read-back). `UPDATE`/`DELETE … RETURNING` fail loud `0A000`; `ON CONFLICT` fails at parse. Never a wrong row. Roadmap Next. |
| PG-4 | **(mitigated) CQL `Duration`/collections have no SQL value representation** | A scan cannot represent a non-NULL duration/list/map column | 6 | 4 | 2 | 48 | `cql_to_value` returns an error and scan failure is propagated as a query error; no fabricated NULL. Widen `Value` to add support (roadmap Later). |
| PG-5 | **Row-codec divergence from CQL/engine** — a write encodes differently than the canonical codec | Postgres-written rows read back wrong/invisible over CQL or the engine (silent corruption) | 10 | 1 | 4 | 40 | **Structural (D10):** INSERT/UPDATE/DELETE use `ferrosa-row-bridge` `build_row`/`build_delete_row`/`build_decorated_key` — the SAME code CQL uses. Reinforced by the differential oracle (PG vs real PG) + the M1 live tests. |
| PG-6 | **Missing table served as empty relation** | A typo'd table silently returns zero rows instead of erroring | 8 | 1 | 3 | 24 | **R15 guard:** `load_table` checks schema metadata first → `NoSuchTable` (`42P01`), distinct from an existing empty table. Covered by `load_table_missing_table_is_no_such_table`. |
| PG-7 | **Binary `numeric` unsupported** — a client requests binary parameters or results | The query could misdecode numeric values or emit invalid wire bytes | 5 | 2 | 2 | 20 | Checked Bind decoding and `encode_value` return explicit unsupported-format errors rather than guessing text. Implement binary numeric (roadmap Next). |
| PG-8 | **No query cancel** — `BackendKeyData` is `(0,0)` | `CancelRequest` closes the connection but cannot target a running query | 4 | 3 | 2 | 24 | TLS is implemented (t_e1c819ad: `SSLRequest` → `S` + rustls, `[postgres] require_tls` refuses plaintext with `28000`, pipelined-bytes-after-`SSLRequest` refused). A real cancel key is roadmap. Threat-model note: `UnknownRole` is a user-enumeration oracle (run dummy verifier — follow-up). |
| PG-12 | **Statement runs without authorization** — a role reads/writes a table it has no grant on | Privilege escalation over the PG listener | 9 | 1 | 2 | 18 | **Mitigated (t_e1c819ad).** `authz` checks `Schema::check_permission` on simple query, `Describe` and `Execute`; `42501` on denial. Exhaustive statement match — an unmapped kind does not compile. Covered by `tests/security_live.rs` (sabotage-verified). |
| PG-13 | **Brute-force over PG bypasses the login limiter** | Unlimited password guessing; a CQL lockout does not protect the PG port | 8 | 1 | 2 | 16 | **Mitigated (t_e1c819ad).** `VerifierStore::admit/record_failure/record_success` route through the schema's shared per-user limiter; lockout is cross-protocol. The limiter is per node and in memory — a restart or another node resets the count (same as CQL). |
| PG-9 | **Float/numeric text-format parity with Postgres** — floats use Rust `{}` shortest-form | A benign formatting difference vs PG (`1.5` vs `1.5000…`) | 3 | 4 | 4 | 48 | The differential oracle compares `f64`-parseable cells numerically with tolerance, so this is not a false alarm; exact text parity is follow-up. |
| PG-10 | **UPDATE/DELETE report `1` row unconditionally** — Cassandra blind upsert/tombstone has no match count | A driver reading the affected-row count sees `1` even when no matching row existed | 4 | 5 | 5 | 100 | Documented Cassandra semantics (`execute_update`/`execute_delete`). Differs from PG's real match count; surface in docs / revisit with read-before-write. |
| PG-T023-01 | **Type maps drift** — catalog, storage provider and RowDescription each kept a private type switch (float/double were `text` on one path, 700/701 on others; t_cd417149) | A driver sees a different type for the same column depending on the path | 6 | 5 | 4 | 120 → 12 | **Fixed (T-023):** one exhaustive `pg_types` map; float and double both `float8`. Tests: `pg_types_float_double_agree_everywhere`, `pg_types_round_trip_every_entry`, `rel_schema_uses_the_pg_types_map_for_every_cql_type`. |
| PG-T023-02 | **Unresolvable column type advertised as `text` (OID 25)** — the old `_ => 25` / `_ => Text` fallbacks | A column of an unknown or dropped UDT type looks like text; a later jsonb column would be advertised as text (DSM tables #3-#5) | 7 | 3 | 5 | 105 → 14 | **Fixed (T-023):** `pg_type_of_column` returns `PgTypeError`; `pg_attribute`, `pg_type`, `catalog_tables` and parameter inference propagate it (`42704`). Composites are named `text_rendered` arms, not a fallback. Tests: `unresolvable_column_type_fails_the_projection_loudly`, `unresolvable_column_type_string_is_refused_loudly`. |
| PG-T023-03 | **Unknown PG type name in DDL** — `cql_type_for_pg_name` (D10) | DDL guesses a CQL type | 6 | 2 | 3 | 36 → 6 | Returns `UnknownPgTypeName`; DDL (T-132a) must surface it. Test: `unknown_pg_type_name_is_refused_loudly`. |
| PG-T132a-01 | **Second schema-change path** — PG DDL writing the registry/engine itself would skip Raft replication in cluster mode | Tables exist on one node only | 8 | 3 | 3 | 72 → 8 | **Mitigated (T-132a):** `ClusterDdl` is a thin adapter over the shared `DdlPath`. Test: `pg_ddl_create_table_round_trip_catalog`. |
| PG-T132a-02 | **Key mapping wrong** — composite key mapped to the wrong partition/clustering split | Wrong data layout, unrecoverable without rewrite | 8 | 3 | 3 | 72 → 8 | **Mitigated:** first key column partitions, the rest cluster ascending in declared order. Test: `pg_ddl_create_table_key_mapping_and_if_not_exists`. |
| PG-T132a-03 | **Guessed or faked type** — an unmapped type stored as text, or a jsonb column stored as text | Silent wrong type, later data corruption | 8 | 3 | 3 | 72 → 8 | **Mitigated:** unmapped type is `42704` naming it; `json`/`jsonb` was `0A000` until T-161a, which creates a CQL `jsonb` column (`json` stored as jsonb, D11); nothing is created under a guessed type. Tests: same key-mapping test, `pg_ddl_create_table_jsonb_and_json_create_jsonb_columns`. **Residual:** `varchar(n)`/`numeric(p,s)` limits are dropped, not enforced. |
| PG-T132a-04 | **Refusals collapse to a syntax error** — every parse error was `42601` | Clients cannot tell a refused feature from a typo | 4 | 5 | 3 | 60 → 12 | **Fixed:** `parse_error_sqlstate` (exhaustive). Test: `pg_ddl_create_table_named_refusals_survive_end_to_end`. |
| PG-T132a-05 | **Duplicate / concurrent / transactional DDL reported as success or a generic error** | A CREATE that created nothing looks done; DDL silently non-transactional in a BEGIN block | 6 | 3 | 3 | 54 → 9 | **Mitigated:** `42P07` unless `IF NOT EXISTS`; a lost race re-checks the registry; `25001` inside a transaction block; `3F000` for a missing keyspace. Tests: key-mapping test, `pg_ddl_create_table_inside_a_transaction_block_is_refused`, `pg_ddl_create_table_without_the_keyspace_is_3f000`. **Residual:** no NOTICE; none for authorization: `CREATE` on the keyspace is checked at dispatch (t_d3930503). |
| PG-Tcf7ca2cc | A corrupt stored cell was scanned as a NULL column | Silent wrong answer from a SELECT | 9 | 3 | 3 | 81 → 9 | **Fixed (t_cf7ca2cc):** `produce_scan` and `read_row_image` propagate the row bridge's `RowDecodeError`; the scan records a failure naming `keyspace.table` and column, so the query errors instead of returning a NULL. Test: `scan_records_failure_naming_table_for_a_corrupt_cell`. |

## Top risks to act on

1. **PG-10 (RPN 100)** — UPDATE/DELETE report `1` row unconditionally (Cassandra
   blind upsert/tombstone); differs from PG's real match count.
2. **PG-11 (RPN 240)** — paused-replica catch-up and mixed-protocol serializability
   remain unverified. The current Jepsen fault gate proves only the recorded
   PostgreSQL history and final convergence on the active quorum.

## Detection assets

- `tests/differential_oracle.rs` — corpus + DML vs real PostgreSQL 16 (gated on
  `live-infra-tests` + `FERROSA_TEST_CONTAINERS=1`).
- `tests/m1_join_live.rs`, `tests/scram_live.rs` — full-stack over
  `tokio-postgres`, no external infra. The DML subset covers parameterized
  `INSERT`/`UPDATE`/`DELETE`, `INSERT … RETURNING id`/`*`, `UPDATE`/`DELETE …
  RETURNING` fail-loud, and extended-protocol DML inside a transaction
  (BEGIN/INSERT RETURNING/ROLLBACK discards; BEGIN/INSERT/COMMIT applies via
  Accord, including cross-node snapshot and predicate-conflict cases).
- `server::txn_atomicity_tests` — local PostgreSQL MVCC snapshots, read-your-
  writes, serializable conflicts, phantoms, rollback, and extended protocol.
- in-crate unit tests for codecs, SQLSTATE mapping, SCRAM vectors, the R15
  guard, and the transaction state machine.

## T-150 jsonb type threading

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation |
|---|---|---|---|---|---|---|---|
| PG-T150-01 | jsonb value shown as SQL NULL or text | Silent wrong data | 9 | 2 | 2 | 36 | Superseded by PG-T160-1: `cql_to_value` now returns `Value::Jsonb`; the wire refusal moved to `encode_value`. |
| PG-T150-02 | jsonb INSERT coerced through a type mismatch or a guess | Wrong stored value | 8 | 2 | 2 | 32 | `value_to_cql` binds only `(Jsonb, Value::Jsonb)`; the `(Jsonb, _)` arm returns `0A000`. |
| PG-T160-1 | jsonb, jsonpath or `text[]` value reaches the wire before the T-161a codec and is sent as SQL NULL or as bytes a driver misdecodes | Silent wrong data | 9 | 2 | 2 | 36 | `encode_value` refuses these variants in both formats before `render_value`, whose deferred arm is unreachable through it. Tested in `pg_types` (`binary: false`) and `encode_value`. Removed by T-161a (PG-T161a-01). |
| PG-T160-2 | jsonb cell altered crossing the storage boundary (re-parse, scale loss) | Silent wrong data | 8 | 2 | 2 | 32 | Cell bytes move unchanged in both directions. `storage_provider_round_trips_a_jsonb_cell` asserts byte identity. |

| PG-T150-01 | jsonb value shown as SQL NULL or text | Silent wrong data | 9 | 2 | 2 | 36 | `cql_to_value` returns an error for `Jsonb`. Scans propagate it as a query error. |
| PG-T150-02 | jsonb INSERT coerced through a type mismatch or a guess | Wrong stored value | 8 | 2 | 2 | 32 | `value_to_cql` has an explicit `(Jsonb, _)` arm returning `0A000`. |

## T-154a jsonb key refusal

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation |
|---|---|---|---|---|---|---|---|
| PG-T154a-01 | `CREATE TABLE t (doc jsonb PRIMARY KEY)` accepted or answered with a vague error | jsonb in key bytes, or a client cannot tell why (D3) | 9 | 3 | 2 | 54 | `plan_create_table` refuses jsonb/json in any PRIMARY KEY column with `42P16` naming the column, ahead of the general `0A000` jsonb refusal. `tests/pg_ddl_create_table.rs`. |
| PG-T154a-02 | The registry's jsonb rules bypassed once the general jsonb refusal lifts (T-300) | jsonb in a key through PG | 9 | 2 | 2 | 36 | `execute_create_table` runs `Schema::check_create_table_jsonb` before the executor; `JsonbInKey` maps to `42P16`, other jsonb refusals to `0A000`. |

## T-161a jsonb on the wire

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation and test |
|----|--------------|--------|---|---|---|-----|---------------------|
| PG-T161a-01 | jsonb result sent as SQL NULL, text in binary format, or bytes a driver misdecodes | Silent wrong data | 9 | 2 | 2 | 36 | `render_value` returns `Result` and has no NULL arm for a value; binary is `0x01` + PgText. Tests: `jsonb_wire_renders_pg_text_and_binary`, `jsonb_wire_row_description_and_encode_value`, `pg_jsonb_wire_version_byte_rules`. Supersedes PG-T160-1. |
| PG-T161a-02 | Invalid or hostile jsonb input written as NULL, as text, or accepted past a limit | Corrupt row, memory pressure | 9 | 3 | 2 | 54 | `decode_param_jsonb` and `value_to_cql` parse with `parse_text_observed` under `QueryContext::jsonb_limits`; `22P02` with a byte offset (input never echoed), `XX000`/`08P01` for a bad/missing version byte (PostgreSQL 16's codes), `54000` over a limit; no row is written. Tests: `pg_invalid_jsonb_param_is_22p02_not_null`, `pg_jsonb_over_limit_input_is_54000_and_writes_nothing`, `jsonb_wire_input_errors_are_typed_and_never_echo`. |
| PG-T161a-03 | A corrupt stored cell reaches the client as NULL or truncated text | Silent wrong data | 9 | 2 | 3 | 54 | `render_text` validates the cell and prints under a fixed output budget: `XX001` for a reader fault, `54000` over budget; a corrupt cell in a scan fails the query. Tests: `jsonb_wire_print_errors_map_to_limit_and_corruption`, `pg_jsonb_corrupt_stored_cell_is_an_error_not_null`. **Residual:** the scan path reports `58000` until T-151 types the row-bridge error. |
| PG-T161a-04 | Limits substituted with a default in the PG front end | Operator-set limits silently ignored | 6 | 2 | 3 | 36 | `QueryContext::jsonb_limits` has no default; `main.rs` passes the resolved value. Constructors must name it (compile error otherwise). |
| PG-T161a-05 | Duplicate keys dropped silently (D6b) | Data differs from input with no trace | 5 | 3 | 3 | 45 | `EdgeLog` writes a `jsonb_duplicate_keys_dropped` warn line per document. Test: `pg_jsonb_literal_and_text_param_round_trip` (last-wins). **Residual:** no counter in this crate. |
| PG-T161a-06 | jsonb column missing from the catalog or reported with the wrong OID | Drivers pick the wrong codec | 6 | 2 | 2 | 24 | `pg_type_of_column` maps the column to 3802. Tests: `pg_type_and_pg_attribute_report_jsonb_3802`, `pg_jsonb_describe_reports_3802`. |
| PG-T161a-07 | jsonb DDL allowed in cluster mode before the D15a ledger exists | Cluster nodes disagree about the type | 7 | 1 | 2 | 14 | Mitigated (T-300, t_57fa8a9e): `Schema::check_create_table_jsonb` refuses jsonb DDL off a standalone node with `0A000` before the executor, and schema apply re-checks. Test: `pg_ddl_create_table_jsonb_refused_off_standalone`. |

## T-301 jsonb slice acceptance

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation and test |
|----|--------------|--------|---|---|---|-----|---------------------|
| PG-T301-01 | ferrosa prints jsonb differently from Postgres 16 (key order, scale, escapes, `-0`) | Clients and `psql` output differ from the reference | 7 | 3 | 2 | 42 | Byte-exact differential over an 80-document corpus on three input paths and two result formats: `differential_oracle_jsonb_corpus_agrees` (live) and `jsonb_slice_valid_corpus_round_trips_on_every_path_and_format` (no infra). |
| PG-T301-02 | A `\u0000` escape is stored (Postgres refuses it, `22P05`) | A value the PG text type cannot hold | 6 | 3 | 2 | 36 | `NulPolicy::Reject` in `ferrosa-jsonb` at the PG edge; `NulEscape` maps to `22P05`. Tests: `jsonb_parse_nul_escape_is_allowed_by_default_and_rejected_on_request`, corpus cases `NUL escape`. |
| PG-T301-03 | Bad or missing binary version byte answered with a different SQLSTATE than Postgres | Driver error handling keyed on SQLSTATE differs | 4 | 3 | 2 | 24 | Matches `XX000` and `08P01`. `differential_oracle_jsonb_bad_binary_version_agrees`, `jsonb_slice_bad_binary_version_byte_is_refused_and_writes_nothing`. Note `XX000` is PG's internal-error class; a friendlier code needs an owner decision. |
| PG-T301-04 | A refused write leaves a row, or an accepted write is not readable | Silent partial write | 8 | 2 | 2 | 32 | `run_one` fails the run if a refused write leaves a row or an accepted one has none; invalid corpus asserts zero rows. |
| PG-T301-05 | The oracle silently skips in CI or nightly | Slice loses its reference check | 6 | 2 | 3 | 36 | Live tests panic without `FERROSA_TEST_CONTAINERS=1`; `ci.yml` `postgres-oracle` runs the whole `differential_oracle` file; the `test` job and `nightly-fuzz.yml` skip `differential_oracle_jsonb` by name. |
