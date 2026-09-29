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
| PG-Tf348ba0b | **Result streaming: a mid-stream failure, a suspended portal, or a vanished client.** Rows now reach the socket as the executor yields them, so (a) an error can occur after rows were sent, (b) a portal suspended by `max_rows` keeps a running query and one blocking thread parked in a channel send, (c) the client can disappear mid-stream | (a) a truncated result reported as success; (b) leaked executor threads exhausting `max_blocking_threads`; (c) an executor scanning a table nobody reads | 8 | 3 | 3 | 72 | **Mitigated.** (a) `ErrorResponse` after the rows already sent, never `CommandComplete` (`result_stream::tests::a_mid_stream_error_follows_the_rows_it_interrupts`); a storage error outranks an executor error and turns a clean end-of-stream into a failure. (b) `Close`, rebind, `Sync` outside a block and session drop release the query (`extended::tests::*_releases_a_suspended_portals_query`, `sync_releases_suspended_portals_only_outside_a_transaction`); the parked-thread cost is documented and remains a **residual** with no per-connection cap on suspended portals. (c) A failed socket write ends the connection and drops the stream, and the executor stops on its next send (`result_stream::tests::dropping_the_stream_stops_the_producer`). Bound proven end to end: `tests/pg_streaming_results.rs` (12 MiB result under a 4 MiB live-heap budget; `max_rows` resume without gap or duplicate). |
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
| PG-T132a-03 | **Guessed or faked type** — an unmapped type stored as text, or a jsonb column stored as text | Silent wrong type, later data corruption | 8 | 3 | 3 | 72 → 8 | **Mitigated:** unmapped type is `42704` naming it; `json`/`jsonb` is `0A000` (no engine type until T-150); nothing is created. Tests: same key-mapping test, `pg_ddl_create_table_jsonb_is_refused_until_the_engine_type_exists`. **Residual:** `varchar(n)`/`numeric(p,s)` limits are dropped, not enforced. |
| PG-T132a-04 | **Refusals collapse to a syntax error** — every parse error was `42601` | Clients cannot tell a refused feature from a typo | 4 | 5 | 3 | 60 → 12 | **Fixed:** `parse_error_sqlstate` (exhaustive). Test: `pg_ddl_create_table_named_refusals_survive_end_to_end`. |
| PG-T132a-05 | **Duplicate / concurrent / transactional DDL reported as success or a generic error** | A CREATE that created nothing looks done; DDL silently non-transactional in a BEGIN block | 6 | 3 | 3 | 54 → 9 | **Mitigated:** `42P07` unless `IF NOT EXISTS`; a lost race re-checks the registry; `25001` inside a transaction block; `3F000` for a missing keyspace. Tests: key-mapping test, `pg_ddl_create_table_inside_a_transaction_block_is_refused`, `pg_ddl_create_table_without_the_keyspace_is_3f000`. **Residual:** no NOTICE; the DDL permission check is an explicit permit at `ddl::authorize_create_table` until PR #465. |
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
| PG-T160-1 | jsonb, jsonpath or `text[]` value reaches the wire before the T-161a codec and is sent as SQL NULL or as bytes a driver misdecodes | Silent wrong data | 9 | 2 | 2 | 36 | `encode_value` refuses these variants in both formats before `render_value`, whose deferred arm is unreachable through it. Tested in `pg_types` (`binary: false`) and `encode_value`. Removed by T-161a. |
| PG-T160-2 | jsonb cell altered crossing the storage boundary (re-parse, scale loss) | Silent wrong data | 8 | 2 | 2 | 32 | Cell bytes move unchanged in both directions. `storage_provider_round_trips_a_jsonb_cell` asserts byte identity. |
