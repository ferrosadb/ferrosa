---
crate: ferrosa-postgres
doc: fmea
last_updated: 2026-06-25
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
- **Read-your-writes inside an open transaction block** — buffered writes are not
  visible to in-transaction reads (the buffer is applied only at `COMMIT`).
  Transactional DML itself IS supported now (buffered + committed via Accord; see
  PG-1) over both the simple and extended protocols.
- `SET`/`RESET` session GUCs (simple-query path returns `0A000`).
- Function calls in DML `VALUES`; most scalar functions beyond
  `version()`/`current_database()`/`current_schema()`.
- TLS (`SSLRequest` is declined with `N`); query cancellation (`BackendKeyData`
  is a `(0,0)` placeholder).
- Binary `numeric` result/param (falls back to text).
- CQL `Duration` + collections (`List`/`Set`/`Map`/`Tuple`/`Udt`/`Vector`) read
  as NULL.

## Failure modes

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| PG-1 | **Transaction atomicity** — `BEGIN`/`COMMIT`/`ROLLBACK` over an Accord-committed buffered write-set | A driver in a transaction gets real atomic multi-key commit, or fail-loud — never partial/faked | 9 | 2 | 2 | 36 | **Implemented.** DML inside an open `T` block BUFFERS as a `ferrosa_storage::accord::TransactionWrite` (`apply_or_buffer` in `query.rs`), over BOTH the simple and extended protocols; `COMMIT` drives the whole write-set through the injected `TransactionCommitter` atomically (`commit_txn` in `server.rs`), `ROLLBACK`/`end_txn` discards the buffer (never applied). No committer (standalone) ⇒ COMMIT of a non-empty buffer fails loud (`0A000`); empty buffer commits cleanly. Write-set capped at `MAX_TXN_WRITES`. Autocommit unchanged. Mirrors the CQL `CqlTransaction` path. Tests: `server::txn_atomicity_tests`, `m1_join_live` extended-DML-in-txn. Residual: read-your-writes / MVCC isolation. |
| PG-2 | **(resolved) `$N` params in DML** — parameterized `INSERT`/`UPDATE`/`DELETE` are now bound at `Bind` and substituted at `Execute` | — | 2 | 1 | 1 | 2 | **Done.** `substitute_param` fails loud `08P01` if a `$N` has no bound value; param OIDs inferred from each placeholder's target column. Covered by the `extended_parameterized_*` live tests. |
| PG-3 | **`INSERT … RETURNING` only; `UPDATE`/`DELETE … RETURNING` + `ON CONFLICT` unsupported** | `UPDATE`/`DELETE … RETURNING` and upsert ORM patterns fail | 5 | 5 | 2 | 50 | `INSERT … RETURNING` done (in-memory row, no read-back). `UPDATE`/`DELETE … RETURNING` fail loud `0A000`; `ON CONFLICT` fails at parse. Never a wrong row. Roadmap Next. |
| PG-4 | **Lossy CQL→Value: `Duration`/collections read as NULL** | A `SELECT` over a table with a duration/list/map column silently shows NULL for that column | 6 | 4 | 6 | 144 | Documented in `storage_provider::cql_to_value` (code comment, never a panic). Detectable only by inspection — no error is raised. Widen `Value` (roadmap Later). |
| PG-5 | **Row-codec divergence from CQL/engine** — a write encodes differently than the canonical codec | Postgres-written rows read back wrong/invisible over CQL or the engine (silent corruption) | 10 | 1 | 4 | 40 | **Structural (D10):** INSERT/UPDATE/DELETE use `ferrosa-row-bridge` `build_row`/`build_delete_row`/`build_decorated_key` — the SAME code CQL uses. Reinforced by the differential oracle (PG vs real PG) + the M1 live tests. |
| PG-6 | **Missing table served as empty relation** | A typo'd table silently returns zero rows instead of erroring | 8 | 1 | 3 | 24 | **R15 guard:** `load_table` checks schema metadata first → `NoSuchTable` (`42P01`), distinct from an existing empty table. Covered by `load_table_missing_table_is_no_such_table`. |
| PG-7 | **Binary `numeric` out of scope** — a client requesting binary results for a numeric column gets text bytes under a numeric OID | A strict binary-only driver could mis-decode a numeric column | 5 | 2 | 5 | 50 | Documented fallback in `encode_value`/`decode_param_binary`; the oracle uses text (`simple_query`) so the gate never exercises it. Implement binary numeric (roadmap Next). |
| PG-8 | **No TLS / no query cancel** — `SSLRequest` declined; `BackendKeyData` is `(0,0)` | Cleartext-only on the wire; `CancelRequest` closes the connection but cannot target a running query | 6 | 3 | 2 | 36 | Declined explicitly (`N`), not faked. Wire TLS + a real cancel key (roadmap). Threat-model note: `UnknownRole` is a user-enumeration oracle (run dummy verifier — follow-up). |
| PG-9 | **Float/numeric text-format parity with Postgres** — floats use Rust `{}` shortest-form | A benign formatting difference vs PG (`1.5` vs `1.5000…`) | 3 | 4 | 4 | 48 | The differential oracle compares `f64`-parseable cells numerically with tolerance, so this is not a false alarm; exact text parity is follow-up. |
| PG-10 | **UPDATE/DELETE report `1` row unconditionally** — Cassandra blind upsert/tombstone has no match count | A driver reading the affected-row count sees `1` even when no matching row existed | 4 | 5 | 5 | 100 | Documented Cassandra semantics (`execute_update`/`execute_delete`). Differs from PG's real match count; surface in docs / revisit with read-before-write. |

## Top risks to act on

1. **PG-4 (RPN 144)** — lossy-NULL for `Duration`/collections is undetectable at
   runtime (no error). Make these fail loud where queried, or widen `Value`.
2. **PG-10 (RPN 100)** — UPDATE/DELETE report `1` row unconditionally (Cassandra
   blind upsert/tombstone); differs from PG's real match count.
3. ~~**PG-1**~~ **RESOLVED** — transactional DML now buffers and commits
   atomically through Accord (`commit_txn`); `ROLLBACK` discards the buffer;
   no-committer COMMIT of a non-empty buffer fails loud. Re-scored to RPN 36
   (residual: read-your-writes / real isolation-MVCC semantics beyond atomic
   multi-key apply are still future work).

## Detection assets

- `tests/differential_oracle.rs` — corpus + DML vs real PostgreSQL 16 (gated on
  `live-infra-tests` + `FERROSA_TEST_CONTAINERS=1`).
- `tests/m1_join_live.rs`, `tests/scram_live.rs` — full-stack over
  `tokio-postgres`, no external infra. The DML subset covers parameterized
  `INSERT`/`UPDATE`/`DELETE`, `INSERT … RETURNING id`/`*`, `UPDATE`/`DELETE …
  RETURNING` fail-loud, and extended-protocol DML inside a transaction
  (BEGIN/INSERT RETURNING/ROLLBACK discards; BEGIN/INSERT/COMMIT applies via a
  mock committer).
- `server::txn_atomicity_tests` — simple-protocol BEGIN/COMMIT/ROLLBACK
  atomicity over a real local engine + `MockTransactionCommitter`.
- in-crate unit tests for codecs, SQLSTATE mapping, SCRAM vectors, the R15
  guard, and the transaction state machine.
