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
| PG-2 | **(resolved) `$N` params in DML** — parameterized `INSERT`/`UPDATE`/`DELETE` are now bound at `Bind` and substituted at `Execute` | — | 2 | 1 | 1 | 2 | **Done.** `substitute_param` fails loud `08P01` if a `$N` has no bound value; param OIDs inferred from each placeholder's target column. Covered by the `extended_parameterized_*` live tests. |
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
