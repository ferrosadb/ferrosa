---
crate: ferrosa-postgres
doc: fmea
last_updated: 2026-06-19
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
  `Sync`/`Close`); `SELECT`-only prepared statements; text + binary param/result
  formats; `$N` type inference for `SELECT`.
- **Auth:** SCRAM-SHA-256 against the live schema role store.
- **Catalog:** `pg_catalog` namespace/class/attribute/type projection.

## Not yet supported (fail-loud gaps)

- `$N` parameters in `INSERT`/`UPDATE`/`DELETE` (DML is literal-only) → `0A000`.
- `RETURNING`, `ON CONFLICT` (upsert), multi-row `INSERT ... VALUES (...),(...)`.
- `UPDATE`/`DELETE` with a non-key or range `WHERE` (only full-PK equality).
- Real transaction **atomicity** — `BEGIN`/`COMMIT`/`ROLLBACK` drive protocol
  state but writes are not yet routed through Accord.
- `SET`/`RESET` session GUCs (simple-query path returns `0A000`).
- Function calls in DML `VALUES`; most scalar functions beyond
  `version()`/`current_database()`/`current_schema()`.
- Preparing non-`SELECT` statements via the extended protocol.
- TLS (`SSLRequest` is declined with `N`); query cancellation (`BackendKeyData`
  is a `(0,0)` placeholder).
- Binary `numeric` result/param (falls back to text).
- CQL `Duration` + collections (`List`/`Set`/`Map`/`Tuple`/`Udt`/`Vector`) read
  as NULL.

## Failure modes

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| PG-1 | **Transaction atomicity is not real** — `BEGIN`/`COMMIT` move the `I`/`T`/`E` status but the commit seam has an empty write-set; DML inside a block applies immediately and is not rolled back by `ROLLBACK` | A driver in a transaction sees PG-shaped status bytes but writes are NOT atomic/isolated — partial visibility / no rollback (silent correctness gap if a client relies on it) | 9 | 5 | 7 | 315 | **Open, top gap.** Accord commit seam is wired (`execute_simple`, blueprint D11) but writes are not yet accumulated/committed through Accord. Simple `SELECT 1` txn path is exercised; DML-in-txn atomicity is NOT. Document loudly as preview. |
| PG-2 | **`$N` params unsupported in DML** — `INSERT`/`UPDATE`/`DELETE` with a `ScalarValue::Param` fail loud `0A000` | Every parameterized write from a real driver (the common path) is rejected; only literal DML works | 7 | 8 | 2 | 112 | Fail-loud `0A000` (no silent wrong write). `SELECT` params DO work via the extended path; extending inference to DML is roadmap Now. |
| PG-3 | **No `RETURNING` / `ON CONFLICT`** — parser/executor do not accept them | ORMs/`INSERT ... RETURNING id` patterns fail | 6 | 7 | 2 | 84 | Fail-loud at parse (`42601`) — never a wrong row. Roadmap Next. |
| PG-4 | **Lossy CQL→Value: `Duration`/collections read as NULL** | A `SELECT` over a table with a duration/list/map column silently shows NULL for that column | 6 | 4 | 6 | 144 | Documented in `storage_provider::cql_to_value` (code comment, never a panic). Detectable only by inspection — no error is raised. Widen `Value` (roadmap Later). |
| PG-5 | **Row-codec divergence from CQL/engine** — a write encodes differently than the canonical codec | Postgres-written rows read back wrong/invisible over CQL or the engine (silent corruption) | 10 | 1 | 4 | 40 | **Structural (D10):** INSERT/UPDATE/DELETE use `ferrosa-row-bridge` `build_row`/`build_delete_row`/`build_decorated_key` — the SAME code CQL uses. Reinforced by the differential oracle (PG vs real PG) + the M1 live tests. |
| PG-6 | **Missing table served as empty relation** | A typo'd table silently returns zero rows instead of erroring | 8 | 1 | 3 | 24 | **R15 guard:** `load_table` checks schema metadata first → `NoSuchTable` (`42P01`), distinct from an existing empty table. Covered by `load_table_missing_table_is_no_such_table`. |
| PG-7 | **Binary `numeric` out of scope** — a client requesting binary results for a numeric column gets text bytes under a numeric OID | A strict binary-only driver could mis-decode a numeric column | 5 | 2 | 5 | 50 | Documented fallback in `encode_value`/`decode_param_binary`; the oracle uses text (`simple_query`) so the gate never exercises it. Implement binary numeric (roadmap Next). |
| PG-8 | **No TLS / no query cancel** — `SSLRequest` declined; `BackendKeyData` is `(0,0)` | Cleartext-only on the wire; `CancelRequest` closes the connection but cannot target a running query | 6 | 3 | 2 | 36 | Declined explicitly (`N`), not faked. Wire TLS + a real cancel key (roadmap). Threat-model note: `UnknownRole` is a user-enumeration oracle (run dummy verifier — follow-up). |
| PG-9 | **Float/numeric text-format parity with Postgres** — floats use Rust `{}` shortest-form | A benign formatting difference vs PG (`1.5` vs `1.5000…`) | 3 | 4 | 4 | 48 | The differential oracle compares `f64`-parseable cells numerically with tolerance, so this is not a false alarm; exact text parity is follow-up. |
| PG-10 | **UPDATE/DELETE report `1` row unconditionally** — Cassandra blind upsert/tombstone has no match count | A driver reading the affected-row count sees `1` even when no matching row existed | 4 | 5 | 5 | 100 | Documented Cassandra semantics (`execute_update`/`execute_delete`). Differs from PG's real match count; surface in docs / revisit with read-before-write. |

## Top risks to act on

1. **PG-1 (RPN 315)** — transaction atomicity is the single most consequential
   gap: the protocol *looks* transactional but writes are not yet Accord-backed.
   Either wire the Accord commit seam or document the preview limitation
   prominently so no client relies on rollback/isolation.
2. **PG-4 (RPN 144)** — lossy-NULL for `Duration`/collections is undetectable at
   runtime (no error). Make these fail loud where queried, or widen `Value`.
3. **PG-2 (RPN 112)** — `$N` params in DML block the most common write path from
   real drivers; extend the extended-protocol inference to DML.

## Detection assets

- `tests/differential_oracle.rs` — corpus + DML vs real PostgreSQL 16 (gated on
  `live-infra-tests` + `FERROSA_TEST_CONTAINERS=1`).
- `tests/m1_join_live.rs`, `tests/scram_live.rs` — full-stack over `tokio-postgres`,
  no external infra.
- ~111 in-crate unit tests for codecs, SQLSTATE mapping, SCRAM vectors, the R15
  guard, and the transaction state machine.
