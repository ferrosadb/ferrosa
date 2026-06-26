---
crate: ferrosa-postgres
doc: roadmap
last_updated: 2026-06-25
---

# ferrosa-postgres — Roadmap

Sourced from in-code fail-loud `0A000`/preview gaps, the FMEA
([fmea.md](fmea.md)), and the dependency/usage review. There are no `TODO`/
`FIXME` markers in the source — the open work is encoded as fail-loud
`feature_not_supported` paths and documented lossy fallbacks instead.

## Done (recent)

- **Accord-backed transaction atomicity** (FMEA PG-1, #206). DML in a `T` block
  buffers as a `TransactionWrite` (`apply_or_buffer`) — over BOTH the simple and
  extended protocols — and `COMMIT` drives the write-set through the injected
  `TransactionCommitter` atomically (`commit_txn`); `ROLLBACK` discards the buffer
  (never applied). No committer (standalone) ⇒ COMMIT of a non-empty buffer fails
  loud; empty buffer commits cleanly. Mirrors the CQL `CqlTransaction` path.
- **Parameterized DML** (was FMEA PG-2, `feat/pg-extended-crud`).
  `INSERT`/`UPDATE`/`DELETE` accept bound `$N` parameters over the extended
  protocol: prepared as `PreparedKind::{Insert,Update,Delete}`, substituted at
  `Execute` via `substitute_param` (fail-loud `08P01` on an unbound `$N`), with
  param OIDs inferred from each placeholder's target column. Transactional
  parameterized DML buffers into the same session write-set.
- **`INSERT … RETURNING col,…`/`*`** (part of FMEA PG-3, `feat/pg-extended-crud`).
  Echoes the just-written values as a `DataRow` (built in-memory, no storage
  read-back) — the `Ecto.Repo.insert` generated-key path; works inside a
  transaction (rows returned now, write commits at COMMIT).

## Now (highest value)

- **Read-your-writes inside an open transaction** (FMEA PG-1 residual). Buffered
  writes are not yet visible to reads in the same transaction; wire the buffer
  into the in-transaction read path (or document the snapshot-isolation gap).

## Next

- **`UPDATE`/`DELETE … RETURNING`** (FMEA PG-3) — today fail loud `0A000`; only
  `INSERT … RETURNING` is wired.
- **`ON CONFLICT` (upsert)** — today a parse error; the common ORM upsert idiom.
- **`= ANY($N)` / IN-list parameter expansion** — Ecto `where: x in ^ids`.
- **Multi-row `INSERT ... VALUES`** and richer `UPDATE`/`DELETE` `WHERE`
  (range/non-key predicates), which today are restricted to single-row, full-PK
  equality.
- **Binary `numeric`** result/param encoding (FMEA PG-7), removing the
  text-bytes fallback.
- **TLS on the wire + real query cancellation** (FMEA PG-8) — handle
  `SSLRequest` instead of declining; mint a real `BackendKeyData` cancel key.
- **Harden the SCRAM unknown-role oracle** — run the exchange against a dummy
  verifier so `UnknownRole` is not a user-enumeration signal (threat-model note
  in `handshake.rs`).

## Later

- **CQL `Duration` + collections** (`List`/`Set`/`Map`/`Tuple`/`Udt`/`Vector`)
  support (FMEA PG-4) — widen `ferrosa_sql::Value` and the
  `cql_to_value`/`value_to_cql` bridges, or fail loud where queried instead of
  reading NULL.
- **Exact float/numeric text-format parity** with Postgres (FMEA PG-9).
- **Real affected-row counts** for `UPDATE`/`DELETE` (FMEA PG-10) — read-before-
  write so the count reflects matches rather than always reporting `1`.
- **Session GUCs** (`SET`/`RESET`) and a broader scalar-function surface
  (`now()`, etc.).
- **More `pg_catalog`/`information_schema` coverage** as drivers/ORMs demand it.

## Non-goals

- Query planning / binding / relational operators — those live in `ferrosa-sql`.
- The storage row encoding — that is `ferrosa-row-bridge` (shared with CQL, D10).
- Cassandra wire compatibility — that is the CQL front-end (`ferrosa-cql`).
