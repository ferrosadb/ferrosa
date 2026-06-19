---
crate: ferrosa-postgres
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-postgres — Roadmap

Sourced from in-code fail-loud `0A000`/preview gaps, the FMEA
([fmea.md](fmea.md)), and the dependency/usage review. There are no `TODO`/
`FIXME` markers in the source — the open work is encoded as fail-loud
`feature_not_supported` paths and documented lossy fallbacks instead.

## Now (highest value)

- **Accord-backed transaction atomicity** (FMEA PG-1). Accumulate a `T`-block's
  writes and commit them through Accord at `COMMIT` (the seam in
  `execute_simple` / blueprint D11), with real `ROLLBACK`. Until then, document
  the preview limitation prominently so no client relies on isolation/rollback.
- **`$N` parameters in DML** (FMEA PG-2). Extend the extended-protocol path so
  `INSERT`/`UPDATE`/`DELETE` accept bound parameters (today literal-only,
  `0A000`). Reuse `decode_param` + the existing `$N` inference.

## Next

- **`RETURNING` and `ON CONFLICT`** (FMEA PG-3) — the common ORM insert idioms.
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
