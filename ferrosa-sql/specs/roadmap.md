---
crate: ferrosa-sql
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-sql — Roadmap

Sourced from the supported-subset review, the FMEA gaps ([fmea.md](fmea.md)), and
the `ferrosa-postgres` consumer needs. The engine is intentionally an M1 slice
(D3, bespoke, no DataFusion); the roadmap is mostly *widening the SQL surface*
toward the Postgres queries real clients send.

## Now (highest value)

- **`IS NULL` / `IS NOT NULL`** (FMEA SQL-1). Add the `IS`/`NOT NULL` tokens and
  grammar plus an `IsNull` predicate path. Today NULL filtering is impossible in
  SQL because `col = NULL` is UNKNOWN under Kleene logic. Top gap.
- **Implicit string → typed coercion in predicates** (FMEA SQL-8). Coerce a
  `Text` RHS to the column's type (`Date`/`Timestamp`/`Numeric`/`Inet`) at bind
  time so naturally-written `where d = '2024-01-01'` works, not only
  `DATE '2024-01-01'`. Fail loud on an uncoercible body.
- **Integer SUM/AVG overflow safety + `Numeric` aggregation** (FMEA SQL-6).
  Promote integer running sums to `BigInt`/checked add and feed `Numeric`
  columns through `add_numeric`.

## Next

- **Richer joins** (FMEA SQL-3): `LEFT`/`RIGHT`/`FULL` outer joins, a multi-table
  FROM / join list, and `ON` predicates beyond a single `a = b` (AND-of-equalities,
  inequality join conditions).
- **Scalar expressions** (FMEA SQL-4): an expression evaluator over `Value` for
  projection arithmetic/functions (`a + b`, `UPPER(x)`, `||`, `CASE`) and
  expression predicates (LHS/RHS richer than column-vs-literal).
- **Set-based DML** (FMEA SQL-7): multi-row `INSERT ... VALUES`, range/`IN`
  predicates in `UPDATE`/`DELETE` WHERE.
- **Bounded operators** (FMEA SQL-5): cap or spill `hash_join` build side, `sort`,
  and `hash_aggregate` input to satisfy Power-of-10 rule 3; lean on
  predicate/projection pushdown into the storage-backed provider.

## Later

- **Subqueries, CTEs (`WITH`), `UNION`/`INTERSECT`/`EXCEPT`, window functions**
  (FMEA SQL-2) — each a separate large effort; sequence by `ferrosa-postgres`
  client demand.
- **Collections / UDT / tuple / vector value types** (FMEA SQL-10) once a
  consuming query needs them over this path.
- **Property-test the round-trip and Kleene tables** as a regression net
  independent of `ferrosa-postgres`.

## Non-goals

- Postgres wire framing, transaction execution (Accord), and storage/S3 framing —
  those belong to `ferrosa-postgres` and the storage layer, not here.
- Embedding DataFusion / Arrow (decision **D3**) — the bespoke value model and
  semantics stay owned in this crate.
