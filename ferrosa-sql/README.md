# ferrosa-sql

> The bespoke relational query engine (parser + planner + physical operators)
> behind Ferrosa's Postgres front-end. **No DataFusion / Arrow** (decision **D3**)
> — it owns its own value model, three-valued NULL logic, and C-collation
> ordering.

## What this crate is

A self-contained relational engine for a **deliberately small SQL subset**: a
hand-written lexer + recursive-descent parser, a binder/planner that resolves
references against a `Catalog`, and a set of Volcano-style physical operators
(scan, filter, project, hash join, sort, hash aggregate, limit/offset). It was
written from scratch rather than embedding DataFusion (decision **D3**) so the
type model, NULL semantics, and row ordering are owned and auditable end-to-end.

`ferrosa-postgres` lowers each incoming SQL statement onto this engine. The
engine knows nothing about the Postgres wire protocol, transactions, or storage
framing — it operates over an abstract `TableProvider` / `Catalog`, backed by an
in-memory table in tests and by Ferrosa storage in production.

## What's implemented

**Parser** (`parse`, `parse_statement`):

- `SELECT [DISTINCT] <* | items> FROM t [alias] [INNER JOIN t2 [alias] ON a.x = b.y]
  [WHERE <bool-expr>] [GROUP BY ...] [HAVING <bool-expr>]
  [ORDER BY ... [ASC|DESC]] [LIMIT n] [OFFSET m]` — one inner equi-join only.
- No-`FROM` scalar selects: `SELECT 1`, `SELECT version()` (zero-arg func),
  `SELECT $1`, `SELECT TRUE`.
- DML (single-row, key-equality WHERE): `INSERT INTO t (cols) VALUES (...)`,
  `UPDATE t SET ... WHERE k = v [AND ...]`, `DELETE FROM t WHERE k = v [AND ...]`.
- Transaction / session statements **parsed** (not executed here): `BEGIN`/`START`,
  `COMMIT`/`END`, `ROLLBACK`/`ABORT`, `SET`, `RESET`.
- WHERE/HAVING boolean expressions: `AND` / `OR` / `NOT` with parentheses and
  the six comparison operators `= != <> < <= > >=`. RHS is a literal or `$N`.
- Aggregates: `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG`.
- Literals: int, float, string (with `''` escape), `TRUE`/`FALSE`/`NULL`, `$N`
  params, and typed literals `TIMESTAMP/DATE/TIME/INET/NUMERIC(=DECIMAL) '...'`.

**Value model** (`types`): `Null`, `Int(i64)`, `Text`, `Bool`,
`Float(OrderedFloat<f64>)`, `Uuid`, `Bytea`, `Timestamp(i64 µs)`, `Date(i32 days)`,
`Time(i64 µs)`, `Inet(IpAddr)`, `Numeric { unscaled: BigInt, scale }` (normalized,
arbitrary precision). `Value::sql_cmp` implements three-valued comparison (NULL or
type-mismatch or NaN ⇒ UNKNOWN), with Int↔Float promotion and value-aligned
numeric compare.

**Operators** (`exec`): `seq_scan`, `filter`, `project`, `hash_join` (inner
equi-join; NULL keys never match), `sort` (stable, multi-key, Postgres NULL
placement: ASC⇒NULLS LAST, DESC⇒NULLS FIRST), `hash_aggregate` (first-seen group
order; ungrouped-empty yields one row), `limit_offset`.

**Planner** (`plan`): `execute`, `describe` (RowDescription shape without running
operators), `infer_param_types` (extended-protocol ParameterDescription).
Fail-loud binding: unknown table/column, ambiguous unqualified column, unknown
qualifier, non-grouped column, aggregate-in-WHERE, invalid ORDER BY ordinal, and
missing `$N` parameter all return a typed `ExecError`.

## How it works

```
parse_statement ─▶ Statement (ast)
                     └─ Select(SelectStmt) ─▶ execute(stmt, catalog, schema, params)
                                                 │ resolve_scope (bind via Catalog)
                                                 │ seq_scan [→ hash_join] → filter
                                                 │ → simple project | hash_aggregate
                                                 │ → sort → limit_offset
                                                 ▼
                                              QueryResult { columns, rows }
```

## Public API (key entry points)

| Area | Items |
|------|-------|
| Parse | `parse`, `parse_statement`, `ParseError` |
| AST | `Statement`, `SelectStmt`, `InsertStmt`, `UpdateStmt`, `DeleteStmt`, `Expr`, `Operand`, `Term`, `Projection`, `SelectItem`, `OrderItem`, `ScalarItem`, `ScalarValue`, `AggArg` |
| Plan | `execute`, `describe`, `infer_param_types`, `QueryResult`, `ExecError` |
| Operators | `seq_scan`, `filter`, `project`, `hash_join`, `sort`, `hash_aggregate`, `limit_offset`, `Predicate`, `CmpOp`, `AggFunc`, `SortKey`, `SortDir`, `RowStream` |
| Catalog | `Catalog`, `MapCatalog`, `SharedTable`, `TableProvider`, `InMemoryTable` |
| Types | `Value`, `Row`, `Column`, `ColumnType`, `RelSchema` |

## Dependencies

**Calls** (ferrosa crates this depends on): **none.** `Cargo.toml` lists no
`ferrosa-*` path dependencies — not even `ferrosa-common`. The engine carries its
own `Value`/`Row`/`RelSchema` model so it stays a standalone leaf. External crates
only: `ordered-float` (total-order `f64` for join/group keys), `num-bigint`
(arbitrary-precision `Numeric`), `uuid`, `chrono` (std-only, typed temporal
literal parsing).

**Called by**: `ferrosa-postgres` — lowers parsed SQL onto this engine's
operators and serves results over the Postgres wire.

## Tests

In-crate unit tests (no `#[ignore]`, no live-infra): `exec.rs` (24), `parser.rs`
(44), `plan.rs` (42), `types.rs` (15), `catalog.rs` (2) — ~127 total. They cover
NULL/Kleene logic, sort NULL placement, aggregate edge cases, numeric
normalization, join key resolution, and binder fail-loud paths.

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, data flow
- [FMEA / known issues](specs/fmea.md) — supported surface vs gaps, ranked by RPN
- [Roadmap](specs/roadmap.md) — Now / Next / Later
