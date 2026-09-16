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
order; ungrouped-empty yields one row), `dedup` (DISTINCT, first-occurrence
order), `limit_offset`.

**Spill** (`spill`): the four BLOCKING operators — `sort`, `hash_aggregate`,
`hash_join` and `dedup` — cannot emit a first row before consuming their whole
input, so they **spill to disk** rather than buffering it (forge `t_50d99192`).
They reuse `ferrosa_storage::external_sort::ExternalSorter`, the same bounded
external merge sort behind `ferrosa-cql` and `ferrosa-graph`: accumulate to a
byte threshold, spill sorted runs, cascade-merge to a bounded fan-in, k-way merge
on finish, fail loud on any spill/merge I/O error.

- **Nothing caps a result.** The only knob is a resident-BYTES threshold — a work
  bound. A query that could be answered is never refused or truncated.
- **Temp location is configurable per node**: inject a `SpillReserver`, or set
  `FERROSA_SQL_TEMP_DIR`; the threshold follows the storage engine's
  `FERROSA_RANGE_SPILL_THRESHOLD_*`.
- **Cancellation and cleanup share one path**: the temp directory is held by a
  `TempSortTableReservation` moved into the output stream, so dropping the stream
  — exhausted, cancelled or abandoned — removes it. `sweep_orphaned_temp_dirs`
  reclaims what a killed process left behind, and runs once on first use.
- **Output order is unchanged.** Each sort-based operator restores the in-memory
  operator's order with a second sort on the arrival tag, so first-seen group
  order, DISTINCT first-occurrence order and the join's left-input order all
  survive. Grouping and DISTINCT sort under a type-aware total order consistent
  with `Value`'s structural `Eq`, never under `sql_cmp` (which would merge
  `Int(1)` with `Text("1")`).

**Planner** (`plan`): `execute`, `execute_with` (caller-supplied spill context),
`describe` (RowDescription shape without running
operators), `infer_param_types` (extended-protocol ParameterDescription).
Fail-loud binding: unknown table/column, ambiguous unqualified column, unknown
qualifier, non-grouped column, aggregate-in-WHERE, invalid ORDER BY ordinal, and
missing `$N` parameter all return a typed `ExecError`; a spill/merge I/O failure
returns `ExecError::Spill` (SQLSTATE `58030` on the wire) rather than a short
result.

## How it works

```
parse_statement ─▶ Statement (ast)
                     └─ Select(SelectStmt) ─▶ execute(stmt, catalog, schema, params)
                                                 │ resolve_scope (bind via Catalog)
                                                 │ seq_scan [→ hash_join*] → filter
                                                 │ → simple project | hash_aggregate*
                                                 │ → dedup* → sort* → limit_offset
                                                 ▼
                                              QueryResult { columns, rows }
```

`*` marks a blocking operator: it spills to disk past the context's byte
threshold and streams its output. Everything upstream of `QueryResult` is
bounded. The `rows` `Vec` itself is not — the Postgres front end's
`render_result` builds every `DataRow` before writing any, so handing it a stream
would relocate the buffer rather than remove it. Streaming the result to the wire
is front-end work, tracked separately.

## Public API (key entry points)

| Area | Items |
|------|-------|
| Parse | `parse`, `parse_statement`, `ParseError` |
| AST | `Statement`, `SelectStmt`, `InsertStmt`, `UpdateStmt`, `DeleteStmt`, `Expr`, `Operand`, `Term`, `Projection`, `SelectItem`, `OrderItem`, `ScalarItem`, `ScalarValue`, `AggArg` |
| Plan | `execute`, `execute_with`, `describe`, `infer_param_types`, `QueryResult`, `ExecError` |
| Operators | `seq_scan`, `filter`, `project`, `hash_join`, `sort`, `hash_aggregate`, `dedup`, `limit_offset`, `fallible`, `try_filter`, `try_project`, `Predicate`, `CmpOp`, `AggFunc`, `SortKey`, `SortDir`, `RowStream`, `TryRowStream` |
| Spill | `SpillCtx`, `SpillReserver`, `DirReserver`, `SpillStats`, `SpillError`, `default_temp_root`, `sweep_orphaned_temp_dirs` |
| Catalog | `Catalog`, `MapCatalog`, `SharedTable`, `TableProvider`, `InMemoryTable` |
| Types | `Value`, `Row`, `Column`, `ColumnType`, `RelSchema` |

## Dependencies

**Calls** (ferrosa crates this depends on): `ferrosa-storage` only, for the
bounded external merge sort and the cancellable temp-table reservation the
blocking operators spill through. Nothing else from it is used, and the engine
still carries its own `Value`/`Row`/`RelSchema` model — it does not adopt
`CqlValue`.

This is a deliberate change from the crate's original standalone-leaf position
(forge `t_50d99192`): reimplementing an external merge sort here to preserve the
leaf status would have duplicated machinery `ferrosa-cql` and `ferrosa-graph`
already share, and duplicated its failure handling with it.

External crates: `ordered-float` (total-order `f64` for join/group keys),
`num-bigint` (arbitrary-precision `Numeric`), `uuid`, `chrono` (std-only, typed
temporal literal parsing), `serde` + `serde_json` (spilled runs are
length-prefixed JSON records), `tracing`.

**Called by**: `ferrosa-postgres` — lowers parsed SQL onto this engine's
operators and serves results over the Postgres wire.

## Tests

In-crate unit tests (no `#[ignore]`, no live-infra): `exec.rs`, `parser.rs`,
`plan.rs`, `types.rs`, `catalog.rs`, `spill.rs` — 144 total. They cover
NULL/Kleene logic, sort NULL placement, aggregate edge cases, numeric
normalization, join key resolution, binder fail-loud paths, and the spill
module's orders, replay buffer and orphan sweep.

`tests/spill_operators.rs` (10 tests) holds the per-operator spill invariant:
given an input larger than the threshold, the operator returns EVERY row, peak
resident rows stay an order of magnitude below the rows processed, output order
is unchanged, a dropped stream removes its temp directory, and a reservation
failure is loud.

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, data flow
- [FMEA / known issues](specs/fmea.md) — supported surface vs gaps, ranked by RPN
- [Roadmap](specs/roadmap.md) — Now / Next / Later
