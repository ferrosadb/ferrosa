---
type: feature
priority: P3
status: open
created: 2026-05-31
context: "Remaining Cassandra CQL example-corpus parse gaps after the auth/duration/role/LIST gap-fill. Each requires a new foundational subsystem; tracked here for triage."
---

# CQL corpus gap-fill — remaining foundation-dependent features

## Status

The Cassandra example corpus (`ferrosa-cql/tests/cassandra_cql_examples.rs`,
201 files) is at **~92% parse coverage**. The tractable, no-new-foundation gaps
are done (auth: roles/HASHED PASSWORD/OPTIONS, GRANT/REVOKE ON TABLE +
multi-perm, ALTER USER, bare SUPERUSER, GRANT/REVOKE role with additive
replication, LIST PERMISSIONS; query: LIKE, ALTER KEYSPACE, ALTER TABLE
RENAME/ALTER TYPE, duration literals + temporal arithmetic, qualified function
calls; transient-replication rejection; Java→WASM UDF example rewrite).

Everything still failing needs a **new foundational subsystem** (or is a test
fixture artifact). None are quick parser wins.

## Remaining real gaps (each its own effort/PR)

| Gap | Foundation required | ~count |
|-----|--------------------|--------|
| Collection + custom/SAI indexes — `CREATE INDEX … (KEYS(m)/VALUES(m)/ENTRIES(m))`, `CREATE CUSTOM INDEX … USING 'SAI'` with analyzers | secondary-index engine extensions | ~18 |
| UDF/UDA code bodies — `LANGUAGE <lang> AS $$…$$` | inline language→WASM compiler — see `feature-inline-language-to-wasm-udf.md` | ~10 |
| JSON I/O — `INSERT … JSON '{…}'` (reverse JSON→typed-value codec; forward `toJson` already exists), `SELECT JSON` (result-encoding chokepoint refactor — `encode_rows` is called from 12+ scattered sites) | JSON codecs / result-path refactor | ~7 |
| Materialized views — `CREATE MATERIALIZED VIEW … AS SELECT …` | MV maintenance engine | 5 |
| Dynamic data masking — `… MASKED WITH f()`, `RESTRICT ROWS`, `DROP MASKED`, `UNMASK`/`SELECT_MASKED` permissions | column-masking engine + masked-permission enforcement | ~4 |
| Expression evaluation inside aggregates — `SELECT AVG(CAST(x AS float))`, `SUM(expr)` | per-row expression evaluator in the aggregate path + `CAST` type coercion | ~3 |
| Triggers — `CREATE/DROP TRIGGER … USING '<class>'` | **recommend won't-implement** (Cassandra-specific server-side trigger classes) | 2 |

## Not real gaps (~14, fixture artifacts — no action)

- Multi-statement `BEGIN BATCH … APPLY BATCH` chopped on `;` by the corpus
  statement splitter (BATCH, incl. `COUNTER BATCH`, parses fine).
- Literal `...` ellipsis placeholders and end-of-line `--` comments after `;`.
- UDF body fragments leaking past the non-CQL filter.

The realistic parse-coverage ceiling without a new subsystem is ~92%; the
fixture artifacts are not ferrosa bugs.

## Closest-to-tractable next pick

`INSERT … JSON` — only the **reverse** JSON→typed-value codec is missing (the
forward `cql_value_to_json` exists); it is bounded and self-contained relative
to the others.
