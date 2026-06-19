---
title: Postgres Differential Oracle — soundness model & coverage
status: implemented
executive_summary: >
  The Postgres front-end is cross-checked against a real PostgreSQL 16 by a
  differential oracle: the same data + the same SQL are run against both, and
  the result sets must agree. This document records the oracle's soundness model
  (the R9/R10/R11 controls), its coverage, and how it is wired into CI. The
  oracle is re-enabled as a dedicated CI job (`postgres-oracle`).
risks: [R9, R10, R11]
---

# Postgres Differential Oracle

Test: `ferrosa-postgres/tests/differential_oracle.rs` (feature `live-infra-tests`).
CI: the `postgres-oracle` job in `.github/workflows/ci.yml` runs it against a real
`postgres:16` with `FERROSA_TEST_CONTAINERS=1`.

## What it checks

Three tests, all over the same source-of-truth dataset materialized identically
into PostgreSQL and ferrosa (so the datasets cannot drift):

1. **`differential_oracle_corpus_agrees`** — a fixed corpus of `SELECT` queries
   (WHERE eq/neq/range/AND/OR/NOT, JOIN, GROUP BY + COUNT/SUM/MIN/MAX/AVG,
   HAVING, DISTINCT, ORDER BY incl. DESC, LIMIT/OFFSET, temporal/numeric/inet
   types, empty-aggregate and ordering edges). Each result set must equal
   PostgreSQL's.
2. **`differential_oracle_rejects_unsupported_queries`** — the restricted-query
   oracle: queries outside the M1 grammar (subquery, `WITH`, `UNION`, window
   function, unknown table/column, `IS [NOT] NULL`) must surface a **clean driver
   error**, never silently-wrong rows.
3. **`differential_oracle_dml_agrees`** — the same `INSERT`/`UPDATE`/`DELETE`
   (incl. `NULL` values and `SET col = NULL`) applied over the wire to both
   sides, with a `SELECT` agreeing after each mutation — exercising the real
   write executors against PostgreSQL semantics.

## Soundness controls

- **Three verdicts (R10).** Each corpus row is `Match`, `Mismatch`
  (the fail-loud signal), or `OutOfScope` (ferrosa errored / a query this corpus
  marks unsupported) — never a silent pass.
- **Sound canonicalizer (R10).** Cell comparison is exact text equality first
  (covers int/text/uuid/inet/date with **no rounding**); a numeric tolerance is
  applied **only** when at least one side is float/scientific-formatted, to
  absorb the genuine `float8`-vs-`numeric` text gap (`1.5` vs
  `1.5000000000000000`). Two integers are never fuzzily matched; text is never
  rounded.
- **Restricted-query oracle (R9).** Unsupported grammar fails loud — the
  rejection test proves ferrosa errors rather than returning unproven rows.
- **Collation contract (R10).** v1 is **`COLLATE "C"` only**: text orders by raw
  byte order. The corpus uses C-collation-safe ASCII and deterministic numeric
  `ORDER BY`; the contract is pinned by
  `ferrosa-sql ... text_order_by_is_c_collation_byte_order`.
- **NULL / three-valued logic (R11).** A self-contained, container-free
  known-answer corpus (`ferrosa-sql ... null_3vl_known_answer_corpus`) asserts
  =/!=/>= against NULL are UNKNOWN→excluded and AND/OR/NOT follow Kleene logic,
  independent of PostgreSQL.

## Running it

```text
FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-postgres \
  --features live-infra-tests --test differential_oracle -- --nocapture
```

Default `cargo test` (feature off) compiles this file to nothing; the dedicated
CI job supplies the container runtime.

## Known gaps (tracked)

- `IS [NOT] NULL` is rejected, not executed — implement parser + 3VL exec, then
  move the cases into the corpus.
- Corpus type breadth (bool / uuid / bytea round-trip) is a future expansion.
