# Project Plan — RRD Materialized Rollups and WASM Time-Series UDFs

> Last updated: 2026-05-20
> Status: Draft execution plan

## Goal

Ship a testable feature where sensor time-series tables can declare
asynchronous materialized rollups via CQL table extensions, observe queue lag
through virtual tables/subscriptions, and use admin-loaded WASM UDFs for custom
rollup functions such as standard deviation.

## Non-Goals

- Synchronous rollup durability in the source write response.
- Arbitrary non-deterministic UDFs in indexed WHERE planning.
- Public URL loading without digest verification.
- General-purpose materialized views outside the time-series rollup path.

## Sprint 1 — Contracts and Validation

1. Define materialization registry and queue interfaces.
2. Validate `consolidation.*` extensions during DDL.
3. Define target table/row schema for min/max/avg plus UDF stddev.
4. Define `FunctionIdentity` shared by schema, router, and executor.
5. Add parser AST for `AS FILE` and `AS URL ... WITH SHA256`.

**Exit criteria**: invalid DDL is rejected; valid DDL creates source/target
metadata; no worker yet.

## Sprint 2 — Materialization Engine

1. Register aggregators from DDL and schema rebuild.
2. Implement durable or reconstructable materialization queue.
3. Implement worker loop that writes target rows.
4. Recompute late windows inside `late_window`.
5. Support cascaded rollups through target writes.

**Exit criteria**: inserts crossing a boundary produce queryable rollup rows
after async processing.

## Sprint 3 — WASM Loading and Execution Completeness

1. Fix `CREATE OR REPLACE`, `IF NOT EXISTS`, overload identity, and drop
   invalidation.
2. Add admin-only file/URL import with SHA-256 verification.
3. Recompile stored UDFs after restart, follower DDL apply, and snapshot apply.
4. Invoke WASM stddev from the materialization worker.
5. Implement deterministic UDF predicates in `WHERE ... ALLOW FILTERING`.

**Exit criteria**: real Component Model UDF can be loaded by hex/file/URL,
called in SELECT, used in rollups, and used in WHERE filtering.

## Sprint 4 — Observability, Subscribe, and Examples

1. Extend `consolidation_status` with live counters.
2. Add `materialization_queues` and `materialization_tasks` virtual tables.
3. Support `SUBSCRIBE SELECT ... DELTA` for rollup target tables and queue
   virtual tables.
4. Update time-series sensor examples and docs to show min/max/avg and WASM
   stddev.
5. Add example CI coverage for the canonical scripts and one focused UDF
   stddev flow.

**Exit criteria**: operators can alert on delayed materialization using only
virtual table state, and users can run the sensor example end to end.

## Success Metrics

- End-to-end rollup integration test passes on a single node.
- Restart/follower UDF recompilation tests pass.
- Queue lag virtual table exposes oldest task age within one polling interval.
- Example docs contain runnable CQL snippets for raw sensor writes, rollup
  queries, and UDF stddev loading.
- No new dependency cycles in DSM.

## Risk Register

| Risk | Mitigation |
| ---- | ---------- |
| Router grows further | move import/validation helpers into focused modules |
| Queue durability scope expands | decide durable table vs reconstructable checkpoint before Sprint 2 |
| UDF sandbox blocks useful stddev | build smallest real stddev Component in tests early |
| Subscribe support over virtual tables diverges | write contract tests before implementation |
