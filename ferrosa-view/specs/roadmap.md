---
crate: ferrosa-view
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-view — Roadmap

Sourced from the code (what is and isn't implemented), the FMEA gaps
([fmea.md](fmea.md)), and the island/integration status. The headline is that
this crate's *primitives* are done; its *value* is unrealized until the engine
consumes it.

## Now (highest value)

- **Integrate the island into the engine** (FMEA MV-1, MV-2). Wire
  `compute_view_delta` into the storage observer / write path and have the
  Accord-coordinated commit apply base+view atomically. This is the single
  highest-value step — until it lands, materialized views do not exist
  end-to-end. Track the read-before-write that produces `prior` and the
  schema-replication of `ViewMetadata` as the first consumers.
- **Make the `extra` predicate fail-loud** (FMEA MV-3). Today
  `ViewPredicate::extra` is accepted at validation and ignored at maintenance.
  Until an evaluator exists, reject (or loudly flag) views whose `WHERE` exceeds
  `IS NOT NULL`, rather than silently over-including rows.

## Next

- **Predicate evaluator** beyond `IS NOT NULL` — compile and evaluate
  `ViewPredicate::extra` inside the projection check so `compute_view_delta`
  honours the full `WHERE`.
- **UDF-computed column projection** (FMEA MV-4) — wire the udf-eval cycle so
  `projected_columns` emits deterministic `Udf` column values, not just `Base`
  projections. Validation already enforces determinism (gate G2).
- **Property tests** (FMEA MV-5) — put the declared but unused `proptest`
  dev-dependency to work: invariants like delete-before-insert ordering on a
  PK change, projection monotonicity, and `ViewMetadata` serde round-trip.

## Later

- **`ViewKind::Snapshot`** — the Postgres-style `REFRESH MATERIALIZED VIEW`
  maintenance model (D1, board task t_02a5a95c). The discriminator is already
  forward-stable so adding it is non-breaking.
- **Debug-assert PK presence** in `view_pk_values` (FMEA MV-6) — replace the
  silent `unwrap_or_default` with an assertion that the row is projected, so a
  contract violation crashes loudly instead of emitting an empty key.

## Non-goals

- I/O, storage row types, Accord coordination, schema replication, or protocol
  framing — those belong to the engine and the frontends, and are forbidden
  dependency edges for this crate (see `dsm-proposed.md`). `ferrosa-view` stays
  a pure, leaf-adjacent core.
