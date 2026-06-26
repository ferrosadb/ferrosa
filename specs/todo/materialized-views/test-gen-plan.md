---
title: Materialized Views — Test Generation Plan (file manifest)
status: draft
created: 2026-06-19
work_item: materialized-views
branch: feature/materialized-views
executive_summary: >
  Concrete test-generation manifest for the materialized-view feature: the exact
  test files, red-first stub function names, fixtures, and mocks to create, mapped
  to crates, the seven test layers, and the build phases P1–P5. The pure-core
  crate ferrosa-view gets the bulk of the always-on tests (validate + delta unit
  and proptest suites) with no fixtures; storage and cluster get integration and
  fault-injection suites gated behind the live-infra-tests feature with panic-on-
  missing-infra per repo policy; ferrosa-cql gets the DDL contract and the
  driver-in-the-loop system_schema.views test that reuses the existing P2-bug
  repro. Each stub is named so it can be committed red first under /tdd, and the
  manifest states the one shared fixture (a model base table + view definition
  builder) to avoid per-test setup duplication. Total: ~11 test files across 5
  crates, phased so P1 lands the entire pure-core and contract surface before any
  maintenance code exists.
---

# Materialized Views — Test Generation Plan

> Realizes [test-specification.md](test-specification.md) as concrete files. Names
> are the red-first stubs to commit under `/tdd`. "Gate" columns reference
> [decisions.md](decisions.md) G1–G7 and DSM elements E1–E14 from
> [dsm-proposed.md](dsm-proposed.md).

## 1. Shared fixtures (build once, reuse everywhere)

Create in `ferrosa-view` test support (and re-export for downstream crates):

- `mk_base_table(...)` — builds a `TableMetadata` for a base table with a chosen
  PK/clustering shape.
- `mk_view_def(base, pk, select, where_pred)` — builds a `ViewMetadata` (used by
  validate, delta, and DDL tests).
- `model_base::ModelBase` — an in-memory `HashMap` model of a base table that
  applies mutations and can **re-project** the view from scratch (the oracle for
  the L2 property tests' "fold == reproject" invariant).
- `mk_mutation(op, pk, cols, ts)` — base mutation builder with explicit timestamp.

Downstream live-infra fixtures (storage/cluster): reuse the existing
`setup_state` engine fixture pattern from `ferrosa-cql` tests and the
`container_runtime()` helper; do **not** hardcode `"docker"`.

## 2. File manifest

### ferrosa-view (NEW crate) — pure core, always-on

| File | Stubs (red-first) | Layer | Gate/Elem |
|------|-------------------|-------|-----------|
| `src/validate.rs` `#[cfg(test)]` | `rejects_view_pk_missing_base_pk_col`, `rejects_two_extra_non_pk_pk_cols`, `requires_is_not_null_on_view_pk`, `rejects_aggregate_select`, `rejects_static_column`, `rejects_counter`, `rejects_chained_mv`, `accepts_minimal_valid_view`, `accepts_predicate_where`, `rejects_nondeterministic_udf_column`, `accepts_deterministic_udf_column`, `rejects_udf_column_in_pk` | L1 | G2, E4 |
| `src/delta.rs` `#[cfg(test)]` | `insert_into_predicate_emits_view_insert`, `insert_outside_predicate_emits_nothing`, `update_non_pk_col_updates_view_row`, `update_view_pk_col_emits_delete_old_and_insert_new`, `predicate_flip_in_emits_insert`, `predicate_flip_out_emits_delete`, `delete_emits_view_delete`, `ttl_expiry_emits_timestamped_delete`, `view_cell_ts_equals_base_ts` | L1 | G4, G6, E5 |
| `src/metadata.rs` `#[cfg(test)]` | `view_kind_incremental_roundtrips`, `serialized_form_reserves_kind_discriminator` (golden bytes) | L1 | G1, E1 |
| `tests/prop_delta.rs` (proptest) | `prop_fold_equals_reproject`, `prop_no_orphan_at_any_prefix`, `prop_replay_is_idempotent`, `prop_delta_is_pure` | L2 | G4, D2, E5 |

### ferrosa-cql — DDL contract + driver-in-the-loop

| File | Stubs | Layer | Gate/Elem |
|------|-------|-------|-----------|
| `src/parser.rs` `#[cfg(test)]` (replace the current "not yet supported" test at `:4436`) | `parses_create_materialized_view`, `parses_drop_materialized_view`, `parses_alter_materialized_view`, `create_mv_roundtrips_to_view_metadata` | L3 | E12 |
| `tests/system_schema_views_contract.rs` | `views_table_returns_row_after_create_mv`, `views_shape_decodes_in_scylla_rust_driver_fork` (driver-in-loop), `views_shape_decodes_in_python_driver`, `repeated_ddl_schema_agreement_succeeds` | L3 | G7, E11 |

> The driver-in-loop test reuses the repro in
> `specs/todo/bug-system-schema-views-column-shape-breaks-scylla-driver.md`. It is
> the authority on G7 — our own encoder asserting its own shape does not close G7.

### ferrosa-schema — replication contract

| File | Stubs | Layer | Gate/Elem |
|------|-------|-------|-----------|
| `tests/view_metadata_replication.rs` | `view_metadata_survives_raft_ddl_roundtrip`, `view_reloads_identically_on_fresh_replica`, `snapshot_views_map_persists` | L3/L4 | G1, E6 |

### ferrosa-storage — lifecycle, observer, view indexing

| File | Stubs | Layer | Gate/Elem |
|------|-------|-------|-----------|
| `tests/mv_lifecycle.rs` | `create_mv_builds_view_table_store`, `drop_mv_removes_view_store`, `alter_mv_applies_options` | L4 | E7 |
| `tests/mv_observer.rs` | `base_write_emits_expected_view_mutations`, `observer_uses_prior_row_for_view_pk_change`, `no_view_means_observer_is_noop` | L4 | E8, E5, G3 |
| `tests/mv_view_indexing.rs` (`live-infra-tests` for FTS/vector flush) | `native_2i_on_view_consistent_immediately`, `view_fts_visible_after_flush`, `view_vector_visible_after_flush`, `two_tier_freshness_view_row_before_index` | L4 | D3, G5, E10 |

### ferrosa-cluster — Accord atomicity, fault injection (live-infra)

| File | Stubs (`#![cfg(feature = "live-infra-tests")]`, panic if env absent) | Layer | Gate/Elem |
|------|-------------------|-------|-----------|
| `tests/mv_accord_atomicity.rs` | `base_and_view_commit_atomically`, `crash_between_base_and_view_leaves_no_divergence`, `view_pk_change_atomic_under_concurrent_reader`, `concurrent_base_writers_same_winner_in_base_and_view`, `replica_failure_mid_maintenance_converges_on_recovery` | L6 | D2, G4, E9 |
| `tests/mv_jepsen_strict_serial.rs` (ferrosa-jepsen harness) | `strict_serializability_holds_over_base_view_interleavings` | L6 | D2, E9 |

### ferrosa-loadgen — performance gates

| File | Stubs | Layer | Gate/Elem |
|------|-------|-------|-----------|
| `benches/mv_fast_path.rs` | `no_view_write_matches_pre_mv_baseline` (**release-blocking**), `assert_no_accord_txn_on_no_view_write` | L7 | G3, E9 |
| `benches/mv_maintenance_cost.rs` | `read_before_write_added_latency_baseline`, `two_stage_async_lag_metric` | L7 | G5, E8/E9 |

## 3. Phasing (which tests land with which build phase)

| Phase | Tests committed red-first | Why now |
|-------|---------------------------|---------|
| **P1** metadata+DDL | all `ferrosa-view` (validate, delta, metadata, prop_delta), `parser` DDL tests, `system_schema_views_contract`, `view_metadata_replication` | The entire pure-core + contract surface is provable before any maintenance code — front-loads the cheapest, highest-leverage coverage |
| **P2** local maintenance | `mv_lifecycle`, `mv_observer` | Observer + delta wiring on a single node |
| **P3** cluster/Accord | `mv_accord_atomicity`, `mv_jepsen_strict_serial`, `mv_fast_path` (G3 gate) | Atomicity + fast-path become meaningful once Accord is wired |
| **P4** view indexing | `mv_view_indexing`, `mv_maintenance_cost` | D3 indexing + lag metrics |
| **P5** driver/repair | re-run `system_schema_views_contract` against finalized shape; seed view-repair tests under `t_f00fdaf7` | G7 close-out |

## 4. Mock/stub policy

- **No mocking of the delta core.** E5/E4 are pure — test with real inputs, real
  `ViewMetadata`. Mocks here would hide the determinism property (D2/G2).
- **`model_base` oracle** stands in for "the correct view" in property tests — it
  is a reference implementation, not a mock.
- **Live infra is real, not mocked.** Per repo policy, cluster/Accord/FTS-flush
  tests use real infrastructure behind `live-infra-tests` and **panic** with setup
  instructions when `FERROSA_TEST_CLUSTER_NODES` / `FERROSA_TEST_CONTAINERS` /
  `FERROSA_TEST_FIRECRACKER` is absent — they never silently skip.
- **Driver-in-loop** uses the bundled `scylla-rust-driver` fork; that is the
  point of G7 — a mock driver would re-introduce the original false premise.

## 5. Definition of done (test side)

- Every gate G1–G7 has a named, green test (traceability matrix in
  [test-specification.md](test-specification.md) §3).
- `cargo test` with full infra = zero failures, zero ignored (repo policy).
- G3 fast-path bench and G7 driver-shape contract are wired into the nightly
  release-blocking job.
