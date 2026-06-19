# ferrosa-view

> The protocol-agnostic, I/O-free **core** of ferrosa materialized views:
> view metadata, DDL validation, and pure view-delta computation.

## What this crate is

A small, pure crate that owns the three conflict-free primitives every
materialized-view code path agrees on:

1. **Metadata** — the schema-replicated description of a view (`ViewMetadata`).
2. **DDL validation** — the single gatekeeper (`validate_view_def`) every
   frontend calls before a view is created.
3. **Delta computation** — the pure incremental-maintenance state machine
   (`compute_view_delta`) that turns a base-row transition into the view
   mutations needed to keep the view consistent with its base.

It is deliberately **pure**: it depends only on `ferrosa-schema` and never on
`ferrosa-storage`, `ferrosa-cluster` (Accord), or any protocol frontend. Keeping
this logic free of I/O is what makes the strict-serializable maintenance
guarantee reproducible and unit-testable.

## Status — island, not yet wired in

**This crate is an "island."** The conflict-free primitives have landed with
unit tests, but the engine/Accord wiring that would *drive* them is tracked
separately and is **not yet integrated**.

- **No other crate depends on `ferrosa-view`.** It is not a path dependency of
  any crate in the workspace — confirmed by an explicit scan. Nothing calls
  `validate_view_def` or `compute_view_delta` in production paths yet.
- The read-before-write that produces a `prior` snapshot, the Accord-coordinated
  base+view commit, the predicate (`ViewPredicate::extra`) and UDF evaluators,
  and the schema-replication of `ViewMetadata` all live in the engine layer and
  are **pending**. See [specs/roadmap.md](specs/roadmap.md).

Document, build, and test this crate as a self-contained core — but do not
mistake a green `cargo test -p ferrosa-view` for end-to-end materialized views.

## What's implemented

- **`ViewMetadata`** and supporting types (`ViewKind`, `ViewColumn`,
  `ColumnSource`, `ViewPredicate`) — serde-serializable, with `primary_key()`
  and `source_of()` helpers. Only `ViewKind::Incremental` exists; `Snapshot`
  (Postgres-style `REFRESH`) is intentionally reserved but absent (decision D1).
- **`validate_view_def`** — a pure function over `(base TableMetadata, view
  ViewMetadata, base_is_view)` enforcing the Cassandra-baseline + ferrosa rules:
  view PK covers base PK; at most one extra PK column; `IS NOT NULL` on every
  view-PK column; no aggregates; no static columns; no counter base; no chained
  views; deterministic-only UDF columns; no computed column in the view PK.
  Returns the first violated rule as a typed `ViewDefError`.
- **`compute_view_delta`** — the pure transition `(prior, next, timestamp) →
  Vec<ViewDelta>`. Handles insert into / out of the predicate, non-PK updates,
  view-PK changes (delete-old + insert-new), and deletes/tombstones. Stamps the
  originating base timestamp on every emitted delta.

## What's NOT implemented (in this crate)

- **Predicate evaluation beyond `IS NOT NULL`.** `ViewPredicate::extra` is a
  placeholder string; `compute_view_delta` projects on the `IS NOT NULL`
  baseline only.
- **UDF / aggregate column projection.** `projected_columns` emits base columns
  only; `Udf`/`Aggregate` sources project to nothing here (deferred to the
  udf-eval cycle).
- **Any I/O, storage row types, Accord coordination, or schema replication.** By
  design — these are forbidden dependency edges and live in the engine layer.

## Public API

| Area | Items |
|------|-------|
| Metadata | `ViewMetadata`, `ViewKind`, `ViewColumn`, `ColumnSource`, `ViewPredicate` |
| Validation | `validate_view_def`, `ViewDefError` |
| Delta | `compute_view_delta`, `ViewDelta`, `RowSnapshot` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-schema`** — `TableMetadata`, `ColumnMetadata`, `ColumnKind`,
  `TableFlag` (the base-table shapes `validate_view_def` reads).

External: `serde`, `uuid`. Dev: `serde_json`, `indexmap`, `proptest`.

**Called by** (crates that depend on this):

- **NONE yet.** `ferrosa-view` is an island awaiting integration — no crate in
  the workspace lists it as a path dependency. This is intentional and tracked;
  see [specs/overview.md](specs/overview.md) and [specs/roadmap.md](specs/roadmap.md).

## Tests

25 in-crate unit tests, all co-located with the code:

- `metadata` — 3 (`ViewKind` round-trip, reserved-discriminator encoding, PK
  iteration order).
- `validate` — 12 (one accept case plus every rejection rule).
- `delta` — 10 (predicate flips, PK change, update, delete, TTL/tombstone,
  determinism).

`proptest` is declared as a dev-dependency but not yet exercised — a tracked gap
(see [specs/fmea.md](specs/fmea.md)).

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, data flow
- [FMEA / known issues](specs/fmea.md) — failure modes + integration gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
