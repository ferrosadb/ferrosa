---
crate: ferrosa-view
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-view — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). The dominant theme is **integration risk**: the primitives are
correct in isolation but nothing drives them yet, so the most severe failure
modes are about the *missing* engine wiring, not bugs in the present code.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| MV-1 | **Island never integrated** — `compute_view_delta` / `validate_view_def` are not called by any crate; the engine/Accord maintenance path is unwritten | Materialized views do not exist end-to-end; a green build implies a feature that is not wired in | 9 | 8 | 3 | 216 | **Open, known.** Documented as an island in README + overview. Engine integration tracked separately (roadmap "Now"). Detection is easy (no callers), hence low D. |
| MV-2 | **Maintenance correctness unverifiable in-crate** — strict-serializable base+view consistency depends on the engine's read-before-write + Accord commit, none of which is exercised here | A subtly wrong integration (lost `prior`, wrong timestamp ordering, non-atomic commit) corrupts the view silently | 9 | 5 | 7 | 315 | **Open.** Pure-core tests prove the delta math; they cannot prove the commit. Needs engine-level integration + Jepsen-style tests once wired. Highest RPN. |
| MV-3 | **Predicate `extra` silently ignored** — `ViewPredicate::extra` is a placeholder string; `compute_view_delta` projects on `IS NOT NULL` only | A view defined with a `WHERE` beyond `IS NOT NULL` would include rows it should exclude | 7 | 6 | 6 | 252 | **Open by design.** Documented placeholder; `validate_view_def` accepts `extra` without enforcing it. Must become fail-loud or be evaluated before the predicate cycle integrates. |
| MV-4 | **UDF / aggregate columns project to nothing** — `projected_columns` emits only `Base` columns; `Udf`/`Aggregate` sources yield no value | A deterministic UDF column passes validation but its value never appears in the view delta | 6 | 6 | 6 | 216 | **Open by design.** Deferred to the udf-eval cycle. Validation already rejects non-deterministic UDFs and aggregates; deterministic UDF projection is a gap. |
| MV-5 | **Property-test net absent** — `proptest` is a declared dev-dependency but unused; coverage is example-based only | Edge cases in projection / PK-change logic (empty values, duplicate PK names, many extra columns) may be untested | 5 | 5 | 6 | 150 | **Open.** Add `proptest` round-trip / invariant tests (delete-before-insert ordering on PK change, projection monotonicity). Listed in roadmap. |
| MV-6 | **`view_pk_values` substitutes empty bytes for an absent PK column** — `row.get(c).cloned().unwrap_or_default()` defaults to `vec![]` | If called on a non-projected row, an empty `Vec<u8>` masquerades as a real key value | 7 | 2 | 7 | 98 | **Mitigated by contract.** Callers gate on `projects()` first, so PK columns are always present; the `unwrap_or_default` is unreachable in correct use but is a fail-silent default rather than an assert. Consider a debug assert. |
| MV-7 | **`ColumnSource::Base` name vs. view-column name confusion** — projection keys output by `vc.name`, sourced by `base_col`; a rename mapping bug would mislabel a column | View column shows another column's value | 8 | 2 | 5 | 80 | Covered by `validate`/`delta` unit tests using distinct base vs. view names; low occurrence. |
| MV-8 | **`ViewKind` forward-compat regression** — a future edit changes the serde encoding of `Incremental`, breaking stored schema | Existing view metadata fails to deserialize after upgrade | 8 | 1 | 4 | 32 | Guarded by `serialized_form_reserves_kind_discriminator` test (gate G1). |

## Top risks to act on

1. **MV-2 (RPN 315)** — the real correctness guarantee lives in the *unwritten*
   engine integration. The pure core cannot prove strict-serializable view
   maintenance; that requires the Accord-coordinated commit plus integration /
   Jepsen tests. This is the risk that matters most once integration begins.
2. **MV-3 (RPN 252)** — the `extra` predicate is accepted at DDL time but never
   enforced. Until the predicate evaluator lands, a view with a non-trivial
   `WHERE` would over-include rows. Make this fail-loud rather than silent.

## Detection assets

- 25 in-crate unit tests (3 metadata, 12 validate, 10 delta) — prove the pure
  primitives.
- **Missing:** any cross-crate caller, any integration test, any property test.
  The absence of callers is itself the detection signal for MV-1.
