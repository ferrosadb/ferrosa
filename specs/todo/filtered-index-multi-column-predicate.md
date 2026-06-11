# Filtered (partial) index: multi-column conjunction predicates

Status: TODO (follow-on to the Filtered partial index, commit `e7762b32`).

## What already shipped

The Filtered partial index is end-to-end for a **single-column** predicate, and
the planner now reasons soundly about RANGE implication and the remote builder
filters natively:

- `ferrosa_index::query_constraint_implies_predicate` — sound subset-containment
  test over the byte ordering (`Q ⊆ P`). Covers Eq-implies-Eq AND range
  implication (`age = 30` / `age > 25` / `age >= 22` all imply `age > 21`;
  `age > 10` / `age >= 21` are withheld). Enumerated true-positive AND withheld
  cases in `ferrosa-index/src/filtered.rs::query_constraint_implies_predicate_enumeration`,
  cross-checked against `evaluate_predicate` so no claimed implication admits a
  non-retained value.
- `ferrosa-cql/src/router.rs::query_implies_filter_predicate` maps every scalar
  `ComparisonOp` to a `FilterOp` and calls the helper. End-to-end soundness +
  completeness proven by `filtered_index_used_when_query_implies_range_predicate`
  and `filtered_index_withheld_when_query_does_not_imply_range_predicate`.
- `build_filter_predicate_from_options` now coerces the `filter_value` string to
  the column's CQL type (`filter_value_to_term`), so numeric/boolean partial
  predicates (e.g. `age int` with `filter_op:'>' , filter_value:'21'`) are
  accepted at CREATE time rather than rejected as a string-vs-int type mismatch.
- Remote builder carries the predicate natively: `build_request_body`
  (`ferrosa-storage`) serializes `FilterPredicate` into the build request under
  `filter_predicate`; `ferrosa-index-builder` deserializes it
  (`BuildRequest::filter_predicate`), recognizes `"filtered"` in
  `parse_index_type`, and threads it into the job via `build_job`, so a remote
  filtered build filters at build time. The local-only fallback short-circuit is
  removed.

## What is deferred (this work item): multi-column conjunction filter

Today `FilterPredicate` is exactly one `(column_position, op, value)`. A filter
over a conjunction of >1 column — e.g.

```sql
CREATE INDEX adults_in_eng ON users (name) USING 'filtered'
  WITH OPTIONS = {'filter': "age > 21 AND dept = 'eng'"}
```

is not yet supported. Delivering it soundly requires a coordinated change across
several layers, which is why it is split out rather than half-wired:

1. **Type / wire format (`ferrosa-index`)**: introduce a `FilterPredicate`
   conjunction (e.g. `FilterPredicate { clauses: Vec<FilterClause> }` where a
   `FilterClause` is the current `(column_position, op, value)`), keeping a
   single-clause form for backward compatibility. `evaluate_predicate` becomes an
   `all(clauses)` fold; `query_constraint_implies_predicate` must prove the query
   implies **every** clause (the query's row-set must be a subset of the
   intersection of each clause's retained set). This is the new serialized shape
   persisted under `__filter_predicate`; bump a version tag so old single-clause
   JSON still deserializes.
2. **Build path (`ferrosa-storage`, `FilteredBuilder`/`LocalBackend::build`)**:
   evaluate the conjunction per row (skip unless all clauses pass). The memtable
   write path must use the same shared evaluator so sidecar and memtable agree.
3. **CQL surface (`ferrosa-cql`)**: parse a conjunction from WITH OPTIONS. Either
   keep the three-key form but allow repeated/indexed keys, or accept a single
   `'filter'` string parsed as a CQL boolean expression restricted to
   `col <op> literal AND ...`. Resolve each column's storage ordinal + encode
   each literal to storage bytes (reuse `filter_value_to_term`). Fail loud on any
   unparseable clause.
4. **Planner (`router.rs`)**: `filtered_index_covered_columns` must mark **all**
   conjunction columns covered, and `query_implies_filter_predicate` must require
   the query to imply each clause (withhold if even one is not provably implied).
5. **Remote builder (`ferrosa-index-builder`)**: no change beyond the predicate
   already round-tripping — once `FilterPredicate` is the conjunction type, the
   existing `filter_predicate` wire field carries it verbatim.

### Soundness requirement

A multi-column partial index retains `rows where clause_1 AND clause_2 AND ...`.
The planner may serve a query from it only when the query's filter-column
constraints provably imply **every** clause (per-clause subset containment via
`query_constraint_implies_predicate`). If any clause is not implied, withhold the
index — serving it would silently drop rows. The completeness test must enumerate
both directions, including the case where one clause is implied but another is
not (must withhold).

### Tests to add (TDD)

- `ferrosa-index`: conjunction `evaluate_predicate` (all-clauses fold) and
  `query_constraint_implies_predicate` over a 2-clause predicate, enumerating
  true-positive and withheld (one-clause-missing) cases.
- `ferrosa-cql`: end-to-end CREATE with a 2-column filter; a query implying both
  clauses uses the index and returns exactly the matching rows; a query implying
  only one clause is withheld and (with ALLOW FILTERING) returns the complete set.
- Persistence round-trip of the conjunction predicate through `system_schema.indexes`.
