---
title: The graph's object index is never created, so object-bound patterns full-scan
status: landed, awaiting independent verification
updated: 2026-10-16
---

# feat: create the object index the SPARQL executor reads through

## Summary

`ferrosa-sparql`'s executor has two arms for a bound object (`TripleOp::ObjectScan`):

- an index-read arm that calls `write_path.index_read_stream(&table_id,
  OBJECT_INDEX_NAME, &index_key, None)` — written, and unreachable;
- a fallback that streams a full `rdf_triples` scan with filtering.

The arm chosen depends on `write_path.declares_index_locally(&table_id,
"rdf_triples_object_idx")` (`executor.rs:654-668`). **Nothing ever declared that
index.** `rdf_triples_schema` (`triple_store.rs:22`) builds the `TableSchema` with
`extensions: Default::default()` and no index, and the three sites that register
the table — `SparqlEngine::new` (`engine.rs:216-235`) and
`ensure_table_registered` (`engine.rs:240`) — called `register_table`, which
passes `vec![]` for indexes.

Consequence: every `?s ?p <o>` pattern (subject *and* predicate unbound — the
planner only reaches `ObjectScan` when neither is bound, `planner.rs:562-574`)
read the entire `rdf_triples` table of the keyspace and filtered in memory, and
emitted `ObjectScan: object index is not declared; streaming full scan with
filtering` on every such query. This is the shape behind the 15 q/s figure.

The `object` column is one of the two **clustering** columns of
`((graph, subject), predicate, object)`, not a regular column, so it cannot be
declared through the cell-based `register_table_with_indexes` path: its value
lives in the clustering-key bytes, not in a row cell.

## Acceptance criteria

- [x] After graph registration, `StorageEngine::declares_index(&triples_table_id(ks),
      "rdf_triples_object_idx")` is `true` for both the eagerly registered default
      keyspace and a lazily registered one.
- [x] An object-bound query returns exactly the stored triples (identity and
      content, before and after a flush), including not returning a named graph's
      triple with the same object.
- [x] The object-bound read stops at the index: over a 20-row table under the
      executor's own 8-row bound, only an index read can complete.
- [x] MIRROR CONTROL — a table with no declared index still streams the full scan,
      still returns the right values, and still warns.
- [x] Negative control: revert the production change, keep the tests; they fail on
      the behaviour while still compiling.
- [ ] Measured scan→index throughput on the bench cluster (owned by the perf lane;
      the local instrument here is the declared-index predicate plus the executor's
      own row counter, never a wall-clock bound).

## Not the cause (ruled out with evidence)

- **The subject-bound read path is not at fault.** `TripleOp::SubjectLookup`
  point-reads a single partition — `write_path.read(&table_id, &key)` at
  `executor.rs:648-652` — so its cost is bounded by one partition's row count by
  construction. The bound-subject shape is served by that arm, not by a scan.
- **The 15 queries/s figure came from object-bound shapes** (`?s ?p <o>`) that
  stream-scan the whole table because no index is declared. `ObjectScan` is
  reached only when the subject and predicate are both unbound, so a query that
  also binds the predicate takes `PredicateScan` and does not exercise this path
  at all; the shape that measured badly is the object-only one.
- **The write-side update half is not missing.** The pipeline that maintains a
  *declared* index on every write is implemented: a lock-free persistent red-black
  memtable index (`ferrosa-storage/src/store.rs`, `IndexCatalog.indexed_clustering_columns`
  consumed at `store.rs:3800-3843`), per-SSTable sidecar files built at flush
  (`scalar_index_from_rows`, `store.rs:1490-1556`), merged at compaction. See
  `specs/reference/secondary-index-pipeline.md`. It engaged for nothing here only
  because nothing declared the index.

## Fix

`SparqlEngine::ensure_object_index(keyspace)` (`ferrosa-sparql/src/engine.rs`),
called from both registration sites, declares the index through the engine-level
path that `CREATE INDEX` (`ferrosa-cql/src/router.rs:12597-12616`) and the restart
reload (`ferrosa-storage/src/engine.rs:6141-6166`) already use:
`StorageEngine::register_index_in_engine` with `target_col: "object"`. That path
resolves the clustering component from the table's own schema (so no hardcoded
component index), registers the index in the build tracker, and submits backfill
jobs for any SSTable that predates the declaration. The declaration is idempotent,
and the name now has one home (`executor::OBJECT_INDEX_NAME`, `pub(crate)`).

The `object` column name moved to `triple_store::OBJECT_COLUMN` so the schema and
the declaration cannot drift.

## Test list

`ferrosa-sparql/tests/sparql_object_index.rs`:

1. `registration_declares_the_object_index_the_object_scan_reads`
2. `a_lazily_registered_keyspace_also_gets_the_object_index`
3. `an_object_bound_query_is_answered_without_reading_the_whole_table`
4. `an_undeclared_object_index_still_scans_and_still_warns` (mirror control)

## NOT DONE

- **The declared-but-not-current window.** The `ObjectScan` arm tests
  `declares_index_locally` only; it does not test index currency.
  `read_by_index_each_after` refuses an index that is not current
  (`ferrosa-storage/src/engine.rs:10536-10541`), so a node that boots with
  SSTables and is still backfilling the newly declared index answers an
  object-bound query with a loud error instead of the scan it used to run. The
  window is bounded by backfill completion, and it is the same window a
  CQL-issued `CREATE INDEX` on `rdf_triples(object)` has always had — but
  declaring the index by default makes it reachable without an operator asking
  for it. Closing it needs an `index_is_current` accessor on
  `WritePath` (`ferrosa-cluster/src/write_path.rs`, alongside
  `declares_index_locally` at line 1261) AND-ed into the arm's guard, which is
  outside this change's crate set.
- **The bench-cluster number.** Local proof is structural (index declared; the
  read path completes under a row bound a scan cannot). The measured scan→index
  throughput belongs to the perf lane.
