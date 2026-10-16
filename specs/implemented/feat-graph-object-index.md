---
title: The graph's object index is never created, so object-bound patterns full-scan
status: implemented, awaiting independent verification
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

- [x] **The declared-but-not-current window — CLOSED** in the same pull request
  (`353ff3b3`), not deferred. The arm now chooses the index only when the index is
  both declared **and** current; `WritePath::index_is_current` mirrors
  `declares_index_locally` over the same engine handle, and a declared index whose
  backfill is still pending falls back to the existing scan with its own WARN
  naming the window. A genuinely undeclared index keeps its scan chosen up front
  and an index read of it is still refused.
- **The bench-cluster number.** Local proof is structural (index declared; the
  read path completes under a row bound a scan cannot). The measured scan→index
  throughput belongs to the perf lane and is still outstanding.

## Implementation Notes

Landed in three commits, merged as `80bdb00c`: `588d1bc7` (declare the index at
registration), `353ff3b3` (scan while it is not current), `09c54f5e` (qualify the
intra-doc link that reds the Docs job).

**"Always created or repaired" — verified already true, no production change.**

Recon on the existing machinery, then probes, then pinned tests, show the declared
object index reaches `is_current == true` with no operator action in every
constructible shape:

- **Created:** all three registration paths — eager `SparqlEngine::new`, lazy
  `ensure_table_registered`, and a node opening over pre-existing SSTables.
- **Repaired after a failure:** `mark_failed_as` stamps `retry_at = now + 60s`,
  `take_due_retries` refuses it before then, and `resubmit_backfill` re-drives it;
  a later success clears pending via `mark_indexed` → `recompute_status` → `Current`.
  The healer runs every compaction tick (`ferrosa/src/maintenance.rs`,
  `heal_secondary_index_backfills`).
- **Repaired after an interruption:** a half-written sidecar is not trusted —
  completion is decided by the tracker's `indexed_sstables`, never by a file
  existing on disk — because the boot tracker starts empty and the declaration
  marks the generation pending regardless of what is on disk.
- **No silent degradation:** a failing build is named (table, index, sstable) in
  `publish_index_backfill_status`, in the gauges, and in an ERROR log line, while
  the object-bound read keeps returning correct rows by scanning.

Pinned by the four cases added in `test(sparql): pin that the graph object index
converges...` plus `a_failed_clustering_index_backfill_converges_through_the_retry`
in `ferrosa-storage`.

**Two findings, recorded rather than fixed here:**

1. Every boot re-marks every live generation of every declared clustering index
   pending and rebuilds it. Idempotent and correct — and it is precisely the
   mechanism that makes repair unconditional. It is also wasteful.
2. `ferrosa-ctl index rebuild` resolves **partition-key** indexes only and refuses
   the rest, so for a clustering-column index (this one) the healer retry is the
   *only* repair that exists. Pinned on a clustering index deliberately, not
   reused from the existing cell-index case.

**Gates on the final head** (`62f0c476`): fmt 0, clippy 0 findings, docs 0 errors,
`ferrosa-sparql` 173 passed / 0 failed, `ferrosa-storage` 2045 passed / 0 failed,
`ferrosa-cluster` 1764 passed / 0 failed.

## Independent verification (separate verifier, merged tree 3c39cc72)

Verdict: **verified with caveats** — the declaration holds at both registration
sites and the arm takes the index path only on declared AND current. Six reds were
re-taken under mutation, each compiling and failing on behaviour. Notable results:

- The cleanest red is the currency half reverted alone (`executor.rs` back to
  `declares_index_locally` only): exactly one test fails, with the refusal text this
  item documents. A FULL revert proves the declaration instead — four of its five
  failures land on fixture preconditions and never reach the access-path choice.
- `an_undeclared_object_index_still_scans_and_still_warns` catches a removed
  undeclared-branch only through its WARN text: both branches stream-scan, so its
  data assertions hold either way. Non-vacuous, but narrower than it reads.
- Claims about the half-written sidecar and the failed-build retry are held down by
  PRE-EXISTING storage unit tests, not by this change; the boot convergence had no
  test here at all until the follow-up added one.

### Limitation found and now documented in the code

`WritePath::index_is_current` is a **local** pre-check. In `Cluster` mode the two
predicates resolve this node's engine while `index_read_stream` fans out to every
replica, so a cluster-wide object-bound read can pass the gate on a current
coordinator index and still meet a replica's loud not-current refusal. The doc
comment previously claimed the three handles could not disagree; that claim is true
only for `Direct` and `Pair`. It is corrected in the same change rather than left
standing. Closing the cluster-mode gap needs a clustered test with divergent
per-node index currency and is tracked separately.
