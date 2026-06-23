# BUG · P1 · `fts_match` is non-deterministic on a multi-node cluster

**Filed:** 2026-06-20 | **Component:** ferrosa (CQL served path + full-text index)
**Downstream:** ferrosa-memory BUG-F-007

## Summary

On a 3-node cluster (RF=3), `SELECT ... WHERE col = fts_match('<token>')` returns
**0 or 1 rows non-deterministically for a single, stable, never-changing row**,
while a normal scan of the same table consistently returns the row. The result
depends on which node coordinates the query and/or on background full-text-index
(FTI) rebuild timing — so native FTS is unusable for lexical recall on a cluster.

The single-node, in-process tests `fulltext_index_fts_match_end_to_end` and
`fulltext_index_fts_match_reads_unflushed_memtable_row`
(`ferrosa-cql/src/router.rs`) **pass** — they do not exercise the served
multi-node path, so they do not catch this.

## Reproduce (live 3-node cluster, RF=3)

```sql
CREATE KEYSPACE ftsprobe WITH replication = {'class':'NetworkTopologyStrategy','datacenter1':'3'};
CREATE TABLE ftsprobe.t (tenant_id uuid, id uuid, body text, PRIMARY KEY ((tenant_id), id));
CREATE INDEX ON ftsprobe.t (body) USING 'fulltext';
INSERT INTO ftsprobe.t (tenant_id, id, body) VALUES (<tid>, <id>, '<token> native fts probe body');
-- repeat with a round-robin client; same stable row:
SELECT id FROM ftsprobe.t WHERE tenant_id = <tid> AND body = fts_match('<token>') LIMIT 5 ALLOW FILTERING;
```

Observed `fts_match` row count over time for one inserted row (client round-robining
across the 3 coordinators), normal scan = 1 throughout:

| t      | 0s | 10s | 30s | 60s | 90s |
|--------|----|-----|-----|-----|-----|
| rows   | 0  | 1   | 0   | 0   | 1   |

Verified against ferrosa `main` (image build `3cec99cc6127`, freshly rebuilt
cluster). Memtable-only reads work (immediate insert often returns 1); the
flapping appears once flush + the **background per-SSTable FTI rebuild**
(`eager_index_build_job` / index-build scheduler) is in play.

## Hypotheses (for the investigating agent)

1. **Coordinator-local evaluation.** The served `fts_match` path may evaluate the
   match only against the coordinator node's local FTI rather than scatter-gathering
   across the token-range replicas. With RF=3 and round-robin, different coordinators
   then return different answers. Check the router/planner `fts_match` execution in
   `ferrosa-cql/src/router.rs` / `planner.rs` — does it route per-token-range like a
   normal indexed read, or run locally?
2. **FTI rebuild swap is not atomic.** Post-flush, the per-SSTable FTI is rebuilt in
   the background; if `fts_match` reads return empty while the new FTI is being
   swapped in (instead of serving the previous FTI until the new one is ready),
   queries during the rebuild window return 0. Check `ferrosa-index/src/fulltext/`
   reader/merge + the index-build scheduler swap.

## Definition of done

- A **multi-node served-path** regression test (the existing single-node in-process
  tests cannot catch this) asserting `fts_match` deterministically returns an
  inserted+flushed row across all coordinators and across an FTI rebuild.
- Consistent cross-replica `fts_match` evaluation and/or atomic FTI swap so a stable
  row is never transiently invisible.

## Workaround (downstream, in place)

ferrosa-memory keeps its `document_terms` / `context_segment_terms` fallback enabled
and does not treat native FTS zero-row responses as proof of no match.

---

## ROOT CAUSE FOUND (2026-06-23) — compaction FTI visibility window

Hypothesis 1 (coordinator-local lookup) is **already fixed**: `fts_match` now
scatter-gathers across all nodes and unions (`coordinate_fulltext_search`,
ferrosa-cluster/src/coordinator/read.rs), and the post-match partition fetch is a
distributed `coordinate_read` (ferrosa-cql/src/router.rs:3418 →
write_path.rs `read` → Cluster→coordinate_read). Both are robust. The test still
flaps, so the residual cause is hypothesis 2 — **non-atomic FTI on compaction**:

- FLUSH builds the FTI sidecar **synchronously** before the SSTable is live
  (ferrosa-storage/src/store.rs flush "Step 5c", ~:2236 → write_fti_sidecar).
- COMPACTION does **not**. ferrosa-storage/src/engine.rs:6478-6534:
  1. opens the merged output SSTable,
  2. `swap_compacted_sstables` makes it LIVE and **deletes the input SSTables +
     their `-FTI-*.db` sidecars** — the new SSTable has **no FTI sidecar yet**,
  3. the FTI is (re)built by an **async** `eager_index_build_job`
     (`scheduler.submit`, :6521-6534).
- `engine.fulltext_search` finds rows only via memtable (active+flushing) FTI +
  the on-disk `-FTI-<index>.db` sidecar files. During the window between (2) and
  (3) the compacted SSTable's rows are in **no** FTI → `fts_match` returns empty
  for them. On RF=3 with replicas compacting together, the **union** goes empty →
  `fts_match` = 0 rows ⇒ the flaky `fts_match_returns_flushed_row_...` failure.
  A normal scan is unaffected (it reads SSTable data directly), matching the
  spec's "normal scan = 1 throughout".

### Fix options
- **A (preferred — mirror flush):** build the FTI sidecar **synchronously** for the
  compacted output (from the opened `reader`) BEFORE `swap_compacted_sstables`
  makes it live (engine.rs ~:6484). Keep the async `eager_index_build_job` as a
  backstop. Closes the window; reads stay fast.
- **C (read-side robustness, complementary):** in `engine.fulltext_search`, for any
  LIVE SSTable lacking an FTI sidecar, build a transient FTI from its text column
  on the fly (same approach as `fulltext_memtable_search`). Guarantees a row is
  never invisible regardless of build timing; unit-testable without a cluster
  (SSTable with no sidecar must still return via fts_match). Slower only during the
  build window.

DoD additions: a unit test that an SSTable with NO FTI sidecar still returns its
rows via fts_match (covers C and the compaction window), plus the existing live
`fts_match_returns_flushed_row_from_each_live_cluster_node`.
