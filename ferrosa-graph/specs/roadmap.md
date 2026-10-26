---
crate: ferrosa-graph
doc: roadmap
last_updated: 2026-10-15
---

# ferrosa-graph — Roadmap

> Reviewed 2026-10-15 against `9e32e82a` (v0.22.3). FMEA G-3 and the G-7
> metrics half are implemented and removed from the forward plan; the G-1
> repair/error counters shipped, leaving only the non-zero production default
> open.

Sourced from the FMEA gaps ([fmea.md](fmea.md)), the code review (no in-source
`TODO`/`FIXME` markers exist), and the adjacency-consistency design.

## Now (highest value)

- **Scalar-index follow-ups.** Bolt first-access setup is automatic and memoized;
  integer/UUID, text, boolean and bounded float equality consume current scalar
  indexes. Extend range/predicate support and explicit Cypher index DDL while
  preserving evaluator semantics. Broad epsilon matches retain a range fallback;
  boolean noncanonical aliases remain bounded but may require 255 probes.
- **Extend count pushdown.** Unfiltered physical-vertex `count(n)`/`count(*)` now
  count metadata. Filtered, grouped, DISTINCT and property counts keep the
  evaluator; preserve null, multiplicity and visibility semantics before adding
  further shortcuts. [Measurements](../../deploy/fly-bench/pipeline/RESULTS.md).

- **Arm adjacency reconciliation in production (FMEA G-1).** The synchronous
  observer is the primary guarantee, but the background reconciler — the only
  thing that catches dropped mutations and crash-recovery drift — is still
  disabled by the default `GraphConfig` (`reconciliation_interval = ZERO`). Ship
  a non-zero production default. The observable half is already in place: pass
  counters export as
  `ferrosa_graph_adjacency_entries_{repaired,removed,rewritten}_total` and
  `_reconcile_errors_total` on `/metrics`, so the fallback firing is an alert,
  not a silent log line.
- **Fail loud on unindexable edge tables (FMEA G-2).** When a `graph.type=edge`
  table lacks `graph.source` / `graph.target`, the observer and reconciler skip it
  silently and the edge is invisible to every traversal. Validate at registration
  (or DDL) time and reject/warn instead of producing a silently broken graph.

## Next

- **Store Cypher map/list property values as jsonb (D12, T-191).** T-012 made
  them a typed error on the schema-less CREATE/MERGE path (FMEA G-11); the jsonb
  write replaces that error. Also fix `expr_to_column_bytes` integer/float widths.

- **Stream phase A of `Expand` end to end (FMEA G-9, t_4ce82a3e).** The anchor
  SCAN no longer materializes, but the frontier it feeds still collects one
  `ExpandState` per surviving anchor row. That is bounded by the answer rather
  than by the table, so it is no longer an OOM-on-any-table vector — but a query
  whose filters keep most of a large table still holds a query-sized frontier.
  The `AnchorScan` operator in `specs/streaming-executor-design.md` is the
  remaining step.
- **Fuzz the PackStream decoder (FMEA G-10).** Depth and pre-allocation are
  bounded and unit-tested; add a cargo-fuzz target over `bolt::codec::decode`
  and `BoltMessage::decode` to find the next parser hazard.
- **Cost-aware variable-length paths (FMEA G-4).** Add a planner estimate for
  `[*min..max]` fan-out and reject/flag unbounded `[*]` at `EXPLAIN` time; make the
  vertex budget cardinality-aware rather than a single global cap.
- **Close the orphan-tombstone race (FMEA G-6).** Give Phase-2 orphan detection a
  consistent snapshot or a grace window keyed on edge-write timestamps so a freshly
  created edge's adjacency row cannot be tombstoned mid-flight.

## Later

- **Finer-grained authorization (FMEA G-5).** Per-relationship-type and
  property-level authz beyond the current per-statement Select/Modify check;
  property redaction in projections.
- **Full column-name graph mapping.** The observer notes a "Phase 1" model
  (partition key as source, clustering as target); complete the general
  source/target column mapping for arbitrary edge-table shapes.
- **Property-test the adjacency invariant.** A round-trip property: for any edge
  mutation sequence, the derived adjacency set equals the reconciler's recomputed
  set — an invariant regression net independent of the executor.

- **The rest of FMEA G-14.**
  - Let a heal survive one replica restarting: CL ALL fails it today.
  - Record a completed heal cluster-wide, so the other nodes skip the rewrite
    that the first one already sent to every replica.
  - Pace the heal's CL ALL writes against client traffic.
  - On a legacy node, serve hops from the edge table while the heal runs,
    instead of refusing them.

## Implemented since the last revision

- **MERGE endpoint identity (2026-10-24).** Schema-on-write UUID fallback keys
  use 16 bytes, and each relationship is scheduled after both endpoint nodes
  bind. Tests cover new relationships, explicit UUID destinations, existing
  opaque keys, traversal, idempotency and deletion. Existing raw keys are not
  rewritten.

- **G-14 — a completed deploy heal is not repeated (2026-10-19).** The heal
  writes `<data_dir>/graph-adjacency-heals.json` once it completes with zero
  errors, and later starts serve at once instead of rerunning a CL ALL rewrite
  of the whole keyspace before the first traversal (t_504435aa).
- **G-14: the heal no longer holds the hot path (2026-10-20).**
  - A query waits on a running heal for at most 1 s, then is refused
    retryably while the heal continues.
  - A start that finds the heal record skips only the cluster-wide rewrite,
    and still runs a local catch-up pass. That pass indexes the edges
    written before the process registered its observer (t_d0dacb7a).
- **Register the adjacency observer before writes are accepted.** It is
  registered lazily, by the first graph query in each process. Writes that
  land before then rely on the catch-up pass, and a node that never gets a
  graph query never indexes forwarded edge writes at all (t_17765000).

- **G-3 — reconciler writes at the keyspace replication strategy (2026-10-03).**
  `write_mutation` / `write_tombstone` write the index at CL ALL under the
  adjacency keyspace's own replication, and both reconcile scans run at CL ONE
  under each keyspace's replication (`graph_replication_strategy`); no hardcoded
  `Simple{rf:1}` / `ConsistencyLevel::One` remains (`adjacency/reconcile.rs`;
  FMEA G-3).
- **G-7 — reconciler read failures surface in `ReconcileMetrics` (2026-10-03).**
  Both scans stream and log every failure — opening the scan and failing
  mid-stream — with the table and how far it got, and each failure increments
  `ReconcileMetrics::errors`; `is_complete()` lets the periodic pass and the
  first-use heal treat an incomplete pass as not healed (t_bc5f0e6f; FMEA G-7).

## Non-goals

- A separate graph storage engine — topology stays in CQL tables + the adjacency
  index; this crate is a query/index layer, not a store.
- Owning durability, replication, or consensus — those belong to `ferrosa-storage`
  / `ferrosa-cluster`. This crate delegates all reads/writes to `WritePath`.
- CQL value-codec ownership — handled upstream; the graph engine consumes
  `Partition`/`Row`/`CellValue` as given.
