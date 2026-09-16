---
crate: ferrosa-graph
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-graph — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), the code review (no in-source
`TODO`/`FIXME` markers exist), and the adjacency-consistency design.

## Now (highest value)

- **Arm + observe adjacency reconciliation (FMEA G-1).** The synchronous observer
  is the primary guarantee, but the background reconciler — the only thing that
  catches dropped mutations and crash-recovery drift — is disabled by the default
  `GraphConfig` (`reconciliation_interval = ZERO`). Ship a non-zero production
  default and add an observable **adjacency-drift counter** (entries repaired per
  pass), so the fallback firing is an alert, not a silent log line.
- **Fail loud on unindexable edge tables (FMEA G-2).** When a `graph.type=edge`
  table lacks `graph.source` / `graph.target`, the observer and reconciler skip it
  silently and the edge is invisible to every traversal. Validate at registration
  (or DDL) time and reject/warn instead of producing a silently broken graph.
- **Route reconciler writes through the keyspace replication strategy (FMEA G-3).**
  Replace the hardcoded `ReplicationStrategy::Simple{rf:1}` / `ConsistencyLevel::One`
  in `write_mutation` / `write_tombstone` with `graph_replication_strategy(schema, ks)`,
  matching the query path so multi-replica keyspaces are repaired uniformly.

## Next

- **Reflect reconciler read failures in `ReconcileMetrics` (FMEA G-7).** The
  discarded `Err(_) => continue` / `unwrap_or_default()` paths are gone — both
  scans stream and log every failure with the table and how far it got
  (t_bc5f0e6f). What remains is the metrics half: a caller reading
  `ReconcileMetrics` still cannot distinguish a complete pass from one that
  skipped a table.
- **Stream phase A of `Expand` end to end (FMEA G-9, t_4ce82a3e).** The anchor
  SCAN no longer materializes, but the frontier it feeds still collects one
  `ExpandState` per surviving anchor row. That is bounded by the answer rather
  than by the table, so it is no longer an OOM-on-any-table vector — but a query
  whose filters keep most of a large table still holds a query-sized frontier.
  The `AnchorScan` operator in `specs/streaming-executor-design.md` is the
  remaining step.
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

## Non-goals

- A separate graph storage engine — topology stays in CQL tables + the adjacency
  index; this crate is a query/index layer, not a store.
- Owning durability, replication, or consensus — those belong to `ferrosa-storage`
  / `ferrosa-cluster`. This crate delegates all reads/writes to `WritePath`.
- CQL value-codec ownership — handled upstream; the graph engine consumes
  `Partition`/`Row`/`CellValue` as given.
