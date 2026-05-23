---
type: gap
priority: P0
status: in_progress
created: 2026-05-20
updated: 2026-05-22
---

# RRD consolidation extensions must materialize rollup tables

## Why

RRD / cascading time-series aggregation exists as storage-layer building
blocks, table-extension parsing, cascade-table metadata creation, examples, and
FMEA coverage. It does not currently produce queryable rollup rows from normal
CQL writes.

Current working pieces:

- `ConsolidationConfig::from_extensions` parses `consolidation.*` table
  extensions.
- `TimeSeriesAggregator` maintains per-partition ring buffers and emits
  `ConsolidationTask` values on boundary crossings and late data.
- `ConsolidationWorker::consolidate_window` computes aggregate values.
- `CREATE TABLE ... WITH extensions = {..., 'consolidation.cascade': 'true'}`
  auto-creates downstream table metadata.
- `system_observability.consolidation_status` lists tables with consolidation
  extensions.

Current missing pieces:

- No create/alter-table hook registers `TimeSeriesAggregator` instances from
  table extensions.
- `ConsolidationWorker` does not run a worker loop and does not write target
  table mutations.
- `TimeSeriesAggregator::on_write` returns no derived mutations, so observer
  dispatch never materializes rollups.
- `consolidation_status` reports configuration only, not live task/write
  metrics.
- DDL accepts incomplete `consolidation.*` extension sets in schema; invalid
  configs are only noticed by config parsing where it is called.
- Queue depth, oldest queued window age, and materialization lag are not exposed
  through virtual tables, so operators cannot alert when async rollups fall
  behind.

Current branch progress:

- Added storage materialization scaffolding for target metadata, queued
  materialization requests, rollup mutation encoding, queue metrics, snapshots,
  and `late_window` stale/drop classification.
- Added storage streaming contracts: ring windows can be visited without
  materializing owned entry batches, built-in order-insensitive functions can
  emit aggregate results from a single pass, and boundary tasks now carry only a
  window descriptor instead of `Vec<RingEntry>` window contents.
- Materialization queue requests are now descriptor-only. They carry target
  metadata, partition key, window bounds, task kind, and retry count, but never
  carry `Vec<f64>` source-window values. Queue drain returns descriptors for a
  worker to stream.
- Rollup mutation encoding accepts an iterator of already-computed aggregate
  results. The only owned collection in that path is the bounded row-cell set
  required by the storage `Row` API.
- Ring allocation now has an optional memory-budget gate. Active rings are
  capped by both `max_rings` and the estimated ring heap budget; writes skip
  allocation and log a warning when no ring can fit, and repeated cold-ring
  evictions emit thrash warnings.
- Added `system_observability.materialization_queues` and
  `system_observability.materialization_status` virtual table scaffolding plus
  focused row-shape tests and subscription row-diff coverage.
- Materialization observability provider reads are callback-based, so a future
  live provider can visit queue/status rows without cloning a full provider
  snapshot. The legacy `VirtualTable::read` boundary still returns
  `Vec<VirtualRow>`.
- Registered the materialization virtual tables at startup with an in-memory
  provider placeholder.
- Extended consolidation function parsing so `wasm:keyspace.function` is a
  valid table-extension function and cascade metadata emits UDF-named columns.

Current branch limitations:

- Active consolidators are registered from table DDL/schema rebuild.
- The background materialization worker drains queues and writes target-table
  mutations through `StorageEngine`.
- The materialization drain path now uses the keyed storage cursor as the
  source of truth for window values instead of trusting the in-memory ring as a
  complete window. This prevents small/evicted rings from producing partial
  rollup rows.
- Late materialization tasks outside `consolidation.late_window` are dropped in
  the engine drain path, increment observable stale/drop counters, and do not
  rewrite existing rollup rows.
- Added CQL-router coverage proving normal CQL `CREATE TABLE` extensions and
  `INSERT` statements enqueue materialization and produce queryable target rows.
- Storage has an in-memory ring streaming path for current windows. Production
  late-window recomputation uses the keyed cursor API, but the current
  `TableStore` implementation still materializes one partition internally
  before visiting rows. A true memtable/SSTable row-streaming implementation
  remains required for very large partitions.
- WASM consolidation functions now require the streaming UDF aggregation ABI at
  executor start. Live RRD DDL accepts `wasm:keyspace.function` rollups so
  schema can reference registered aggregate components; materialization fails
  clearly if the referenced function is missing or lacks the streaming ABI.
- Median remains non-streaming because it requires ordered/materialized window
  state; it needs a bounded approximate sketch or external sort design before
  production materialization.
- Materialization virtual tables are backed by live storage queue state, though
  the current CQL trait still returns `Vec<VirtualRow>` at the outer boundary.

## History

- `b4f6f11f33ab2e0413f92a1f438d5bd5861e6298`
  (`2026-03-20T23:11:40-07:00`) added the harness-visible
  `examples/timeseries-rrd/schema.cql`, `data.cql`, and `queries.cql`.
- `397dc3bd5c82730eedfdcc4f422e3a760b727175`
  (`2026-03-20T23:19:51-07:00`) added richer RRD demo scripts
  (`built-in-functions.cql`, `composite.cql`, `custom-wasm-udf.cql`,
  `late-data.cql`, `median.cql`) that the example harness never ran.
- `663b36da40208e473d1e98349e3229cc352d6759`
  (`2026-03-20T23:26:02-07:00`) added storage integration coverage in
  `ferrosa-storage/tests/timeseries_cascade.rs`.
- `951fd07527a9d2d8a648e5e9a135d901e5c32526`
  (`2026-03-21T10:41:01-07:00`) added CQL cascade table creation and
  `system_observability.consolidation_status`, making the base example
  plausibly schema/query-passable.
- `246637372e6d974dd51d633c077f7048bdc13cb0`
  (`2026-04-09T14:28:16-07:00`) moved example testing from
  `.github/workflows/test-examples.yml` into `.github/workflows/ci.yml`; it did
  not remove the base RRD example from test discovery.

## Proposed change

Implement a real materialization path for tables configured with
`consolidation.*`:

1. Validate consolidation extensions during `CREATE TABLE` and `ALTER TABLE`.
   Required keys are `consolidation.interval`, `consolidation.functions`,
   `consolidation.target`, and `consolidation.columns`. Reject unknown
   functions, empty function lists, non-numeric source columns, zero intervals,
   zero capacities, self-referential targets, and cascade cycles.
2. Add a registry that owns active table consolidators. On schema load and DDL
   apply, instantiate `TimeSeriesAggregator` for every valid source table and
   register it with `StorageEngine`.
3. Add a worker loop per consolidator or a shared worker pool that consumes
   `ConsolidationTask`, builds target-table mutations, writes through
   `StorageEngine`, and routes downstream rollup writes through observers so
   cascades continue. Materialization is asynchronous: source writes may return
   before target rows are visible, but queued tasks must be durable enough to
   survive process failure or must be reconstructable by scanning source windows
   on recovery.
4. Define target row shape explicitly. Initial shape should use source partition
   key columns, the rollup window start timestamp as clustering key, and one
   `double` column per `(source_column, function)` pair, for example
   `value_min`, `value_max`, `value_avg`, `value_stddev`.
5. Repair cascade metadata generation so each downstream tier's
   `consolidation.columns` names match the columns actually emitted by the
   previous tier, or define terminal tiers explicitly if re-aggregating aggregate
   columns is not desired.
6. Recompute stale windows for late-arriving data up to a configurable window.
   `consolidation.late_window` remains the per-table control. Late writes inside
   the window enqueue a re-materialization task for the affected target row;
   writes older than the window are rejected for rollup correction or recorded
   as dropped-late data in observability counters.
7. Extend `consolidation_status` to expose live metrics:
   `windows_consolidated`, `late_arrivals`, `consolidation_drops`,
   `decode_failures`, last error, and target table.
8. Add materialization queue virtual tables, for example
   `system_observability.materialization_queues` and
   `system_observability.materialization_tasks`, with queue depth, oldest task
   age, source table, target table, window start, retries, last error,
   max configured delay, and whether the queue is alerting.
9. Ensure materialized target writes and materialization virtual tables work with
   the existing `SUBSCRIBE SELECT ... EVERY ... DELTA` operator. Users should be
   able to subscribe to target tables for new rollup rows and to virtual tables
   for lag/queue-state changes.

## Acceptance criteria

- [ ] `CREATE TABLE ... WITH extensions = {'consolidation.interval': '5m', ...}`
      registers an active consolidator without process restart.
- [ ] Invalid consolidation extension sets fail DDL with a useful error.
- [ ] Inserts that cross a window boundary create queryable rows in the target
      table.
- [ ] Cascaded rollups materialize through at least two tiers in an integration
      test.
- [ ] Late data inside `late_window` re-computes the affected target row.
- [x] Late data older than `late_window` increments an observable stale/drop
      counter and does not silently rewrite rollups.
- [x] Materialization remains asynchronous while exposing max observed lag,
      oldest queued task age, and queue depth.
- [ ] Queue and task virtual tables can be queried and subscribed with
      `SUBSCRIBE SELECT ... DELTA`.
- [ ] A delayed materialization queue can be detected by an alert rule using
      only virtual-table state.
- [x] `system_observability.consolidation_status` reflects live counters rather
      than configuration only.
- [ ] `examples/timeseries-rrd/schema.cql`, `data.cql`, and `queries.cql`
      verify real target rows, not only table existence.
- [ ] At least one extra RRD demo script is either made harness-visible or
      explicitly documented as non-testable example material.

## Verification commands

```bash
cargo test -p ferrosa-storage --test timeseries_cascade
cargo test -p ferrosa-cql --lib parse_create_table_with_extensions
```

Add a new end-to-end CQL or router integration test that creates
`examples/timeseries-rrd/schema.cql`, inserts data crossing a boundary, and
selects the expected aggregate row from `sensor_5m`.

## Related

- `ferrosa-storage/src/timeseries/`
- `ferrosa-storage/tests/timeseries_cascade.rs`
- `ferrosa-cql/src/router.rs::create_cascade_tables_if_needed`
- `ferrosa-cql/src/virtual_tables/consolidation_status.rs`
- `examples/timeseries-rrd/`
- `specs/archive/analysis/fmea-rrd-timeseries.md`
