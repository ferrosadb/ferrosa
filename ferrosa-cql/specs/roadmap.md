---
crate: ferrosa-cql
doc: roadmap
last_updated: 2026-06-30
---

# ferrosa-cql — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), in-code "not yet implemented"
markers, and the dependency/usage review. In-code TODO density is very low; the
real backlog is structural and security-shaped.

## Now (highest value)

- **Sign / scope-bind the `paging_state` cursor** (FMEA CQL-2). Today
  `PagingState::encode/decode` is an unsigned length-prefixed pk+ck+flag. Add an
  HMAC with a per-server key (or bind the cursor to the originating query/prepared
  id) so a tampered token cannot resume at an attacker-chosen partition.
- **Decompose `router.rs`** (FMEA CQL-5). At 22.4k LoC it is the dominant review
  risk. Split into per-family submodules (`router/dml.rs`, `router/ddl.rs`,
  `router/roles.rs`, `router/types_functions.rs`) keeping `route()` as the thin
  dispatcher. This directly lowers the chance of a missing permission check
  (CQL-4) slipping through.
- **Enforce the M8 permission-check invariant by test** (FMEA CQL-4). Add a test
  (or lint) asserting every `route_*` handler calls `check_permission`, so a new
  handler cannot ship without authorization.

## Next

- **Streaming range reads — remove the server-side result cap**
  (`specs/proposed/streaming-range-reads-no-cap.md`). Steps 1–2 landed:
  - Step 1: the *projected* full-scan arm in `router.rs` consumes the uncapped
    streaming variant `range_read_projected_stream_all_with` (move-only, no
    `Vec<Partition>` materialization), bounded solely by the query's `LIMIT`.
  - Step 2: the `DEFAULT_RANGE_READ_LIMIT` (10_000) **result cap** is removed
    from the O(1)-streamable shapes. `SELECT SUM/MIN/MAX/AVG` over a full scan
    now folds through an O(1) streaming accumulator
    (`stream_builtin_aggregates`) over the uncapped
    `range_read_stream_all_with` — exact over the whole table, no `all_rows`
    materialization. A user `LIMIT N` above the storage OOM guard streams (take-`N`
    partitions) instead of hitting the Vec-materialization cap. `SELECT DISTINCT
    <partition key>` and simple `WHERE … ALLOW FILTERING` scans were already
    uncapped (step 1 / paged-filter path).
  - Step 5 (landed): the unbounded `ORDER BY` (no `LIMIT`) global sort now
    **spills** instead of fail-loud-refusing. The router streams the uncapped
    scan through `sort_rows_from_partition_stream_spilling` →
    `ferrosa_storage::ExternalSorter` (bounded-memory external merge sort with
    cascade k-way merge; RAM-budget reader + 50%-default spill threshold in
    `ferrosa_storage::spill_budget`). Complete, correctly ordered, memory bounded
    by the threshold, no cap other than the query's `LIMIT`. `DISTINCT`/aggregate/
    function-projection keep their `range_read_limited_rows_checked` fail-loud cap.
    Remaining: per-group spill when `GROUP BY` lands (deferred; not yet parsed).
  - `fts_match` arm bounded (t_ee98faa0, landed): the full-text branch no longer
    accumulates every matching row before LIMIT (the live `hybrid_search` OOM
    shape). LIMIT early-exits the partition fetch loop (peak ≈ limit rows + one
    partition); no-LIMIT pages with a partition-granular `PagingState` cursor in
    deterministic partition-key order. Residual: the O(matches) hit-set of small
    doc keys. See `specs/proposed/streaming-range-reads-no-cap.md`.
- **CQL native protocol v5 — remaining gaps** (follow-up to v3/v4/v5 conformance
  work). The server now accepts v5, enables modern framing with multi-envelope
  decode, emits the v5 `result_metadata_id` in PREPARE/EXECUTE responses, and
  passes 37/38 DataStax Java driver smoke tests. Remaining gaps:
  - **DROP KEYSPACE schema-agreement race** (FMEA CQL-11): the DataStax driver
    closes its control connection after CREATE INDEX and the new control
    connection subscribes ~2 ms too late to receive the `DROP KEYSPACE` event.
    The `watch` channel fallback partially mitigates this.
  - Full query-parameter flag parsing for v5 QUERY/EXECUTE (page size,
    default timestamp, serial consistency, keyspace override, `now_in_seconds`).
  - `EXECUTE` `Skip_metadata` flag and `Metadata_changed` result-set metadata
    responses.
  - v5 error-body extensions (`WriteFailure`/`ReadFailure` endpoint maps).
  - Continuous paging / backpressure.
- **Complete LWT-on-Accord for non-cluster modes** (FMEA CQL-1). The CQL layer
  fails loud when `peer_manager`/`accord_clock` are absent; finish the coordinator
  driver (`fix/p0-03b-accord-network`, p0-03b gap) so standalone/pair-mode LWT
  executes rather than erroring.
- **Bound per-tick subscription work** (FMEA CQL-6). Add a per-subscription
  row/byte budget and backpressure so a broad subscribed SELECT cannot re-scan a
  large table every interval.
- **Subscription/CDC delta correctness tests** against `ferrosa-cdc` — verify the
  dual-timestamp (Accord ts / apply ts) ordering under concurrent writes.

## Later

- **`COMPACT` support** (FMEA CQL-7) — only if a real operator need emerges; UCS
  compaction runs automatically, so this stays a deliberate non-feature for now.
- **Collections / UDT / tuple / vector round-trip coverage** at the CQL boundary,
  tracking the matching `ferrosa-row-bridge` type-matrix work so unsupported types
  fail loud rather than decoding to NULL.
- **Prepared-statement cache observability** — expose hit/miss/evict counters via
  the Prometheus endpoint to tune the W-TinyLFU weight budget.

## Non-goals

- The row encode/decode codec — it lives in `ferrosa-row-bridge` (D10) and is only
  re-exported here. Changes to encoding belong there, not in this crate.
- Cassandra internode wire compatibility — Ferrosa uses its own internode protocol
  (`ferrosa-net`); only the *client* CQL protocol is Cassandra-compatible.
