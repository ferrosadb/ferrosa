---
type: todo
priority: P0
status: in_progress
created: 2026-05-20
updated: 2026-05-22
---

# RRD materialization needs a keyed streaming storage cursor

## Why

Late-window recomputation cannot use `StorageEngine::read` or
`read_range`, because those APIs materialize full partitions or result
vectors. RRD workers need to stream rows for one `(table, partition key,
time window)` into aggregation accumulators.

## Acceptance Criteria

- [x] Add a storage API that visits rows for one partition/window without
  returning `Partition` or `Vec<Partition>`.
- [x] The API has tests proving it streams rows through a callback/iterator.
- [x] Missing partitions and empty windows produce zero visited rows.
- [x] The implementation uses existing SSTable row-streaming primitives where
  possible.
- [x] The API is documented as the only production path for stale rollup
  recomputation.

## Progress Notes

- Added `StorageEngine::visit_time_series_window_rows` and the corresponding
  `TableStore` visitor.
- The RRD materialization worker now uses this visitor as the source of truth
  for rollup windows, including when the in-memory ring is incomplete.
- `TableStore::visit_time_series_window_rows` no longer calls
  `TableStore::read`. It reads memtable rows by reference and uses
  `PartitionIter::next_partition_header_only` + `next_clustered_row` for
  SSTables, k-way merging duplicate clustering rows across sources before
  invoking the callback.
- Remaining hardening: skip directly to the first clustering timestamp in the
  requested window once row-index support is available for imported SSTables.
