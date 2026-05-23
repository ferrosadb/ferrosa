---
type: todo
priority: P0
status: implemented
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
- `TableStore::visit_time_series_window_rows` now streams matching SSTable
  partitions through `PartitionIter::next_partition_header_only` and
  `next_clustered_row`.
- Overlapping active/flushing memtable and SSTable sources are merged by
  clustering key with cell-level last-write-wins before the visitor sees each
  row.
- The cursor keeps one row head per contributing source instead of
  materializing a full SSTable partition.
- In-memory memtable sources still contribute cloned rows for the exact
  partition key, which is acceptable because memtables are already resident.
