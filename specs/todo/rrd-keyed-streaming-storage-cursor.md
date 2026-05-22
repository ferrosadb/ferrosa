---
type: todo
priority: P0
status: draft
created: 2026-05-20
updated: 2026-05-20
---

# RRD materialization needs a keyed streaming storage cursor

## Why

Late-window recomputation cannot use `StorageEngine::read` or
`read_range`, because those APIs materialize full partitions or result
vectors. RRD workers need to stream rows for one `(table, partition key,
time window)` into aggregation accumulators.

## Acceptance Criteria

- Add a storage API that visits rows for one partition/window without
  returning `Partition` or `Vec<Partition>`.
- The API has tests proving it streams rows through a callback/iterator.
- Missing partitions and empty windows produce zero visited rows.
- The implementation uses existing SSTable row-streaming primitives where
  possible.
- The API is documented as the only production path for stale rollup
  recomputation.

