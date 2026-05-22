---
type: todo
priority: P0
status: in-process
created: 2026-05-20
updated: 2026-05-22
---

# Async RRD worker writes target rollup rows

## Why

Boundary and late-data tasks are emitted, but no worker loop drains
descriptor tasks and writes target table mutations through storage.

## Acceptance Criteria

- Worker drains descriptor-only tasks.
- Worker streams source rows from ring or keyed storage cursor.
- Worker uses streaming consolidation for min/max/avg/stddev/count/sum.
- Worker writes one bounded target row mutation per window.
- End-to-end tests prove CQL inserts eventually produce queryable rollup rows.
- Late data inside `consolidation.late_window` recomputes affected windows.

## Progress Notes

- Added the synchronous worker core:
  `StorageEngine::process_one_time_series_materialization`.
- The core drains one descriptor, streams source values from the table ring via
  `RingBuffer::visit_window`, folds built-in functions without collecting source
  windows, and writes one bounded target mutation.
- Added deterministic overwrite timestamps for late-data recomputes so stale
  rollup cells are replaced without using wall-clock state.
- Tests now cover boundary materialization into queryable target rows and
  in-ring late-data recomputation.
- Added a background materialization worker owned by `Arc<StorageEngine>`.
- Derived rollup writes now dispatch storage observers, so downstream cascade
  tiers can enqueue their own materialization tasks.
- Tests now cover background worker materialization and a two-tier cascade.
- The drain path now computes rollups through
  `StorageEngine::visit_time_series_window_rows`, so target rows are derived
  from storage state rather than whichever subset of values remains in the
  active ring.
- Stale late-data tasks outside `consolidation.late_window` are dropped before
  recomputation, increment `stale_drops_total`, and leave existing rollup rows
  unchanged.
- Added CQL-router coverage for source/target DDL, source inserts, materializer
  drain, and target rollup reads.

## Still Not Working

- The keyed storage cursor is callback-shaped and is wired into the worker, but
  the current `TableStore` implementation still reads a full partition
  internally before visiting matching rows. Very large partitions still need a
  true memtable/SSTable streaming cursor.
- WASM aggregate functions are still rejected by the streaming materialization
  path until the streaming WASM aggregate ABI is implemented.
- End-to-end process supervision is still covered by the example script rather
  than an in-process router test with the background worker running.
