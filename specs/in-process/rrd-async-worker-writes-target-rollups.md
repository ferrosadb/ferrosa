---
type: todo
priority: P0
status: in-process
created: 2026-05-20
updated: 2026-05-20
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

## Still Not Working

- Late data is recomputed only from values still present in the ring. The keyed
  storage cursor path for windows that have fallen out of RAM is not wired.
- `consolidation.late_window` classification is specified/tested at the
  descriptor level, but the engine drain path does not yet drop stale tasks.
- WASM aggregate functions are still rejected by the streaming materialization
  path until the streaming WASM aggregate ABI is implemented.
- CQL-level eventual rollup tests are not complete yet; current coverage is at
  the storage-engine layer.
