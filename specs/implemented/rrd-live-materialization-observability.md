---
type: todo
priority: P0
status: in_progress
created: 2026-05-20
updated: 2026-05-22
---

# Live materialization observability without snapshot Vecs

## Why

`system_observability.materialization_*` currently has schema scaffolding and a
callback-shaped provider, but the global virtual table path still materializes
`Vec<VirtualRow>` and startup registration uses an in-memory placeholder.

## Acceptance Criteria

- [x] Live queue/status providers read storage-backed metrics/descriptors.
- [x] CQL virtual table reads are paged or streamed so queue observability is
  bounded.
- [x] DELTA subscriptions can observe virtual table changes without retaining
  unbounded previous snapshots.
- [x] Tests prove queue lag, oldest task age, and alerting state are visible.

## Progress Notes

- Startup registers `materialization_queues` and `materialization_status` with
  a storage-backed provider.
- Storage exposes bounded per-consolidator queue/status descriptors:
  queue depth, oldest task age, max delay, alerting state, pending/completed
  counters, and stale drops.
- Subscription row-diff coverage includes materialization virtual-table rows.
- Added `VirtualTable::visit_rows` as the streaming virtual-table boundary.
  Existing tables keep the default `read` adapter, while
  `materialization_queues` and `materialization_status` override it to emit one
  row at a time from the storage-backed provider.
- The CQL router now encodes virtual table rows through `visit_rows` and patches
  the protocol row count after streaming, avoiding `Vec<VirtualRow>` and
  `Vec<Vec<CqlValue>>` materialization on the virtual-table path.
