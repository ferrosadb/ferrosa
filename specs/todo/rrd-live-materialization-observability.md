---
type: todo
priority: P0
status: draft
created: 2026-05-20
updated: 2026-05-20
---

# Live materialization observability without snapshot Vecs

## Why

`system_observability.materialization_*` currently has schema scaffolding and a
callback-shaped provider, but the global virtual table path still materializes
`Vec<VirtualRow>` and startup registration uses an in-memory placeholder.

## Acceptance Criteria

- Live queue/status providers read storage-backed metrics/descriptors.
- CQL virtual table reads are paged or streamed so queue observability is
  bounded.
- DELTA subscriptions can observe virtual table changes without retaining
  unbounded previous snapshots.
- Tests prove queue lag, oldest task age, and alerting state are visible.

