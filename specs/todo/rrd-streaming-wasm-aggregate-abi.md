---
type: todo
priority: P0
status: draft
created: 2026-05-20
updated: 2026-05-20
---

# Streaming WASM aggregate ABI for RRD UDF rollups

## Why

Scalar/list-style WASM UDF execution is not acceptable for production RRD
rollups because it requires materialized windows. Sensor examples need a UDF
standard deviation path, but it must be stateful and streaming.

## Acceptance Criteria

- Define `init/update/finalize` or equivalent WASM aggregate contract.
- Reject non-streaming WASM functions in materialization config.
- Tests prove a WASM aggregate receives values one at a time and emits a bounded
  result.
- Docs show sensor stddev using the streaming aggregate ABI.

