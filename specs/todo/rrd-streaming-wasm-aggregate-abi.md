---
type: todo
priority: P0
status: in_progress
created: 2026-05-20
updated: 2026-05-22
---

# Streaming WASM aggregate ABI for RRD UDF rollups

## Why

Scalar/list-style WASM UDF execution is not acceptable for production RRD
rollups because it requires materialized windows. Sensor examples need a UDF
standard deviation path, but it must be stateful and streaming.

## Acceptance Criteria

- [ ] Define `init/update/finalize` or equivalent WASM aggregate contract.
- [x] Reject non-streaming WASM functions in materialization config.
- [ ] Tests prove a WASM aggregate receives values one at a time and emits a bounded
  result.
- [ ] Docs show sensor stddev using the streaming aggregate ABI.

## Progress Notes

- Live RRD table config now rejects `wasm:keyspace.function` rollups during
  config parse / CQL DDL with a clear "streaming WASM aggregate ABI" error.
- The `custom-wasm-udf.cql` example documents the future syntax and states that
  it is intentionally rejected in this release.
- The actual streaming ABI design and execution path remain open.
