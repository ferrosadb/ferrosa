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

- [x] Define `init/update/finalize` or equivalent WASM aggregate contract.
- [x] Reject non-streaming WASM functions before materialization execution.
- [x] Tests prove a WASM aggregate receives values one at a time and emits a
  bounded result.
- [x] Docs show sensor stddev using the streaming aggregate ABI.
- [x] RRD materialization worker invokes the ABI for `wasm:keyspace.function`
  rollups without materializing a window.

## Progress Notes

- Live RRD table config now accepts `wasm:keyspace.function` rollups. CQL-loaded
  WASM bytes carrying the ABI marker compile as `FunctionKind::Aggregate`; scalar
  function registrations are rejected when the materializer attempts to start
  an aggregate invocation.
- `UdfExecutor::compile_streaming_aggregate` exists and requires a
  `ferrosa:streaming-aggregate:v1` component custom-section marker before it
  registers a function as `FunctionKind::Aggregate`.
- `UdfExecutor::start_streaming_aggregate` returns a stateful invocation object
  that calls `init`, accepts one `update(value)` call per row, and then
  `finalize`s to one `f64`.
- Executor tests cover marker rejection, aggregate-kind registration, scalar
  rejection, and a stddev Component Model aggregate driven one value at a time.
- The `custom-wasm-udf.cql` example documents the streaming Rust/Welford stddev
  shape.
- The RRD worker starts one invocation per WASM rollup function, streams decoded
  row values through `update(value)`, and writes the `finalize()` result in the
  target rollup row. The storage test uses a Rust Welford stddev implementation
  behind the storage trait; the executor test uses a real Component Model
  stddev aggregate.

## Contract v1

RRD WASM aggregates must be Component Model components with custom section
`ferrosa:streaming-aggregate:v1` and these exports:

- `init()`: initialize bounded component-instance state.
- `update(value: float64)`: consume exactly one numeric sample.
- `finalize() -> float64`: emit one numeric rollup result.

The materializer must stream source rows into `update`; it must not pass
`list<double>` or any materialized window to WASM.

## Implementation Notes

- `ferrosa-udf` exposes `start_streaming_aggregate`, returning a live invocation
  with `update` and `finalize`.
- `ferrosa-cql` compiles `CREATE FUNCTION ... LANGUAGE wasm` bodies with the
  ABI marker as aggregate functions and installs a storage adapter at startup.
- `ferrosa-storage` owns the row-streaming materialization loop and depends only
  on a small `TimeSeriesWasmAggregateExecutor` trait, so it never needs to
  collect window values for WASM.
