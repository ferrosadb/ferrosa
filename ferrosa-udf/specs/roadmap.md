---
crate: ferrosa-udf
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-udf — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), the in-code design notes
(`asc.rs`, `component.rs`), and the dependency/usage review. No in-code
`TODO`/`FIXME` markers exist; the items below come from the gap analysis.

## Now (highest value)

- **Enforce + prove determinism** (FMEA UDF-1). Assert at `compile` that a
  registered component imports nothing (the `Linker` is already empty, so any
  import is a latent failure), and add a cross-instance test that the same args
  yield the same result. This guards replica convergence and UDA merge.
- **Classify traps structurally, not by string** (FMEA UDF-3). Replace
  `e.to_string().contains("fuel"/"epoch")` in `call`, `call_by_key`, and
  `map_component_call_error` with `e.downcast_ref::<wasmtime::Trap>()` matches on
  `OutOfFuel` / interrupt so trap-text changes can't silently reclassify errors.

## Next

- **Enforce `max_memory_bytes`** (FMEA UDF-6). Install a `Store` limiter
  (`ResourceLimiter` / `StoreLimits`) so the configured per-invocation memory cap
  is actually applied — today it is config-only.
- **Tighten the epoch timeout** (FMEA UDF-2). Tick the engine epoch at a fraction
  of `max_execution_time` (or raise the deadline count) so the wall-clock bound
  cannot overshoot ~2×.
- **In-crate UDA + pool-isolation coverage** (FMEA UDF-8, UDF-5). Add round-trip
  tests for the `uda` world (`init`/`accumulate`/`merge`/`serialize-state`/
  `finalize`) and a test that a pooled scalar instance does not leak guest global
  state across calls. Both paths are currently only exercised from `ferrosa-cql`.

## Later

- **Collection/temporal/decimal asc UDFs** (FMEA UDF-4). The reduced
  componentization WIT world drops recursive cases; bridge `list`/`set`/`map`/
  `tuple`/`udt` (and temporal/decimal) across the canonical ABI — the design note
  in `component.rs` points at a msgpack-blob `collection-val` approach.
- **Bounded inline-compile** (FMEA UDF-9). Add a per-compile timeout/cancellation
  around the serialized asc compiler thread so a slow or hostile source cannot
  stall the single-job channel indefinitely.

## Non-goals

- CQL parsing, schema metadata, DDL replication, or query planning — those belong
  to `ferrosa-cql` / `ferrosa-session`, which orchestrate this crate.
- Languages other than WASM components and (behind `asc-udf`) AssemblyScript.
