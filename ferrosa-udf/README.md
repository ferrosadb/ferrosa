# ferrosa-udf

> WASM-sandboxed execution for CQL User-Defined Functions and Aggregates, plus
> an optional inline AssemblyScript→WASM compiler. All Wasmtime internals are
> encapsulated here — callers see only `UdfExecutor`.

## What this crate is

The execution substrate for CQL `CREATE FUNCTION` / `CREATE AGGREGATE`. It owns
the Wasmtime Component Model engine, a compiled-component registry, an instance
pool, and the bidirectional conversion between `ferrosa_common::CqlValue` and the
WIT `cql-value` ABI. Callers (`ferrosa-cql`, `ferrosa-session`, `ferrosa-ctl`,
`ferrosa`) hand it WASM component bytes plus a CQL signature and get back a typed
result — they never touch Wasmtime, fuel, or the WIT encoding directly.

A UDF is a WASM **component** exporting `invoke(list<cql-value>) -> result<cql-value, string>`.
A streaming aggregate is a component exporting `init` / `update(f64)` / `finalize() -> f64`,
gated behind a `ferrosa:streaming-aggregate:v1` custom-section marker.

## What's implemented

- **`UdfExecutor`** — Wasmtime Component Model engine with fuel metering + epoch
  interruption. Compiles, caches, resolves, invalidates, and invokes components.
  Thread-safe (`Arc`-shareable).
- **Scalar UDF invocation** — `compile`, `resolve`/`call`/`call_by_key`. The
  conversion chain is `CqlValue` → `WitCqlValue` → `Val::Variant` → WASM →
  `Val::Result` → `WitCqlValue` → `CqlValue`.
- **Streaming aggregates** — `compile_streaming_aggregate`,
  `call_streaming_aggregate`, `start_streaming_aggregate` →
  `StreamingAggregateInvocation::{update, finalize}` (the RRD-rollup `f64` path).
- **UDA instances** — `create_uda_instance[_by_key]` return a raw
  `(Store, Instance)` for the caller (`ferrosa-cql`) to drive `init`/`accumulate`/
  `merge`/`serialize-state`/`finalize` per the `uda` WIT world.
- **Registry** — `SlotMap`-backed `(keyspace, name, arg_types)` → component,
  O(1) `FunctionKey` lookup on the hot path; CREATE-OR-REPLACE and DROP semantics
  (`invalidate` drains the pool so stale code is never reused).
- **Instance pool** — up to 8 warm `(Store, Instance)` pairs per function;
  acquire/release on `call()` to amortise instantiation.
- **Type conversion** (`convert`) — `cql_to_wit` / `wit_to_cql` for all 26
  `CqlValue` cases (scalars, collections, tuple, UDT; `Vector` maps to a list of
  floats).
- **Sandbox limits** (`SandboxConfig`) — memory, per-call fuel, per-aggregate
  fuel, wall-clock timeout (epoch), cache capacity, max WASM upload size.
- **Inline AssemblyScript compiler** (feature `asc-udf`, modules `asc` +
  `component`) — runs the `asc`+`binaryen` JS toolchain inside QuickJS with a
  wasmtime-backed `WebAssembly` shim, then componentizes the core module to the
  `udf` WIT world via `wit-component`. Powers `CREATE FUNCTION ... LANGUAGE assemblyscript`.

## How it works

| Module | Responsibility |
|--------|----------------|
| `executor` (`src/executor.rs`, ~1.9k LoC) | `UdfExecutor`, registry, instance pool, `Val`↔`WitCqlValue` encode/decode, fuel/epoch wiring |
| `convert` (`src/convert.rs`, ~750 LoC) | `WitCqlValue` enum + `cql_to_wit` / `wit_to_cql` (type-directed) |
| `sandbox` (`src/sandbox.rs`) | `SandboxConfig` resource limits |
| `arena` (`src/arena.rs`) | `UdfArena` per-query bump allocator for arg lowering |
| `error` (`src/error.rs`) | `UdfError` |
| `asc` (`src/asc.rs`, feature `asc-udf`) | inline AssemblyScript→core-WASM compiler (QuickJS + wasmtime shim) |
| `component` (`src/component.rs`, feature `asc-udf`) | core-WASM → `udf`-world component (adapter codegen + `wit-component`) |

The WIT `cql-value` variant is **recursive** (list/set/map/tuple/udt contain
`cql-value`), so `wasmtime::component::bindgen!` cannot be used. The executor
hand-encodes `Val::Variant` by kebab-case discriminant name instead. The WIT
contract lives at `src/wit/ferrosa-udf.wit` (worlds `udf` and `uda`).

## Public API (key entry points)

| Area | Items |
|------|-------|
| Executor | `UdfExecutor::new`, `compile`, `compile_streaming_aggregate`, `resolve`, `get_kind`, `invalidate` |
| Scalar invoke | `call`, `call_by_key` |
| Aggregate | `call_streaming_aggregate`, `start_streaming_aggregate`, `StreamingAggregateInvocation`, `create_uda_instance[_by_key]`, `wasm_declares_streaming_aggregate_abi` |
| Config / types | `SandboxConfig`, `FunctionKind`, `UdfArena`, `UdfError` |
| Conversion | `convert::{WitCqlValue, cql_to_wit, wit_to_cql}` |
| AssemblyScript (`asc-udf`) | `asc::compile_assemblyscript`, `component::{compile_to_component, componentize}`, `asc::AscError` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — `CqlValue`, `CqlType` (the type model it converts to/from
  the WIT ABI).

External: `wasmtime` (component-model), `slotmap`, `num-bigint`, `uuid`, `bumpalo`,
`thiserror`, `tracing`. Under `asc-udf`: `rquickjs`, `tokio`, `wit-component`,
`wit-parser`.

**Called by** (crates that depend on this):

- **`ferrosa`** — constructs the process-wide `UdfExecutor`.
- **`ferrosa-cql`** — DDL replication of `CREATE FUNCTION`/`AGGREGATE`, query-time
  invocation, the streaming-aggregate driver (`wasm_aggregate.rs`), the
  AssemblyScript compile path (`router.rs`), and `From<UdfError> for CqlError`.
- **`ferrosa-ctl`** — cluster/UDF management surface.
- **`ferrosa-session`** — holds the shared `Arc<UdfExecutor>` in session context.

## Tests

89 `#[test]` functions in-crate. Default `cargo test -p ferrosa-udf` (no features)
runs the conversion round-trips (`convert`), the executor registry/pool/encoding
tests (`executor`), and the `SandboxConfig` tests (`sandbox`).

The `asc`/`component` end-to-end tests are feature-gated: the componentization
pipeline tests build under `--features asc-udf`; the tests that actually run the
15 MB asc+binaryen toolchain require `--features 'asc-udf live-infra-tests'` and
`FERROSA_ASC_BUNDLE` pointing at a bundle built by
`examples/asc-poc/build-bundle.sh` (they `panic!` with setup instructions if it
is absent, per repo test policy — no silent skips).

```bash
cargo test -p ferrosa-udf
cargo test -p ferrosa-udf --features asc-udf
FERROSA_ASC_BUNDLE=/tmp/asc-host/asc-bundle.mjs \
  cargo test -p ferrosa-udf --features 'asc-udf live-infra-tests'
```

## Specs

- [Architecture overview](specs/overview.md) — module map, ABI, invariants, data flow
- [FMEA / known issues](specs/fmea.md) — failure modes + gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
