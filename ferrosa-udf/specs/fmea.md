---
crate: ferrosa-udf
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-udf — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). This crate runs untrusted guest code on the hot query path, so
sandbox-escape and determinism failures dominate the top of the table.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| UDF-1 | Non-deterministic UDF result (guest reads wall-clock-ish host state, or relies on f32/f64 NaN-bit or rounding differences) | Same row yields different values across replicas/reruns → divergent reads, broken UDA merge | 9 | 5 | 8 | 360 | **Open gap.** No host-side determinism enforcement beyond the sandbox having no clock/RNG imports wired in `Linker::new` (an *empty* linker — guest gets no imports). There is no positive determinism test, no float-mode pinning, and no audit that a compiled component imports nothing. Add a "component imports must be empty" check at `compile` and a cross-instance determinism test. |
| UDF-2 | Epoch ticker interval == `max_execution_time`, so the wall-clock timeout can overshoot by up to ~2× | A runaway guest can run nearly twice the configured budget before the epoch deadline (`set_epoch_deadline(1)`) fires | 6 | 6 | 6 | 216 | **Known.** `udf-epoch-ticker` sleeps `max_execution_time` then increments once; with deadline=1 the worst case is two intervals. Fuel (`max_fuel`) is the primary bound; epoch is the backstop. Tighten by ticking at a fraction of the timeout. |
| UDF-3 | Error-message string matching to classify traps (`msg.contains("fuel")` / `"epoch"`) | A Wasmtime version or locale change to trap text silently reclassifies `ResourceExhausted` as `ExecutionFailed` (or vice versa) | 5 | 4 | 7 | 140 | **Open gap.** `map_component_call_error` + the inline closures in `call`/`call_by_key` parse `e.to_string()`. Prefer `e.downcast_ref::<wasmtime::Trap>()` and match `Trap::OutOfFuel` / interrupt explicitly. |
| UDF-4 | asc-compiled UDF restricted to numeric/text/ascii/blob; collection/temporal/decimal args silently unavailable | `CREATE FUNCTION ... LANGUAGE assemblyscript` over a `list`/`map`/`decimal`/`timestamp` arg is rejected at compile | 4 | 5 | 3 | 60 | **By design, documented.** `component.rs::abi_type` returns a clear `AscError::Compile` naming the type; the reduced WIT world drops recursive cases. Fails loud, not silent. A msgpack-blob bridge is the planned path (roadmap). |
| UDF-5 | Pooled instance reuse carries guest mutable globals across calls | A scalar UDF that mutates a WASM global sees state from a prior invocation → non-deterministic / leaked data between rows or tenants | 8 | 3 | 7 | 168 | **Partial.** `call()` resets fuel + epoch on the warm store but does **not** reset guest linear memory or globals; correctness relies on the canonical post-return `__reset` (asc adapter) and well-behaved guests. Hand-written components that hold global state would observe carryover. No test asserts pool-reuse isolation. |
| UDF-6 | `max_memory_bytes` in `SandboxConfig` is not enforced on the `Store` | A guest can grow linear memory past the configured cap (up to Wasmtime/OS limits) → host memory pressure | 7 | 3 | 6 | 126 | **Open gap.** `SandboxConfig.max_memory_bytes` (default 16 MB) is defined and tested for its value, but no `StoreLimits`/`ResourceLimiter` is installed in `new`/`call`. Wire `Store::limiter` to enforce it. |
| UDF-7 | Lock poisoning on the registry `RwLock` panics every subsequent call | A panic while holding the registry write lock takes down all UDF execution process-wide | 7 | 1 | 4 | 28 | **Accepted.** `.expect("registry lock poisoned")` is fail-loud per repo policy; the held critical sections are small and panic-free. |
| UDF-8 | UDA `merge`/`serialize-state` correctness depends entirely on the guest | Distributed aggregation produces wrong results if the guest's serialize/merge is not associative/commutative | 8 | 3 | 8 | 192 | **Out of crate scope but undertested here.** The `uda` world is exposed via raw `(Store, Instance)` (`create_uda_instance`); this crate has **no** in-crate UDA round-trip or merge test — all UDA driving + coverage lives in `ferrosa-cql`. |
| UDF-9 | asc compiler thread is process-lifetime and never dropped (by design) | QuickJS runtime + 15 MB binaryen held for process life; a wedged compile job blocks the single `sync_channel(0)` serially | 4 | 3 | 5 | 60 | **By design, documented in `asc.rs`.** The runtime cannot be safely dropped (GC cycle assertion); a persistent thread amortises init. Compiles are serialized — a slow/hostile source delays others. No per-compile timeout. |

## Top risks to act on

1. **UDF-1 (RPN 360)** — determinism is the highest risk and is essentially
   unguarded today. The empty `Linker` denies host imports (good), but nothing
   asserts a component imports nothing, pins float behaviour, or proves
   cross-instance reproducibility. This directly threatens replica convergence
   and UDA correctness.
2. **UDF-2 (RPN 216)** — the wall-clock timeout can overshoot ~2×; tighten the
   ticker cadence relative to the deadline.
3. **UDF-8 (RPN 192)** / **UDF-5 (RPN 168)** — UDA correctness and pool-reuse
   isolation have **no in-crate test**; the green build is not a real safety
   signal for those paths.

## Detection assets

- In-crate: 42 conversion round-trip tests (`convert`), 36 executor tests
  (registry/pool lifecycle, `Val` encode/decode round-trips, fuel/epoch engine
  config, streaming-aggregate stddev end-to-end), 2 `SandboxConfig` tests.
- `asc`/`component`: componentization-pipeline tests under `--features asc-udf`;
  full toolchain tests under `--features 'asc-udf live-infra-tests'` +
  `FERROSA_ASC_BUNDLE` (panic with setup instructions if absent).
- Out-of-crate: `ferrosa-cql` exercises DDL replication, query-time invocation,
  the UDA driver (`wasm_aggregate.rs`), and the asc compile path (`router.rs`).
