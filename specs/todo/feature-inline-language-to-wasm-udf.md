---
type: design
priority: P3
status: open
created: 2026-05-31
context: "CQL UDF gap-fill — ferrosa runs WASM UDFs, not Java. Explore compiling a high-level language to WASM inline at CREATE FUNCTION time."
---

# Design: inline language → WASM for `CREATE FUNCTION`

> **Status (2026-05-31): IMPLEMENTED for numeric UDFs** on branch
> `feature-inline-udf-compile` (commits `271bb62`→`3c376b4`). `CREATE FUNCTION …
> LANGUAGE assemblyscript AS '<src>'` compiles inline AS → core wasm
> (`ferrosa_udf::asc`) → udf-world component (`ferrosa_udf::component`) → executor,
> for `int/bigint/float/double/smallint/tinyint`. Behind the `asc-udf` feature
> (ferrosa-udf + ferrosa-cql); the 15 MB asc bundle is a runtime artifact via
> `FERROSA_ASC_BUNDLE`. **Remaining:** text/blob/collection arg+return types (need
> linear-memory marshalling / a `collection-val` msgpack bridge); caching/perf;
> resource bounds + STRIDE on the source-as-input surface. See the "Path to
> production" and Componentization findings below.

## Spike findings (2026-05-31) — binaryen REQUIRES the WebAssembly API

Empirically tested (`npm i assemblyscript binaryen`, node 25; Javy 8.1.1;
wasmtime 45 CLI):

1. **Correction to an earlier result.** A first test appeared to show the
   toolchain compiling with `globalThis.WebAssembly` deleted — that was a **false
   positive from ES-module import hoisting**: the static `import` ran (loading
   binaryen *with* `WebAssembly` present) *before* the `delete` statement
   executed. Denying `WebAssembly` *before* binaryen loads (dynamic import, or in
   a JS engine that simply lacks it) makes binaryen **fail at its
   `WebAssembly.instantiate`** (`_r` in `binaryen/index.js`). So:
   **binaryen (the npm Emscripten build) requires the `WebAssembly` API** to run
   its codegen. It contains `wasm2js` symbols, but that pure-JS fallback is **not
   auto-selected** when `WebAssembly` is merely absent.
2. **It does drive fully in-memory.** `asc.main(args, { readFile, writeFile,
   listFiles })` with callbacks compiles a source **string** to wasm **bytes**
   with no `fs`/WASI and no CLI. (Still true and useful.)
3. **Vendored footprint ≈ 15 MB of JS** (`asc.js` + `assemblyscript.js` +
   `binaryen/index.js`); the native `binaryen/bin/` is not needed.

### The Javy-guest-in-Wasmtime PoC ran, then hit exactly this

`esbuild` bundles the in-memory driver + asc + binaryen to a ~15 MB ESM (node
builtins externalized as dead code). `javy build -J event-loop=y` produced a
41 MB `compiler.wasm`. Running it under `wasmtime` (stdin = AS source) **executed
QuickJS and asc all the way to binaryen codegen**, then aborted with
`WebAssembly is not defined` → with a `validate` stub, `WA.instantiate
unsupported`. QuickJS (and thus a Javy/`quickjs-wasi` guest) has **no
WebAssembly**, and nesting a real WASM engine inside the guest is the unsolved
part. So the pure-guest path is **blocked** by binaryen's WebAssembly need.

### Revised recommendation

Two viable paths; pick one:

- **(A) Host-side JS engine + Wasmtime-backed `WebAssembly` shim** (recommended).
  Embed Boa (pure Rust, permissive) or `rquickjs` (QuickJS, MIT) in the ferrosa
  process to run `asc.js` + `binaryen.js`. Provide a global `WebAssembly` whose
  `compile`/`instantiate`/`Module`/`Instance`/`Memory` are **backed by ferrosa's
  existing wasmtime** — so binaryen's real (fast) WASM executes *in our
  wasmtime*, sandboxed, while the JS engine just orchestrates on the host. The
  fiddly part is the `Memory` ↔ ArrayBuffer bridge and exported-function
  marshalling. This still satisfies "use the wasmtime we have" for the heavy work.
- **(B) Produce a `WASM=0` binaryen** (Emscripten pure-JS, zero WebAssembly
  references) so the whole toolchain runs in a Javy/QuickJS guest *inside*
  wasmtime with no shim. Requires building binaryen with `-s WASM=0` (an
  Emscripten toolchain build) or forcing its `wasm2js` path — an artifact-
  production effort, but then the runtime story is the cleanest (all-in-wasmtime,
  no host JS engine).

The remaining `asc.js`/Emscripten environment shims (TLA→ESM, externalised node
builtins, event loop) are solved; the open fork is **(A) host-engine + WASM shim**
vs **(B) WASM=0 binaryen**.

### Bundling details (still valid)

- `asc.js` uses **top-level await** → bundle as **ESM** (not IIFE/CJS).
- `asc.js` has no static node imports; `binaryen` (Emscripten) does
  `require("fs"|"path"|"url"|...)` and conditional `import("node:...")`, all dead
  code under the callback I/O → externalize all node builtins → single ~15 MB
  ESM. `javy build -J event-loop=y -J text-encoding=y -J javy-stream-io=y`
  produced a 41 MB `compiler.wasm`.
- The remaining engine work is **not** the node/Emscripten shims (solved) — it is
  binaryen's `WebAssembly` requirement (see the corrected findings above). That
  decides the fork: **(A) host JS engine + Wasmtime-backed WebAssembly shim** vs
  **(B) WASM=0 binaryen for an all-in-Wasmtime guest**.

## PoC progress — Path A (host rquickjs + wasmtime-backed `WebAssembly`)

`ferrosa-udf/examples/asc_compile_poc.rs` (rquickjs 0.9 async; build the JS bundle
with `ferrosa-udf/examples/asc-poc/build-bundle.sh`). Empirically established:

- **rquickjs runs the asc + binaryen toolchain.** With a `console` shim added
  (binaryen's Emscripten init uses `console`; QuickJS lacks it), the 15 MB ESM
  bundle's top-level await (binaryen init) runs to completion and reaches
  binaryen's `WebAssembly.instantiate`. So QuickJS-on-the-host can drive the
  whole toolchain; the only host shims needed are `console`, `performance.now`,
  and `WebAssembly`.
- **Captured the exact shim surface** (the instrumented `WebAssembly` stub):
  - binaryen instantiates an **~8.77 MB** wasm (its embedded module).
  - **one import namespace `a`** with **~80 functions** (Emscripten env glue,
    minified names: `b, ja, u, x, k, U, w, …`). These are JS functions the
    bundle supplies in the import object — the wasm calls back into them.
  - binaryen's wasm **exports its memory**; Emscripten's JS reads/writes that
    linear memory directly via the `Memory.buffer` ArrayBuffer.

### Binaryen wasm analyzed in wasmtime (`examples/wasm_imports.rs`)

Dumped binaryen's wasm (`examples/asc_compile_poc.rs` writes it via `__dumpWasm`)
and loaded it in wasmtime 44 — **wasmtime accepts it**. Surface:

- **82 function imports, all in namespace `a`**, and **all params/results are
  numeric** (`i32`/`i64`/`f32`/`f64`) — no `externref`/`funcref`. Signatures
  range from `() -> ()` to 16×`i32 -> ()`. A handful use `i64`
  (`a::p,a::D,a::ga,a::Q,a::M,a::aa,a::W`) → marshal as JS **BigInt**; a few use
  `f32`/`f64`.
- binaryen's wasm **exports its `memory`**.

So the bridge is *only* number marshalling + a shared-memory view — no reference
types. The implementation mechanism (re-entrancy + memory) is the work:

- **Re-entrancy (rquickjs ↔ wasmtime).** The wasm runs inside the JS call to
  `WebAssembly.instantiate`, so a `Ctx<'js>` is on the stack, but a wasmtime host
  closure must be `'static`. Bridge it by storing the raw QuickJS context
  (`Ctx::as_raw()` → `NonNull<JSContext>`) plus the imports as
  `Persistent<Function>` in the wasmtime `Store` data; inside each host func,
  `unsafe { Ctx::from_raw(raw) }` and call the persisted JS import. Valid because
  the QuickJS context outlives the instantiate call. (Mind the earlier
  double-borrow rule: never run pending jobs while a host func holds the ctx.)
- **Memory aliasing.** Expose `instance.exports.memory` as a `WebAssembly.Memory`
  whose `.buffer` is an **external ArrayBuffer over the wasmtime linear memory**
  (QuickJS `JS_NewArrayBuffer` with no-op free over `Memory::data_ptr`/`len`).
  On `memory.grow` (Emscripten's `emscripten_resize_heap` import) the pointer can
  move, so re-derive a fresh ArrayBuffer and let Emscripten's `updateMemoryViews`
  re-read `.buffer` — matching its own model.

### Reproduce

```
./ferrosa-udf/examples/asc-poc/build-bundle.sh /tmp/asc-host/asc-bundle.mjs
cargo run -p ferrosa-udf --example asc_compile_poc -- /tmp/asc-host/asc-bundle.mjs   # runs toolchain, dumps /tmp/binaryen.wasm
cargo run -p ferrosa-udf --example wasm_imports -- /tmp/binaryen.wasm                # dumps the 82-import surface
```

### Feasibility of the mechanism — CONFIRMED (rquickjs 0.9)

Both load-bearing primitives exist:

- **Re-entrancy:** `Ctx::as_raw() -> NonNull<JSContext>` and
  `unsafe Ctx::from_raw(NonNull<JSContext>)` (`rquickjs-core/src/context/ctx.rs`).
  Store the raw ctx in the wasmtime `Store` data; rebuild a `Ctx` inside each
  host func to call the persisted JS import.
- **Zero-copy memory:** the safe `ArrayBuffer::{new,new_copy}` only **copy**
  (unusable — Emscripten writes into memory and expects the wasm to see it). But
  **`JS_NewArrayBuffer`** is exported in `rquickjs-sys` → create an **external**
  ArrayBuffer over `wasmtime::Memory`'s data pointer (no-op free fn), re-derived
  on grow. This is the one place we drop to the raw QuickJS FFI.
- **Imports:** wire all 82 generically with `wasmtime::Func::new(&FuncType, …)`
  (dynamic `&[Val]` → JS, JS → `&mut [Val]`); `i64` ↔ JS `BigInt`.

So Path A is buildable end to end; the remaining work is the implementation
(below), not a feasibility question.

### Remaining shim to build (well-defined)

Implement a `WebAssembly` global in rquickjs backed by ferrosa's wasmtime:

1. `WebAssembly.Module(bytes)` → `wasmtime::Module::new`.
2. `WebAssembly.instantiate(bytes|module, importObject)`:
   - Build a wasmtime `Linker`; for each `importObject.a.<name>` (a **JS**
     function), register a wasmtime host function that **calls back into
     rquickjs**, marshalling `i32/i64/f32/f64` args/results.
   - Instantiate; expose `instance.exports.<fn>` as **JS functions** that call
     the wasmtime instance's exports (the JS→wasm direction).
   - Expose `instance.exports.memory` as a `WebAssembly.Memory` whose `buffer`
     **aliases the wasmtime instance's linear memory** (the fiddly part — and on
     `memory.grow` the ArrayBuffer must be re-derived, mirroring Emscripten's
     `updateMemoryViews`).
3. `WebAssembly.Memory` / `Table` / `Global` wrappers as needed.

Net: binaryen's real (fast) WASM runs **in ferrosa's wasmtime**, sandboxed; the
JS engine on the host just orchestrates. The cross-runtime function bridging and
the memory aliasing are the implementation work; everything upstream (engine,
bundle, async, init shims, import surface) is proven.

### Shim BUILT and PROVEN end-to-end (2026-05-31) — `examples/asc_compile.rs`

The full bridge now runs. `cargo run -p ferrosa-udf --example asc_compile -- \
/tmp/asc-host/asc-bundle.mjs "<AS source>"` compiles AssemblyScript to WASM
**entirely through the wasmtime-backed shim** and the output executes:

- `add(a,b)` → 55-byte module, `add(1,2) → 3`.
- `fib(n:i32):i64` with a `for` loop → 94-byte module, runs (loop + i64 codegen).

Both outputs start with `\0asm`, load in a *fresh* wasmtime store, and run. The
shim handles binaryen (8.77 MB) **and** asc's own runtime, exiting cleanly (`0`).

What the build pinned down (the unknowns from the design above, now closed):

1. **Memory is an export named `Ca`, not `memory`.** Emscripten reads
   `wasmExports.Ca` → `.buffer`. Look memory up by `ExternType::Memory`, not by a
   hardcoded name. `.buffer` is a **getter** returning a fresh external
   ArrayBuffer (`JS_NewArrayBuffer`, no copy/free) over `Memory::data_ptr/size`,
   so callers always see current (post-grow) memory.
2. **There IS a funcref table export `tB` (13 513 entries)** — earlier analysis
   missed it because `wasm_imports.rs` only matched Memory/Func exports. Emscripten
   binds it as `wasmTable` and binaryen's indirect-call glue does
   `wasmTable.get(idx)`. Must bridge `WebAssembly.Table`: `.get(idx)` returns a JS
   callable wrapping `Table::get → Ref::Func`; `.length` getter from `Table::size`.
3. **Re-entrancy is real and required.** binaryen's ctors call exports *from inside
   host imports* (import → JS → export). A `RefCell` panics; wasmtime supports
   nested activations on one store, so the shim accesses the store through an
   `UnsafeCell` accessor. (SPIKE shortcut: aliasing `&mut Store` is technically UB;
   production threads the active `Caller` instead of re-borrowing the owned store.)
4. **WebAssembly JS arg semantics matter:** missing trailing args default to `0`,
   extras are ignored. Marshalling must **pad/truncate to the wasm arity**, not
   `zip` (which truncated and tripped `expected 6 arguments, got 5`).
5. **asc needs `fetch` + `WebAssembly.instantiateStreaming`** for its inline
   `f64_pow` helper, shipped as a `data:application/wasm;base64,…` URL. Shim adds a
   `fetch` for `data:` URLs (host-side base64 decode → `Uint8Array`) and JS
   `instantiateStreaming`/`compileStreaming` wrappers over the native `instantiate`.
6. **Prelude globals QuickJS lacks:** `console`, `performance.now`, **`self`**,
   `global` (binaryen probes `typeof window/global/self`).
7. **i64 imports/exports marshal via `BigInt`** (`JS_NewBigInt64` / `BigInt::to_i64`).
8. **Teardown:** asc/binaryen create JS↔Rust reference cycles across the FFI
   boundary that QuickJS's GC can't trace, so freeing the runtime trips its debug
   `gc_obj_list` assertion. The one-shot example `process::exit(0)`s after a
   successful compile; a long-lived embedding must drop the shim's Stores/Persistents
   (break the cycle) before freeing the runtime.

### Path to production (next when picked up)

- Replace the `UnsafeCell` store with proper `Caller`-threaded re-entrancy (stash
  the active `StoreContextMut` for the JS-invoked export wrappers; fall back to the
  owned store at top level). Removes the only UB in the spike.
- Lifecycle: build the shim + compile once per `CREATE FUNCTION`, then tear down
  cleanly (no `process::exit`). Cache the asc/binaryen `Module`s across compiles.
- Wire into DDL: `CREATE FUNCTION … LANGUAGE assemblyscript AS $$ … $$` → compile
  to WASM bytes → store/replicate the bytes exactly like an uploaded `.wasm` UDF.
- Resource bounds: wasmtime fuel/epoch + memory cap on the *compiler* store; cap
  source size and output size; STRIDE the source-as-input surface.

## Goal

Let users write UDF/UDA bodies in a high-level language inside
`CREATE FUNCTION … AS $$ …source… $$` and have ferrosa compile that source to a
WASM component **at definition time**, instead of requiring a pre-compiled
artifact. ferrosa already runs UDFs as WASM components in Wasmtime; this adds a
source→WASM front end.

Today the only supported form is a pre-compiled WASM module
(`LANGUAGE wasm AS '<hex>' | AS FILE '<path>' | AS URL '<url>' WITH SHA256 = …`).
The vendored Cassandra Java-UDF examples were rewritten to this WASM form
(see `cassandra/doc/modules/cassandra/examples/CQL/{create_function,uda,
function_dollarsign,function_udfcontext}.cql`).

## Candidate language: AssemblyScript

AssemblyScript (TypeScript-like → WASM) is the strongest fit because its
toolchain is **embeddable**, unlike Java:

- `asc` is a JavaScript program that drives **binaryen** (shipped as WASM).
- To keep ferrosa self-contained, embed a small JS engine and run `asc.js` +
  `binaryen.wasm` in-process.

### Compiler-hosting options

| Option | Trade-off |
|--------|-----------|
| **QuickJS via `rquickjs`** (recommended) | Tiny embeddable C engine, mature Rust bindings, self-contained. Risk: `asc.js` targets Node/browser → needs FS/host shims + compat testing. |
| Shell out to `node asc` | Trivial to prototype, but adds Node.js as a deployment dependency + supply-chain surface. Rejected for a self-contained DB. |
| Embed V8 (`deno_core`/`rusty_v8`) | Heavy binary/build; overkill. |

Other languages to keep in mind when picking the long-term answer: Rust→WASM
(needs the Rust toolchain — not embeddable), TinyGo, Grain, or a small DSL
compiled directly to WASM in Rust (most controllable, least familiar to users).
The open question the owner wants to think through: **what is the best general
way to get a language inline → WASM** without dragging in a heavy toolchain.

## Architecture (regardless of language)

1. Parser: `LANGUAGE assemblyscript AS $$ …source… $$` — needs a lexer change for
   dollar-quoted bodies + the language keyword. (Small.)
2. **Compile at the coordinator**, then replicate the resulting **WASM bytes**
   through the existing WASM-UDF storage/replication path, so replicas never
   compile untrusted source and every node converges on identical bytes
   (deterministic — matches how DDL already replicates).
3. The compiled UDF runs under the **existing Wasmtime sandbox** — no new
   execution surface.

## STRIDE

- **Denial of Service (new):** compiling untrusted source. Bound the compile
  with CPU/fuel, memory, and wall-clock limits; rate-limit per role.
  Coordinator-only compilation contains it to one node.
- **Tampering / EoP:** compiler runs sandboxed (QuickJS/Wasmtime); output runs
  in the existing UDF sandbox. No raw host access from either stage.
- **Supply chain:** vendoring `asc` + `binaryen.wasm` pins a toolchain version;
  capture the version and a digest so compiled output is reproducible/auditable.

## Effort

~1–2 weeks for a solid version. The parser/DDL groundwork (`$$` bodies +
`LANGUAGE <lang>`) is small; the bulk is the engine integration, compile shims,
and resource sandboxing. Tractable because Wasmtime is already in-process, but
its own PR — not a quick gap-close.

## Next steps when picked up

1. Spike `rquickjs` + `asc.js` + `binaryen.wasm`: compile a trivial AS function
   to WASM in-process, de-risk compat/perf.
2. Add `$$…$$` dollar-quote lexing + `LANGUAGE assemblyscript` parsing (route to
   a compile step).
3. Wire coordinator compile → store WASM bytes via the existing path.
4. Sandbox limits + STRIDE-DoS tests.
