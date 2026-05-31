---
type: design
priority: P3
status: open
created: 2026-05-31
context: "CQL UDF gap-fill — ferrosa runs WASM UDFs, not Java. Explore compiling a high-level language to WASM inline at CREATE FUNCTION time."
---

# Design: inline language → WASM for `CREATE FUNCTION`

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
