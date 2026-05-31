---
type: design
priority: P3
status: open
created: 2026-05-31
context: "CQL UDF gap-fill — ferrosa runs WASM UDFs, not Java. Explore compiling a high-level language to WASM inline at CREATE FUNCTION time."
---

# Design: inline language → WASM for `CREATE FUNCTION`

## Spike findings (2026-05-31) — asc runs with no WASM dependency

Empirically tested (`npm i assemblyscript binaryen`, node 25):

1. **The whole toolchain is pure JavaScript.** `asc` (dist `asc.js` 0.9 MB +
   `assemblyscript.js` 0.8 MB) and the `binaryen` npm package (`index.js` 13 MB)
   are pure JS. Compilation **succeeds with `globalThis.WebAssembly` deleted**
   (verified on non-trivial code with loops) — binaryen's npm build is the
   Emscripten **JS fallback**, not WASM. The `WebAssembly.compile/instantiate`
   references are an *optional* fast path, not required.
2. **It drives fully in-memory.** `asc.main(args, { readFile, writeFile,
   listFiles })` with callbacks compiles a source **string** to wasm **bytes**
   with no `fs`/WASI and no CLI (`process.argv/exit` are only the CLI wrapper).
3. **Vendored footprint ≈ 15 MB of JS** (`asc.js` + `assemblyscript.js` +
   `binaryen/index.js`); the 79 MB native `binaryen/bin/` is **not** needed.

### Consequence — the "into our Wasmtime" question is answered: YES, cleanly

The earlier blocker (running `asc` in Wasmtime would need nested WASM, because
binaryen-as-WASM needs `WebAssembly.instantiate` → no WASM-guest JS engine
provides that) **does not apply**: binaryen is pure JS, so there is no inner
WASM. Therefore:

- **Recommended: run a JS engine *as a WASM/WASI guest inside ferrosa's existing
  Wasmtime*** (a Javy / `quickjs-wasi`-style QuickJS, or StarlingMonkey),
  executing `asc.js` + `binaryen.js`, source-in → wasm-out. The guest engine
  needs **no** WebAssembly support. The compile inherits Wasmtime's
  sandbox + fuel/memory/epoch limits — the STRIDE-DoS bound for free.
- Host-side Boa (pure Rust) / `rquickjs` is the fallback (simpler wiring, but the
  compile is not auto-sandboxed by Wasmtime — needs separate limits).

### Next validation (implementation spike)

1. Pick the WASM-guest JS engine that cleanly runs `asc.js` + 13 MB
   `binaryen.js` (needs ESM, async, TypedArrays, BigInt for the `long` dep):
   evaluate Javy/`quickjs-wasi` (QuickJS — good compat, small) vs StarlingMonkey
   (SpiderMonkey — heavier, very compatible).
2. Bundle the ~15 MB JS + a thin driver into the guest; pass source via
   stdin/WASI or a component interface, return wasm bytes.
3. Run it under the **wasmtime 44 component runtime** ferrosa already has
   (`ferrosa-udf`); measure compile latency (pure-JS binaryen is slower than
   WASM binaryen — acceptable for occasional DDL) and bound guest memory.

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
