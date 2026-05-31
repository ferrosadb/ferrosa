---
type: design
priority: P3
status: open
created: 2026-05-31
context: "CQL UDF gap-fill — ferrosa runs WASM UDFs, not Java. Explore compiling a high-level language to WASM inline at CREATE FUNCTION time."
---

# Design: inline language → WASM for `CREATE FUNCTION`

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
