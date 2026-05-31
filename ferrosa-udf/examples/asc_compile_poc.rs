//! Spike PoC for the inline AssemblyScript-> WASM UDF compiler (Path A: host JS
//! engine + a `WebAssembly` shim backed by ferrosa's wasmtime).
//!
//! This first cut runs the bundled `asc` + `binaryen` toolchain in QuickJS
//! (rquickjs, async) with an *instrumented* `WebAssembly` stub, to capture
//! exactly what binaryen asks of `WebAssembly` (import namespaces/functions,
//! memory import-vs-export). That defines the real wasmtime-backed shim.
//!
//! Run:
//!   cargo run -p ferrosa-udf --example asc_compile_poc -- /tmp/asc-host/asc-bundle.mjs

use rquickjs::{async_with, AsyncContext, AsyncRuntime, CatchResultExt, Function, Module, Promise};

const PRELUDE: &str = r#"
globalThis.console = {
  log: (...a) => __log(a.map(String).join(" ")),
  warn: (...a) => __log("[warn] " + a.map(String).join(" ")),
  error: (...a) => __log("[error] " + a.map(String).join(" ")),
  info: (...a) => __log(a.map(String).join(" ")),
  debug: () => {},
};
globalThis.performance = globalThis.performance || { now: () => 0 };
globalThis.WebAssembly = {
  validate(b) { __log("WA.validate len=" + (b && (b.byteLength ?? b.length))); return true; },
  Memory(d) { __log("WA.Memory " + JSON.stringify(d)); this.buffer = new ArrayBuffer(((d && d.initial) || 0) * 65536); },
  Table(d) { __log("WA.Table " + JSON.stringify(d)); },
  Global(d) { __log("WA.Global " + JSON.stringify(d)); this.value = (d && d.value) || 0; },
  Module(b) { __log("WA.Module len=" + (b.byteLength ?? b.length)); this.__bytes = b; },
  Instance(m, imports) {
    __log("WA.Instance importNS=" + Object.keys(imports || {}).join(","));
    for (const ns of Object.keys(imports || {})) __log("  ns[" + ns + "]=" + Object.keys(imports[ns]).slice(0, 80).join(","));
    this.exports = {};
  },
  async instantiate(b, imports) {
    const blen = (b && (b.byteLength ?? b.length)) ?? (b && b.__bytes && (b.__bytes.byteLength ?? b.__bytes.length));
    __log("WA.instantiate bytes=" + blen + " importNS=" + Object.keys(imports || {}).join(","));
    for (const ns of Object.keys(imports || {})) __log("  ns[" + ns + "]=" + Object.keys(imports[ns]).slice(0, 80).join(","));
    try { __dumpWasm(b instanceof Uint8Array ? b : new Uint8Array(b.__bytes ?? b)); } catch (e) { __log("dump failed: " + e); }
    throw new Error("WA.instantiate stub (logging only)");
  },
  compile(b) { __log("WA.compile len=" + (b.byteLength ?? b.length)); throw new Error("WA.compile stub"); },
  RuntimeError: Error, CompileError: Error, LinkError: Error,
};
"#;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let bundle_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/asc-host/asc-bundle.mjs".to_string());
    let source = std::env::args().nth(2).unwrap_or_else(|| {
        "export function add(a: i32, b: i32): i32 { return a + b; }".to_string()
    });
    let bundle = std::fs::read_to_string(&bundle_path).expect("read bundle");
    eprintln!("[poc] bundle {} ({} bytes)", bundle_path, bundle.len());

    let rt = AsyncRuntime::new().unwrap();
    rt.set_memory_limit(2 * 1024 * 1024 * 1024).await;
    rt.set_max_stack_size(64 * 1024 * 1024).await;
    let ctx = AsyncContext::full(&rt).await.unwrap();

    async_with!(ctx => |ctx| {
        let log = Function::new(ctx.clone(), |msg: String| eprintln!("[js] {msg}")).unwrap();
        ctx.globals().set("__log", log).unwrap();
        let dump = Function::new(ctx.clone(), |bytes: rquickjs::TypedArray<'_, u8>| {
            let data: &[u8] = bytes.as_bytes().unwrap_or(&[]);
            std::fs::write("/tmp/binaryen.wasm", data).ok();
            eprintln!("[poc] dumped {} wasm bytes -> /tmp/binaryen.wasm", data.len());
        })
        .unwrap();
        ctx.globals().set("__dumpWasm", dump).unwrap();
        ctx.eval::<(), _>(PRELUDE).catch(&ctx).expect("prelude");

        // Evaluate the bundle module and AWAIT its top-level await (binaryen init).
        eprintln!("[poc] evaluating bundle module...");
        let m = Module::evaluate(ctx.clone(), "asc_bundle", bundle).catch(&ctx);
        match m {
            Ok(promise) => match promise.into_future::<()>().await.catch(&ctx) {
                Ok(()) => eprintln!("[poc] module evaluated OK"),
                Err(e) => eprintln!("[poc] module TLA REJECTED: {e}"),
            },
            Err(e) => eprintln!("[poc] module eval error: {e}"),
        }

        let defined: bool = ctx
            .eval("typeof globalThis.__ascCompile === 'function'")
            .unwrap_or(false);
        eprintln!("[poc] __ascCompile defined = {defined}");
        if !defined { return; }

        // Run the compile; the instrumented WebAssembly stub logs binaryen's calls.
        eprintln!("[poc] compiling AS source...");
        let compile: Function = ctx.globals().get("__ascCompile").unwrap();
        let res: Result<Promise, _> = compile.call((source.clone(),));
        match res {
            Ok(p) => match p.into_future::<rquickjs::Value>().await.catch(&ctx) {
                Ok(_v) => eprintln!("[poc] compile resolved (unexpected with stub)"),
                Err(e) => eprintln!("[poc] compile rejected (expected at instantiate stub):\n{e}"),
            },
            Err(e) => eprintln!("[poc] compile call error: {e:?}"),
        }
    })
    .await;

    rt.idle().await;
}
