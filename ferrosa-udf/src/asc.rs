//! Inline AssemblyScript -> WASM compiler.
//!
//! Runs the `asc` + `binaryen` JavaScript toolchain inside QuickJS (via rquickjs)
//! with a `WebAssembly` global backed by ferrosa's own wasmtime, so binaryen's
//! 8.77 MB codegen module runs sandboxed in wasmtime on the host. This lets
//! `CREATE FUNCTION ... LANGUAGE assemblyscript` compile source to a core WASM
//! module with no external `node`/`emcc` toolchain.
//!
//! ## Lifecycle
//!
//! The QuickJS runtime + binaryen init is expensive (parse 15 MB of JS, run
//! binaryen's WASM ctors) and, because the asc/binaryen toolchain creates
//! JS<->Rust reference cycles across the FFI boundary that QuickJS's GC cannot
//! trace, the runtime cannot be cleanly dropped (doing so trips a debug
//! `gc_obj_list` assertion). Both problems are solved the same way: a single
//! **persistent compiler thread** owns the runtime for the process lifetime,
//! initialises it once, and serves every compile request. Init cost is amortised
//! and the runtime is never freed, so there is no per-compile leak and no
//! teardown assertion.
//!
//! ## Re-entrancy
//!
//! binaryen calls its own exports from inside host imports (import -> JS ->
//! export). A host import publishes its wasmtime `Caller` to a per-instance
//! active-context cell while it re-enters JS, so the JS-side export/memory/table
//! wrappers run against the live activation; the owned store's `RefCell` is
//! borrowed only at the outermost call. See [`with_store_ctx`].

use std::cell::{Cell, RefCell};
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::sync::OnceLock;

use rquickjs::function::{Args, Rest};
use rquickjs::{
    async_with, AsyncContext, AsyncRuntime, CatchResultExt, Ctx, Function, Object, Persistent,
    Value,
};
use wasmtime::{
    AsContextMut, Caller, Engine, Func, Instance, Linker, Memory, Module as WtModule, Ref, Store,
    StoreContextMut, Val, ValType,
};

/// Failure modes of inline AssemblyScript compilation.
#[derive(Debug, thiserror::Error)]
pub enum AscError {
    /// The asc toolchain bundle is not configured or could not be loaded. Set
    /// `FERROSA_ASC_BUNDLE` to a bundle built via
    /// `ferrosa-udf/examples/asc-poc/build-bundle.sh`.
    #[error("AssemblyScript UDF support unavailable: {0}")]
    Unavailable(String),
    /// The AssemblyScript source failed to compile (syntax/type error, or
    /// binaryen rejected it). The message is the toolchain's own diagnostic.
    #[error("AssemblyScript compilation failed: {0}")]
    Compile(String),
    /// An internal error in the compiler host (thread/channel failure).
    #[error("inline compiler internal error: {0}")]
    Internal(String),
}

/// Compile AssemblyScript source to a **core** WASM module.
///
/// The returned bytes are a standalone core module exporting the source's
/// functions. The compiler thread is spawned lazily on first use and reused.
pub fn compile_assemblyscript(source: &str) -> Result<Vec<u8>, AscError> {
    let sender = compiler()
        .as_ref()
        .map_err(|e| AscError::Unavailable(e.clone()))?;
    let (reply_tx, reply_rx) = channel();
    sender
        .send(Job {
            source: source.to_string(),
            reply: reply_tx,
        })
        .map_err(|_| AscError::Internal("compiler thread is gone".into()))?;
    reply_rx
        .recv()
        .map_err(|_| AscError::Internal("compiler thread dropped the reply".into()))?
        .map_err(AscError::Compile)
}

/// A compile request sent to the persistent compiler thread.
struct Job {
    source: String,
    reply: Sender<Result<Vec<u8>, String>>,
}

/// Process-wide handle to the compiler thread (or the reason it is unavailable).
fn compiler() -> &'static Result<SyncSender<Job>, String> {
    static COMPILER: OnceLock<Result<SyncSender<Job>, String>> = OnceLock::new();
    COMPILER.get_or_init(spawn_compiler)
}

/// Read the bundle, spawn the compiler thread, and wait for it to finish init.
fn spawn_compiler() -> Result<SyncSender<Job>, String> {
    let path = std::env::var("FERROSA_ASC_BUNDLE").map_err(|_| {
        "FERROSA_ASC_BUNDLE is not set (point it at a bundle built via \
         ferrosa-udf/examples/asc-poc/build-bundle.sh)"
            .to_string()
    })?;
    let bundle = std::fs::read_to_string(&path)
        .map_err(|e| format!("reading FERROSA_ASC_BUNDLE {path}: {e}"))?;

    let (job_tx, job_rx) = sync_channel::<Job>(0);
    let (init_tx, init_rx) = channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("asc-compiler".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(move || compiler_main(bundle, job_rx, init_tx))
        .map_err(|e| format!("spawning asc compiler thread: {e}"))?;

    match init_rx.recv() {
        Ok(Ok(())) => Ok(job_tx),
        Ok(Err(e)) => Err(e),
        Err(_) => Err("asc compiler thread died during initialisation".into()),
    }
}

/// The persistent compiler thread: owns the QuickJS runtime for the process
/// lifetime, initialises binaryen once, then serves compile jobs.
fn compiler_main(bundle: String, jobs: Receiver<Job>, init: Sender<Result<(), String>>) {
    let trt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = init.send(Err(format!("tokio runtime: {e}")));
            return;
        }
    };

    trt.block_on(async move {
        let engine = Engine::default();
        let rt = match AsyncRuntime::new() {
            Ok(rt) => rt,
            Err(e) => {
                let _ = init.send(Err(format!("quickjs runtime: {e}")));
                return;
            }
        };
        rt.set_memory_limit(2 * 1024 * 1024 * 1024).await;
        rt.set_max_stack_size(64 * 1024 * 1024).await;
        let ctx = match AsyncContext::full(&rt).await {
            Ok(c) => c,
            Err(e) => {
                let _ = init.send(Err(format!("quickjs context: {e}")));
                return;
            }
        };

        // One-time init: install the wasmtime-backed `WebAssembly`, evaluate the
        // bundle (binaryen ctors run here), and confirm __ascCompile is defined.
        let init_result: Result<(), String> = async_with!(ctx => |ctx| {
            let log = Function::new(ctx.clone(), |msg: String| {
                tracing::trace!(target: "ferrosa_udf::asc", "{msg}");
            })
            .map_err(|e| format!("install __log: {e}"))?;
            ctx.globals().set("__log", log).map_err(|e| format!("set __log: {e}"))?;
            ctx.eval::<(), _>(PRELUDE).catch(&ctx).map_err(|e| format!("prelude: {e}"))?;
            install_webassembly(&ctx, engine.clone());

            let module = rquickjs::Module::evaluate(ctx.clone(), "asc_bundle", bundle.as_bytes())
                .catch(&ctx)
                .map_err(|e| format!("evaluating asc bundle: {e}"))?;
            module
                .into_future::<()>()
                .await
                .catch(&ctx)
                .map_err(|e| format!("asc bundle init (top-level await): {e}"))?;

            let defined: bool = ctx
                .eval("typeof globalThis.__ascCompile === 'function'")
                .unwrap_or(false);
            if !defined {
                return Err("asc bundle did not define globalThis.__ascCompile".to_string());
            }
            Ok(())
        })
        .await;

        if let Err(e) = init_result {
            let _ = init.send(Err(e));
            return;
        }
        if init.send(Ok(())).is_err() {
            return; // requester gave up
        }

        // Serve compile jobs until all senders drop. The runtime stays alive on
        // this thread for the whole process — never dropped, so no teardown.
        while let Ok(job) = jobs.recv() {
            let out = compile_one(&ctx, &job.source).await;
            let _ = job.reply.send(out);
        }
    });
}

/// Run one compile against the already-initialised QuickJS context.
async fn compile_one(ctx: &AsyncContext, source: &str) -> Result<Vec<u8>, String> {
    let source = source.to_string();
    async_with!(ctx => |ctx| {
        let compile: Function = ctx
            .globals()
            .get("__ascCompile")
            .map_err(|e| format!("__ascCompile missing: {e}"))?;
        let promise: rquickjs::Promise = compile
            .call((source,))
            .map_err(|e| format!("invoking __ascCompile: {e}"))?;
        match promise.into_future::<Value>().await.catch(&ctx) {
            Ok(v) => {
                let ta = rquickjs::TypedArray::<u8>::from_value(v)
                    .map_err(|e| format!("compile result was not a Uint8Array: {e}"))?;
                Ok(ta.as_bytes().unwrap_or(&[]).to_vec())
            }
            Err(e) => Err(format!("{e}")),
        }
    })
    .await
}

// ===========================================================================
// wasmtime-backed `WebAssembly` shim
// ===========================================================================

/// Raw QuickJS context pointer, shuttled into wasmtime host closures so they can
/// call back into JS. Single-threaded use only.
#[derive(Clone, Copy)]
struct CtxPtr(NonNull<rquickjs::qjs::JSContext>);
unsafe impl Send for CtxPtr {}
unsafe impl Sync for CtxPtr {}

/// wasmtime `Store` data: lets host funcs reach the JS import object.
struct ShimData {
    ctx: CtxPtr,
    imports_a: Persistent<Object<'static>>,
}

/// Lifetime-erased pointer to the wasmtime store context that is *currently
/// executing* this instance. A host import publishes its `Caller` here while it
/// re-enters JS, so the JS-side wrappers run against the SAME live activation
/// instead of re-borrowing the owned store (which would alias).
type ActiveCtx = Cell<*mut StoreContextMut<'static, ShimData>>;

/// Single-threaded-only `Send + Sync` handle to the active-context cell, so it
/// can be captured by wasmtime host closures (which require `Send + Sync`).
struct SendActive(Rc<ActiveCtx>);
unsafe impl Send for SendActive {}
unsafe impl Sync for SendActive {}

impl SendActive {
    // Methods (vs. field access) so a `move` closure captures the whole wrapper —
    // disjoint closure capture would otherwise grab the inner `!Send` `Rc`.
    fn replace(
        &self,
        p: *mut StoreContextMut<'static, ShimData>,
    ) -> *mut StoreContextMut<'static, ShimData> {
        self.0.replace(p)
    }
    fn set(&self, p: *mut StoreContextMut<'static, ShimData>) {
        self.0.set(p)
    }
}

/// One instantiated module, shared with its JS-side `instance.exports`. The owned
/// store is borrowed only at the OUTERMOST call into this instance; deeper
/// (re-entrant) calls run through `active`, so the `RefCell` is never borrowed
/// re-entrantly and no `&mut Store` is ever aliased.
struct Shared {
    store: RefCell<Store<ShimData>>,
    instance: Instance,
    memory: Option<Memory>,
    active: Rc<ActiveCtx>,
}

/// Run `f` with a `StoreContextMut` for `shared`'s store. If this instance is
/// already executing (a host import re-entered JS), reuse the live activation;
/// otherwise borrow the owned store and publish it as active for the call's span.
fn with_store_ctx<R>(
    shared: &Rc<Shared>,
    f: impl FnOnce(&mut StoreContextMut<'_, ShimData>) -> R,
) -> R {
    let active = shared.active.get();
    if active.is_null() {
        let mut guard = shared.store.borrow_mut();
        let mut scm = guard.as_context_mut();
        let ptr = std::ptr::from_mut(&mut scm).cast::<StoreContextMut<'static, ShimData>>();
        let prev = shared.active.replace(ptr);
        let r = f(&mut scm);
        shared.active.set(prev);
        r
    } else {
        // SAFETY: `active` points to a `StoreContextMut` live on an outer frame,
        // kept valid by push/pop discipline; reborrow to a fresh shorter lifetime.
        let outer: &mut StoreContextMut<'static, ShimData> = unsafe { &mut *active };
        let mut scm = outer.as_context_mut();
        f(&mut scm)
    }
}

/// The export shapes we bridge into `instance.exports`.
enum ExportKind {
    Func,
    Memory,
    Table,
    Global,
    Other,
}

impl ExportKind {
    fn of(ty: &wasmtime::ExternType) -> Self {
        match ty {
            wasmtime::ExternType::Func(_) => ExportKind::Func,
            wasmtime::ExternType::Memory(_) => ExportKind::Memory,
            wasmtime::ExternType::Table(_) => ExportKind::Table,
            wasmtime::ExternType::Global(_) => ExportKind::Global,
            _ => ExportKind::Other,
        }
    }
}

const PRELUDE: &str = r#"
globalThis.console = {
  log: (...a) => __log(a.map(String).join(" ")),
  warn: (...a) => __log("[warn] " + a.map(String).join(" ")),
  error: (...a) => __log("[error] " + a.map(String).join(" ")),
  info: (...a) => __log(a.map(String).join(" ")),
  debug: () => {},
};
globalThis.performance = globalThis.performance || { now: () => 0 };
globalThis.self = globalThis;
globalThis.global = globalThis;
"#;

/// Convert a wasmtime `Val` to a JS value.
fn val_to_js<'js>(ctx: &Ctx<'js>, v: &Val) -> rquickjs::Result<Value<'js>> {
    Ok(match v {
        Val::I32(n) => Value::new_int(ctx.clone(), *n),
        Val::I64(n) => bigint_from_i64(ctx, *n)?,
        Val::F32(b) => Value::new_float(ctx.clone(), f32::from_bits(*b) as f64),
        Val::F64(b) => Value::new_float(ctx.clone(), f64::from_bits(*b)),
        other => {
            return Err(rquickjs::Error::new_from_js_message(
                "wasm",
                "val",
                format!("unsupported import param {other:?}"),
            ))
        }
    })
}

/// Convert a JS value back to a wasmtime `Val` of the requested type.
fn js_to_val(ty: &ValType, v: &Value<'_>) -> rquickjs::Result<Val> {
    Ok(match ty {
        ValType::I32 => Val::I32(coerce_i32(v)),
        ValType::I64 => Val::I64(coerce_i64(v)),
        ValType::F32 => Val::F32((v.as_float().unwrap_or(0.0) as f32).to_bits()),
        ValType::F64 => Val::F64(v.as_float().unwrap_or(0.0).to_bits()),
        other => {
            return Err(rquickjs::Error::new_from_js_message(
                "wasm",
                "val",
                format!("unsupported result {other:?}"),
            ))
        }
    })
}

fn coerce_i32(v: &Value<'_>) -> i32 {
    if let Some(i) = v.as_int() {
        i
    } else if let Some(f) = v.as_float() {
        f as i64 as i32
    } else {
        0
    }
}

fn coerce_i64(v: &Value<'_>) -> i64 {
    if let Some(bi) = v.as_big_int() {
        bi.clone().to_i64().unwrap_or(0)
    } else if let Some(i) = v.as_int() {
        i as i64
    } else if let Some(f) = v.as_float() {
        f as i64
    } else {
        0
    }
}

fn bigint_from_i64<'js>(ctx: &Ctx<'js>, n: i64) -> rquickjs::Result<Value<'js>> {
    let raw = unsafe { rquickjs::qjs::JS_NewBigInt64(ctx.as_raw().as_ptr(), n) };
    Ok(unsafe { Value::from_raw(ctx.clone(), raw) })
}

/// Build an ArrayBuffer that aliases `[ptr, len)` (the wasmtime linear memory),
/// without copying or freeing it.
fn external_array_buffer<'js>(ctx: &Ctx<'js>, ptr: *mut u8, len: usize) -> Value<'js> {
    let raw = unsafe {
        rquickjs::qjs::JS_NewArrayBuffer(
            ctx.as_raw().as_ptr(),
            ptr,
            len as _,
            None,
            std::ptr::null_mut(),
            false,
        )
    };
    unsafe { Value::from_raw(ctx.clone(), raw) }
}

/// Minimal standard-alphabet base64 decoder (skips whitespace, stops at `=`).
fn base64_decode(s: &str) -> Vec<u8> {
    let mut table = [255u8; 256];
    for (i, c) in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        .iter()
        .enumerate()
    {
        table[*c as usize] = i as u8;
    }
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0u32;
    for &b in s.as_bytes() {
        if b == b'=' {
            break;
        }
        let v = table[b as usize];
        if v == 255 {
            continue; // skip newlines / padding whitespace
        }
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    out
}

/// JS wiring for asc's inline helper-wasm load path: `fetch(dataUrl)` +
/// `WebAssembly.instantiateStreaming`. Only `data:...;base64,` URLs are supported.
const STREAMING_JS: &str = r#"
globalThis.fetch = async (u) => {
  const bytes = __fetchDataUrl(u);
  return { ok: true, arrayBuffer: async () => bytes.buffer };
};
WebAssembly.instantiateStreaming = async (src, imp) => {
  const r = await src;
  const buf = await r.arrayBuffer();
  return WebAssembly.instantiate(new Uint8Array(buf), imp || {});
};
WebAssembly.compileStreaming = async (src) => {
  const r = await src;
  const buf = await r.arrayBuffer();
  return new WebAssembly.Module(new Uint8Array(buf));
};
"#;

fn install_webassembly<'js>(ctx: &Ctx<'js>, engine: Engine) {
    let wa = Object::new(ctx.clone()).unwrap();

    // WebAssembly.validate(bytes) -> bool
    let eng_v = engine.clone();
    wa.set(
        "validate",
        Function::new(ctx.clone(), move |bytes: rquickjs::TypedArray<'js, u8>| {
            WtModule::new(&eng_v, bytes.as_bytes().unwrap_or(&[])).is_ok()
        })
        .unwrap(),
    )
    .unwrap();

    // WebAssembly.instantiate(bytes, importObject) -> { instance }
    let eng_i = engine.clone();
    wa.set(
        "instantiate",
        Function::new(
            ctx.clone(),
            move |cx: Ctx<'js>, bytes: rquickjs::TypedArray<'js, u8>, imports: Object<'js>| {
                instantiate(
                    &cx,
                    &eng_i,
                    bytes.as_bytes().unwrap_or(&[]).to_vec(),
                    imports,
                )
            },
        )
        .unwrap(),
    )
    .unwrap();

    ctx.globals().set("WebAssembly", wa).unwrap();

    // `fetch` for data: URLs (asc's inline f64_pow helper wasm) + streaming wrappers.
    let fetch = Function::new(
        ctx.clone(),
        |cx: Ctx<'js>, url: String| -> rquickjs::Result<rquickjs::TypedArray<'js, u8>> {
            let b64 = url.rsplit_once("base64,").map(|(_, b)| b).unwrap_or("");
            rquickjs::TypedArray::new(cx, base64_decode(b64))
        },
    )
    .unwrap();
    ctx.globals().set("__fetchDataUrl", fetch).unwrap();
    ctx.eval::<(), _>(STREAMING_JS).unwrap();
}

fn instantiate<'js>(
    ctx: &Ctx<'js>,
    engine: &Engine,
    bytes: Vec<u8>,
    imports: Object<'js>,
) -> rquickjs::Result<Object<'js>> {
    let module = WtModule::new(engine, &bytes).map_err(|e| {
        rquickjs::Error::new_from_js_message("wasm", "wasm", format!("compile: {e}"))
    })?;

    let imports_a: Object = imports
        .get("a")
        .unwrap_or_else(|_| Object::new(ctx.clone()).unwrap());
    let data = ShimData {
        ctx: CtxPtr(ctx.as_raw()),
        imports_a: Persistent::save(ctx, imports_a),
    };
    let mut store = Store::new(engine, data);
    let active: Rc<ActiveCtx> = Rc::new(Cell::new(std::ptr::null_mut()));

    // Wire every `a::<name>` import generically: call the JS import function.
    let mut linker: Linker<ShimData> = Linker::new(engine);
    for imp in module.imports() {
        if imp.module() != "a" {
            continue;
        }
        let name = imp.name().to_string();
        let ty = match imp.ty() {
            wasmtime::ExternType::Func(ft) => ft,
            _ => continue,
        };
        let nm = name.clone();
        let res_tys: Vec<ValType> = ty.results().collect();
        let active_h = SendActive(active.clone());
        linker
            .func_new(
                "a",
                &name,
                ty,
                move |mut caller: Caller<'_, ShimData>, params, results| {
                    // Publish this activation so JS that re-enters our exports during
                    // the import runs against THIS store, then restore on the way out.
                    let mut scm = caller.as_context_mut();
                    let ptr =
                        std::ptr::from_mut(&mut scm).cast::<StoreContextMut<'static, ShimData>>();
                    let prev = active_h.replace(ptr);
                    // Copy out what the JS call needs; hold no borrow of `scm` across it.
                    let (cptr, imports_a) = {
                        let d = scm.data();
                        (d.ctx.0, d.imports_a.clone())
                    };
                    let result = call_js_import(cptr, &imports_a, &nm, params, results, &res_tys);
                    active_h.set(prev);
                    result
                },
            )
            .map_err(|e| {
                rquickjs::Error::new_from_js_message("wasm", "wasm", format!("link {name}: {e}"))
            })?;
    }

    let instance = linker.instantiate(&mut store, &module).map_err(|e| {
        rquickjs::Error::new_from_js_message("wasm", "wasm", format!("instantiate: {e}"))
    })?;
    // The exported memory is named by the module (binaryen calls it "Ca", not
    // "memory"); find it by ExternType rather than a hardcoded name.
    let mem_export_name: Option<String> = module
        .exports()
        .find(|e| matches!(e.ty(), wasmtime::ExternType::Memory(_)))
        .map(|e| e.name().to_string());
    let memory = mem_export_name
        .as_deref()
        .and_then(|n| instance.get_memory(&mut store, n));

    let running = Rc::new(Shared {
        store: RefCell::new(store),
        instance,
        memory,
        active,
    });

    // Build instance.exports under each export's real name.
    let exports = Object::new(ctx.clone())?;
    let kinds: Vec<(String, ExportKind)> = module
        .exports()
        .map(|e| (e.name().to_string(), ExportKind::of(&e.ty())))
        .collect();
    for (ename, kind) in kinds {
        match kind {
            ExportKind::Func => {
                let run = running.clone();
                let en = ename.clone();
                let f = Function::new(ctx.clone(), move |cx: Ctx<'js>, args: Rest<Value<'js>>| {
                    call_export(&cx, &run, &en, &args.0)
                })?;
                exports.set(&ename, f)?;
            }
            ExportKind::Memory => {
                let mem_obj = build_memory_object(ctx, &running)?;
                exports.set(&ename, mem_obj)?;
            }
            ExportKind::Table => {
                let tbl = build_table_object(ctx, &running, ename.clone())?;
                exports.set(&ename, tbl)?;
            }
            ExportKind::Global => {
                // Emscripten reads exported globals via `.value`; expose a snapshot.
                let inst = running.instance;
                let val = with_store_ctx(&running, |scm| {
                    inst.get_global(&mut *scm, &ename).map(|g| g.get(&mut *scm))
                });
                if let Some(v) = val {
                    let go = Object::new(ctx.clone())?;
                    go.set("value", val_to_js(ctx, &v)?)?;
                    exports.set(&ename, go)?;
                }
            }
            ExportKind::Other => {}
        }
    }

    let result = Object::new(ctx.clone())?;
    let inst_obj = Object::new(ctx.clone())?;
    inst_obj.set("exports", exports)?;
    result.set("instance", inst_obj)?;
    Ok(result)
}

/// A `WebAssembly.Memory`-like object: `.buffer` getter aliasing the live
/// wasmtime linear memory, plus `.grow(pages)`.
fn build_memory_object<'js>(ctx: &Ctx<'js>, running: &Rc<Shared>) -> rquickjs::Result<Object<'js>> {
    let mem_obj = Object::new(ctx.clone())?;
    let run = running.clone();
    let buf_getter = Function::new(ctx.clone(), move |cx: Ctx<'js>| -> Value<'js> {
        let mem = run.memory.unwrap();
        let (ptr, len) = with_store_ctx(&run, |scm| {
            (mem.data_ptr(&mut *scm), mem.data_size(&mut *scm))
        });
        external_array_buffer(&cx, ptr, len)
    })?;
    // `buffer` is a getter so callers always observe the current (possibly grown) memory.
    define_getter(ctx, &mem_obj, "buffer", buf_getter)?;
    let run_g = running.clone();
    let grow = Function::new(ctx.clone(), move |pages: u64| -> i64 {
        let mem = run_g.memory.unwrap();
        with_store_ctx(&run_g, |scm| match mem.grow(&mut *scm, pages) {
            Ok(prev) => prev as i64,
            Err(_) => -1,
        })
    })?;
    mem_obj.set("grow", grow)?;
    Ok(mem_obj)
}

/// Marshal JS args and call a JS import function. Touches only the QuickJS side
/// (no store borrow), so it is safe to run while a store activation is published.
fn call_js_import(
    cptr: NonNull<rquickjs::qjs::JSContext>,
    imports_a: &Persistent<Object<'static>>,
    name: &str,
    params: &[Val],
    results: &mut [Val],
    res_tys: &[ValType],
) -> wasmtime::Result<()> {
    let ctx = unsafe { Ctx::from_raw(cptr) };
    let a = imports_a.clone().restore(&ctx).map_err(wt_err)?;
    let f: Function = a.get(name).map_err(|e| {
        tracing::warn!(target: "ferrosa_udf::asc", "import a::{name} missing in import object: {e}");
        wt_err(e)
    })?;
    let mut args = Args::new(ctx.clone(), params.len());
    for p in params.iter() {
        args.push_arg(val_to_js(&ctx, p).map_err(wt_err)?)
            .map_err(wt_err)?;
    }
    let ret: Value = match f.call_arg::<Value>(args) {
        Ok(v) => v,
        Err(e) => {
            let detail = ctx.catch();
            let msg = detail
                .as_exception()
                .map(|x| {
                    format!(
                        "{}\n{}",
                        x.message().unwrap_or_default(),
                        x.stack().unwrap_or_default()
                    )
                })
                .unwrap_or_else(|| format!("{detail:?}"));
            tracing::debug!(target: "ferrosa_udf::asc", "import a::{name} threw: {msg}");
            return Err(wt_err(e));
        }
    };
    if let (Some(slot), Some(rty)) = (results.get_mut(0), res_tys.first()) {
        *slot = js_to_val(rty, &ret).map_err(wt_err)?;
    }
    Ok(())
}

fn call_export<'js>(
    ctx: &Ctx<'js>,
    run: &Rc<Shared>,
    name: &str,
    args: &[Value<'js>],
) -> rquickjs::Result<Value<'js>> {
    let inst = run.instance;
    let func = with_store_ctx(run, |scm| inst.get_func(&mut *scm, name))
        .ok_or_else(|| rquickjs::Error::new_from_js_message("wasm", "export", "missing export"))?;
    invoke_func(ctx, run, func, name, args)
}

/// Call a wasmtime `Func` with JS args, marshalling the (single) result back.
fn invoke_func<'js>(
    ctx: &Ctx<'js>,
    run: &Rc<Shared>,
    func: Func,
    label: &str,
    args: &[Value<'js>],
) -> rquickjs::Result<Value<'js>> {
    with_store_ctx(run, |scm| {
        let ty = func.ty(&mut *scm);
        // WebAssembly JS semantics: missing trailing args default to 0, extra args
        // are ignored. Pad/truncate to the wasm arity rather than zip-truncating.
        let params: Vec<Val> = ty
            .params()
            .enumerate()
            .map(|(i, t)| match args.get(i) {
                Some(v) => js_to_val(&t, v),
                None => Ok(default_val(t)),
            })
            .collect::<rquickjs::Result<_>>()?;
        let mut results: Vec<Val> = ty.results().map(default_val).collect();
        func.call(&mut *scm, &params, &mut results).map_err(|e| {
            rquickjs::Error::new_from_js_message("wasm", "wasm", format!("call {label}: {e}"))
        })?;
        match results.first() {
            Some(v) => val_to_js(ctx, v),
            None => Ok(Value::new_undefined(ctx.clone())),
        }
    })
}

/// Wrap a wasmtime `Func` handle as a JS callable (used for table entries).
fn wrap_func<'js>(ctx: &Ctx<'js>, run: &Rc<Shared>, func: Func) -> rquickjs::Result<Function<'js>> {
    let run = run.clone();
    Function::new(ctx.clone(), move |cx: Ctx<'js>, args: Rest<Value<'js>>| {
        invoke_func(&cx, &run, func, "table_entry", &args.0)
    })
}

/// A `WebAssembly.Table`-like object: `.get(idx)` returns a JS callable for the
/// funcref at that slot (binaryen's indirect-call glue uses `wasmTable.get`).
fn build_table_object<'js>(
    ctx: &Ctx<'js>,
    running: &Rc<Shared>,
    tname: String,
) -> rquickjs::Result<Object<'js>> {
    let tbl = Object::new(ctx.clone())?;
    let run = running.clone();
    let tn = tname.clone();
    let get = Function::new(
        ctx.clone(),
        move |cx: Ctx<'js>, idx: u32| -> rquickjs::Result<Value<'js>> {
            let inst = run.instance;
            let func = with_store_ctx(&run, |scm| match inst.get_table(&mut *scm, &tn) {
                Some(t) => match t.get(&mut *scm, idx as u64) {
                    Some(Ref::Func(f)) => f,
                    _ => None,
                },
                None => None,
            });
            match func {
                Some(f) => Ok(wrap_func(&cx, &run, f)?.into_value()),
                None => Ok(Value::new_null(cx.clone())),
            }
        },
    )?;
    tbl.set("get", get)?;

    let run_l = running.clone();
    let tn_l = tname;
    let len_getter = Function::new(ctx.clone(), move |_cx: Ctx<'js>| -> u32 {
        let inst = run_l.instance;
        with_store_ctx(&run_l, |scm| match inst.get_table(&mut *scm, &tn_l) {
            Some(t) => t.size(&mut *scm) as u32,
            None => 0,
        })
    })?;
    define_getter(ctx, &tbl, "length", len_getter)?;
    Ok(tbl)
}

fn default_val(t: ValType) -> Val {
    match t {
        ValType::I32 => Val::I32(0),
        ValType::I64 => Val::I64(0),
        ValType::F32 => Val::F32(0),
        ValType::F64 => Val::F64(0),
        _ => Val::I32(0),
    }
}

fn define_getter<'js>(
    ctx: &Ctx<'js>,
    obj: &Object<'js>,
    name: &str,
    getter: Function<'js>,
) -> rquickjs::Result<()> {
    let define: Function =
        ctx.eval("(o,n,g)=>Object.defineProperty(o,n,{get:g,configurable:true,enumerable:true})")?;
    define.call::<_, ()>((obj.clone(), name, getter))?;
    Ok(())
}

fn wt_err(e: rquickjs::Error) -> wasmtime::Error {
    wasmtime::Error::msg(format!("js import: {e}"))
}

#[cfg(all(test, feature = "live-infra-tests"))]
mod tests {
    use super::*;

    /// Locate the asc bundle for the live-infra test, or panic with setup
    /// instructions (per the repository test policy — no silent skips).
    fn require_bundle() {
        if std::env::var_os("FERROSA_ASC_BUNDLE").is_some() {
            return;
        }
        panic!(
            "FERROSA_ASC_BUNDLE is not set. Build the asc bundle and point the env var at it:\n  \
             ./ferrosa-udf/examples/asc-poc/build-bundle.sh /tmp/asc-host/asc-bundle.mjs\n  \
             FERROSA_ASC_BUNDLE=/tmp/asc-host/asc-bundle.mjs cargo test -p ferrosa-udf \
             --features 'asc-udf live-infra-tests' asc::"
        );
    }

    /// Given AssemblyScript source for `add`, the compiler emits a core WASM
    /// module that wasmtime loads and that computes a + b.
    #[test]
    fn compiles_add_to_runnable_core_wasm() {
        require_bundle();
        let wasm =
            compile_assemblyscript("export function add(a: i32, b: i32): i32 { return a + b; }")
                .expect("compile add");

        assert_eq!(&wasm[..4], b"\0asm", "output is not a WASM module");

        let engine = Engine::default();
        let module = WtModule::new(&engine, &wasm).expect("wasmtime accepts asc output");
        let mut store: Store<()> = Store::new(&engine, ());
        let instance = Linker::<()>::new(&engine)
            .instantiate(&mut store, &module)
            .expect("instantiate asc output");
        let add = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "add")
            .expect("add export with (i32,i32)->i32");
        assert_eq!(add.call(&mut store, (2, 3)).expect("call add"), 5);
    }

    /// A second compile reuses the persistent compiler thread (no re-init).
    #[test]
    fn second_compile_reuses_compiler() {
        require_bundle();
        let a =
            compile_assemblyscript("export function f(): i32 { return 1; }").expect("compile f");
        let b =
            compile_assemblyscript("export function g(): i32 { return 2; }").expect("compile g");
        assert_eq!(&a[..4], b"\0asm");
        assert_eq!(&b[..4], b"\0asm");
        assert_ne!(a, b, "different sources should yield different modules");
    }
}
