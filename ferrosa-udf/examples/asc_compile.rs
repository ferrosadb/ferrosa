//! Inline AssemblyScript -> WASM compiler (Path A): run the asc + binaryen JS
//! toolchain in QuickJS (rquickjs) on the host, with a `WebAssembly` global
//! backed by ferrosa's wasmtime, so binaryen's wasm runs sandboxed in wasmtime.
//!
//!   ./ferrosa-udf/examples/asc-poc/build-bundle.sh /tmp/asc-host/asc-bundle.mjs
//!   cargo run -p ferrosa-udf --example asc_compile -- /tmp/asc-host/asc-bundle.mjs
//!
//! Status: spike. Memory is aliased zero-copy over the wasmtime instance;
//! re-entrant JS imports use Ctx::from_raw. i64 imports are marshalled via
//! BigInt. memory.grow re-derives the buffer.

use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::rc::Rc;

use rquickjs::function::{Args, Rest};
use rquickjs::{
    async_with, AsyncContext, AsyncRuntime, CatchResultExt, Ctx, Function, Object, Persistent,
    Value,
};
use wasmtime::{
    Engine, Func, Instance, Linker, Memory, Module as WtModule, Ref, Store, Val, ValType,
};

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

/// Everything for one instantiated binaryen module, shared with the JS-side
/// export functions and the memory buffer getter.
struct Running {
    store: Store<ShimData>,
    instance: Instance,
    memory: Option<Memory>,
}

/// Single-threaded shared store that permits *re-entrant* access: binaryen's
/// init calls exports from inside host imports (import -> JS -> export), which a
/// `RefCell` would reject. wasmtime itself supports nested activations on one
/// store; only Rust's borrow check is in the way. SPIKE: aliasing `&mut Store`
/// is technically UB; the production path threads the active `Caller` instead.
struct StoreSlot(UnsafeCell<Running>);

impl StoreSlot {
    fn new(r: Running) -> Self {
        StoreSlot(UnsafeCell::new(r))
    }
    #[allow(clippy::mut_from_ref)]
    unsafe fn get(&self) -> &mut Running {
        &mut *self.0.get()
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

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let bundle_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/asc-host/asc-bundle.mjs".to_string());
    let source = std::env::args().nth(2).unwrap_or_else(|| {
        "export function add(a: i32, b: i32): i32 { return a + b; }".to_string()
    });
    let bundle = std::fs::read_to_string(&bundle_path).expect("read bundle");
    eprintln!("[asc] bundle {} ({} bytes)", bundle_path, bundle.len());

    let engine = Engine::default();

    let rt = AsyncRuntime::new().unwrap();
    rt.set_memory_limit(2 * 1024 * 1024 * 1024).await;
    rt.set_max_stack_size(64 * 1024 * 1024).await;
    let ctx = AsyncContext::full(&rt).await.unwrap();

    async_with!(ctx => |ctx| {
        let log = Function::new(ctx.clone(), |msg: String| eprintln!("[js] {msg}")).unwrap();
        ctx.globals().set("__log", log).unwrap();
        ctx.eval::<(), _>(PRELUDE).catch(&ctx).expect("prelude");
        install_webassembly(&ctx, engine.clone());

        eprintln!("[asc] evaluating bundle module...");
        match rquickjs::Module::evaluate(ctx.clone(), "asc_bundle", bundle).catch(&ctx) {
            Ok(p) => match p.into_future::<()>().await.catch(&ctx) {
                Ok(()) => eprintln!("[asc] module evaluated OK"),
                Err(e) => { eprintln!("[asc] module TLA REJECTED: {e}"); return; }
            },
            Err(e) => { eprintln!("[asc] module eval error: {e}"); return; }
        }

        let defined: bool = ctx.eval("typeof globalThis.__ascCompile === 'function'").unwrap_or(false);
        eprintln!("[asc] __ascCompile defined = {defined}");
        if !defined { return; }

        eprintln!("[asc] compiling AS source...");
        let compile: Function = ctx.globals().get("__ascCompile").unwrap();
        let res: Result<rquickjs::Promise, _> = compile.call((source.clone(),));
        match res {
            Ok(p) => match p.into_future::<Value>().await.catch(&ctx) {
                Ok(v) => {
                    let ta = rquickjs::TypedArray::<u8>::from_value(v).expect("compile result is a Uint8Array");
                    let wasm = ta.as_bytes().unwrap_or(&[]).to_vec();
                    eprintln!("[asc] COMPILE OK — wasm bytes = {}", wasm.len());
                    std::fs::write("/tmp/asc_out.wasm", &wasm).ok();
                    verify_output(&engine, &wasm);
                }
                Err(e) => eprintln!("[asc] compile rejected:\n{e}"),
            },
            Err(e) => eprintln!("[asc] compile call error: {e:?}"),
        }
    })
    .await;

    rt.idle().await;

    // The asc/binaryen toolchain creates JS<->Rust reference cycles across the
    // FFI boundary (the native `WebAssembly` funcs capture Rust `Store`s; binaryen's
    // module scope retains the JS `instance.exports`) that QuickJS's GC cannot trace.
    // Dropping the runtime would trip its debug `gc_obj_list` assertion. The compile
    // has already succeeded, so exit before teardown; a production embedding tears
    // the shim down explicitly (drop the Stores/Persistents) before freeing the runtime.
    drop(ctx);
    std::process::exit(0);
}

/// Independently load the asc-produced wasm in a fresh wasmtime store and run
/// its `add` export, proving the inline pipeline emitted a working module.
fn verify_output(engine: &Engine, wasm: &[u8]) {
    let module = match WtModule::new(engine, wasm) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("[verify] asc output is NOT valid wasm: {e}");
            return;
        }
    };
    eprintln!("[verify] wasmtime accepted asc output; exports:");
    for e in module.exports() {
        eprintln!("[verify]   {} : {:?}", e.name(), e.ty());
    }
    let mut store: Store<()> = Store::new(engine, ());
    let linker: Linker<()> = Linker::new(engine);
    let instance = match linker.instantiate(&mut store, &module) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("[verify] instantiate failed (may need env imports): {e}");
            return;
        }
    };
    // Call the first exported function with a fixed input (1,2,3,... per param)
    // to prove the emitted code actually runs.
    let first_func = module.exports().find_map(|e| match e.ty() {
        wasmtime::ExternType::Func(_) => Some(e.name().to_string()),
        _ => None,
    });
    let Some(fname) = first_func else {
        eprintln!("[verify] no function export to run");
        return;
    };
    let func = instance.get_func(&mut store, &fname).unwrap();
    let ty = func.ty(&store);
    let args: Vec<Val> = ty
        .params()
        .enumerate()
        .map(|(i, t)| {
            let n = (i + 1) as i64;
            match t {
                ValType::I32 => Val::I32(n as i32),
                ValType::I64 => Val::I64(n),
                ValType::F32 => Val::F32((n as f32).to_bits()),
                ValType::F64 => Val::F64((n as f64).to_bits()),
                _ => Val::I32(0),
            }
        })
        .collect();
    let mut results: Vec<Val> = ty.results().map(default_val).collect();
    match func.call(&mut store, &args, &mut results) {
        Ok(()) => eprintln!("[verify] {fname}({args:?}) -> {results:?}"),
        Err(e) => eprintln!("[verify] {fname} trapped: {e}"),
    }
}

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

    // WebAssembly.instantiate(bytes, importObject) -> { instance, module }
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
        linker
            .func_new("a", &name, ty, move |caller, params, results| {
                if std::env::var_os("ASC_TRACE").is_some() {
                    eprintln!("[imp] a::{nm}({params:?})");
                }
                let cptr = caller.data().ctx.0;
                let imports_a = caller.data().imports_a.clone();
                let ctx = unsafe { Ctx::from_raw(cptr) };
                let a = imports_a.restore(&ctx).map_err(wt_err)?;
                let f: Function = a.get(nm.as_str()).map_err(|e| {
                    eprintln!("[imp] a::{nm} MISSING in import object: {e}");
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
                        eprintln!("[imp] a::{nm} threw: {msg}");
                        return Err(wt_err(e));
                    }
                };
                if let (Some(slot), Some(rty)) = (results.get_mut(0), res_tys.first()) {
                    *slot = js_to_val(rty, &ret).map_err(wt_err)?;
                }
                Ok(())
            })
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

    let running = Rc::new(StoreSlot::new(Running {
        store,
        instance,
        memory,
    }));

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
                let val = {
                    let r = unsafe { running.get() };
                    let Running {
                        store, instance, ..
                    } = r;
                    instance
                        .get_global(&mut *store, &ename)
                        .map(|g| g.get(&mut *store))
                };
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
fn build_memory_object<'js>(
    ctx: &Ctx<'js>,
    running: &Rc<StoreSlot>,
) -> rquickjs::Result<Object<'js>> {
    let mem_obj = Object::new(ctx.clone())?;
    let run = running.clone();
    let buf_getter = Function::new(ctx.clone(), move |cx: Ctx<'js>| -> Value<'js> {
        let r = unsafe { run.get() };
        let mem = r.memory.unwrap();
        let ptr = mem.data_ptr(&r.store);
        let len = mem.data_size(&r.store);
        external_array_buffer(&cx, ptr, len)
    })?;
    // `buffer` is a getter so callers always observe the current (possibly grown) memory.
    define_getter(ctx, &mem_obj, "buffer", buf_getter)?;
    let run_g = running.clone();
    let grow = Function::new(ctx.clone(), move |pages: u64| -> i64 {
        let r = unsafe { run_g.get() };
        let mem = r.memory.unwrap();
        match mem.grow(&mut r.store, pages) {
            Ok(prev) => prev as i64,
            Err(_) => -1,
        }
    })?;
    mem_obj.set("grow", grow)?;
    Ok(mem_obj)
}

fn call_export<'js>(
    ctx: &Ctx<'js>,
    run: &Rc<StoreSlot>,
    name: &str,
    args: &[Value<'js>],
) -> rquickjs::Result<Value<'js>> {
    let func = {
        let r = unsafe { run.get() };
        r.instance.get_func(&mut r.store, name)
    }
    .ok_or_else(|| rquickjs::Error::new_from_js_message("wasm", "export", "missing export"))?;
    invoke_func(ctx, run, func, name, args)
}

/// Call a wasmtime `Func` with JS args, marshalling the (single) result back.
fn invoke_func<'js>(
    ctx: &Ctx<'js>,
    run: &Rc<StoreSlot>,
    func: Func,
    label: &str,
    args: &[Value<'js>],
) -> rquickjs::Result<Value<'js>> {
    let r = unsafe { run.get() };
    let store = &mut r.store;
    let ty = func.ty(&*store);
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
    if std::env::var_os("ASC_TRACE").is_some() {
        eprintln!("[exp] {label}({params:?})");
    }
    func.call(&mut *store, &params, &mut results).map_err(|e| {
        let trap = e.downcast_ref::<wasmtime::Trap>();
        eprintln!("[exp] {label} TRAP {trap:?}: {e:?}");
        rquickjs::Error::new_from_js_message("wasm", "wasm", format!("call {label}: {e}"))
    })?;
    match results.first() {
        Some(v) => val_to_js(ctx, v),
        None => Ok(Value::new_undefined(ctx.clone())),
    }
}

/// Wrap a wasmtime `Func` handle as a JS callable (used for table entries).
fn wrap_func<'js>(
    ctx: &Ctx<'js>,
    run: &Rc<StoreSlot>,
    func: Func,
) -> rquickjs::Result<Function<'js>> {
    let run = run.clone();
    Function::new(ctx.clone(), move |cx: Ctx<'js>, args: Rest<Value<'js>>| {
        invoke_func(&cx, &run, func, "table_entry", &args.0)
    })
}

/// A `WebAssembly.Table`-like object: `.get(idx)` returns a JS callable for the
/// funcref at that slot (binaryen's indirect-call glue uses `wasmTable.get`).
fn build_table_object<'js>(
    ctx: &Ctx<'js>,
    running: &Rc<StoreSlot>,
    tname: String,
) -> rquickjs::Result<Object<'js>> {
    let tbl = Object::new(ctx.clone())?;
    let run = running.clone();
    let tn = tname.clone();
    let get = Function::new(
        ctx.clone(),
        move |cx: Ctx<'js>, idx: u32| -> rquickjs::Result<Value<'js>> {
            let func = {
                let r = unsafe { run.get() };
                match r.instance.get_table(&mut r.store, &tn) {
                    Some(t) => match t.get(&mut r.store, idx as u64) {
                        Some(Ref::Func(f)) => f,
                        _ => None,
                    },
                    None => None,
                }
            };
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
        let r = unsafe { run_l.get() };
        r.instance
            .get_table(&mut r.store, &tn_l)
            .map(|t| t.size(&r.store) as u32)
            .unwrap_or(0)
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
