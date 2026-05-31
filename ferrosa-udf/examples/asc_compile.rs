//! Demo driver for the inline AssemblyScript -> WASM compiler library
//! (`ferrosa_udf::asc`). Requires the `asc-udf` feature.
//!
//!   ./ferrosa-udf/examples/asc-poc/build-bundle.sh /tmp/asc-host/asc-bundle.mjs
//!   cargo run -p ferrosa-udf --example asc_compile --features asc-udf -- \
//!       /tmp/asc-host/asc-bundle.mjs \
//!       'export function add(a: i32, b: i32): i32 { return a + b; }'

use wasmtime::{Engine, Linker, Store, Val, ValType};

fn main() {
    let bundle = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/asc-host/asc-bundle.mjs".to_string());
    let source = std::env::args().nth(2).unwrap_or_else(|| {
        "export function add(a: i32, b: i32): i32 { return a + b; }".to_string()
    });
    // The library locates the bundle via FERROSA_ASC_BUNDLE.
    std::env::set_var("FERROSA_ASC_BUNDLE", &bundle);
    eprintln!("[asc] bundle {bundle}");

    match ferrosa_udf::asc::compile_assemblyscript(&source) {
        Ok(wasm) => {
            eprintln!("[asc] COMPILE OK — wasm bytes = {}", wasm.len());
            std::fs::write("/tmp/asc_out.wasm", &wasm).ok();
            verify_output(&wasm);
        }
        Err(e) => eprintln!("[asc] compile error: {e}"),
    }
}

/// Load the asc output in a fresh wasmtime store and run its first function
/// export with a fixed input, proving the emitted module executes.
fn verify_output(wasm: &[u8]) {
    let engine = Engine::default();
    let module = match wasmtime::Module::new(&engine, wasm) {
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
    let mut store: Store<()> = Store::new(&engine, ());
    let instance = match Linker::<()>::new(&engine).instantiate(&mut store, &module) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("[verify] instantiate failed: {e}");
            return;
        }
    };
    let Some(fname) = module.exports().find_map(|e| match e.ty() {
        wasmtime::ExternType::Func(_) => Some(e.name().to_string()),
        _ => None,
    }) else {
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
    let mut results: Vec<Val> = ty
        .results()
        .map(|t| match t {
            ValType::I64 => Val::I64(0),
            ValType::F32 => Val::F32(0),
            ValType::F64 => Val::F64(0),
            _ => Val::I32(0),
        })
        .collect();
    match func.call(&mut store, &args, &mut results) {
        Ok(()) => eprintln!("[verify] {fname}({args:?}) -> {results:?}"),
        Err(e) => eprintln!("[verify] {fname} trapped: {e}"),
    }
}
