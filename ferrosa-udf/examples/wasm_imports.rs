//! Spike helper: load a wasm module in wasmtime and dump its imports/exports
//! with signatures — used to size the wasmtime-backed `WebAssembly` shim for the
//! inline-language UDF compiler.
//!
//!   cargo run -p ferrosa-udf --example wasm_imports -- /tmp/binaryen.wasm

use std::collections::BTreeMap;
use wasmtime::{Engine, ExternType, Module};

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: wasm_imports <file.wasm>");
    let engine = Engine::default();
    let module = match Module::from_file(&engine, &path) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("wasmtime REJECTED {path}: {e}");
            std::process::exit(1);
        }
    };
    println!("wasmtime accepted {path}\n");

    // Imports grouped by signature, so we see the distinct shapes to bridge.
    let mut by_sig: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut nfunc = 0usize;
    let mut other = Vec::new();
    for imp in module.imports() {
        let full = format!("{}::{}", imp.module(), imp.name());
        match imp.ty() {
            ExternType::Func(ft) => {
                nfunc += 1;
                let sig = format!(
                    "({}) -> ({})",
                    ft.params()
                        .map(|t| format!("{t}"))
                        .collect::<Vec<_>>()
                        .join(","),
                    ft.results()
                        .map(|t| format!("{t}"))
                        .collect::<Vec<_>>()
                        .join(","),
                );
                by_sig.entry(sig).or_default().push(full);
            }
            other_ty => other.push(format!("{full}: {other_ty:?}")),
        }
    }
    println!("== {nfunc} function imports, grouped by signature ==");
    for (sig, names) in &by_sig {
        println!(
            "  {:3}x  {}   e.g. {}",
            names.len(),
            sig,
            names[..names.len().min(6)].join(",")
        );
    }
    if !other.is_empty() {
        println!("\n== non-function imports ({}) ==", other.len());
        for o in &other {
            println!("  {o}");
        }
    }

    // Exports of interest (memory + a few function exports).
    println!("\n== exports ==");
    let mut mem = 0;
    let mut func = 0;
    let mut sample = Vec::new();
    for exp in module.exports() {
        match exp.ty() {
            ExternType::Memory(_) => {
                mem += 1;
                println!("  memory export: {}", exp.name());
            }
            ExternType::Func(_) => {
                func += 1;
                if sample.len() < 12 {
                    sample.push(exp.name().to_string());
                }
            }
            _ => {}
        }
    }
    println!(
        "  {func} function exports (e.g. {}), {mem} memory export(s)",
        sample.join(",")
    );
}
