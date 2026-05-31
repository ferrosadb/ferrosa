//! Wrap an asc-compiled **core** WASM module into a wasmtime **component** that
//! matches the executor's `udf` WIT world (`invoke(list<cql-value>) ->
//! result<cql-value, string>`).
//!
//! The core module must already implement the canonical-ABI `invoke` export
//! (plus `memory` and `cabi_realloc`). This step embeds the `udf` world metadata
//! and runs `wit-component`'s encoder, which emits the `canon lift` wrapping so
//! wasmtime's component model can call it.

use crate::asc::AscError;

/// The `udf` world used for componentization.
///
/// This is the host `cql-value` variant with the **recursive** collection cases
/// (`list-val`, `set-val`, `map-val`, `tuple-val`, `udt-val`) removed — WIT's
/// topological resolver rejects self-referential types, and the Component Model
/// cannot encode them. The executor lowers/lifts `Val::Variant` **by case name**,
/// so every retained case keeps the same name and a UDF over any of these scalar
/// types round-trips correctly. UDFs whose args/return use the omitted collection
/// types are not yet componentizable (a future msgpack-blob bridge, cf. the
/// `collection-val` approach in the guest-uda example).
const UDF_WIT: &str = r#"
package ferrosa:udf-component@0.2.0;

interface types {
    variant cql-value {
        null,
        int-val(s32),
        bigint-val(s64),
        float-val(f32),
        double-val(f64),
        boolean-val(bool),
        text-val(string),
        blob-val(list<u8>),
        uuid-val(string),
        timestamp-val(s64),
        date-val(s32),
        time-val(s64),
        smallint-val(s16),
        tinyint-val(s8),
        inet-val(string),
        decimal-val(tuple<list<u8>, s32>),
        varint-val(list<u8>),
        duration-val(tuple<s32, s32, s64>),
        ascii-val(string),
        timeuuid-val(string),
        counter-val(s64),
    }
}

world udf {
    use types.{cql-value};
    export invoke: func(args: list<cql-value>) -> result<cql-value, string>;
}
"#;

use ferrosa_common::CqlType;

/// How a numeric CQL type crosses the canonical-ABI `cql-value` boundary.
struct AbiType {
    /// AssemblyScript `load`/`store` type (e.g. `i32`, `i64`, `f64`).
    mem: &'static str,
    /// AssemblyScript parameter/return type the user's function uses.
    decl: &'static str,
    /// `cql-value` variant discriminant (case index in the non-recursive WIT).
    disc: u8,
}

/// Map a CQL type to its canonical-ABI representation, or reject it (only the
/// fixed-width numeric scalar types are supported by the generated adapter).
fn abi_type(t: &CqlType) -> Result<AbiType, AscError> {
    let (mem, decl, disc) = match t {
        CqlType::Int => ("i32", "i32", 1),
        CqlType::Bigint => ("i64", "i64", 2),
        CqlType::Float => ("f32", "f32", 3),
        CqlType::Double => ("f64", "f64", 4),
        CqlType::Smallint => ("i16", "i16", 12),
        CqlType::Tinyint => ("i8", "i8", 13),
        other => {
            return Err(AscError::Compile(format!(
                "AssemblyScript UDFs currently support only fixed-width numeric \
                 types (int, bigint, float, double, smallint, tinyint); got {other:?}"
            )))
        }
    };
    Ok(AbiType { mem, decl, disc })
}

/// Generate the AssemblyScript `invoke` adapter that bridges the canonical ABI
/// to the user's function `name`. Appended to the user's source and compiled
/// together, so it can call `name` directly.
///
/// Each lowered `cql-value` argument is 24 bytes (payload at +8); the result
/// area is `result<cql-value,string>` (Ok discriminant at +0, the cql-value at
/// +8: its discriminant at +8, payload at +16). A bump arena (reset by the
/// canonical post-return `cabi_post_invoke`) backs both arg lowering and the
/// result, so nothing leaks across pooled invocations.
fn generate_adapter(
    name: &str,
    arg_types: &[CqlType],
    return_type: &CqlType,
) -> Result<String, AscError> {
    let ret = abi_type(return_type)?;
    let mut call_args = Vec::with_capacity(arg_types.len());
    for (i, t) in arg_types.iter().enumerate() {
        let a = abi_type(t)?;
        let off = 8 + i * 24;
        call_args.push(format!("load<{}>(argsPtr + {off})", a.mem));
    }
    let call = format!("{name}({})", call_args.join(", "));

    // Fixed scratch regions, no mutable globals / allocator: pulling in asc's
    // stub runtime (mutable heap-offset global) makes binaryen's optimizer abort.
    // `memory.data(N)` reserves a static, zeroed buffer and forces the initial
    // memory to cover it, giving a safe base for both arg lowering and the result.
    // The host lowers the (flat numeric) args list in one `cabi_realloc` call; the
    // guest owns the result area, so no post-return cleanup is needed.
    Ok(format!(
        r#"
// --- generated canonical-ABI adapter (ferrosa-udf) ---
const __scratch: usize = memory.data(131072);
const __ARG: i32 = <i32>__scratch;
const __RET: i32 = <i32>__scratch + 65536;
export function cabi_realloc(ptr: i32, oldSize: i32, align: i32, newSize: i32): i32 {{
  return __ARG;
}}
export function invoke(argsPtr: i32, argsLen: i32): i32 {{
  let __r: {ret_decl} = {call};
  store<u8>(__RET, 0);
  store<u8>(__RET + 8, {ret_disc});
  store<{ret_mem}>(__RET + 16, __r);
  return __RET;
}}
"#,
        ret_decl = ret.decl,
        ret_mem = ret.mem,
        ret_disc = ret.disc,
    ))
}

/// Compile AssemblyScript `source` (which must export a function named `name`)
/// into a `udf`-world component invoking it with the given CQL signature.
pub fn compile_to_component(
    name: &str,
    source: &str,
    arg_types: &[CqlType],
    return_type: &CqlType,
) -> Result<Vec<u8>, AscError> {
    let adapter = generate_adapter(name, arg_types, return_type)?;
    let full = format!("{source}\n{adapter}");
    let core = crate::asc::compile_assemblyscript(&full)?;
    componentize(&core)
}

/// Wrap a canonical-ABI core module into a `udf`-world component.
pub fn componentize(core_wasm: &[u8]) -> Result<Vec<u8>, AscError> {
    let mut resolve = wit_parser::Resolve::new();
    let pkg = resolve
        .push_str("ferrosa-udf.wit", UDF_WIT)
        .map_err(|e| AscError::Internal(format!("parsing udf WIT: {e}")))?;
    let world = resolve
        .select_world(&[pkg], Some("udf"))
        .map_err(|e| AscError::Internal(format!("selecting udf world: {e}")))?;

    let mut module = core_wasm.to_vec();
    wit_component::embed_component_metadata(
        &mut module,
        &resolve,
        world,
        wit_component::StringEncoding::UTF8,
    )
    .map_err(|e| AscError::Internal(format!("embedding component metadata: {e}")))?;

    wit_component::ComponentEncoder::default()
        .module(&module)
        .map_err(|e| AscError::Compile(format!("componentizing core module: {e}")))?
        .validate(true)
        .encode()
        .map_err(|e| AscError::Compile(format!("encoding component: {e}")))
}

#[cfg(all(test, feature = "asc-udf"))]
mod tests {
    use super::*;
    use crate::{SandboxConfig, UdfExecutor};
    use ferrosa_common::{CqlType, CqlValue};

    /// A hand-written canonical-ABI core module whose `invoke` ignores its args
    /// and returns `Ok(int-val(42))`. Proves the componentization pipeline and
    /// the canonical-ABI return layout for `result<cql-value, string>`.
    ///
    /// Return area at offset 16: result discriminant (Ok=0) at +0; the ok
    /// `cql-value` at +8 (its discriminant int-val=1 at +8, s32 payload at +16).
    const CONST_INVOKE_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $bump (mut i32) (i32.const 1024))
      (func (export "cabi_realloc") (param i32 i32 i32 i32) (result i32)
        (local $r i32)
        (local.set $r (global.get $bump))
        (global.set $bump (i32.add (global.get $bump) (local.get 3)))
        (local.get $r))
      (func (export "invoke") (param i32 i32) (result i32)
        (i32.store8 (i32.const 16) (i32.const 0))   ;; result discriminant = Ok
        (i32.store8 (i32.const 24) (i32.const 1))   ;; cql-value discriminant = int-val
        (i32.store (i32.const 32) (i32.const 42))   ;; s32 payload = 42
        (i32.const 16)))
    "#;

    #[test]
    fn constant_invoke_returns_ok_int() {
        let core = wat::parse_str(CONST_INVOKE_WAT).expect("assemble core module");
        let component = componentize(&core).expect("componentize");

        let exec = UdfExecutor::new(SandboxConfig::default()).expect("executor");
        exec.compile("ks", "answer", &[], &component)
            .expect("executor accepts componentized module");
        let out = exec
            .call("ks", "answer", vec![], &[], &CqlType::Int)
            .expect("invoke");
        assert_eq!(out, CqlValue::Int(42));
    }

    /// Build a core module whose `invoke` body is `body`. The lowered
    /// `list<cql-value>` arrives as (`$a` = element pointer, `$n` = length); each
    /// element is a 24-byte `cql-value` (discriminant at +0, payload at +8).
    fn core_with_invoke(body: &str) -> Vec<u8> {
        let src = format!(
            r#"
            (module
              (memory (export "memory") 1)
              (global $bump (mut i32) (i32.const 1024))
              (func (export "cabi_realloc") (param i32 i32 i32 i32) (result i32)
                (local $r i32)
                (local.set $r (global.get $bump))
                (global.set $bump (i32.add (global.get $bump) (local.get 3)))
                (local.get $r))
              (func (export "invoke") (param $a i32) (param $n i32) (result i32)
                (i32.store8 (i32.const 16) (i32.const 0))   ;; result = Ok
                (i32.store8 (i32.const 24) (i32.const 1))   ;; cql-value = int-val
                (i32.store (i32.const 32) ({body}))         ;; s32 payload
                (i32.const 16)))
            "#
        );
        wat::parse_str(&src).expect("assemble core module")
    }

    /// `invoke` reads arg[0] as an int and returns it: proves list-element and
    /// payload offsets for the lowered `list<cql-value>`.
    #[test]
    fn echo_invoke_reads_first_int_arg() {
        let core = core_with_invoke("i32.load (i32.add (local.get $a) (i32.const 8))");
        let component = componentize(&core).expect("componentize");
        let exec = UdfExecutor::new(SandboxConfig::default()).expect("executor");
        exec.compile("ks", "echo", &[CqlType::Int], &component)
            .expect("compile");
        let out = exec
            .call(
                "ks",
                "echo",
                vec![CqlValue::Int(7)],
                &[CqlType::Int],
                &CqlType::Int,
            )
            .expect("invoke");
        assert_eq!(out, CqlValue::Int(7));
    }

    /// `generate_adapter` rejects non-numeric types with a clear error.
    #[test]
    fn adapter_rejects_text_type() {
        let err = generate_adapter("f", &[CqlType::Varchar], &CqlType::Int).unwrap_err();
        assert!(
            matches!(err, AscError::Compile(ref m) if m.contains("numeric")),
            "expected a numeric-only Compile error, got {err:?}"
        );
    }

    /// `invoke` adds arg[0] + arg[1]: proves element stride (24 bytes).
    #[test]
    fn add_invoke_sums_two_int_args() {
        let core = core_with_invoke(
            "i32.add \
               (i32.load (i32.add (local.get $a) (i32.const 8))) \
               (i32.load (i32.add (local.get $a) (i32.const 32)))",
        );
        let component = componentize(&core).expect("componentize");
        let exec = UdfExecutor::new(SandboxConfig::default()).expect("executor");
        exec.compile("ks", "add", &[CqlType::Int, CqlType::Int], &component)
            .expect("compile");
        let out = exec
            .call(
                "ks",
                "add",
                vec![CqlValue::Int(2), CqlValue::Int(3)],
                &[CqlType::Int, CqlType::Int],
                &CqlType::Int,
            )
            .expect("invoke");
        assert_eq!(out, CqlValue::Int(5));
    }

    /// End-to-end: compile AssemblyScript `add` source through the asc toolchain,
    /// generate the adapter, componentize, and invoke via the executor.
    /// Requires the asc bundle (FERROSA_ASC_BUNDLE) — see asc::tests.
    #[cfg(feature = "live-infra-tests")]
    #[test]
    fn assemblyscript_add_compiles_componentizes_and_runs() {
        if std::env::var_os("FERROSA_ASC_BUNDLE").is_none() {
            panic!(
                "FERROSA_ASC_BUNDLE is not set. Build the asc bundle first:\n  \
                 ./ferrosa-udf/examples/asc-poc/build-bundle.sh /tmp/asc-host/asc-bundle.mjs\n  \
                 FERROSA_ASC_BUNDLE=/tmp/asc-host/asc-bundle.mjs cargo test -p ferrosa-udf \
                 --features 'asc-udf live-infra-tests' component::"
            );
        }
        let args = [CqlType::Int, CqlType::Int];
        let component = compile_to_component(
            "add",
            "export function add(a: i32, b: i32): i32 { return a + b; }",
            &args,
            &CqlType::Int,
        )
        .expect("compile AssemblyScript to component");

        let exec = UdfExecutor::new(SandboxConfig::default()).expect("executor");
        exec.compile("ks", "add", &args, &component)
            .expect("executor compiles asc component");
        let out = exec
            .call(
                "ks",
                "add",
                vec![CqlValue::Int(2), CqlValue::Int(3)],
                &args,
                &CqlType::Int,
            )
            .expect("invoke");
        assert_eq!(out, CqlValue::Int(5));
    }
}
