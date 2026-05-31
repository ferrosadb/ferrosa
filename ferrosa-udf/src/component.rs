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

/// How a CQL type crosses the canonical-ABI `cql-value` boundary.
enum Repr {
    /// Fixed-width numeric: a single `load`/`store` of `mem`-typed payload.
    Num {
        mem: &'static str,
        decl: &'static str,
    },
    /// UTF-8 string (`text`/`ascii`): canonical `(ptr, len)` <-> AS `string`.
    Text,
    /// Byte string (`blob`): canonical `list<u8>` `(ptr, len)` <-> AS `Uint8Array`.
    Blob,
}

/// Map a CQL type to its canonical-ABI representation (and `cql-value`
/// discriminant), or reject it (collection/temporal/decimal types are not yet
/// supported by the generated adapter).
fn abi_type(t: &CqlType) -> Result<(Repr, u8), AscError> {
    Ok(match t {
        CqlType::Int => (
            Repr::Num {
                mem: "i32",
                decl: "i32",
            },
            1,
        ),
        CqlType::Bigint => (
            Repr::Num {
                mem: "i64",
                decl: "i64",
            },
            2,
        ),
        CqlType::Float => (
            Repr::Num {
                mem: "f32",
                decl: "f32",
            },
            3,
        ),
        CqlType::Double => (
            Repr::Num {
                mem: "f64",
                decl: "f64",
            },
            4,
        ),
        CqlType::Smallint => (
            Repr::Num {
                mem: "i16",
                decl: "i16",
            },
            12,
        ),
        CqlType::Tinyint => (
            Repr::Num {
                mem: "i8",
                decl: "i8",
            },
            13,
        ),
        CqlType::Varchar => (Repr::Text, 6),
        CqlType::Ascii => (Repr::Text, 18),
        CqlType::Blob => (Repr::Blob, 7),
        other => {
            return Err(AscError::Compile(format!(
                "AssemblyScript UDFs support numeric (int, bigint, float, double, \
                 smallint, tinyint), text/ascii, and blob types; got {other:?}"
            )))
        }
    })
}

impl Repr {
    /// AssemblyScript type the user's function declares for this position.
    fn decl(&self) -> &'static str {
        match self {
            Repr::Num { decl, .. } => decl,
            Repr::Text => "string",
            Repr::Blob => "Uint8Array",
        }
    }

    /// AS expression reading argument `i` (payload at `argsPtr + i*24 + 8`,
    /// strings/blobs as a `(ptr, len)` pair at +8/+12).
    fn read_arg(&self, i: usize) -> String {
        let p = i * 24 + 8;
        let l = i * 24 + 12;
        match self {
            Repr::Num { mem, .. } => format!("load<{mem}>(argsPtr + {p})"),
            Repr::Text => format!(
                "String.UTF8.decodeUnsafe(<usize>load<i32>(argsPtr + {p}), <usize>load<i32>(argsPtr + {l}))"
            ),
            Repr::Blob => format!("__u8(load<i32>(argsPtr + {p}), load<i32>(argsPtr + {l}))"),
        }
    }

    /// AS statements writing `__r` into the result `cql-value` at `__out` (its
    /// discriminant at +8, payload at +16). String/blob bytes are bump-allocated
    /// and reclaimed by the post-return `__reset`.
    fn write_result(&self, disc: u8) -> String {
        match self {
            Repr::Num { mem, .. } => {
                format!("store<u8>(__out + 8, {disc});\n  store<{mem}>(__out + 16, __r);")
            }
            Repr::Text => format!(
                "store<u8>(__out + 8, {disc});\n  \
                 let __n: i32 = <i32>String.UTF8.byteLength(__r);\n  \
                 let __b: i32 = <i32>heap.alloc(<usize>__n);\n  \
                 String.UTF8.encodeUnsafe(changetype<usize>(__r), __r.length, <usize>__b);\n  \
                 store<i32>(__out + 16, __b);\n  store<i32>(__out + 20, __n);"
            ),
            Repr::Blob => format!(
                "store<u8>(__out + 8, {disc});\n  \
                 let __n: i32 = __r.length;\n  \
                 let __b: i32 = <i32>heap.alloc(<usize>__n);\n  \
                 memory.copy(<usize>__b, __r.dataStart, <usize>__n);\n  \
                 store<i32>(__out + 16, __b);\n  store<i32>(__out + 20, __n);"
            ),
        }
    }
}

/// Generate the AssemblyScript `invoke` adapter that bridges the canonical ABI
/// to the user's function `name`. Appended to the user's source and compiled
/// together, so it can call `name` directly.
///
/// Each lowered `cql-value` argument is 24 bytes (payload at +8); the result
/// area is `result<cql-value,string>` (Ok discriminant at +0, the cql-value at
/// +8: its discriminant at +8, payload at +16). Allocations (arg lowering via
/// `cabi_realloc`, the result struct, decoded args, serialized strings/blobs)
/// all go through the AS stub bump allocator and are reclaimed in one shot by the
/// canonical post-return `cabi_post_invoke` calling `__reset`, so nothing leaks
/// across pooled invocations.
fn generate_adapter(
    name: &str,
    arg_types: &[CqlType],
    return_type: &CqlType,
) -> Result<String, AscError> {
    let (ret_repr, ret_disc) = abi_type(return_type)?;
    let mut call_args = Vec::with_capacity(arg_types.len());
    for (i, t) in arg_types.iter().enumerate() {
        let (repr, _) = abi_type(t)?;
        call_args.push(repr.read_arg(i));
    }
    let call = format!("{name}({})", call_args.join(", "));

    // Helper to copy raw bytes into an AS Uint8Array (for blob args).
    let blob_helper = if arg_types.iter().any(|t| matches!(t, CqlType::Blob)) {
        "function __u8(ptr: i32, len: i32): Uint8Array {\n  \
           let a = new Uint8Array(len);\n  \
           memory.copy(a.dataStart, <usize>ptr, <usize>len);\n  \
           return a;\n}\n"
    } else {
        ""
    };

    Ok(format!(
        r#"
// --- generated canonical-ABI adapter (ferrosa-udf) ---
{blob_helper}export function cabi_realloc(ptr: i32, oldSize: i32, align: i32, newSize: i32): i32 {{
  return <i32>heap.alloc(<usize>newSize);
}}
export function cabi_post_invoke(ret: i32): void {{ __reset(); }}
export function invoke(argsPtr: i32, argsLen: i32): i32 {{
  let __r: {ret_decl} = {call};
  let __out: i32 = <i32>heap.alloc(32);
  store<u8>(__out, 0);
  {result_write}
  return __out;
}}
"#,
        ret_decl = ret_repr.decl(),
        result_write = ret_repr.write_result(ret_disc),
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
        .map_err(|e| AscError::Compile(format!("encoding component: {e:#}")))
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

    /// `generate_adapter` rejects still-unsupported types with a clear error.
    #[test]
    fn adapter_rejects_unsupported_type() {
        let err = generate_adapter("f", &[CqlType::Decimal], &CqlType::Int).unwrap_err();
        assert!(
            matches!(err, AscError::Compile(ref m) if m.contains("Decimal")),
            "expected a Compile error naming the type, got {err:?}"
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

    #[cfg(feature = "live-infra-tests")]
    fn require_bundle() {
        if std::env::var_os("FERROSA_ASC_BUNDLE").is_none() {
            panic!(
                "FERROSA_ASC_BUNDLE is not set. Build the asc bundle first:\n  \
                 ./ferrosa-udf/examples/asc-poc/build-bundle.sh /tmp/asc-host/asc-bundle.mjs"
            );
        }
    }

    /// End-to-end text UDF: a `string -> string` function round-trips through the
    /// canonical `text-val(string)` boundary.
    #[cfg(feature = "live-infra-tests")]
    #[test]
    fn assemblyscript_text_uppercase_roundtrips() {
        require_bundle();
        let args = [CqlType::Varchar];
        let component = compile_to_component(
            "upper",
            "export function upper(s: string): string { return s.toUpperCase(); }",
            &args,
            &CqlType::Varchar,
        )
        .expect("compile text UDF");
        let exec = UdfExecutor::new(SandboxConfig::default()).expect("executor");
        exec.compile("ks", "upper", &args, &component)
            .expect("compile");
        let out = exec
            .call(
                "ks",
                "upper",
                vec![CqlValue::Text("héllo".into())],
                &args,
                &CqlType::Varchar,
            )
            .expect("invoke");
        assert_eq!(out, CqlValue::Text("HÉLLO".into()));
    }

    /// End-to-end blob UDF: a `Uint8Array -> Uint8Array` function round-trips
    /// through the canonical `blob-val(list<u8>)` boundary.
    #[cfg(feature = "live-infra-tests")]
    #[test]
    fn assemblyscript_blob_reverse_roundtrips() {
        require_bundle();
        let args = [CqlType::Blob];
        let component = compile_to_component(
            "rev",
            "export function rev(b: Uint8Array): Uint8Array { \
               let o = new Uint8Array(b.length); \
               for (let i = 0; i < b.length; i++) o[i] = b[b.length - 1 - i]; \
               return o; }",
            &args,
            &CqlType::Blob,
        )
        .expect("compile blob UDF");
        let exec = UdfExecutor::new(SandboxConfig::default()).expect("executor");
        exec.compile("ks", "rev", &args, &component)
            .expect("compile");
        let out = exec
            .call(
                "ks",
                "rev",
                vec![CqlValue::Blob(vec![1, 2, 3, 4])],
                &args,
                &CqlType::Blob,
            )
            .expect("invoke");
        assert_eq!(out, CqlValue::Blob(vec![4, 3, 2, 1]));
    }
}
