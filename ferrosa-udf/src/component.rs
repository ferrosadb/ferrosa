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
}
