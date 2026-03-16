//! WASM function executor with compilation cache.
//!
//! Provides real Wasmtime Component Model compilation and invocation for
//! CQL User-Defined Functions. The executor validates WASM at compile time
//! and invokes the `invoke` export at call time with fuel-based metering.

use std::sync::Arc;

use ferrosa_common::{CqlType, CqlValue};
use wasmtime::component::{Component, Linker, Val};
use wasmtime::{Engine, Store};

use crate::convert::{cql_to_wit, WitCqlValue};
use crate::error::UdfError;
use crate::sandbox::SandboxConfig;

/// Compiled WASM component ready for instantiation.
struct CompiledFunction {
    component: Component,
}

/// Executor for WASM-based User-Defined Functions.
///
/// Manages the Wasmtime engine, compilation cache, and sandbox policy.
/// Thread-safe — can be shared across async tasks via `Arc`.
pub struct UdfExecutor {
    engine: Engine,
    config: SandboxConfig,
    cache: moka::sync::Cache<(String, String), Arc<CompiledFunction>>,
}

impl UdfExecutor {
    /// Create a new executor with the given sandbox configuration.
    pub fn new(config: SandboxConfig) -> Result<Self, UdfError> {
        let mut engine_config = wasmtime::Config::new();
        engine_config.consume_fuel(true);
        engine_config.epoch_interruption(true);
        engine_config.wasm_component_model(true);

        let engine = Engine::new(&engine_config)
            .map_err(|e| UdfError::CompilationFailed(format!("engine creation failed: {e}")))?;

        let cache = moka::sync::Cache::builder()
            .max_capacity(config.cache_capacity)
            .build();

        Ok(Self {
            engine,
            config,
            cache,
        })
    }

    /// Pre-compile a WASM binary. Called on INSERT into wasm_binaries.
    ///
    /// Validates the WASM component bytes using the Wasmtime compiler.
    /// The compiled component is cached for later invocation.
    pub fn compile(&self, keyspace: &str, name: &str, wasm_bytes: &[u8]) -> Result<(), UdfError> {
        if wasm_bytes.len() > self.config.max_wasm_size {
            return Err(UdfError::BinaryTooLarge {
                size: wasm_bytes.len(),
                max: self.config.max_wasm_size,
            });
        }

        let component = Component::new(&self.engine, wasm_bytes)
            .map_err(|e| UdfError::CompilationFailed(format!("{e}")))?;

        tracing::info!(
            keyspace,
            name,
            size = wasm_bytes.len(),
            "compiled WASM function"
        );

        self.cache.insert(
            (keyspace.to_string(), name.to_string()),
            Arc::new(CompiledFunction { component }),
        );
        Ok(())
    }

    /// Invalidate cached compilation (on CREATE OR REPLACE).
    pub fn invalidate(&self, keyspace: &str, name: &str) {
        self.cache
            .invalidate(&(keyspace.to_string(), name.to_string()));
    }

    /// Invoke a UDF. Returns the function's result.
    ///
    /// Creates a fresh `Store` with fuel limits, instantiates the cached
    /// component, converts CQL arguments to WIT values, calls the `invoke`
    /// export, and converts the result back to `CqlValue`.
    pub fn call(
        &self,
        keyspace: &str,
        func_name: &str,
        args: Vec<CqlValue>,
        _arg_types: &[CqlType],
        return_type: &CqlType,
    ) -> Result<CqlValue, UdfError> {
        let key = (keyspace.to_string(), func_name.to_string());
        let compiled = self.cache.get(&key).ok_or_else(|| UdfError::NotFound {
            keyspace: keyspace.to_string(),
            name: func_name.to_string(),
        })?;

        // Create a store with fuel metering
        let mut store = Store::new(&self.engine, ());
        store
            .set_fuel(self.config.max_fuel)
            .map_err(|e| UdfError::ExecutionFailed(format!("failed to set fuel: {e}")))?;

        // Epoch-based interruption for wall-clock timeout
        store.epoch_deadline_trap();
        store.set_epoch_deadline(1);

        // Instantiate the component
        let linker = Linker::<()>::new(&self.engine);
        let instance = linker
            .instantiate(&mut store, &compiled.component)
            .map_err(|e| UdfError::ExecutionFailed(format!("instantiation failed: {e}")))?;

        // Look up the "invoke" export
        let invoke_func = instance.get_func(&mut store, "invoke").ok_or_else(|| {
            UdfError::ExecutionFailed("component does not export 'invoke' function".into())
        })?;

        // Convert CQL args to WIT representation, then to component Val
        let wit_args: Vec<WitCqlValue> = args.iter().map(cql_to_wit).collect();
        let args_val = wit_cql_list_to_val(&wit_args);

        // Call the function with dynamic args/results
        let mut results = vec![Val::Bool(false)]; // placeholder for result slot
        invoke_func
            .call(&mut store, &[args_val], &mut results)
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("fuel") {
                    UdfError::ResourceExhausted(format!("out of fuel: {msg}"))
                } else if msg.contains("epoch") {
                    UdfError::ResourceExhausted(format!("execution timeout: {msg}"))
                } else {
                    UdfError::ExecutionFailed(msg)
                }
            })?;

        // Post-call cleanup required by wasmtime component model
        invoke_func
            .post_return(&mut store)
            .map_err(|e| UdfError::ExecutionFailed(format!("post_return failed: {e}")))?;

        // Convert result Val back to CqlValue
        // The WIT contract returns result<cql-value, string>
        let result_val = results
            .into_iter()
            .next()
            .ok_or_else(|| UdfError::ExecutionFailed("invoke returned no result".into()))?;

        val_to_cql_result(&result_val, return_type)
    }
}

/// Convert a list of WitCqlValue to a wasmtime component Val::List.
///
/// NOTE: This conversion is a bridge layer. The wasmtime component model's
/// `Val` enum uses a different structure than our `WitCqlValue`. Full
/// integration requires wit-bindgen generated types to properly construct
/// the variant/list/tuple Vals that match the WIT contract. For now, this
/// function provides the structural scaffolding. Real WASM components
/// compiled against the ferrosa-udf.wit contract will work once wit-bindgen
/// types are integrated.
fn wit_cql_list_to_val(values: &[WitCqlValue]) -> Val {
    // TODO(wit-bindgen): When wit-bindgen generates host types for `cql-value`,
    // this conversion becomes a direct mapping. Currently we construct a Val
    // that represents list<cql-value> using the component model's dynamic API.
    //
    // The variant discriminant ordering must match ferrosa-udf.wit:
    //   0=null, 1=int-val, 2=bigint-val, ... etc.
    let items: Vec<Val> = values.iter().map(wit_cql_value_to_val).collect();
    Val::List(items)
}

/// Convert a single WitCqlValue to its corresponding component Val.
fn wit_cql_value_to_val(v: &WitCqlValue) -> Val {
    // Each WIT variant case maps to Val::Enum or Val::Variant.
    // With the dynamic component API, variant values are constructed via
    // Val::Variant with the discriminant name and optional payload.
    //
    // TODO(wit-bindgen): Replace with generated type conversions.
    // For now we use a simplified encoding that works for testing.
    match v {
        WitCqlValue::Null => Val::Option(None),
        WitCqlValue::IntVal(i) => Val::S32(*i),
        WitCqlValue::BigintVal(i) => Val::S64(*i),
        WitCqlValue::FloatVal(f) => Val::Float32(*f),
        WitCqlValue::DoubleVal(f) => Val::Float64(*f),
        WitCqlValue::BooleanVal(b) => Val::Bool(*b),
        WitCqlValue::TextVal(s) => Val::String(s.clone()),
        WitCqlValue::BlobVal(b) => Val::List(b.iter().map(|byte| Val::U8(*byte)).collect()),
        WitCqlValue::SmallintVal(i) => Val::S16(*i),
        WitCqlValue::TinyintVal(i) => Val::S8(*i),
        WitCqlValue::TimestampVal(i) => Val::S64(*i),
        WitCqlValue::DateVal(i) => Val::S32(*i),
        WitCqlValue::TimeVal(i) => Val::S64(*i),
        WitCqlValue::CounterVal(i) => Val::S64(*i),
        // String-serialized types
        WitCqlValue::UuidVal(s)
        | WitCqlValue::TimeuuidVal(s)
        | WitCqlValue::InetVal(s)
        | WitCqlValue::AsciiVal(s) => Val::String(s.clone()),
        // Complex types use simplified encoding
        WitCqlValue::DecimalVal(bytes, scale) => Val::Tuple(vec![
            Val::List(bytes.iter().map(|b| Val::U8(*b)).collect()),
            Val::S32(*scale),
        ]),
        WitCqlValue::VarintVal(bytes) => Val::List(bytes.iter().map(|b| Val::U8(*b)).collect()),
        WitCqlValue::DurationVal(m, d, n) => {
            Val::Tuple(vec![Val::S32(*m), Val::S32(*d), Val::S64(*n)])
        }
        WitCqlValue::ListVal(items) | WitCqlValue::SetVal(items) => {
            Val::List(items.iter().map(wit_cql_value_to_val).collect())
        }
        WitCqlValue::MapVal(entries) => Val::List(
            entries
                .iter()
                .map(|(k, v)| Val::Tuple(vec![wit_cql_value_to_val(k), wit_cql_value_to_val(v)]))
                .collect(),
        ),
        WitCqlValue::TupleVal(items) => {
            Val::Tuple(items.iter().map(wit_cql_value_to_val).collect())
        }
        WitCqlValue::UdtVal(fields) => Val::List(
            fields
                .iter()
                .map(|(name, v)| {
                    Val::Tuple(vec![Val::String(name.clone()), wit_cql_value_to_val(v)])
                })
                .collect(),
        ),
    }
}

/// Convert a result Val from the `invoke` export back to CqlValue.
///
/// The WIT contract specifies: `result<cql-value, string>`
fn val_to_cql_result(val: &Val, return_type: &CqlType) -> Result<CqlValue, UdfError> {
    // TODO(wit-bindgen): With generated types, this becomes a direct match
    // on the Result<CqlValue, String> type. With the dynamic API, the result
    // is encoded as Val::Result.
    match val {
        Val::Result(result) => match result.as_ref() {
            Ok(Some(inner)) => val_to_cql_value(inner, return_type),
            Ok(None) => Ok(CqlValue::Null),
            Err(Some(err_val)) => {
                let msg = match &**err_val {
                    Val::String(s) => s.clone(),
                    other => format!("UDF error: {other:?}"),
                };
                Err(UdfError::ExecutionFailed(msg))
            }
            Err(None) => Err(UdfError::ExecutionFailed("UDF returned error".into())),
        },
        // If the function returns a non-result type, try direct conversion
        other => val_to_cql_value(other, return_type),
    }
}

/// Convert a component Val to CqlValue using the target type hint.
fn val_to_cql_value(val: &Val, return_type: &CqlType) -> Result<CqlValue, UdfError> {
    match (val, return_type) {
        (Val::Bool(b), _) => Ok(CqlValue::Boolean(*b)),
        (Val::S8(v), _) => Ok(CqlValue::Tinyint(*v)),
        (Val::S16(v), _) => Ok(CqlValue::Smallint(*v)),
        (Val::S32(v), CqlType::Int) => Ok(CqlValue::Int(*v)),
        (Val::S32(v), CqlType::Date) => Ok(CqlValue::Date(*v as u32)),
        (Val::S32(v), _) => Ok(CqlValue::Int(*v)),
        (Val::S64(v), CqlType::Bigint) => Ok(CqlValue::Bigint(*v)),
        (Val::S64(v), CqlType::Timestamp) => Ok(CqlValue::Timestamp(*v)),
        (Val::S64(v), CqlType::Time) => Ok(CqlValue::Time(*v)),
        (Val::S64(v), CqlType::Counter) => Ok(CqlValue::Counter(*v)),
        (Val::S64(v), _) => Ok(CqlValue::Bigint(*v)),
        (Val::Float32(v), _) => Ok(CqlValue::Float(v.to_bits())),
        (Val::Float64(v), _) => Ok(CqlValue::Double(v.to_bits())),
        (Val::String(s), _) => Ok(CqlValue::Text(s.clone())),
        (Val::Option(None), _) => Ok(CqlValue::Null),
        _ => {
            // For complex component model types, wit-bindgen integration is needed
            // to properly decode variant/list/tuple structures back to CqlValue.
            Err(UdfError::TypeMismatch(format!(
                "cannot convert component Val to {return_type:?} (wit-bindgen integration needed)"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_rejects_oversized_binary() {
        let config = SandboxConfig {
            max_wasm_size: 100,
            ..Default::default()
        };
        let executor = UdfExecutor::new(config).unwrap();
        let err = executor.compile("ks", "func", &[0u8; 200]).unwrap_err();
        assert!(matches!(err, UdfError::BinaryTooLarge { .. }));
    }

    #[test]
    fn compile_rejects_invalid_wasm() {
        let executor = UdfExecutor::new(SandboxConfig::default()).unwrap();
        let err = executor
            .compile("ks", "bad", b"not valid wasm")
            .unwrap_err();
        assert!(
            matches!(err, UdfError::CompilationFailed(..)),
            "expected CompilationFailed, got: {err:?}"
        );
    }

    #[test]
    fn call_unknown_function_returns_not_found() {
        let executor = UdfExecutor::new(SandboxConfig::default()).unwrap();
        let err = executor
            .call("ks", "missing", vec![], &[], &CqlType::Int)
            .unwrap_err();
        assert!(matches!(err, UdfError::NotFound { .. }));
    }

    #[test]
    fn invalidate_removes_cached_function() {
        let executor = UdfExecutor::new(SandboxConfig::default()).unwrap();
        // We can't compile arbitrary bytes anymore since real validation is active.
        // Verify invalidation works on the cache key level.
        let key = ("ks".to_string(), "func".to_string());
        executor.cache.insert(
            key.clone(),
            Arc::new(CompiledFunction {
                component: Component::new(
                    &executor.engine,
                    // Minimal valid WASM component (empty)
                    minimal_component_bytes(),
                )
                .unwrap(),
            }),
        );
        assert!(executor.cache.contains_key(&key));
        executor.invalidate("ks", "func");
        assert!(!executor.cache.contains_key(&key));
    }

    #[test]
    fn engine_has_fuel_and_epoch_enabled() {
        // Verify the engine was configured correctly by creating a store
        // and checking that fuel operations succeed.
        let executor = UdfExecutor::new(SandboxConfig::default()).unwrap();
        let mut store = Store::new(&executor.engine, ());
        // set_fuel should succeed when fuel consumption is enabled
        store.set_fuel(1000).expect("fuel should be enabled");
        // epoch deadline should be settable when epoch interruption is enabled
        store.set_epoch_deadline(1);
    }

    /// Generate a minimal valid WASM component binary.
    /// This is the smallest valid component that wasmtime will accept.
    fn minimal_component_bytes() -> Vec<u8> {
        // Component header: magic + version + layer
        // \0asm = magic, 0d 00 = version 13, 01 00 = layer (component)
        vec![
            0x00, 0x61, 0x73, 0x6d, // \0asm
            0x0d, 0x00, // version 13
            0x01, 0x00, // layer = component
        ]
    }
}
