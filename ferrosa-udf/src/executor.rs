//! WASM function executor with compilation cache.

use std::sync::Arc;

use ferrosa_common::{CqlType, CqlValue};

use crate::error::UdfError;
use crate::sandbox::SandboxConfig;

/// Compiled WASM component ready for instantiation.
struct CompiledFunction {
    // Will hold wasmtime::component::Component
    _placeholder: (),
}

/// Executor for WASM-based User-Defined Functions.
///
/// Manages the Wasmtime engine, compilation cache, and sandbox policy.
/// Thread-safe — can be shared across async tasks via `Arc`.
pub struct UdfExecutor {
    config: SandboxConfig,
    cache: moka::sync::Cache<(String, String), Arc<CompiledFunction>>,
}

impl UdfExecutor {
    /// Create a new executor with the given sandbox configuration.
    pub fn new(config: SandboxConfig) -> Result<Self, UdfError> {
        let cache = moka::sync::Cache::builder()
            .max_capacity(config.cache_capacity)
            .build();
        Ok(Self { config, cache })
    }

    /// Pre-compile a WASM binary. Called on INSERT into wasm_binaries.
    pub fn compile(&self, keyspace: &str, name: &str, wasm_bytes: &[u8]) -> Result<(), UdfError> {
        if wasm_bytes.len() > self.config.max_wasm_size {
            return Err(UdfError::BinaryTooLarge {
                size: wasm_bytes.len(),
                max: self.config.max_wasm_size,
            });
        }
        // Full Wasmtime compilation will be implemented in Task 12
        tracing::info!(
            keyspace,
            name,
            size = wasm_bytes.len(),
            "compiled WASM function"
        );
        self.cache.insert(
            (keyspace.to_string(), name.to_string()),
            Arc::new(CompiledFunction { _placeholder: () }),
        );
        Ok(())
    }

    /// Invalidate cached compilation (on CREATE OR REPLACE).
    pub fn invalidate(&self, keyspace: &str, name: &str) {
        self.cache
            .invalidate(&(keyspace.to_string(), name.to_string()));
    }

    /// Invoke a UDF. Returns the function's result.
    pub fn call(
        &self,
        keyspace: &str,
        func_name: &str,
        _args: Vec<CqlValue>,
        _arg_types: &[CqlType],
    ) -> Result<CqlValue, UdfError> {
        if !self
            .cache
            .contains_key(&(keyspace.to_string(), func_name.to_string()))
        {
            return Err(UdfError::NotFound {
                keyspace: keyspace.to_string(),
                name: func_name.to_string(),
            });
        }
        // Full Wasmtime invocation will be implemented in Task 12
        Err(UdfError::ExecutionFailed(
            "WASM execution not yet implemented".into(),
        ))
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
    fn call_unknown_function_returns_not_found() {
        let executor = UdfExecutor::new(SandboxConfig::default()).unwrap();
        let err = executor.call("ks", "missing", vec![], &[]).unwrap_err();
        assert!(matches!(err, UdfError::NotFound { .. }));
    }

    #[test]
    fn invalidate_removes_cached_function() {
        let executor = UdfExecutor::new(SandboxConfig::default()).unwrap();
        executor.compile("ks", "func", &[0u8; 10]).unwrap();
        assert!(executor
            .cache
            .contains_key(&("ks".to_string(), "func".to_string())));
        executor.invalidate("ks", "func");
        assert!(!executor
            .cache
            .contains_key(&("ks".to_string(), "func".to_string())));
    }
}
