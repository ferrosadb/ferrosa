//! Sandbox configuration for WASM UDF execution.

use std::time::Duration;

/// Resource limits for WASM function invocations.
///
/// Each invocation gets its own `Store` with these limits.
/// Fuel-based CPU metering and epoch interruption provide
/// deterministic resource control.
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    /// Maximum WASM linear memory per invocation (default: 16 MB).
    pub max_memory_bytes: usize,

    /// Fuel units per invocation (default: 1,000,000 ≈ 1M instructions).
    /// Wasmtime traps with `OutOfFuel` when exhausted.
    pub max_fuel: u64,

    /// Hard wall-clock timeout (default: 5 s).
    /// Uses Wasmtime epoch interruption.
    pub max_execution_time: Duration,

    /// Maximum compiled module cache size (default: 256 entries).
    pub cache_capacity: u64,

    /// Maximum WASM binary upload size (default: 10 MB).
    pub max_wasm_size: usize,

    /// Per-aggregate total fuel cap (default: 10,000,000).
    pub max_aggregate_fuel: u64,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 16 * 1024 * 1024,
            max_fuel: 1_000_000,
            max_execution_time: Duration::from_secs(5),
            cache_capacity: 256,
            max_wasm_size: 10 * 1024 * 1024,
            max_aggregate_fuel: 10_000_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = SandboxConfig::default();
        assert_eq!(config.max_memory_bytes, 16 * 1024 * 1024);
        assert_eq!(config.max_fuel, 1_000_000);
        assert_eq!(config.max_execution_time, Duration::from_secs(5));
        assert_eq!(config.cache_capacity, 256);
        assert_eq!(config.max_wasm_size, 10 * 1024 * 1024);
        assert_eq!(config.max_aggregate_fuel, 10_000_000);
    }
}
