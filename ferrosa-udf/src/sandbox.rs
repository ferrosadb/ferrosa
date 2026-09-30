//! Sandbox configuration for WASM UDF execution.

use std::time::Duration;

use wasmtime::{StoreLimits, StoreLimitsBuilder};

use crate::error::UdfError;

/// Resource limits for WASM function invocations.
///
/// Each invocation gets its own `Store` with these limits.
/// Fuel-based CPU metering and epoch interruption provide
/// deterministic resource control.
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    /// Maximum WASM linear memory per guest `Store` (default: 16 MiB).
    ///
    /// Enforced through wasmtime `StoreLimits` on every store the executor
    /// creates. Operator-facing: `[udf] max_memory_bytes` in the ferrosa TOML
    /// or `FERROSA_UDF_MAX_MEMORY_BYTES` (TOML wins). Validated by
    /// [`SandboxConfig::validate`]: must be in
    /// `MIN_MEMORY_BYTES..=MAX_MEMORY_BYTES_LIMIT`.
    pub max_memory_bytes: usize,

    /// Maximum table elements per table (default: 100,000).
    pub max_table_elements: usize,

    /// Maximum instances per store (default: 64).
    pub max_instances: usize,

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
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_table_elements: 100_000,
            max_instances: 64,
            max_fuel: 1_000_000,
            max_execution_time: Duration::from_secs(5),
            cache_capacity: 256,
            max_wasm_size: 10 * 1024 * 1024,
            max_aggregate_fuel: 10_000_000,
        }
    }
}

/// Default guest memory limit: 16 MiB.
pub const DEFAULT_MAX_MEMORY_BYTES: usize = 16 * 1024 * 1024;
/// Smallest accepted limit: one wasm page (64 KiB).
pub const MIN_MEMORY_BYTES: usize = 64 * 1024;
/// Largest accepted limit: 4 GiB, the wasm32 linear-memory ceiling.
pub const MAX_MEMORY_BYTES_LIMIT: usize = 4 * 1024 * 1024 * 1024;
/// Config key naming the memory limit, used in errors and logs.
pub const MEMORY_CONFIG_KEY: &str = "[udf] max_memory_bytes";

impl SandboxConfig {
    /// Validate the configuration; called by `UdfExecutor::new`.
    ///
    /// Fails loud on a zero, sub-page or absurd memory limit rather than
    /// silently running with an unusable sandbox.
    pub fn validate(&self) -> Result<(), UdfError> {
        if !(MIN_MEMORY_BYTES..=MAX_MEMORY_BYTES_LIMIT).contains(&self.max_memory_bytes) {
            return Err(UdfError::InvalidConfig(format!(
                "{MEMORY_CONFIG_KEY} = {} is out of range; must be between {MIN_MEMORY_BYTES} \
                 and {MAX_MEMORY_BYTES_LIMIT} bytes",
                self.max_memory_bytes
            )));
        }
        if self.max_table_elements == 0 || self.max_instances == 0 {
            return Err(UdfError::InvalidConfig(
                "max_table_elements and max_instances must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

/// Per-store state holding the resource limiter (`Store::limiter`).
pub struct GuestState {
    limits: StoreLimits,
    configured_memory_bytes: usize,
}

impl GuestState {
    pub fn new(config: &SandboxConfig) -> Self {
        let limits = StoreLimitsBuilder::new()
            .memory_size(config.max_memory_bytes)
            .table_elements(config.max_table_elements)
            .instances(config.max_instances)
            .build();
        Self {
            limits,
            configured_memory_bytes: config.max_memory_bytes,
        }
    }
}

/// Typed trap raised when a guest is denied memory; recovered by downcast.
#[derive(Debug)]
pub struct MemoryLimitTrap {
    pub configured_bytes: usize,
    pub requested_bytes: usize,
}

impl std::fmt::Display for MemoryLimitTrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "guest memory growth to {} bytes denied by limit {}",
            self.requested_bytes, self.configured_bytes
        )
    }
}

impl std::error::Error for MemoryLimitTrap {}

impl wasmtime::ResourceLimiter for GuestState {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if self.limits.memory_growing(current, desired, maximum)? {
            return Ok(true);
        }
        Err(wasmtime::Error::new(MemoryLimitTrap {
            configured_bytes: self.configured_memory_bytes,
            requested_bytes: desired,
        }))
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.limits.table_growing(current, desired, maximum)
    }

    fn instances(&self) -> usize {
        self.limits.instances()
    }

    fn tables(&self) -> usize {
        self.limits.tables()
    }

    fn memories(&self) -> usize {
        self.limits.memories()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_zero_memory_limit() {
        let config = SandboxConfig {
            max_memory_bytes: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            matches!(err, crate::error::UdfError::InvalidConfig(ref m) if m.contains("max_memory_bytes")),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_rejects_absurd_memory_limit() {
        let config = SandboxConfig {
            max_memory_bytes: usize::MAX,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_accepts_default() {
        SandboxConfig::default().validate().unwrap();
    }

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

    #[test]
    fn custom_config_values() {
        let config = SandboxConfig {
            max_memory_bytes: 4 * 1024 * 1024,
            max_table_elements: 1000,
            max_instances: 8,
            max_fuel: 500_000,
            max_execution_time: Duration::from_millis(250),
            cache_capacity: 64,
            max_wasm_size: 1024 * 1024,
            max_aggregate_fuel: 2_000_000,
        };
        assert_eq!(config.max_memory_bytes, 4 * 1024 * 1024);
        assert_eq!(config.max_fuel, 500_000);
        assert_eq!(config.max_execution_time, Duration::from_millis(250));
        assert_eq!(config.cache_capacity, 64);
        assert_eq!(config.max_wasm_size, 1024 * 1024);
        assert_eq!(config.max_aggregate_fuel, 2_000_000);
    }
}
