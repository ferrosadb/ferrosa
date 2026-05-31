//! Configuration for time-series consolidation, parsed from table extensions.

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::consolidation::ConsolidationFn;

/// Environment override for the default RRD ring memory budget.
pub const RING_MEMORY_BUDGET_ENV: &str = "FERROSA_RRD_RING_MEMORY_BUDGET_BYTES";

const DEFAULT_RING_MEMORY_BUDGET_FRACTION_DENOMINATOR: usize = 20; // 5%
const FALLBACK_RING_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const CGROUP_UNLIMITED_SENTINEL: usize = 1usize << 60;
const NO_RING_MEMORY_BUDGET: u64 = u64::MAX;

/// Configuration for time-series consolidation on a table.
#[derive(Debug, Clone)]
pub struct ConsolidationConfig {
    /// Consolidation interval.
    pub interval: Duration,
    /// Functions to apply to each window.
    pub functions: Vec<ConsolidationFn>,
    /// Target table for consolidated output.
    pub target_table: String,
    /// Whether to auto-create cascade chain.
    pub cascade: bool,
    /// How far back to accept late re-aggregation.
    pub late_window: Duration,
    /// Which columns to aggregate.
    pub columns: Vec<String>,
    /// Ring buffer capacity (default 512).
    pub ring_capacity: usize,
    /// Maximum number of ring buffers (default 10,000).
    pub max_rings: usize,
    /// Optional heap budget for all active ring buffers.
    pub ring_memory_budget_bytes: Option<usize>,
    /// Emit a thrash warning every N ring evictions. Zero disables warnings.
    pub ring_thrash_warn_evictions: u64,
    /// Cascade multipliers for auto-creating downstream tables.
    pub cascade_multipliers: Vec<u32>,
    /// Bounded channel capacity for consolidation tasks (default 1024).
    pub channel_capacity: usize,
}

/// Runtime-adjustable time-series consolidation controls.
///
/// These values are shared by active aggregators and can be changed without
/// rebuilding table metadata. Table extensions and env/defaults initialize this
/// state; writable control-plane virtual tables can update it later.
#[derive(Debug)]
pub struct TimeSeriesRuntimeSettings {
    ring_memory_budget_bytes: AtomicU64,
    ring_thrash_warn_evictions: AtomicU64,
}

impl TimeSeriesRuntimeSettings {
    pub fn from_config(config: &ConsolidationConfig) -> Self {
        Self::new(
            config.ring_memory_budget_bytes,
            config.ring_thrash_warn_evictions,
        )
    }

    pub fn new(ring_memory_budget_bytes: Option<usize>, ring_thrash_warn_evictions: u64) -> Self {
        Self {
            ring_memory_budget_bytes: AtomicU64::new(
                ring_memory_budget_bytes
                    .map(|value| value as u64)
                    .unwrap_or(NO_RING_MEMORY_BUDGET),
            ),
            ring_thrash_warn_evictions: AtomicU64::new(ring_thrash_warn_evictions),
        }
    }

    pub fn ring_memory_budget_bytes(&self) -> Option<usize> {
        let value = self.ring_memory_budget_bytes.load(Ordering::Relaxed);
        if value == NO_RING_MEMORY_BUDGET {
            None
        } else {
            Some(value as usize)
        }
    }

    pub fn set_ring_memory_budget_bytes(&self, value: Option<usize>) {
        self.ring_memory_budget_bytes.store(
            value
                .map(|value| value as u64)
                .unwrap_or(NO_RING_MEMORY_BUDGET),
            Ordering::Relaxed,
        );
    }

    pub fn ring_thrash_warn_evictions(&self) -> u64 {
        self.ring_thrash_warn_evictions.load(Ordering::Relaxed)
    }

    pub fn set_ring_thrash_warn_evictions(&self, value: u64) {
        self.ring_thrash_warn_evictions
            .store(value, Ordering::Relaxed);
    }
}

impl Default for ConsolidationConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(300), // 5 minutes
            functions: vec![],
            target_table: String::new(),
            cascade: false,
            late_window: Duration::from_secs(600), // 2x default interval
            columns: vec![],
            ring_capacity: 512,
            max_rings: 10_000,
            ring_memory_budget_bytes: default_ring_memory_budget_bytes(),
            ring_thrash_warn_evictions: 100,
            cascade_multipliers: vec![3, 4],
            channel_capacity: 1024,
        }
    }
}

impl ConsolidationConfig {
    /// Parse from table extensions map. Returns `None` if no `consolidation.*` keys.
    /// Returns `Some(Err(...))` if keys are present but invalid.
    pub fn from_extensions(ext: &HashMap<String, String>) -> Option<Result<Self, String>> {
        // Check if any consolidation key exists.
        let has_consolidation = ext.keys().any(|k| k.starts_with("consolidation."));
        if !has_consolidation {
            return None;
        }

        Some(Self::parse_extensions(ext))
    }

    fn parse_extensions(ext: &HashMap<String, String>) -> Result<Self, String> {
        let mut config = Self::default();

        // Required: interval (must be > 0).
        if let Some(interval_str) = ext.get("consolidation.interval") {
            config.interval = parse_duration(interval_str)?;
            if config.interval == Duration::ZERO {
                return Err("consolidation.interval must be positive (non-zero)".to_string());
            }
        } else {
            return Err("consolidation.interval is required".to_string());
        }

        // Required: functions.
        if let Some(funcs_str) = ext.get("consolidation.functions") {
            config.functions = ConsolidationFn::parse_list(funcs_str)?;
            validate_live_materialization_functions(&config.functions)?;
        } else {
            return Err("consolidation.functions is required".to_string());
        }

        // Required: target.
        if let Some(target) = ext.get("consolidation.target") {
            config.target_table = target.clone();
        } else {
            return Err("consolidation.target is required".to_string());
        }

        // Optional: cascade.
        if let Some(cascade_str) = ext.get("consolidation.cascade") {
            config.cascade = cascade_str.to_lowercase() == "true";
        }

        // Optional: late_window.
        if let Some(late_str) = ext.get("consolidation.late_window") {
            config.late_window = parse_duration(late_str)?;
        } else {
            // Default: 2x interval.
            config.late_window = config.interval * 2;
        }

        // Optional: columns.
        if let Some(cols_str) = ext.get("consolidation.columns") {
            config.columns = cols_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        // Optional: ring_capacity (must be > 0).
        if let Some(cap_str) = ext.get("consolidation.ring_capacity") {
            config.ring_capacity = cap_str
                .parse()
                .map_err(|_| format!("invalid ring_capacity: '{cap_str}'"))?;
            if config.ring_capacity == 0 {
                return Err("ring_capacity must be greater than zero".to_string());
            }
        }

        // Optional: max_rings.
        if let Some(max_str) = ext.get("consolidation.max_rings") {
            config.max_rings = max_str
                .parse()
                .map_err(|_| format!("invalid max_rings: '{max_str}'"))?;
            if config.max_rings == 0 {
                return Err("max_rings must be greater than zero".to_string());
            }
        }

        // Optional: ring_memory_budget_bytes. Some(0) means the configured
        // budget cannot fit a ring and writes will skip ring allocation.
        if let Some(budget_str) = ext.get("consolidation.ring_memory_budget_bytes") {
            config.ring_memory_budget_bytes = Some(
                budget_str
                    .parse()
                    .map_err(|_| format!("invalid ring_memory_budget_bytes: '{budget_str}'"))?,
            );
        }

        // Optional: ring_thrash_warn_evictions.
        if let Some(threshold_str) = ext.get("consolidation.ring_thrash_warn_evictions") {
            config.ring_thrash_warn_evictions = threshold_str
                .parse()
                .map_err(|_| format!("invalid ring_thrash_warn_evictions: '{threshold_str}'"))?;
        }

        // Optional: channel_capacity (default 1024).
        if let Some(cap_str) = ext.get("consolidation.channel_capacity") {
            config.channel_capacity = cap_str
                .parse()
                .map_err(|_| format!("invalid channel_capacity: '{cap_str}'"))?;
        }

        // Optional: cascade_multipliers.
        if let Some(mult_str) = ext.get("consolidation.cascade_multipliers") {
            config.cascade_multipliers = mult_str
                .split(',')
                .map(|s| {
                    s.trim()
                        .parse::<u32>()
                        .map_err(|_| format!("invalid cascade multiplier: '{}'", s.trim()))
                })
                .collect::<Result<Vec<_>, _>>()?;
        }

        if config.columns.is_empty() {
            return Err("consolidation.columns is required".to_string());
        }

        Ok(config)
    }

    /// Returns the interval in microseconds.
    pub fn interval_micros(&self) -> i64 {
        self.interval.as_micros() as i64
    }
}

fn validate_live_materialization_functions(functions: &[ConsolidationFn]) -> Result<(), String> {
    for function in functions {
        match function {
            ConsolidationFn::Median => {
                return Err(
                    "consolidation function 'median' requires materialized windows and is not supported for live RRD materialization yet".to_string()
                );
            }
            ConsolidationFn::Composite(inner) => validate_live_materialization_functions(inner)?,
            _ => {}
        }
    }
    Ok(())
}

/// Derive the default active-ring heap budget.
///
/// The explicit environment override wins. Without an override, Ferrosa uses a
/// conservative fraction of the cgroup/process memory limit. If no platform
/// memory signal is available, it falls back to a bounded 64 MiB budget instead
/// of leaving ring allocation unbounded.
pub fn default_ring_memory_budget_bytes() -> Option<usize> {
    derive_ring_memory_budget_bytes(
        std::env::var(RING_MEMORY_BUDGET_ENV).ok().as_deref(),
        detect_process_memory_limit_bytes(),
    )
}

fn derive_ring_memory_budget_bytes(
    env_override: Option<&str>,
    memory_limit_bytes: Option<usize>,
) -> Option<usize> {
    if let Some(raw) = env_override {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            tracing::warn!(
                env = RING_MEMORY_BUDGET_ENV,
                "empty RRD ring memory budget override ignored"
            );
        } else if let Ok(value) = trimmed.parse::<usize>() {
            return Some(value);
        } else {
            tracing::warn!(
                env = RING_MEMORY_BUDGET_ENV,
                value = trimmed,
                "invalid RRD ring memory budget override ignored"
            );
        }
    }

    memory_limit_bytes
        .map(|limit| limit / DEFAULT_RING_MEMORY_BUDGET_FRACTION_DENOMINATOR)
        .filter(|budget| *budget > 0)
        .or(Some(FALLBACK_RING_MEMORY_BUDGET_BYTES))
}

fn detect_process_memory_limit_bytes() -> Option<usize> {
    read_cgroup_v2_memory_max()
        .or_else(read_cgroup_v1_memory_limit)
        .or_else(read_proc_mem_total)
}

fn read_cgroup_v2_memory_max() -> Option<usize> {
    let raw = fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    parse_cgroup_memory_limit(raw.trim())
}

fn read_cgroup_v1_memory_limit() -> Option<usize> {
    let raw = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes").ok()?;
    parse_cgroup_memory_limit(raw.trim())
}

fn parse_cgroup_memory_limit(raw: &str) -> Option<usize> {
    if raw == "max" {
        return None;
    }
    let value = raw.parse::<usize>().ok()?;
    if value == 0 || value >= CGROUP_UNLIMITED_SENTINEL {
        None
    } else {
        Some(value)
    }
}

fn read_proc_mem_total() -> Option<usize> {
    let raw = fs::read_to_string("/proc/meminfo").ok()?;
    parse_proc_mem_total(&raw)
}

fn parse_proc_mem_total(raw: &str) -> Option<usize> {
    let line = raw.lines().find(|line| line.starts_with("MemTotal:"))?;
    let mut parts = line.split_whitespace();
    let _label = parts.next()?;
    let kib = parts.next()?.parse::<usize>().ok()?;
    kib.checked_mul(1024)
}

/// Parse a duration string like "5m", "15m", "1h", "30s", "100ms".
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration string".to_string());
    }

    // Try common suffixes.
    if let Some(num) = s.strip_suffix("ms") {
        let n: u64 = num
            .trim()
            .parse()
            .map_err(|_| format!("invalid duration: '{s}'"))?;
        return Ok(Duration::from_millis(n));
    }
    if let Some(num) = s.strip_suffix('d') {
        let n: u64 = num
            .trim()
            .parse()
            .map_err(|_| format!("invalid duration: '{s}'"))?;
        return Ok(Duration::from_secs(n * 86_400));
    }
    if let Some(num) = s.strip_suffix('h') {
        let n: u64 = num
            .trim()
            .parse()
            .map_err(|_| format!("invalid duration: '{s}'"))?;
        return Ok(Duration::from_secs(n * 3600));
    }
    if let Some(num) = s.strip_suffix('m') {
        let n: u64 = num
            .trim()
            .parse()
            .map_err(|_| format!("invalid duration: '{s}'"))?;
        return Ok(Duration::from_secs(n * 60));
    }
    if let Some(num) = s.strip_suffix('s') {
        let n: u64 = num
            .trim()
            .parse()
            .map_err(|_| format!("invalid duration: '{s}'"))?;
        return Ok(Duration::from_secs(n));
    }

    // Try bare number as seconds.
    let n: u64 = s.parse().map_err(|_| format!("invalid duration: '{s}'"))?;
    Ok(Duration::from_secs(n))
}

/// Check if the number of consolidation columns exceeds the SmallVec inline limit.
/// Returns a warning message if > 8 columns.
pub fn check_column_count_warning(column_count: usize) -> Option<String> {
    if column_count > 8 {
        Some(format!(
            "consolidation covers {} columns (> 8). SmallVec will spill to heap, \
             which is functional but slower. Consider reducing the number of \
             consolidated columns for optimal performance.",
            column_count
        ))
    } else {
        None
    }
}

/// Validate that all consolidation columns are numeric types.
///
/// Accepts: double, float, int, bigint, counter.
/// Rejects: text, blob, uuid, timestamp, boolean, etc.
pub fn validate_numeric_columns(
    column_names: &[String],
    column_types: &HashMap<String, String>,
) -> Result<(), String> {
    let numeric_types = ["double", "float", "int", "bigint", "counter"];
    for col in column_names {
        match column_types.get(col) {
            Some(ct) => {
                if !numeric_types.contains(&ct.as_str()) {
                    return Err(format!(
                        "column '{}' has type '{}', but consolidation requires a numeric type \
                         (double, float, int, bigint, counter)",
                        col, ct
                    ));
                }
            }
            None => {
                return Err(format!("column '{}' not found in table", col));
            }
        }
    }
    Ok(())
}

/// Detect cycles in the consolidation cascade chain.
///
/// Walks from `source_table` through each table's `consolidation.target` extension,
/// checking for cycles. Returns `Err` if a cycle is detected.
pub fn detect_cascade_cycle(
    source_table: &str,
    all_extensions: &HashMap<String, HashMap<String, String>>,
) -> Result<(), String> {
    let mut visited = std::collections::HashSet::new();
    visited.insert(source_table.to_string());

    let mut current = source_table.to_string();
    while let Some(ext) = all_extensions.get(&current) {
        let target = match ext.get("consolidation.target") {
            Some(t) => t.clone(),
            None => break, // no consolidation config, chain terminates
        };

        if !visited.insert(target.clone()) {
            return Err(format!(
                "cascade cycle detected: '{}' -> '{}' creates a loop",
                current, target
            ));
        }
        current = target;
    }

    Ok(())
}

/// Describes a downstream table to be auto-created in a cascade chain.
#[derive(Debug, Clone, PartialEq)]
pub struct CascadeTableSpec {
    /// Name of the downstream table.
    pub table_name: String,
    /// Consolidation interval for this tier.
    pub interval: Duration,
    /// Target table for this tier's consolidation output (next tier or empty for terminal).
    pub target: Option<String>,
    /// Output column names: `{original}_{function}`.
    pub output_columns: Vec<String>,
}

/// Generate the cascade chain specification from a source table's config.
///
/// Returns a list of downstream tables to create (not including the source).
/// The last table in the chain has no target (terminal).
pub fn generate_cascade_chain(
    source_table: &str,
    config: &ConsolidationConfig,
) -> Vec<CascadeTableSpec> {
    if !config.cascade {
        return vec![];
    }

    let mut chain = Vec::new();
    let mut current_interval = config.interval;
    let mut current_target_name = config.target_table.clone();

    let output_columns: Vec<String> = config
        .columns
        .iter()
        .flat_map(|col| {
            config
                .functions
                .iter()
                .filter_map(|f| {
                    let suffix = f.output_suffix();
                    suffix.map(|s| format!("{}_{}", col, s))
                })
                .collect::<Vec<_>>()
        })
        .collect();

    for (i, &multiplier) in config.cascade_multipliers.iter().enumerate() {
        let next_interval =
            Duration::from_micros(current_interval.as_micros() as u64 * multiplier as u64);

        // Derive the next table name from the source pattern and the computed interval.
        let next_table_name = derive_table_name(source_table, &next_interval);

        let is_last_multiplier = i + 1 >= config.cascade_multipliers.len();

        // Current tier outputs to the next tier.
        let next_target = if is_last_multiplier {
            // Terminal: no further consolidation from the next table, but we still
            // point this tier's output to the next table.
            None
        } else {
            Some(next_table_name.clone())
        };

        chain.push(CascadeTableSpec {
            table_name: current_target_name.clone(),
            interval: current_interval,
            target: next_target,
            output_columns: output_columns.clone(),
        });

        current_interval = next_interval;
        current_target_name = next_table_name;
    }

    // Add the final terminal table (receives data but does not consolidate further).
    chain.push(CascadeTableSpec {
        table_name: current_target_name,
        interval: current_interval,
        target: None,
        output_columns,
    });

    chain
}

/// Derive a table name from the source table and interval.
/// e.g., "sensor_1s" with 15m interval -> "sensor_15m".
fn derive_table_name(source: &str, interval: &Duration) -> String {
    let suffix = format_duration_short(interval);
    // Try to replace the last _Xs suffix, else append.
    if let Some(base) = source.rsplit_once('_') {
        format!("{}_{}", base.0, suffix)
    } else {
        format!("{}_{}", source, suffix)
    }
}

/// Format a duration as a short string: "5m", "15m", "1h".
fn format_duration_short(d: &Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 && secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs >= 60 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{}s", secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_variants() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("100ms").unwrap(), Duration::from_millis(100));
        assert_eq!(parse_duration("60").unwrap(), Duration::from_secs(60));
        // Day units: a weekly rollup is '7d', a daily one '1d'.
        assert_eq!(parse_duration("1d").unwrap(), Duration::from_secs(86_400));
        assert_eq!(
            parse_duration("7d").unwrap(),
            Duration::from_secs(7 * 86_400)
        );
    }

    #[test]
    fn parse_duration_errors() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("5x").is_err());
    }

    #[test]
    fn config_from_extensions_none_when_no_keys() {
        let ext = HashMap::new();
        assert!(ConsolidationConfig::from_extensions(&ext).is_none());
    }

    #[test]
    fn config_from_extensions_parses_full() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "5m".into());
        ext.insert("consolidation.functions".into(), "min,max,avg".into());
        ext.insert("consolidation.target".into(), "sensor_5m".into());
        ext.insert("consolidation.cascade".into(), "true".into());
        ext.insert("consolidation.late_window".into(), "10m".into());
        ext.insert("consolidation.columns".into(), "value".into());

        let config = ConsolidationConfig::from_extensions(&ext).unwrap().unwrap();
        assert_eq!(config.interval, Duration::from_secs(300));
        assert_eq!(config.functions.len(), 3);
        assert_eq!(config.target_table, "sensor_5m");
        assert!(config.cascade);
        assert_eq!(config.late_window, Duration::from_secs(600));
        assert_eq!(config.columns, vec!["value"]);
    }

    #[test]
    fn config_from_extensions_error_missing_required() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "5m".into());
        // Missing functions and target.
        let result = ConsolidationConfig::from_extensions(&ext).unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn config_rejects_non_streaming_materialization_functions() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "5m".into());
        ext.insert("consolidation.functions".into(), "median".into());
        ext.insert("consolidation.target".into(), "sensor_5m".into());
        ext.insert("consolidation.columns".into(), "value".into());

        let err = ConsolidationConfig::from_extensions(&ext)
            .unwrap()
            .unwrap_err();
        assert!(
            err.contains("requires materialized windows"),
            "expected materialized-window error for median: {err}"
        );
    }

    #[test]
    fn config_accepts_wasm_materialization_function_for_executor_validation() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "5m".into());
        ext.insert("consolidation.functions".into(), "wasm:plant.stddev".into());
        ext.insert("consolidation.target".into(), "sensor_5m".into());
        ext.insert("consolidation.columns".into(), "value".into());

        let config = ConsolidationConfig::from_extensions(&ext).unwrap().unwrap();
        assert_eq!(
            config.functions,
            vec![super::super::consolidation::ConsolidationFn::Wasm {
                keyspace: "plant".to_string(),
                function_name: "stddev".to_string(),
            }]
        );
    }

    // --- Task 16: Cascade and ring param parsing tests ---

    #[test]
    fn config_cascade_multipliers() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "5m".into());
        ext.insert("consolidation.functions".into(), "avg".into());
        ext.insert("consolidation.target".into(), "t".into());
        ext.insert("consolidation.columns".into(), "v".into());
        ext.insert("consolidation.cascade_multipliers".into(), "3,4,6".into());

        let config = ConsolidationConfig::from_extensions(&ext).unwrap().unwrap();
        assert_eq!(config.cascade_multipliers, vec![3, 4, 6]);
    }

    #[test]
    fn config_custom_ring_params() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "1m".into());
        ext.insert("consolidation.functions".into(), "sum".into());
        ext.insert("consolidation.target".into(), "t".into());
        ext.insert("consolidation.columns".into(), "v".into());
        ext.insert("consolidation.ring_capacity".into(), "1024".into());
        ext.insert("consolidation.max_rings".into(), "5000".into());

        let config = ConsolidationConfig::from_extensions(&ext).unwrap().unwrap();
        assert_eq!(config.ring_capacity, 1024);
        assert_eq!(config.max_rings, 5000);
    }

    #[test]
    fn config_parses_ring_memory_budget_and_thrash_threshold() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "1m".into());
        ext.insert("consolidation.functions".into(), "sum".into());
        ext.insert("consolidation.target".into(), "t".into());
        ext.insert("consolidation.columns".into(), "v".into());
        ext.insert(
            "consolidation.ring_memory_budget_bytes".into(),
            "1048576".into(),
        );
        ext.insert(
            "consolidation.ring_thrash_warn_evictions".into(),
            "25".into(),
        );

        let config = ConsolidationConfig::from_extensions(&ext).unwrap().unwrap();

        assert_eq!(config.ring_memory_budget_bytes, Some(1_048_576));
        assert_eq!(config.ring_thrash_warn_evictions, 25);
    }

    #[test]
    fn ring_memory_budget_env_override_wins_over_detected_limit() {
        assert_eq!(
            derive_ring_memory_budget_bytes(Some("123456"), Some(2 * 1024 * 1024 * 1024)),
            Some(123_456)
        );
    }

    #[test]
    fn ring_memory_budget_derives_from_detected_memory_limit() {
        assert_eq!(
            derive_ring_memory_budget_bytes(None, Some(2 * 1024 * 1024 * 1024)),
            Some(107_374_182)
        );
    }

    #[test]
    fn ring_memory_budget_falls_back_when_no_memory_signal_exists() {
        assert_eq!(
            derive_ring_memory_budget_bytes(None, None),
            Some(64 * 1024 * 1024)
        );
    }

    #[test]
    fn ring_memory_budget_ignores_invalid_env_override() {
        assert_eq!(
            derive_ring_memory_budget_bytes(Some("not-a-number"), Some(1024 * 1024 * 1024)),
            Some(53_687_091)
        );
    }

    #[test]
    fn parse_cgroup_memory_limit_rejects_unbounded_values() {
        assert_eq!(parse_cgroup_memory_limit("max"), None);
        assert_eq!(parse_cgroup_memory_limit("0"), None);
        assert_eq!(parse_cgroup_memory_limit(&(1usize << 61).to_string()), None);
        assert_eq!(parse_cgroup_memory_limit("2147483648"), Some(2_147_483_648));
    }

    #[test]
    fn parse_proc_mem_total_reads_kib() {
        assert_eq!(
            parse_proc_mem_total("MemTotal:       2048000 kB\nMemFree: 1 kB\n"),
            Some(2_097_152_000)
        );
    }

    #[test]
    fn table_extension_ring_memory_budget_overrides_derived_default() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "1m".into());
        ext.insert("consolidation.functions".into(), "sum".into());
        ext.insert("consolidation.target".into(), "t".into());
        ext.insert("consolidation.columns".into(), "v".into());
        ext.insert(
            "consolidation.ring_memory_budget_bytes".into(),
            "4096".into(),
        );

        let config = ConsolidationConfig::from_extensions(&ext).unwrap().unwrap();

        assert_eq!(config.ring_memory_budget_bytes, Some(4096));
    }

    #[test]
    fn config_interval_micros() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "5m".into());
        ext.insert("consolidation.functions".into(), "avg".into());
        ext.insert("consolidation.target".into(), "t".into());
        ext.insert("consolidation.columns".into(), "v".into());

        let config = ConsolidationConfig::from_extensions(&ext).unwrap().unwrap();
        assert_eq!(config.interval_micros(), 300_000_000);
    }

    // --- Task 18: SmallVec column count warning tests ---

    #[test]
    fn column_count_warning_none_for_8_or_fewer() {
        assert!(check_column_count_warning(1).is_none());
        assert!(check_column_count_warning(8).is_none());
    }

    #[test]
    fn column_count_warning_some_for_more_than_8() {
        let warning = check_column_count_warning(9).unwrap();
        assert!(warning.contains("9 columns"));
        assert!(warning.contains("heap"));
    }

    // --- Task 17: DDL validation tests ---

    #[test]
    fn validate_numeric_columns_accepts_numeric() {
        let mut types = HashMap::new();
        types.insert("temp".into(), "double".into());
        types.insert("count".into(), "bigint".into());
        types.insert("pressure".into(), "float".into());

        assert!(validate_numeric_columns(&["temp".into(), "count".into()], &types).is_ok());
    }

    #[test]
    fn validate_numeric_columns_rejects_text() {
        let mut types = HashMap::new();
        types.insert("name".into(), "text".into());

        let err = validate_numeric_columns(&["name".into()], &types).unwrap_err();
        assert!(err.contains("text"));
        assert!(err.contains("numeric"));
    }

    #[test]
    fn validate_numeric_columns_rejects_missing() {
        let types = HashMap::new();
        let err = validate_numeric_columns(&["missing".into()], &types).unwrap_err();
        assert!(err.contains("not found"));
    }

    #[test]
    fn detect_cycle_no_cycle() {
        let mut all_ext = HashMap::new();
        let mut ext_1s = HashMap::new();
        ext_1s.insert("consolidation.target".into(), "sensor_5m".into());
        all_ext.insert("sensor_1s".into(), ext_1s);

        let mut ext_5m = HashMap::new();
        ext_5m.insert("consolidation.target".into(), "sensor_15m".into());
        all_ext.insert("sensor_5m".into(), ext_5m);

        // sensor_15m has no consolidation (terminal).
        all_ext.insert("sensor_15m".into(), HashMap::new());

        assert!(detect_cascade_cycle("sensor_1s", &all_ext).is_ok());
    }

    #[test]
    fn detect_cycle_finds_cycle() {
        let mut all_ext = HashMap::new();
        let mut ext_a = HashMap::new();
        ext_a.insert("consolidation.target".into(), "b".into());
        all_ext.insert("a".into(), ext_a);

        let mut ext_b = HashMap::new();
        ext_b.insert("consolidation.target".into(), "a".into()); // cycle!
        all_ext.insert("b".into(), ext_b);

        let err = detect_cascade_cycle("a", &all_ext).unwrap_err();
        assert!(err.contains("cycle"));
    }

    #[test]
    fn detect_cycle_self_reference() {
        let mut all_ext = HashMap::new();
        let mut ext = HashMap::new();
        ext.insert("consolidation.target".into(), "self_table".into());
        all_ext.insert("self_table".into(), ext);

        let err = detect_cascade_cycle("self_table", &all_ext).unwrap_err();
        assert!(err.contains("cycle"));
    }

    // --- Task 19: Auto-cascade table creation tests ---

    #[test]
    fn generate_cascade_chain_3_tiers() {
        let config = ConsolidationConfig {
            interval: Duration::from_secs(300), // 5m
            functions: vec![
                super::super::consolidation::ConsolidationFn::Min,
                super::super::consolidation::ConsolidationFn::Max,
                super::super::consolidation::ConsolidationFn::Avg,
            ],
            target_table: "sensor_5m".into(),
            cascade: true,
            columns: vec!["value".into()],
            cascade_multipliers: vec![3, 4],
            ..ConsolidationConfig::default()
        };

        let chain = generate_cascade_chain("sensor_1s", &config);

        // Should produce 3 specs: sensor_5m -> sensor_15m -> sensor_1h (terminal).
        assert_eq!(chain.len(), 3);

        assert_eq!(chain[0].table_name, "sensor_5m");
        assert_eq!(chain[0].interval, Duration::from_secs(300));
        assert_eq!(chain[0].target, Some("sensor_15m".into()));

        assert_eq!(chain[1].table_name, "sensor_15m");
        assert_eq!(chain[1].interval, Duration::from_secs(900)); // 15m
        assert_eq!(chain[1].target, None);

        assert_eq!(chain[2].table_name, "sensor_1h");
        assert_eq!(chain[2].interval, Duration::from_secs(3600)); // 1h
        assert_eq!(chain[2].target, None);
    }

    #[test]
    fn generate_cascade_chain_names_wasm_output_columns_by_function() {
        let config = ConsolidationConfig {
            interval: Duration::from_secs(300),
            functions: vec![
                super::super::consolidation::ConsolidationFn::Min,
                super::super::consolidation::ConsolidationFn::Wasm {
                    keyspace: "plant".to_string(),
                    function_name: "stddev".to_string(),
                },
            ],
            target_table: "sensor_5m".into(),
            cascade: true,
            columns: vec!["vibration_mm_s".into(), "temperature_c".into()],
            cascade_multipliers: vec![3],
            ..ConsolidationConfig::default()
        };

        let chain = generate_cascade_chain("sensor_1s", &config);

        assert_eq!(
            chain[0].output_columns,
            vec![
                "vibration_mm_s_min",
                "vibration_mm_s_stddev",
                "temperature_c_min",
                "temperature_c_stddev",
            ]
        );
    }

    #[test]
    fn generate_cascade_chain_no_cascade() {
        let config = ConsolidationConfig {
            cascade: false,
            ..ConsolidationConfig::default()
        };
        let chain = generate_cascade_chain("t", &config);
        assert!(chain.is_empty());
    }

    #[test]
    fn derive_table_name_replaces_suffix() {
        assert_eq!(
            derive_table_name("sensor_1s", &Duration::from_secs(900)),
            "sensor_15m"
        );
        assert_eq!(
            derive_table_name("sensor_5m", &Duration::from_secs(3600)),
            "sensor_1h"
        );
    }

    #[test]
    fn format_duration_short_variants() {
        assert_eq!(format_duration_short(&Duration::from_secs(300)), "5m");
        assert_eq!(format_duration_short(&Duration::from_secs(900)), "15m");
        assert_eq!(format_duration_short(&Duration::from_secs(3600)), "1h");
        assert_eq!(format_duration_short(&Duration::from_secs(45)), "45s");
    }

    // --- FMEA Fix 1: Reject zero interval and zero capacity at parse time ---

    #[test]
    fn config_rejects_zero_interval() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "0s".into());
        ext.insert("consolidation.functions".into(), "avg".into());
        ext.insert("consolidation.target".into(), "t".into());
        ext.insert("consolidation.columns".into(), "v".into());

        let result = ConsolidationConfig::from_extensions(&ext).unwrap();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("interval"),
            "error should mention interval: {err}"
        );
        assert!(
            err.contains("zero") || err.contains("positive"),
            "error should mention zero/positive: {err}"
        );
    }

    #[test]
    fn config_rejects_zero_capacity() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "5m".into());
        ext.insert("consolidation.functions".into(), "avg".into());
        ext.insert("consolidation.target".into(), "t".into());
        ext.insert("consolidation.columns".into(), "v".into());
        ext.insert("consolidation.ring_capacity".into(), "0".into());

        let result = ConsolidationConfig::from_extensions(&ext).unwrap();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("ring_capacity"),
            "error should mention ring_capacity: {err}"
        );
    }

    // --- FMEA Fix 4: Default channel capacity ---

    #[test]
    fn config_default_channel_capacity() {
        let config = ConsolidationConfig::default();
        assert_eq!(
            config.channel_capacity, 1024,
            "default channel_capacity should be 1024"
        );
    }

    #[test]
    fn config_parses_custom_channel_capacity() {
        let mut ext = HashMap::new();
        ext.insert("consolidation.interval".into(), "5m".into());
        ext.insert("consolidation.functions".into(), "avg".into());
        ext.insert("consolidation.target".into(), "t".into());
        ext.insert("consolidation.columns".into(), "v".into());
        ext.insert("consolidation.channel_capacity".into(), "2048".into());

        let config = ConsolidationConfig::from_extensions(&ext).unwrap().unwrap();
        assert_eq!(config.channel_capacity, 2048);
    }
}
