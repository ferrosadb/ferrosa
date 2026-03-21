//! Configuration for time-series consolidation, parsed from table extensions.

use std::collections::HashMap;
use std::time::Duration;

use super::consolidation::ConsolidationFn;

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
    /// Cascade multipliers for auto-creating downstream tables.
    pub cascade_multipliers: Vec<u32>,
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
            cascade_multipliers: vec![3, 4],
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
                    let suffix = match f {
                        ConsolidationFn::Min => Some("min"),
                        ConsolidationFn::Max => Some("max"),
                        ConsolidationFn::Avg => Some("avg"),
                        ConsolidationFn::Median => Some("median"),
                        ConsolidationFn::StdDev => Some("stddev"),
                        ConsolidationFn::Count => Some("count"),
                        ConsolidationFn::Sum => Some("sum"),
                        _ => None,
                    };
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
}
