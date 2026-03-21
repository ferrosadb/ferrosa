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

        // Required: interval.
        if let Some(interval_str) = ext.get("consolidation.interval") {
            config.interval = parse_duration(interval_str)?;
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

        // Optional: ring_capacity.
        if let Some(cap_str) = ext.get("consolidation.ring_capacity") {
            config.ring_capacity = cap_str
                .parse()
                .map_err(|_| format!("invalid ring_capacity: '{cap_str}'"))?;
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
}
