//! Consolidation functions for time-series aggregation.
//!
//! Stub implementation for Sprint R-1 dependency. Will be replaced when
//! Sprint R-1's work is merged.

/// Aggregation functions supported for time-series consolidation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationFn {
    /// Minimum value in the window.
    Min,
    /// Maximum value in the window.
    Max,
    /// Arithmetic mean of values in the window.
    Avg,
    /// Sum of values in the window.
    Sum,
    /// Number of data points in the window.
    Count,
    /// Last (most recent) value in the window.
    Last,
    /// First (earliest) value in the window.
    First,
}

impl ConsolidationFn {
    /// Parse a comma-separated list of function names.
    ///
    /// Valid names: `min`, `max`, `avg`, `sum`, `count`, `last`, `first`.
    pub fn parse_list(s: &str) -> Result<Vec<Self>, String> {
        s.split(',')
            .map(|name| {
                let name = name.trim().to_lowercase();
                match name.as_str() {
                    "min" => Ok(ConsolidationFn::Min),
                    "max" => Ok(ConsolidationFn::Max),
                    "avg" => Ok(ConsolidationFn::Avg),
                    "sum" => Ok(ConsolidationFn::Sum),
                    "count" => Ok(ConsolidationFn::Count),
                    "last" => Ok(ConsolidationFn::Last),
                    "first" => Ok(ConsolidationFn::First),
                    _ => Err(format!("unknown consolidation function: '{name}'")),
                }
            })
            .collect()
    }
}

/// Accumulator for incremental aggregation within a window.
#[derive(Debug, Clone)]
pub struct Accumulator {
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
    first: Option<f64>,
    last: Option<f64>,
}

impl Accumulator {
    /// Create a new empty accumulator.
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: None,
            last: None,
        }
    }

    /// Add a value to the accumulator.
    pub fn add(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        if self.first.is_none() {
            self.first = Some(value);
        }
        self.last = Some(value);
    }

    /// Finalize the given function for this accumulator.
    pub fn finalize(&self, func: ConsolidationFn) -> f64 {
        match func {
            ConsolidationFn::Min => self.min,
            ConsolidationFn::Max => self.max,
            ConsolidationFn::Avg => {
                if self.count == 0 {
                    0.0
                } else {
                    self.sum / self.count as f64
                }
            }
            ConsolidationFn::Sum => self.sum,
            ConsolidationFn::Count => self.count as f64,
            ConsolidationFn::Last => self.last.unwrap_or(0.0),
            ConsolidationFn::First => self.first.unwrap_or(0.0),
        }
    }
}

impl Default for Accumulator {
    fn default() -> Self {
        Self::new()
    }
}

/// Consolidate a slice of values using the given functions.
///
/// Returns one f64 result per function, in the same order as `functions`.
pub fn consolidate_values(values: &[f64], functions: &[ConsolidationFn]) -> Vec<f64> {
    if values.is_empty() {
        return vec![0.0; functions.len()];
    }

    let mut acc = Accumulator::new();
    for &v in values {
        acc.add(v);
    }

    functions.iter().map(|f| acc.finalize(*f)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_list_valid() {
        let funcs = ConsolidationFn::parse_list("min,max,avg").unwrap();
        assert_eq!(funcs.len(), 3);
        assert_eq!(funcs[0], ConsolidationFn::Min);
        assert_eq!(funcs[1], ConsolidationFn::Max);
        assert_eq!(funcs[2], ConsolidationFn::Avg);
    }

    #[test]
    fn parse_list_invalid() {
        assert!(ConsolidationFn::parse_list("min,foo").is_err());
    }

    #[test]
    fn consolidate_basic() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let funcs = vec![
            ConsolidationFn::Min,
            ConsolidationFn::Max,
            ConsolidationFn::Avg,
            ConsolidationFn::Sum,
            ConsolidationFn::Count,
        ];
        let results = consolidate_values(&values, &funcs);
        assert!((results[0] - 1.0).abs() < f64::EPSILON);
        assert!((results[1] - 5.0).abs() < f64::EPSILON);
        assert!((results[2] - 3.0).abs() < f64::EPSILON);
        assert!((results[3] - 15.0).abs() < f64::EPSILON);
        assert!((results[4] - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn consolidate_empty() {
        let results = consolidate_values(&[], &[ConsolidationFn::Avg]);
        assert!((results[0] - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn accumulator_first_last() {
        let mut acc = Accumulator::new();
        acc.add(10.0);
        acc.add(20.0);
        acc.add(30.0);
        assert!((acc.finalize(ConsolidationFn::First) - 10.0).abs() < f64::EPSILON);
        assert!((acc.finalize(ConsolidationFn::Last) - 30.0).abs() < f64::EPSILON);
    }
}
