//! Consolidation functions and single-pass accumulator.
//!
//! Built-in Rust functions for the hot path. WASM UDF fallback for custom logic.

/// A consolidation function applied to a window of time-series data.
#[derive(Debug, Clone, PartialEq)]
pub enum ConsolidationFn {
    Min,
    Max,
    /// Maps both "avg" and "mean".
    Avg,
    /// Requires sorted values, O(n log n).
    Median,
    /// Population standard deviation (Welford's online algorithm).
    StdDev,
    Count,
    Sum,
    /// Runs multiple functions in a single pass, produces multi-column output.
    /// Must not be nested. Must not contain Wasm.
    Composite(Vec<ConsolidationFn>),
    /// Custom WASM UDF. Runs after the single-pass loop with the full batch.
    Wasm {
        keyspace: String,
        function_name: String,
    },
}

/// Single-pass accumulator using Welford's online algorithm for variance.
///
/// Computes min, max, sum, count, mean, and M2 (for stddev) in one pass.
/// Median requires a separate sorted copy, allocated lazily.
pub struct Accumulator {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    /// Running mean for Welford's algorithm.
    mean: f64,
    /// Sum of squared differences from the running mean (Welford's M2).
    m2: f64,
    /// Lazily allocated sorted buffer for Median computation.
    sorted: Option<Vec<f64>>,
    /// Whether median is needed (controls sorted buffer allocation).
    needs_median: bool,
}

impl Accumulator {
    /// Create a new accumulator.
    ///
    /// If `needs_median` is true, a sorted buffer is allocated to collect all values.
    pub fn new(needs_median: bool) -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            mean: 0.0,
            m2: 0.0,
            sorted: if needs_median { Some(Vec::new()) } else { None },
            needs_median,
        }
    }

    /// Feed a value into the accumulator.
    pub fn push(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;

        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }

        // Welford's online algorithm for variance.
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;

        if let Some(ref mut sorted) = self.sorted {
            sorted.push(value);
        }
    }

    /// Returns the number of values accumulated.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Returns the sum of all values.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Returns whether median computation is enabled.
    pub fn needs_median(&self) -> bool {
        self.needs_median
    }

    /// Returns the minimum value. Returns `f64::INFINITY` if empty.
    pub fn min(&self) -> f64 {
        self.min
    }

    /// Returns the maximum value. Returns `f64::NEG_INFINITY` if empty.
    pub fn max(&self) -> f64 {
        self.max
    }

    /// Returns the arithmetic mean. Returns `NaN` if empty.
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            return f64::NAN;
        }
        self.sum / self.count as f64
    }

    /// Returns the population standard deviation (Welford's algorithm).
    /// Returns `NaN` if empty.
    pub fn stddev(&self) -> f64 {
        if self.count == 0 {
            return f64::NAN;
        }
        (self.m2 / self.count as f64).sqrt()
    }

    /// Returns the median. Requires `needs_median` to be true at construction.
    /// Panics if `needs_median` was false.
    pub fn median(&mut self) -> f64 {
        let sorted = self
            .sorted
            .as_mut()
            .expect("median requires needs_median=true at construction");

        assert!(!sorted.is_empty(), "cannot compute median of empty set");

        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = sorted.len();
        if n % 2 == 1 {
            sorted[n / 2]
        } else {
            (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
        }
    }

    /// Extract the result for a given consolidation function.
    pub fn result_for(&mut self, func: &ConsolidationFn) -> f64 {
        match func {
            ConsolidationFn::Min => self.min(),
            ConsolidationFn::Max => self.max(),
            ConsolidationFn::Avg => self.avg(),
            ConsolidationFn::StdDev => self.stddev(),
            ConsolidationFn::Count => self.count() as f64,
            ConsolidationFn::Sum => self.sum(),
            ConsolidationFn::Median => self.median(),
            _ => panic!("result_for does not handle Composite or Wasm"),
        }
    }
}

impl ConsolidationFn {
    /// Parse a function name string into a `ConsolidationFn`.
    ///
    /// Supports: min, max, avg, mean, median, stddev, count, sum.
    /// Returns `None` for unrecognized names.
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_lowercase().as_str() {
            "min" => Some(ConsolidationFn::Min),
            "max" => Some(ConsolidationFn::Max),
            "avg" | "mean" => Some(ConsolidationFn::Avg),
            "median" => Some(ConsolidationFn::Median),
            "stddev" => Some(ConsolidationFn::StdDev),
            "count" => Some(ConsolidationFn::Count),
            "sum" => Some(ConsolidationFn::Sum),
            _ => None,
        }
    }

    /// Parse a comma-separated list of function names.
    /// Returns error with the unrecognized name if any function is unknown.
    pub fn parse_list(s: &str) -> Result<Vec<Self>, String> {
        let mut funcs = Vec::new();
        for name in s.split(',') {
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            match Self::parse(name) {
                Some(f) => funcs.push(f),
                None => return Err(format!("unknown consolidation function: '{name}'")),
            }
        }
        Ok(funcs)
    }
}

/// Run consolidation functions over a slice of f64 values.
///
/// Returns one f64 result per function. For `Composite`, returns results
/// for each inner function (flattened). `Wasm` is not handled here -- see
/// the async worker path.
pub fn consolidate_values(values: &[f64], functions: &[ConsolidationFn]) -> Vec<f64> {
    assert!(!values.is_empty(), "cannot consolidate empty window");

    // Determine if median is needed anywhere.
    let needs_median = functions_need_median(functions);

    let mut acc = Accumulator::new(needs_median);
    for &v in values {
        acc.push(v);
    }

    let mut results = Vec::new();
    collect_results(&mut acc, functions, &mut results);
    results
}

/// Check if any function in the list (including nested Composite) needs median.
fn functions_need_median(functions: &[ConsolidationFn]) -> bool {
    for f in functions {
        match f {
            ConsolidationFn::Median => return true,
            ConsolidationFn::Composite(inner) => {
                if functions_need_median(inner) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

/// Collect results for each function from the accumulator.
fn collect_results(acc: &mut Accumulator, functions: &[ConsolidationFn], results: &mut Vec<f64>) {
    for f in functions {
        match f {
            ConsolidationFn::Composite(inner) => {
                collect_results(acc, inner, results);
            }
            ConsolidationFn::Wasm { .. } => {
                // WASM is handled externally; push NaN as placeholder.
                results.push(f64::NAN);
            }
            other => {
                results.push(acc.result_for(other));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulator_basic_stats() {
        let mut acc = Accumulator::new(false);
        for v in &[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            acc.push(*v);
        }
        assert_eq!(acc.count(), 8);
        assert!((acc.sum() - 40.0).abs() < f64::EPSILON);
        assert!((acc.min() - 2.0).abs() < f64::EPSILON);
        assert!((acc.max() - 9.0).abs() < f64::EPSILON);
        assert!((acc.avg() - 5.0).abs() < f64::EPSILON);
        // Population stddev of [2,4,4,4,5,5,7,9] = 2.0
        assert!((acc.stddev() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn accumulator_empty() {
        let acc = Accumulator::new(false);
        assert_eq!(acc.count(), 0);
        assert!(acc.avg().is_nan());
        assert!(acc.stddev().is_nan());
        assert_eq!(acc.min(), f64::INFINITY);
        assert_eq!(acc.max(), f64::NEG_INFINITY);
        assert!((acc.sum() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn accumulator_single_value() {
        let mut acc = Accumulator::new(false);
        acc.push(42.0);
        assert_eq!(acc.count(), 1);
        assert!((acc.min() - 42.0).abs() < f64::EPSILON);
        assert!((acc.max() - 42.0).abs() < f64::EPSILON);
        assert!((acc.avg() - 42.0).abs() < f64::EPSILON);
        assert!((acc.sum() - 42.0).abs() < f64::EPSILON);
        assert!((acc.stddev() - 0.0).abs() < f64::EPSILON);
    }

    // -- Median tests --

    #[test]
    fn accumulator_median_odd() {
        let mut acc = Accumulator::new(true);
        for v in &[3.0, 1.0, 4.0, 1.0, 5.0] {
            acc.push(*v);
        }
        // Sorted: [1, 1, 3, 4, 5]. Median = 3.0.
        assert!((acc.median() - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn accumulator_median_even() {
        let mut acc = Accumulator::new(true);
        for v in &[1.0, 2.0, 3.0, 4.0] {
            acc.push(*v);
        }
        // Sorted: [1, 2, 3, 4]. Median = (2+3)/2 = 2.5.
        assert!((acc.median() - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    #[should_panic(expected = "needs_median=true")]
    fn accumulator_median_panics_without_flag() {
        let mut acc = Accumulator::new(false);
        acc.push(1.0);
        let _ = acc.median();
    }

    // -- Composite and consolidate tests --

    #[test]
    fn composite_produces_same_as_individual() {
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];

        // Individual accumulators.
        let mut min_acc = Accumulator::new(false);
        let mut max_acc = Accumulator::new(false);
        let mut avg_acc = Accumulator::new(false);
        for &v in &values {
            min_acc.push(v);
            max_acc.push(v);
            avg_acc.push(v);
        }

        // Composite: single pass.
        let funcs = vec![
            ConsolidationFn::Min,
            ConsolidationFn::Max,
            ConsolidationFn::Avg,
        ];
        let results = consolidate_values(&values, &funcs);

        assert_eq!(results.len(), 3);
        assert!((results[0] - min_acc.min()).abs() < f64::EPSILON);
        assert!((results[1] - max_acc.max()).abs() < f64::EPSILON);
        assert!((results[2] - avg_acc.avg()).abs() < f64::EPSILON);
    }

    #[test]
    fn composite_with_median() {
        let values = vec![1.0, 3.0, 5.0, 7.0, 9.0];
        let funcs = vec![
            ConsolidationFn::Avg,
            ConsolidationFn::Median,
            ConsolidationFn::StdDev,
        ];
        let results = consolidate_values(&values, &funcs);

        assert_eq!(results.len(), 3);
        // avg = 5.0
        assert!((results[0] - 5.0).abs() < f64::EPSILON);
        // median = 5.0
        assert!((results[1] - 5.0).abs() < f64::EPSILON);
        // stddev = sqrt((16+4+0+4+16)/5) = sqrt(8)
        assert!((results[2] - (8.0_f64).sqrt()).abs() < 1e-10);
    }

    // -- Parse tests --

    #[test]
    fn consolidation_fn_parse() {
        assert_eq!(ConsolidationFn::parse("min"), Some(ConsolidationFn::Min));
        assert_eq!(ConsolidationFn::parse("Max"), Some(ConsolidationFn::Max));
        assert_eq!(ConsolidationFn::parse("avg"), Some(ConsolidationFn::Avg));
        assert_eq!(ConsolidationFn::parse("mean"), Some(ConsolidationFn::Avg));
        assert_eq!(
            ConsolidationFn::parse("median"),
            Some(ConsolidationFn::Median)
        );
        assert_eq!(
            ConsolidationFn::parse("STDDEV"),
            Some(ConsolidationFn::StdDev)
        );
        assert_eq!(ConsolidationFn::parse("unknown"), None);
    }

    #[test]
    fn consolidation_fn_parse_list() {
        let funcs = ConsolidationFn::parse_list("min,max, avg ,stddev").unwrap();
        assert_eq!(funcs.len(), 4);
        assert_eq!(funcs[0], ConsolidationFn::Min);
        assert_eq!(funcs[1], ConsolidationFn::Max);
        assert_eq!(funcs[2], ConsolidationFn::Avg);
        assert_eq!(funcs[3], ConsolidationFn::StdDev);
    }

    #[test]
    fn consolidation_fn_parse_list_error() {
        let err = ConsolidationFn::parse_list("min,percentile99").unwrap_err();
        assert!(err.contains("percentile99"));
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn accumulator_sum_matches_brute_force(
            values in prop::collection::vec(-1e6_f64..1e6, 1..200)
        ) {
            let mut acc = Accumulator::new(false);
            for &v in &values {
                acc.push(v);
            }
            let brute_sum: f64 = values.iter().sum();
            prop_assert!((acc.sum() - brute_sum).abs() < 1e-6);
            prop_assert_eq!(acc.count(), values.len() as u64);

            let brute_min = values.iter().cloned().fold(f64::INFINITY, f64::min);
            let brute_max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            prop_assert!((acc.min() - brute_min).abs() < f64::EPSILON);
            prop_assert!((acc.max() - brute_max).abs() < f64::EPSILON);
        }

        #[test]
        fn consolidate_values_matches_individual(
            values in prop::collection::vec(-1e3_f64..1e3, 1..100)
        ) {
            let funcs = vec![
                ConsolidationFn::Min,
                ConsolidationFn::Max,
                ConsolidationFn::Avg,
                ConsolidationFn::Sum,
                ConsolidationFn::Count,
            ];
            let results = consolidate_values(&values, &funcs);

            let brute_min = values.iter().cloned().fold(f64::INFINITY, f64::min);
            let brute_max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let brute_sum: f64 = values.iter().sum();
            let brute_avg = brute_sum / values.len() as f64;

            prop_assert!((results[0] - brute_min).abs() < f64::EPSILON);
            prop_assert!((results[1] - brute_max).abs() < f64::EPSILON);
            prop_assert!((results[2] - brute_avg).abs() < 1e-10);
            prop_assert!((results[3] - brute_sum).abs() < 1e-6);
            prop_assert!((results[4] - values.len() as f64).abs() < f64::EPSILON);
        }
    }
}
