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
}
