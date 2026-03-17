//! Accumulator-based aggregation framework for graph queries.
//!
//! Provides trait-based accumulators for standard aggregate functions
//! (count, sum, avg, min, max, collect) used in Cypher RETURN clauses.

use serde_json::Value;

use crate::error::{GraphError, Result};

/// An accumulator for a single aggregation function.
pub trait Accumulator: Send {
    /// Feed a value into the accumulator.
    fn accumulate(&mut self, value: &Value);
    /// Return the final aggregated result.
    fn finish(&self) -> Value;
    /// Name of this aggregation for error messages.
    fn name(&self) -> &str;
}

/// Counts non-null values. For `count(*)`, all rows are counted.
pub struct CountAcc {
    count: u64,
    count_star: bool,
}

impl CountAcc {
    /// Create a new CountAcc.
    ///
    /// If `count_star` is true, all rows are counted regardless of null values.
    pub fn new(count_star: bool) -> Self {
        Self {
            count: 0,
            count_star,
        }
    }
}

impl Accumulator for CountAcc {
    fn accumulate(&mut self, value: &Value) {
        if self.count_star || !value.is_null() {
            self.count += 1;
        }
    }

    fn finish(&self) -> Value {
        Value::Number(serde_json::Number::from(self.count))
    }

    fn name(&self) -> &str {
        "count"
    }
}

/// Sums numeric values as f64. Non-numeric values are ignored.
/// Returns Null if no numeric values were accumulated.
pub struct SumAcc {
    sum: f64,
    has_value: bool,
}

impl Default for SumAcc {
    fn default() -> Self {
        Self::new()
    }
}

impl SumAcc {
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            has_value: false,
        }
    }
}

impl Accumulator for SumAcc {
    fn accumulate(&mut self, value: &Value) {
        if let Some(n) = value.as_f64() {
            self.sum += n;
            self.has_value = true;
        }
    }

    fn finish(&self) -> Value {
        if self.has_value {
            serde_json::json!(self.sum)
        } else {
            Value::Null
        }
    }

    fn name(&self) -> &str {
        "sum"
    }
}

/// Computes the average of numeric values. Returns Null for zero rows (FMEA F8).
pub struct AvgAcc {
    sum: f64,
    count: u64,
}

impl Default for AvgAcc {
    fn default() -> Self {
        Self::new()
    }
}

impl AvgAcc {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Accumulator for AvgAcc {
    fn accumulate(&mut self, value: &Value) {
        if let Some(n) = value.as_f64() {
            self.sum += n;
            self.count += 1;
        }
    }

    fn finish(&self) -> Value {
        if self.count == 0 {
            Value::Null
        } else {
            serde_json::json!(self.sum / self.count as f64)
        }
    }

    fn name(&self) -> &str {
        "avg"
    }
}

/// Tracks the minimum numeric value.
pub struct MinAcc {
    min: Option<f64>,
}

impl Default for MinAcc {
    fn default() -> Self {
        Self::new()
    }
}

impl MinAcc {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Accumulator for MinAcc {
    fn accumulate(&mut self, value: &Value) {
        if let Some(n) = value.as_f64() {
            self.min = Some(match self.min {
                Some(current) => current.min(n),
                None => n,
            });
        }
    }

    fn finish(&self) -> Value {
        match self.min {
            Some(v) => serde_json::json!(v),
            None => Value::Null,
        }
    }

    fn name(&self) -> &str {
        "min"
    }
}

/// Tracks the maximum numeric value.
pub struct MaxAcc {
    max: Option<f64>,
}

impl Default for MaxAcc {
    fn default() -> Self {
        Self::new()
    }
}

impl MaxAcc {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Accumulator for MaxAcc {
    fn accumulate(&mut self, value: &Value) {
        if let Some(n) = value.as_f64() {
            self.max = Some(match self.max {
                Some(current) => current.max(n),
                None => n,
            });
        }
    }

    fn finish(&self) -> Value {
        match self.max {
            Some(v) => serde_json::json!(v),
            None => Value::Null,
        }
    }

    fn name(&self) -> &str {
        "max"
    }
}

/// Collects values into a JSON array.
///
/// Enforces a configurable size limit (default 10,000) to prevent
/// unbounded memory growth (FMEA F6).
pub struct CollectAcc {
    values: Vec<Value>,
    max_size: usize,
    exceeded: bool,
}

impl CollectAcc {
    pub fn new(max_size: usize) -> Self {
        Self {
            values: Vec::new(),
            max_size,
            exceeded: false,
        }
    }
}

impl Accumulator for CollectAcc {
    fn accumulate(&mut self, value: &Value) {
        if self.exceeded {
            return;
        }
        if self.values.len() >= self.max_size {
            self.exceeded = true;
            return;
        }
        self.values.push(value.clone());
    }

    fn finish(&self) -> Value {
        Value::Array(self.values.clone())
    }

    fn name(&self) -> &str {
        "collect"
    }
}

impl CollectAcc {
    /// Returns true if the size limit was exceeded during accumulation.
    pub fn exceeded(&self) -> bool {
        self.exceeded
    }
}

/// Create an accumulator for the given aggregate function name.
///
/// `count_star` should be true when the argument is `*` (i.e., count all rows).
pub fn create_accumulator(
    name: &str,
    count_star: bool,
    max_collect_size: usize,
) -> Result<Box<dyn Accumulator>> {
    match name.to_lowercase().as_str() {
        "count" => Ok(Box::new(CountAcc::new(count_star))),
        "sum" => Ok(Box::new(SumAcc::new())),
        "avg" => Ok(Box::new(AvgAcc::new())),
        "min" => Ok(Box::new(MinAcc::new())),
        "max" => Ok(Box::new(MaxAcc::new())),
        "collect" => Ok(Box::new(CollectAcc::new(max_collect_size))),
        other => Err(GraphError::Validation(format!(
            "unknown aggregate function: {other}"
        ))),
    }
}

/// Check whether a function name is an aggregate function.
pub fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max" | "collect"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_acc_basic() {
        let mut acc = CountAcc::new(false);
        acc.accumulate(&serde_json::json!(1));
        acc.accumulate(&serde_json::json!(2));
        acc.accumulate(&serde_json::json!(3));
        assert_eq!(acc.finish(), serde_json::json!(3u64));
    }

    #[test]
    fn count_acc_with_nulls() {
        let mut acc = CountAcc::new(false);
        acc.accumulate(&serde_json::json!(1));
        acc.accumulate(&Value::Null);
        acc.accumulate(&serde_json::json!(3));
        acc.accumulate(&Value::Null);
        // Only non-null values counted
        assert_eq!(acc.finish(), serde_json::json!(2u64));

        // count(*) counts all including nulls
        let mut star_acc = CountAcc::new(true);
        star_acc.accumulate(&serde_json::json!(1));
        star_acc.accumulate(&Value::Null);
        star_acc.accumulate(&serde_json::json!(3));
        star_acc.accumulate(&Value::Null);
        assert_eq!(star_acc.finish(), serde_json::json!(4u64));
    }

    #[test]
    fn sum_acc_basic() {
        let mut acc = SumAcc::new();
        acc.accumulate(&serde_json::json!(10));
        acc.accumulate(&serde_json::json!(20));
        acc.accumulate(&serde_json::json!(30));
        assert_eq!(acc.finish(), serde_json::json!(60.0));
    }

    #[test]
    fn sum_acc_non_numeric_ignored() {
        let mut acc = SumAcc::new();
        acc.accumulate(&serde_json::json!(10));
        acc.accumulate(&serde_json::json!("not a number"));
        acc.accumulate(&serde_json::json!(20));
        acc.accumulate(&Value::Null);
        assert_eq!(acc.finish(), serde_json::json!(30.0));
    }

    #[test]
    fn sum_acc_empty_returns_null() {
        let acc = SumAcc::new();
        assert_eq!(acc.finish(), Value::Null);
    }

    #[test]
    fn avg_acc_basic() {
        let mut acc = AvgAcc::new();
        acc.accumulate(&serde_json::json!(10));
        acc.accumulate(&serde_json::json!(20));
        acc.accumulate(&serde_json::json!(30));
        assert_eq!(acc.finish(), serde_json::json!(20.0));
    }

    #[test]
    fn avg_acc_empty_returns_null() {
        let acc = AvgAcc::new();
        assert_eq!(acc.finish(), Value::Null);
    }

    #[test]
    fn min_acc_basic() {
        let mut acc = MinAcc::new();
        acc.accumulate(&serde_json::json!(30));
        acc.accumulate(&serde_json::json!(10));
        acc.accumulate(&serde_json::json!(20));
        assert_eq!(acc.finish(), serde_json::json!(10.0));
    }

    #[test]
    fn min_acc_empty_returns_null() {
        let acc = MinAcc::new();
        assert_eq!(acc.finish(), Value::Null);
    }

    #[test]
    fn max_acc_basic() {
        let mut acc = MaxAcc::new();
        acc.accumulate(&serde_json::json!(10));
        acc.accumulate(&serde_json::json!(30));
        acc.accumulate(&serde_json::json!(20));
        assert_eq!(acc.finish(), serde_json::json!(30.0));
    }

    #[test]
    fn max_acc_empty_returns_null() {
        let acc = MaxAcc::new();
        assert_eq!(acc.finish(), Value::Null);
    }

    #[test]
    fn collect_acc_basic() {
        let mut acc = CollectAcc::new(10_000);
        acc.accumulate(&serde_json::json!(1));
        acc.accumulate(&serde_json::json!("hello"));
        acc.accumulate(&serde_json::json!(9.81));
        assert_eq!(acc.finish(), serde_json::json!([1, "hello", 9.81]));
        assert!(!acc.exceeded());
    }

    #[test]
    fn collect_acc_size_limit() {
        let mut acc = CollectAcc::new(3);
        acc.accumulate(&serde_json::json!(1));
        acc.accumulate(&serde_json::json!(2));
        acc.accumulate(&serde_json::json!(3));
        // This should trigger the limit
        acc.accumulate(&serde_json::json!(4));
        acc.accumulate(&serde_json::json!(5));
        assert!(acc.exceeded());
        // Only the first 3 values should be collected
        assert_eq!(acc.finish(), serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn is_aggregate_function_recognizes_all() {
        assert!(is_aggregate_function("count"));
        assert!(is_aggregate_function("COUNT"));
        assert!(is_aggregate_function("Sum"));
        assert!(is_aggregate_function("avg"));
        assert!(is_aggregate_function("MIN"));
        assert!(is_aggregate_function("max"));
        assert!(is_aggregate_function("Collect"));
        assert!(!is_aggregate_function("id"));
        assert!(!is_aggregate_function("toUpper"));
    }

    #[test]
    fn create_accumulator_valid() {
        let acc = create_accumulator("count", false, 10_000).unwrap();
        assert_eq!(acc.name(), "count");

        let acc = create_accumulator("SUM", false, 10_000).unwrap();
        assert_eq!(acc.name(), "sum");
    }

    #[test]
    fn create_accumulator_unknown() {
        let result = create_accumulator("unknown_func", false, 10_000);
        assert!(result.is_err());
    }
}
