//! Expression evaluator for Cypher expressions.
//!
//! Operates on `serde_json::Value` bindings — each variable maps to a JSON
//! object representing a row's properties. Implements three-valued logic
//! (true / false / null) for boolean operations and NULL propagation for
//! arithmetic and comparisons.

use std::collections::HashMap;

use serde_json::Value;

use crate::error::{GraphError, Result};
use crate::parser::{ArithOp, CompareOp, Expr, Literal};

/// Evaluate a Cypher expression against variable bindings.
///
/// Each binding maps a variable name to a JSON object representing a row.
pub fn eval_expr(expr: &Expr, bindings: &HashMap<String, Value>) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_json(lit)),

        Expr::Var(name) => Ok(bindings.get(name.as_str()).cloned().unwrap_or(Value::Null)),

        Expr::Property { var, name } => {
            let obj = bindings.get(var.as_str()).cloned().unwrap_or(Value::Null);
            match obj {
                Value::Object(map) => Ok(map.get(name.as_str()).cloned().unwrap_or(Value::Null)),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }

        Expr::Comparison { left, op, right } => {
            let lv = eval_expr(left, bindings)?;
            let rv = eval_expr(right, bindings)?;
            Ok(eval_compare(&lv, op, &rv))
        }

        Expr::Arithmetic { left, op, right } => {
            let lv = eval_expr(left, bindings)?;
            let rv = eval_expr(right, bindings)?;
            Ok(eval_arithmetic(&lv, op, &rv))
        }

        Expr::And(a, b) => {
            let av = eval_expr(a, bindings)?;
            // Three-valued AND: false short-circuits regardless of the other side.
            let a_bool = as_bool(&av);
            if a_bool == Some(false) {
                return Ok(Value::Bool(false));
            }
            let bv = eval_expr(b, bindings)?;
            let b_bool = as_bool(&bv);
            if b_bool == Some(false) {
                return Ok(Value::Bool(false));
            }
            // Both true → true; either null → null.
            if a_bool == Some(true) && b_bool == Some(true) {
                Ok(Value::Bool(true))
            } else {
                Ok(Value::Null)
            }
        }

        Expr::Or(a, b) => {
            let av = eval_expr(a, bindings)?;
            // Three-valued OR: true short-circuits regardless of the other side.
            let a_bool = as_bool(&av);
            if a_bool == Some(true) {
                return Ok(Value::Bool(true));
            }
            let bv = eval_expr(b, bindings)?;
            let b_bool = as_bool(&bv);
            if b_bool == Some(true) {
                return Ok(Value::Bool(true));
            }
            // Both false → false; either null → null.
            if a_bool == Some(false) && b_bool == Some(false) {
                Ok(Value::Bool(false))
            } else {
                Ok(Value::Null)
            }
        }

        Expr::Not(e) => {
            let v = eval_expr(e, bindings)?;
            match as_bool(&v) {
                Some(b) => Ok(Value::Bool(!b)),
                None => Ok(Value::Null),
            }
        }

        Expr::IsNull(e) => {
            let v = eval_expr(e, bindings)?;
            Ok(Value::Bool(v.is_null()))
        }

        Expr::IsNotNull(e) => {
            let v = eval_expr(e, bindings)?;
            Ok(Value::Bool(!v.is_null()))
        }

        Expr::Function { name, args } => eval_function(name, args, bindings),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert an AST `Literal` to a JSON `Value`.
fn literal_to_json(lit: &Literal) -> Value {
    match lit {
        Literal::Integer(i) => Value::Number((*i).into()),
        Literal::Float(f) => serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number),
        Literal::String(s) => Value::String(s.clone()),
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Null => Value::Null,
    }
}

/// Extract a bool from a JSON value. Returns `None` for Null (three-valued logic).
fn as_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::Null => None,
        // Non-boolean, non-null values: treat as truthy (consistent with Cypher).
        _ => Some(true),
    }
}

/// Evaluate a comparison, returning Bool or Null.
fn eval_compare(left: &Value, op: &CompareOp, right: &Value) -> Value {
    // NULL propagation: if either side is null, result is null.
    if left.is_null() || right.is_null() {
        return Value::Null;
    }

    // Try numeric comparison first.
    if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
        let result = match op {
            CompareOp::Eq => (l - r).abs() < f64::EPSILON,
            CompareOp::Neq => (l - r).abs() >= f64::EPSILON,
            CompareOp::Lt => l < r,
            CompareOp::Gt => l > r,
            CompareOp::LtEq => l <= r,
            CompareOp::GtEq => l >= r,
        };
        return Value::Bool(result);
    }

    // String comparison.
    if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
        let result = match op {
            CompareOp::Eq => l == r,
            CompareOp::Neq => l != r,
            CompareOp::Lt => l < r,
            CompareOp::Gt => l > r,
            CompareOp::LtEq => l <= r,
            CompareOp::GtEq => l >= r,
        };
        return Value::Bool(result);
    }

    // Boolean comparison: false < true.
    if let (Some(l), Some(r)) = (left.as_bool(), right.as_bool()) {
        let li = l as u8;
        let ri = r as u8;
        let result = match op {
            CompareOp::Eq => li == ri,
            CompareOp::Neq => li != ri,
            CompareOp::Lt => li < ri,
            CompareOp::Gt => li > ri,
            CompareOp::LtEq => li <= ri,
            CompareOp::GtEq => li >= ri,
        };
        return Value::Bool(result);
    }

    // Type mismatch → Null (FMEA F1).
    Value::Null
}

/// Evaluate arithmetic, returning a Number or Null.
fn eval_arithmetic(left: &Value, op: &ArithOp, right: &Value) -> Value {
    // NULL propagation.
    if left.is_null() || right.is_null() {
        return Value::Null;
    }

    let l = match left.as_f64() {
        Some(n) => n,
        None => return Value::Null,
    };
    let r = match right.as_f64() {
        Some(n) => n,
        None => return Value::Null,
    };

    let result = match op {
        ArithOp::Add => l + r,
        ArithOp::Sub => l - r,
        ArithOp::Mul => l * r,
        ArithOp::Div => {
            if r == 0.0 {
                return Value::Null;
            }
            l / r
        }
    };

    // Preserve integer type when both operands are integers and result is exact.
    let is_int_left = left.as_i64().is_some() || left.as_u64().is_some();
    let is_int_right = right.as_i64().is_some() || right.as_u64().is_some();
    if is_int_left && is_int_right && result == result.trunc() {
        let i = result as i64;
        return Value::Number(i.into());
    }

    serde_json::Number::from_f64(result).map_or(Value::Null, Value::Number)
}

// ---------------------------------------------------------------------------
// Built-in scalar functions
// ---------------------------------------------------------------------------

/// Dispatch a function call to the appropriate built-in.
fn eval_function(name: &str, args: &[Expr], bindings: &HashMap<String, Value>) -> Result<Value> {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "id" => fn_id(args, bindings),
        "type" => fn_type(args, bindings),
        "tostring" => fn_to_string(args, bindings),
        "tointeger" => fn_to_integer(args, bindings),
        "tofloat" => fn_to_float(args, bindings),
        "coalesce" => fn_coalesce(args, bindings),
        "size" => fn_size(args, bindings),
        "keys" => fn_keys(args, bindings),
        "abs" => fn_abs(args, bindings),
        _ => Err(GraphError::Validation(format!("unknown function: {name}"))),
    }
}

/// `id(n)` — return the variable's `_id` property, or the hex key if the
/// binding is a string (hex-encoded partition key).
fn fn_id(args: &[Expr], bindings: &HashMap<String, Value>) -> Result<Value> {
    require_args("id", args, 1)?;
    let v = eval_expr(&args[0], bindings)?;
    match &v {
        Value::Object(map) => Ok(map.get("_id").cloned().unwrap_or(Value::Null)),
        Value::String(_) => Ok(v), // already a hex key
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

/// `type(r)` — return the variable's `_type` property.
fn fn_type(args: &[Expr], bindings: &HashMap<String, Value>) -> Result<Value> {
    require_args("type", args, 1)?;
    let v = eval_expr(&args[0], bindings)?;
    match &v {
        Value::Object(map) => Ok(map.get("_type").cloned().unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

/// `toString(expr)` — convert to string.
fn fn_to_string(args: &[Expr], bindings: &HashMap<String, Value>) -> Result<Value> {
    require_args("toString", args, 1)?;
    let v = eval_expr(&args[0], bindings)?;
    match &v {
        Value::Null => Ok(Value::Null),
        Value::String(_) => Ok(v),
        Value::Number(n) => Ok(Value::String(n.to_string())),
        Value::Bool(b) => Ok(Value::String(b.to_string())),
        _ => Ok(Value::String(v.to_string())),
    }
}

/// `toInteger(expr)` — convert to integer.
fn fn_to_integer(args: &[Expr], bindings: &HashMap<String, Value>) -> Result<Value> {
    require_args("toInteger", args, 1)?;
    let v = eval_expr(&args[0], bindings)?;
    match &v {
        Value::Null => Ok(Value::Null),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Number(i.into()))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Number((f as i64).into()))
            } else {
                Ok(Value::Null)
            }
        }
        Value::String(s) => match s.parse::<i64>() {
            Ok(i) => Ok(Value::Number(i.into())),
            Err(_) => Ok(Value::Null),
        },
        Value::Bool(b) => Ok(Value::Number(if *b { 1 } else { 0 }.into())),
        _ => Ok(Value::Null),
    }
}

/// `toFloat(expr)` — convert to float.
fn fn_to_float(args: &[Expr], bindings: &HashMap<String, Value>) -> Result<Value> {
    require_args("toFloat", args, 1)?;
    let v = eval_expr(&args[0], bindings)?;
    match &v {
        Value::Null => Ok(Value::Null),
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Ok(serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number))
            } else {
                Ok(Value::Null)
            }
        }
        Value::String(s) => match s.parse::<f64>() {
            Ok(f) => Ok(serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number)),
            Err(_) => Ok(Value::Null),
        },
        _ => Ok(Value::Null),
    }
}

/// `coalesce(a, b, ...)` — return first non-null argument.
fn fn_coalesce(args: &[Expr], bindings: &HashMap<String, Value>) -> Result<Value> {
    for arg in args {
        let v = eval_expr(arg, bindings)?;
        if !v.is_null() {
            return Ok(v);
        }
    }
    Ok(Value::Null)
}

/// `size(collection)` — return length of string or array.
fn fn_size(args: &[Expr], bindings: &HashMap<String, Value>) -> Result<Value> {
    require_args("size", args, 1)?;
    let v = eval_expr(&args[0], bindings)?;
    match &v {
        Value::String(s) => Ok(Value::Number((s.len() as i64).into())),
        Value::Array(arr) => Ok(Value::Number((arr.len() as i64).into())),
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

/// `keys(map)` — return array of keys from an object.
fn fn_keys(args: &[Expr], bindings: &HashMap<String, Value>) -> Result<Value> {
    require_args("keys", args, 1)?;
    let v = eval_expr(&args[0], bindings)?;
    match &v {
        Value::Object(map) => {
            let keys: Vec<Value> = map.keys().map(|k| Value::String(k.clone())).collect();
            Ok(Value::Array(keys))
        }
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

/// `abs(n)` — absolute value.
fn fn_abs(args: &[Expr], bindings: &HashMap<String, Value>) -> Result<Value> {
    require_args("abs", args, 1)?;
    let v = eval_expr(&args[0], bindings)?;
    match &v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Number(i.abs().into()))
            } else if let Some(f) = n.as_f64() {
                Ok(serde_json::Number::from_f64(f.abs()).map_or(Value::Null, Value::Number))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

/// Validate argument count.
fn require_args(name: &str, args: &[Expr], expected: usize) -> Result<()> {
    if args.len() != expected {
        return Err(GraphError::Validation(format!(
            "{name}() requires {expected} argument(s), got {}",
            args.len()
        )));
    }
    Ok(())
}

/// Evaluate a filter expression and return whether it passed (true).
/// Returns `true` only if the expression evaluates to `Bool(true)`.
/// Null and false both mean "filtered out".
pub fn filter_passes(expr: &Expr, bindings: &HashMap<String, Value>) -> Result<bool> {
    let v = eval_expr(expr, bindings)?;
    Ok(v == Value::Bool(true))
}

/// Convert a partition's cells to a JSON object suitable for bindings.
///
/// Since we don't carry schema metadata into the executor yet, this produces
/// a minimal object with `_id` (hex key) and `_raw_cells` (array of cell
/// values). Property-name-based lookup requires schema threading in a future
/// revision.
pub fn partition_to_json(partition: &ferrosa_sstable::types::Partition, hex_id: &str) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("_id".to_string(), Value::String(hex_id.to_string()));

    // Collect cell values as an array for access.
    let all_rows = partition.static_row.iter().chain(partition.rows.iter());
    for row in all_rows {
        for (col_idx, cell) in &row.cells {
            let key = format!("col_{col_idx}");
            let val = match &cell.value {
                Some(bytes) => match std::str::from_utf8(bytes) {
                    Ok(s) => Value::String(s.to_string()),
                    Err(_) => Value::String(hex::encode(bytes)),
                },
                None => Value::Null,
            };
            map.insert(key, val);
        }
    }

    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{ArithOp, CompareOp, Expr, Literal};
    use serde_json::json;

    fn empty_bindings() -> HashMap<String, Value> {
        HashMap::new()
    }

    fn sample_bindings() -> HashMap<String, Value> {
        let mut b = HashMap::new();
        b.insert(
            "n".to_string(),
            json!({"name": "Alice", "age": 30, "_id": "abc123", "_type": "KNOWS"}),
        );
        b.insert("m".to_string(), json!({"name": "Bob", "age": 25}));
        b
    }

    // -----------------------------------------------------------------------
    // Literal evaluation
    // -----------------------------------------------------------------------

    #[test]
    fn eval_literal_integer() {
        let expr = Expr::Literal(Literal::Integer(42));
        let result = eval_expr(&expr, &empty_bindings()).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn eval_literal_string() {
        let expr = Expr::Literal(Literal::String("hello".into()));
        let result = eval_expr(&expr, &empty_bindings()).unwrap();
        assert_eq!(result, json!("hello"));
    }

    #[test]
    fn eval_literal_null() {
        let expr = Expr::Literal(Literal::Null);
        let result = eval_expr(&expr, &empty_bindings()).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn eval_literal_bool() {
        let expr = Expr::Literal(Literal::Bool(true));
        let result = eval_expr(&expr, &empty_bindings()).unwrap();
        assert_eq!(result, json!(true));

        let expr = Expr::Literal(Literal::Bool(false));
        let result = eval_expr(&expr, &empty_bindings()).unwrap();
        assert_eq!(result, json!(false));
    }

    // -----------------------------------------------------------------------
    // Variable and property lookup
    // -----------------------------------------------------------------------

    #[test]
    fn eval_property_lookup() {
        let expr = Expr::Property {
            var: "n".into(),
            name: "name".into(),
        };
        let result = eval_expr(&expr, &sample_bindings()).unwrap();
        assert_eq!(result, json!("Alice"));
    }

    #[test]
    fn eval_property_missing_returns_null() {
        let expr = Expr::Property {
            var: "n".into(),
            name: "nonexistent".into(),
        };
        let result = eval_expr(&expr, &sample_bindings()).unwrap();
        assert_eq!(result, Value::Null);

        // Missing variable entirely.
        let expr = Expr::Property {
            var: "z".into(),
            name: "name".into(),
        };
        let result = eval_expr(&expr, &sample_bindings()).unwrap();
        assert_eq!(result, Value::Null);
    }

    // -----------------------------------------------------------------------
    // Comparison
    // -----------------------------------------------------------------------

    #[test]
    fn eval_compare_eq() {
        let expr = Expr::Comparison {
            left: Box::new(Expr::Literal(Literal::Integer(10))),
            op: CompareOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(10))),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(true));

        let expr = Expr::Comparison {
            left: Box::new(Expr::Literal(Literal::Integer(10))),
            op: CompareOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(20))),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(false));
    }

    #[test]
    fn eval_compare_lt() {
        let expr = Expr::Comparison {
            left: Box::new(Expr::Literal(Literal::Integer(5))),
            op: CompareOp::Lt,
            right: Box::new(Expr::Literal(Literal::Integer(10))),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(true));

        let expr = Expr::Comparison {
            left: Box::new(Expr::Literal(Literal::Integer(10))),
            op: CompareOp::Lt,
            right: Box::new(Expr::Literal(Literal::Integer(5))),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(false));
    }

    #[test]
    fn eval_compare_null_propagation() {
        let expr = Expr::Comparison {
            left: Box::new(Expr::Literal(Literal::Null)),
            op: CompareOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(10))),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);

        let expr = Expr::Comparison {
            left: Box::new(Expr::Literal(Literal::Integer(10))),
            op: CompareOp::Lt,
            right: Box::new(Expr::Literal(Literal::Null)),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);
    }

    #[test]
    fn eval_compare_type_mismatch_returns_null() {
        // Comparing a string to an integer should return Null (FMEA F1).
        let expr = Expr::Comparison {
            left: Box::new(Expr::Literal(Literal::String("hello".into()))),
            op: CompareOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(10))),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);
    }

    // -----------------------------------------------------------------------
    // Arithmetic
    // -----------------------------------------------------------------------

    #[test]
    fn eval_arithmetic_add() {
        let expr = Expr::Arithmetic {
            left: Box::new(Expr::Literal(Literal::Integer(3))),
            op: ArithOp::Add,
            right: Box::new(Expr::Literal(Literal::Integer(4))),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(7));
    }

    #[test]
    fn eval_arithmetic_null_propagation() {
        let expr = Expr::Arithmetic {
            left: Box::new(Expr::Literal(Literal::Null)),
            op: ArithOp::Add,
            right: Box::new(Expr::Literal(Literal::Integer(4))),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);

        let expr = Expr::Arithmetic {
            left: Box::new(Expr::Literal(Literal::Integer(4))),
            op: ArithOp::Mul,
            right: Box::new(Expr::Literal(Literal::Null)),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);
    }

    // -----------------------------------------------------------------------
    // Boolean logic (three-valued)
    // -----------------------------------------------------------------------

    #[test]
    fn eval_and_short_circuit() {
        // false AND true = false
        let expr = Expr::And(
            Box::new(Expr::Literal(Literal::Bool(false))),
            Box::new(Expr::Literal(Literal::Bool(true))),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(false));

        // true AND true = true
        let expr = Expr::And(
            Box::new(Expr::Literal(Literal::Bool(true))),
            Box::new(Expr::Literal(Literal::Bool(true))),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(true));
    }

    #[test]
    fn eval_or_short_circuit() {
        // true OR false = true
        let expr = Expr::Or(
            Box::new(Expr::Literal(Literal::Bool(true))),
            Box::new(Expr::Literal(Literal::Bool(false))),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(true));

        // false OR false = false
        let expr = Expr::Or(
            Box::new(Expr::Literal(Literal::Bool(false))),
            Box::new(Expr::Literal(Literal::Bool(false))),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(false));
    }

    #[test]
    fn eval_and_null_three_valued() {
        // Null AND false = false
        let expr = Expr::And(
            Box::new(Expr::Literal(Literal::Null)),
            Box::new(Expr::Literal(Literal::Bool(false))),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(false));

        // Null AND true = Null
        let expr = Expr::And(
            Box::new(Expr::Literal(Literal::Null)),
            Box::new(Expr::Literal(Literal::Bool(true))),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);

        // false AND Null = false
        let expr = Expr::And(
            Box::new(Expr::Literal(Literal::Bool(false))),
            Box::new(Expr::Literal(Literal::Null)),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(false));

        // true AND Null = Null
        let expr = Expr::And(
            Box::new(Expr::Literal(Literal::Bool(true))),
            Box::new(Expr::Literal(Literal::Null)),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);
    }

    #[test]
    fn eval_or_null_three_valued() {
        // Null OR true = true
        let expr = Expr::Or(
            Box::new(Expr::Literal(Literal::Null)),
            Box::new(Expr::Literal(Literal::Bool(true))),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(true));

        // Null OR false = Null
        let expr = Expr::Or(
            Box::new(Expr::Literal(Literal::Null)),
            Box::new(Expr::Literal(Literal::Bool(false))),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);

        // true OR Null = true
        let expr = Expr::Or(
            Box::new(Expr::Literal(Literal::Bool(true))),
            Box::new(Expr::Literal(Literal::Null)),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(true));

        // false OR Null = Null
        let expr = Expr::Or(
            Box::new(Expr::Literal(Literal::Bool(false))),
            Box::new(Expr::Literal(Literal::Null)),
        );
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);
    }

    #[test]
    fn eval_not_bool() {
        let expr = Expr::Not(Box::new(Expr::Literal(Literal::Bool(true))));
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(false));

        let expr = Expr::Not(Box::new(Expr::Literal(Literal::Bool(false))));
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(true));
    }

    #[test]
    fn eval_not_null() {
        let expr = Expr::Not(Box::new(Expr::Literal(Literal::Null)));
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);
    }

    // -----------------------------------------------------------------------
    // IS NULL / IS NOT NULL
    // -----------------------------------------------------------------------

    #[test]
    fn eval_is_null() {
        let expr = Expr::IsNull(Box::new(Expr::Literal(Literal::Null)));
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(true));

        let expr = Expr::IsNull(Box::new(Expr::Literal(Literal::Integer(5))));
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(false));
    }

    #[test]
    fn eval_is_not_null() {
        let expr = Expr::IsNotNull(Box::new(Expr::Literal(Literal::Null)));
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(false));

        let expr = Expr::IsNotNull(Box::new(Expr::Literal(Literal::Integer(5))));
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(true));
    }

    // -----------------------------------------------------------------------
    // Built-in functions
    // -----------------------------------------------------------------------

    #[test]
    fn eval_function_coalesce() {
        let expr = Expr::Function {
            name: "coalesce".into(),
            args: vec![
                Expr::Literal(Literal::Null),
                Expr::Literal(Literal::Null),
                Expr::Literal(Literal::Integer(42)),
                Expr::Literal(Literal::Integer(99)),
            ],
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(42));

        // All null → null.
        let expr = Expr::Function {
            name: "coalesce".into(),
            args: vec![Expr::Literal(Literal::Null), Expr::Literal(Literal::Null)],
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);
    }

    #[test]
    fn eval_function_to_string() {
        let expr = Expr::Function {
            name: "toString".into(),
            args: vec![Expr::Literal(Literal::Integer(42))],
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!("42"));

        let expr = Expr::Function {
            name: "toString".into(),
            args: vec![Expr::Literal(Literal::Bool(true))],
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!("true"));

        let expr = Expr::Function {
            name: "toString".into(),
            args: vec![Expr::Literal(Literal::Null)],
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);
    }

    #[test]
    fn eval_function_size() {
        let expr = Expr::Function {
            name: "size".into(),
            args: vec![Expr::Literal(Literal::String("hello".into()))],
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(5));

        let expr = Expr::Function {
            name: "size".into(),
            args: vec![Expr::Literal(Literal::Null)],
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);
    }

    #[test]
    fn eval_function_id() {
        let bindings = sample_bindings();
        let expr = Expr::Function {
            name: "id".into(),
            args: vec![Expr::Var("n".into())],
        };
        assert_eq!(eval_expr(&expr, &bindings).unwrap(), json!("abc123"));
    }

    #[test]
    fn eval_function_type() {
        let bindings = sample_bindings();
        let expr = Expr::Function {
            name: "type".into(),
            args: vec![Expr::Var("n".into())],
        };
        assert_eq!(eval_expr(&expr, &bindings).unwrap(), json!("KNOWS"));
    }

    #[test]
    fn eval_function_abs() {
        let expr = Expr::Function {
            name: "abs".into(),
            args: vec![Expr::Literal(Literal::Integer(-7))],
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(7));

        let expr = Expr::Function {
            name: "abs".into(),
            args: vec![Expr::Literal(Literal::Float(-3.25))],
        };
        let result = eval_expr(&expr, &empty_bindings()).unwrap();
        assert_eq!(result.as_f64().unwrap(), 3.25);
    }

    #[test]
    fn eval_function_to_integer() {
        let expr = Expr::Function {
            name: "toInteger".into(),
            args: vec![Expr::Literal(Literal::String("42".into()))],
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(42));

        let expr = Expr::Function {
            name: "toInteger".into(),
            args: vec![Expr::Literal(Literal::Float(3.7))],
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), json!(3));
    }

    #[test]
    fn eval_function_to_float() {
        let expr = Expr::Function {
            name: "toFloat".into(),
            args: vec![Expr::Literal(Literal::String("3.25".into()))],
        };
        let result = eval_expr(&expr, &empty_bindings()).unwrap();
        assert!((result.as_f64().unwrap() - 3.25).abs() < f64::EPSILON);
    }

    #[test]
    fn eval_function_keys() {
        let bindings = sample_bindings();
        let expr = Expr::Function {
            name: "keys".into(),
            args: vec![Expr::Var("m".into())],
        };
        let result = eval_expr(&expr, &bindings).unwrap();
        let keys = result.as_array().unwrap();
        let key_strs: Vec<&str> = keys.iter().map(|v| v.as_str().unwrap()).collect();
        assert!(key_strs.contains(&"name"));
        assert!(key_strs.contains(&"age"));
    }

    #[test]
    fn eval_unknown_function_returns_error() {
        let expr = Expr::Function {
            name: "bogus".into(),
            args: vec![],
        };
        let result = eval_expr(&expr, &empty_bindings());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("unknown function: bogus"),
            "unexpected error: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // filter_passes helper
    // -----------------------------------------------------------------------

    #[test]
    fn filter_passes_true() {
        let expr = Expr::Literal(Literal::Bool(true));
        assert!(filter_passes(&expr, &empty_bindings()).unwrap());
    }

    #[test]
    fn filter_passes_false() {
        let expr = Expr::Literal(Literal::Bool(false));
        assert!(!filter_passes(&expr, &empty_bindings()).unwrap());
    }

    #[test]
    fn filter_passes_null() {
        let expr = Expr::Literal(Literal::Null);
        assert!(!filter_passes(&expr, &empty_bindings()).unwrap());
    }

    #[test]
    fn eval_property_from_bindings() {
        let bindings = sample_bindings();
        let expr = Expr::Comparison {
            left: Box::new(Expr::Property {
                var: "n".into(),
                name: "age".into(),
            }),
            op: CompareOp::Gt,
            right: Box::new(Expr::Literal(Literal::Integer(25))),
        };
        assert_eq!(eval_expr(&expr, &bindings).unwrap(), json!(true));
    }

    #[test]
    fn eval_arithmetic_with_properties() {
        let bindings = sample_bindings();
        let expr = Expr::Arithmetic {
            left: Box::new(Expr::Property {
                var: "n".into(),
                name: "age".into(),
            }),
            op: ArithOp::Add,
            right: Box::new(Expr::Literal(Literal::Integer(5))),
        };
        assert_eq!(eval_expr(&expr, &bindings).unwrap(), json!(35));
    }

    #[test]
    fn eval_division_by_zero_returns_null() {
        let expr = Expr::Arithmetic {
            left: Box::new(Expr::Literal(Literal::Integer(10))),
            op: ArithOp::Div,
            right: Box::new(Expr::Literal(Literal::Integer(0))),
        };
        assert_eq!(eval_expr(&expr, &empty_bindings()).unwrap(), Value::Null);
    }
}
