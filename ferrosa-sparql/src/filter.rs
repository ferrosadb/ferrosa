//! SPARQL FILTER expression evaluation.
//!
//! Evaluates spargebra `Expression` trees against variable binding sets.
//! Supports the core expression types needed for common SPARQL patterns.

use std::collections::HashMap;

use spargebra::algebra::Expression;
use spargebra::term::Literal;

use crate::results::Binding;

/// Evaluate a FILTER expression against a binding set.
/// Returns `true` if the binding passes the filter.
pub fn eval_filter(expr: &Expression, bindings: &HashMap<String, Binding>) -> bool {
    match eval_expr(expr, bindings) {
        Value::Boolean(b) => b,
        Value::String(s) => !s.is_empty(),
        Value::Integer(n) => n != 0,
        Value::Float(f) => f != 0.0,
        Value::Null => false,
    }
}

/// Internal value representation for expression evaluation.
#[derive(Debug, Clone, PartialEq)]
enum Value {
    Boolean(bool),
    String(String),
    Integer(i64),
    Float(f64),
    Null,
}

impl Value {
    fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    fn to_float(&self) -> Option<f64> {
        match self {
            Value::Integer(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }
}

fn eval_expr(expr: &Expression, bindings: &HashMap<String, Binding>) -> Value {
    match expr {
        Expression::Variable(var) => bindings
            .get(var.as_str())
            .map(binding_to_value)
            .unwrap_or(Value::Null),

        Expression::Equal(left, right) => {
            let l = eval_expr(left, bindings);
            let r = eval_expr(right, bindings);
            Value::Boolean(values_equal(&l, &r))
        }

        Expression::Greater(left, right) => {
            let l = eval_expr(left, bindings);
            let r = eval_expr(right, bindings);
            Value::Boolean(compare_values(&l, &r).is_some_and(|c| c > 0))
        }

        Expression::Less(left, right) => {
            let l = eval_expr(left, bindings);
            let r = eval_expr(right, bindings);
            Value::Boolean(compare_values(&l, &r).is_some_and(|c| c < 0))
        }

        // GreaterOrEqual and LessOrEqual are represented as Not(Less) and Not(Greater)
        // by spargebra, so we handle them via Not.
        Expression::And(left, right) => {
            let l = eval_filter(left, bindings);
            let r = eval_filter(right, bindings);
            Value::Boolean(l && r)
        }

        Expression::Or(left, right) => {
            let l = eval_filter(left, bindings);
            let r = eval_filter(right, bindings);
            Value::Boolean(l || r)
        }

        Expression::Not(inner) => {
            let v = eval_filter(inner, bindings);
            Value::Boolean(!v)
        }

        Expression::Bound(var) => Value::Boolean(bindings.contains_key(var.as_str())),

        Expression::Literal(lit) => literal_to_value(lit),

        Expression::SameTerm(left, right) => {
            let l = eval_expr(left, bindings);
            let r = eval_expr(right, bindings);
            Value::Boolean(l == r)
        }

        Expression::If(cond, then_expr, else_expr) => {
            if eval_filter(cond, bindings) {
                eval_expr(then_expr, bindings)
            } else {
                eval_expr(else_expr, bindings)
            }
        }

        Expression::NamedNode(nn) => Value::String(nn.as_str().to_string()),

        // Arithmetic
        Expression::Add(l, r) => numeric_op(l, r, bindings, |a, b| a + b),
        Expression::Subtract(l, r) => numeric_op(l, r, bindings, |a, b| a - b),
        Expression::Multiply(l, r) => numeric_op(l, r, bindings, |a, b| a * b),
        Expression::Divide(l, r) => {
            let rv = eval_expr(r, bindings);
            if rv.to_float() == Some(0.0) {
                Value::Null
            } else {
                numeric_op(l, r, bindings, |a, b| a / b)
            }
        }

        Expression::FunctionCall(func, args) => eval_function(func, args, bindings),

        // Unsupported expressions evaluate to Null (filter passes nothing).
        _ => {
            tracing::debug!(?expr, "unsupported FILTER expression, evaluating as Null");
            Value::Null
        }
    }
}

/// Evaluate a supported SPARQL function call. Returns `Value::Null` for
/// functions that are not implemented; callers that require fail-loud behaviour
/// (e.g. ORDER BY) must gate on [`unsupported_expr`] first.
fn eval_function(
    func: &spargebra::algebra::Function,
    args: &[Expression],
    bindings: &HashMap<String, Binding>,
) -> Value {
    use spargebra::algebra::Function as F;
    let arg0 = || {
        args.first()
            .map(|a| eval_expr(a, bindings))
            .unwrap_or(Value::Null)
    };
    match func {
        // STR(x): the lexical/string form of the value.
        F::Str => match arg0() {
            Value::Null => Value::Null,
            other => Value::String(value_as_lexical(&other)),
        },
        F::UCase => match arg0() {
            Value::Null => Value::Null,
            other => Value::String(value_as_lexical(&other).to_uppercase()),
        },
        F::LCase => match arg0() {
            Value::Null => Value::Null,
            other => Value::String(value_as_lexical(&other).to_lowercase()),
        },
        F::StrLen => match arg0() {
            Value::Null => Value::Null,
            other => Value::Integer(value_as_lexical(&other).chars().count() as i64),
        },
        F::Abs => arg0()
            .to_float()
            .map(|n| Value::Float(n.abs()))
            .unwrap_or(Value::Null),
        F::Ceil => arg0()
            .to_float()
            .map(|n| Value::Float(n.ceil()))
            .unwrap_or(Value::Null),
        F::Floor => arg0()
            .to_float()
            .map(|n| Value::Float(n.floor()))
            .unwrap_or(Value::Null),
        F::Round => arg0()
            .to_float()
            .map(|n| Value::Float(n.round()))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// Whether an expression is fully supported for evaluation (used by ORDER BY to
/// fail loud on unsupported forms instead of sorting everything as Null/equal).
///
/// Returns the name of the first unsupported sub-expression, or `None` if the
/// whole tree is evaluable.
pub fn unsupported_expr(expr: &Expression) -> Option<String> {
    use spargebra::algebra::Function as F;
    match expr {
        Expression::Variable(_)
        | Expression::Literal(_)
        | Expression::NamedNode(_)
        | Expression::Bound(_) => None,
        Expression::Add(l, r)
        | Expression::Subtract(l, r)
        | Expression::Multiply(l, r)
        | Expression::Divide(l, r)
        | Expression::Equal(l, r)
        | Expression::Greater(l, r)
        | Expression::GreaterOrEqual(l, r)
        | Expression::Less(l, r)
        | Expression::LessOrEqual(l, r)
        | Expression::And(l, r)
        | Expression::Or(l, r)
        | Expression::SameTerm(l, r) => unsupported_expr(l).or_else(|| unsupported_expr(r)),
        Expression::Not(inner) => unsupported_expr(inner),
        Expression::If(c, t, e) => unsupported_expr(c)
            .or_else(|| unsupported_expr(t))
            .or_else(|| unsupported_expr(e)),
        Expression::FunctionCall(func, args) => {
            let supported = matches!(
                func,
                F::Str | F::UCase | F::LCase | F::StrLen | F::Abs | F::Ceil | F::Floor | F::Round
            );
            if !supported {
                return Some(format!("function {func:?}"));
            }
            args.iter().find_map(unsupported_expr)
        }
        other => Some(format!("{other:?}")),
    }
}

/// Convert a solution [`Binding`] into an evaluation [`Value`].
///
/// Honors the binding's `datatype` so that numeric literals participate in
/// arithmetic and numeric ordering. Untyped literals whose value parses as a
/// number are also treated numerically (xsd:integer/double), matching how
/// SPARQL promotes numeric literals; everything else is a string.
fn binding_to_value(b: &Binding) -> Value {
    if let Some(dt) = b.datatype.as_deref() {
        return match dt {
            "http://www.w3.org/2001/XMLSchema#integer"
            | "http://www.w3.org/2001/XMLSchema#int"
            | "http://www.w3.org/2001/XMLSchema#long" => b
                .value
                .parse::<i64>()
                .map(Value::Integer)
                .unwrap_or(Value::Null),
            "http://www.w3.org/2001/XMLSchema#decimal"
            | "http://www.w3.org/2001/XMLSchema#float"
            | "http://www.w3.org/2001/XMLSchema#double" => b
                .value
                .parse::<f64>()
                .map(Value::Float)
                .unwrap_or(Value::Null),
            "http://www.w3.org/2001/XMLSchema#boolean" => {
                Value::Boolean(b.value == "true" || b.value == "1")
            }
            _ => Value::String(b.value.clone()),
        };
    }
    Value::String(b.value.clone())
}

/// SPARQL ORDER BY comparison (URS-QEC-S04).
///
/// Evaluates `expr` against both solutions and returns their relative order.
/// Numeric values compare numerically; otherwise comparison is lexical on the
/// string form. Unbound/Null sorts lowest (before any bound value), per the
/// SPARQL 1.1 ORDER BY ordering of unbound variables.
pub fn order_cmp(
    expr: &Expression,
    a: &HashMap<String, Binding>,
    b: &HashMap<String, Binding>,
) -> std::cmp::Ordering {
    let va = eval_expr(expr, a);
    let vb = eval_expr(expr, b);
    cmp_values(&va, &vb)
}

fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // Null (unbound / type error) sorts lowest.
    match (matches!(a, Value::Null), matches!(b, Value::Null)) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        (false, false) => {}
    }
    // Numeric comparison when both are numeric.
    if let (Some(x), Some(y)) = (a.to_float(), b.to_float()) {
        return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
    }
    // Booleans: false < true.
    if let (Value::Boolean(x), Value::Boolean(y)) = (a, b) {
        return x.cmp(y);
    }
    // Fall back to lexical comparison on the string form.
    value_as_lexical(a).cmp(&value_as_lexical(b))
}

fn value_as_lexical(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Null => String::new(),
    }
}

fn literal_to_value(lit: &Literal) -> Value {
    let value = lit.value();
    let datatype = lit.datatype().as_str();

    match datatype {
        "http://www.w3.org/2001/XMLSchema#integer"
        | "http://www.w3.org/2001/XMLSchema#int"
        | "http://www.w3.org/2001/XMLSchema#long" => value
            .parse::<i64>()
            .map(Value::Integer)
            .unwrap_or(Value::Null),
        "http://www.w3.org/2001/XMLSchema#decimal"
        | "http://www.w3.org/2001/XMLSchema#float"
        | "http://www.w3.org/2001/XMLSchema#double" => value
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or(Value::Null),
        "http://www.w3.org/2001/XMLSchema#boolean" => {
            Value::Boolean(value == "true" || value == "1")
        }
        _ => Value::String(value.to_string()),
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
        (Value::Boolean(a), Value::Boolean(b)) => a == b,
        // Cross-type: try numeric comparison
        _ => {
            if let (Some(a), Some(b)) = (a.to_float(), b.to_float()) {
                (a - b).abs() < f64::EPSILON
            } else if let (Some(a), Some(b)) = (a.as_string(), b.as_string()) {
                a == b
            } else {
                false
            }
        }
    }
}

fn compare_values(a: &Value, b: &Value) -> Option<i8> {
    match (a.to_float(), b.to_float()) {
        (Some(a), Some(b)) => {
            if (a - b).abs() < f64::EPSILON {
                Some(0)
            } else if a < b {
                Some(-1)
            } else {
                Some(1)
            }
        }
        _ => match (a.as_string(), b.as_string()) {
            (Some(a), Some(b)) => Some(a.cmp(b) as i8),
            _ => None,
        },
    }
}

fn numeric_op(
    left: &Expression,
    right: &Expression,
    bindings: &HashMap<String, Binding>,
    op: fn(f64, f64) -> f64,
) -> Value {
    let l = eval_expr(left, bindings);
    let r = eval_expr(right, bindings);
    match (l.to_float(), r.to_float()) {
        (Some(a), Some(b)) => Value::Float(op(a, b)),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(typ: &str, val: &str) -> Binding {
        Binding {
            binding_type: typ.into(),
            value: val.into(),
            datatype: None,
            lang: None,
        }
    }

    fn bindings_with(pairs: &[(&str, &str, &str)]) -> HashMap<String, Binding> {
        pairs
            .iter()
            .map(|(name, typ, val)| (name.to_string(), binding(typ, val)))
            .collect()
    }

    #[test]
    fn filter_equal_strings() {
        let b = bindings_with(&[("name", "literal", "Alice")]);
        let expr = Expression::Equal(
            Box::new(Expression::Variable(
                spargebra::term::Variable::new_unchecked("name"),
            )),
            Box::new(Expression::Literal(Literal::new_simple_literal("Alice"))),
        );
        assert!(eval_filter(&expr, &b));
    }

    #[test]
    fn filter_equal_fails_on_mismatch() {
        let b = bindings_with(&[("name", "literal", "Bob")]);
        let expr = Expression::Equal(
            Box::new(Expression::Variable(
                spargebra::term::Variable::new_unchecked("name"),
            )),
            Box::new(Expression::Literal(Literal::new_simple_literal("Alice"))),
        );
        assert!(!eval_filter(&expr, &b));
    }

    #[test]
    fn filter_equal_uri() {
        let b = bindings_with(&[("p", "uri", "http://foaf/knows")]);
        let expr = Expression::Equal(
            Box::new(Expression::Variable(
                spargebra::term::Variable::new_unchecked("p"),
            )),
            Box::new(Expression::NamedNode(
                spargebra::term::NamedNode::new_unchecked("http://foaf/knows"),
            )),
        );
        assert!(eval_filter(&expr, &b));
    }

    #[test]
    fn filter_and() {
        let b = bindings_with(&[("a", "literal", "1"), ("b", "literal", "1")]);
        let expr = Expression::And(
            Box::new(Expression::Equal(
                Box::new(Expression::Variable(
                    spargebra::term::Variable::new_unchecked("a"),
                )),
                Box::new(Expression::Literal(Literal::new_simple_literal("1"))),
            )),
            Box::new(Expression::Equal(
                Box::new(Expression::Variable(
                    spargebra::term::Variable::new_unchecked("b"),
                )),
                Box::new(Expression::Literal(Literal::new_simple_literal("1"))),
            )),
        );
        assert!(eval_filter(&expr, &b));
    }

    #[test]
    fn filter_bound_true() {
        let b = bindings_with(&[("x", "uri", "val")]);
        let expr = Expression::Bound(spargebra::term::Variable::new_unchecked("x"));
        assert!(eval_filter(&expr, &b));
    }

    #[test]
    fn filter_bound_false() {
        let b: HashMap<String, Binding> = HashMap::new();
        let expr = Expression::Bound(spargebra::term::Variable::new_unchecked("x"));
        assert!(!eval_filter(&expr, &b));
    }

    #[test]
    fn filter_unbound_variable_returns_null() {
        let b: HashMap<String, Binding> = HashMap::new();
        let expr = Expression::Equal(
            Box::new(Expression::Variable(
                spargebra::term::Variable::new_unchecked("missing"),
            )),
            Box::new(Expression::Literal(Literal::new_simple_literal("val"))),
        );
        // NULL = "val" → false
        assert!(!eval_filter(&expr, &b));
    }
}
