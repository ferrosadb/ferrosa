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
            .map(|b| Value::String(b.value.clone()))
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

        // Unsupported expressions evaluate to Null (filter passes nothing).
        _ => {
            tracing::debug!(?expr, "unsupported FILTER expression, evaluating as Null");
            Value::Null
        }
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
