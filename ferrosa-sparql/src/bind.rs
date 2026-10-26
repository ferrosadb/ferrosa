//! Term-preserving evaluation for the supported BIND expression subset.
//!
//! FILTER's value coercions are intentionally not used here: a BIND result is
//! an RDF term, including its datatype, language and exact integer value.

use std::collections::HashMap;

use spargebra::algebra::Expression;

use crate::error::SparqlError;
use crate::results::Binding;

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// Reject unsupported forms before executing any part of the query.
pub(crate) fn validate(expr: &Expression) -> Result<(), SparqlError> {
    match expr {
        Expression::Variable(_) | Expression::Literal(_) | Expression::NamedNode(_) => Ok(()),
        Expression::Add(l, r)
        | Expression::Subtract(l, r)
        | Expression::Multiply(l, r)
        | Expression::Divide(l, r) => {
            validate(l)?;
            validate(r)
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) => validate(inner),
        _ => Err(SparqlError::Plan(format!(
            "unsupported BIND expression: {expr:?}"
        ))),
    }
}

/// `None` is a per-solution expression error, not a reason to discard the row.
/// Unsupported numeric evaluation returns a loud query error instead.
pub(crate) fn evaluate(
    expr: &Expression,
    bindings: &HashMap<String, Binding>,
) -> Result<Option<Binding>, SparqlError> {
    let term = match expr {
        Expression::Variable(var) => bindings.get(var.as_str()).cloned(),
        Expression::Literal(lit) => Some(Binding {
            binding_type: "literal".into(),
            value: lit.value().into(),
            datatype: Some(lit.datatype().as_str().into()),
            lang: lit.language().map(str::to_owned),
        }),
        Expression::NamedNode(iri) => Some(Binding {
            binding_type: "uri".into(),
            value: iri.as_str().into(),
            datatype: None,
            lang: None,
        }),
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) => {
            let Some(value) = integer_operand(inner, bindings)? else {
                return Ok(None);
            };
            let result = if matches!(expr, Expression::UnaryMinus(_)) {
                value.checked_neg()
            } else {
                Some(value)
            };
            result.map(|n| literal(n.to_string(), "integer"))
        }
        Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            let Some(l) = integer_operand(left, bindings)? else {
                return Ok(None);
            };
            let Some(r) = integer_operand(right, bindings)? else {
                return Ok(None);
            };
            match expr {
                Expression::Add(_, _) => l.checked_add(r),
                Expression::Subtract(_, _) => l.checked_sub(r),
                Expression::Multiply(_, _) => l.checked_mul(r),
                Expression::Divide(_, _) => {
                    return Ok((r != 0).then(|| literal(integer_quotient(l, r), "decimal")));
                }
                _ => unreachable!("arithmetic expression matched above"),
            }
            .map(|n| literal(n.to_string(), "integer"))
        }
        _ => {
            return Err(SparqlError::Plan(format!(
                "unsupported BIND expression: {expr:?}"
            )));
        }
    };
    Ok(term)
}

fn literal(value: String, datatype: &str) -> Binding {
    Binding {
        binding_type: "literal".into(),
        value,
        datatype: Some(format!("{XSD}{datatype}")),
        lang: None,
    }
}

fn integer_operand(
    expr: &Expression,
    bindings: &HashMap<String, Binding>,
) -> Result<Option<i64>, SparqlError> {
    let Some(term) = evaluate(expr, bindings)? else {
        return Ok(None);
    };
    if term.binding_type != "literal" || term.lang.is_some() {
        return Ok(None);
    }
    match term.datatype.as_deref().and_then(|dt| dt.strip_prefix(XSD)) {
        Some("integer" | "int" | "long") => Ok(term.value.parse().ok()),
        Some("decimal" | "float" | "double") => Err(SparqlError::Execution(
            "BIND arithmetic on decimal/float/double operands is not supported".into(),
        )),
        _ => Ok(None),
    }
}

/// Integer division promotes to decimal, without an f64 round-trip. Use i128
/// for the absolute magnitude of i64::MIN and remainder * 10. Fractional output
/// is bounded to 18 digits, truncated toward zero. The caller rejects zero.
fn integer_quotient(numerator: i64, denominator: i64) -> String {
    let n = i128::from(numerator).abs();
    let d = i128::from(denominator).abs();
    let sign = if numerator != 0 && (numerator < 0) != (denominator < 0) {
        "-"
    } else {
        ""
    };
    let mut result = format!("{sign}{}.", n / d);
    let mut remainder = n % d;
    for _ in 0..18 {
        remainder *= 10;
        result.push(char::from(b'0' + (remainder / d) as u8));
        remainder %= d;
        if remainder == 0 {
            break;
        }
    }
    result
}
