//! Evaluate a compiled expression over a row.
//!
//! Every node yields a [`Value`]; a condition's truth is that value read as SQL
//! truth (`Bool(b)` is `b`, NULL is UNKNOWN), exactly as `Expr.lean`'s `eval` and
//! `truth` define it. An error (division by zero, overflow, a bad cast input, a
//! subquery returning two rows) is an [`ExecError`] carrying the SQLSTATE
//! PostgreSQL reports; nothing is ever coerced into a NULL to keep going.

use std::cmp::Ordering;
use std::sync::Arc;

use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};

use super::run::{self, Frame, Rt};
use super::{CExpr, Func};
use crate::ast::BinOp;
use crate::exec::CmpOp;
use crate::plan::ExecError;
use crate::types::{ArrayElem, ArrayValue, ColumnType, Row, Value};

/// What an expression reads: its own row, that row's frame when it was lent to
/// subqueries, and the enclosing queries' frames.
pub(crate) struct Env<'a> {
    pub(crate) row: &'a Row,
    /// This row as a frame; present exactly when the query `needs_frame`.
    pub(crate) frame: Option<&'a Arc<Frame>>,
    /// The enclosing query's current row (depth 1); its parents are deeper.
    pub(crate) outer: Option<&'a Arc<Frame>>,
    pub(crate) rt: &'a Arc<Rt>,
}

type R<T> = Result<T, ExecError>;

/// SQL truth of a value: `Some(b)` for a boolean, `None` (UNKNOWN) for NULL.
pub(crate) fn truth(v: &Value) -> R<Option<bool>> {
    match v {
        Value::Bool(b) => Ok(Some(*b)),
        Value::Null => Ok(None),
        other => Err(ExecError::eval(
            "42804",
            format!(
                "argument of a condition must be boolean, not {}",
                type_name(other)
            ),
        )),
    }
}

fn tv(t: Option<bool>) -> Value {
    t.map_or(Value::Null, Value::Bool)
}

pub(crate) fn eval(e: &CExpr, env: &Env<'_>) -> R<Value> {
    // The parser bounds the tree's depth; grow the stack rather than trust a
    // debug build's frame sizes within that bound.
    stacker::maybe_grow(64 * 1024, 1024 * 1024, || eval_node(e, env))
}

fn eval_node(e: &CExpr, env: &Env<'_>) -> R<Value> {
    match e {
        CExpr::Col { depth, idx } => column(*depth, *idx, env),
        CExpr::Lit(v) => Ok(v.clone()),
        CExpr::Param(n) => env
            .rt
            .params
            .get(n - 1)
            .cloned()
            .ok_or(ExecError::MissingParameter(*n)),
        CExpr::Cast(inner, ty) => cast(eval(inner, env)?, *ty),
        CExpr::Neg(inner, ty) => negate(eval(inner, env)?, *ty),
        CExpr::Not(inner) => Ok(tv(truth(&eval(inner, env)?)?.map(|b| !b))),
        CExpr::Arith {
            op,
            left,
            right,
            ty,
        } => arith(*op, eval(left, env)?, eval(right, env)?, *ty),
        CExpr::Cmp(op, l, r) => Ok(tv(compare(*op, &eval(l, env)?, &eval(r, env)?))),
        CExpr::And(l, r) => {
            // FALSE dominates, so the right side is not evaluated after a FALSE.
            let a = truth(&eval(l, env)?)?;
            if a == Some(false) {
                return Ok(Value::Bool(false));
            }
            Ok(tv(kleene_and(a, truth(&eval(r, env)?)?)))
        }
        CExpr::Or(l, r) => {
            let a = truth(&eval(l, env)?)?;
            if a == Some(true) {
                return Ok(Value::Bool(true));
            }
            Ok(tv(kleene_or(a, truth(&eval(r, env)?)?)))
        }
        CExpr::IsNull(inner, negated) => Ok(Value::Bool(eval(inner, env)?.is_null() != *negated)),
        CExpr::InList {
            expr,
            list,
            negated,
        } => {
            let x = eval(expr, env)?;
            let mut acc = Some(false);
            for item in list {
                acc = kleene_or(acc, compare(CmpOp::Eq, &x, &eval(item, env)?));
                if acc == Some(true) {
                    break;
                }
            }
            Ok(tv(if *negated { acc.map(|b| !b) } else { acc }))
        }
        CExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let x = eval(expr, env)?;
            let lo = compare(CmpOp::Ge, &x, &eval(low, env)?);
            let hi = compare(CmpOp::Le, &x, &eval(high, env)?);
            let t = kleene_and(lo, hi);
            Ok(tv(if *negated { t.map(|b| !b) } else { t }))
        }
        CExpr::Any { left, op, right } => any(*op, &eval(left, env)?, &eval(right, env)?),
        CExpr::Func(f, args) => function(*f, args, env),
        CExpr::Case {
            operand,
            whens,
            otherwise,
        } => case(operand.as_deref(), whens, otherwise.as_deref(), env),
        CExpr::Exists(q, negated) => {
            let found = run::subquery_rows(q, env, 1)?.into_iter().next().is_some();
            Ok(Value::Bool(found != *negated))
        }
        CExpr::Subquery(q) => {
            let mut rows = run::subquery_rows(q, env, 2)?;
            match rows.len() {
                0 => Ok(Value::Null),
                1 => Ok(rows.pop().expect("one row").0.swap_remove(0)),
                _ => Err(ExecError::eval(
                    "21000",
                    "more than one row returned by a subquery used as an expression",
                )),
            }
        }
        CExpr::ArraySubquery(q, elem) => {
            let rows = run::subquery_rows(q, env, usize::MAX)?;
            let items = rows.into_iter().map(|mut r| r.0.swap_remove(0)).collect();
            Ok(Value::Array(ArrayValue::new(*elem, items)))
        }
        CExpr::Array(items, elem) => {
            let mut values = Vec::with_capacity(items.len());
            for i in items {
                values.push(cast(eval(i, env)?, super::elem_type(*elem))?);
            }
            Ok(Value::Array(ArrayValue::new(*elem, values)))
        }
        CExpr::AtUtc(inner) => match eval(inner, env)? {
            // Both directions keep the instant's microseconds: UTC has no offset.
            v @ (Value::Timestamp(_) | Value::Null) => Ok(v),
            other => Err(ExecError::eval(
                "42883",
                format!("AT TIME ZONE needs a timestamp, not {}", type_name(&other)),
            )),
        },
    }
}

fn column(depth: usize, idx: usize, env: &Env<'_>) -> R<Value> {
    let row = if depth == 0 {
        env.row
    } else {
        let mut frame = env.outer;
        for _ in 1..depth {
            frame = frame.and_then(|f| f.parent.as_ref());
        }
        &frame
            .ok_or_else(|| internal(format!("no enclosing row at depth {depth}")))?
            .row
    };
    row.0.get(idx).cloned().ok_or_else(|| {
        internal(format!(
            "column slot {idx} outside a row of {}",
            row.0.len()
        ))
    })
}

pub(crate) fn internal(message: String) -> ExecError {
    ExecError::eval("XX000", message)
}

pub(crate) fn kleene_and(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

pub(crate) fn kleene_or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

/// Order two values; `None` when either is NULL or they do not compare.
/// Integers and numerics compare by value.
pub(crate) fn order(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Int(x), Value::Numeric { .. }) => Value::numeric(BigInt::from(*x), 0).sql_cmp(b),
        (Value::Numeric { .. }, Value::Int(y)) => a.sql_cmp(&Value::numeric(BigInt::from(*y), 0)),
        _ => a.sql_cmp(b),
    }
}

pub(crate) fn compare(op: CmpOp, a: &Value, b: &Value) -> Option<bool> {
    let ord = order(a, b)?;
    Some(match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
    })
}

/// `x <op> ANY(array)`: TRUE if some element satisfies it, else UNKNOWN if `x`
/// or an element is NULL, else FALSE (`any_true_iff`, `any_false_iff`).
fn any(op: CmpOp, x: &Value, arr: &Value) -> R<Value> {
    let items: &[Value] = match arr {
        Value::Null => return Ok(Value::Null),
        Value::Array(a) => &a.items,
        other => {
            return Err(ExecError::eval(
                "42809",
                format!(
                    "op ANY/ALL (array) requires an array, not {}",
                    type_name(other)
                ),
            ))
        }
    };
    let mut acc = Some(false);
    for item in items {
        acc = kleene_or(acc, compare(op, x, item));
        if acc == Some(true) {
            break;
        }
    }
    Ok(tv(acc))
}

fn case(
    operand: Option<&CExpr>,
    whens: &[(CExpr, CExpr)],
    otherwise: Option<&CExpr>,
    env: &Env<'_>,
) -> R<Value> {
    let subject = match operand {
        Some(o) => Some(eval(o, env)?),
        None => None,
    };
    for (when, then) in whens {
        let w = eval(when, env)?;
        let hit = match &subject {
            Some(s) => compare(CmpOp::Eq, s, &w) == Some(true),
            None => truth(&w)? == Some(true),
        };
        if hit {
            return eval(then, env);
        }
    }
    otherwise.map_or(Ok(Value::Null), |o| eval(o, env))
}

fn function(f: Func, args: &[CExpr], env: &Env<'_>) -> R<Value> {
    match f {
        Func::Coalesce => {
            for a in args {
                let v = eval(a, env)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }
        Func::NullIf => {
            let a = eval(&args[0], env)?;
            let b = eval(&args[1], env)?;
            Ok(if compare(CmpOp::Eq, &a, &b) == Some(true) {
                Value::Null
            } else {
                a
            })
        }
        Func::CharLength => match eval(&args[0], env)? {
            Value::Null => Ok(Value::Null),
            Value::Text(t) => Ok(Value::Int(t.as_str().chars().count() as i64)),
            other => Err(no_function("char_length", &other)),
        },
        Func::Lower | Func::Upper => match eval(&args[0], env)? {
            Value::Null => Ok(Value::Null),
            Value::Text(t) => Ok(Value::Text(
                if f == Func::Lower {
                    t.as_str().to_lowercase()
                } else {
                    t.as_str().to_uppercase()
                }
                .into(),
            )),
            other => Err(no_function("lower/upper", &other)),
        },
        Func::Mod => arith(
            BinOp::Mod,
            eval(&args[0], env)?,
            eval(&args[1], env)?,
            ColumnType::BigInt,
        ),
        Func::Abs => match eval(&args[0], env)? {
            Value::Int(n) => n
                .checked_abs()
                .map(Value::Int)
                .ok_or_else(|| out_of_range("bigint")),
            Value::Float(x) => Ok(Value::float(x.0.abs())),
            Value::Numeric { unscaled, scale } => Ok(Value::numeric(unscaled.abs(), scale)),
            Value::Null => Ok(Value::Null),
            other => Err(no_function("abs", &other)),
        },
        Func::Greatest | Func::Least => {
            let want = if f == Func::Greatest {
                Ordering::Greater
            } else {
                Ordering::Less
            };
            let mut best = Value::Null;
            for a in args {
                let v = eval(a, env)?;
                if v.is_null() {
                    continue;
                }
                if best.is_null() || order(&v, &best) == Some(want) {
                    best = v;
                }
            }
            Ok(best)
        }
        Func::Now => Ok(Value::Timestamp(env.rt.now_micros)),
    }
}

fn negate(v: Value, ty: ColumnType) -> R<Value> {
    match v {
        Value::Null => Ok(Value::Null),
        Value::Int(n) => int_result(n.checked_neg(), ty),
        Value::Float(x) => Ok(Value::float(-x.0)),
        Value::Numeric { unscaled, scale } => Ok(Value::numeric(-unscaled, scale)),
        other => Err(no_operator("-", &other, &other)),
    }
}

/// Arithmetic and `||`. NULL in, NULL out.
pub(crate) fn arith(op: BinOp, a: Value, b: Value, ty: ColumnType) -> R<Value> {
    if a.is_null() || b.is_null() {
        return Ok(Value::Null);
    }
    if op == BinOp::Concat {
        let mut s = text_of(&a)?;
        s.push_str(&text_of(&b)?);
        return Ok(Value::Text(s.into()));
    }
    match (&a, &b) {
        (Value::Int(x), Value::Int(y)) => int_arith(op, *x, *y, ty),
        (Value::Float(_), _) | (_, Value::Float(_)) => float_arith(op, as_f64(&a)?, as_f64(&b)?),
        (Value::Numeric { .. } | Value::Int(_), Value::Numeric { .. } | Value::Int(_)) => {
            numeric_arith(op, as_numeric(&a)?, as_numeric(&b)?)
        }
        _ => Err(no_operator(op_symbol(op), &a, &b)),
    }
}

fn int_arith(op: BinOp, x: i64, y: i64, ty: ColumnType) -> R<Value> {
    if matches!(op, BinOp::Div | BinOp::Mod) && y == 0 {
        return Err(ExecError::eval("22012", "division by zero"));
    }
    let r = match op {
        BinOp::Add => x.checked_add(y),
        BinOp::Sub => x.checked_sub(y),
        BinOp::Mul => x.checked_mul(y),
        BinOp::Div => x.checked_div(y),
        BinOp::Mod => x.checked_rem(y),
        _ => return Err(internal(format!("{op:?} is not arithmetic"))),
    };
    int_result(r, ty)
}

/// An integer result, range-checked against the result type (int4 is 32-bit).
fn int_result(r: Option<i64>, ty: ColumnType) -> R<Value> {
    match (r, ty) {
        (Some(n), ColumnType::Int) if i32::try_from(n).is_err() => Err(out_of_range("integer")),
        (Some(n), _) => Ok(Value::Int(n)),
        (None, ColumnType::Int) => Err(out_of_range("integer")),
        (None, _) => Err(out_of_range("bigint")),
    }
}

fn float_arith(op: BinOp, x: f64, y: f64) -> R<Value> {
    if matches!(op, BinOp::Div | BinOp::Mod) && y == 0.0 {
        return Err(ExecError::eval("22012", "division by zero"));
    }
    let r = match op {
        BinOp::Add => x + y,
        BinOp::Sub => x - y,
        BinOp::Mul => x * y,
        BinOp::Div => x / y,
        BinOp::Mod => x % y,
        _ => return Err(internal(format!("{op:?} is not arithmetic"))),
    };
    if r.is_finite() || !x.is_finite() || !y.is_finite() {
        Ok(Value::float(r))
    } else {
        Err(out_of_range("double precision"))
    }
}

fn numeric_arith(op: BinOp, (ua, sa): (BigInt, i32), (ub, sb): (BigInt, i32)) -> R<Value> {
    let scale = sa.max(sb);
    let align = |u: BigInt, s: i32| u * BigInt::from(10).pow((scale - s) as u32);
    match op {
        BinOp::Add => Ok(Value::numeric(align(ua, sa) + align(ub, sb), scale)),
        BinOp::Sub => Ok(Value::numeric(align(ua, sa) - align(ub, sb), scale)),
        BinOp::Mul => Ok(Value::numeric(ua * ub, sa + sb)),
        BinOp::Mod => {
            let (a, b) = (align(ua, sa), align(ub, sb));
            if b.is_zero() {
                return Err(ExecError::eval("22012", "division by zero"));
            }
            Ok(Value::numeric(a % b, scale))
        }
        BinOp::Div => Err(ExecError::Unsupported("numeric division".into())),
        _ => Err(internal(format!("{op:?} is not arithmetic"))),
    }
}

fn as_f64(v: &Value) -> R<f64> {
    match v {
        Value::Float(x) => Ok(x.0),
        Value::Int(n) => Ok(*n as f64),
        Value::Numeric { unscaled, scale } => numeric_text(unscaled, *scale)
            .parse()
            .map_err(|_| out_of_range("double precision")),
        other => Err(no_operator("arithmetic", other, other)),
    }
}

fn as_numeric(v: &Value) -> R<(BigInt, i32)> {
    match v {
        Value::Int(n) => Ok((BigInt::from(*n), 0)),
        Value::Numeric { unscaled, scale } => Ok((unscaled.clone(), *scale)),
        other => Err(no_operator("arithmetic", other, other)),
    }
}

/// Cast a value to `ty`. A cast PostgreSQL would refuse, or one this engine does
/// not implement, is an error; it never yields NULL.
pub(crate) fn cast(v: Value, ty: ColumnType) -> R<Value> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    match ty {
        ColumnType::Text => Ok(Value::Text(text_of(&v)?.into())),
        ColumnType::Int | ColumnType::BigInt => to_int(v, ty),
        ColumnType::Bool => to_bool(v),
        ColumnType::Float => match v {
            Value::Text(t) => t
                .as_str()
                .trim()
                .parse::<f64>()
                .map(Value::float)
                .map_err(|_| invalid_input("double precision", t.as_str())),
            other => as_f64(&other).map(Value::float),
        },
        ColumnType::Numeric => match v {
            Value::Int(n) => Ok(Value::numeric(BigInt::from(n), 0)),
            Value::Numeric { .. } => Ok(v),
            Value::Text(t) => parse_numeric(t.as_str()),
            Value::Float(x) => parse_numeric(&x.0.to_string()),
            other => Err(cannot_cast(&other, ty)),
        },
        ColumnType::Uuid => match v {
            Value::Uuid(_) => Ok(v),
            Value::Text(t) => uuid::Uuid::parse_str(t.as_str().trim())
                .map(Value::Uuid)
                .map_err(|_| invalid_input("uuid", t.as_str())),
            other => Err(cannot_cast(&other, ty)),
        },
        ColumnType::Timestamp | ColumnType::TimestampTz => match v {
            Value::Timestamp(_) => Ok(v),
            Value::Date(d) => Ok(Value::Timestamp(i64::from(d) * 86_400_000_000)),
            other => Err(cannot_cast(&other, ty)),
        },
        ColumnType::Array(elem) => cast_array(v, elem),
        other_ty => {
            if super::literal_type(&v) == Some(other_ty) {
                Ok(v)
            } else {
                Err(cannot_cast(&v, other_ty))
            }
        }
    }
}

fn cast_array(v: Value, elem: ArrayElem) -> R<Value> {
    let items: Vec<Value> = match v {
        Value::Array(a) if a.elem == elem => return Ok(Value::Array(a)),
        Value::Array(a) => a.items.iter().cloned().collect(),
        Value::TextArray(items) => items
            .into_iter()
            .map(|s| s.map_or(Value::Null, |s| Value::Text(s.into())))
            .collect(),
        other => return Err(cannot_cast(&other, ColumnType::Array(elem))),
    };
    let target = super::elem_type(elem);
    let items = items
        .into_iter()
        .map(|i| cast(i, target))
        .collect::<R<Vec<_>>>()?;
    Ok(Value::Array(ArrayValue::new(elem, items)))
}

fn to_int(v: Value, ty: ColumnType) -> R<Value> {
    let n = match v {
        Value::Int(n) => n,
        Value::Bool(b) => i64::from(b),
        Value::Text(t) => t
            .as_str()
            .trim()
            .parse::<i64>()
            .map_err(|_| invalid_input(int_name(ty), t.as_str()))?,
        Value::Float(x) => {
            let r = x.0.round_ties_even();
            if !r.is_finite() || r < i64::MIN as f64 || r >= i64::MAX as f64 {
                return Err(out_of_range(int_name(ty)));
            }
            r as i64
        }
        Value::Numeric { unscaled, scale } => round_numeric(&unscaled, scale)
            .to_i64()
            .ok_or_else(|| out_of_range(int_name(ty)))?,
        other => return Err(cannot_cast(&other, ty)),
    };
    int_result(Some(n), ty)
}

/// Round half away from zero, as PostgreSQL rounds numeric to integer.
fn round_numeric(unscaled: &BigInt, scale: i32) -> BigInt {
    if scale <= 0 {
        return unscaled * BigInt::from(10).pow((-scale) as u32);
    }
    let div = BigInt::from(10).pow(scale as u32);
    let (q, r) = (unscaled / &div, unscaled % &div);
    if r.abs() * 2 >= div {
        q + if unscaled.is_negative() { -1 } else { 1 }
    } else {
        q
    }
}

fn int_name(ty: ColumnType) -> &'static str {
    if ty == ColumnType::Int {
        "integer"
    } else {
        "bigint"
    }
}

fn to_bool(v: Value) -> R<Value> {
    match v {
        Value::Bool(_) => Ok(v),
        Value::Int(n) => Ok(Value::Bool(n != 0)),
        Value::Text(t) => match t.as_str().trim().to_ascii_lowercase().as_str() {
            "t" | "true" | "y" | "yes" | "on" | "1" => Ok(Value::Bool(true)),
            "f" | "false" | "n" | "no" | "off" | "0" => Ok(Value::Bool(false)),
            _ => Err(invalid_input("boolean", t.as_str())),
        },
        other => Err(cannot_cast(&other, ColumnType::Bool)),
    }
}

fn parse_numeric(s: &str) -> R<Value> {
    let t = s.trim();
    let (neg, digits) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    let valid = !(int.is_empty() && frac.is_empty())
        && int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit());
    if !valid {
        return Err(invalid_input("numeric", s));
    }
    let unscaled: BigInt = format!("{int}{frac}")
        .parse()
        .map_err(|_| invalid_input("numeric", s))?;
    let scale = i32::try_from(frac.len()).map_err(|_| out_of_range("numeric"))?;
    Ok(Value::numeric(
        if neg { -unscaled } else { unscaled },
        scale,
    ))
}

/// PostgreSQL's text form of a value, as `::text` and `||` produce it.
pub(crate) fn text_of(v: &Value) -> R<String> {
    Ok(match v {
        Value::Text(t) => t.as_str().to_string(),
        Value::Int(n) => n.to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Float(x) => x.0.to_string(),
        Value::Uuid(u) => u.to_string(),
        Value::Numeric { unscaled, scale } => numeric_text(unscaled, *scale),
        Value::Array(a)
            if a.elem == ArrayElem::Text
                || a.elem == ArrayElem::Int
                || a.elem == ArrayElem::BigInt =>
        {
            let parts = a
                .items
                .iter()
                .map(|i| match i {
                    Value::Null => Ok("NULL".to_string()),
                    other => text_of(other),
                })
                .collect::<R<Vec<_>>>()?;
            format!("{{{}}}", parts.join(","))
        }
        other => {
            return Err(ExecError::Unsupported(format!(
                "a text form of {}",
                type_name(other)
            )))
        }
    })
}

pub(crate) fn numeric_text(unscaled: &BigInt, scale: i32) -> String {
    if scale <= 0 {
        return (unscaled * BigInt::from(10).pow((-scale) as u32)).to_string();
    }
    let digits = unscaled.abs().to_string();
    let scale = scale as usize;
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    let sign = if unscaled.is_negative() { "-" } else { "" };
    format!("{sign}{int}.{frac}")
}

fn op_symbol(op: BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Concat => "||",
        _ => "?",
    }
}

pub(crate) fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "unknown",
        Value::Int(_) => "integer",
        Value::Text(_) => "text",
        Value::Bool(_) => "boolean",
        Value::Float(_) => "double precision",
        Value::Uuid(_) => "uuid",
        Value::Bytea(_) => "bytea",
        Value::Timestamp(_) => "timestamp",
        Value::Date(_) => "date",
        Value::Time(_) => "time",
        Value::Inet(_) => "inet",
        Value::Numeric { .. } => "numeric",
        Value::Jsonb(_) => "jsonb",
        Value::JsonPath(_) => "jsonpath",
        Value::TextArray(_) | Value::Array(_) => "array",
    }
}

fn out_of_range(ty: &str) -> ExecError {
    ExecError::eval("22003", format!("{ty} out of range"))
}

fn invalid_input(ty: &str, input: &str) -> ExecError {
    ExecError::eval(
        "22P02",
        format!("invalid input syntax for type {ty}: \"{input}\""),
    )
}

fn cannot_cast(v: &Value, ty: ColumnType) -> ExecError {
    ExecError::eval(
        "42846",
        format!("cannot cast type {} to {ty:?}", type_name(v)),
    )
}

fn no_operator(op: &str, a: &Value, b: &Value) -> ExecError {
    ExecError::eval(
        "42883",
        format!(
            "operator does not exist: {} {op} {}",
            type_name(a),
            type_name(b)
        ),
    )
}

fn no_function(name: &str, arg: &Value) -> ExecError {
    ExecError::eval(
        "42883",
        format!("function {name}({}) does not exist", type_name(arg)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_text_places_the_point() {
        assert_eq!(numeric_text(&BigInt::from(15), 1), "1.5");
        assert_eq!(numeric_text(&BigInt::from(-5), 2), "-0.05");
        assert_eq!(numeric_text(&BigInt::from(7), 0), "7");
    }

    #[test]
    fn int4_arithmetic_overflows_loudly() {
        let err = arith(
            BinOp::Add,
            Value::Int(i32::MAX as i64),
            Value::Int(1),
            ColumnType::Int,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ExecError::Eval {
                sqlstate: "22003",
                ..
            }
        ));
        assert_eq!(
            arith(
                BinOp::Add,
                Value::Int(i32::MAX as i64),
                Value::Int(1),
                ColumnType::BigInt
            ),
            Ok(Value::Int(i32::MAX as i64 + 1))
        );
    }

    #[test]
    fn division_by_zero_is_an_error_not_null() {
        let err = arith(BinOp::Div, Value::Int(1), Value::Int(0), ColumnType::Int).unwrap_err();
        assert!(matches!(
            err,
            ExecError::Eval {
                sqlstate: "22012",
                ..
            }
        ));
    }

    #[test]
    fn bad_cast_input_is_an_error_not_null() {
        let err = cast(Value::Text("x".into()), ColumnType::Int).unwrap_err();
        assert!(matches!(
            err,
            ExecError::Eval {
                sqlstate: "22P02",
                ..
            }
        ));
    }

    #[test]
    fn integers_and_numerics_compare_by_value() {
        let one_point_oh = Value::numeric(BigInt::from(10), 1);
        assert_eq!(
            compare(CmpOp::Eq, &Value::Int(1), &one_point_oh),
            Some(true)
        );
    }
}
