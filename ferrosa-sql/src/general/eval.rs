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
        CExpr::Cmp(op, l, r) => Ok(tv(compare(*op, &eval(l, env)?, &eval(r, env)?)?)),
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
                acc = kleene_or(acc, compare(CmpOp::Eq, &x, &eval(item, env)?)?);
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
            let lo = compare(CmpOp::Ge, &x, &eval(low, env)?)?;
            let hi = compare(CmpOp::Le, &x, &eval(high, env)?)?;
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
        CExpr::Exists(q, negated) => memoized(e, q, env, || {
            let found = run::subquery_stream(q, env)?.next().transpose()?.is_some();
            Ok(Value::Bool(found != *negated))
        }),
        CExpr::Subquery(q) => memoized(e, q, env, || {
            let mut rows = run::subquery_stream(q, env)?;
            let Some(first) = rows.next().transpose()? else {
                return Ok(Value::Null);
            };
            if rows.next().transpose()?.is_some() {
                return Err(ExecError::eval(
                    "21000",
                    "more than one row returned by a subquery used as an expression",
                ));
            }
            first_column(first)
        }),
        CExpr::ArraySubquery(q, elem) => memoized(e, q, env, || {
            // ARRAY(subquery) is one value holding every row, as in PostgreSQL.
            let items = run::subquery_stream(q, env)?
                .map(|row| row.and_then(first_column))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Array(ArrayValue::new(*elem, items)))
        }),
        CExpr::Array(items, elem) => {
            let mut values = Vec::with_capacity(items.len());
            for i in items {
                values.push(cast(eval(i, env)?, super::elem_type(*elem))?);
            }
            Ok(Value::Array(ArrayValue::new(*elem, values)))
        }
        CExpr::CatalogCast(name) => Err(internal(format!(
            "a ::{name} cast reached evaluation unresolved; the front end resolves catalog casts"
        ))),
        CExpr::AtUtc(inner) => match eval(inner, env)? {
            // Both directions keep the instant's microseconds: UTC has no offset.
            v @ (Value::Timestamp(_) | Value::Null) => Ok(v),
            other => Err(ExecError::eval(
                "42883",
                format!("AT TIME ZONE needs a timestamp, not {}", type_name(&other)),
            )),
        },
        CExpr::StoreMillis(inner) => match eval(inner, env)? {
            // Exactly the storage's `micros / 1000` (toward zero), back in micros.
            Value::Timestamp(micros) => Ok(Value::Timestamp(micros / 1000 * 1000)),
            Value::Null => Ok(Value::Null),
            other => Err(internal(format!(
                "a millisecond column was assigned a {}",
                type_name(&other)
            ))),
        },
    }
}

/// A one-column subquery row's value. The planner admits only one-column subqueries here, so a
/// row with no column is an engine fault, reported rather than read as NULL.
fn first_column(row: Row) -> R<Value> {
    row.0
        .into_iter()
        .next()
        .ok_or_else(|| internal("a one-column subquery produced a row with no column".into()))
}

/// An uncorrelated subquery has one value for the whole statement: compute it
/// once and reuse it (an array value is shared, not copied). A correlated one
/// depends on the outer row and is computed every time.
fn memoized(
    node: &CExpr,
    q: &super::Query,
    env: &Env<'_>,
    compute: impl FnOnce() -> R<Value>,
) -> R<Value> {
    if q.correlated {
        return compute();
    }
    let key = std::ptr::from_ref(node) as usize;
    let cached = memo_lock(env.rt).get(&key).cloned();
    if let Some(v) = cached {
        return Ok(v);
    }
    // Computed outside the lock: the subquery may itself hold memoised nodes.
    let value = compute()?;
    memo_lock(env.rt).insert(key, value.clone());
    Ok(value)
}

fn memo_lock(rt: &Rt) -> std::sync::MutexGuard<'_, std::collections::HashMap<usize, Value>> {
    rt.memo.lock().unwrap_or_else(|p| p.into_inner())
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

/// Compare two values: UNKNOWN (`Ok(None)`) when either is NULL or a float is
/// NaN; an error when two non-NULL values of different types meet, exactly as
/// PostgreSQL refuses `text = integer`. A mismatch is never folded into UNKNOWN,
/// which would silently drop rows (`Expr.lean` `strict_compare_*`).
pub(crate) fn compare(op: CmpOp, a: &Value, b: &Value) -> R<Option<bool>> {
    if a.is_null() || b.is_null() {
        return Ok(None);
    }
    let Some(ord) = order(a, b) else {
        let nan = |v: &Value| matches!(v, Value::Float(x) if x.0.is_nan());
        if nan(a) || nan(b) {
            return Ok(None);
        }
        return Err(ExecError::eval(
            "42883",
            format!(
                "operator does not exist: {} {} {}",
                type_name(a),
                cmp_symbol(op),
                type_name(b)
            ),
        ));
    };
    Ok(Some(match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
    }))
}

fn cmp_symbol(op: CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "=",
        CmpOp::Ne => "<>",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
    }
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
        acc = kleene_or(acc, compare(op, x, item)?);
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
            Some(s) => compare(CmpOp::Eq, s, &w)? == Some(true),
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
            Ok(if compare(CmpOp::Eq, &a, &b)? == Some(true) {
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
        Func::Timezone => {
            let zone = eval(&args[0], env)?;
            let ts = eval(&args[1], env)?;
            match (&zone, &ts) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Text(z), Value::Timestamp(_)) if super::is_utc(z.as_str()) => Ok(ts),
                (Value::Text(z), Value::Timestamp(_)) => Err(ExecError::Unsupported(format!(
                    "timezone() with zone {:?} (only UTC)",
                    z.as_str()
                ))),
                _ => Err(no_function("timezone", &ts)),
            }
        }
        Func::ArrayAppend(elem) => {
            let item = cast(eval(&args[1], env)?, super::elem_type(elem))?;
            let mut items: Vec<Value> = match eval(&args[0], env)? {
                Value::Null => Vec::with_capacity(1),
                Value::Array(a) => a.items.iter().cloned().collect(),
                other => return Err(no_function("array_append", &other)),
            };
            items.push(item);
            Ok(Value::Array(ArrayValue::new(elem, items)))
        }
        Func::JsonBuildObject => json_build_object(args, env),
    }
}

/// `json_build_object(k, v, …)`: keys are text, values are scalars (a nested
/// json value is not supported yet and is refused, never stringified).
fn json_build_object(args: &[CExpr], env: &Env<'_>) -> R<Value> {
    use ferrosa_jsonb::{JsonbBuilder, JsonbValue, Number};
    let limits = json_limits()?;
    let json_err = |e: ferrosa_jsonb::JsonbError| ExecError::eval("22023", e.to_string());
    let mut b = JsonbBuilder::new(limits);
    b.begin_object().map_err(json_err)?;
    for pair in args.chunks(2) {
        let key = match eval(&pair[0], env)? {
            Value::Null => {
                return Err(ExecError::eval(
                    "22004",
                    "null value not allowed for object key",
                ))
            }
            k => text_of(&k)?,
        };
        b.key(&key).map_err(json_err)?;
        match eval(&pair[1], env)? {
            Value::Null => b.null(),
            Value::Bool(v) => b.boolean(v),
            Value::Int(n) => b.number(Number::from_i64(n)),
            Value::Float(x) => b.number(Number::from_f64_shortest(x.0).map_err(json_err)?),
            Value::Numeric { unscaled, scale } => {
                b.number(Number::parse_lexeme(&numeric_text(&unscaled, scale)).map_err(json_err)?)
            }
            v @ (Value::Text(_) | Value::Uuid(_) | Value::Timestamp(_)) => b.string(&text_of(&v)?),
            other => {
                return Err(ExecError::Unsupported(format!(
                    "json_build_object with a {} value",
                    type_name(&other)
                )))
            }
        }
        .map_err(json_err)?;
    }
    b.end_object().map_err(json_err)?;
    let encoded = b.finish().map_err(json_err)?;
    JsonbValue::from_encoded(encoded)
        .map(Value::Jsonb)
        .map_err(json_err)
}

/// The engine's jsonb limits: the crate defaults (the front end's configured
/// limits apply to client input; values built here are bounded by these).
fn json_limits() -> R<ferrosa_jsonb::Limits> {
    ferrosa_jsonb::Limits::from_config(
        &ferrosa_jsonb::LimitsConfig::default(),
        ferrosa_jsonb::HARD_MAX_ENCODED_BYTES as u64,
    )
    .map_err(|e| internal(format!("jsonb limits: {e}")))
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
        ColumnType::Oid => match to_int(v, ColumnType::BigInt)? {
            Value::Int(n) if (0..=i64::from(u32::MAX)).contains(&n) => Ok(Value::Int(n)),
            _ => Err(out_of_range("oid")),
        },
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
            Value::Text(t) => parse_timestamp(t.as_str()).map(Value::Timestamp),
            Value::Date(d) => Ok(Value::Timestamp(i64::from(d) * 86_400_000_000)),
            other => Err(cannot_cast(&other, ty)),
        },
        ColumnType::Array(elem) => cast_array(v, elem),
        ColumnType::Jsonb => match v {
            Value::Jsonb(_) => Ok(v),
            Value::Text(t) => {
                let encoded = ferrosa_jsonb::parse_text(t.as_str().as_bytes(), &json_limits()?)
                    .map_err(|e| {
                        ExecError::eval("22P02", format!("invalid input syntax for type json: {e}"))
                    })?;
                ferrosa_jsonb::JsonbValue::from_encoded(encoded)
                    .map(Value::Jsonb)
                    .map_err(|e| ExecError::eval("22P02", e.to_string()))
            }
            other => Err(cannot_cast(&other, ty)),
        },
        other_ty => {
            if super::literal_type(&v) == Some(other_ty) {
                Ok(v)
            } else {
                Err(cannot_cast(&v, other_ty))
            }
        }
    }
}

/// `YYYY-MM-DD[( |T)HH:MM[:SS[.ffffff]]][Z|+00[:00]]` as microseconds since the
/// Unix epoch. Only a zero UTC offset is accepted; any other offset needs a zone
/// database and is refused (0A000), never silently ignored.
fn parse_timestamp(input: &str) -> R<i64> {
    let bad = || {
        ExecError::eval(
            "22007",
            format!("invalid input syntax for type timestamp: \"{input}\""),
        )
    };
    let s = input.trim();
    let s = s
        .strip_suffix('Z')
        .or_else(|| s.strip_suffix("+00:00"))
        .or_else(|| s.strip_suffix("+00"))
        .unwrap_or(s);
    if s.len() > 10 && (s[10..].contains('+') || s[11..].contains('-')) {
        return Err(ExecError::Unsupported(format!(
            "a timestamp with a non-UTC offset: {input:?}"
        )));
    }
    let (date, time) = match s.split_once([' ', 'T']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let days = parse_date_days(date).ok_or_else(bad)?;
    let micros_of_day = match time {
        None => 0,
        Some(t) => parse_time_micros(t).ok_or_else(bad)?,
    };
    days.checked_mul(86_400_000_000)
        .and_then(|d| d.checked_add(micros_of_day))
        .ok_or_else(bad)
}

fn parse_date_days(d: &str) -> Option<i64> {
    let mut parts = d.splitn(3, '-');
    let y: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let day: i64 = parts.next()?.parse().ok()?;
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let month_len = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=12).contains(&m) || day < 1 || day > month_len[(m - 1) as usize] {
        return None;
    }
    Some(days_from_civil(y, m, day))
}

fn parse_time_micros(t: &str) -> Option<i64> {
    let (hms, frac) = t.split_once('.').unwrap_or((t, ""));
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let sec: i64 = match parts.next() {
        Some(x) => x.parse().ok()?,
        None => 0,
    };
    if parts.next().is_some() || h > 23 || m > 59 || sec > 59 || frac.len() > 6 {
        return None;
    }
    if !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let frac_micros: i64 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<6}").parse().ok()?
    };
    Some(((h * 60 + m) * 60 + sec) * 1_000_000 + frac_micros)
}

/// Howard Hinnant's civil-to-days algorithm, the inverse of `civil_from_days`.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
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
        Value::Jsonb(doc) => {
            let view = doc
                .view()
                .map_err(|e| ExecError::eval("22023", e.to_string()))?;
            ferrosa_jsonb::print_to_string(
                view.root(),
                ferrosa_jsonb::TextStyle::PgText,
                usize::MAX,
            )
            .map_err(|e| ExecError::eval("54000", format!("{e:?}")))?
        }
        Value::Timestamp(micros) => timestamp_text(*micros),
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

/// `YYYY-MM-DD HH:MM:SS[.ffffff]` for microseconds since the Unix epoch (UTC).
fn timestamp_text(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let base = format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        tod / 3600,
        tod % 3600 / 60,
        tod % 60
    );
    if frac == 0 {
        base
    } else {
        format!("{base}.{}", format!("{frac:06}").trim_end_matches('0'))
    }
}

/// Howard Hinnant's days-to-civil algorithm (proleptic Gregorian).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
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
    fn timestamp_text_round_trips_through_parse() {
        for micros in [0, 951_827_696_500_000, -1, 1_577_923_200_000_000] {
            assert_eq!(parse_timestamp(&timestamp_text(micros)), Ok(micros));
        }
        assert_eq!(parse_timestamp("2020-01-02"), Ok(1_577_923_200_000_000));
        assert_eq!(
            parse_timestamp("2020-01-02T00:00:00Z"),
            Ok(1_577_923_200_000_000)
        );
        assert!(parse_timestamp("2021-02-29").is_err(), "not a leap year");
        assert!(matches!(
            parse_timestamp("2020-01-02 00:00:00+05"),
            Err(ExecError::Unsupported(_))
        ));
    }

    #[test]
    fn timestamp_text_is_postgres_iso() {
        assert_eq!(timestamp_text(0), "1970-01-01 00:00:00");
        // 2000-02-29 12:34:56.5 UTC: a leap day, fractional seconds trimmed.
        assert_eq!(timestamp_text(951_827_696_500_000), "2000-02-29 12:34:56.5");
        // Before the epoch.
        assert_eq!(timestamp_text(-1), "1969-12-31 23:59:59.999999");
    }

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

    /// `'{"a": 1}'::jsonb` parses the text (it was refused as an impossible
    /// cast), and malformed JSON is 22P02, never NULL.
    #[test]
    fn text_casts_to_jsonb_by_parsing_it() {
        let v = cast(Value::Text("{\"a\": 1}".into()), ColumnType::Jsonb).unwrap();
        let Value::Jsonb(j) = v else {
            panic!("expected jsonb, got {v:?}")
        };
        assert_eq!(text_of(&Value::Jsonb(j)).unwrap(), "{\"a\": 1}");
        let err = cast(Value::Text("{nope".into()), ColumnType::Jsonb).unwrap_err();
        assert!(
            matches!(
                err,
                ExecError::Eval {
                    sqlstate: "22P02",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn integers_and_numerics_compare_by_value() {
        let one_point_oh = Value::numeric(BigInt::from(10), 1);
        assert_eq!(
            compare(CmpOp::Eq, &Value::Int(1), &one_point_oh),
            Ok(Some(true))
        );
    }
}
