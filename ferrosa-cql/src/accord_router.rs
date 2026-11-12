//! AccordRouter: CQL-layer LWT-on-Accord routing.
//!
//! This module bridges the CQL statement router with the Accord consensus
//! protocol. It implements:
//!
//! - **Routing decisions**: DML statements (INSERT/UPDATE/DELETE/SELECT) go
//!   through Accord in cluster mode; standalone mode bypasses Accord entirely.
//! - **LWT semantics**: `INSERT IF NOT EXISTS`, `UPDATE/DELETE IF condition`,
//!   and batch CAS use read-before-write to evaluate conditions.
//! - **Result sets**: The `[applied]` boolean column follows the Cassandra
//!   wire protocol contract — `true` on success, `false` with current row
//!   values on failure.
//!
//! # Design
//!
//! The `AccordRouter` is a stateless decision layer. It does not own an
//! `AccordStateMachine` directly — in production, the coordinator submits
//! transactions through the Accord protocol. Here we implement the
//! *execute phase* logic that the state machine calls after consensus.

use std::collections::HashMap;

use bytes::BytesMut;

use crate::ast::*;
use crate::error::CqlError;
use crate::result;
use crate::types::{CqlType, CqlValue};
use ferrosa_cluster::consistency::ConsistencyLevel;

// ---------------------------------------------------------------------------
// Routing mode
// ---------------------------------------------------------------------------

/// Deployment topology for routing decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingMode {
    /// Single-node: bypass Accord, execute locally.
    Standalone,
    /// Multi-node cluster: route through Accord for linearizability.
    Cluster,
}

// ---------------------------------------------------------------------------
// Routing decision
// ---------------------------------------------------------------------------

/// Where a statement should be executed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// Execute through the Accord consensus protocol.
    Accord,
    /// Execute locally without Accord (standalone mode).
    Local,
}

/// Determine routing for a DML statement based on deployment mode.
///
/// In cluster mode, only LWT statements (INSERT IF NOT EXISTS,
/// UPDATE/DELETE IF condition) route through Accord for linearizability.
/// Regular DML uses tunable consistency via the WritePath coordinator.
/// In standalone mode, everything is local.
/// Whether a statement carries a conditional (compare-and-set) clause.
///
/// These are the statements CQL calls lightweight transactions: `IF NOT
/// EXISTS`, `IF EXISTS`, and `IF <column> <op> <value>`. Each asks the server
/// to decide atomically whether the mutation applies -- a consensus question
/// the tunable write path cannot answer correctly.
///
/// Deliberately keyed on the statement rather than the request's serial
/// consistency: that field is optional in the protocol, so keying on it makes
/// correctness depend on a driver sending something the server does not
/// require.
fn statement_is_lwt(stmt: &Statement) -> bool {
    match stmt {
        Statement::Insert(s) => s.if_not_exists,
        Statement::Update(s) => s.if_exists || !s.if_conditions.is_empty(),
        Statement::Delete(s) => s.if_exists || !s.if_conditions.is_empty(),
        _ => false,
    }
}

pub fn route_decision(
    mode: RoutingMode,
    stmt: &Statement,
    serial_consistency: Option<ConsistencyLevel>,
) -> RouteDecision {
    if mode == RoutingMode::Standalone {
        return RouteDecision::Local;
    }

    // LWT: decided by the STATEMENT, not by an optional protocol field.
    //
    // This used to read `if serial_consistency.is_some()`, on the stated
    // assumption that "serial_consistency is set by the CQL protocol when the
    // client uses IF NOT EXISTS / IF condition". That is not the protocol.
    // Serial consistency is optional and defaults to SERIAL server-side, and
    // drivers do not send it unless explicitly asked -- so an ordinary
    // `UPDATE ... IF col = val` arrived with `None`, routed to `Local`, and the
    // local write path applied the mutation while ignoring the condition.
    //
    // Verified against the live cluster on 2026-08-22:
    //
    //     seeded v='original'
    //     UPDATE .. SET v='changed' WHERE k=1 IF v='WRONG'  ->  v='changed'
    //
    // A false condition, and the write landed. Every compare-and-set built on
    // this silently degraded to an unconditional write; ferrosa-memory's
    // consolidation lease is one casualty (it claimed 277 requests it believed
    // it had lost, so each was marked leased and then skipped).
    //
    // `IF NOT EXISTS` escaped because the local path implements that one case,
    // which is why the breakage looked selective.
    if statement_is_lwt(stmt) {
        return RouteDecision::Accord;
    }

    // A linearizable READ carries no conditions, so it is still requested the
    // only way CQL allows: by asking for SERIAL consistency.
    if serial_consistency.is_some() {
        return match stmt {
            Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Select(_)
            | Statement::Batch(_) => RouteDecision::Accord,
            _ => RouteDecision::Local,
        };
    }

    // Regular DML: use WritePath (tunable CL, not Accord).
    RouteDecision::Local
}

/// Resolve the *effective* serial consistency that signals a linearizable,
/// Accord-routed operation — the signal [`route_decision`] keys on.
///
/// There is no separate "serial read" verb in CQL: a client requests a
/// linearizable read by issuing the statement at a regular consistency of
/// `SERIAL` / `LOCAL_SERIAL`. LWT writes additionally carry an explicit
/// serial-consistency field. Either is a request for Accord linearizability, so
/// this collapses both into one signal:
///
/// - an explicit serial-consistency flag (`explicit_serial`) always wins;
/// - otherwise a regular consistency of `SERIAL` / `LOCAL_SERIAL` is itself the
///   signal;
/// - any other regular consistency is a non-linearizable (tunable-CL) request.
///
/// Invariant: the result is `Some` iff the request must route through Accord for
/// linearizability. It never fabricates a serial level from a normal CL.
pub fn effective_serial_consistency(
    consistency: ConsistencyLevel,
    explicit_serial: Option<ConsistencyLevel>,
) -> Option<ConsistencyLevel> {
    if let Some(sc) = explicit_serial {
        return Some(sc);
    }
    match consistency {
        ConsistencyLevel::Serial | ConsistencyLevel::LocalSerial => Some(consistency),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// LWT condition evaluation
// ---------------------------------------------------------------------------

/// Result of evaluating an LWT condition against the current row state.
#[derive(Debug, Clone, PartialEq)]
pub struct LwtResult {
    /// Whether the conditional mutation was applied.
    pub applied: bool,
    /// Current row values (column_name -> value). Populated when `applied` is
    /// false, to return the existing row to the client per Cassandra protocol.
    pub current_values: HashMap<String, Option<CqlValue>>,
}

/// Evaluate an `INSERT IF NOT EXISTS` condition.
///
/// Returns `applied=true` if the row does not exist (None), `applied=false`
/// with the existing row values if it does.
pub fn eval_insert_if_not_exists(
    existing_row: Option<HashMap<String, Option<CqlValue>>>,
) -> LwtResult {
    match existing_row {
        None => LwtResult {
            applied: true,
            current_values: HashMap::new(),
        },
        // Move the row into the result — the caller no longer needs it, so this
        // is a pointer move, not a clone of every cell.
        Some(row) => LwtResult {
            applied: false,
            current_values: row,
        },
    }
}

/// The test one `IF` condition applies, with its operand already converted to
/// the condition column's declared type. `None` is CQL `null`.
#[derive(Debug, Clone, PartialEq)]
pub enum ConditionTest {
    Eq(Option<CqlValue>),
    NotEq(Option<CqlValue>),
    Lt(Option<CqlValue>),
    Gt(Option<CqlValue>),
    LtEq(Option<CqlValue>),
    GtEq(Option<CqlValue>),
    /// `IN (...)`: each element typed as the column.
    In(Vec<Option<CqlValue>>),
}

/// One `IF` condition whose operand carries the column's declared type.
///
/// The parser hands the evaluator an untyped [`Term`]: a bound timestamp has
/// already become an integer literal, a bound `date` a blob literal, and an
/// unprepared `'2024-01-15'` a string. Only the column's declared type says
/// what any of those mean, so the operand is converted against it once, up
/// front, and the comparison is then between two values of the SAME type.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedIfCondition {
    pub column: String,
    pub test: ConditionTest,
}

/// Convert each condition's operand to its column's declared type.
///
/// `column_type` answers the declared type of a column, `Ok(None)` when the
/// table has no such column.
///
/// # Errors
///
/// `CqlError::Invalid` for a condition on an undefined column, for an operand
/// that is not a value of the column's type, and for an `IN` whose operand is
/// not a list. A condition that cannot be evaluated is refused, never reported
/// as `[applied]=false`: that answer means "the row did not match", which a
/// caller acts on.
pub fn type_if_conditions<F>(
    conditions: &[IfCondition],
    column_type: F,
) -> Result<Vec<TypedIfCondition>, CqlError>
where
    F: Fn(&str) -> Result<Option<CqlType>, CqlError>,
{
    conditions
        .iter()
        .map(|cond| {
            let ty = column_type(&cond.column)?.ok_or_else(|| {
                CqlError::Invalid(format!(
                    "Undefined column name {} in IF condition",
                    cond.column
                ))
            })?;
            let value = |term: &Term| typed_operand(term, &ty);
            let test = match (&cond.operator, &cond.value) {
                (IfOperator::Eq, term) => ConditionTest::Eq(value(term)?),
                (IfOperator::NotEq, term) => ConditionTest::NotEq(value(term)?),
                (IfOperator::Lt, term) => ConditionTest::Lt(value(term)?),
                (IfOperator::Gt, term) => ConditionTest::Gt(value(term)?),
                (IfOperator::LtEq, term) => ConditionTest::LtEq(value(term)?),
                (IfOperator::GtEq, term) => ConditionTest::GtEq(value(term)?),
                (IfOperator::In, Term::InList(items) | Term::ListLiteral(items)) => {
                    ConditionTest::In(items.iter().map(value).collect::<Result<_, _>>()?)
                }
                (IfOperator::In, other) => {
                    return Err(CqlError::Invalid(format!(
                        "IF {} IN expects a list of values, got {other:?}",
                        cond.column
                    )))
                }
            };
            Ok(TypedIfCondition {
                column: cond.column.clone(),
                test,
            })
        })
        .collect()
}

/// One operand as a value of `ty`; CQL `null` is `None`, as in a decoded row.
fn typed_operand(term: &Term, ty: &CqlType) -> Result<Option<CqlValue>, CqlError> {
    match crate::bridge::term_to_cql_value(term, ty)? {
        CqlValue::Null => Ok(None),
        value => Ok(Some(value)),
    }
}

/// An LWT statement's predicate with every operand typed, ready to evaluate
/// against the row read at the statement's timestamp.
#[derive(Debug, Clone, PartialEq)]
pub enum TypedLwtPredicate {
    /// `INSERT ... IF NOT EXISTS`.
    NotExists,
    /// `UPDATE`/`DELETE ... IF EXISTS` and/or `IF <conditions>`.
    Conditions {
        if_exists: bool,
        conditions: Vec<TypedIfCondition>,
    },
}

impl TypedLwtPredicate {
    /// The typed predicate `stmt` carries, or `None` when it is not an LWT.
    ///
    /// # Errors
    ///
    /// As [`type_if_conditions`].
    pub fn for_statement<F>(stmt: &Statement, column_type: F) -> Result<Option<Self>, CqlError>
    where
        F: Fn(&str) -> Result<Option<CqlType>, CqlError>,
    {
        let (if_exists, conditions) = match stmt {
            Statement::Insert(s) if s.if_not_exists => return Ok(Some(Self::NotExists)),
            Statement::Update(s) if s.if_exists || !s.if_conditions.is_empty() => {
                (s.if_exists, &s.if_conditions)
            }
            Statement::Delete(s) if s.if_exists || !s.if_conditions.is_empty() => {
                (s.if_exists, &s.if_conditions)
            }
            _ => return Ok(None),
        };
        Ok(Some(Self::Conditions {
            if_exists,
            conditions: type_if_conditions(conditions, column_type)?,
        }))
    }

    /// Evaluate against the row at `t` (`None` when the row is absent), with
    /// the canonical [`eval_insert_if_not_exists`] / [`eval_if_conditions`].
    pub fn eval(&self, existing_row: Option<HashMap<String, Option<CqlValue>>>) -> LwtResult {
        match self {
            Self::NotExists => eval_insert_if_not_exists(existing_row),
            Self::Conditions {
                if_exists,
                conditions,
            } => eval_if_conditions(conditions, *if_exists, existing_row),
        }
    }
}

/// Evaluate IF conditions on an UPDATE or DELETE.
///
/// Each condition is checked against the current row values. All
/// conditions must be satisfied for the mutation to be applied. Operands are
/// already typed as their columns (see [`type_if_conditions`]), so every
/// comparison is between two values of the column's declared type.
///
/// If `if_exists` is true and the row is `None`, returns `applied=false`.
///
/// `existing_row` is consumed: on a non-match the row is MOVED into
/// `current_values` (no per-cell clone), which is the whole point of the owned
/// signature.
///
/// Returns `applied=true` if all conditions match, `applied=false` with
/// current values otherwise.
pub fn eval_if_conditions(
    conditions: &[TypedIfCondition],
    if_exists: bool,
    existing_row: Option<HashMap<String, Option<CqlValue>>>,
) -> LwtResult {
    // IF EXISTS check: row must exist.
    let row = match existing_row {
        None => {
            if if_exists || !conditions.is_empty() {
                return LwtResult {
                    applied: false,
                    current_values: HashMap::new(),
                };
            }
            // No conditions and no IF EXISTS — unconditional, always applied.
            return LwtResult {
                applied: true,
                current_values: HashMap::new(),
            };
        }
        Some(r) => r,
    };

    // Evaluate each condition against the row.
    for cond in conditions {
        let current = row.get(&cond.column).and_then(Option::as_ref);
        let matches = condition_holds(&cond.test, current);

        if !matches {
            // Move the row into the result rather than cloning every cell.
            return LwtResult {
                applied: false,
                current_values: row,
            };
        }
    }

    LwtResult {
        applied: true,
        current_values: HashMap::new(),
    }
}

// ---------------------------------------------------------------------------
// LWT predicate classification + statement-level evaluation
// ---------------------------------------------------------------------------

/// How an LWT statement's IF clause must be evaluated against the row at `t`.
///
/// This mirrors the replica-side `ReadPredicate` but stays in the CQL layer
/// where the predicate operators (`IfCondition`/`Term`/`CqlValue`) are defined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LwtPredicateKind {
    /// `INSERT IF NOT EXISTS`: condition holds iff the row is absent at `t`.
    /// The replica answers this via its existence path (no schema needed).
    NotExists,
    /// Generic `IF <conditions>` / `IF EXISTS` on UPDATE/DELETE (or an INSERT
    /// with explicit IF conditions): the coordinator reads the row at `t` and
    /// evaluates the predicate with [`eval_if_conditions`].
    Generic,
}

/// Classify the LWT predicate an INSERT/UPDATE/DELETE carries.
///
/// Returns `None` for statements with no conditional clause (not an LWT — must
/// not route through the read-vote path).
pub fn classify_lwt(stmt: &Statement) -> Option<LwtPredicateKind> {
    match stmt {
        Statement::Insert(s) if s.if_not_exists => Some(LwtPredicateKind::NotExists),
        Statement::Update(s) if s.if_exists || !s.if_conditions.is_empty() => {
            Some(LwtPredicateKind::Generic)
        }
        Statement::Delete(s) if s.if_exists || !s.if_conditions.is_empty() => {
            Some(LwtPredicateKind::Generic)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// LWT result set encoding
// ---------------------------------------------------------------------------

/// Encode an LWT result set with the `[applied]` column.
///
/// When `applied` is true, returns a single row: `[[applied]=true]`.
/// When `applied` is false, returns a single row with `[applied]=false`
/// followed by the current column values.
///
/// `condition_columns` lists the column names (and types) that should appear
/// in the result set alongside `[applied]`, in order. These are the columns
/// referenced in the IF conditions or all columns for IF NOT EXISTS.
pub fn encode_lwt_result(
    lwt: &LwtResult,
    keyspace: &str,
    table: &str,
    condition_columns: &[(String, CqlType)],
) -> BytesMut {
    let mut col_names = vec!["[applied]".to_string()];
    let mut col_types = vec![CqlType::Boolean];

    if !lwt.applied {
        for (name, cql_type) in condition_columns {
            col_names.push(name.clone());
            col_types.push(cql_type.clone());
        }
    }

    let mut row_values: Vec<Option<CqlValue>> = vec![Some(CqlValue::Boolean(lwt.applied))];

    if !lwt.applied {
        for (name, _) in condition_columns {
            let val = lwt.current_values.get(name).cloned().flatten();
            row_values.push(val);
        }
    }

    result::encode_rows(&col_names, &col_types, keyspace, table, &[row_values])
}

// ---------------------------------------------------------------------------
// Batch CAS
// ---------------------------------------------------------------------------

/// Per-statement result in a batch CAS operation.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchCasResult {
    /// Whether this individual statement's condition was satisfied.
    pub applied: bool,
    /// Current row values when not applied.
    pub current_values: HashMap<String, Option<CqlValue>>,
}

/// Evaluate a batch of statements with CAS conditions.
///
/// All conditions across all statements must be satisfied for the batch to
/// be applied atomically. If any condition fails, none of the mutations
/// are applied and per-row results are returned.
///
/// `predicates[i]` is statement `i`'s typed predicate
/// ([`TypedLwtPredicate::for_statement`]), `None` for a statement without
/// conditions.
pub fn eval_batch_cas(
    predicates: &[Option<TypedLwtPredicate>],
    row_states: &[Option<HashMap<String, Option<CqlValue>>>],
) -> (bool, Vec<BatchCasResult>) {
    assert_eq!(
        predicates.len(),
        row_states.len(),
        "statement count must match row state count"
    );

    let mut results = Vec::with_capacity(predicates.len());
    let mut all_applied = true;

    for (predicate, existing_row) in predicates.iter().zip(row_states.iter()) {
        let lwt = match predicate {
            Some(predicate) => predicate.eval(existing_row.clone()),
            // Statements without conditions are always "applied" individually.
            None => LwtResult {
                applied: true,
                current_values: HashMap::new(),
            },
        };

        if !lwt.applied {
            all_applied = false;
        }

        results.push(BatchCasResult {
            applied: lwt.applied,
            current_values: lwt.current_values,
        });
    }

    (all_applied, results)
}

/// Encode batch CAS results.
///
/// When all applied: single row with `[applied]=true`.
/// When any failed: one row per statement with `[applied]` and current values
/// for the failing rows.
pub fn encode_batch_cas_result(
    all_applied: bool,
    results: &[BatchCasResult],
    keyspace: &str,
    table: &str,
    condition_columns: &[(String, CqlType)],
) -> BytesMut {
    if all_applied {
        let col_names = vec!["[applied]".to_string()];
        let col_types = vec![CqlType::Boolean];
        let row = vec![Some(CqlValue::Boolean(true))];
        return result::encode_rows(&col_names, &col_types, keyspace, table, &[row]);
    }

    // Failure: emit per-row results.
    let mut col_names = vec!["[applied]".to_string()];
    let mut col_types = vec![CqlType::Boolean];
    for (name, cql_type) in condition_columns {
        col_names.push(name.clone());
        col_types.push(cql_type.clone());
    }

    let mut rows = Vec::with_capacity(results.len());
    for r in results {
        let mut row_values: Vec<Option<CqlValue>> = vec![Some(CqlValue::Boolean(r.applied))];
        for (name, _) in condition_columns {
            let val = r.current_values.get(name).cloned().flatten();
            row_values.push(val);
        }
        rows.push(row_values);
    }

    result::encode_rows(&col_names, &col_types, keyspace, table, &rows)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Whether the row's `current` value passes one typed condition.
///
/// CQL null semantics: `=` and `IN` match a null only against a null, `!=`
/// holds whenever the two differ, and an ordering operator never holds with a
/// null on either side.
fn condition_holds(test: &ConditionTest, current: Option<&CqlValue>) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let order = |operand: &Option<CqlValue>| cmp_values(current, operand.as_ref());
    match test {
        ConditionTest::Eq(operand) => values_eq(current, operand.as_ref()),
        ConditionTest::NotEq(operand) => !values_eq(current, operand.as_ref()),
        ConditionTest::Lt(operand) => order(operand) == Some(Less),
        ConditionTest::Gt(operand) => order(operand) == Some(Greater),
        ConditionTest::LtEq(operand) => matches!(order(operand), Some(Less | Equal)),
        ConditionTest::GtEq(operand) => matches!(order(operand), Some(Greater | Equal)),
        ConditionTest::In(items) => items.iter().any(|item| values_eq(current, item.as_ref())),
    }
}

/// Equality for LWT `IF` conditions, between two values of one column type.
///
/// A `decimal` is equal by numeric value (`1.5` = `1.50`), as Cassandra's
/// `DecimalType` compares; every other type is equal exactly when its encoded
/// value is.
fn values_eq(a: Option<&CqlValue>, b: Option<&CqlValue>) -> bool {
    match (a, b) {
        (
            Some(CqlValue::Decimal {
                scale: sa,
                unscaled: ua,
            }),
            Some(CqlValue::Decimal {
                scale: sb,
                unscaled: ub,
            }),
        ) => decimal_cmp(*sa, ua, *sb, ub) == std::cmp::Ordering::Equal,
        _ => a == b,
    }
}

/// Order two values of one column type, as the column's CQL type orders them.
///
/// Returns `None` when they are not orderable: a null on either side, a NaN,
/// a type CQL does not order in a condition (`duration`, collections, tuples,
/// UDTs, vectors), or two different types — which typed operands rule out.
fn cmp_values(a: Option<&CqlValue>, b: Option<&CqlValue>) -> Option<std::cmp::Ordering> {
    let (Some(a), Some(b)) = (a, b) else {
        return None;
    };
    match (a, b) {
        (CqlValue::Int(x), CqlValue::Int(y)) => Some(x.cmp(y)),
        (CqlValue::Bigint(x), CqlValue::Bigint(y))
        | (CqlValue::Counter(x), CqlValue::Counter(y))
        | (CqlValue::Timestamp(x), CqlValue::Timestamp(y))
        | (CqlValue::Time(x), CqlValue::Time(y)) => Some(x.cmp(y)),
        (CqlValue::Smallint(x), CqlValue::Smallint(y)) => Some(x.cmp(y)),
        (CqlValue::Tinyint(x), CqlValue::Tinyint(y)) => Some(x.cmp(y)),
        (CqlValue::Varint(x), CqlValue::Varint(y)) => Some(x.cmp(y)),
        (CqlValue::Date(x), CqlValue::Date(y)) => Some(x.cmp(y)),
        (CqlValue::Boolean(x), CqlValue::Boolean(y)) => Some(x.cmp(y)),
        (CqlValue::Text(x), CqlValue::Text(y)) | (CqlValue::Ascii(x), CqlValue::Ascii(y)) => {
            Some(x.cmp(y))
        }
        (CqlValue::Blob(x), CqlValue::Blob(y)) => Some(x.cmp(y)),
        (CqlValue::Double(x), CqlValue::Double(y)) => {
            f64::from_bits(*x).partial_cmp(&f64::from_bits(*y))
        }
        (CqlValue::Float(x), CqlValue::Float(y)) => {
            f32::from_bits(*x).partial_cmp(&f32::from_bits(*y))
        }
        (
            CqlValue::Decimal {
                scale: sa,
                unscaled: ua,
            },
            CqlValue::Decimal {
                scale: sb,
                unscaled: ub,
            },
        ) => Some(decimal_cmp(*sa, ua, *sb, ub)),
        (CqlValue::Timeuuid(x), CqlValue::Timeuuid(y)) => Some(timeuuid_cmp(x, y)),
        (CqlValue::Uuid(x), CqlValue::Uuid(y)) => Some(uuid_cmp(x, y)),
        (CqlValue::Inet(x), CqlValue::Inet(y)) => Some(inet_octets(x).cmp(&inet_octets(y))),
        // jsonb orders by the D18 total order.
        (CqlValue::Jsonb(x), CqlValue::Jsonb(y)) => Some(x.cmp(y)),
        (CqlValue::Null, _)
        | (CqlValue::Ascii(_), _)
        | (CqlValue::Bigint(_), _)
        | (CqlValue::Blob(_), _)
        | (CqlValue::Boolean(_), _)
        | (CqlValue::Counter(_), _)
        | (CqlValue::Decimal { .. }, _)
        | (CqlValue::Double(_), _)
        | (CqlValue::Float(_), _)
        | (CqlValue::Int(_), _)
        | (CqlValue::Timestamp(_), _)
        | (CqlValue::Uuid(_), _)
        | (CqlValue::Text(_), _)
        | (CqlValue::Varint(_), _)
        | (CqlValue::Timeuuid(_), _)
        | (CqlValue::Inet(_), _)
        | (CqlValue::Date(_), _)
        | (CqlValue::Time(_), _)
        | (CqlValue::Smallint(_), _)
        | (CqlValue::Tinyint(_), _)
        | (CqlValue::Duration { .. }, _)
        | (CqlValue::List(_), _)
        | (CqlValue::Set(_), _)
        | (CqlValue::Map(_), _)
        | (CqlValue::Tuple(_), _)
        | (CqlValue::Vector(_), _)
        | (CqlValue::Udt(_), _)
        | (CqlValue::Jsonb(_), _) => None,
    }
}

/// Compare `ua * 10^-sa` with `ub * 10^-sb` by numeric value.
///
/// Signs decide first; then the adjusted exponent (digit count minus scale)
/// decides without rescaling. Only when the adjusted exponents are equal is one
/// side rescaled, and then the scale gap equals the digit-count gap, so the
/// multiplication is bounded by the operands' own size rather than by an
/// arbitrary `i32` scale.
fn decimal_cmp(
    sa: i32,
    ua: &num_bigint::BigInt,
    sb: i32,
    ub: &num_bigint::BigInt,
) -> std::cmp::Ordering {
    use num_bigint::Sign;
    use std::cmp::Ordering;

    let sign_order = ua.sign().cmp(&ub.sign());
    if sign_order != Ordering::Equal || ua.sign() == Sign::NoSign {
        return sign_order;
    }
    let digits = |u: &num_bigint::BigInt| u.magnitude().to_str_radix(10).len() as i64;
    let adjusted = |u: &num_bigint::BigInt, s: i32| digits(u) - i64::from(s);
    let magnitude_order = match adjusted(ua, sa).cmp(&adjusted(ub, sb)) {
        Ordering::Equal => {
            let ten = num_bigint::BigUint::from(10u32);
            let (ma, mb) = (ua.magnitude(), ub.magnitude());
            match sa.cmp(&sb) {
                Ordering::Equal => ma.cmp(mb),
                Ordering::Greater => ma.cmp(&(mb * ten.pow(sa.abs_diff(sb)))),
                Ordering::Less => (ma * ten.pow(sa.abs_diff(sb))).cmp(mb),
            }
        }
        unequal => unequal,
    };
    match ua.sign() {
        Sign::Minus => magnitude_order.reverse(),
        Sign::NoSign | Sign::Plus => magnitude_order,
    }
}

/// The 60-bit timestamp of a version-1 uuid.
fn uuid_v1_timestamp(u: &uuid::Uuid) -> u64 {
    let (time_low, time_mid, time_hi_and_version, _) = u.as_fields();
    (u64::from(time_hi_and_version & 0x0fff) << 48)
        | (u64::from(time_mid) << 32)
        | u64::from(time_low)
}

/// Order two timeuuids as Cassandra's `TimeUUIDType` does: by embedded time,
/// then by the clock-sequence-and-node bytes compared as signed bytes.
fn timeuuid_cmp(a: &uuid::Uuid, b: &uuid::Uuid) -> std::cmp::Ordering {
    let signed = |u: &uuid::Uuid| u.as_fields().3.map(|byte| byte as i8);
    uuid_v1_timestamp(a)
        .cmp(&uuid_v1_timestamp(b))
        .then_with(|| signed(a).cmp(&signed(b)))
}

/// Order two uuids as Cassandra's `UUIDType` does: by version, then by
/// embedded time when both are version 1, then by their bytes.
fn uuid_cmp(a: &uuid::Uuid, b: &uuid::Uuid) -> std::cmp::Ordering {
    let by_version = a.get_version_num().cmp(&b.get_version_num());
    let by_time = || {
        if a.get_version_num() == 1 {
            uuid_v1_timestamp(a).cmp(&uuid_v1_timestamp(b))
        } else {
            std::cmp::Ordering::Equal
        }
    };
    by_version
        .then_with(by_time)
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

/// An address's network-order bytes: `inet` orders by these, not by its text.
fn inet_octets(addr: &std::net::IpAddr) -> Vec<u8> {
    match addr {
        std::net::IpAddr::V4(v4) => v4.octets().to_vec(),
        std::net::IpAddr::V6(v6) => v6.octets().to_vec(),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {

    /// A conditional statement is an LWT whether or not the client asked for
    /// SERIAL consistency.
    ///
    /// `route_decision` keyed the whole decision on
    /// `serial_consistency.is_some()`, justified by the comment
    /// "serial_consistency is set by the CQL protocol when the client uses IF
    /// NOT EXISTS / IF condition". That is not so: serial consistency is an
    /// OPTIONAL protocol field defaulting to SERIAL server-side, and drivers do
    /// not send it unless asked. So `UPDATE ... IF col = val` from an ordinary
    /// client routed to `Local`, and the local write path applies the mutation
    /// while ignoring the conditions entirely.
    ///
    /// Measured on the live cluster 2026-08-22, on a scratch table:
    ///
    /// ```text
    /// seeded v='original'
    /// UPDATE .. SET v='changed' WHERE k=1 IF v='WRONG'   ->  v='changed'
    /// ```
    ///
    /// The condition was false and the write applied anyway. Downstream that
    /// silently broke every compare-and-set built on it: ferrosa-memory's
    /// consolidation lease claimed 277 requests it believed it had lost, marked
    /// each leased and skipped it, and consolidation never ran.
    ///
    /// `IF NOT EXISTS` is unaffected -- the local path implements that one --
    /// which is why the failure looked selective rather than total.
    #[test]
    fn a_conditional_update_is_an_lwt_without_explicit_serial_consistency() {
        let stmt = Statement::Update(UpdateStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            assignments: vec![Assignment::Simple {
                column: "v".into(),
                value: Term::IntegerLiteral(42),
            }],
            where_clauses: vec![WhereClause {
                column: "id".into(),
                op: ComparisonOp::Eq,
                value: Term::IntegerLiteral(1),
                token_fn: false,
            }],
            if_exists: false,
            if_conditions: vec![IfCondition {
                column: "state".into(),
                operator: IfOperator::Eq,
                value: Term::StringLiteral("pending".into()),
            }],
            using_timestamp: None,
            using_ttl: None,
        });

        assert_eq!(
            route_decision(RoutingMode::Cluster, &stmt, None),
            RouteDecision::Accord,
            "a statement carrying IF conditions must route as an LWT even when \
             the client sent no serial consistency; routing it Local applies the \
             mutation and drops the condition"
        );
    }

    /// The same for `IF EXISTS`, which is equally a compare-and-set.
    #[test]
    fn a_conditional_delete_is_an_lwt_without_explicit_serial_consistency() {
        let stmt = Statement::Delete(DeleteStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            columns: vec![],
            where_clauses: vec![WhereClause {
                column: "id".into(),
                op: ComparisonOp::Eq,
                value: Term::IntegerLiteral(1),
                token_fn: false,
            }],
            if_exists: true,
            if_conditions: vec![],
            using_timestamp: None,
        });

        assert_eq!(
            route_decision(RoutingMode::Cluster, &stmt, None),
            RouteDecision::Accord,
            "IF EXISTS is a condition too"
        );
    }

    /// An unconditional write is unchanged: it must still take the tunable
    /// WritePath, not consensus. Routing every write through Accord would be a
    /// large and unintended performance change.
    #[test]
    fn an_unconditional_write_still_routes_local() {
        let stmt = Statement::Update(UpdateStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            assignments: vec![Assignment::Simple {
                column: "v".into(),
                value: Term::IntegerLiteral(42),
            }],
            where_clauses: vec![WhereClause {
                column: "id".into(),
                op: ComparisonOp::Eq,
                value: Term::IntegerLiteral(1),
                token_fn: false,
            }],
            if_exists: false,
            if_conditions: vec![],
            using_timestamp: None,
            using_ttl: None,
        });

        assert_eq!(
            route_decision(RoutingMode::Cluster, &stmt, None),
            RouteDecision::Local
        );
    }
    use super::*;

    // -----------------------------------------------------------------------
    // A3.3: CQL Router to AccordCoordinator
    // -----------------------------------------------------------------------

    #[test]
    fn cql_route_through_accord() {
        // INSERT/UPDATE/DELETE route through Accord in cluster mode.
        let insert = Statement::Insert(InsertStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            columns: vec!["id".into()],
            values: vec![Term::IntegerLiteral(1)],
            if_not_exists: false,
            using_timestamp: None,
            using_ttl: None,
        });
        let update = Statement::Update(UpdateStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            assignments: vec![Assignment::Simple {
                column: "v".into(),
                value: Term::IntegerLiteral(42),
            }],
            where_clauses: vec![WhereClause {
                column: "id".into(),
                op: ComparisonOp::Eq,
                value: Term::IntegerLiteral(1),
                token_fn: false,
            }],
            if_exists: false,
            if_conditions: vec![],
            using_timestamp: None,
            using_ttl: None,
        });
        let delete = Statement::Delete(DeleteStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            columns: vec![],
            where_clauses: vec![WhereClause {
                column: "id".into(),
                op: ComparisonOp::Eq,
                value: Term::IntegerLiteral(1),
                token_fn: false,
            }],
            if_exists: false,
            if_conditions: vec![],
            using_timestamp: None,
        });

        assert_eq!(
            route_decision(
                RoutingMode::Cluster,
                &insert,
                Some(ConsistencyLevel::Serial)
            ),
            RouteDecision::Accord,
            "INSERT must route through Accord in cluster mode"
        );
        assert_eq!(
            route_decision(
                RoutingMode::Cluster,
                &update,
                Some(ConsistencyLevel::Serial)
            ),
            RouteDecision::Accord,
            "UPDATE must route through Accord in cluster mode"
        );
        assert_eq!(
            route_decision(
                RoutingMode::Cluster,
                &delete,
                Some(ConsistencyLevel::Serial)
            ),
            RouteDecision::Accord,
            "DELETE must route through Accord in cluster mode"
        );
    }

    #[test]
    fn cql_route_select_through_accord() {
        // SELECT uses the linearizable read path through Accord in cluster mode.
        let select = Statement::Select(SelectStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            columns: vec![SelectColumn::Star],
            distinct: false,
            where_clauses: vec![WhereClause {
                column: "id".into(),
                op: ComparisonOp::Eq,
                value: Term::IntegerLiteral(1),
                token_fn: false,
            }],
            order_by: vec![],
            limit: None,
            allow_filtering: false,
            ann_of: None,
            geo_nearest: None,
            geo_predicates: vec![],
        });

        assert_eq!(
            route_decision(
                RoutingMode::Cluster,
                &select,
                Some(ConsistencyLevel::Serial)
            ),
            RouteDecision::Accord,
            "SELECT must use linearizable read path through Accord in cluster mode"
        );
    }

    #[test]
    fn cql_route_standalone_bypasses_accord() {
        // All DML bypasses Accord in standalone mode.
        let insert = Statement::Insert(InsertStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            columns: vec!["id".into()],
            values: vec![Term::IntegerLiteral(1)],
            if_not_exists: false,
            using_timestamp: None,
            using_ttl: None,
        });
        let select = Statement::Select(SelectStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            columns: vec![SelectColumn::Star],
            distinct: false,
            where_clauses: vec![],
            order_by: vec![],
            limit: None,
            allow_filtering: false,
            ann_of: None,
            geo_nearest: None,
            geo_predicates: vec![],
        });

        assert_eq!(
            route_decision(RoutingMode::Standalone, &insert, None),
            RouteDecision::Local,
            "INSERT must bypass Accord in standalone mode"
        );
        assert_eq!(
            route_decision(RoutingMode::Standalone, &select, None),
            RouteDecision::Local,
            "SELECT must bypass Accord in standalone mode"
        );

        // DDL never goes through Accord even in cluster mode.
        let ddl = Statement::CreateKeyspace(CreateKeyspaceStatement {
            name: "ks".into(),
            if_not_exists: false,
            replication: vec![],
            durable_writes: None,
        });
        assert_eq!(
            route_decision(RoutingMode::Cluster, &ddl, Some(ConsistencyLevel::Serial)),
            RouteDecision::Local,
            "DDL must not route through Accord even in cluster mode"
        );
    }

    #[test]
    fn regular_serial_consistency_is_the_linearizable_read_signal() {
        // A CQL client requests a linearizable read by issuing the SELECT at a
        // regular consistency of SERIAL / LOCAL_SERIAL (there is no separate
        // "serial read" verb). ferrosa must treat that regular consistency as the
        // effective serial-consistency signal so the read routes through Accord.
        assert_eq!(
            effective_serial_consistency(ConsistencyLevel::Serial, None),
            Some(ConsistencyLevel::Serial),
            "regular SERIAL is a linearizable-read signal"
        );
        assert_eq!(
            effective_serial_consistency(ConsistencyLevel::LocalSerial, None),
            Some(ConsistencyLevel::LocalSerial),
            "regular LOCAL_SERIAL is a linearizable-read signal"
        );
    }

    #[test]
    fn non_serial_regular_consistency_is_not_linearizable() {
        // A normal read/write consistency does NOT request linearizability.
        assert_eq!(
            effective_serial_consistency(ConsistencyLevel::Quorum, None),
            None
        );
        assert_eq!(
            effective_serial_consistency(ConsistencyLevel::One, None),
            None
        );
    }

    #[test]
    fn explicit_serial_consistency_flag_passes_through() {
        // An LWT write carries an explicit serial_consistency alongside a normal
        // regular consistency; that explicit signal must win.
        assert_eq!(
            effective_serial_consistency(ConsistencyLevel::One, Some(ConsistencyLevel::Serial)),
            Some(ConsistencyLevel::Serial),
            "explicit serial_consistency takes precedence over a non-serial regular CL"
        );
    }

    #[test]
    fn serial_select_routes_through_accord_end_to_end() {
        // The whole point: a SELECT issued at regular SERIAL consistency routes to
        // Accord for a linearizable read.
        let select = Statement::Select(SelectStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            columns: vec![SelectColumn::Star],
            distinct: false,
            where_clauses: vec![WhereClause {
                column: "id".into(),
                op: ComparisonOp::Eq,
                value: Term::IntegerLiteral(1),
                token_fn: false,
            }],
            order_by: vec![],
            limit: None,
            allow_filtering: false,
            ann_of: None,
            geo_nearest: None,
            geo_predicates: vec![],
        });
        let serial = effective_serial_consistency(ConsistencyLevel::Serial, None);
        assert_eq!(
            route_decision(RoutingMode::Cluster, &select, serial),
            RouteDecision::Accord,
            "SELECT at regular SERIAL must route through Accord"
        );
    }

    #[test]
    fn cql_route_batch_through_accord() {
        // BATCH routes through Accord in cluster mode.
        let batch = Statement::Batch(BatchStatement {
            batch_type: BatchType::Logged,
            statements: vec![
                Statement::Insert(InsertStatement {
                    keyspace: Some("ks".into()),
                    table: "t".into(),
                    columns: vec!["id".into()],
                    values: vec![Term::IntegerLiteral(1)],
                    if_not_exists: true,
                    using_timestamp: None,
                    using_ttl: None,
                }),
                Statement::Update(UpdateStatement {
                    keyspace: Some("ks".into()),
                    table: "t".into(),
                    assignments: vec![Assignment::Simple {
                        column: "v".into(),
                        value: Term::IntegerLiteral(99),
                    }],
                    where_clauses: vec![WhereClause {
                        column: "id".into(),
                        op: ComparisonOp::Eq,
                        value: Term::IntegerLiteral(2),
                        token_fn: false,
                    }],
                    if_exists: false,
                    if_conditions: vec![],
                    using_timestamp: None,
                    using_ttl: None,
                }),
            ],
            using_timestamp: None,
        });

        assert_eq!(
            route_decision(RoutingMode::Cluster, &batch, Some(ConsistencyLevel::Serial)),
            RouteDecision::Accord,
            "BATCH must route through Accord in cluster mode"
        );
    }

    // -----------------------------------------------------------------------
    // A3.4: LWT INSERT IF NOT EXISTS
    // -----------------------------------------------------------------------

    #[test]
    fn lwt_insert_if_not_exists() {
        // Row does not exist: [applied]=true.
        let result = eval_insert_if_not_exists(None);
        assert!(result.applied, "INSERT IF NOT EXISTS on new row must apply");
        assert!(
            result.current_values.is_empty(),
            "no current values when applied"
        );

        // Row exists: [applied]=false, returns current values.
        let mut existing = HashMap::new();
        existing.insert("id".to_string(), Some(CqlValue::Int(1)));
        existing.insert("name".to_string(), Some(CqlValue::Text("Alice".into())));

        let result = eval_insert_if_not_exists(Some(existing.clone()));
        assert!(
            !result.applied,
            "INSERT IF NOT EXISTS on existing row must not apply"
        );
        assert_eq!(
            result.current_values.get("id"),
            Some(&Some(CqlValue::Int(1))),
            "must return current id value"
        );
        assert_eq!(
            result.current_values.get("name"),
            Some(&Some(CqlValue::Text("Alice".into()))),
            "must return current name value"
        );
    }

    #[test]
    fn lwt_result_set_format() {
        // Applied: result set has only [applied]=true.
        let applied = LwtResult {
            applied: true,
            current_values: HashMap::new(),
        };
        let buf = encode_lwt_result(&applied, "ks", "t", &[]);
        // Verify it starts with the Rows kind (0x0002).
        assert_eq!(
            &buf[0..4],
            &[0x00, 0x00, 0x00, 0x02],
            "must be a Rows result"
        );

        // Not applied: result set has [applied]=false + condition columns.
        let mut current = HashMap::new();
        current.insert("v".to_string(), Some(CqlValue::Int(42)));
        let not_applied = LwtResult {
            applied: false,
            current_values: current,
        };
        let condition_cols = vec![("v".to_string(), CqlType::Int)];
        let buf = encode_lwt_result(&not_applied, "ks", "t", &condition_cols);
        assert_eq!(
            &buf[0..4],
            &[0x00, 0x00, 0x00, 0x02],
            "must be a Rows result"
        );
        // The buffer should be larger than the applied case (has extra columns).
        assert!(
            buf.len() > 20,
            "not-applied result must include condition column data"
        );
    }

    // -----------------------------------------------------------------------
    // A3.5: LWT IF Conditions on UPDATE/DELETE
    // -----------------------------------------------------------------------

    /// A column-type lookup over a fixed `(name, type)` schema.
    fn schema_of<'a>(
        columns: &'a [(&'a str, CqlType)],
    ) -> impl Fn(&str) -> Result<Option<CqlType>, CqlError> + 'a {
        move |name| {
            Ok(columns
                .iter()
                .find(|(column, _)| *column == name)
                .map(|(_, ty)| ty.clone()))
        }
    }

    /// Type `conditions` against `columns` and evaluate them against `row`.
    fn eval(
        conditions: &[IfCondition],
        columns: &[(&str, CqlType)],
        if_exists: bool,
        row: Option<HashMap<String, Option<CqlValue>>>,
    ) -> LwtResult {
        let typed = type_if_conditions(conditions, schema_of(columns))
            .expect("every test condition types against its column");
        eval_if_conditions(&typed, if_exists, row)
    }

    /// `v int`, the column most of these tests condition on.
    const V_INT: &[(&str, CqlType)] = &[("v", CqlType::Int)];
    /// `v bigint`.
    const V_BIGINT: &[(&str, CqlType)] = &[("v", CqlType::Bigint)];

    #[test]
    fn lwt_update_if_condition() {
        // Condition matches: applied=true.
        let mut row = HashMap::new();
        row.insert("v".to_string(), Some(CqlValue::Int(10)));

        let conditions = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::Eq,
            value: Term::IntegerLiteral(10),
        }];

        let result = eval(&conditions, V_INT, false, Some(row.clone()));
        assert!(
            result.applied,
            "UPDATE IF v=10 on row where v=10 must apply"
        );

        // Condition does not match: applied=false, returns current values.
        let conditions_mismatch = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::Eq,
            value: Term::IntegerLiteral(99),
        }];

        let result = eval(&conditions_mismatch, V_INT, false, Some(row.clone()));
        assert!(
            !result.applied,
            "UPDATE IF v=99 on row where v=10 must not apply"
        );
        assert_eq!(
            result.current_values.get("v"),
            Some(&Some(CqlValue::Int(10))),
            "must return current v value on failure"
        );

        // Row doesn't exist + IF condition: applied=false.
        let result = eval(&conditions, V_INT, false, None);
        assert!(
            !result.applied,
            "UPDATE IF v=10 on non-existent row must not apply"
        );
    }

    #[test]
    fn lwt_delete_if_condition() {
        // DELETE IF col=? works the same way as UPDATE IF.
        let mut row = HashMap::new();
        row.insert("status".to_string(), Some(CqlValue::Text("active".into())));

        let conditions = vec![IfCondition {
            column: "status".into(),
            operator: IfOperator::Eq,
            value: Term::StringLiteral("active".into()),
        }];

        let status = &[("status", CqlType::Varchar)];
        let result = eval(&conditions, status, false, Some(row.clone()));
        assert!(
            result.applied,
            "DELETE IF status='active' on matching row must apply"
        );

        let conditions_mismatch = vec![IfCondition {
            column: "status".into(),
            operator: IfOperator::Eq,
            value: Term::StringLiteral("inactive".into()),
        }];

        let result = eval(&conditions_mismatch, status, false, Some(row.clone()));
        assert!(
            !result.applied,
            "DELETE IF status='inactive' on row where status='active' must not apply"
        );
        assert_eq!(
            result.current_values.get("status"),
            Some(&Some(CqlValue::Text("active".into()))),
            "must return current status value on failure"
        );

        // IF EXISTS on non-existent row.
        let result = eval(&[], status, true, None);
        assert!(
            !result.applied,
            "DELETE IF EXISTS on non-existent row must not apply"
        );

        // IF EXISTS on existing row (no other conditions).
        let result = eval(&[], status, true, Some(row.clone()));
        assert!(
            result.applied,
            "DELETE IF EXISTS on existing row must apply"
        );
    }

    // -----------------------------------------------------------------------
    // A3.6: Batch CAS
    // -----------------------------------------------------------------------

    #[test]
    fn batch_cas_all_or_nothing() {
        // All conditions pass: batch applied atomically.
        let stmts = [
            Statement::Insert(InsertStatement {
                keyspace: Some("ks".into()),
                table: "t".into(),
                columns: vec!["id".into(), "v".into()],
                values: vec![Term::IntegerLiteral(1), Term::IntegerLiteral(100)],
                if_not_exists: true,
                using_timestamp: None,
                using_ttl: None,
            }),
            Statement::Update(UpdateStatement {
                keyspace: Some("ks".into()),
                table: "t".into(),
                assignments: vec![Assignment::Simple {
                    column: "v".into(),
                    value: Term::IntegerLiteral(200),
                }],
                where_clauses: vec![WhereClause {
                    column: "id".into(),
                    op: ComparisonOp::Eq,
                    value: Term::IntegerLiteral(2),
                    token_fn: false,
                }],
                if_exists: false,
                if_conditions: vec![IfCondition {
                    column: "v".into(),
                    operator: IfOperator::Eq,
                    value: Term::IntegerLiteral(50),
                }],
                using_timestamp: None,
                using_ttl: None,
            }),
        ];

        // Row 1 doesn't exist (INSERT IF NOT EXISTS passes).
        // Row 2 exists with v=50 (UPDATE IF v=50 passes).
        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), Some(CqlValue::Int(2)));
        row2.insert("v".to_string(), Some(CqlValue::Int(50)));

        let columns = &[("id", CqlType::Int), ("v", CqlType::Int)];
        let predicates: Vec<_> = stmts
            .iter()
            .map(|s| TypedLwtPredicate::for_statement(s, schema_of(columns)).unwrap())
            .collect();
        let row_states = vec![None, Some(row2)];
        let (all_applied, results) = eval_batch_cas(&predicates, &row_states);

        assert!(
            all_applied,
            "batch must be fully applied when all conditions pass"
        );
        assert_eq!(results.len(), 2);
        assert!(results[0].applied);
        assert!(results[1].applied);

        // One condition fails: entire batch not applied.
        let mut row2_wrong = HashMap::new();
        row2_wrong.insert("id".to_string(), Some(CqlValue::Int(2)));
        row2_wrong.insert("v".to_string(), Some(CqlValue::Int(999)));

        let row_states_fail = vec![None, Some(row2_wrong)];
        let (all_applied, results) = eval_batch_cas(&predicates, &row_states_fail);

        assert!(
            !all_applied,
            "batch must not be applied when any condition fails"
        );
        assert!(
            results[0].applied,
            "first statement's condition passed individually"
        );
        assert!(!results[1].applied, "second statement's condition failed");
        assert_eq!(
            results[1].current_values.get("v"),
            Some(&Some(CqlValue::Int(999))),
            "failed row must include current values"
        );
    }

    #[test]
    fn batch_cas_result_format() {
        // All applied: single row with [applied]=true.
        let results_applied = vec![
            BatchCasResult {
                applied: true,
                current_values: HashMap::new(),
            },
            BatchCasResult {
                applied: true,
                current_values: HashMap::new(),
            },
        ];

        let buf = encode_batch_cas_result(true, &results_applied, "ks", "t", &[]);
        assert_eq!(
            &buf[0..4],
            &[0x00, 0x00, 0x00, 0x02],
            "must be a Rows result"
        );

        // Partial failure: per-row [applied] on failure.
        let mut current = HashMap::new();
        current.insert("v".to_string(), Some(CqlValue::Int(999)));
        let results_partial = vec![
            BatchCasResult {
                applied: true,
                current_values: HashMap::new(),
            },
            BatchCasResult {
                applied: false,
                current_values: current,
            },
        ];

        let condition_cols = vec![("v".to_string(), CqlType::Int)];
        let buf = encode_batch_cas_result(false, &results_partial, "ks", "t", &condition_cols);
        assert_eq!(
            &buf[0..4],
            &[0x00, 0x00, 0x00, 0x02],
            "must be a Rows result"
        );
        // Should have 2 rows (per-statement results).
        // The row count is at a variable offset due to metadata, but the buffer
        // must be substantially larger than the all-applied case.
        assert!(
            buf.len() > 30,
            "per-row failure result must include condition column data for each row"
        );
    }

    // -----------------------------------------------------------------------
    // Additional edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn lwt_if_not_eq_operator() {
        let mut row = HashMap::new();
        row.insert("v".to_string(), Some(CqlValue::Int(10)));

        let conditions = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::NotEq,
            value: Term::IntegerLiteral(10),
        }];

        let result = eval(&conditions, V_INT, false, Some(row.clone()));
        assert!(!result.applied, "IF v != 10 should fail when v=10");

        let conditions2 = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::NotEq,
            value: Term::IntegerLiteral(99),
        }];

        let result = eval(&conditions2, V_INT, false, Some(row.clone()));
        assert!(result.applied, "IF v != 99 should pass when v=10");
    }

    #[test]
    fn lwt_multiple_conditions_all_must_match() {
        let mut row = HashMap::new();
        row.insert("a".to_string(), Some(CqlValue::Int(1)));
        row.insert("b".to_string(), Some(CqlValue::Text("ok".into())));

        // Both conditions match.
        let conditions = vec![
            IfCondition {
                column: "a".into(),
                operator: IfOperator::Eq,
                value: Term::IntegerLiteral(1),
            },
            IfCondition {
                column: "b".into(),
                operator: IfOperator::Eq,
                value: Term::StringLiteral("ok".into()),
            },
        ];

        let columns = &[("a", CqlType::Int), ("b", CqlType::Varchar)];
        let result = eval(&conditions, columns, false, Some(row.clone()));
        assert!(result.applied, "both conditions match, should apply");

        // First condition matches, second does not.
        let conditions_partial = vec![
            IfCondition {
                column: "a".into(),
                operator: IfOperator::Eq,
                value: Term::IntegerLiteral(1),
            },
            IfCondition {
                column: "b".into(),
                operator: IfOperator::Eq,
                value: Term::StringLiteral("nope".into()),
            },
        ];

        let result = eval(&conditions_partial, columns, false, Some(row.clone()));
        assert!(!result.applied, "second condition fails, should not apply");
    }

    #[test]
    fn lwt_null_column_handling() {
        // Column is NULL in the row.
        let mut row = HashMap::new();
        row.insert("v".to_string(), None);

        let conditions = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::Eq,
            value: Term::Null,
        }];

        let result = eval(&conditions, V_INT, false, Some(row.clone()));
        assert!(result.applied, "IF v=NULL should match when v is NULL");

        let conditions_non_null = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::Eq,
            value: Term::IntegerLiteral(10),
        }];

        let result = eval(&conditions_non_null, V_INT, false, Some(row.clone()));
        assert!(!result.applied, "IF v=10 should fail when v is NULL");
    }

    #[test]
    fn lwt_comparison_operators() {
        let mut row = HashMap::new();
        row.insert("v".to_string(), Some(CqlValue::Int(50)));

        // v > 40: should pass
        let cond_gt = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::Gt,
            value: Term::IntegerLiteral(40),
        }];
        assert!(eval(&cond_gt, V_INT, false, Some(row.clone())).applied);

        // v < 60: should pass
        let cond_lt = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::Lt,
            value: Term::IntegerLiteral(60),
        }];
        assert!(eval(&cond_lt, V_INT, false, Some(row.clone())).applied);

        // v >= 50: should pass
        let cond_ge = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::GtEq,
            value: Term::IntegerLiteral(50),
        }];
        assert!(eval(&cond_ge, V_INT, false, Some(row.clone())).applied);

        // v <= 50: should pass
        let cond_le = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::LtEq,
            value: Term::IntegerLiteral(50),
        }];
        assert!(eval(&cond_le, V_INT, false, Some(row.clone())).applied);

        // v > 50: should fail
        let cond_gt_fail = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::Gt,
            value: Term::IntegerLiteral(50),
        }];
        assert!(!eval(&cond_gt_fail, V_INT, false, Some(row.clone())).applied);
    }

    #[test]
    fn lwt_in_operator() {
        let mut row = HashMap::new();
        row.insert("v".to_string(), Some(CqlValue::Int(10)));

        let cond_in = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::In,
            value: Term::InList(vec![
                Term::IntegerLiteral(10),
                Term::IntegerLiteral(20),
                Term::IntegerLiteral(30),
            ]),
        }];

        assert!(eval(&cond_in, V_INT, false, Some(row.clone())).applied);

        let cond_in_miss = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::In,
            value: Term::InList(vec![Term::IntegerLiteral(1), Term::IntegerLiteral(2)]),
        }];

        assert!(!eval(&cond_in_miss, V_INT, false, Some(row.clone())).applied);
    }

    // -----------------------------------------------------------------------
    // Generic-IF: statement classification + read-row evaluation (Task #30).
    // -----------------------------------------------------------------------

    fn insert_if_not_exists() -> Statement {
        Statement::Insert(InsertStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            columns: vec!["id".into()],
            values: vec![Term::IntegerLiteral(1)],
            if_not_exists: true,
            using_timestamp: None,
            using_ttl: None,
        })
    }

    fn update_if_v_eq(expected: i64) -> Statement {
        Statement::Update(UpdateStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            assignments: vec![Assignment::Simple {
                column: "v".into(),
                value: Term::IntegerLiteral(99),
            }],
            where_clauses: vec![WhereClause {
                column: "id".into(),
                op: ComparisonOp::Eq,
                value: Term::IntegerLiteral(1),
                token_fn: false,
            }],
            if_exists: false,
            if_conditions: vec![IfCondition {
                column: "v".into(),
                operator: IfOperator::Eq,
                value: Term::IntegerLiteral(expected),
            }],
            using_timestamp: None,
            using_ttl: None,
        })
    }

    /// Evaluate `stmt`'s typed predicate over `id int, v int`.
    fn eval_lwt_for_statement(
        stmt: &Statement,
        row: Option<HashMap<String, Option<CqlValue>>>,
    ) -> Option<LwtResult> {
        let columns = &[("id", CqlType::Int), ("v", CqlType::Int)];
        TypedLwtPredicate::for_statement(stmt, schema_of(columns))
            .expect("the statement types against id int, v int")
            .map(|predicate| predicate.eval(row))
    }

    #[test]
    fn classify_lwt_distinguishes_not_exists_from_generic() {
        assert_eq!(
            classify_lwt(&insert_if_not_exists()),
            Some(LwtPredicateKind::NotExists)
        );
        assert_eq!(
            classify_lwt(&update_if_v_eq(50)),
            Some(LwtPredicateKind::Generic)
        );
        // A plain INSERT with no IF is not an LWT.
        let plain = Statement::Insert(InsertStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            columns: vec!["id".into()],
            values: vec![Term::IntegerLiteral(1)],
            if_not_exists: false,
            using_timestamp: None,
            using_ttl: None,
        });
        assert_eq!(classify_lwt(&plain), None);
    }

    // (a) UPDATE … IF v=50 where the stored row matches -> applied.
    #[test]
    fn eval_generic_if_match_applies() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Some(CqlValue::Int(1)));
        row.insert("v".to_string(), Some(CqlValue::Int(50)));

        let res = eval_lwt_for_statement(&update_if_v_eq(50), Some(row.clone()))
            .expect("UPDATE IF is an LWT");
        assert!(res.applied, "matching IF v=50 must apply");
    }

    // (b) UPDATE … IF v=50 where the stored row does NOT match -> not applied,
    //     and current_values returns the real stored row.
    #[test]
    fn eval_generic_if_mismatch_returns_current_values() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Some(CqlValue::Int(1)));
        row.insert("v".to_string(), Some(CqlValue::Int(999)));

        let res = eval_lwt_for_statement(&update_if_v_eq(50), Some(row.clone()))
            .expect("UPDATE IF is an LWT");
        assert!(!res.applied, "non-matching IF v=50 must NOT apply");
        assert_eq!(
            res.current_values.get("v"),
            Some(&Some(CqlValue::Int(999))),
            "current_values must carry the real stored row"
        );
    }

    // (c) INSERT IF NOT EXISTS via the existence path: absent -> applied,
    //     present -> not applied (existence semantics, no generic predicate).
    #[test]
    fn eval_insert_if_not_exists_via_existence_path() {
        let applied = eval_lwt_for_statement(&insert_if_not_exists(), None)
            .expect("INSERT IF NOT EXISTS is an LWT");
        assert!(
            applied.applied,
            "absent row -> INSERT IF NOT EXISTS applies"
        );

        let mut row = HashMap::new();
        row.insert("id".to_string(), Some(CqlValue::Int(1)));
        let not_applied = eval_lwt_for_statement(&insert_if_not_exists(), Some(row.clone()))
            .expect("INSERT IF NOT EXISTS is an LWT");
        assert!(
            !not_applied.applied,
            "present row -> INSERT IF NOT EXISTS must not apply"
        );
    }

    /// An integer literal must compare equal to the column's ACTUAL integer
    /// type, not to `Int` alone.
    ///
    /// `term_to_cql_value_simple` narrowed every integer literal to
    /// `CqlValue::Int(i32)`, while a `bigint` column reads back as
    /// `CqlValue::Bigint(i64)`. `Eq` compared `Option<CqlValue>` directly, so
    /// `Bigint(321) == Int(321)` was FALSE — two variants of the same enum.
    /// Compare-and-set on any 64-bit column could therefore never apply.
    ///
    /// Verified live 2026-08-22 against the 3-node cluster: an LWT issued with
    /// the value just SELECTed came back `[applied]=false` with the returned
    /// current value equal to the one the condition required. The caller (the
    /// mobile-control cursor allocator) retried 32 times and reported
    /// "remained contended", which sent two diagnosis cycles hunting a
    /// competing writer that did not exist.
    #[test]
    fn if_eq_applies_across_every_integer_width() {
        // (column value as stored, literal in the IF clause)
        let cases: Vec<(CqlType, CqlValue)> = vec![
            (CqlType::Int, CqlValue::Int(321)),
            (CqlType::Bigint, CqlValue::Bigint(321)),
            (CqlType::Smallint, CqlValue::Smallint(321)),
            (CqlType::Tinyint, CqlValue::Tinyint(99)),
            (CqlType::Counter, CqlValue::Counter(321)),
            (
                CqlType::Varint,
                CqlValue::Varint(num_bigint::BigInt::from(321)),
            ),
        ];

        for (label, stored) in cases {
            let literal = match &stored {
                CqlValue::Tinyint(_) => 99i64,
                _ => 321i64,
            };
            let mut row: HashMap<String, Option<CqlValue>> = HashMap::new();
            row.insert("v".to_string(), Some(stored.clone()));

            let conditions = vec![IfCondition {
                column: "v".into(),
                operator: IfOperator::Eq,
                value: Term::IntegerLiteral(literal),
            }];

            let result = eval(&conditions, &[("v", label.clone())], false, Some(row));
            assert!(
                result.applied,
                "IF v = {literal} must apply when the {label:?} column holds \
                 {stored:?}"
            );
        }
    }

    /// `!=` is the DANGEROUS direction of the same defect.
    ///
    /// When the variants differ, `current_val != expected` is trivially true,
    /// so `IF v != 321` APPLIED against a bigint column holding exactly 321 —
    /// a conditional update firing precisely when its guard said it must not.
    #[test]
    fn if_not_eq_does_not_apply_when_the_bigint_value_is_equal() {
        let mut row: HashMap<String, Option<CqlValue>> = HashMap::new();
        row.insert("v".to_string(), Some(CqlValue::Bigint(321)));

        let conditions = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::NotEq,
            value: Term::IntegerLiteral(321),
        }];

        let result = eval(&conditions, V_BIGINT, false, Some(row.clone()));
        assert!(
            !result.applied,
            "IF v != 321 must NOT apply when the bigint column holds 321"
        );
    }

    /// Ordering operators went through `cmp_values`, which only had
    /// same-variant arms and returned `None` for anything else — so every
    /// comparison silently evaluated to not-matching on a bigint column.
    #[test]
    fn if_ordering_operators_work_on_a_bigint_column() {
        let mut row: HashMap<String, Option<CqlValue>> = HashMap::new();
        row.insert("v".to_string(), Some(CqlValue::Bigint(321)));

        let expect = |operator: IfOperator, literal: i64, want: bool, why: &str| {
            let conditions = vec![IfCondition {
                column: "v".into(),
                operator,
                value: Term::IntegerLiteral(literal),
            }];
            let result = eval(&conditions, V_BIGINT, false, Some(row.clone()));
            assert_eq!(result.applied, want, "{why}");
        };

        expect(IfOperator::Gt, 320, true, "321 > 320 must apply");
        expect(IfOperator::Gt, 321, false, "321 > 321 must not apply");
        expect(IfOperator::Lt, 322, true, "321 < 322 must apply");
        expect(IfOperator::Lt, 321, false, "321 < 321 must not apply");
        expect(IfOperator::GtEq, 321, true, "321 >= 321 must apply");
        expect(IfOperator::LtEq, 321, true, "321 <= 321 must apply");
        expect(IfOperator::GtEq, 322, false, "321 >= 322 must not apply");
    }

    /// The CAS shape real callers use: read a value, then conditionally write
    /// using the value just read. This must succeed on the FIRST attempt.
    ///
    /// This is the regression that broke the cursor allocator. Note the first
    /// CAS against a fresh table used to succeed and every later one failed —
    /// not flakiness, but two different paths: with no row the caller uses
    /// `INSERT ... IF NOT EXISTS` (an existence check, no value comparison),
    /// and only once a row exists does it take the `IF col = ?` path.
    #[test]
    fn compare_and_set_on_a_bigint_cursor_applies_first_attempt() {
        let mut row: HashMap<String, Option<CqlValue>> = HashMap::new();
        row.insert("next_cursor".to_string(), Some(CqlValue::Bigint(65)));

        // Read 65, then CAS on 65 — exactly what the allocator does.
        let observed = match row.get("next_cursor") {
            Some(Some(CqlValue::Bigint(n))) => *n,
            other => panic!("expected a bigint cursor, got {other:?}"),
        };

        let conditions = vec![IfCondition {
            column: "next_cursor".into(),
            operator: IfOperator::Eq,
            value: Term::IntegerLiteral(observed),
        }];

        let cursor = &[("next_cursor", CqlType::Bigint)];
        let result = eval(&conditions, cursor, false, Some(row.clone()));
        assert!(
            result.applied,
            "a CAS using the value just read must apply on the first attempt; \
             the allocator retried 32x and reported false contention"
        );
    }

    /// `IN` used the same variant-sensitive `==`, so it failed on a bigint
    /// column exactly as `=` did.
    #[test]
    fn if_in_matches_a_bigint_column() {
        let mut row: HashMap<String, Option<CqlValue>> = HashMap::new();
        row.insert("v".to_string(), Some(CqlValue::Bigint(321)));

        let present = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::In,
            value: Term::InList(vec![Term::IntegerLiteral(7), Term::IntegerLiteral(321)]),
        }];
        assert!(
            eval(&present, V_BIGINT, false, Some(row.clone())).applied,
            "IF v IN (7, 321) must apply when the bigint column holds 321"
        );

        let absent = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::In,
            value: Term::InList(vec![Term::IntegerLiteral(7), Term::IntegerLiteral(8)]),
        }];
        assert!(
            !eval(&absent, V_BIGINT, false, Some(row.clone())).applied,
            "IF v IN (7, 8) must NOT apply when the bigint column holds 321"
        );
    }
}
