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
    existing_row: Option<&HashMap<String, Option<CqlValue>>>,
) -> LwtResult {
    match existing_row {
        None => LwtResult {
            applied: true,
            current_values: HashMap::new(),
        },
        Some(row) => LwtResult {
            applied: false,
            current_values: row.clone(),
        },
    }
}

/// Evaluate IF conditions on an UPDATE or DELETE.
///
/// Each `IfCondition` is checked against the current row values. All
/// conditions must be satisfied for the mutation to be applied.
///
/// If `if_exists` is true and the row is `None`, returns `applied=false`.
///
/// Returns `applied=true` if all conditions match, `applied=false` with
/// current values otherwise.
pub fn eval_if_conditions(
    conditions: &[IfCondition],
    if_exists: bool,
    existing_row: Option<&HashMap<String, Option<CqlValue>>>,
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
        let current_val = row.get(&cond.column).cloned().flatten();
        let expected = term_to_cql_value_simple(&cond.value);

        let matches = match cond.operator {
            IfOperator::Eq => values_eq(&current_val, &expected),
            IfOperator::NotEq => !values_eq(&current_val, &expected),
            IfOperator::Lt => cmp_values(&current_val, &expected) == Some(std::cmp::Ordering::Less),
            IfOperator::Gt => {
                cmp_values(&current_val, &expected) == Some(std::cmp::Ordering::Greater)
            }
            IfOperator::LtEq => matches!(
                cmp_values(&current_val, &expected),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ),
            IfOperator::GtEq => matches!(
                cmp_values(&current_val, &expected),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ),
            IfOperator::In => match &cond.value {
                Term::InList(items) => items.iter().any(|item| {
                    let item_val = term_to_cql_value_simple(item);
                    values_eq(&current_val, &item_val)
                }),
                _ => false,
            },
        };

        if !matches {
            return LwtResult {
                applied: false,
                current_values: row.clone(),
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

/// Evaluate an LWT statement's IF clause against the row state at `t`.
///
/// `existing_row` is the row read at the Accord-agreed timestamp `t` (decoded to
/// `column -> value`), or `None` if the row was absent at `t`. Reuses the
/// canonical [`eval_insert_if_not_exists`] / [`eval_if_conditions`] evaluators —
/// no divergent logic. Returns `None` if the statement is not an LWT.
pub fn eval_lwt_for_statement(
    stmt: &Statement,
    existing_row: Option<&HashMap<String, Option<CqlValue>>>,
) -> Option<LwtResult> {
    match stmt {
        Statement::Insert(s) if s.if_not_exists => Some(eval_insert_if_not_exists(existing_row)),
        Statement::Update(s) if s.if_exists || !s.if_conditions.is_empty() => Some(
            eval_if_conditions(&s.if_conditions, s.if_exists, existing_row),
        ),
        Statement::Delete(s) if s.if_exists || !s.if_conditions.is_empty() => Some(
            eval_if_conditions(&s.if_conditions, s.if_exists, existing_row),
        ),
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
pub fn eval_batch_cas(
    statements: &[Statement],
    row_states: &[Option<HashMap<String, Option<CqlValue>>>],
) -> (bool, Vec<BatchCasResult>) {
    assert_eq!(
        statements.len(),
        row_states.len(),
        "statement count must match row state count"
    );

    let mut results = Vec::with_capacity(statements.len());
    let mut all_applied = true;

    for (stmt, existing_row) in statements.iter().zip(row_states.iter()) {
        let lwt = match stmt {
            Statement::Insert(s) if s.if_not_exists => {
                eval_insert_if_not_exists(existing_row.as_ref())
            }
            Statement::Update(s) if !s.if_conditions.is_empty() || s.if_exists => {
                eval_if_conditions(&s.if_conditions, s.if_exists, existing_row.as_ref())
            }
            Statement::Delete(s) if !s.if_conditions.is_empty() || s.if_exists => {
                eval_if_conditions(&s.if_conditions, s.if_exists, existing_row.as_ref())
            }
            // Statements without conditions are always "applied" individually.
            _ => LwtResult {
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

/// Simple term-to-CqlValue conversion for condition evaluation.
///
/// This handles the common literal cases needed for IF condition comparison.
/// For full type-aware conversion, the bridge module is used instead.
fn term_to_cql_value_simple(term: &Term) -> Option<CqlValue> {
    match term {
        Term::StringLiteral(s) => Some(CqlValue::Text(s.clone())),
        Term::IntegerLiteral(n) => Some(CqlValue::Int(*n as i32)),
        Term::FloatLiteral(f) => Some(CqlValue::Double(f.to_bits())),
        Term::BoolLiteral(b) => Some(CqlValue::Boolean(*b)),
        Term::UuidLiteral(u) => Some(CqlValue::Uuid(*u)),
        Term::Null => None,
        _ => None,
    }
}

/// The integer value of a `CqlValue`, if it is one of the integer types.
///
/// CQL's integer family is spread across several `CqlValue` variants, and the
/// width a literal lands in is an artefact of the parser, not of the column.
/// Comparisons must therefore be made on the VALUE, not on the variant:
/// `Bigint(321)` and `Int(321)` denote the same number and must compare equal.
///
/// `BigInt` rather than `i128` because `Varint` is arbitrary-precision, so any
/// fixed-width normalisation would be lossy at the extremes.
fn as_integer(value: &CqlValue) -> Option<num_bigint::BigInt> {
    match value {
        CqlValue::Int(n) => Some(num_bigint::BigInt::from(*n)),
        CqlValue::Bigint(n) => Some(num_bigint::BigInt::from(*n)),
        CqlValue::Counter(n) => Some(num_bigint::BigInt::from(*n)),
        CqlValue::Smallint(n) => Some(num_bigint::BigInt::from(*n)),
        CqlValue::Tinyint(n) => Some(num_bigint::BigInt::from(*n)),
        CqlValue::Varint(n) => Some(n.clone()),
        _ => None,
    }
}

/// Equality for LWT `IF` conditions.
///
/// Integers compare by value across widths; everything else keeps the derived
/// `PartialEq`. Without this, `term_to_cql_value_simple` narrowing every
/// integer literal to `Int(i32)` meant `IF <bigint_col> = ?` could never be
/// true, and — worse — `IF <bigint_col> != ?` was always true, so a guarded
/// update fired exactly when its guard said it must not.
fn values_eq(a: &Option<CqlValue>, b: &Option<CqlValue>) -> bool {
    if let (Some(a), Some(b)) = (a, b) {
        if let (Some(a), Some(b)) = (as_integer(a), as_integer(b)) {
            return a == b;
        }
    }
    a == b
}

/// Compare two optional CqlValues for ordering.
///
/// Returns None if the values are not comparable (different types or nulls).
fn cmp_values(a: &Option<CqlValue>, b: &Option<CqlValue>) -> Option<std::cmp::Ordering> {
    // Integers first, so widths are compared by value rather than by variant.
    if let (Some(x), Some(y)) = (a, b) {
        if let (Some(x), Some(y)) = (as_integer(x), as_integer(y)) {
            return Some(x.cmp(&y));
        }
    }
    match (a, b) {
        (Some(CqlValue::Int(a)), Some(CqlValue::Int(b))) => Some(a.cmp(b)),
        (Some(CqlValue::Bigint(a)), Some(CqlValue::Bigint(b))) => Some(a.cmp(b)),
        (Some(CqlValue::Text(a)), Some(CqlValue::Text(b))) => Some(a.cmp(b)),
        (Some(CqlValue::Double(a_bits)), Some(CqlValue::Double(b_bits))) => {
            let a_f = f64::from_bits(*a_bits);
            let b_f = f64::from_bits(*b_bits);
            a_f.partial_cmp(&b_f)
        }
        (Some(CqlValue::Float(a_bits)), Some(CqlValue::Float(b_bits))) => {
            let a_f = f32::from_bits(*a_bits);
            let b_f = f32::from_bits(*b_bits);
            a_f.partial_cmp(&b_f)
        }
        _ => None,
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

        let result = eval_insert_if_not_exists(Some(&existing));
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

        let result = eval_if_conditions(&conditions, false, Some(&row));
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

        let result = eval_if_conditions(&conditions_mismatch, false, Some(&row));
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
        let result = eval_if_conditions(&conditions, false, None);
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

        let result = eval_if_conditions(&conditions, false, Some(&row));
        assert!(
            result.applied,
            "DELETE IF status='active' on matching row must apply"
        );

        let conditions_mismatch = vec![IfCondition {
            column: "status".into(),
            operator: IfOperator::Eq,
            value: Term::StringLiteral("inactive".into()),
        }];

        let result = eval_if_conditions(&conditions_mismatch, false, Some(&row));
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
        let result = eval_if_conditions(&[], true, None);
        assert!(
            !result.applied,
            "DELETE IF EXISTS on non-existent row must not apply"
        );

        // IF EXISTS on existing row (no other conditions).
        let result = eval_if_conditions(&[], true, Some(&row));
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
        let stmts = vec![
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

        let row_states = vec![None, Some(row2)];
        let (all_applied, results) = eval_batch_cas(&stmts, &row_states);

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
        let (all_applied, results) = eval_batch_cas(&stmts, &row_states_fail);

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

        let result = eval_if_conditions(&conditions, false, Some(&row));
        assert!(!result.applied, "IF v != 10 should fail when v=10");

        let conditions2 = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::NotEq,
            value: Term::IntegerLiteral(99),
        }];

        let result = eval_if_conditions(&conditions2, false, Some(&row));
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

        let result = eval_if_conditions(&conditions, false, Some(&row));
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

        let result = eval_if_conditions(&conditions_partial, false, Some(&row));
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

        let result = eval_if_conditions(&conditions, false, Some(&row));
        assert!(result.applied, "IF v=NULL should match when v is NULL");

        let conditions_non_null = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::Eq,
            value: Term::IntegerLiteral(10),
        }];

        let result = eval_if_conditions(&conditions_non_null, false, Some(&row));
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
        assert!(eval_if_conditions(&cond_gt, false, Some(&row)).applied);

        // v < 60: should pass
        let cond_lt = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::Lt,
            value: Term::IntegerLiteral(60),
        }];
        assert!(eval_if_conditions(&cond_lt, false, Some(&row)).applied);

        // v >= 50: should pass
        let cond_ge = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::GtEq,
            value: Term::IntegerLiteral(50),
        }];
        assert!(eval_if_conditions(&cond_ge, false, Some(&row)).applied);

        // v <= 50: should pass
        let cond_le = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::LtEq,
            value: Term::IntegerLiteral(50),
        }];
        assert!(eval_if_conditions(&cond_le, false, Some(&row)).applied);

        // v > 50: should fail
        let cond_gt_fail = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::Gt,
            value: Term::IntegerLiteral(50),
        }];
        assert!(!eval_if_conditions(&cond_gt_fail, false, Some(&row)).applied);
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

        assert!(eval_if_conditions(&cond_in, false, Some(&row)).applied);

        let cond_in_miss = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::In,
            value: Term::InList(vec![Term::IntegerLiteral(1), Term::IntegerLiteral(2)]),
        }];

        assert!(!eval_if_conditions(&cond_in_miss, false, Some(&row)).applied);
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

        let res =
            eval_lwt_for_statement(&update_if_v_eq(50), Some(&row)).expect("UPDATE IF is an LWT");
        assert!(res.applied, "matching IF v=50 must apply");
    }

    // (b) UPDATE … IF v=50 where the stored row does NOT match -> not applied,
    //     and current_values returns the real stored row.
    #[test]
    fn eval_generic_if_mismatch_returns_current_values() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Some(CqlValue::Int(1)));
        row.insert("v".to_string(), Some(CqlValue::Int(999)));

        let res =
            eval_lwt_for_statement(&update_if_v_eq(50), Some(&row)).expect("UPDATE IF is an LWT");
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
        let not_applied = eval_lwt_for_statement(&insert_if_not_exists(), Some(&row))
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
        let cases: Vec<(&str, CqlValue)> = vec![
            ("int", CqlValue::Int(321)),
            ("bigint", CqlValue::Bigint(321)),
            ("smallint", CqlValue::Smallint(321)),
            ("tinyint", CqlValue::Tinyint(99)),
            ("counter", CqlValue::Counter(321)),
            ("varint", CqlValue::Varint(num_bigint::BigInt::from(321))),
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

            let result = eval_if_conditions(&conditions, false, Some(&row));
            assert!(
                result.applied,
                "IF v = {literal} must apply when the {label} column holds \
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

        let result = eval_if_conditions(&conditions, false, Some(&row));
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
            let result = eval_if_conditions(&conditions, false, Some(&row));
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

        let result = eval_if_conditions(&conditions, false, Some(&row));
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
            eval_if_conditions(&present, false, Some(&row)).applied,
            "IF v IN (7, 321) must apply when the bigint column holds 321"
        );

        let absent = vec![IfCondition {
            column: "v".into(),
            operator: IfOperator::In,
            value: Term::InList(vec![Term::IntegerLiteral(7), Term::IntegerLiteral(8)]),
        }];
        assert!(
            !eval_if_conditions(&absent, false, Some(&row)).applied,
            "IF v IN (7, 8) must NOT apply when the bigint column holds 321"
        );
    }
}
