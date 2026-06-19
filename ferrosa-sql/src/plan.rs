//! Bind + execute an M1 `SelectStmt` against a [`Catalog`].
//!
//! Pipeline: scan(from) [→ hash_join(scan(join))] [→ filter] → project. Column
//! references resolve through a scope of `(binding_name, RelSchema, base_offset)`
//! entries; an unqualified name that matches more than one table is rejected
//! (fail loud), and an unknown table/column errors rather than returning wrong
//! or empty results.

use std::fmt;

use std::cmp::Ordering;
use std::collections::HashSet;

use crate::ast::{
    AggArg, ColumnRef, Expr, Operand, Projection, SelectItem, SelectStmt, TableRef, Term,
};
use crate::catalog::Catalog;
use crate::exec::{
    hash_aggregate, hash_join, limit_offset, seq_scan, sort, AggFunc, CmpOp, SortKey,
};
use crate::types::{Column, ColumnType, RelSchema, Row, Value};

/// The result of executing a query: output column metadata + materialized rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    NoSuchTable {
        schema: String,
        table: String,
    },
    NoSuchColumn(String),
    AmbiguousColumn(String),
    UnknownQualifier(String),
    /// A non-aggregated column in the SELECT list (or a HAVING column that is
    /// neither a group column nor an aggregate) is absent from `GROUP BY`.
    NotGrouped(String),
    /// An `ORDER BY` ordinal is out of range of the output columns.
    InvalidOrderBy(String),
    /// An aggregate function was used in a `WHERE` clause (illegal — aggregates
    /// belong in `HAVING`).
    AggregateInWhere(String),
    /// A `$N` parameter placeholder referenced an index with no bound value
    /// (out of range of the supplied `params`). Carries the 1-based index.
    MissingParameter(usize),
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecError::NoSuchTable { schema, table } => {
                write!(f, "relation \"{schema}.{table}\" does not exist")
            }
            ExecError::NoSuchColumn(c) => write!(f, "column \"{c}\" does not exist"),
            ExecError::AmbiguousColumn(c) => write!(f, "column reference \"{c}\" is ambiguous"),
            ExecError::UnknownQualifier(q) => {
                write!(f, "missing FROM-clause entry for table \"{q}\"")
            }
            ExecError::NotGrouped(c) => write!(
                f,
                "column \"{c}\" must appear in the GROUP BY clause or be used in an aggregate function"
            ),
            ExecError::InvalidOrderBy(c) => {
                write!(f, "ORDER BY position \"{c}\" is not in select list")
            }
            ExecError::AggregateInWhere(a) => write!(
                f,
                "aggregate functions are not allowed in WHERE: \"{a}\" (use HAVING)"
            ),
            ExecError::MissingParameter(n) => {
                write!(f, "there is no parameter ${n}")
            }
        }
    }
}

/// Resolve a comparison [`Term`] to a concrete [`Value`]: a literal yields
/// itself; a `$N` parameter looks up `params[N-1]`, failing loud with
/// [`ExecError::MissingParameter`] when the index is out of range.
fn resolve_term<'a>(term: &'a Term, params: &'a [Value]) -> Result<&'a Value, ExecError> {
    match term {
        Term::Literal(v) => Ok(v),
        Term::Param(n) => params.get(n - 1).ok_or(ExecError::MissingParameter(*n)),
    }
}

/// Apply a [`CmpOp`] to a definite ordering.
fn apply_cmp(op: CmpOp, ord: Ordering) -> bool {
    match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
    }
}

/// Evaluate a boolean expression over `row` under three-valued Kleene logic,
/// returning `None` for UNKNOWN. `resolve` maps each leaf comparison's operand
/// to the value to compare (its column/aggregate slot in `row`); the RHS
/// [`Term`] is resolved against `params` (parameters are validated up front, so
/// a missing one — impossible here — is treated as UNKNOWN). A comparison is
/// UNKNOWN when [`Value::sql_cmp`] is `None`.
fn eval_kleene(
    expr: &Expr,
    row: &Row,
    params: &[Value],
    resolve: &dyn Fn(&Operand) -> usize,
) -> Option<bool> {
    match expr {
        Expr::Compare { left, op, value } => {
            let idx = resolve(left);
            let rhs = resolve_term(value, params).ok()?;
            row.0[idx].sql_cmp(rhs).map(|ord| apply_cmp(*op, ord))
        }
        Expr::Not(inner) => eval_kleene(inner, row, params, resolve).map(|b| !b),
        Expr::And(l, r) => {
            let a = eval_kleene(l, row, params, resolve);
            let b = eval_kleene(r, row, params, resolve);
            kleene_and(a, b)
        }
        Expr::Or(l, r) => {
            let a = eval_kleene(l, row, params, resolve);
            let b = eval_kleene(r, row, params, resolve);
            kleene_or(a, b)
        }
    }
}

/// Validate that every `$N` parameter referenced anywhere in a boolean
/// expression has a bound value in `params`. Fail-loud entry point so a missing
/// parameter is reported before any row is evaluated.
fn validate_params(expr: &Expr, params: &[Value]) -> Result<(), ExecError> {
    match expr {
        Expr::Compare { value, .. } => resolve_term(value, params).map(|_| ()),
        Expr::Not(inner) => validate_params(inner, params),
        Expr::And(l, r) | Expr::Or(l, r) => {
            validate_params(l, params)?;
            validate_params(r, params)
        }
    }
}

/// Kleene AND: `Some(false)` dominates; else UNKNOWN if either is UNKNOWN.
fn kleene_and(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

/// Kleene OR: `Some(true)` dominates; else UNKNOWN if either is UNKNOWN.
fn kleene_or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

impl std::error::Error for ExecError {}

/// Execute a parsed statement against `catalog`; bare table names resolve under
/// `default_schema`. `params` supplies the bound values for any `$N` parameter
/// placeholders in the WHERE/HAVING terms (the prepared/extended-query path);
/// the simple-query path passes `&[]`.
pub fn execute(
    stmt: &SelectStmt,
    catalog: &dyn Catalog,
    default_schema: &str,
    params: &[Value],
) -> Result<QueryResult, ExecError> {
    // Fail loud up front if a referenced `$N` has no bound value.
    if let Some(f) = &stmt.filter {
        validate_params(f, params)?;
    }
    if let Some(h) = &stmt.having {
        validate_params(h, params)?;
    }

    let (scope, combined_schema) = resolve_scope(stmt, catalog, default_schema)?;

    // Scan the FROM (and hash-join the JOIN, if any) into the base row set. The
    // binding scope / combined schema already came from `resolve_scope`.
    let from_provider = resolve_table(catalog, &stmt.from, default_schema)?;
    let base_rows: Vec<Row> = if let Some(join) = &stmt.join {
        let join_provider = resolve_table(catalog, &join.table, default_schema)?;
        let from_schema = from_provider.schema();
        let join_schema = join_provider.schema();
        let from_binding = stmt.from.binding_name();
        let join_binding = join.table.binding_name();
        let (left_key, right_key) = resolve_join_keys(
            from_binding,
            from_schema,
            join_binding,
            join_schema,
            &join.left,
            &join.right,
        )?;
        hash_join(
            seq_scan(&*from_provider),
            seq_scan(&*join_provider),
            left_key,
            right_key,
        )
    } else {
        seq_scan(&*from_provider).collect()
    };

    // WHERE: pre-resolve every comparison's column operand to a scope index
    // (aggregates are illegal here), then keep rows that evaluate to Some(true)
    // under Kleene logic.
    let filtered: Vec<Row> = if let Some(f) = &stmt.filter {
        let idx_map = resolve_where_operands(f, &scope)?;
        let resolve = |op: &Operand| idx_map[&OperandKey::of(op)];
        base_rows
            .into_iter()
            .filter(|r| eval_kleene(f, r, params, &resolve) == Some(true))
            .collect()
    } else {
        base_rows
    };

    // Aggregate mode iff GROUP BY is present, any select item is an aggregate,
    // or HAVING is present.
    let is_aggregate = !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || matches!(&stmt.projection, Projection::Items(items)
            if items.iter().any(|i| matches!(i, SelectItem::Aggregate { .. })));

    let (columns, rows) = if is_aggregate {
        plan_aggregate(stmt, &scope, &combined_schema, filtered, params)?
    } else {
        plan_simple(stmt, &scope, &combined_schema, filtered)?
    };

    // LIMIT / OFFSET apply to the final output rows.
    let offset = stmt.offset.unwrap_or(0) as usize;
    let limit = stmt.limit.map(|n| n as usize);
    let rows = limit_offset(rows, offset, limit);

    Ok(QueryResult { columns, rows })
}

/// Build the FROM/JOIN binding scope and the combined output schema for `stmt`,
/// resolving table and join-key references through `catalog` WITHOUT scanning
/// any rows. Shared by [`execute`] (which then scans) and [`describe`] (which
/// only needs the column shape).
fn resolve_scope(
    stmt: &SelectStmt,
    catalog: &dyn Catalog,
    default_schema: &str,
) -> Result<(Vec<Bound>, RelSchema), ExecError> {
    let from_provider = resolve_table(catalog, &stmt.from, default_schema)?;
    let from_schema = from_provider.schema().clone();
    let from_binding = stmt.from.binding_name().to_string();

    let mut scope = vec![Bound {
        binding: from_binding.clone(),
        schema: from_schema.clone(),
        base: 0,
    }];

    if let Some(join) = &stmt.join {
        let join_provider = resolve_table(catalog, &join.table, default_schema)?;
        let join_schema = join_provider.schema().clone();
        let join_binding = join.table.binding_name().to_string();

        // Validate the `ON a = b` keys resolve (same fail-loud check as execute).
        resolve_join_keys(
            &from_binding,
            &from_schema,
            &join_binding,
            &join_schema,
            &join.left,
            &join.right,
        )?;

        scope.push(Bound {
            binding: join_binding,
            schema: join_schema.clone(),
            base: from_schema.width(),
        });
        let mut cols = from_schema.columns.clone();
        cols.extend(join_schema.columns);
        Ok((scope, RelSchema::new(cols)))
    } else {
        Ok((scope, from_schema))
    }
}

/// Describe the OUTPUT columns of `stmt` (the `RowDescription` shape) without
/// running any operators or evaluating WHERE/HAVING — so it works before any
/// parameters are bound. Uses the same projection column-derivation as
/// [`execute`], so a later execute over the same statement produces matching
/// columns.
pub fn describe(
    stmt: &SelectStmt,
    catalog: &dyn Catalog,
    default_schema: &str,
) -> Result<Vec<Column>, ExecError> {
    let (scope, combined_schema) = resolve_scope(stmt, catalog, default_schema)?;

    // Aggregate mode iff GROUP BY / HAVING present or any aggregate select item.
    let is_aggregate = !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || matches!(&stmt.projection, Projection::Items(items)
            if items.iter().any(|i| matches!(i, SelectItem::Aggregate { .. })));

    if is_aggregate {
        aggregate_output_columns(stmt, &scope, &combined_schema)
    } else {
        let (columns, _indices) = simple_projection(stmt, &scope, &combined_schema)?;
        Ok(columns)
    }
}

/// Infer the [`ColumnType`] of each `$N` parameter from the comparison it
/// appears in: a `column <op> $N` takes the column's type; an aggregate operand
/// (`COUNT(*) > $N`, only legal in HAVING) takes the aggregate's result type.
/// Returns the inferred types indexed by 0-based parameter position (`$1` ⇒
/// index 0). Used to answer the extended-protocol `ParameterDescription` so the
/// driver serializes parameters with the right type. A `$N` that is never
/// referenced defaults to [`ColumnType::Text`] (the most permissive choice).
pub fn infer_param_types(
    stmt: &SelectStmt,
    catalog: &dyn Catalog,
    default_schema: &str,
) -> Result<Vec<ColumnType>, ExecError> {
    let (scope, combined_schema) = resolve_scope(stmt, catalog, default_schema)?;
    let mut inferred: Vec<Option<ColumnType>> = Vec::new();

    let mut record = |idx: usize, ty: ColumnType| {
        if inferred.len() < idx + 1 {
            inferred.resize(idx + 1, None);
        }
        // First binding wins; consistent re-use of the same `$N` keeps its type.
        if inferred[idx].is_none() {
            inferred[idx] = Some(ty);
        }
    };

    if let Some(f) = &stmt.filter {
        infer_in_expr(f, &scope, &combined_schema, &mut record)?;
    }
    if let Some(h) = &stmt.having {
        infer_in_expr(h, &scope, &combined_schema, &mut record)?;
    }

    Ok(inferred
        .into_iter()
        .map(|t| t.unwrap_or(ColumnType::Text))
        .collect())
}

/// Walk a boolean expression, recording the inferred type of each `$N` param
/// against the column / aggregate it is compared with.
fn infer_in_expr(
    expr: &Expr,
    scope: &[Bound],
    combined_schema: &RelSchema,
    record: &mut impl FnMut(usize, ColumnType),
) -> Result<(), ExecError> {
    match expr {
        Expr::Compare { left, value, .. } => {
            if let Term::Param(n) = value {
                let ty = operand_type(left, scope, combined_schema)?;
                record(n - 1, ty);
            }
            Ok(())
        }
        Expr::Not(inner) => infer_in_expr(inner, scope, combined_schema, record),
        Expr::And(l, r) | Expr::Or(l, r) => {
            infer_in_expr(l, scope, combined_schema, record)?;
            infer_in_expr(r, scope, combined_schema, record)
        }
    }
}

/// The [`ColumnType`] an operand evaluates to: a column's declared type, or an
/// aggregate's result type (COUNT/SUM/MIN/MAX/AVG per [`aggregate_column`]).
fn operand_type(
    operand: &Operand,
    scope: &[Bound],
    combined_schema: &RelSchema,
) -> Result<ColumnType, ExecError> {
    match operand {
        Operand::Column(cr) => {
            let gi = resolve_column(scope, cr)?;
            Ok(combined_schema.columns[gi].ty)
        }
        Operand::Aggregate { func, arg } => {
            let arg_col = resolve_agg_arg(arg, scope)?;
            Ok(aggregate_column(*func, arg_col, combined_schema).ty)
        }
    }
}

/// Resolve a non-aggregate projection into `(output columns, source indices)`:
/// `SELECT *` selects every column of the combined schema; a column list
/// resolves each name to its global slot. An aggregate item is unreachable here
/// (it forces aggregate mode upstream). Shared by [`plan_simple`] and
/// [`describe`] so the two derive identical output columns.
fn simple_projection(
    stmt: &SelectStmt,
    scope: &[Bound],
    combined_schema: &RelSchema,
) -> Result<(Vec<Column>, Vec<usize>), ExecError> {
    match &stmt.projection {
        Projection::Star => Ok((
            combined_schema.columns.clone(),
            (0..combined_schema.width()).collect(),
        )),
        Projection::Items(items) => {
            let mut columns = Vec::with_capacity(items.len());
            let mut indices = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    SelectItem::Column(cr) => {
                        let gi = resolve_column(scope, cr)?;
                        indices.push(gi);
                        columns.push(combined_schema.columns[gi].clone());
                    }
                    // Unreachable: an aggregate item forces aggregate mode.
                    SelectItem::Aggregate { .. } => unreachable!("aggregate in simple plan"),
                }
            }
            Ok((columns, indices))
        }
    }
}

/// Non-aggregate path. Without DISTINCT, ORDER BY resolves against the input
/// scope (so it may name a non-selected column) and is applied before
/// projection. With DISTINCT, the rows are projected and deduped first, then
/// ORDER BY resolves against the OUTPUT columns.
fn plan_simple(
    stmt: &SelectStmt,
    scope: &[Bound],
    combined_schema: &RelSchema,
    rows: Vec<Row>,
) -> Result<(Vec<Column>, Vec<Row>), ExecError> {
    // Resolve the projection into output columns + source indices.
    let (columns, indices) = simple_projection(stmt, scope, combined_schema)?;

    let project = |rows: Vec<Row>| -> Vec<Row> {
        rows.into_iter()
            .map(|r| Row(indices.iter().map(|&i| r.0[i].clone()).collect()))
            .collect()
    };

    if stmt.distinct {
        // DISTINCT: project + dedup first, then ORDER BY against output columns.
        let deduped = dedup_rows(project(rows));
        let out = if stmt.order_by.is_empty() {
            deduped
        } else {
            let mut keys = Vec::with_capacity(stmt.order_by.len());
            for item in &stmt.order_by {
                let col = resolve_output_position(&item.column, &columns)?;
                keys.push(SortKey { col, dir: item.dir });
            }
            sort(deduped, &keys)
        };
        return Ok((columns, out));
    }

    // No DISTINCT: ORDER BY against the input scope, applied before projecting.
    let rows = if stmt.order_by.is_empty() {
        rows
    } else {
        let mut keys = Vec::with_capacity(stmt.order_by.len());
        for item in &stmt.order_by {
            let col = resolve_column(scope, &item.column)?;
            keys.push(SortKey { col, dir: item.dir });
        }
        sort(rows, &keys)
    };

    Ok((columns, project(rows)))
}

/// Aggregate path. Compute the UNION of aggregates referenced by the SELECT list
/// and by HAVING into an internal layout `[group_cols..., all_unique_aggs...]`,
/// evaluate HAVING against that layout (keep groups where it is `Some(true)`),
/// then project the SELECT items, dedup if DISTINCT, and ORDER BY against the
/// output columns.
fn plan_aggregate(
    stmt: &SelectStmt,
    scope: &[Bound],
    combined_schema: &RelSchema,
    rows: Vec<Row>,
    params: &[Value],
) -> Result<(Vec<Column>, Vec<Row>), ExecError> {
    // Resolve GROUP BY columns to global indices.
    let mut group_cols = Vec::with_capacity(stmt.group_by.len());
    for cr in &stmt.group_by {
        group_cols.push(resolve_column(scope, cr)?);
    }

    let items = match &stmt.projection {
        // `SELECT *` with GROUP BY is not meaningfully supported here; reject.
        Projection::Star => return Err(ExecError::NotGrouped("*".into())),
        Projection::Items(items) => items,
    };

    // The internal aggregate set: the union of aggregates from SELECT and HAVING.
    // `agg_defs` is parallel to the agg tail of the hash_aggregate layout; an
    // operand is deduplicated by its (func, arg-global-index) identity.
    let mut agg_defs: Vec<(AggFunc, Option<usize>)> = Vec::new();
    let mut agg_keys: Vec<AggKey> = Vec::new();

    // Where each SELECT item lives in the internal layout `[group..., aggs...]`.
    enum Slot {
        Group(usize),
        Agg(usize),
    }
    let mut slots: Vec<Slot> = Vec::with_capacity(items.len());
    let mut out_columns: Vec<Column> = Vec::with_capacity(items.len());

    // SELECT list: plain columns must be grouped; aggregates feed the union.
    for item in items {
        match item {
            SelectItem::Column(cr) => {
                let gi = resolve_column(scope, cr)?;
                let pos = group_cols
                    .iter()
                    .position(|&g| g == gi)
                    .ok_or_else(|| ExecError::NotGrouped(cr.qualified_name()))?;
                slots.push(Slot::Group(pos));
                out_columns.push(combined_schema.columns[gi].clone());
            }
            SelectItem::Aggregate { func, arg } => {
                let arg_col = resolve_agg_arg(arg, scope)?;
                let agg_index = intern_agg(*func, arg_col, &mut agg_defs, &mut agg_keys);
                slots.push(Slot::Agg(agg_index));
                out_columns.push(aggregate_column(*func, arg_col, combined_schema));
            }
        }
    }

    // HAVING: resolve its operands into the internal layout. A HAVING column must
    // be a group column; a HAVING aggregate joins the union (computing it even if
    // it is absent from the SELECT list).
    let having_map = if let Some(h) = &stmt.having {
        let mut operands = Vec::new();
        collect_operands(h, &mut operands);
        let mut map = std::collections::HashMap::new();
        for op in operands {
            let internal_idx = match op {
                Operand::Column(cr) => {
                    let gi = resolve_column(scope, cr)?;
                    let pos = group_cols
                        .iter()
                        .position(|&g| g == gi)
                        .ok_or_else(|| ExecError::NotGrouped(cr.qualified_name()))?;
                    pos // group columns occupy the front of the layout
                }
                Operand::Aggregate { func, arg } => {
                    let arg_col = resolve_agg_arg(arg, scope)?;
                    let agg_index = intern_agg(*func, arg_col, &mut agg_defs, &mut agg_keys);
                    group_cols.len() + agg_index
                }
            };
            map.insert(OperandKey::of(op), internal_idx);
        }
        Some(map)
    } else {
        None
    };

    // Compute the internal layout `[group values..., union agg values...]`.
    let internal_rows = hash_aggregate(rows, &group_cols, &agg_defs);

    // HAVING filter over the internal layout (Kleene; keep Some(true)).
    let group_len = group_cols.len();
    let kept: Vec<Row> = if let (Some(h), Some(map)) = (&stmt.having, &having_map) {
        let resolve = |op: &Operand| map[&OperandKey::of(op)];
        internal_rows
            .into_iter()
            .filter(|r| eval_kleene(h, r, params, &resolve) == Some(true))
            .collect()
    } else {
        internal_rows
    };

    // Project the SELECT items out of the internal layout.
    let projected: Vec<Row> = kept
        .into_iter()
        .map(|r| {
            let values = slots
                .iter()
                .map(|slot| match slot {
                    Slot::Group(i) => r.0[*i].clone(),
                    Slot::Agg(i) => r.0[group_len + *i].clone(),
                })
                .collect();
            Row(values)
        })
        .collect();

    // DISTINCT dedup (first-occurrence order), then ORDER BY against output.
    let deduped = if stmt.distinct {
        dedup_rows(projected)
    } else {
        projected
    };

    let out_rows = if stmt.order_by.is_empty() {
        deduped
    } else {
        let mut keys = Vec::with_capacity(stmt.order_by.len());
        for item in &stmt.order_by {
            let col = resolve_output_position(&item.column, &out_columns)?;
            keys.push(SortKey { col, dir: item.dir });
        }
        sort(deduped, &keys)
    };

    Ok((out_columns, out_rows))
}

/// Derive the OUTPUT columns of an aggregate-mode query (SELECT list only),
/// without computing any aggregate values. Each plain column must be a GROUP BY
/// column (else `NotGrouped`); each aggregate yields its `count`/`sum`/… column.
/// Shared by [`describe`] (and mirrors the column derivation inside
/// [`plan_aggregate`]).
fn aggregate_output_columns(
    stmt: &SelectStmt,
    scope: &[Bound],
    combined_schema: &RelSchema,
) -> Result<Vec<Column>, ExecError> {
    let mut group_cols = Vec::with_capacity(stmt.group_by.len());
    for cr in &stmt.group_by {
        group_cols.push(resolve_column(scope, cr)?);
    }

    let items = match &stmt.projection {
        Projection::Star => return Err(ExecError::NotGrouped("*".into())),
        Projection::Items(items) => items,
    };

    let mut out_columns = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Column(cr) => {
                let gi = resolve_column(scope, cr)?;
                if !group_cols.contains(&gi) {
                    return Err(ExecError::NotGrouped(cr.qualified_name()));
                }
                out_columns.push(combined_schema.columns[gi].clone());
            }
            SelectItem::Aggregate { func, arg } => {
                let arg_col = resolve_agg_arg(arg, scope)?;
                out_columns.push(aggregate_column(*func, arg_col, combined_schema));
            }
        }
    }
    Ok(out_columns)
}

/// Identity of an aggregate for deduplication: its function plus arg-column
/// global index (`None` for `COUNT(*)`).
type AggKey = (AggFunc, Option<usize>);

/// Resolve an aggregate argument to a global column index (`None` for `*`).
fn resolve_agg_arg(arg: &AggArg, scope: &[Bound]) -> Result<Option<usize>, ExecError> {
    match arg {
        AggArg::Star => Ok(None),
        AggArg::Column(cr) => Ok(Some(resolve_column(scope, cr)?)),
    }
}

/// Intern an aggregate into the union, returning its index in the agg tail.
/// Reuses an existing slot when an identical `(func, arg-index)` is already
/// present.
fn intern_agg(
    func: AggFunc,
    arg_col: Option<usize>,
    agg_defs: &mut Vec<(AggFunc, Option<usize>)>,
    agg_keys: &mut Vec<AggKey>,
) -> usize {
    let key = (func, arg_col);
    if let Some(pos) = agg_keys.iter().position(|k| *k == key) {
        return pos;
    }
    agg_defs.push((func, arg_col));
    agg_keys.push(key);
    agg_defs.len() - 1
}

/// Deduplicate rows preserving first-occurrence order.
fn dedup_rows(rows: Vec<Row>) -> Vec<Row> {
    let mut seen: HashSet<Vec<Value>> = HashSet::new();
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        if seen.insert(r.0.clone()) {
            out.push(r);
        }
    }
    out
}

/// The output column for an aggregate: name `count`/`sum`/`min`/`max`/`avg`.
/// COUNT is always `Int`; AVG is always `Float`; SUM and MIN/MAX take the
/// argument column's type.
fn aggregate_column(func: AggFunc, arg_col: Option<usize>, schema: &RelSchema) -> Column {
    let (name, ty) = match func {
        AggFunc::Count => ("count", ColumnType::Int),
        AggFunc::Sum => ("sum", arg_ty(arg_col, schema)),
        AggFunc::Avg => ("avg", ColumnType::Float),
        AggFunc::Min => ("min", arg_ty(arg_col, schema)),
        AggFunc::Max => ("max", arg_ty(arg_col, schema)),
    };
    Column::new(name, ty)
}

fn arg_ty(arg_col: Option<usize>, schema: &RelSchema) -> ColumnType {
    match arg_col {
        Some(c) => schema.columns[c].ty,
        None => ColumnType::Int,
    }
}

/// Resolve an ORDER BY key against the aggregate output columns: a bare integer
/// name is a 1-based ordinal; otherwise match by column name.
fn resolve_output_position(col: &ColumnRef, out_columns: &[Column]) -> Result<usize, ExecError> {
    if col.qualifier.is_none() {
        if let Ok(ordinal) = col.name.parse::<usize>() {
            if ordinal == 0 || ordinal > out_columns.len() {
                return Err(ExecError::InvalidOrderBy(col.name.clone()));
            }
            return Ok(ordinal - 1);
        }
    }
    out_columns
        .iter()
        .position(|c| c.name == col.name)
        .ok_or_else(|| ExecError::NoSuchColumn(col.name.clone()))
}

/// A cheap, hashable identity for an [`Operand`] so resolved indices can be
/// memoized per distinct operand without deriving `Hash` on the AST.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum OperandKey {
    Column {
        qualifier: Option<String>,
        name: String,
    },
    Aggregate {
        func: AggFunc,
        arg: Option<String>,
    },
}

impl OperandKey {
    fn of(op: &Operand) -> OperandKey {
        match op {
            Operand::Column(c) => OperandKey::Column {
                qualifier: c.qualifier.clone(),
                name: c.name.clone(),
            },
            Operand::Aggregate { func, arg } => OperandKey::Aggregate {
                func: *func,
                arg: match arg {
                    AggArg::Star => None,
                    AggArg::Column(c) => Some(c.qualified_name()),
                },
            },
        }
    }
}

/// Collect every operand referenced by a boolean expression (in first-seen
/// order), pushing each into `out`.
fn collect_operands<'a>(expr: &'a Expr, out: &mut Vec<&'a Operand>) {
    match expr {
        Expr::Compare { left, .. } => out.push(left),
        Expr::Not(inner) => collect_operands(inner, out),
        Expr::And(l, r) | Expr::Or(l, r) => {
            collect_operands(l, out);
            collect_operands(r, out);
        }
    }
}

/// Resolve every operand in a WHERE expression to a scope index. An aggregate
/// operand in WHERE is a fail-loud error.
fn resolve_where_operands(
    expr: &Expr,
    scope: &[Bound],
) -> Result<std::collections::HashMap<OperandKey, usize>, ExecError> {
    let mut operands = Vec::new();
    collect_operands(expr, &mut operands);
    let mut map = std::collections::HashMap::new();
    for op in operands {
        match op {
            Operand::Column(c) => {
                let idx = resolve_column(scope, c)?;
                map.insert(OperandKey::of(op), idx);
            }
            Operand::Aggregate { func, arg } => {
                let rendered = render_aggregate(*func, arg);
                return Err(ExecError::AggregateInWhere(rendered));
            }
        }
    }
    Ok(map)
}

/// Render an aggregate call for diagnostics, e.g. `COUNT(*)` or `SUM(amount)`.
fn render_aggregate(func: AggFunc, arg: &AggArg) -> String {
    let name = match func {
        AggFunc::Count => "COUNT",
        AggFunc::Sum => "SUM",
        AggFunc::Min => "MIN",
        AggFunc::Max => "MAX",
        AggFunc::Avg => "AVG",
    };
    match arg {
        AggArg::Star => format!("{name}(*)"),
        AggArg::Column(c) => format!("{name}({})", c.qualified_name()),
    }
}

/// One bound relation in the FROM/JOIN scope.
struct Bound {
    binding: String,
    schema: RelSchema,
    base: usize,
}

fn resolve_table(
    catalog: &dyn Catalog,
    table: &TableRef,
    default_schema: &str,
) -> Result<crate::catalog::SharedTable, ExecError> {
    let schema = table.schema.as_deref().unwrap_or(default_schema);
    catalog
        .resolve(schema, &table.table)
        .ok_or_else(|| ExecError::NoSuchTable {
            schema: schema.to_string(),
            table: table.table.clone(),
        })
}

/// Local column index within one table, honoring an optional qualifier.
fn local_index(binding: &str, schema: &RelSchema, col: &ColumnRef) -> Option<usize> {
    if let Some(q) = &col.qualifier {
        if q != binding {
            return None;
        }
    }
    schema.index_of(&col.name)
}

/// Resolve an `ON left = right` condition into `(from_local, join_local)` key
/// indices, accepting either column order.
fn resolve_join_keys(
    from_binding: &str,
    from_schema: &RelSchema,
    join_binding: &str,
    join_schema: &RelSchema,
    left: &ColumnRef,
    right: &ColumnRef,
) -> Result<(usize, usize), ExecError> {
    if let (Some(l), Some(r)) = (
        local_index(from_binding, from_schema, left),
        local_index(join_binding, join_schema, right),
    ) {
        return Ok((l, r));
    }
    if let (Some(r), Some(l)) = (
        local_index(from_binding, from_schema, right),
        local_index(join_binding, join_schema, left),
    ) {
        return Ok((r, l));
    }
    Err(ExecError::NoSuchColumn(format!(
        "{} = {}",
        left.name, right.name
    )))
}

/// Resolve a column reference to a global index into the joined row.
fn resolve_column(scope: &[Bound], col: &ColumnRef) -> Result<usize, ExecError> {
    let mut found: Option<usize> = None;
    for b in scope {
        if let Some(q) = &col.qualifier {
            if q != &b.binding {
                continue;
            }
        }
        if let Some(local) = b.schema.index_of(&col.name) {
            let global = b.base + local;
            if found.is_some() {
                return Err(ExecError::AmbiguousColumn(col.name.clone()));
            }
            found = Some(global);
        }
    }
    // A qualifier that matched no table at all is a distinct error.
    if found.is_none() {
        if let Some(q) = &col.qualifier {
            if !scope.iter().any(|b| &b.binding == q) {
                return Err(ExecError::UnknownQualifier(q.clone()));
            }
        }
    }
    found.ok_or_else(|| ExecError::NoSuchColumn(col.name.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MapCatalog;
    use crate::parser::parse;
    use crate::provider::InMemoryTable;
    use crate::types::{Column, ColumnType, RelSchema, Row, Value};
    use std::sync::Arc;

    fn catalog() -> MapCatalog {
        let users = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![
                Column::new("id", ColumnType::Int),
                Column::new("name", ColumnType::Text),
            ]),
            vec![
                Row::new(vec![Value::Int(1), Value::Text("alice".into())]),
                Row::new(vec![Value::Int(2), Value::Text("bob".into())]),
            ],
        ));
        let orders = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![
                Column::new("oid", ColumnType::Int),
                Column::new("uid", ColumnType::Int),
            ]),
            vec![
                Row::new(vec![Value::Int(10), Value::Int(1)]),
                Row::new(vec![Value::Int(11), Value::Int(1)]),
                Row::new(vec![Value::Int(12), Value::Int(2)]),
            ],
        ));
        MapCatalog::new()
            .with_table("public", "users", users)
            .with_table("public", "orders", orders)
    }

    fn run(sql: &str) -> QueryResult {
        execute(&parse(sql).unwrap(), &catalog(), "public", &[]).unwrap()
    }

    #[test]
    fn execute_substitutes_a_bound_parameter() {
        // The M1 join with `$1` substituted by Int(1) returns alice's two orders.
        let stmt = parse(
            "SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = $1",
        )
        .unwrap();
        let r = execute(&stmt, &catalog(), "public", &[Value::Int(1)]).unwrap();
        assert_eq!(r.rows.len(), 2);
        for row in &r.rows {
            assert_eq!(row.get(0), &Value::Text("alice".into()));
        }
        // A different bound value selects bob's single order.
        let r2 = execute(&stmt, &catalog(), "public", &[Value::Int(2)]).unwrap();
        assert_eq!(r2.rows.len(), 1);
        assert_eq!(r2.rows[0].get(0), &Value::Text("bob".into()));
    }

    /// Self-contained NULL / three-valued-logic known-answer corpus (oracle R11).
    /// No container, no Postgres — fixed expected results assert ferrosa's 3VL
    /// at the SELECT level: comparisons with NULL are UNKNOWN and excluded;
    /// AND/OR/NOT follow Kleene logic. Runs in the default test gate.
    #[test]
    fn null_3vl_known_answer_corpus() {
        // t(id int, v text NULL, n int NULL) with NULLs in v and/or n.
        let t = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![
                Column::new("id", ColumnType::Int),
                Column::new("v", ColumnType::Text),
                Column::new("n", ColumnType::Int),
            ]),
            vec![
                Row::new(vec![Value::Int(1), Value::Text("x".into()), Value::Int(10)]),
                Row::new(vec![Value::Int(2), Value::Null, Value::Int(20)]),
                Row::new(vec![Value::Int(3), Value::Text("y".into()), Value::Null]),
                Row::new(vec![Value::Int(4), Value::Null, Value::Null]),
            ],
        ));
        let cat = MapCatalog::new().with_table("public", "t", t);
        let ids = |sql: &str| -> Vec<i64> {
            let r = execute(&parse(sql).unwrap(), &cat, "public", &[]).unwrap();
            let mut v: Vec<i64> = r
                .rows
                .iter()
                .map(|row| match row.get(0) {
                    Value::Int(i) => *i,
                    other => panic!("expected int id, got {other:?}"),
                })
                .collect();
            v.sort_unstable();
            v
        };

        // Equality / inequality against a NULL operand is UNKNOWN -> row excluded.
        assert_eq!(ids("SELECT id FROM t WHERE v = 'x' ORDER BY id"), vec![1]);
        assert_eq!(ids("SELECT id FROM t WHERE v != 'x' ORDER BY id"), vec![3]);
        assert_eq!(ids("SELECT id FROM t WHERE n >= 20 ORDER BY id"), vec![2]);
        // AND: UNKNOWN unless the false branch short-circuits.
        assert_eq!(
            ids("SELECT id FROM t WHERE v = 'x' AND n = 10 ORDER BY id"),
            vec![1]
        );
        // OR: TRUE if either branch is TRUE, even when the other is UNKNOWN.
        assert_eq!(
            ids("SELECT id FROM t WHERE v = 'x' OR n = 20 ORDER BY id"),
            vec![1, 2]
        );
        // NOT UNKNOWN is UNKNOWN -> NULL rows still excluded.
        assert_eq!(
            ids("SELECT id FROM t WHERE NOT (v = 'x') ORDER BY id"),
            vec![3]
        );
    }

    /// v1 collation contract (oracle R10): ferrosa orders text by raw BYTE order
    /// (C / POSIX collation), NOT locale collation — so uppercase sorts before
    /// lowercase (`B`=0x42 < `a`=0x61), unlike an en_US locale. The differential
    /// oracle relies on this (its corpus uses C-collation-safe ASCII), so pin it
    /// with a self-contained known-answer test.
    #[test]
    fn text_order_by_is_c_collation_byte_order() {
        let t = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![Column::new("s", ColumnType::Text)]),
            vec![
                Row::new(vec![Value::Text("apple".into())]),
                Row::new(vec![Value::Text("Banana".into())]),
                Row::new(vec![Value::Text("apricot".into())]),
                Row::new(vec![Value::Text("Cherry".into())]),
            ],
        ));
        let cat = MapCatalog::new().with_table("public", "t", t);
        let r = execute(
            &parse("SELECT s FROM t ORDER BY s").unwrap(),
            &cat,
            "public",
            &[],
        )
        .unwrap();
        let got: Vec<String> = r
            .rows
            .iter()
            .map(|row| match row.get(0) {
                Value::Text(s) => s.clone(),
                other => panic!("expected text, got {other:?}"),
            })
            .collect();
        // Byte order: uppercase 'B'/'C' (0x42/0x43) precede lowercase 'a' (0x61).
        assert_eq!(got, vec!["Banana", "Cherry", "apple", "apricot"]);
    }

    #[test]
    fn missing_parameter_fails_loud() {
        let stmt = parse("SELECT name FROM users WHERE id = $1").unwrap();
        // No params bound ⇒ MissingParameter(1), not an empty/wrong result.
        let err = execute(&stmt, &catalog(), "public", &[]).unwrap_err();
        assert!(matches!(err, ExecError::MissingParameter(1)));
    }

    #[test]
    fn describe_returns_output_columns_for_a_param_query_without_binding() {
        // describe works with NO params bound: it returns only the column shape.
        let stmt = parse(
            "SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = $1",
        )
        .unwrap();
        let cols = describe(&stmt, &catalog(), "public").unwrap();
        assert_eq!(
            cols.iter()
                .map(|c| (c.name.as_str(), c.ty))
                .collect::<Vec<_>>(),
            [("name", ColumnType::Text), ("oid", ColumnType::Int)]
        );
    }

    #[test]
    fn describe_matches_execute_columns_for_aggregate() {
        let stmt = parse("SELECT region, COUNT(*) FROM sales GROUP BY region").unwrap();
        let described = describe(&stmt, &sales_catalog(), "public").unwrap();
        let executed = execute(&stmt, &sales_catalog(), "public", &[])
            .unwrap()
            .columns;
        assert_eq!(described, executed);
    }

    #[test]
    fn describe_fails_loud_on_unknown_table() {
        let stmt = parse("SELECT * FROM nope").unwrap();
        let err = describe(&stmt, &catalog(), "public").unwrap_err();
        assert!(matches!(err, ExecError::NoSuchTable { .. }));
    }

    #[test]
    fn select_star_returns_all_columns_and_rows() {
        let r = run("SELECT * FROM users");
        assert_eq!(
            r.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "name"]
        );
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn projection_selects_named_columns() {
        let r = run("SELECT name FROM users");
        assert_eq!(r.columns.len(), 1);
        assert_eq!(r.columns[0].name, "name");
        assert_eq!(r.rows[0], Row::new(vec![Value::Text("alice".into())]));
    }

    #[test]
    fn filter_restricts_rows() {
        let r = run("SELECT name FROM users WHERE id = 2");
        assert_eq!(r.rows, vec![Row::new(vec![Value::Text("bob".into())])]);
    }

    #[test]
    fn the_m1_join_returns_correct_rows() {
        let r =
            run("SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = 1");
        assert_eq!(
            r.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["name", "oid"]
        );
        assert_eq!(r.rows.len(), 2); // alice has two orders
        for row in &r.rows {
            assert_eq!(row.get(0), &Value::Text("alice".into()));
        }
        let oids: Vec<&Value> = r.rows.iter().map(|row| row.get(1)).collect();
        assert!(oids.contains(&&Value::Int(10)) && oids.contains(&&Value::Int(11)));
    }

    #[test]
    fn unknown_table_fails_loud() {
        let err = execute(
            &parse("SELECT * FROM nope").unwrap(),
            &catalog(),
            "public",
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::NoSuchTable { .. }));
    }

    #[test]
    fn unknown_column_fails_loud() {
        let err = execute(
            &parse("SELECT zzz FROM users").unwrap(),
            &catalog(),
            "public",
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::NoSuchColumn(_)));
    }

    fn sales_catalog() -> MapCatalog {
        // region (text), amount (nullable int)
        let sales = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![
                Column::new("region", ColumnType::Text),
                Column::new("amount", ColumnType::Int),
            ]),
            vec![
                Row::new(vec![Value::Text("east".into()), Value::Int(10)]),
                Row::new(vec![Value::Text("west".into()), Value::Int(5)]),
                Row::new(vec![Value::Text("east".into()), Value::Int(20)]),
                Row::new(vec![Value::Text("west".into()), Value::Null]),
            ],
        ));
        MapCatalog::new().with_table("public", "sales", sales)
    }

    fn run_sales(sql: &str) -> QueryResult {
        execute(&parse(sql).unwrap(), &sales_catalog(), "public", &[]).unwrap()
    }

    #[test]
    fn group_by_with_count_and_sum() {
        let r = run_sales("SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region");
        assert_eq!(
            r.columns
                .iter()
                .map(|c| (c.name.as_str(), c.ty))
                .collect::<Vec<_>>(),
            [
                ("region", ColumnType::Text),
                ("count", ColumnType::Int),
                ("sum", ColumnType::Int),
            ]
        );
        // east: count 2, sum 30; west: count 2, sum 5 (one NULL ignored).
        assert_eq!(
            r.rows[0].0,
            vec![Value::Text("east".into()), Value::Int(2), Value::Int(30)]
        );
        assert_eq!(
            r.rows[1].0,
            vec![Value::Text("west".into()), Value::Int(2), Value::Int(5)]
        );
    }

    #[test]
    fn avg_ungrouped_gives_fractional_float() {
        // AVG over the int `amount` column: (10 + 5 + 20) / 3 = 11.666...
        let r = run_sales("SELECT AVG(amount) FROM sales");
        assert_eq!(r.columns.len(), 1);
        assert_eq!(r.columns[0].name, "avg");
        assert_eq!(r.columns[0].ty, ColumnType::Float);
        let Value::Float(of) = &r.rows[0].0[0] else {
            panic!("expected float avg");
        };
        assert!((of.0 - 35.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn avg_grouped_output_types_and_values() {
        let r = run_sales("SELECT region, AVG(amount) FROM sales GROUP BY region");
        assert_eq!(
            r.columns
                .iter()
                .map(|c| (c.name.as_str(), c.ty))
                .collect::<Vec<_>>(),
            [("region", ColumnType::Text), ("avg", ColumnType::Float)]
        );
        // east: (10+20)/2 = 15.0; west: 5/1 = 5.0 (NULL ignored).
        assert_eq!(
            r.rows[0].0,
            vec![Value::Text("east".into()), Value::float(15.0)]
        );
        assert_eq!(
            r.rows[1].0,
            vec![Value::Text("west".into()), Value::float(5.0)]
        );
    }

    #[test]
    fn avg_unknown_arg_column_fails_loud() {
        // AVG's argument must resolve like any other aggregate argument.
        let err = execute(
            &parse("SELECT AVG(nope) FROM sales").unwrap(),
            &sales_catalog(),
            "public",
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::NoSuchColumn(_)));
    }

    #[test]
    fn ungrouped_count_star_over_table() {
        let r = run_sales("SELECT COUNT(*) FROM sales");
        assert_eq!(r.columns.len(), 1);
        assert_eq!(r.columns[0].name, "count");
        assert_eq!(r.rows, vec![Row::new(vec![Value::Int(4)])]);
    }

    #[test]
    fn min_max_aggregate() {
        let r = run_sales("SELECT MIN(amount), MAX(amount) FROM sales");
        assert_eq!(r.columns[0].name, "min");
        assert_eq!(r.columns[1].name, "max");
        assert_eq!(r.columns[0].ty, ColumnType::Int);
        assert_eq!(r.rows[0].0, vec![Value::Int(5), Value::Int(20)]);
    }

    #[test]
    fn order_by_asc_and_desc_with_null() {
        // ASC ⇒ NULLS LAST
        let r = run_sales("SELECT amount FROM sales ORDER BY amount ASC");
        assert_eq!(
            r.rows.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![Value::Int(5), Value::Int(10), Value::Int(20), Value::Null]
        );
        // DESC ⇒ NULLS FIRST
        let r = run_sales("SELECT amount FROM sales ORDER BY amount DESC");
        assert_eq!(
            r.rows.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![Value::Null, Value::Int(20), Value::Int(10), Value::Int(5)]
        );
    }

    #[test]
    fn order_by_non_selected_column() {
        // Order by amount but only select region.
        let r = run_sales("SELECT region FROM sales ORDER BY amount ASC");
        assert_eq!(r.columns.len(), 1);
        // First by amount asc: 5(west),10(east),20(east),NULL(west)
        assert_eq!(
            r.rows.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![
                Value::Text("west".into()),
                Value::Text("east".into()),
                Value::Text("east".into()),
                Value::Text("west".into()),
            ]
        );
    }

    #[test]
    fn limit_and_offset() {
        let r = run_sales("SELECT amount FROM sales ORDER BY amount ASC LIMIT 2 OFFSET 1");
        assert_eq!(
            r.rows.iter().map(|r| r.get(0).clone()).collect::<Vec<_>>(),
            vec![Value::Int(10), Value::Int(20)]
        );
    }

    #[test]
    fn non_grouped_column_fails_loud() {
        let err = execute(
            &parse("SELECT region, amount FROM sales GROUP BY region").unwrap(),
            &sales_catalog(),
            "public",
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::NotGrouped(_)));
    }

    #[test]
    fn order_by_ordinal_against_output() {
        let r = run_sales("SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY 2 DESC");
        // both groups have count 2; stable, so order preserved (east, west).
        // Make it discriminating: order by region desc via ordinal 1.
        let r2 = run_sales("SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY 1 DESC");
        assert_eq!(r2.rows[0].0[0], Value::Text("west".into()));
        assert_eq!(r2.rows[1].0[0], Value::Text("east".into()));
        // ordinal-2 sort still yields both rows.
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn order_by_ordinal_out_of_range_fails_loud() {
        let err = execute(
            &parse("SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY 5").unwrap(),
            &sales_catalog(),
            "public",
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::InvalidOrderBy(_)));
    }

    #[test]
    fn aggregate_with_where_and_join() {
        // users(id,name) JOIN orders(oid,uid); count orders per user, filtered.
        let r = execute(
            &parse(
                "SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.uid \
                 WHERE u.id = 1 GROUP BY u.name",
            )
            .unwrap(),
            &catalog(),
            "public",
            &[],
        )
        .unwrap();
        assert_eq!(r.columns[0].name, "name");
        assert_eq!(r.columns[1].name, "count");
        assert_eq!(
            r.rows,
            vec![Row::new(vec![Value::Text("alice".into()), Value::Int(2)])]
        );
    }

    #[test]
    fn ungrouped_aggregate_over_empty_filter_yields_one_row() {
        let r = run_sales("SELECT COUNT(*), SUM(amount) FROM sales WHERE amount > 1000");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].0, vec![Value::Int(0), Value::Null]);
    }

    // region (text), amount (nullable int) — one row has a NULL amount so a
    // comparison on it yields UNKNOWN under Kleene logic.
    fn nullable_catalog() -> MapCatalog {
        let t = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![
                Column::new("a", ColumnType::Int),
                Column::new("b", ColumnType::Int),
            ]),
            vec![
                Row::new(vec![Value::Int(1), Value::Int(2)]),
                Row::new(vec![Value::Int(1), Value::Int(9)]),
                Row::new(vec![Value::Int(5), Value::Int(2)]),
                Row::new(vec![Value::Int(1), Value::Null]),
            ],
        ));
        MapCatalog::new().with_table("public", "t", t)
    }

    fn run_t(sql: &str) -> QueryResult {
        execute(&parse(sql).unwrap(), &nullable_catalog(), "public", &[]).unwrap()
    }

    #[test]
    fn where_and_filters_rows() {
        // a = 1 AND b = 2 ⇒ only row (1,2).
        let r = run_t("SELECT a, b FROM t WHERE a = 1 AND b = 2");
        assert_eq!(r.rows, vec![Row::new(vec![Value::Int(1), Value::Int(2)])]);
    }

    #[test]
    fn where_or_filters_rows() {
        // a = 5 OR b = 9 ⇒ rows (1,9) and (5,2).
        let r = run_t("SELECT a, b FROM t WHERE a = 5 OR b = 9");
        assert_eq!(r.rows.len(), 2);
        assert!(r
            .rows
            .contains(&Row::new(vec![Value::Int(1), Value::Int(9)])));
        assert!(r
            .rows
            .contains(&Row::new(vec![Value::Int(5), Value::Int(2)])));
    }

    #[test]
    fn where_not_filters_rows() {
        // NOT a = 1 ⇒ only the row with a = 5.
        let r = run_t("SELECT a, b FROM t WHERE NOT a = 1");
        assert_eq!(r.rows, vec![Row::new(vec![Value::Int(5), Value::Int(2)])]);
    }

    #[test]
    fn where_parentheses_override_precedence() {
        // (a = 1 OR a = 5) AND b = 2 ⇒ rows (1,2) and (5,2).
        let r = run_t("SELECT a, b FROM t WHERE (a = 1 OR a = 5) AND b = 2");
        assert_eq!(r.rows.len(), 2);
        assert!(r
            .rows
            .contains(&Row::new(vec![Value::Int(1), Value::Int(2)])));
        assert!(r
            .rows
            .contains(&Row::new(vec![Value::Int(5), Value::Int(2)])));
    }

    #[test]
    fn where_null_unknown_row_is_excluded() {
        // The (1, NULL) row: `b = 2` is UNKNOWN, so `a = 1 AND b = 2` is
        // UNKNOWN ⇒ excluded (only the literal (1,2) row qualifies).
        let r = run_t("SELECT a, b FROM t WHERE a = 1 AND b = 2");
        assert_eq!(r.rows.len(), 1);
        // And NOT (b = 2) over the NULL row stays UNKNOWN ⇒ excluded too.
        let r2 = run_t("SELECT a, b FROM t WHERE NOT b = 2");
        // b = 2 is true for (1,2) and (5,2); NOT ⇒ those drop. (1,9) keeps,
        // (1,NULL) is UNKNOWN ⇒ excluded. So exactly one row: (1,9).
        assert_eq!(r2.rows, vec![Row::new(vec![Value::Int(1), Value::Int(9)])]);
    }

    #[test]
    fn where_or_with_null_can_still_pass() {
        // (b = 9) OR (a = 1): for (1,NULL), b=9 is UNKNOWN but a=1 is true ⇒
        // Kleene OR ⇒ true, row kept. All three a=1 rows plus none else.
        let r = run_t("SELECT a, b FROM t WHERE b = 9 OR a = 1");
        assert_eq!(r.rows.len(), 3); // (1,2),(1,9),(1,NULL)
    }

    #[test]
    fn aggregate_in_where_fails_loud() {
        let err = execute(
            &parse("SELECT a FROM t WHERE COUNT(*) > 1").unwrap(),
            &nullable_catalog(),
            "public",
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::AggregateInWhere(_)));
    }

    #[test]
    fn having_count_filters_groups() {
        // east: count 2, west: count 2 in the sales catalog. HAVING COUNT(*) > 1
        // keeps both; > 2 keeps none.
        let r = run_sales("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 1");
        assert_eq!(r.rows.len(), 2);
        let none =
            run_sales("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 2");
        assert!(none.rows.is_empty());
    }

    #[test]
    fn having_on_sum_filters_groups() {
        // east sum 30, west sum 5. HAVING SUM(amount) > 10 keeps only east.
        let r = run_sales(
            "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 10",
        );
        assert_eq!(
            r.rows,
            vec![Row::new(vec![Value::Text("east".into()), Value::Int(30)])]
        );
    }

    #[test]
    fn having_aggregate_not_in_select_list_is_still_computed() {
        // SELECT only region + COUNT(*), but HAVING filters on SUM(amount),
        // which is absent from the SELECT list and must still be computed.
        let r =
            run_sales("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING SUM(amount) > 10");
        // Only east (sum 30 > 10); output has just region + count columns.
        assert_eq!(r.columns.len(), 2);
        assert_eq!(
            r.rows,
            vec![Row::new(vec![Value::Text("east".into()), Value::Int(2)])]
        );
    }

    #[test]
    fn having_without_group_by_is_whole_table_group() {
        // No GROUP BY: one whole-table group. COUNT(*) is 4 > 3 ⇒ one row kept.
        let r = run_sales("SELECT COUNT(*) FROM sales HAVING COUNT(*) > 3");
        assert_eq!(r.rows, vec![Row::new(vec![Value::Int(4)])]);
        // COUNT(*) is 4, not > 10 ⇒ zero rows.
        let none = run_sales("SELECT COUNT(*) FROM sales HAVING COUNT(*) > 10");
        assert!(none.rows.is_empty());
    }

    #[test]
    fn having_non_grouped_column_fails_loud() {
        // `amount` is neither a group column nor an aggregate ⇒ NotGrouped.
        let err = execute(
            &parse("SELECT region FROM sales GROUP BY region HAVING amount > 1").unwrap(),
            &sales_catalog(),
            "public",
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::NotGrouped(_)));
    }

    #[test]
    fn select_distinct_dedups_rows() {
        // region has duplicates (east, west each twice) ⇒ DISTINCT yields 2 rows
        // in first-occurrence order.
        let r = run_sales("SELECT DISTINCT region FROM sales");
        assert_eq!(
            r.rows,
            vec![
                Row::new(vec![Value::Text("east".into())]),
                Row::new(vec![Value::Text("west".into())]),
            ]
        );
    }

    #[test]
    fn distinct_with_order_by_resolves_against_output() {
        // DISTINCT region ORDER BY region DESC ⇒ west, east.
        let r = run_sales("SELECT DISTINCT region FROM sales ORDER BY region DESC");
        assert_eq!(
            r.rows,
            vec![
                Row::new(vec![Value::Text("west".into())]),
                Row::new(vec![Value::Text("east".into())]),
            ]
        );
    }

    #[test]
    fn ambiguous_unqualified_column_fails_loud() {
        // both users and orders would need a shared column; craft one:
        let shared = Arc::new(InMemoryTable::new(
            RelSchema::new(vec![Column::new("x", ColumnType::Int)]),
            vec![Row::new(vec![Value::Int(1)])],
        ));
        let cat = MapCatalog::new()
            .with_table("public", "a", shared.clone())
            .with_table("public", "b", shared);
        let err = execute(
            &parse("SELECT x FROM a JOIN b ON a.x = b.x").unwrap(),
            &cat,
            "public",
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::AmbiguousColumn(_)));
    }
}
