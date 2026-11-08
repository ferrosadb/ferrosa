//! Module: enforced column and table constraints — `NOT NULL` (and `CHECK`, below).
//!
//! A constraint that is parsed but not enforced is the worst outcome: the client believes the
//! table holds no NULL and it does. `CREATE TABLE … worker text NOT NULL` used to be parsed
//! and dropped for every non-key column (see `oban-compat.md`, "Found while building W3").
//! The rule now (Lean `Ferrosa.Sql.Constraint`): a write is admitted only if the row it
//! produces satisfies every constraint of its table, else it is refused and writes nothing
//! (`admitted_writes_preserve_constraints`).
//!
//! * `NOT NULL` is recorded as the table extension `pg.not_null.<column>` = `true` (an empty
//!   value is "nullable", since extensions cannot be deleted). Key columns are NOT NULL by
//!   construction and need no record. An `INSERT` that omits such a column, or writes NULL to
//!   it, and an `UPDATE` that sets it to NULL, are refused with `23502`. (Column DEFAULTs are
//!   applied by the INSERT path before this check once that lands, W2.)
//!
//! * `CHECK (<expr>)` is recorded as `pg.check.<name>` = the expression's canonical SQL (an
//!   empty value is a dropped constraint). Every write evaluates every CHECK on the COMPLETE
//!   new row (an UPDATE reads the current row first when the table has a CHECK, so a
//!   cross-column check like `attempt between 0 and max_attempts` sees real values). A check
//!   that is false refuses the write with `23514`; one that is NULL (unknown) passes, as in
//!   PostgreSQL (`unknown_check_passes`). Evaluation is W1's expression engine: the check is
//!   compiled as `SELECT (<check>) FROM <one-row table>` and run through
//!   `ferrosa_sql::execute_streaming`. `NOT VALID` skips validating existing rows only; new
//!   writes are always checked (`not_valid_still_checks_new_writes`).
//! * [`check_row`] is the single entry point a write path calls with the complete new row:
//!   enum labels, `NOT NULL`, then every `CHECK`.
//!
//! Last revised: 2026-11-08

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use ferrosa_schema::{Schema, TableMetadata};
use ferrosa_sql::{Column, InMemoryTable, MapCatalog, RelSchema, Row, RowSink, Value};

use crate::messages::BackendMessage;
use crate::query::error_response;

const NOT_NULL_PREFIX: &str = "pg.not_null.";
const CHECK_PREFIX: &str = "pg.check.";

/// The extension recording `column` as `NOT NULL`.
pub(crate) fn not_null_extension(column: &str) -> (String, String) {
    (format!("{NOT_NULL_PREFIX}{column}"), "true".to_string())
}

/// Whether `column` of `meta` is recorded `NOT NULL`.
pub(crate) fn is_not_null(meta: &TableMetadata, column: &str) -> bool {
    meta.extensions
        .get(&format!("{NOT_NULL_PREFIX}{column}"))
        .is_some_and(|v| v == "true")
}

fn null_violation(meta: &TableMetadata, column: &str) -> BackendMessage {
    error_response(
        "23502",
        &format!(
            "null value in column \"{column}\" of relation \"{}\" violates not-null constraint",
            meta.name
        ),
    )
}

/// Refuse writing `value` to a `NOT NULL` column when it is NULL.
pub(crate) fn check_column_value(
    meta: &TableMetadata,
    column: &str,
    value: &Value,
) -> Result<(), BackendMessage> {
    if matches!(value, Value::Null) && is_not_null(meta, column) {
        return Err(null_violation(meta, column));
    }
    Ok(())
}

/// Refuse an inserted row that omits, or sets NULL in, a `NOT NULL` column. `supplied` holds
/// the values the INSERT gave; an absent column is NULL.
pub(crate) fn check_inserted_row(
    meta: &TableMetadata,
    supplied: &HashMap<String, Value>,
) -> Result<(), BackendMessage> {
    for column in meta.columns.keys() {
        if !is_not_null(meta, column) {
            continue;
        }
        match supplied.get(column) {
            None | Some(Value::Null) => return Err(null_violation(meta, column)),
            Some(_) => {}
        }
    }
    Ok(())
}

/// The extension recording CHECK constraint `name` as `sql` (`""` drops it).
pub(crate) fn check_extension(name: &str, sql: &str) -> (String, String) {
    (format!("{CHECK_PREFIX}{name}"), sql.to_string())
}

/// The live CHECK constraints of `meta`, as `(name, sql)`, in name order.
pub(crate) fn checks(meta: &TableMetadata) -> Vec<(&str, &str)> {
    let mut out: Vec<(&str, &str)> = meta
        .extensions
        .iter()
        .filter_map(|(key, sql)| {
            let name = key.strip_prefix(CHECK_PREFIX)?;
            (!sql.is_empty()).then_some((name, sql.as_str()))
        })
        .collect();
    out.sort_unstable();
    out
}

/// Whether CHECK constraint `name` is live on `meta`.
pub(crate) fn has_check(meta: &TableMetadata, name: &str) -> bool {
    meta.extensions
        .get(&format!("{CHECK_PREFIX}{name}"))
        .is_some_and(|sql| !sql.is_empty())
}

/// The single entry point for a write: validate the COMPLETE new row (`row`; an absent
/// column is NULL) against the table's enum columns, `NOT NULL` columns and CHECKs. The
/// values must already be of their columns' types (as the write path coerced them; see
/// `query::typed_row_value`), not raw literals.
pub(crate) fn check_row(
    schema: &Schema,
    meta: &TableMetadata,
    row: &HashMap<String, Value>,
) -> Result<(), BackendMessage> {
    let view = schema.snapshot();
    for (column, value) in row {
        if crate::pg_enum::column_enum(meta, column).is_some() {
            crate::pg_enum::check_value(&view, meta, column, value)?;
        }
    }
    check_inserted_row(meta, row)?;
    check_checks(schema, meta, row)
}

/// Evaluate every CHECK of `meta` on `row`.
pub(crate) fn check_checks(
    schema: &Schema,
    meta: &TableMetadata,
    row: &HashMap<String, Value>,
) -> Result<(), BackendMessage> {
    let constraints = checks(meta);
    if constraints.is_empty() {
        return Ok(());
    }
    let table = one_row_table(schema, meta, row)?;
    for (name, sql) in constraints {
        if evaluate(&table, sql)? == Some(false) {
            return Err(error_response(
                "23514",
                &format!(
                    "new row for relation \"{}\" violates check constraint \"{name}\"",
                    meta.name
                ),
            ));
        }
    }
    Ok(())
}

const ROW_TABLE: &str = "__pg_check_row";

/// The candidate row as a one-row relation named [`ROW_TABLE`], typed like the table.
fn one_row_table(
    schema: &Schema,
    meta: &TableMetadata,
    row: &HashMap<String, Value>,
) -> Result<Arc<InMemoryTable>, BackendMessage> {
    let mut columns = Vec::with_capacity(meta.columns.len());
    let mut values = Vec::with_capacity(meta.columns.len());
    for col in meta.columns.values() {
        let pg = crate::pg_types::pg_type_of_column(&col.column_type, &meta.keyspace, schema)
            .map_err(|e| error_response("42704", &e.to_string()))?;
        columns.push(Column::new(&col.name, pg.column_type));
        // The relation owns its row; the caller keeps the row it will write.
        values.push(row.get(&col.name).cloned().unwrap_or(Value::Null));
    }
    Ok(Arc::new(InMemoryTable::new(
        RelSchema::new(columns),
        vec![Row::new(values)],
    )))
}

/// The first value of the first row a query produces, streamed (nothing collected).
#[derive(Default)]
struct FirstValue(Option<Value>);

impl RowSink for FirstValue {
    fn columns(&mut self, _columns: &[Column]) -> ControlFlow<()> {
        ControlFlow::Continue(())
    }

    fn row(&mut self, row: Row) -> ControlFlow<()> {
        self.0 = row.0.into_iter().next();
        ControlFlow::Break(())
    }
}

/// Evaluate boolean expression `sql` over the one-row relation: `Some(b)`, or `None` for
/// NULL (unknown). A non-boolean result or an evaluation error is reported, never passed.
fn evaluate(table: &Arc<InMemoryTable>, sql: &str) -> Result<Option<bool>, BackendMessage> {
    let stmt = ferrosa_sql::parse(&format!("SELECT ({sql}) FROM {ROW_TABLE}"))
        .map_err(|e| error_response("XX000", &format!("stored CHECK does not parse: {e}")))?;
    let catalog = MapCatalog::new().with_table("pg_check", ROW_TABLE, Arc::clone(table) as _);
    let mut sink = FirstValue::default();
    ferrosa_sql::execute_streaming(
        &stmt,
        &catalog,
        "pg_check",
        &[],
        &ferrosa_sql::SpillCtx::default(),
        &mut sink,
    )
    .map_err(|e| crate::query::exec_error_response(&e))?;
    match sink.0 {
        Some(Value::Bool(b)) => Ok(Some(b)),
        Some(Value::Null) | None => Ok(None),
        Some(other) => Err(error_response(
            "42804",
            &format!("CHECK expression \"{sql}\" is not boolean (got {other:?})"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta_with_check(sql: &str) -> (Schema, TableMetadata) {
        let schema = crate::catalog::test_support::empty_schema();
        let mut meta = crate::pg_enum::meta_table("public");
        meta.columns.insert(
            "n".into(),
            ferrosa_schema::ColumnMetadata {
                name: "n".into(),
                kind: ferrosa_schema::ColumnKind::Regular,
                position: 0,
                column_type: "int".into(),
                clustering_order: ferrosa_schema::ClusteringOrder::None,
                mask: None,
            },
        );
        let (k, v) = check_extension("positive", sql);
        meta.extensions.insert(k, v);
        (schema, meta)
    }

    fn row(n: Value) -> HashMap<String, Value> {
        HashMap::from([("n".to_string(), n)])
    }

    /// False refuses (23514), true admits, NULL (unknown) admits.
    #[test]
    fn checks_follow_three_valued_logic() {
        let (schema, meta) = meta_with_check("n > 0");
        assert!(check_checks(&schema, &meta, &row(Value::Int(1))).is_ok());
        assert!(check_checks(&schema, &meta, &row(Value::Null)).is_ok());
        let Err(BackendMessage::ErrorResponse { fields }) =
            check_checks(&schema, &meta, &row(Value::Int(0)))
        else {
            panic!("n > 0 must refuse 0");
        };
        assert!(fields.contains(&(b'C', "23514".to_string())));
    }

    #[test]
    fn a_dropped_check_no_longer_applies() {
        let (schema, mut meta) = meta_with_check("n > 0");
        let (k, v) = check_extension("positive", "");
        meta.extensions.insert(k, v);
        assert!(checks(&meta).is_empty());
        assert!(check_checks(&schema, &meta, &row(Value::Int(0))).is_ok());
    }
}
