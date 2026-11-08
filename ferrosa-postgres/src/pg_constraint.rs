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
//! Last revised: 2026-11-08

use std::collections::HashMap;

use ferrosa_schema::TableMetadata;
use ferrosa_sql::Value;

use crate::messages::BackendMessage;
use crate::query::error_response;

const NOT_NULL_PREFIX: &str = "pg.not_null.";

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
