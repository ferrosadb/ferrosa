//! Module: `INSERT` fills the columns it omits (lane W2).
//!
//! An omitted column takes, in order:
//!
//! 1. a D15 id from [`crate::serial_id::next_id`] when it is `bigserial`;
//! 2. its recorded `DEFAULT` ([`crate::pg_default::column_default`]), compiled
//!    once per statement by `ferrosa_sql::compile_default` and evaluated per row
//!    at the statement's time;
//! 3. otherwise NULL, which the NOT NULL check then judges.
//!
//! A column the statement names keeps its value, NULL included: an explicit NULL
//! is a value, not a request for the default (`Dml.lean`, `explicit_value_wins`,
//! `omitted_column_takes_default`, `omitted_without_default_is_null`). The filled
//! values reach the constraint check, the storage row and `RETURNING`, like the
//! supplied ones.
//!
//! Last revised: 2026-11-08

use std::collections::HashMap;

use ferrosa_common::CqlValue;
use ferrosa_schema::{ColumnKind, Schema, TableMetadata};
use ferrosa_sql::{DefaultExpr, Value as SqlValue};

use crate::messages::BackendMessage;
use crate::query::{error_response, value_to_cql};

/// How one omitted column is filled.
enum Fill {
    Serial,
    Default(DefaultExpr),
}

/// The fills of one `INSERT` statement, compiled once.
pub(crate) struct InsertDefaults {
    fills: Vec<(String, Fill)>,
}

/// What a fill writes into, for one row.
pub(crate) struct RowFill<'a> {
    pub(crate) col_values: &'a mut HashMap<String, CqlValue>,
    pub(crate) sql_values: &'a mut HashMap<String, SqlValue>,
    pub(crate) regular_cells: &'a mut Vec<(u16, CqlValue)>,
}

/// The table and session a fill resolves types and time against.
pub(crate) struct FillCtx<'a> {
    pub(crate) meta: &'a TableMetadata,
    pub(crate) schema: &'a Schema,
    pub(crate) ks: &'a str,
    pub(crate) timestamp_micros: i64,
    pub(crate) jsonb_limits: &'a ferrosa_jsonb::Limits,
}

impl InsertDefaults {
    /// Compile the fill of every column of `meta` that `supplied` omits.
    ///
    /// # Errors
    ///
    /// A recorded DEFAULT that does not compile: never skipped.
    pub(crate) fn compile(
        meta: &TableMetadata,
        supplied: &[String],
    ) -> Result<Self, BackendMessage> {
        let mut fills = Vec::new();
        for column in meta.columns.keys() {
            if supplied.iter().any(|s| s == column) {
                continue;
            }
            if crate::pg_default::is_bigserial(meta, column) {
                fills.push((column.clone(), Fill::Serial));
            } else if let Some(sql) = crate::pg_default::column_default(meta, column) {
                let compiled = ferrosa_sql::compile_default(sql)
                    .map_err(|e| crate::query::exec_error_response(&e))?;
                fills.push((column.clone(), Fill::Default(compiled)));
            }
        }
        Ok(Self { fills })
    }

    /// Fill one row's omitted columns.
    ///
    /// # Errors
    ///
    /// The DEFAULT's evaluation error, or a value its column cannot store.
    pub(crate) fn apply(
        &self,
        ctx: &FillCtx<'_>,
        row: &mut RowFill<'_>,
    ) -> Result<(), BackendMessage> {
        for (column, fill) in &self.fills {
            debug_assert!(
                !row.col_values.contains_key(column),
                "a supplied column is never filled"
            );
            let value = match fill {
                Fill::Serial => SqlValue::Int(crate::serial_id::next_id()),
                Fill::Default(expr) => expr
                    .eval(ctx.timestamp_micros)
                    .map_err(|e| crate::query::exec_error_response(&e))?,
            };
            let meta = &ctx.meta.columns[column];
            let ty = ferrosa_row_bridge::parse_cql_type_in_keyspace(
                &meta.column_type,
                ctx.ks,
                ctx.schema,
            )
            .map_err(|e| error_response("42704", &e.to_string()))?;
            let cql = value_to_cql(&value, &ty, ctx.jsonb_limits)?;
            if matches!(meta.kind, ColumnKind::Regular | ColumnKind::Static) {
                let index = ctx.meta.storage_column_index(column).ok_or_else(|| {
                    error_response(
                        "42703",
                        &format!("column \"{column}\" not found in storage schema"),
                    )
                })?;
                row.regular_cells.push((index, cql.clone()));
            }
            // RETURNING reports the value as stored.
            let stored = crate::storage_provider::cql_to_value(&cql)
                .map_err(|e| error_response("XX000", &e))?;
            row.col_values.insert(column.clone(), cql);
            row.sql_values.insert(column.clone(), stored);
        }
        Ok(())
    }
}
