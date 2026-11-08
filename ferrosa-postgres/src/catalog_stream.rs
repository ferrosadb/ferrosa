//! Module: `pg_catalog` relations served as lazily generated streams.
//!
//! The relations in [`crate::catalog`] are built whole into `InMemoryTable`s. The ones here
//! are generated on pull instead: a scan holds the schema snapshot (shared, not copied)
//! and the names of the objects it will visit, and builds each row only when the executor
//! asks for it, so no result set is collected to be forwarded.
//!
//! * `pg_description` — one row per commented table: `objoid` (the table's `pg_class`
//!   OID), `classoid` = 1259 (`pg_class`), `objsubid` = 0, `description`. A table's comment
//!   is `COMMENT ON TABLE` ([`crate::ddl::COMMENT_EXTENSION`]); an empty comment is no
//!   comment, as in PostgreSQL.
//!
//! Last revised: 2026-11-08

use std::sync::Arc;

use ferrosa_schema::{Schema, SchemaSnapshot};
use ferrosa_sql::{Column, ColumnType, RelSchema, Row, SharedTable, TableProvider, Value};

use crate::catalog::relation_oid;
use crate::ddl::COMMENT_EXTENSION;

/// `pg_class`'s own OID, the `classoid` of a table's description.
const PG_CLASS_OID: i64 = 1259;

/// The streamed relation `name`, or `None` when it is not one of this module's.
pub(crate) fn streamed_catalog_table(name: &str, schema: &Schema) -> Option<SharedTable> {
    match name {
        "pg_description" => Some(Arc::new(PgDescription::new(schema.snapshot()))),
        _ => None,
    }
}

/// The comment of a table, when it has a non-empty one.
pub(crate) fn table_comment(meta: &ferrosa_schema::TableMetadata) -> Option<&str> {
    meta.extensions
        .get(COMMENT_EXTENSION)
        .map(String::as_str)
        .filter(|text| !text.is_empty())
}

/// `pg_catalog.pg_description`.
pub(crate) struct PgDescription {
    schema: RelSchema,
    snapshot: Arc<SchemaSnapshot>,
    /// The commented tables, in name order: the scan's cursor runs over these keys and
    /// builds each row on pull.
    commented: Arc<[(String, String)]>,
}

impl PgDescription {
    fn new(snapshot: Arc<SchemaSnapshot>) -> Self {
        let mut commented: Vec<(String, String)> = snapshot
            .tables
            .iter()
            .filter(|(_, meta)| table_comment(meta).is_some())
            .map(|(key, _)| key.clone()) // a name key per row, not the row
            .collect();
        commented.sort();
        Self {
            schema: RelSchema::new(vec![
                Column::new("objoid", ColumnType::Int),
                Column::new("classoid", ColumnType::Int),
                Column::new("objsubid", ColumnType::Int),
                Column::new("description", ColumnType::Text),
            ]),
            snapshot,
            commented: commented.into(),
        }
    }
}

impl TableProvider for PgDescription {
    fn schema(&self) -> &RelSchema {
        &self.schema
    }

    fn scan(&self) -> Box<dyn Iterator<Item = Row> + Send> {
        Box::new(DescriptionRows {
            snapshot: Arc::clone(&self.snapshot),
            commented: Arc::clone(&self.commented),
            next: 0,
        })
    }
}

/// The pull cursor over [`PgDescription`].
struct DescriptionRows {
    snapshot: Arc<SchemaSnapshot>,
    commented: Arc<[(String, String)]>,
    next: usize,
}

impl Iterator for DescriptionRows {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        let (keyspace, table) = self.commented.get(self.next)?;
        self.next += 1;
        let comment = self
            .snapshot
            .tables
            // The map is keyed by an owned (String, String) pair, so a lookup needs one.
            .get(&(keyspace.clone(), table.clone()))
            .and_then(table_comment)?;
        Some(Row::new(vec![
            Value::Int(i64::from(relation_oid(keyspace, table))),
            Value::Int(PG_CLASS_OID),
            Value::Int(0),
            Value::Text(comment.to_string().into()),
        ]))
    }
}
