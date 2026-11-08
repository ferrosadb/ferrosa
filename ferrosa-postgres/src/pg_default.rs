//! Module: how column DEFAULTs and `bigserial` columns are recorded in the schema.
//!
//! Shared by DDL (W3, which records them) and INSERT (W2, which applies them), so the two
//! cannot disagree about the keys. Both live as table extensions, like the declared key
//! (`pg_key`) and enum columns (`pg_enum`):
//!
//! * `pg.column_default.<column>` = the default expression's SQL text. The text is rendered
//!   by the DDL parser from its tokens, so it is canonical SQL (parse it; never compare it
//!   byte-for-byte with what the client sent). An empty value means "no default" — table
//!   extensions can be set but not deleted, so `ALTER COLUMN … DROP DEFAULT` writes "".
//! * `pg.column_serial.<column>` = `bigserial`: the column's value, when an INSERT omits it,
//!   is a D15 id from [`crate::serial_id`]. `serial`/`smallserial` are refused at DDL time.
//!
//! Last revised: 2026-11-08

use std::collections::HashMap;

use ferrosa_schema::TableMetadata;

/// Prefix of a column's DEFAULT extension.
pub(crate) const DEFAULT_PREFIX: &str = "pg.column_default.";
/// Prefix of a `bigserial` column's extension.
pub(crate) const SERIAL_PREFIX: &str = "pg.column_serial.";
/// The one serial kind recorded (D15).
pub(crate) const BIGSERIAL: &str = "bigserial";

/// The extension recording `sql_text` as `column`'s DEFAULT (`""` clears it).
pub(crate) fn default_extension(column: &str, sql_text: &str) -> (String, String) {
    (format!("{DEFAULT_PREFIX}{column}"), sql_text.to_string())
}

/// The extension marking `column` as `bigserial`.
pub(crate) fn serial_extension(column: &str) -> (String, String) {
    (format!("{SERIAL_PREFIX}{column}"), BIGSERIAL.to_string())
}

/// `column`'s DEFAULT expression text, if it has one.
// Read by INSERT when applying defaults (W2 lane, feat/pg-w2-dml); here only by tests.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn column_default<'a>(meta: &'a TableMetadata, column: &str) -> Option<&'a str> {
    meta.extensions
        .get(&format!("{DEFAULT_PREFIX}{column}"))
        .map(String::as_str)
        .filter(|text| !text.is_empty())
}

/// Whether `column` is a `bigserial` column.
// Read by INSERT when applying defaults (W2 lane, feat/pg-w2-dml); here only by tests.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn is_bigserial(meta: &TableMetadata, column: &str) -> bool {
    meta.extensions
        .get(&format!("{SERIAL_PREFIX}{column}"))
        .is_some_and(|kind| kind == BIGSERIAL)
}

/// Every DEFAULT of `meta`, by column (empty texts excluded).
// Read by INSERT when applying defaults (W2 lane, feat/pg-w2-dml); here only by tests.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn all_defaults(meta: &TableMetadata) -> HashMap<&str, &str> {
    meta.extensions
        .iter()
        .filter_map(|(key, text)| {
            let column = key.strip_prefix(DEFAULT_PREFIX)?;
            (!text.is_empty()).then_some((column, text.as_str()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta_with(extensions: Vec<(String, String)>) -> TableMetadata {
        TableMetadata {
            keyspace: "public".to_string(),
            name: "t".to_string(),
            id: uuid::Uuid::new_v4(),
            columns: Default::default(),
            partition_key: Vec::new(),
            clustering_key: Vec::new(),
            params: Default::default(),
            flags: Default::default(),
            extensions: extensions.into_iter().collect(),
            is_system: false,
        }
    }

    #[test]
    fn defaults_and_serials_round_trip() {
        let meta = meta_with(vec![
            default_extension("queue", "'default'"),
            default_extension("gone", ""),
            serial_extension("id"),
        ]);
        assert_eq!(column_default(&meta, "queue"), Some("'default'"));
        assert_eq!(
            column_default(&meta, "gone"),
            None,
            "an empty default is no default"
        );
        assert_eq!(column_default(&meta, "id"), None);
        assert!(is_bigserial(&meta, "id"));
        assert!(!is_bigserial(&meta, "queue"));
        assert_eq!(all_defaults(&meta).len(), 1);
    }
}
