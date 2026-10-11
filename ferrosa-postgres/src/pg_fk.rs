//! Enforced PostgreSQL `FOREIGN KEY` constraints, recorded on the child table and
//! checked as **lookups** — never as scans.
//!
//! An FK that parses but is not enforced is a lie, and this front end does not ship
//! those: every constraint recorded here is checked on the write that could violate it,
//! and a constraint this module cannot enforce is refused by name at `ADD` time (see
//! `ddl::execute_add_foreign_key`).
//!
//! ## What is recorded and where
//!
//! A constraint lives in the child table's [`TableMetadata::extensions`] under
//! [`FOREIGN_KEY_PREFIX`]`<name>`, exactly as the *declared* primary key lives under
//! `pg.primary_key` (see [`crate::pg_key`]). Extension entries MERGE rather than replace,
//! so several `pg.foreign_key.*` keys coexist with `pg.primary_key` and with each other.
//! Nothing is added to [`TableMetadata`] itself.
//!
//! ## Direction and the two indexes
//!
//! - **Child side** (an `INSERT`/`UPDATE` on the child): the parent row must exist. The
//!   parent is looked up by the referenced column — a point read when that column is the
//!   parent's storage key, otherwise through the secondary index `ADD PRIMARY KEY` built
//!   over it (`<parent>_pkey`). Either way it is a lookup.
//! - **Parent side** (a `DELETE`/`UPDATE` of the parent): no child may still reference it.
//!   This needs an index over the CHILD's referencing column, so `ADD FOREIGN KEY` creates
//!   one over that column through the same `create_index` path `ADD PRIMARY KEY` uses.
//!
//! Secondary indexes are **single-column only** (`build_replicated_index` covers
//! `target_columns.first()`), so a multi-column FK or a multi-column referenced key is
//! refused by name rather than recorded and mis-enforced.

use std::collections::HashMap;

use ferrosa_common::CqlValue;
use ferrosa_schema::{Schema, SchemaSnapshot, TableMetadata};
use ferrosa_storage::{StorageEngine, TableId};

/// Extension-key prefix for a recorded foreign key. The constraint name follows it.
pub const FOREIGN_KEY_PREFIX: &str = "pg.foreign_key.";

/// A foreign key recorded on a child table, resolved to single columns.
///
/// Both `child_column` and `parent_column` are single names: the index machinery this
/// enforcement is built on is single-column, and a multi-column constraint is refused
/// before it is ever recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    /// The constraint name, as written or as PostgreSQL would auto-name it.
    pub name: String,
    /// The referencing (child) column.
    pub child_column: String,
    /// The keyspace both tables live in (PostgreSQL FKs do not span databases).
    pub keyspace: String,
    /// The referenced (parent) table.
    pub parent_table: String,
    /// The referenced (parent) column.
    pub parent_column: String,
}

impl ForeignKey {
    /// The secondary index over the CHILD column that the parent-side check uses.
    ///
    /// Named after the constraint, which is unique per table — the same namespace the
    /// `<parent>_pkey` index already lives in.
    pub fn child_index_name(&self) -> String {
        self.name.clone()
    }
}

/// The extension key a constraint name is stored under.
pub fn extension_key(name: &str) -> String {
    format!("{FOREIGN_KEY_PREFIX}{name}")
}

/// The PostgreSQL default constraint name for a column `REFERENCES`:
/// `<child>_<column>_fkey`.
pub fn default_name(child_table: &str, child_column: &str) -> String {
    format!("{child_table}_{child_column}_fkey")
}

/// Encode an FK body for storage. Single-column only, so four `|`-separated fields.
///
/// A constraint name, table name or column name is an SQL identifier and never contains
/// `|`, so the separator is unambiguous.
pub fn encode(fk: &ForeignKey) -> String {
    format!(
        "{}|{}|{}|{}",
        fk.child_column, fk.keyspace, fk.parent_table, fk.parent_column
    )
}

/// Decode an FK body stored under constraint `name`. `None` for a malformed body.
pub fn decode(name: &str, raw: &str) -> Option<ForeignKey> {
    let mut parts = raw.split('|');
    let child_column = parts.next()?.to_string();
    let keyspace = parts.next()?.to_string();
    let parent_table = parts.next()?.to_string();
    let parent_column = parts.next()?.to_string();
    if parts.next().is_some() {
        return None;
    }
    if child_column.is_empty()
        || keyspace.is_empty()
        || parent_table.is_empty()
        || parent_column.is_empty()
    {
        return None;
    }
    Some(ForeignKey {
        name: name.to_string(),
        child_column,
        keyspace,
        parent_table,
        parent_column,
    })
}

/// Every foreign key recorded on `meta`, ordered by name so a check is deterministic.
pub fn recorded(meta: &TableMetadata) -> Vec<ForeignKey> {
    let mut keys: Vec<(&String, &String)> = meta
        .extensions
        .iter()
        .filter(|(key, _)| key.starts_with(FOREIGN_KEY_PREFIX))
        .collect();
    keys.sort_by(|a, b| a.0.cmp(b.0));
    keys.into_iter()
        .filter_map(|(key, value)| {
            let name = &key[FOREIGN_KEY_PREFIX.len()..];
            decode(name, value)
        })
        .collect()
}

/// Render a key value for the `23503` message, Postgres style (the value, not a debug dump).
fn render_key(value: &CqlValue) -> String {
    match value {
        CqlValue::Null => "NULL".to_string(),
        CqlValue::Text(s) | CqlValue::Ascii(s) => s.clone(),
        CqlValue::Int(i) => i.to_string(),
        CqlValue::Bigint(i) | CqlValue::Counter(i) | CqlValue::Time(i) => i.to_string(),
        CqlValue::Smallint(i) => i.to_string(),
        CqlValue::Tinyint(i) => i.to_string(),
        CqlValue::Boolean(b) => b.to_string(),
        CqlValue::Uuid(u) | CqlValue::Timeuuid(u) => u.to_string(),
        CqlValue::Inet(ip) => ip.to_string(),
        other => format!("{other:?}"),
    }
}

/// Whether the parent row a child references exists, as a LOOKUP.
///
/// - the referenced column IS the parent's storage key -> a point read;
/// - otherwise it is the parent's declared key, whose `ADD PRIMARY KEY` built a secondary
///   index named `<parent>_pkey` -> an index lookup through [`StorageEngine::read_by_index_exists`].
///
/// The index path fails loud if the index is missing or not current; it never reports a
/// missing parent on the strength of an index it could not consult.
fn parent_row_exists(
    engine: &StorageEngine,
    parent: &TableMetadata,
    parent_column: &str,
    value: &CqlValue,
) -> Result<bool, String> {
    let table_id = TableId::new(&parent.keyspace, &parent.name);
    let is_storage_key =
        crate::pg_key::storage_key_columns(parent) == vec![parent_column.to_string()];
    if is_storage_key {
        let key = ferrosa_row_bridge::build_decorated_key(std::slice::from_ref(value), &[])
            .map_err(|error| error.to_string())?;
        return Ok(engine
            .read_clustering_row(&table_id, &key, &[])
            .map_err(|error| error.to_string())?
            .is_some());
    }
    let index = format!("{}_pkey", parent.name);
    let key_bytes = ferrosa_row_bridge::encode_value(value);
    engine
        .read_by_index_exists(&table_id, &index, &key_bytes)
        .map_err(|error| error.to_string())
}

/// Check every foreign key declared on `child` against one row's `values`, returning the
/// SQLSTATE and message of the first violation.
///
/// A column the statement did not supply is NULL, and a NULL referencing value satisfies
/// the constraint under PostgreSQL's default MATCH SIMPLE — so it is skipped, exactly as
/// Postgres does. `values` is keyed by column name and holds resolved CQL values.
///
/// The parent lookup is a normal read: it touches the parent's storage (or index), so the
/// storage engine records it in the read set a serializable commit validates — a concurrent
/// delete of the parent bumps the table epoch and fails the commit (see `mvcc::validate_snapshot`).
pub fn check_child_row(
    engine: &StorageEngine,
    schema: &Schema,
    child: &TableMetadata,
    values: &HashMap<String, CqlValue>,
) -> Result<(), (String, String)> {
    let keys = recorded(child);
    if keys.is_empty() {
        return Ok(());
    }
    let snapshot = schema.snapshot();
    check_child_row_with_snapshot(engine, &snapshot, child, values)
}

/// [`check_child_row`] against an already-taken snapshot.
pub fn check_child_row_with_snapshot(
    engine: &StorageEngine,
    snapshot: &SchemaSnapshot,
    child: &TableMetadata,
    values: &HashMap<String, CqlValue>,
) -> Result<(), (String, String)> {
    for fk in recorded(child) {
        let Some(value) = values.get(&fk.child_column) else {
            continue; // not supplied -> NULL -> MATCH SIMPLE does not check it
        };
        if matches!(value, CqlValue::Null) {
            continue;
        }
        let parent = snapshot
            .tables
            .get(&(fk.keyspace.clone(), fk.parent_table.clone()))
            .ok_or_else(|| {
                (
                    "42P01".to_string(),
                    format!(
                        "foreign key constraint \"{}\" references relation \"{}\", which does not exist",
                        fk.name, fk.parent_table
                    ),
                )
            })?;
        let exists =
            parent_row_exists(engine, parent, &fk.parent_column, value).map_err(|error| {
                (
                    "58000".to_string(),
                    format!(
                        "foreign key constraint \"{}\" could not be checked: {error}",
                        fk.name
                    ),
                )
            })?;
        if !exists {
            return Err((
                "23503".to_string(),
                format!(
                    "insert or update on table \"{}\" violates foreign key constraint \"{}\": \
                     Key ({})=({}) is not present in table \"{}\"",
                    child.name,
                    fk.name,
                    fk.child_column,
                    render_key(value),
                    fk.parent_table
                ),
            ));
        }
    }
    Ok(())
}

/// Check every foreign key that references `parent`, for a DELETE that would remove a row whose
/// column values are `values`.
///
/// The parent-side direction. Each referencing constraint has a secondary index over the CHILD's
/// column (built by `ADD FOREIGN KEY`), so "does any child still reference this value" is an
/// index lookup, never a scan. Returns the SQLSTATE and message of the first live reference.
///
/// A constraint whose child column IS the child's storage key has no secondary index — the
/// primary structure serves lookups *by* that column, not the reverse — so the probe reads the
/// child partition whose key is the referencing value directly (a point read). It is still a
/// lookup, never a scan.
///
/// The DELETE identifies the parent row by key columns only, so a referencing value is checked
/// only when the referenced column is one of the supplied key values: the referenced column is
/// required to be the parent's key (enforced at `ADD FOREIGN KEY`), so that is the ordinary case
/// (`DELETE FROM customers WHERE id = ...`). A referenced column the DELETE could not name is
/// unreachable through this front end and needs no check.
pub fn check_parent_row(
    engine: &StorageEngine,
    snapshot: &SchemaSnapshot,
    parent: &TableMetadata,
    values: &HashMap<String, CqlValue>,
) -> Result<(), (String, String)> {
    for child in snapshot.tables.values() {
        for fk in recorded(child) {
            if fk.keyspace != parent.keyspace || fk.parent_table != parent.name {
                continue;
            }
            let Some(value) = values.get(&fk.parent_column) else {
                continue;
            };
            if matches!(value, CqlValue::Null) {
                continue;
            }
            let child_id = TableId::new(&child.keyspace, &child.name);
            let key_bytes = ferrosa_row_bridge::encode_value(value);
            let referenced = if crate::pg_key::storage_key_columns(child)
                == vec![fk.child_column.clone()]
            {
                // The child's FK column IS its storage key: the child row for this value is the
                // partition keyed by it, so a point read answers "does any child reference it".
                let key = ferrosa_row_bridge::build_decorated_key(std::slice::from_ref(value), &[])
                    .map_err(|error| {
                        (
                            "58000".to_string(),
                            format!(
                                "foreign key constraint \"{}\" could not be checked: {error}",
                                fk.name
                            ),
                        )
                    })?;
                engine
                    .read_clustering_row(&child_id, &key, &[])
                    .map_err(|error| {
                        (
                            "58000".to_string(),
                            format!(
                                "foreign key constraint \"{}\" could not be checked: {error}",
                                fk.name
                            ),
                        )
                    })?
                    .is_some()
            } else {
                engine
                    .read_by_index_exists(&child_id, &fk.child_index_name(), &key_bytes)
                    .map_err(|error| {
                        (
                            "58000".to_string(),
                            format!(
                                "foreign key constraint \"{}\" could not be checked: {error}",
                                fk.name
                            ),
                        )
                    })?
            };
            if referenced {
                return Err((
                    "23503".to_string(),
                    format!(
                        "update or delete on table \"{}\" violates foreign key constraint \"{}\" \
                         on table \"{}\": Key ({})=({}) is still referenced from table \"{}\"",
                        parent.name,
                        fk.name,
                        child.name,
                        fk.parent_column,
                        render_key(value),
                        child.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_schema::{ClusteringOrder, ColumnKind, ColumnMetadata, TableParams};
    use indexmap::IndexMap;
    use std::collections::HashSet;

    fn column(name: &str, kind: ColumnKind) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position: 0,
            column_type: "int".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        }
    }

    fn table(name: &str, extensions: HashMap<String, String>) -> TableMetadata {
        let mut columns = IndexMap::new();
        columns.insert("bid".to_string(), column("bid", ColumnKind::Regular));
        TableMetadata {
            keyspace: "public".to_string(),
            name: name.to_string(),
            id: uuid::Uuid::nil(),
            columns,
            partition_key: vec![ferrosa_common::timeuuid::SYNTHETIC_KEY_COLUMN.to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions,
            is_system: false,
        }
    }

    fn fk() -> ForeignKey {
        ForeignKey {
            name: "h_bid_fkey".to_string(),
            child_column: "bid".to_string(),
            keyspace: "public".to_string(),
            parent_table: "b".to_string(),
            parent_column: "bid".to_string(),
        }
    }

    /// The body round-trips through encode/decode without loss.
    #[test]
    fn encode_and_decode_round_trip() {
        let original = fk();
        let body = encode(&original);
        assert_eq!(decode("h_bid_fkey", &body), Some(original));
    }

    /// A recorded constraint is found by its prefix, and only by its prefix.
    #[test]
    fn recorded_reads_only_the_foreign_key_namespace() {
        let mut extensions = HashMap::new();
        extensions.insert(extension_key("h_bid_fkey"), encode(&fk()));
        // A declared primary key lives beside it and must not be mistaken for an FK.
        extensions.insert("pg.primary_key".to_string(), "bid".to_string());
        let meta = table("h", extensions);
        let keys = recorded(&meta);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], fk());
    }

    /// A malformed body is skipped rather than surfaced as a half-parsed constraint.
    #[test]
    fn a_malformed_body_is_not_a_constraint() {
        assert_eq!(decode("x", "bid|public|b"), None, "too few fields");
        assert_eq!(
            decode("x", "bid|public|b|bid|extra"),
            None,
            "too many fields"
        );
        assert_eq!(decode("x", "|public|b|bid"), None, "an empty column");
    }

    /// The default name matches PostgreSQL's `<table>_<column>_fkey`.
    #[test]
    fn the_default_name_is_postgres_s() {
        assert_eq!(
            default_name("pgbench_tellers", "bid"),
            "pgbench_tellers_bid_fkey"
        );
    }
}
