//! Module: PostgreSQL enum types (`CREATE TYPE … AS ENUM`, `ALTER TYPE … ADD VALUE`).
//!
//! Oban declares `oban_job_state` as an enum and types `oban_jobs.state` by it. Ferrosa
//! stores an enum-typed column as **text** and validates every write against the type's
//! labels: a value outside the enum is refused (`22P02`), never stored. Labels only grow
//! (there is no `DROP VALUE`, in PostgreSQL either), so a stored value stays a label. Lean:
//! `Ferrosa.Sql.Enum` (`validated_is_label`, `addValue_idempotent`,
//! `stored_labels_survive_add_value`); [`EnumType`] is its `EnumType`.
//!
//! Where a definition lives. A type is keyspace-scoped and not tied to a table, and the
//! schema layer has no replicated "alter type" operation. So each keyspace that declares an
//! enum gets one hidden, row-less metadata table, [`META_TABLE`], whose table extension
//! `pg.enum.<name>` holds the labels. Creating the type and adding a value are then ordinary
//! `CREATE TABLE` / `ALTER TABLE` schema changes through `DdlPath` — replicated like any DDL,
//! and buffered by transactional DDL like any DDL. The metadata table is hidden from the
//! `pg_catalog` projections (its name has ferrosa's reserved `_sys_` prefix); CQL clients
//! can see it.
//!
//! A column typed by an enum carries the table extension `pg.enum_column.<column>` =
//! `<keyspace>.<type>`.
//!
//! `ADD VALUE` writes ONE extension key per added label (`pg.enum_value.["name","label"]`),
//! never the whole list, so two sessions adding different labels at once both keep theirs
//! (it used to rewrite the list, and the later write lost the other label; see
//! `oban-compat.md` "Found while building"). The type is the declared labels followed by the
//! added ones in the order they were added.
//!
//! Wire type: an enum column is described as `text` (OID 25) in `RowDescription` and
//! `ParameterDescription`, while `pg_attribute.atttypid` and `pg_type` carry the enum's own
//! OID. PostgreSQL would put the enum OID on the wire too; doing so needs the engine's
//! column type to carry a type OID (coordinated with W1), and a driver then fetches the
//! type through the bootstrap query. Text is exact for the values themselves.
//!
//! Last revised: 2026-11-08

use std::collections::{HashMap, HashSet};

use ferrosa_schema::{
    ClusteringOrder, ColumnKind, ColumnMetadata, SchemaSnapshot, TableMetadata, TableParams,
};
use indexmap::IndexMap;

use crate::messages::BackendMessage;
use crate::query::error_response;

/// The hidden per-keyspace table whose extensions hold the keyspace's enum definitions.
pub(crate) const META_TABLE: &str = "_sys_pg_types";
const ENUM_PREFIX: &str = "pg.enum.";
const ENUM_COLUMN_PREFIX: &str = "pg.enum_column.";

/// An enum type: its labels, in declaration order, without duplicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnumType {
    labels: Vec<String>,
}

impl EnumType {
    /// `CREATE TYPE … AS ENUM (labels)`: a repeated label is refused (`42710`).
    pub(crate) fn create(labels: Vec<String>) -> Result<Self, BackendMessage> {
        let mut seen = HashSet::with_capacity(labels.len());
        if let Some(dup) = labels.iter().find(|l| !seen.insert(l.as_str())) {
            return Err(error_response(
                "42710",
                &format!("enum label \"{dup}\" used more than once"),
            ));
        }
        Ok(Self { labels })
    }

    #[cfg(test)]
    pub(crate) fn labels(&self) -> &[String] {
        &self.labels
    }

    pub(crate) fn contains(&self, label: &str) -> bool {
        self.labels.iter().any(|l| l == label)
    }

    /// `ADD VALUE IF NOT EXISTS l`: append `l` unless it is already a label.
    pub(crate) fn add_value(mut self, label: String) -> Self {
        if !self.contains(&label) {
            self.labels.push(label);
        }
        self
    }

    /// Validate one value written to a column of this type.
    pub(crate) fn validate(&self, type_name: &str, value: &str) -> Result<(), BackendMessage> {
        if self.contains(value) {
            return Ok(());
        }
        Err(error_response(
            "22P02",
            &format!("invalid input value for enum {type_name}: \"{value}\""),
        ))
    }

    fn encode(&self) -> String {
        // A list of strings always serializes; there is no error to lose here.
        serde_json::to_string(&self.labels).unwrap_or_default()
    }

    fn decode(text: &str) -> Option<Self> {
        serde_json::from_str::<Vec<String>>(text)
            .ok()
            .map(|labels| Self { labels })
    }
}

/// The table extension key holding enum `name`.
pub(crate) fn definition_key(name: &str) -> String {
    format!("{ENUM_PREFIX}{name}")
}

/// The table extensions that store `ty` as enum `name`.
pub(crate) fn definition_extension(name: &str, ty: &EnumType) -> HashMap<String, String> {
    HashMap::from([(definition_key(name), ty.encode())])
}

/// The table extension that records `ADD VALUE label` on enum `name`: ONE key per added
/// label, so the write never rewrites another session's label (two concurrent adds of
/// different labels both survive; Lean `concurrent_adds_both_survive`). The value orders
/// added labels by the wall-clock microsecond of the statement (ties by label).
pub(crate) fn add_value_extension(
    name: &str,
    current: &EnumType,
    label: String,
) -> HashMap<String, String> {
    debug_assert!(!current.contains(&label), "ADD VALUE of an existing label");
    let micros = crate::query::wall_clock_micros();
    HashMap::from([(added_label_key(name, &label), format!("{micros:020}"))])
}

const ADDED_PREFIX: &str = "pg.enum_value.";

/// The key of added label `label` of enum `name`: both parts JSON-encoded, so neither may
/// contain a separator that confuses the decoding.
fn added_label_key(name: &str, label: &str) -> String {
    // A pair of strings always serializes; there is no error to lose here.
    let pair = serde_json::to_string(&(name, label)).unwrap_or_default();
    format!("{ADDED_PREFIX}{pair}")
}

/// The labels added to enum `name` by `ADD VALUE`, in the order they were added.
fn added_labels(meta: &TableMetadata, name: &str) -> Vec<String> {
    let mut added: Vec<(&str, String)> = meta
        .extensions
        .iter()
        .filter_map(|(key, order)| {
            let pair = key.strip_prefix(ADDED_PREFIX)?;
            let (owner, label): (String, String) = serde_json::from_str(pair).ok()?;
            (owner == name).then_some((order.as_str(), label))
        })
        .collect();
    added.sort();
    added.into_iter().map(|(_, label)| label).collect()
}

/// Enum `keyspace.name` as `view` defines it. A stored definition that does not decode is
/// reported, not treated as absent.
pub(crate) fn lookup(
    view: &SchemaSnapshot,
    keyspace: &str,
    name: &str,
) -> Result<Option<EnumType>, BackendMessage> {
    let Some(meta) = view
        .tables
        .get(&(keyspace.to_string(), META_TABLE.to_string()))
    else {
        return Ok(None);
    };
    let Some(text) = meta.extensions.get(&definition_key(name)) else {
        return Ok(None);
    };
    let base = EnumType::decode(text).ok_or_else(|| {
        tracing::error!(keyspace, name, "stored enum definition does not decode");
        error_response(
            "XX001",
            &format!("the stored definition of enum {keyspace}.{name} is corrupt"),
        )
    })?;
    let full = added_labels(meta, name)
        .into_iter()
        .fold(base, EnumType::add_value);
    Ok(Some(full))
}

/// Every enum `view` defines, as `(keyspace, name, type)`, in name order.
pub(crate) fn all(view: &SchemaSnapshot) -> Vec<(String, String, EnumType)> {
    let mut out: Vec<(String, String, EnumType)> = view
        .tables
        .iter()
        .filter(|((_, table), _)| table == META_TABLE)
        .flat_map(|((keyspace, _), meta)| {
            meta.extensions.iter().filter_map(move |(key, text)| {
                let name = key.strip_prefix(ENUM_PREFIX)?;
                let ty = added_labels(meta, name)
                    .into_iter()
                    .fold(EnumType::decode(text)?, EnumType::add_value);
                Some((keyspace.clone(), name.to_string(), ty))
            })
        })
        .collect();
    out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    out
}

/// The hidden metadata table for `keyspace`.
pub(crate) fn meta_table(keyspace: &str) -> TableMetadata {
    let mut columns = IndexMap::new();
    columns.insert(
        "type_name".to_string(),
        ColumnMetadata {
            name: "type_name".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    TableMetadata {
        keyspace: keyspace.to_string(),
        name: META_TABLE.to_string(),
        id: uuid::Uuid::new_v4(),
        columns,
        partition_key: vec!["type_name".to_string()],
        clustering_key: Vec::new(),
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: HashMap::new(),
        is_system: false,
    }
}

/// The extension marking `column` as typed by enum `keyspace.name`.
pub(crate) fn column_extension(column: &str, keyspace: &str, name: &str) -> (String, String) {
    (
        format!("{ENUM_COLUMN_PREFIX}{column}"),
        format!("{keyspace}.{name}"),
    )
}

/// The enum `(keyspace, name)` a column is typed by, if any.
pub(crate) fn column_enum<'a>(meta: &'a TableMetadata, column: &str) -> Option<(&'a str, &'a str)> {
    meta.extensions
        .get(&format!("{ENUM_COLUMN_PREFIX}{column}"))
        .and_then(|qualified| qualified.split_once('.'))
}

/// Validate a value written to `column` of `meta` against its enum, if it has one. NULL
/// is not an enum value and is the column's `NOT NULL` rule's business.
pub(crate) fn check_value(
    view: &SchemaSnapshot,
    meta: &TableMetadata,
    column: &str,
    value: &ferrosa_sql::Value,
) -> Result<(), BackendMessage> {
    let Some((keyspace, name)) = column_enum(meta, column) else {
        return Ok(());
    };
    let ty = lookup(view, keyspace, name)?.ok_or_else(|| {
        error_response(
            "42704",
            &format!("type \"{keyspace}.{name}\" of column \"{column}\" does not exist"),
        )
    })?;
    match value {
        ferrosa_sql::Value::Null => Ok(()),
        ferrosa_sql::Value::Text(text) => ty.validate(name, text),
        other => Err(error_response(
            "42804",
            &format!("column \"{column}\" is of type {name} but the value is {other:?}"),
        )),
    }
}

/// Cast a text value to enum `keyspace.name`: the integration point for the expression
/// engine's `'x'::schema.type`. `42704` when no such type exists, `22P02` when the text is
/// not a label.
// Called by the W1 expression engine's cast evaluation once it merges; until then only the
// tests below call it, and the parser refuses enum casts by name (`UnsupportedCast`).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn cast_text_to_enum(
    view: &SchemaSnapshot,
    keyspace: &str,
    name: &str,
    text: &str,
) -> Result<String, BackendMessage> {
    let ty = lookup(view, keyspace, name)?
        .ok_or_else(|| error_response("42704", &format!("type \"{name}\" does not exist")))?;
    ty.validate(name, text)?;
    Ok(text.to_string())
}

/// The OID of enum `keyspace.name`.
pub(crate) fn enum_oid(keyspace: &str, name: &str) -> u32 {
    crate::catalog::synthetic_oid("type", &format!("{keyspace}.{name}"))
}

/// The OID of the array type `_name` of enum `keyspace.name`.
pub(crate) fn enum_array_oid(keyspace: &str, name: &str) -> u32 {
    crate::catalog::synthetic_oid("arraytype", &format!("{keyspace}.{name}"))
}

/// One `pg_type` row for an enum or its array type, with the bootstrap column names
/// Postgrex reads (`typsend` … `typelem`). The catalog's `pg_type` includes these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnumTypeRow {
    pub(crate) oid: u32,
    pub(crate) typname: String,
    pub(crate) typnamespace: u32,
    /// `e` for the enum, `b` for its array type.
    pub(crate) typtype: &'static str,
    pub(crate) typsend: &'static str,
    pub(crate) typreceive: &'static str,
    pub(crate) typoutput: &'static str,
    pub(crate) typinput: &'static str,
    /// 0 for the enum; the enum's OID for its array type.
    pub(crate) typelem: u32,
    pub(crate) typrelid: u32,
    pub(crate) typbasetype: u32,
}

/// The two `pg_type` rows of every enum `view` defines: the enum and its array type.
pub(crate) fn enum_type_rows(view: &SchemaSnapshot) -> Vec<EnumTypeRow> {
    let mut rows = Vec::new();
    for (keyspace, name, _) in all(view) {
        let oid = enum_oid(&keyspace, &name);
        let typnamespace = crate::catalog::namespace_oid(&keyspace);
        rows.push(EnumTypeRow {
            oid,
            typname: name.clone(),
            typnamespace,
            typtype: "e",
            typsend: "enum_send",
            typreceive: "enum_recv",
            typoutput: "enum_out",
            typinput: "enum_in",
            typelem: 0,
            typrelid: 0,
            typbasetype: 0,
        });
        rows.push(EnumTypeRow {
            oid: enum_array_oid(&keyspace, &name),
            typname: format!("_{name}"),
            typnamespace,
            typtype: "b",
            typsend: "array_send",
            typreceive: "array_recv",
            typoutput: "array_out",
            typinput: "array_in",
            typelem: oid,
            typrelid: 0,
            typbasetype: 0,
        });
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    /// The Lean model's `addValue_idempotent` and `addValue_keeps_labels`.
    #[test]
    fn add_value_is_idempotent_and_keeps_labels() {
        let ty = EnumType::create(labels(&["available", "scheduled"])).unwrap();
        let once = ty.clone().add_value("cancelled".into());
        let twice = once.clone().add_value("cancelled".into());
        assert_eq!(once, twice);
        assert!(ty.labels().iter().all(|l| once.contains(l)));
        assert_eq!(once.labels().last().map(String::as_str), Some("cancelled"));
    }

    #[test]
    fn create_refuses_duplicates_and_validate_refuses_non_labels() {
        assert!(EnumType::create(labels(&["a", "a"])).is_err());
        let ty = EnumType::create(labels(&["a"])).unwrap();
        assert!(ty.validate("t", "a").is_ok());
        let Err(BackendMessage::ErrorResponse { fields }) = ty.validate("t", "b") else {
            panic!("must refuse");
        };
        assert!(fields.contains(&(b'C', "22P02".to_string())));
    }

    #[test]
    fn cast_text_to_enum_validates_against_the_view() {
        let schema = crate::catalog::test_support::empty_schema();
        let mut view = ferrosa_schema::SchemaSnapshot::clone(&schema.snapshot());
        let mut meta = meta_table("public");
        let ty = EnumType::create(labels(&["available"])).unwrap();
        meta.extensions
            .extend(definition_extension("oban_job_state", &ty));
        view.tables
            .insert(("public".into(), META_TABLE.into()), meta);
        assert_eq!(
            cast_text_to_enum(&view, "public", "oban_job_state", "available").unwrap(),
            "available"
        );
        let code = |r: Result<String, BackendMessage>| match r {
            Err(BackendMessage::ErrorResponse { fields }) => fields
                .into_iter()
                .find(|(f, _)| *f == b'C')
                .map(|(_, c)| c)
                .unwrap(),
            other => panic!("expected an error, got {other:?}"),
        };
        assert_eq!(
            code(cast_text_to_enum(&view, "public", "oban_job_state", "x")),
            "22P02"
        );
        assert_eq!(
            code(cast_text_to_enum(&view, "public", "missing", "x")),
            "42704"
        );
    }

    /// Two sessions add different labels to the same type from the same view; their schema
    /// changes land one after the other. Both labels must survive (Lean:
    /// `concurrent_adds_both_survive`) — the whole-list rewrite lost the first.
    #[test]
    fn concurrent_add_values_both_survive() {
        let mut meta = meta_table("public");
        let base = EnumType::create(labels(&["a"])).unwrap();
        meta.extensions.extend(definition_extension("t", &base));
        let seen = base.clone(); // both sessions planned against the same view
        let first = add_value_extension("t", &seen, "b".into());
        let second = add_value_extension("t", &seen, "c".into());
        meta.extensions.extend(first);
        meta.extensions.extend(second);
        let mut view = ferrosa_schema::SchemaSnapshot::clone(
            &crate::catalog::test_support::empty_schema().snapshot(),
        );
        view.tables
            .insert(("public".into(), META_TABLE.into()), meta);
        let ty = lookup(&view, "public", "t").unwrap().expect("defined");
        assert!(
            ty.contains("b"),
            "the first concurrent add was lost: {ty:?}"
        );
        assert!(ty.contains("c"), "{ty:?}");
        assert_eq!(
            all(&view).len(),
            1,
            "added labels are not types of their own"
        );
    }

    #[test]
    fn definitions_round_trip() {
        let ty = EnumType::create(labels(&["x, y", "\"q\""])).unwrap();
        assert_eq!(EnumType::decode(&ty.encode()), Some(ty));
    }
}
