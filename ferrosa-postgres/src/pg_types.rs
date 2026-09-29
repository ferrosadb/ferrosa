//! Module: the one `CqlType` <-> Postgres type map (OID, typname, typlen, engine
//! `ColumnType`, text/binary support) for the whole `ferrosa-postgres` crate.
//! Correctness: every `CqlType` maps to exactly one [`PgType`] through an
//! exhaustive match; the catalog projection, the storage provider's `RelSchema`,
//! RowDescription and parameter inference all read this module instead of a
//! private string switch, so they cannot drift (FM-20, T-023, board t_cd417149).
//! Every mapped `typname` resolves back to a `CqlType` (the D10 PG-name map).
//! Last revised: 2026-09-28
//! Last changed: Created; replaces `catalog::type_oid`/`type_name`,
//! `storage_provider::engine_column_type` and `query::cql_type_to_column_type`.
//!
//! # Float and double
//!
//! The engine has one floating column type, `ColumnType::Float`, carrying an
//! `f64`. Both `float` and `double` therefore advertise `float8` (OID 701, 8
//! bytes), which is what the row bytes are. Advertising `float4` (700) for a CQL
//! `float` would promise 4-byte binary values the encoder never emits.
//!
//! # Composite types
//!
//! Collections, tuples, vectors, UDTs and `duration` have no native engine
//! column type. They are named arms that map to `text` with
//! [`PgType::text_rendered`] set: the value is delivered as its CQL text
//! rendering. This is designed and documented, not a fallback. A type string
//! that cannot be resolved at all is a [`PgTypeError`], never silently `text`.
//!
//! # JSONB (T-160 engine type, T-161a wire codec)
//!
//! `jsonb` (OID 3802), `json` (114), `jsonpath` (4072) and `text[]` (1009) are
//! engine column types ([`ColumnType::Jsonb`] and friends) with entries in
//! [`ALL_PG_TYPES`]. `CqlType::Jsonb` maps to `jsonb`. The wire codec is not
//! implemented (T-161a): the entries are `binary: false` and
//! `query::encode_value` refuses these values loudly in both formats.
//! `cql_type_for_pg_name` still refuses the name `jsonb` (DDL is T-131).

use std::fmt;

use ferrosa_common::cql_type::CqlType;
use ferrosa_schema::Schema;
use ferrosa_sql::ColumnType;

/// Postgres OID of `json` (D11: stored as jsonb).
pub const PG_OID_JSON: u32 = 114;
/// Postgres OID of `jsonb` (D11).
pub const PG_OID_JSONB: u32 = 3802;
/// Postgres OID of `jsonpath` (D11).
pub const PG_OID_JSONPATH: u32 = 4072;

/// One Postgres type as ferrosa advertises it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgType {
    /// Postgres type OID.
    pub oid: u32,
    /// Canonical `pg_type.typname`.
    pub typname: &'static str,
    /// `pg_type.typlen`: fixed byte size, or `-1` for variable length.
    pub typlen: i16,
    /// The engine relational column type carrying values of this type.
    pub column_type: ColumnType,
    /// Whether the binary wire format is implemented for this type.
    pub binary: bool,
    /// The value is a CQL text rendering of a composite with no native engine
    /// type (see the module docs). Always false for scalar-backed entries.
    pub text_rendered: bool,
}

/// Why a type could not be mapped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgTypeError {
    /// A stored CQL type string did not resolve to a `CqlType`.
    UnresolvedCqlType { column_type: String, reason: String },
    /// A Postgres type name has no CQL mapping (DDL, D10).
    UnknownPgTypeName(String),
}

impl fmt::Display for PgTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnresolvedCqlType {
                column_type,
                reason,
            } => write!(
                f,
                "cannot map CQL type '{column_type}' to Postgres: {reason}"
            ),
            Self::UnknownPgTypeName(name) => {
                write!(f, "type \"{name}\" does not exist or is not supported")
            }
        }
    }
}

impl std::error::Error for PgTypeError {}

const fn entry(
    oid: u32,
    typname: &'static str,
    typlen: i16,
    column_type: ColumnType,
    binary: bool,
) -> PgType {
    PgType {
        oid,
        typname,
        typlen,
        column_type,
        binary,
        text_rendered: false,
    }
}

/// Every engine-backed Postgres type ferrosa advertises, one row per
/// `ColumnType`. Kept in step with [`for_column_type`] by a unit test.
pub const ALL_PG_TYPES: [PgType; 16] = [
    entry(23, "int4", 4, ColumnType::Int, true),
    entry(20, "int8", 8, ColumnType::BigInt, true),
    entry(25, "text", -1, ColumnType::Text, true),
    entry(16, "bool", 1, ColumnType::Bool, true),
    entry(701, "float8", 8, ColumnType::Float, true),
    entry(2950, "uuid", 16, ColumnType::Uuid, true),
    entry(17, "bytea", -1, ColumnType::Bytea, true),
    entry(1114, "timestamp", 8, ColumnType::Timestamp, true),
    entry(1082, "date", 4, ColumnType::Date, true),
    entry(1083, "time", 8, ColumnType::Time, true),
    entry(869, "inet", -1, ColumnType::Inet, true),
    // Binary numeric is not implemented: the encoder refuses it.
    entry(1700, "numeric", -1, ColumnType::Numeric, false),
    // T-160: the engine carries jsonb, json (stored as jsonb, D11), jsonpath and
    // text[]. The wire codec is T-161a, so `binary` is false and
    // `query::encode_value` refuses these values in both formats (PG-T160-1).
    entry(PG_OID_JSONB, "jsonb", -1, ColumnType::Jsonb, false),
    entry(PG_OID_JSON, "json", -1, ColumnType::Json, false),
    entry(PG_OID_JSONPATH, "jsonpath", -1, ColumnType::JsonPath, false),
    entry(1009, "_text", -1, ColumnType::TextArray, false),
];

/// The Postgres type for an engine relational column type.
pub fn for_column_type(ty: ColumnType) -> PgType {
    let idx = match ty {
        ColumnType::Int => 0,
        ColumnType::BigInt => 1,
        ColumnType::Text => 2,
        ColumnType::Bool => 3,
        ColumnType::Float => 4,
        ColumnType::Uuid => 5,
        ColumnType::Bytea => 6,
        ColumnType::Timestamp => 7,
        ColumnType::Date => 8,
        ColumnType::Time => 9,
        ColumnType::Inet => 10,
        ColumnType::Numeric => 11,
        ColumnType::Jsonb => 12,
        ColumnType::Json => 13,
        ColumnType::JsonPath => 14,
        ColumnType::TextArray => 15,
    };
    ALL_PG_TYPES[idx]
}

/// The engine column type a `CqlType` is carried as, and whether it is a text
/// rendering of a composite.
fn column_type_of(t: &CqlType) -> (ColumnType, bool) {
    match t {
        CqlType::Int | CqlType::Smallint | CqlType::Tinyint => (ColumnType::Int, false),
        CqlType::Bigint | CqlType::Counter => (ColumnType::BigInt, false),
        CqlType::Boolean => (ColumnType::Bool, false),
        CqlType::Float | CqlType::Double => (ColumnType::Float, false),
        CqlType::Uuid | CqlType::Timeuuid => (ColumnType::Uuid, false),
        CqlType::Blob => (ColumnType::Bytea, false),
        CqlType::Timestamp => (ColumnType::Timestamp, false),
        CqlType::Date => (ColumnType::Date, false),
        CqlType::Time => (ColumnType::Time, false),
        CqlType::Inet => (ColumnType::Inet, false),
        CqlType::Decimal | CqlType::Varint => (ColumnType::Numeric, false),
        CqlType::Ascii | CqlType::Varchar => (ColumnType::Text, false),
        CqlType::Duration
        | CqlType::List(_)
        | CqlType::Map(_, _)
        | CqlType::Set(_)
        | CqlType::Tuple(_)
        | CqlType::Udt { .. }
        | CqlType::Vector(_, _) => (ColumnType::Text, true),
        // T-160: a native engine column type (OID 3802). The wire codec is
        // T-161a; until then `query::encode_value` refuses jsonb values.
        CqlType::Jsonb => (ColumnType::Jsonb, false),
    }
}

/// The Postgres type for a `CqlType`. Total: every variant has a named arm.
pub fn pg_type_of(t: &CqlType) -> PgType {
    let (column_type, text_rendered) = column_type_of(t);
    PgType {
        text_rendered,
        ..for_column_type(column_type)
    }
}

/// Resolve a stored CQL type string once (row-bridge parser, with UDT context)
/// and map it. An unresolvable string is a typed error, never `text`.
pub fn pg_type_of_column(
    column_type: &str,
    keyspace: &str,
    schema: &Schema,
) -> Result<PgType, PgTypeError> {
    ferrosa_row_bridge::parse_cql_type_in_keyspace(column_type, keyspace, schema)
        .map(|t| pg_type_of(&t))
        .map_err(|e| PgTypeError::UnresolvedCqlType {
            column_type: column_type.to_string(),
            reason: e.to_string(),
        })
}

/// The Postgres type advertised for a `PgType` OID, if ferrosa mints it.
pub fn by_oid(oid: u32) -> Option<PgType> {
    ALL_PG_TYPES.iter().find(|p| p.oid == oid).copied()
}

/// D10: resolve a Postgres type name (as written in DDL) to a `CqlType`.
///
/// Accepts the canonical `typname` and the common SQL spellings. Anything else
/// is [`PgTypeError::UnknownPgTypeName`]: DDL must refuse, not guess.
pub fn cql_type_for_pg_name(name: &str) -> Result<CqlType, PgTypeError> {
    let lower = name.trim().to_ascii_lowercase();
    match lower.as_str() {
        "int2" | "smallint" => Ok(CqlType::Smallint),
        "int4" | "int" | "integer" => Ok(CqlType::Int),
        "int8" | "bigint" => Ok(CqlType::Bigint),
        "text" | "varchar" | "character varying" => Ok(CqlType::Varchar),
        "bool" | "boolean" => Ok(CqlType::Boolean),
        "float4" | "real" => Ok(CqlType::Float),
        "float8" | "double precision" => Ok(CqlType::Double),
        "bytea" => Ok(CqlType::Blob),
        "uuid" => Ok(CqlType::Uuid),
        "timestamp" | "timestamp without time zone" => Ok(CqlType::Timestamp),
        "date" => Ok(CqlType::Date),
        "time" | "time without time zone" => Ok(CqlType::Time),
        "inet" => Ok(CqlType::Inet),
        "numeric" | "decimal" => Ok(CqlType::Decimal),
        _ => Err(PgTypeError::UnknownPgTypeName(name.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::cql_type::names::SCALAR_TYPES;

    /// Scalars plus one of each composite: every `CqlType` shape.
    fn every_cql_type() -> Vec<CqlType> {
        let mut all: Vec<CqlType> = SCALAR_TYPES.to_vec();
        all.push(CqlType::List(Box::new(CqlType::Int)));
        all.push(CqlType::Set(Box::new(CqlType::Varchar)));
        all.push(CqlType::Map(
            Box::new(CqlType::Varchar),
            Box::new(CqlType::Int),
        ));
        all.push(CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]));
        all.push(CqlType::Vector(Box::new(CqlType::Float), 3));
        all.push(CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "u".to_string(),
            fields: vec![("a".to_string(), CqlType::Int)],
        });
        all
    }

    #[test]
    fn pg_types_float_double_agree_everywhere() {
        let float = pg_type_of(&CqlType::Float);
        let double = pg_type_of(&CqlType::Double);
        assert_eq!(float, double);
        assert_eq!(float.oid, 701);
        assert_eq!(float.column_type, ColumnType::Float);
        // The wire path derives its OID and size from the same entry.
        let cols = [ferrosa_sql::Column::new("x", float.column_type)];
        let wire = crate::query::row_description_fields(&cols, &[]);
        assert_eq!(wire[0].type_oid, 701);
        assert_eq!(wire[0].type_size, 8);
    }

    #[test]
    fn pg_types_round_trip_every_entry() {
        for t in every_cql_type() {
            let pg = pg_type_of(&t);
            // Exactly one entry per engine type, reachable by OID.
            assert_eq!(by_oid(pg.oid).map(|p| p.column_type), Some(pg.column_type));
            if t == CqlType::Jsonb {
                // DDL name resolution for jsonb is T-131; the type name is
                // still refused by `cql_type_for_pg_name`.
                assert_eq!((pg.oid, pg.typname), (PG_OID_JSONB, "jsonb"));
                continue;
            }
            // The mapped name resolves back to a CqlType with the same PgType.
            let back = cql_type_for_pg_name(pg.typname).expect("typname resolves");
            let again = pg_type_of(&back);
            assert_eq!((again.oid, again.typname), (pg.oid, pg.typname), "{t:?}");
        }
    }

    #[test]
    fn every_entry_is_consistent_with_for_column_type() {
        for e in ALL_PG_TYPES {
            assert_eq!(for_column_type(e.column_type), e, "{}", e.typname);
            assert!(!e.text_rendered);
            // `column_type_oid` narrows to i32 for the wire; no entry may clamp.
            assert!(i32::try_from(e.oid).is_ok(), "{}", e.typname);
        }
        let oids: std::collections::HashSet<u32> = ALL_PG_TYPES.iter().map(|e| e.oid).collect();
        assert_eq!(oids.len(), ALL_PG_TYPES.len(), "OIDs are unique");
        for oid in [PG_OID_JSON, PG_OID_JSONB, PG_OID_JSONPATH, 1009] {
            let e = by_oid(oid).expect("T-160 engine type");
            // No wire codec until T-161a: never advertise a binary format.
            assert!(!e.binary, "{}", e.typname);
        }
    }

    #[test]
    fn composites_are_named_text_rendered_arms() {
        for t in every_cql_type() {
            let pg = pg_type_of(&t);
            // jsonb is a native engine type since T-160 (not text-rendered).
            let composite = !SCALAR_TYPES.contains(&t) || t == CqlType::Duration;
            if t == CqlType::Jsonb {
                assert!(!pg.text_rendered);
                continue;
            }
            assert_eq!(pg.text_rendered, composite, "{t:?}");
            if composite {
                assert_eq!((pg.oid, pg.column_type), (25, ColumnType::Text));
            }
        }
    }

    #[test]
    fn former_scalar_paths_are_preserved() {
        let cases = [
            (CqlType::Smallint, 23),
            (CqlType::Counter, 20),
            (CqlType::Timeuuid, 2950),
            (CqlType::Varint, 1700),
            (CqlType::Ascii, 25),
            (CqlType::Blob, 17),
        ];
        for (t, oid) in cases {
            assert_eq!(pg_type_of(&t).oid, oid, "{t:?}");
        }
        assert!(!pg_type_of(&CqlType::Decimal).binary);
        assert!(pg_type_of(&CqlType::Int).binary);
    }

    #[test]
    fn unknown_pg_type_name_is_refused_loudly() {
        for bad in ["jsonb", "money", "", "int4range"] {
            let err = cql_type_for_pg_name(bad).expect_err(bad);
            assert_eq!(err, PgTypeError::UnknownPgTypeName(bad.to_string()));
            assert!(err.to_string().contains("does not exist"));
        }
    }

    #[test]
    fn unresolvable_column_type_string_is_refused_loudly() {
        let schema = crate::catalog::test_support::empty_schema();
        for bad in ["", "no_such_udt", "map<text,"] {
            let err = pg_type_of_column(bad, "ks", &schema).expect_err(bad);
            assert!(
                matches!(err, PgTypeError::UnresolvedCqlType { .. }),
                "{bad}: {err}"
            );
        }
        let ok = pg_type_of_column("frozen<list<int>>", "ks", &schema).expect("resolves");
        assert!(ok.text_rendered);
    }
}
