//! Module: the one CQL type-name registry (name <-> `CqlType` <-> marshal class).
//! Correctness: every scalar `CqlType` round-trips name -> type -> name, and each
//! front-end (CQL bridge, row-bridge parser, schema converters) resolves through
//! these functions instead of a private string switch (FM-20, T-022).
//! Last revised: 2026-09-28
//! Last changed: Created; replaces the duplicated string switches (COM-T022-01).
//!
//! Adding a scalar type means editing exactly two places here: the exhaustive
//! [`scalar_info`] match (a missing arm is a compile error) and
//! [`SCALAR_TYPES`] (guarded by the round-trip test). Grammar for collections,
//! tuples, vectors and UDTs stays in the parsers; only names live here.

use super::CqlType;

/// The Cassandra jsonb proof-of-concept custom class (D20).
pub const JSONB_MARSHAL_CLASS: &str = "org.apache.cassandra.db.marshal.JsonbType";

/// Registry row for one scalar type.
struct ScalarInfo {
    /// Canonical lowercase CQL name.
    name: &'static str,
    /// Additional accepted spellings (e.g. `varchar` for `text`).
    aliases: &'static [&'static str],
    /// Cassandra marshal class name.
    marshal: &'static str,
}

/// Registry entry for any `CqlType`: a scalar row or a composite kind name.
enum Entry {
    Scalar(ScalarInfo),
    Composite(&'static str),
}

const fn info(
    name: &'static str,
    aliases: &'static [&'static str],
    marshal: &'static str,
) -> Entry {
    Entry::Scalar(ScalarInfo {
        name,
        aliases,
        marshal,
    })
}

/// Every scalar `CqlType`, for name lookup. Order is irrelevant.
pub static SCALAR_TYPES: [CqlType; 21] = [
    CqlType::Ascii,
    CqlType::Bigint,
    CqlType::Blob,
    CqlType::Boolean,
    CqlType::Counter,
    CqlType::Decimal,
    CqlType::Double,
    CqlType::Float,
    CqlType::Int,
    CqlType::Timestamp,
    CqlType::Uuid,
    CqlType::Varchar,
    CqlType::Varint,
    CqlType::Timeuuid,
    CqlType::Inet,
    CqlType::Date,
    CqlType::Time,
    CqlType::Smallint,
    CqlType::Tinyint,
    CqlType::Duration,
    CqlType::Jsonb,
];

/// Exhaustive registry match over every `CqlType` variant.
fn entry(t: &CqlType) -> Entry {
    match t {
        CqlType::Ascii => info("ascii", &[], "org.apache.cassandra.db.marshal.AsciiType"),
        CqlType::Bigint => info("bigint", &[], "org.apache.cassandra.db.marshal.LongType"),
        CqlType::Blob => info("blob", &[], "org.apache.cassandra.db.marshal.BytesType"),
        CqlType::Boolean => info(
            "boolean",
            &[],
            "org.apache.cassandra.db.marshal.BooleanType",
        ),
        CqlType::Counter => info(
            "counter",
            &[],
            "org.apache.cassandra.db.marshal.CounterColumnType",
        ),
        CqlType::Decimal => info(
            "decimal",
            &[],
            "org.apache.cassandra.db.marshal.DecimalType",
        ),
        CqlType::Double => info("double", &[], "org.apache.cassandra.db.marshal.DoubleType"),
        CqlType::Float => info("float", &[], "org.apache.cassandra.db.marshal.FloatType"),
        CqlType::Int => info("int", &[], "org.apache.cassandra.db.marshal.Int32Type"),
        CqlType::Timestamp => info(
            "timestamp",
            &[],
            "org.apache.cassandra.db.marshal.TimestampType",
        ),
        CqlType::Uuid => info("uuid", &[], "org.apache.cassandra.db.marshal.UUIDType"),
        CqlType::Varchar => info(
            "text",
            &["varchar"],
            "org.apache.cassandra.db.marshal.UTF8Type",
        ),
        CqlType::Varint => info("varint", &[], "org.apache.cassandra.db.marshal.IntegerType"),
        CqlType::Timeuuid => info(
            "timeuuid",
            &[],
            "org.apache.cassandra.db.marshal.TimeUUIDType",
        ),
        CqlType::Inet => info(
            "inet",
            &[],
            "org.apache.cassandra.db.marshal.InetAddressType",
        ),
        CqlType::Date => info(
            "date",
            &[],
            "org.apache.cassandra.db.marshal.SimpleDateType",
        ),
        CqlType::Time => info("time", &[], "org.apache.cassandra.db.marshal.TimeType"),
        CqlType::Smallint => info("smallint", &[], "org.apache.cassandra.db.marshal.ShortType"),
        CqlType::Tinyint => info("tinyint", &[], "org.apache.cassandra.db.marshal.ByteType"),
        CqlType::Duration => info(
            "duration",
            &[],
            "org.apache.cassandra.db.marshal.DurationType",
        ),
        // The marshal class is the POC's custom class (D20): accepted as a DDL
        // alias by `custom_class_type`, and reported for schema round trips.
        CqlType::Jsonb => info("jsonb", &[], JSONB_MARSHAL_CLASS),
        CqlType::List(_) => Entry::Composite("list"),
        CqlType::Map(_, _) => Entry::Composite("map"),
        CqlType::Set(_) => Entry::Composite("set"),
        CqlType::Tuple(_) => Entry::Composite("tuple"),
        CqlType::Vector(_, _) => Entry::Composite("vector"),
        CqlType::Udt { .. } => Entry::Composite("udt"),
    }
}

/// The scalar row for `t`, or `None` for a composite.
fn scalar_info(t: &CqlType) -> Option<ScalarInfo> {
    match entry(t) {
        Entry::Scalar(i) => Some(i),
        Entry::Composite(_) => None,
    }
}

/// Canonical CQL name of a scalar type; `None` for non-scalars.
pub fn scalar_name(t: &CqlType) -> Option<&'static str> {
    scalar_info(t).map(|i| i.name)
}

/// Extra accepted spellings of a scalar (empty for most).
pub fn scalar_aliases(t: &CqlType) -> &'static [&'static str] {
    scalar_info(t).map_or(&[], |i| i.aliases)
}

/// Cassandra marshal class of a scalar type; `None` for non-scalars.
pub fn scalar_marshal_class(t: &CqlType) -> Option<&'static str> {
    scalar_info(t).map(|i| i.marshal)
}

/// Resolve a scalar name or alias, case-sensitively (already-lowercase input).
pub fn scalar_from_name(name: &str) -> Option<CqlType> {
    SCALAR_TYPES
        .iter()
        .find(|t| scalar_info(t).is_some_and(|i| i.name == name || i.aliases.contains(&name)))
        .cloned()
}

/// Resolve a scalar name or alias ignoring ASCII case.
pub fn scalar_from_name_ci(name: &str) -> Option<CqlType> {
    SCALAR_TYPES
        .iter()
        .find(|t| {
            scalar_info(t).is_some_and(|i| {
                i.name.eq_ignore_ascii_case(name)
                    || i.aliases.iter().any(|a| a.eq_ignore_ascii_case(name))
            })
        })
        .cloned()
}

/// A quoted custom class name that is not the jsonb alias (D20).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownCustomClass(pub String);

impl std::fmt::Display for UnknownCustomClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unsupported custom type class '{}': only '{JSONB_MARSHAL_CLASS}' (jsonb) is accepted",
            self.0
        )
    }
}

impl std::error::Error for UnknownCustomClass {}

/// Resolve a quoted custom class to a type. Only the POC jsonb class resolves;
/// every other class is a loud error (D20, FM-37).
pub fn custom_class_type(class: &str) -> Result<CqlType, UnknownCustomClass> {
    if class == JSONB_MARSHAL_CLASS {
        Ok(CqlType::Jsonb)
    } else {
        Err(UnknownCustomClass(class.to_string()))
    }
}

/// Where a `jsonb` was found that D21 forbids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonbNestingError {
    /// `set<jsonb>`: a set needs a stored order inside the cell (D3).
    SetElement,
    /// `map<jsonb, _>`: a map key needs the same (D3).
    MapKey,
    /// `vector<jsonb>`: vectors are numeric.
    VectorElement,
}

impl std::fmt::Display for JsonbNestingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self {
            Self::SetElement => {
                "set<jsonb> is not supported: a set element needs a jsonb order in key bytes (D3, D21)"
            }
            Self::MapKey => {
                "map<jsonb, ...> is not supported: a map key needs a jsonb order in key bytes (D3, D21)"
            }
            Self::VectorElement => "vector<jsonb> is not supported: vector elements are numeric (D21)",
        };
        f.write_str(what)
    }
}

impl std::error::Error for JsonbNestingError {}

/// Reject the jsonb nestings D21 forbids, at any depth. jsonb as a list
/// element, map value, tuple element or UDT field is allowed.
pub fn check_jsonb_nesting(t: &CqlType) -> Result<(), JsonbNestingError> {
    match t {
        CqlType::Set(inner) => {
            if **inner == CqlType::Jsonb {
                return Err(JsonbNestingError::SetElement);
            }
            check_jsonb_nesting(inner)
        }
        CqlType::Map(k, v) => {
            if **k == CqlType::Jsonb {
                return Err(JsonbNestingError::MapKey);
            }
            check_jsonb_nesting(k)?;
            check_jsonb_nesting(v)
        }
        CqlType::Vector(elem, _) => {
            if **elem == CqlType::Jsonb {
                return Err(JsonbNestingError::VectorElement);
            }
            check_jsonb_nesting(elem)
        }
        CqlType::List(inner) => check_jsonb_nesting(inner),
        CqlType::Tuple(types) => types.iter().try_for_each(check_jsonb_nesting),
        CqlType::Udt { fields, .. } => fields
            .iter()
            .try_for_each(|(_, ty)| check_jsonb_nesting(ty)),
        CqlType::Ascii
        | CqlType::Bigint
        | CqlType::Blob
        | CqlType::Boolean
        | CqlType::Counter
        | CqlType::Decimal
        | CqlType::Double
        | CqlType::Float
        | CqlType::Int
        | CqlType::Timestamp
        | CqlType::Uuid
        | CqlType::Varchar
        | CqlType::Varint
        | CqlType::Timeuuid
        | CqlType::Inet
        | CqlType::Date
        | CqlType::Time
        | CqlType::Smallint
        | CqlType::Tinyint
        | CqlType::Duration
        | CqlType::Jsonb => Ok(()),
    }
}

/// Short kind name for error messages: the scalar name, or `list`, `map`,
/// `set`, `tuple`, `vector`, `udt`.
pub fn kind_name(t: &CqlType) -> &'static str {
    match entry(t) {
        Entry::Scalar(i) => i.name,
        Entry::Composite(kind) => kind,
    }
}

/// Full CQL display name (`list<text>`, `map<text, int>`, `ks.udt`), as used in
/// `system_schema` `field_types` / aggregate columns.
pub fn display_name(t: &CqlType) -> String {
    match t {
        CqlType::List(inner) => format!("list<{}>", display_name(inner)),
        CqlType::Set(inner) => format!("set<{}>", display_name(inner)),
        CqlType::Map(k, v) => format!("map<{}, {}>", display_name(k), display_name(v)),
        CqlType::Tuple(types) => {
            let inner: Vec<String> = types.iter().map(display_name).collect();
            format!("tuple<{}>", inner.join(", "))
        }
        CqlType::Vector(elem, dim) => format!("vector<{}, {}>", display_name(elem), dim),
        CqlType::Udt { keyspace, name, .. } => format!("{keyspace}.{name}"),
        other => kind_name(other).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names_round_trip_every_scalar() {
        assert_eq!(SCALAR_TYPES.len(), 21);
        for ty in SCALAR_TYPES.iter() {
            let name = scalar_name(ty).expect("scalar has a canonical name");
            assert_eq!(scalar_from_name(name).as_ref(), Some(ty), "{name}");
            let class = scalar_marshal_class(ty).expect("scalar has a marshal class");
            assert!(class.starts_with("org.apache.cassandra.db.marshal."));
            assert_eq!(display_name(ty), name);
            assert_eq!(kind_name(ty), name);
            for alias in scalar_aliases(ty) {
                assert_eq!(scalar_from_name(alias).as_ref(), Some(ty), "{alias}");
            }
        }
    }

    #[test]
    fn scalar_names_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for ty in SCALAR_TYPES.iter() {
            let name = scalar_name(ty).expect("named");
            assert!(seen.insert(name), "duplicate {name}");
            for a in scalar_aliases(ty) {
                assert!(seen.insert(a), "duplicate alias {a}");
            }
        }
    }

    #[test]
    fn lookup_is_exact_and_ci_variant_folds_case() {
        assert_eq!(scalar_from_name("INT"), None);
        assert_eq!(scalar_from_name_ci("INT"), Some(CqlType::Int));
        assert_eq!(scalar_from_name("varchar"), Some(CqlType::Varchar));
        assert_eq!(scalar_name(&CqlType::Varchar), Some("text"));
        assert_eq!(scalar_from_name("nonsense"), None);
    }

    #[test]
    fn non_scalars_have_no_scalar_entry() {
        let list = CqlType::List(Box::new(CqlType::Int));
        assert_eq!(scalar_name(&list), None);
        assert_eq!(scalar_marshal_class(&list), None);
        assert_eq!(kind_name(&list), "list");
        assert_eq!(display_name(&list), "list<int>");
        let map = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int));
        assert_eq!(display_name(&map), "map<text, int>");
        let vec = CqlType::Vector(Box::new(CqlType::Float), 3);
        assert_eq!(display_name(&vec), "vector<float, 3>");
    }

    #[test]
    fn type_names_jsonb_and_poc_alias() {
        assert_eq!(scalar_from_name("jsonb"), Some(CqlType::Jsonb));
        assert_eq!(scalar_name(&CqlType::Jsonb), Some("jsonb"));
        assert_eq!(
            custom_class_type("org.apache.cassandra.db.marshal.JsonbType"),
            Ok(CqlType::Jsonb)
        );
        let err = custom_class_type("org.apache.cassandra.db.marshal.UTF8Type");
        assert_eq!(
            err,
            Err(UnknownCustomClass(
                "org.apache.cassandra.db.marshal.UTF8Type".to_string()
            ))
        );
        assert!(custom_class_type("").is_err());
    }

    #[test]
    fn jsonb_marshal_class_is_the_poc_alias() {
        assert_eq!(
            scalar_marshal_class(&CqlType::Jsonb),
            Some("org.apache.cassandra.db.marshal.JsonbType")
        );
    }

    #[test]
    fn nesting_rules_d21() {
        let j = || Box::new(CqlType::Jsonb);
        assert_eq!(check_jsonb_nesting(&CqlType::List(j())), Ok(()));
        assert_eq!(
            check_jsonb_nesting(&CqlType::Map(Box::new(CqlType::Varchar), j())),
            Ok(())
        );
        assert_eq!(
            check_jsonb_nesting(&CqlType::Tuple(vec![CqlType::Jsonb])),
            Ok(())
        );
        assert_eq!(
            check_jsonb_nesting(&CqlType::Set(j())),
            Err(JsonbNestingError::SetElement)
        );
        assert_eq!(
            check_jsonb_nesting(&CqlType::Map(j(), Box::new(CqlType::Int))),
            Err(JsonbNestingError::MapKey)
        );
        assert_eq!(
            check_jsonb_nesting(&CqlType::Vector(j(), 3)),
            Err(JsonbNestingError::VectorElement)
        );
        // Rejected at depth too.
        let deep = CqlType::List(Box::new(CqlType::Set(j())));
        assert_eq!(
            check_jsonb_nesting(&deep),
            Err(JsonbNestingError::SetElement)
        );
        let udt = CqlType::Udt {
            keyspace: "k".into(),
            name: "u".into(),
            fields: vec![("f".into(), CqlType::Jsonb)],
        };
        assert_eq!(check_jsonb_nesting(&udt), Ok(()));
    }
}
