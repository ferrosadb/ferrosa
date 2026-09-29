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
pub static SCALAR_TYPES: [CqlType; 20] = [
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
        assert_eq!(SCALAR_TYPES.len(), 20);
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
}
