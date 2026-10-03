//! Full CQL type descriptors and runtime values.
//!
//! These types were moved from `ferrosa-cql` so that `ferrosa-udf` (and other
//! crates below `ferrosa-cql` in the dependency graph) can reference them
//! without creating circular dependencies.

pub mod names;

use std::net::IpAddr;

use num_bigint::BigInt;
use serde::{Deserialize, Serialize};

/// CQL data type with protocol type ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CqlType {
    Ascii,                           // 0x0001
    Bigint,                          // 0x0002
    Blob,                            // 0x0003
    Boolean,                         // 0x0004
    Counter,                         // 0x0005
    Decimal,                         // 0x0006
    Double,                          // 0x0007
    Float,                           // 0x0008
    Int,                             // 0x0009
    Timestamp,                       // 0x000B
    Uuid,                            // 0x000C
    Varchar,                         // 0x000D
    Varint,                          // 0x000E
    Timeuuid,                        // 0x000F
    Inet,                            // 0x0010
    Date,                            // 0x0011
    Time,                            // 0x0012
    Smallint,                        // 0x0013
    Tinyint,                         // 0x0014
    Duration,                        // 0x0015
    List(Box<CqlType>),              // 0x0020
    Map(Box<CqlType>, Box<CqlType>), // 0x0021
    Set(Box<CqlType>),               // 0x0022
    Tuple(Vec<CqlType>),             // 0x0031
    /// User-Defined Type (0x0030).
    Udt {
        keyspace: String,
        name: String,
        fields: Vec<(String, CqlType)>,
    },
    /// Vector type: element type + fixed dimension.
    /// Cassandra encodes vectors as Custom type (0x0000) on the wire.
    Vector(Box<CqlType>, usize),
    /// Validated jsonb document (D3: regular columns only). Cassandra has no
    /// native id for it, so on the wire it is `Custom` (0x0000) like vectors;
    /// `system_schema` reports `text` (D6a).
    Jsonb,
}

impl CqlType {
    /// Returns the protocol type ID for this type.
    pub fn type_id(&self) -> u16 {
        match self {
            Self::Ascii => 0x0001,
            Self::Bigint => 0x0002,
            Self::Blob => 0x0003,
            Self::Boolean => 0x0004,
            Self::Counter => 0x0005,
            Self::Decimal => 0x0006,
            Self::Double => 0x0007,
            Self::Float => 0x0008,
            Self::Int => 0x0009,
            Self::Timestamp => 0x000B,
            Self::Uuid => 0x000C,
            Self::Varchar => 0x000D,
            Self::Varint => 0x000E,
            Self::Timeuuid => 0x000F,
            Self::Inet => 0x0010,
            Self::Date => 0x0011,
            Self::Time => 0x0012,
            Self::Smallint => 0x0013,
            Self::Tinyint => 0x0014,
            Self::Duration => 0x0015,
            Self::List(_) => 0x0020,
            Self::Map(_, _) => 0x0021,
            Self::Set(_) => 0x0022,
            Self::Udt { .. } => 0x0030,
            Self::Tuple(_) => 0x0031,
            Self::Jsonb => 0x0000, // Custom (no native Cassandra id, D6a)
            Self::Vector(_, _) => 0x0000, // Custom — Cassandra encodes vectors as Custom type
        }
    }
}

/// A CQL value at runtime.
///
/// Covers all scalar and collection types. Float/Double store raw bits
/// as u32/u64 so `Eq` can be derived. `Ord` is implemented manually
/// using `f32::total_cmp`/`f64::total_cmp` for IEEE 754 total ordering.
///
/// Note: `Null` is signaled out-of-band via the CQL wire protocol
/// length prefix (-1). `encode_value` for `Null` returns an empty vec;
/// callers are responsible for writing the -1 length prefix when encoding
/// a null cell. `decode_value` is never called for null (the caller
/// checks the length prefix first).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CqlValue {
    Null,
    Ascii(String),
    Bigint(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(i64),
    Decimal {
        scale: i32,
        #[serde(with = "bigint_serde")]
        unscaled: BigInt,
    },
    Double(u64), // f64 bits for Eq/Ord
    Float(u32),  // f32 bits for Eq/Ord
    Int(i32),
    Timestamp(i64),
    Uuid(uuid::Uuid),
    Text(String), // varchar
    #[serde(with = "bigint_serde")]
    Varint(BigInt),
    Timeuuid(uuid::Uuid),
    Inet(IpAddr),
    Date(u32),
    Time(i64),
    Smallint(i16),
    Tinyint(i8),
    /// CQL duration: months (i32), days (i32), nanoseconds (i64).
    /// Encoded as three zigzag-encoded variable-length integers.
    Duration {
        months: i32,
        days: i32,
        nanos: i64,
    },
    /// Ordered list of values.
    List(Vec<CqlValue>),
    /// Set of values. Uses Vec (not BTreeSet) to preserve exact wire order
    /// without re-sorting. The CQL protocol sends sets pre-sorted and
    /// pre-deduplicated. The bridge layer converts to BTreeSet if needed.
    Set(Vec<CqlValue>),
    /// Map of key-value pairs. Uses Vec (not BTreeMap) to preserve wire
    /// order. Same rationale as Set.
    Map(Vec<(CqlValue, CqlValue)>),
    /// Tuple -- fixed number of typed elements, some potentially null.
    Tuple(Vec<Option<CqlValue>>),
    /// Vector of f32 values (Cassandra 5.0 `vector<float, N>`).
    /// Stored as u32 bit patterns (like Float) so `Eq` can be derived.
    Vector(Vec<u32>),
    /// User-Defined Type -- named fields, some potentially null.
    Udt(Vec<(String, Option<CqlValue>)>),
    /// Validated jsonb cell (T-150). Never raw bytes: the only constructors of
    /// `JsonbValue` validate. `Eq`/`Ord`/`Hash` are by value (D2a, D18).
    Jsonb(ferrosa_jsonb::JsonbValue),
}

impl PartialOrd for CqlValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CqlValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Ascii(a), Self::Ascii(b)) | (Self::Text(a), Self::Text(b)) => a.cmp(b),
            (Self::Bigint(a), Self::Bigint(b))
            | (Self::Counter(a), Self::Counter(b))
            | (Self::Timestamp(a), Self::Timestamp(b))
            | (Self::Time(a), Self::Time(b)) => a.cmp(b),
            (
                Self::Duration {
                    months: ma,
                    days: da,
                    nanos: na,
                },
                Self::Duration {
                    months: mb,
                    days: db,
                    nanos: nb,
                },
            ) => ma.cmp(mb).then_with(|| da.cmp(db)).then_with(|| na.cmp(nb)),
            (Self::Int(a), Self::Int(b)) => a.cmp(b),
            (Self::Smallint(a), Self::Smallint(b)) => a.cmp(b),
            (Self::Tinyint(a), Self::Tinyint(b)) => a.cmp(b),
            (Self::Boolean(a), Self::Boolean(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => f32::from_bits(*a).total_cmp(&f32::from_bits(*b)),
            (Self::Double(a), Self::Double(b)) => f64::from_bits(*a).total_cmp(&f64::from_bits(*b)),
            (Self::Blob(a), Self::Blob(b)) => a.cmp(b),
            (Self::Uuid(a), Self::Uuid(b)) | (Self::Timeuuid(a), Self::Timeuuid(b)) => a.cmp(b),
            (Self::Inet(a), Self::Inet(b)) => a.to_string().cmp(&b.to_string()),
            (Self::Date(a), Self::Date(b)) => a.cmp(b),
            (Self::Varint(a), Self::Varint(b)) => a.cmp(b),
            (
                Self::Decimal {
                    scale: sa,
                    unscaled: ua,
                },
                Self::Decimal {
                    scale: sb,
                    unscaled: ub,
                },
            ) => sa.cmp(sb).then_with(|| ua.cmp(ub)),
            (Self::List(a), Self::List(b)) | (Self::Set(a), Self::Set(b)) => a.cmp(b),
            (Self::Map(a), Self::Map(b)) => a.cmp(b),
            (Self::Tuple(a), Self::Tuple(b)) => a.cmp(b),
            (Self::Vector(a), Self::Vector(b)) => {
                for (ba, bb) in a.iter().zip(b.iter()) {
                    let ord = f32::from_bits(*ba).total_cmp(&f32::from_bits(*bb));
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                a.len().cmp(&b.len())
            }
            (Self::Udt(a), Self::Udt(b)) => a.cmp(b),
            (Self::Jsonb(a), Self::Jsonb(b)) => a.cmp(b),
            // Different variants: order by variant index. Every variant is
            // listed on the left so adding a `CqlValue` variant without a
            // same-variant arm above fails to compile instead of comparing
            // Equal (jsonb hazard H3, FM-18).
            (Self::Null, _)
            | (Self::Ascii(_), _)
            | (Self::Bigint(_), _)
            | (Self::Blob(_), _)
            | (Self::Boolean(_), _)
            | (Self::Counter(_), _)
            | (Self::Decimal { .. }, _)
            | (Self::Double(_), _)
            | (Self::Float(_), _)
            | (Self::Int(_), _)
            | (Self::Timestamp(_), _)
            | (Self::Uuid(_), _)
            | (Self::Text(_), _)
            | (Self::Varint(_), _)
            | (Self::Timeuuid(_), _)
            | (Self::Inet(_), _)
            | (Self::Date(_), _)
            | (Self::Time(_), _)
            | (Self::Smallint(_), _)
            | (Self::Tinyint(_), _)
            | (Self::Duration { .. }, _)
            | (Self::List(_), _)
            | (Self::Set(_), _)
            | (Self::Map(_), _)
            | (Self::Tuple(_), _)
            | (Self::Vector(_), _)
            | (Self::Udt(_), _)
            | (Self::Jsonb(_), _) => self.discriminant_index().cmp(&other.discriminant_index()),
        }
    }
}

impl CqlValue {
    /// Discriminant index for cross-type ordering.
    fn discriminant_index(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Ascii(_) => 1,
            Self::Bigint(_) => 2,
            Self::Blob(_) => 3,
            Self::Boolean(_) => 4,
            Self::Counter(_) => 5,
            Self::Decimal { .. } => 6,
            Self::Double(_) => 7,
            Self::Float(_) => 8,
            Self::Int(_) => 9,
            Self::Timestamp(_) => 10,
            Self::Uuid(_) => 11,
            Self::Text(_) => 12,
            Self::Varint(_) => 13,
            Self::Timeuuid(_) => 14,
            Self::Inet(_) => 15,
            Self::Date(_) => 16,
            Self::Time(_) => 17,
            Self::Smallint(_) => 18,
            Self::Tinyint(_) => 19,
            Self::Duration { .. } => 20,
            Self::List(_) => 21,
            Self::Set(_) => 22,
            Self::Map(_) => 23,
            Self::Tuple(_) => 24,
            Self::Vector(_) => 25,
            Self::Udt(_) => 26,
            Self::Jsonb(_) => 27,
        }
    }
}

/// Compact canonical JSON text of a jsonb value (D6), for text-producing paths
/// such as `toJson`. Nothing is truncated: a failure is an error, never a
/// shortened string.
pub fn jsonb_canonical_text(
    j: &ferrosa_jsonb::JsonbValue,
) -> Result<String, ferrosa_jsonb::PrintError> {
    let view = j.view()?;
    ferrosa_jsonb::print_to_string(view.root(), ferrosa_jsonb::TextStyle::Canonical, usize::MAX)
}

/// Serde helper for `num_bigint::BigInt` which doesn't implement
/// `Serialize`/`Deserialize` out of the box.
mod bigint_serde {
    use num_bigint::BigInt;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(val: &BigInt, ser: S) -> Result<S::Ok, S::Error> {
        val.to_signed_bytes_be().serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<BigInt, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(de)?;
        Ok(BigInt::from_signed_bytes_be(&bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cql_type_udt_stores_fields() {
        let udt = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("zip".to_string(), CqlType::Int),
            ],
        };
        match udt {
            CqlType::Udt { fields, .. } => assert_eq!(fields.len(), 2),
            _ => panic!("expected Udt"),
        }
    }

    #[test]
    fn cql_value_udt_stores_named_fields() {
        let val = CqlValue::Udt(vec![
            (
                "street".to_string(),
                Some(CqlValue::Text("123 Main".to_string())),
            ),
            ("zip".to_string(), Some(CqlValue::Int(62701))),
        ]);
        match val {
            CqlValue::Udt(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "street");
            }
            _ => panic!("expected Udt"),
        }
    }

    #[test]
    fn cql_type_udt_type_id() {
        let udt = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "my_type".to_string(),
            fields: vec![],
        };
        assert_eq!(udt.type_id(), 0x0030);
    }

    #[test]
    fn cql_value_udt_ordering() {
        let a = CqlValue::Udt(vec![("x".to_string(), Some(CqlValue::Int(1)))]);
        let b = CqlValue::Udt(vec![("x".to_string(), Some(CqlValue::Int(2)))]);
        assert!(a < b);
    }

    #[test]
    fn cql_type_existing_variants_preserved() {
        // Verify existing type IDs are unchanged after the move
        assert_eq!(CqlType::Ascii.type_id(), 0x0001);
        assert_eq!(CqlType::Int.type_id(), 0x0009);
        assert_eq!(CqlType::Varchar.type_id(), 0x000D);
        assert_eq!(CqlType::List(Box::new(CqlType::Int)).type_id(), 0x0020);
        assert_eq!(CqlType::Tuple(vec![CqlType::Int]).type_id(), 0x0031);
    }

    /// One (lo, hi) pair per `CqlValue` variant, lo < hi by content.
    fn ordered_pairs() -> Vec<(CqlValue, CqlValue)> {
        use std::net::Ipv4Addr;
        let u1 = uuid::Uuid::from_u128(1);
        let u2 = uuid::Uuid::from_u128(2);
        let dur = |d| CqlValue::Duration {
            months: 0,
            days: d,
            nanos: 0,
        };
        vec![
            (CqlValue::Null, CqlValue::Null),
            (CqlValue::Ascii("a".into()), CqlValue::Ascii("b".into())),
            (CqlValue::Bigint(1), CqlValue::Bigint(2)),
            (CqlValue::Blob(vec![1]), CqlValue::Blob(vec![2])),
            (CqlValue::Boolean(false), CqlValue::Boolean(true)),
            (CqlValue::Counter(1), CqlValue::Counter(2)),
            (
                CqlValue::Decimal {
                    scale: 1,
                    unscaled: 1.into(),
                },
                CqlValue::Decimal {
                    scale: 1,
                    unscaled: 2.into(),
                },
            ),
            (
                CqlValue::Double(1.0f64.to_bits()),
                CqlValue::Double(2.0f64.to_bits()),
            ),
            (
                CqlValue::Float(1.0f32.to_bits()),
                CqlValue::Float(2.0f32.to_bits()),
            ),
            (CqlValue::Int(1), CqlValue::Int(2)),
            (CqlValue::Timestamp(1), CqlValue::Timestamp(2)),
            (CqlValue::Uuid(u1), CqlValue::Uuid(u2)),
            (CqlValue::Text("a".into()), CqlValue::Text("b".into())),
            (CqlValue::Varint(1.into()), CqlValue::Varint(2.into())),
            (CqlValue::Timeuuid(u1), CqlValue::Timeuuid(u2)),
            (
                CqlValue::Inet(Ipv4Addr::new(10, 0, 0, 1).into()),
                CqlValue::Inet(Ipv4Addr::new(10, 0, 0, 2).into()),
            ),
            (CqlValue::Date(1), CqlValue::Date(2)),
            (CqlValue::Time(1), CqlValue::Time(2)),
            (CqlValue::Smallint(1), CqlValue::Smallint(2)),
            (CqlValue::Tinyint(1), CqlValue::Tinyint(2)),
            (dur(1), dur(2)),
            (
                CqlValue::List(vec![CqlValue::Int(1)]),
                CqlValue::List(vec![CqlValue::Int(2)]),
            ),
            (
                CqlValue::Set(vec![CqlValue::Int(1)]),
                CqlValue::Set(vec![CqlValue::Int(2)]),
            ),
            (
                CqlValue::Map(vec![(CqlValue::Int(1), CqlValue::Int(1))]),
                CqlValue::Map(vec![(CqlValue::Int(1), CqlValue::Int(2))]),
            ),
            (
                CqlValue::Tuple(vec![Some(CqlValue::Int(1))]),
                CqlValue::Tuple(vec![Some(CqlValue::Int(2))]),
            ),
            (
                CqlValue::Vector(vec![1.0f32.to_bits()]),
                CqlValue::Vector(vec![2.0f32.to_bits()]),
            ),
            (
                CqlValue::Udt(vec![("x".into(), Some(CqlValue::Int(1)))]),
                CqlValue::Udt(vec![("x".into(), Some(CqlValue::Int(2)))]),
            ),
            (jsonb_of("1"), jsonb_of("2")),
        ]
    }

    #[test]
    fn cqlvalue_cmp_has_no_equal_wildcard() {
        use std::cmp::Ordering;
        let pairs = ordered_pairs();
        assert_eq!(pairs.len(), 28, "one pair per CqlValue variant");
        for (lo, hi) in &pairs {
            assert_eq!(lo.cmp(lo), Ordering::Equal, "{lo:?} equals itself");
            if lo == hi {
                continue; // Null has a single value
            }
            assert_eq!(lo.cmp(hi), Ordering::Less, "{lo:?} < {hi:?}");
            assert_eq!(hi.cmp(lo), Ordering::Greater, "{hi:?} > {lo:?}");
        }
    }

    #[test]
    fn no_wildcard_default_for_new_variant() {
        // Values of different variants never compare Equal, and the order
        // follows the variant index in both directions.
        let pairs = ordered_pairs();
        for (i, (a, _)) in pairs.iter().enumerate() {
            for (j, (b, _)) in pairs.iter().enumerate() {
                assert_eq!(a.cmp(b), i.cmp(&j), "{a:?} vs {b:?}");
            }
        }
    }

    fn jsonb_of(text: &str) -> CqlValue {
        use ferrosa_jsonb::{parse_text, JsonbValue, Limits, LimitsConfig};
        let limits = Limits::from_config_with_env(&LimitsConfig::default(), &|_| None, 64 << 20)
            .expect("default limits");
        let enc = parse_text(text.as_bytes(), &limits).expect("valid json");
        CqlValue::Jsonb(JsonbValue::from_encoded(enc).expect("valid cell"))
    }

    #[test]
    fn cqlvalue_cmp_jsonb_is_not_always_equal() {
        use std::cmp::Ordering;
        let a = jsonb_of("1");
        let b = jsonb_of("2");
        assert_ne!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a.cmp(&b), Ordering::Less);
        assert_eq!(b.cmp(&a), Ordering::Greater);
        // D18 kind order: Object > Array > Boolean > Number > String > Null.
        // A non-empty array stands for the Array kind: PostgreSQL sorts a
        // top-level EMPTY array below every scalar, null included (eafcfb3c).
        assert!(jsonb_of("{}") > jsonb_of("[1]"));
        assert!(jsonb_of("[1]") > jsonb_of("true"));
        assert!(jsonb_of("true") > jsonb_of("1"));
        assert!(jsonb_of("1") > jsonb_of("\"s\""));
        assert!(jsonb_of("\"s\"") > jsonb_of("null"));
        // The PostgreSQL exception: a top-level empty array sorts lowest.
        assert!(jsonb_of("[]") < jsonb_of("null"));
        assert!(jsonb_of("[]") < jsonb_of("[1]"));
    }

    #[test]
    fn cqlvalue_jsonb_ord_eq_hash_delegate_to_jsonb_value() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let (x, y) = (jsonb_of("1.0"), jsonb_of("1"));
        // D2a: equal by value although the bytes differ in scale.
        assert_eq!(x, y);
        assert_eq!(x.cmp(&y), std::cmp::Ordering::Equal);
        let hash = |v: &CqlValue| {
            let mut h = DefaultHasher::new();
            match v {
                CqlValue::Jsonb(j) => j.hash(&mut h),
                other => panic!("not jsonb: {other:?}"),
            }
            h.finish()
        };
        assert_eq!(hash(&x), hash(&y));
        assert_ne!(x, jsonb_of("2"));
    }

    #[test]
    fn cqlvalue_jsonb_orders_between_variants_by_discriminant() {
        assert!(CqlValue::Udt(vec![]) < jsonb_of("null"));
        assert!(jsonb_of("null") > CqlValue::Vector(vec![]));
    }

    #[test]
    fn jsonb_canonical_text_prints_the_document() {
        match jsonb_of("{\"a\": [1, 2.50]}") {
            CqlValue::Jsonb(j) => {
                assert_eq!(
                    jsonb_canonical_text(&j).expect("prints"),
                    "{\"a\":[1,2.50]}"
                )
            }
            other => panic!("not jsonb: {other:?}"),
        }
    }

    #[test]
    fn cqltype_jsonb_type_id_is_custom() {
        assert_eq!(CqlType::Jsonb.type_id(), 0x0000);
    }
}
