//! Full CQL type descriptors and runtime values.
//!
//! These types were moved from `ferrosa-cql` so that `ferrosa-udf` (and other
//! crates below `ferrosa-cql` in the dependency graph) can reference them
//! without creating circular dependencies.

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
    /// Vector type (CQL v5): `vector<element_type, dimension>`.
    Vector(Box<CqlType>, u32), // 0x0032
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
            Self::Vector(_, _) => 0x0032,
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
    /// User-Defined Type -- named fields, some potentially null.
    Udt(Vec<(String, Option<CqlValue>)>),
}

impl PartialOrd for CqlValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CqlValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let d_self = std::mem::discriminant(self);
        let d_other = std::mem::discriminant(other);
        if d_self != d_other {
            return self.discriminant_index().cmp(&other.discriminant_index());
        }
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
            (Self::Udt(a), Self::Udt(b)) => a.cmp(b),
            _ => Ordering::Equal, // same discriminant, unreachable
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
            Self::Udt(_) => 25,
        }
    }
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
}
