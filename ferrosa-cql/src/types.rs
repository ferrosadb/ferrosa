//! CQL type system: type identifiers, value encoding, and decoding.
//!
//! The CQL native protocol assigns a 16-bit type ID to each data type.
//! Collection types (list, map, set) carry their element type IDs inline.
//! `CqlValue` is the runtime representation used throughout query execution.

use std::net::IpAddr;

use num_bigint::BigInt;

use crate::error::CqlError;

/// CQL data type with protocol type ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CqlType {
    Ascii,   // 0x0001
    Bigint,  // 0x0002
    Blob,    // 0x0003
    Boolean, // 0x0004
    Counter, // 0x0005
    Decimal, // 0x0006
    Double,  // 0x0007
    Float,   // 0x0008
    Int,     // 0x0009
    // 0x000A = custom, not supported
    Timestamp, // 0x000B
    Uuid,      // 0x000C
    Varchar,   // 0x000D
    Varint,    // 0x000E
    Timeuuid,  // 0x000F
    Inet,      // 0x0010
    Date,      // 0x0011
    Time,      // 0x0012
    Smallint,  // 0x0013
    Tinyint,   // 0x0014
    // 0x0015 = duration, deferred
    // 0x0030 = UDT, deferred
    List(Box<CqlType>),              // 0x0020
    Map(Box<CqlType>, Box<CqlType>), // 0x0021
    Set(Box<CqlType>),               // 0x0022
    Tuple(Vec<CqlType>),             // 0x0031
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
            Self::List(_) => 0x0020,
            Self::Map(_, _) => 0x0021,
            Self::Set(_) => 0x0022,
            Self::Tuple(_) => 0x0031,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CqlValue {
    Null,
    Ascii(String),
    Bigint(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(i64),
    Decimal {
        scale: i32,
        unscaled: BigInt,
    },
    Double(u64), // f64 bits for Eq/Ord
    Float(u32),  // f32 bits for Eq/Ord
    Int(i32),
    Timestamp(i64),
    Uuid(uuid::Uuid),
    Text(String), // varchar
    Varint(BigInt),
    Timeuuid(uuid::Uuid),
    Inet(IpAddr),
    Date(u32),
    Time(i64),
    Smallint(i16),
    Tinyint(i8),
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
            Self::List(_) => 20,
            Self::Set(_) => 21,
            Self::Map(_) => 22,
            Self::Tuple(_) => 23,
        }
    }

    /// Encode this value as CQL wire-format bytes (no length prefix).
    pub fn encode_value(&self) -> Vec<u8> {
        match self {
            Self::Ascii(s) | Self::Text(s) => s.as_bytes().to_vec(),
            Self::Bigint(n) | Self::Counter(n) | Self::Timestamp(n) => n.to_be_bytes().to_vec(),
            Self::Blob(b) => b.clone(),
            Self::Boolean(b) => vec![if *b { 1 } else { 0 }],
            Self::Decimal { scale, unscaled } => {
                let mut buf = scale.to_be_bytes().to_vec();
                buf.extend_from_slice(&unscaled.to_signed_bytes_be());
                buf
            }
            Self::Double(bits) => bits.to_be_bytes().to_vec(),
            Self::Float(bits) => bits.to_be_bytes().to_vec(),
            Self::Int(n) => n.to_be_bytes().to_vec(),
            Self::Uuid(u) | Self::Timeuuid(u) => u.as_bytes().to_vec(),
            Self::Varint(n) => n.to_signed_bytes_be(),
            Self::Inet(ip) => match ip {
                IpAddr::V4(v4) => v4.octets().to_vec(),
                IpAddr::V6(v6) => v6.octets().to_vec(),
            },
            Self::Date(d) => d.to_be_bytes().to_vec(),
            Self::Time(t) => t.to_be_bytes().to_vec(),
            Self::Smallint(n) => n.to_be_bytes().to_vec(),
            Self::Tinyint(n) => n.to_be_bytes().to_vec(),
            Self::List(items) | Self::Set(items) => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&(items.len() as i32).to_be_bytes());
                for item in items {
                    let encoded = item.encode_value();
                    buf.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                    buf.extend_from_slice(&encoded);
                }
                buf
            }
            Self::Map(entries) => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());
                for (k, v) in entries {
                    let ek = k.encode_value();
                    buf.extend_from_slice(&(ek.len() as i32).to_be_bytes());
                    buf.extend_from_slice(&ek);
                    let ev = v.encode_value();
                    buf.extend_from_slice(&(ev.len() as i32).to_be_bytes());
                    buf.extend_from_slice(&ev);
                }
                buf
            }
            Self::Tuple(elements) => {
                let mut buf = Vec::new();
                for elem in elements {
                    match elem {
                        Some(val) => {
                            let encoded = val.encode_value();
                            buf.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                            buf.extend_from_slice(&encoded);
                        }
                        None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                    }
                }
                buf
            }
            Self::Null => vec![],
        }
    }

    /// Decode a value from CQL wire-format bytes given its type.
    pub fn decode_value(cql_type: &CqlType, bytes: &[u8]) -> Result<Self, CqlError> {
        match cql_type {
            CqlType::Ascii => Ok(Self::Ascii(
                String::from_utf8(bytes.to_vec())
                    .map_err(|e| CqlError::Invalid(format!("invalid ASCII: {e}")))?,
            )),
            CqlType::Varchar => Ok(Self::Text(
                String::from_utf8(bytes.to_vec())
                    .map_err(|e| CqlError::Invalid(format!("invalid UTF-8: {e}")))?,
            )),
            CqlType::Bigint => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("bigint requires 8 bytes".into()))?;
                Ok(Self::Bigint(i64::from_be_bytes(arr)))
            }
            CqlType::Counter => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("counter requires 8 bytes".into()))?;
                Ok(Self::Counter(i64::from_be_bytes(arr)))
            }
            CqlType::Blob => Ok(Self::Blob(bytes.to_vec())),
            CqlType::Boolean => {
                if bytes.len() != 1 {
                    return Err(CqlError::Invalid("boolean requires 1 byte".into()));
                }
                Ok(Self::Boolean(bytes[0] != 0))
            }
            CqlType::Double => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("double requires 8 bytes".into()))?;
                Ok(Self::Double(u64::from_be_bytes(arr)))
            }
            CqlType::Float => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("float requires 4 bytes".into()))?;
                Ok(Self::Float(u32::from_be_bytes(arr)))
            }
            CqlType::Int => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("int requires 4 bytes".into()))?;
                Ok(Self::Int(i32::from_be_bytes(arr)))
            }
            CqlType::Timestamp => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("timestamp requires 8 bytes".into()))?;
                Ok(Self::Timestamp(i64::from_be_bytes(arr)))
            }
            CqlType::Uuid => {
                if bytes.len() != 16 {
                    return Err(CqlError::Invalid("uuid requires 16 bytes".into()));
                }
                Ok(Self::Uuid(uuid::Uuid::from_slice(bytes).map_err(|e| {
                    CqlError::Invalid(format!("invalid uuid: {e}"))
                })?))
            }
            CqlType::Timeuuid => {
                if bytes.len() != 16 {
                    return Err(CqlError::Invalid("timeuuid requires 16 bytes".into()));
                }
                Ok(Self::Timeuuid(uuid::Uuid::from_slice(bytes).map_err(
                    |e| CqlError::Invalid(format!("invalid timeuuid: {e}")),
                )?))
            }
            CqlType::Inet => match bytes.len() {
                4 => {
                    let arr: [u8; 4] = bytes.try_into().unwrap();
                    Ok(Self::Inet(IpAddr::V4(arr.into())))
                }
                16 => {
                    let arr: [u8; 16] = bytes.try_into().unwrap();
                    Ok(Self::Inet(IpAddr::V6(arr.into())))
                }
                n => Err(CqlError::Invalid(format!(
                    "inet requires 4 or 16 bytes, got {n}"
                ))),
            },
            CqlType::Date => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("date requires 4 bytes".into()))?;
                Ok(Self::Date(u32::from_be_bytes(arr)))
            }
            CqlType::Time => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("time requires 8 bytes".into()))?;
                Ok(Self::Time(i64::from_be_bytes(arr)))
            }
            CqlType::Smallint => {
                let arr: [u8; 2] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("smallint requires 2 bytes".into()))?;
                Ok(Self::Smallint(i16::from_be_bytes(arr)))
            }
            CqlType::Tinyint => {
                if bytes.len() != 1 {
                    return Err(CqlError::Invalid("tinyint requires 1 byte".into()));
                }
                Ok(Self::Tinyint(bytes[0] as i8))
            }
            CqlType::Varint => Ok(Self::Varint(BigInt::from_signed_bytes_be(bytes))),
            CqlType::Decimal => {
                if bytes.len() < 4 {
                    return Err(CqlError::Invalid(
                        "decimal requires at least 4 bytes".into(),
                    ));
                }
                let scale = i32::from_be_bytes(bytes[..4].try_into().unwrap());
                let unscaled = BigInt::from_signed_bytes_be(&bytes[4..]);
                Ok(Self::Decimal { scale, unscaled })
            }
            CqlType::List(elem_type) => {
                let (count, mut pos) = read_collection_header(bytes)?;
                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let (val, next_pos) = read_collection_element(bytes, pos, elem_type)?;
                    items.push(val);
                    pos = next_pos;
                }
                Ok(Self::List(items))
            }
            CqlType::Set(elem_type) => {
                let (count, mut pos) = read_collection_header(bytes)?;
                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let (val, next_pos) = read_collection_element(bytes, pos, elem_type)?;
                    items.push(val);
                    pos = next_pos;
                }
                Ok(Self::Set(items))
            }
            CqlType::Map(key_type, val_type) => {
                let (count, mut pos) = read_collection_header(bytes)?;
                let mut entries = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let (k, kpos) = read_collection_element(bytes, pos, key_type)?;
                    let (v, vpos) = read_collection_element(bytes, kpos, val_type)?;
                    entries.push((k, v));
                    pos = vpos;
                }
                Ok(Self::Map(entries))
            }
            CqlType::Tuple(elem_types) => {
                let mut elements = Vec::with_capacity(elem_types.len());
                let mut pos = 0;
                for et in elem_types {
                    if pos + 4 > bytes.len() {
                        return Err(CqlError::Invalid("tuple truncated".into()));
                    }
                    let len = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    if len < 0 {
                        elements.push(None);
                    } else {
                        let end = pos + len as usize;
                        if end > bytes.len() {
                            return Err(CqlError::Invalid("tuple element truncated".into()));
                        }
                        elements.push(Some(CqlValue::decode_value(et, &bytes[pos..end])?));
                        pos = end;
                    }
                }
                Ok(Self::Tuple(elements))
            }
        }
    }
}

/// Read the 4-byte element count from a collection header.
fn read_collection_header(bytes: &[u8]) -> Result<(i32, usize), CqlError> {
    if bytes.len() < 4 {
        return Err(CqlError::Invalid("collection too short for count".into()));
    }
    let count = i32::from_be_bytes(bytes[..4].try_into().unwrap());
    if count < 0 {
        return Err(CqlError::Invalid("negative collection count".into()));
    }
    Ok((count, 4))
}

/// Read one length-prefixed element from a collection at `pos`.
fn read_collection_element(
    bytes: &[u8],
    pos: usize,
    elem_type: &CqlType,
) -> Result<(CqlValue, usize), CqlError> {
    if pos + 4 > bytes.len() {
        return Err(CqlError::Invalid(
            "collection truncated at element length".into(),
        ));
    }
    let len = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
    if len < 0 {
        return Ok((CqlValue::Null, pos + 4));
    }
    let len = len as usize;
    let end = pos + 4 + len;
    if end > bytes.len() {
        return Err(CqlError::Invalid(
            "collection truncated at element data".into(),
        ));
    }
    let val = CqlValue::decode_value(elem_type, &bytes[pos + 4..end])?;
    Ok((val, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    // === Task 5: CqlType tests ===

    #[test]
    fn type_id_roundtrip() {
        let types = [
            (0x0001, CqlType::Ascii),
            (0x0002, CqlType::Bigint),
            (0x0003, CqlType::Blob),
            (0x0004, CqlType::Boolean),
            (0x0005, CqlType::Counter),
            (0x0006, CqlType::Decimal),
            (0x0007, CqlType::Double),
            (0x0008, CqlType::Float),
            (0x0009, CqlType::Int),
            (0x000B, CqlType::Timestamp),
            (0x000C, CqlType::Uuid),
            (0x000D, CqlType::Varchar),
            (0x000E, CqlType::Varint),
            (0x000F, CqlType::Timeuuid),
            (0x0010, CqlType::Inet),
            (0x0011, CqlType::Date),
            (0x0012, CqlType::Time),
            (0x0013, CqlType::Smallint),
            (0x0014, CqlType::Tinyint),
            (0x0020, CqlType::List(Box::new(CqlType::Int))),
            (
                0x0021,
                CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int)),
            ),
            (0x0022, CqlType::Set(Box::new(CqlType::Uuid))),
        ];
        for &(id, ref expected_variant) in &types {
            if !matches!(
                expected_variant,
                CqlType::List(_) | CqlType::Map(_, _) | CqlType::Set(_)
            ) {
                assert_eq!(expected_variant.type_id(), id);
            }
        }
    }

    #[test]
    fn type_id_for_collections() {
        assert_eq!(CqlType::List(Box::new(CqlType::Int)).type_id(), 0x0020);
        assert_eq!(
            CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int)).type_id(),
            0x0021
        );
        assert_eq!(CqlType::Set(Box::new(CqlType::Uuid)).type_id(), 0x0022);
    }

    // === Task 6: CqlValue scalar encode/decode tests ===

    #[test]
    fn encode_decode_int() {
        let val = CqlValue::Int(42);
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Int, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_bigint() {
        let val = CqlValue::Bigint(i64::MAX);
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Bigint, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_text() {
        let val = CqlValue::Text("hello world".to_string());
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Varchar, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_boolean_true() {
        let val = CqlValue::Boolean(true);
        let bytes = val.encode_value();
        assert_eq!(bytes, vec![1]);
        let decoded = CqlValue::decode_value(&CqlType::Boolean, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_boolean_false() {
        let val = CqlValue::Boolean(false);
        let bytes = val.encode_value();
        assert_eq!(bytes, vec![0]);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn encode_decode_float() {
        let val = CqlValue::Float(3.14f32.to_bits());
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Float, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_double() {
        let val = CqlValue::Double(std::f64::consts::PI.to_bits());
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Double, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_uuid() {
        let id = uuid::Uuid::new_v4();
        let val = CqlValue::Uuid(id);
        let bytes = val.encode_value();
        assert_eq!(bytes.len(), 16);
        let decoded = CqlValue::decode_value(&CqlType::Uuid, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_blob() {
        let val = CqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Blob, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_inet_v4() {
        let val = CqlValue::Inet(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)));
        let bytes = val.encode_value();
        assert_eq!(bytes.len(), 4);
        let decoded = CqlValue::decode_value(&CqlType::Inet, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_inet_v6() {
        let val = CqlValue::Inet(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));
        let bytes = val.encode_value();
        assert_eq!(bytes.len(), 16);
        let decoded = CqlValue::decode_value(&CqlType::Inet, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_smallint() {
        let val = CqlValue::Smallint(-1234);
        let bytes = val.encode_value();
        assert_eq!(bytes.len(), 2);
        let decoded = CqlValue::decode_value(&CqlType::Smallint, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_tinyint() {
        let val = CqlValue::Tinyint(-42);
        let bytes = val.encode_value();
        assert_eq!(bytes.len(), 1);
        let decoded = CqlValue::decode_value(&CqlType::Tinyint, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_date() {
        let val = CqlValue::Date(19000);
        let bytes = val.encode_value();
        assert_eq!(bytes.len(), 4);
        let decoded = CqlValue::decode_value(&CqlType::Date, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_time() {
        let nanos: i64 = 12 * 3_600_000_000_000 + 30 * 60_000_000_000;
        let val = CqlValue::Time(nanos);
        let bytes = val.encode_value();
        assert_eq!(bytes.len(), 8);
        let decoded = CqlValue::decode_value(&CqlType::Time, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_timestamp() {
        let val = CqlValue::Timestamp(1_710_000_000_000);
        let bytes = val.encode_value();
        assert_eq!(bytes.len(), 8);
        let decoded = CqlValue::decode_value(&CqlType::Timestamp, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_varint() {
        let val = CqlValue::Varint(BigInt::from(123_456_789i64));
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Varint, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_varint_negative() {
        let val = CqlValue::Varint(BigInt::from(-1i64));
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Varint, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_decimal() {
        let val = CqlValue::Decimal {
            scale: 2,
            unscaled: BigInt::from(12345i64),
        };
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Decimal, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_ascii() {
        let val = CqlValue::Ascii("hello".into());
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Ascii, &bytes).unwrap();
        assert_eq!(decoded, val);
        assert!(matches!(decoded, CqlValue::Ascii(_)));
    }

    #[test]
    fn encode_decode_counter() {
        let val = CqlValue::Counter(42);
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Counter, &bytes).unwrap();
        assert_eq!(decoded, val);
        assert!(matches!(decoded, CqlValue::Counter(_)));
    }

    #[test]
    fn encode_decode_timeuuid() {
        let val = CqlValue::Timeuuid(uuid::Uuid::new_v4());
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(&CqlType::Timeuuid, &bytes).unwrap();
        assert_eq!(decoded, val);
        assert!(matches!(decoded, CqlValue::Timeuuid(_)));
    }

    #[test]
    fn float_ord_uses_total_ordering() {
        let neg = CqlValue::Float((-1.0f32).to_bits());
        let pos = CqlValue::Float(1.0f32.to_bits());
        assert!(neg < pos);
    }

    // === Task 7: Collection encode/decode tests ===

    #[test]
    fn encode_decode_list_of_ints() {
        let val = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)]);
        let bytes = val.encode_value();
        let decoded =
            CqlValue::decode_value(&CqlType::List(Box::new(CqlType::Int)), &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_empty_list() {
        let val = CqlValue::List(vec![]);
        let bytes = val.encode_value();
        let decoded =
            CqlValue::decode_value(&CqlType::List(Box::new(CqlType::Int)), &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_set_of_text() {
        let val = CqlValue::Set(vec![CqlValue::Text("a".into()), CqlValue::Text("b".into())]);
        let bytes = val.encode_value();
        let decoded =
            CqlValue::decode_value(&CqlType::Set(Box::new(CqlType::Varchar)), &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_map_text_to_int() {
        let val = CqlValue::Map(vec![
            (CqlValue::Text("x".into()), CqlValue::Int(10)),
            (CqlValue::Text("y".into()), CqlValue::Int(20)),
        ]);
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(
            &CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int)),
            &bytes,
        )
        .unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_nested_list_of_maps() {
        let inner = CqlValue::Map(vec![(CqlValue::Text("k".into()), CqlValue::Int(1))]);
        let val = CqlValue::List(vec![inner]);
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(
            &CqlType::List(Box::new(CqlType::Map(
                Box::new(CqlType::Varchar),
                Box::new(CqlType::Int),
            ))),
            &bytes,
        )
        .unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_tuple() {
        let val = CqlValue::Tuple(vec![
            Some(CqlValue::Int(42)),
            None,
            Some(CqlValue::Text("hello".into())),
        ]);
        let bytes = val.encode_value();
        let decoded = CqlValue::decode_value(
            &CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar, CqlType::Varchar]),
            &bytes,
        )
        .unwrap();
        assert_eq!(decoded, val);
    }
}
