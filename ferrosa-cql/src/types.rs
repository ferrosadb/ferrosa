//! CQL type system: type identifiers, value encoding, and decoding.
//!
//! The CQL native protocol assigns a 16-bit type ID to each data type.
//! Collection types (list, map, set) carry their element type IDs inline.
//! `CqlValue` is the runtime representation used throughout query execution.
//!
//! The canonical definitions of [`CqlType`] and [`CqlValue`] live in
//! `ferrosa-common` so that `ferrosa-udf` can depend on them without
//! pulling in the full CQL crate. This module re-exports them and
//! provides the wire-format encode/decode logic.

use std::net::IpAddr;

use num_bigint::BigInt;

pub use ferrosa_common::{CqlType, CqlValue};

use crate::error::CqlError;

/// Encode a [`CqlValue`] as CQL wire-format bytes (no length prefix).
pub fn encode_value(value: &CqlValue) -> Vec<u8> {
    match value {
        CqlValue::Ascii(s) | CqlValue::Text(s) => s.as_bytes().to_vec(),
        CqlValue::Bigint(n) | CqlValue::Counter(n) | CqlValue::Timestamp(n) => {
            n.to_be_bytes().to_vec()
        }
        CqlValue::Blob(b) => b.clone(),
        CqlValue::Boolean(b) => vec![if *b { 1 } else { 0 }],
        CqlValue::Decimal { scale, unscaled } => {
            let mut buf = scale.to_be_bytes().to_vec();
            buf.extend_from_slice(&unscaled.to_signed_bytes_be());
            buf
        }
        CqlValue::Double(bits) => bits.to_be_bytes().to_vec(),
        CqlValue::Float(bits) => bits.to_be_bytes().to_vec(),
        CqlValue::Int(n) => n.to_be_bytes().to_vec(),
        CqlValue::Uuid(u) | CqlValue::Timeuuid(u) => u.as_bytes().to_vec(),
        CqlValue::Varint(n) => n.to_signed_bytes_be(),
        CqlValue::Inet(ip) => match ip {
            IpAddr::V4(v4) => v4.octets().to_vec(),
            IpAddr::V6(v6) => v6.octets().to_vec(),
        },
        CqlValue::Date(d) => d.to_be_bytes().to_vec(),
        CqlValue::Time(t) => t.to_be_bytes().to_vec(),
        CqlValue::Smallint(n) => n.to_be_bytes().to_vec(),
        CqlValue::Tinyint(n) => n.to_be_bytes().to_vec(),
        CqlValue::Duration {
            months,
            days,
            nanos,
        } => {
            let mut buf = Vec::new();
            buf.extend(encode_vint(*months as i64));
            buf.extend(encode_vint(*days as i64));
            buf.extend(encode_vint(*nanos));
            buf
        }
        CqlValue::List(items) | CqlValue::Set(items) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(items.len() as i32).to_be_bytes());
            for item in items {
                let encoded = encode_value(item);
                buf.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                buf.extend_from_slice(&encoded);
            }
            buf
        }
        CqlValue::Map(entries) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());
            for (k, v) in entries {
                let ek = encode_value(k);
                buf.extend_from_slice(&(ek.len() as i32).to_be_bytes());
                buf.extend_from_slice(&ek);
                let ev = encode_value(v);
                buf.extend_from_slice(&(ev.len() as i32).to_be_bytes());
                buf.extend_from_slice(&ev);
            }
            buf
        }
        CqlValue::Tuple(elements) => {
            let mut buf = Vec::new();
            for elem in elements {
                match elem {
                    Some(val) => {
                        let encoded = encode_value(val);
                        buf.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                        buf.extend_from_slice(&encoded);
                    }
                    None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                }
            }
            buf
        }
        CqlValue::Vector(bits) => {
            let mut buf = Vec::with_capacity(bits.len() * 4);
            for b in bits {
                buf.extend_from_slice(&f32::from_bits(*b).to_be_bytes());
            }
            buf
        }
        CqlValue::Udt(fields) => {
            let mut buf = Vec::new();
            for (_name, value) in fields {
                match value {
                    Some(val) => {
                        let encoded = encode_value(val);
                        buf.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                        buf.extend_from_slice(&encoded);
                    }
                    None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                }
            }
            buf
        }
        CqlValue::Null => vec![],
    }
}

/// Decode a [`CqlValue`] from CQL wire-format bytes given its type.
pub fn decode_value(cql_type: &CqlType, bytes: &[u8]) -> Result<CqlValue, CqlError> {
    match cql_type {
        CqlType::Ascii => Ok(CqlValue::Ascii(
            String::from_utf8(bytes.to_vec())
                .map_err(|e| CqlError::Invalid(format!("invalid ASCII: {e}")))?,
        )),
        CqlType::Varchar => Ok(CqlValue::Text(
            String::from_utf8(bytes.to_vec())
                .map_err(|e| CqlError::Invalid(format!("invalid UTF-8: {e}")))?,
        )),
        CqlType::Bigint => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| CqlError::Invalid("bigint requires 8 bytes".into()))?;
            Ok(CqlValue::Bigint(i64::from_be_bytes(arr)))
        }
        CqlType::Counter => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| CqlError::Invalid("counter requires 8 bytes".into()))?;
            Ok(CqlValue::Counter(i64::from_be_bytes(arr)))
        }
        CqlType::Blob => Ok(CqlValue::Blob(bytes.to_vec())),
        CqlType::Boolean => {
            if bytes.len() != 1 {
                return Err(CqlError::Invalid("boolean requires 1 byte".into()));
            }
            Ok(CqlValue::Boolean(bytes[0] != 0))
        }
        CqlType::Double => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| CqlError::Invalid("double requires 8 bytes".into()))?;
            Ok(CqlValue::Double(u64::from_be_bytes(arr)))
        }
        CqlType::Float => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| CqlError::Invalid("float requires 4 bytes".into()))?;
            Ok(CqlValue::Float(u32::from_be_bytes(arr)))
        }
        CqlType::Int => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| CqlError::Invalid("int requires 4 bytes".into()))?;
            Ok(CqlValue::Int(i32::from_be_bytes(arr)))
        }
        CqlType::Timestamp => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| CqlError::Invalid("timestamp requires 8 bytes".into()))?;
            Ok(CqlValue::Timestamp(i64::from_be_bytes(arr)))
        }
        CqlType::Uuid => {
            if bytes.len() != 16 {
                return Err(CqlError::Invalid("uuid requires 16 bytes".into()));
            }
            Ok(CqlValue::Uuid(uuid::Uuid::from_slice(bytes).map_err(
                |e| CqlError::Invalid(format!("invalid uuid: {e}")),
            )?))
        }
        CqlType::Timeuuid => {
            if bytes.len() != 16 {
                return Err(CqlError::Invalid("timeuuid requires 16 bytes".into()));
            }
            Ok(CqlValue::Timeuuid(uuid::Uuid::from_slice(bytes).map_err(
                |e| CqlError::Invalid(format!("invalid timeuuid: {e}")),
            )?))
        }
        CqlType::Inet => match bytes.len() {
            4 => {
                let arr: [u8; 4] = bytes.try_into().unwrap();
                Ok(CqlValue::Inet(IpAddr::V4(arr.into())))
            }
            16 => {
                let arr: [u8; 16] = bytes.try_into().unwrap();
                Ok(CqlValue::Inet(IpAddr::V6(arr.into())))
            }
            n => Err(CqlError::Invalid(format!(
                "inet requires 4 or 16 bytes, got {n}"
            ))),
        },
        CqlType::Date => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| CqlError::Invalid("date requires 4 bytes".into()))?;
            Ok(CqlValue::Date(u32::from_be_bytes(arr)))
        }
        CqlType::Time => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| CqlError::Invalid("time requires 8 bytes".into()))?;
            Ok(CqlValue::Time(i64::from_be_bytes(arr)))
        }
        CqlType::Smallint => {
            let arr: [u8; 2] = bytes
                .try_into()
                .map_err(|_| CqlError::Invalid("smallint requires 2 bytes".into()))?;
            Ok(CqlValue::Smallint(i16::from_be_bytes(arr)))
        }
        CqlType::Tinyint => {
            if bytes.len() != 1 {
                return Err(CqlError::Invalid("tinyint requires 1 byte".into()));
            }
            Ok(CqlValue::Tinyint(bytes[0] as i8))
        }
        CqlType::Duration => {
            let mut offset = 0;
            let months = decode_vint(bytes, &mut offset)? as i32;
            let days = decode_vint(bytes, &mut offset)? as i32;
            let nanos = decode_vint(bytes, &mut offset)?;
            Ok(CqlValue::Duration {
                months,
                days,
                nanos,
            })
        }
        CqlType::Varint => Ok(CqlValue::Varint(BigInt::from_signed_bytes_be(bytes))),
        CqlType::Decimal => {
            if bytes.len() < 4 {
                return Err(CqlError::Invalid(
                    "decimal requires at least 4 bytes".into(),
                ));
            }
            let scale = i32::from_be_bytes(bytes[..4].try_into().unwrap());
            let unscaled = BigInt::from_signed_bytes_be(&bytes[4..]);
            Ok(CqlValue::Decimal { scale, unscaled })
        }
        CqlType::List(elem_type) => {
            let (count, mut pos) = read_collection_header(bytes)?;
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let (val, next_pos) = read_collection_element(bytes, pos, elem_type)?;
                items.push(val);
                pos = next_pos;
            }
            Ok(CqlValue::List(items))
        }
        CqlType::Set(elem_type) => {
            let (count, mut pos) = read_collection_header(bytes)?;
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let (val, next_pos) = read_collection_element(bytes, pos, elem_type)?;
                items.push(val);
                pos = next_pos;
            }
            Ok(CqlValue::Set(items))
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
            Ok(CqlValue::Map(entries))
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
                    elements.push(Some(decode_value(et, &bytes[pos..end])?));
                    pos = end;
                }
            }
            Ok(CqlValue::Tuple(elements))
        }
        CqlType::Vector(_, dim) => {
            let expected = *dim * 4;
            if bytes.len() != expected {
                return Err(CqlError::Invalid(format!(
                    "vector<float, {}> requires {} bytes, got {}",
                    dim,
                    expected,
                    bytes.len()
                )));
            }
            let mut bits = Vec::with_capacity(*dim);
            for i in 0..*dim {
                let offset = i * 4;
                let arr: [u8; 4] = bytes[offset..offset + 4].try_into().unwrap();
                bits.push(f32::from_be_bytes(arr).to_bits());
            }
            Ok(CqlValue::Vector(bits))
        }
        CqlType::Udt {
            fields: field_defs, ..
        } => {
            let mut result_fields = Vec::with_capacity(field_defs.len());
            let mut pos = 0;
            for (field_name, field_type) in field_defs {
                if pos >= bytes.len() {
                    // Cassandra allows shorter encodings — trailing fields are null
                    result_fields.push((field_name.clone(), None));
                    continue;
                }
                if pos + 4 > bytes.len() {
                    return Err(CqlError::Invalid("UDT field truncated at length".into()));
                }
                let len = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
                pos += 4;
                if len < 0 {
                    result_fields.push((field_name.clone(), None));
                } else {
                    let end = pos + len as usize;
                    if end > bytes.len() {
                        return Err(CqlError::Invalid("UDT field truncated at data".into()));
                    }
                    let val = decode_value(field_type, &bytes[pos..end])?;
                    result_fields.push((field_name.clone(), Some(val)));
                    pos = end;
                }
            }
            Ok(CqlValue::Udt(result_fields))
        }
    }
}

/// Zigzag-encode and write a signed integer as a variable-length byte sequence.
/// This is the encoding Cassandra uses for the CQL duration type (0x0015).
fn encode_vint(value: i64) -> Vec<u8> {
    let zigzag = if value >= 0 {
        (value as u64) << 1
    } else {
        ((-(value + 1)) as u64) << 1 | 1
    };
    let mut buf = Vec::new();
    let mut v = zigzag;
    loop {
        if v < 0x80 {
            buf.push(v as u8);
            break;
        }
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf
}

/// Decode a zigzag-encoded variable-length integer from `data` at `offset`.
fn decode_vint(data: &[u8], offset: &mut usize) -> Result<i64, CqlError> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        if *offset >= data.len() {
            return Err(CqlError::Invalid("truncated vint in duration".into()));
        }
        let byte = data[*offset];
        *offset += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    // Zigzag decode
    Ok(((result >> 1) as i64) ^ (-((result & 1) as i64)))
}

/// Read the 4-byte element count from a collection header.
///
/// Validates that `count` does not exceed what the remaining buffer can
/// possibly hold. Each collection element needs at least a 4-byte length
/// prefix, so `count` cannot exceed `(remaining_bytes) / 4`. This prevents
/// a corrupt or malicious count from triggering a multi-gigabyte allocation.
fn read_collection_header(bytes: &[u8]) -> Result<(i32, usize), CqlError> {
    if bytes.len() < 4 {
        return Err(CqlError::Invalid("collection too short for count".into()));
    }
    let count = i32::from_be_bytes(bytes[..4].try_into().unwrap());
    if count < 0 {
        return Err(CqlError::Invalid("negative collection count".into()));
    }
    let remaining = bytes.len() - 4;
    if count as usize > remaining / 4 {
        tracing::warn!(
            count,
            remaining,
            first_bytes = ?&bytes[..std::cmp::min(16, bytes.len())],
            "collection decode: count exceeds buffer — possible data corruption"
        );
        return Err(CqlError::Invalid(format!(
            "collection count {} exceeds buffer capacity ({} bytes remaining)",
            count, remaining
        )));
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
    let val = decode_value(elem_type, &bytes[pos + 4..end])?;
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
            (0x0015, CqlType::Duration),
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
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Int, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_bigint() {
        let val = CqlValue::Bigint(i64::MAX);
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Bigint, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_text() {
        let val = CqlValue::Text("hello world".to_string());
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Varchar, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_boolean_true() {
        let val = CqlValue::Boolean(true);
        let bytes = encode_value(&val);
        assert_eq!(bytes, vec![1]);
        let decoded = decode_value(&CqlType::Boolean, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_boolean_false() {
        let val = CqlValue::Boolean(false);
        let bytes = encode_value(&val);
        assert_eq!(bytes, vec![0]);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn encode_decode_float() {
        let val = CqlValue::Float(3.14f32.to_bits());
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Float, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_double() {
        let val = CqlValue::Double(std::f64::consts::PI.to_bits());
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Double, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_uuid() {
        let id = uuid::Uuid::new_v4();
        let val = CqlValue::Uuid(id);
        let bytes = encode_value(&val);
        assert_eq!(bytes.len(), 16);
        let decoded = decode_value(&CqlType::Uuid, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_blob() {
        let val = CqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Blob, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_inet_v4() {
        let val = CqlValue::Inet(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)));
        let bytes = encode_value(&val);
        assert_eq!(bytes.len(), 4);
        let decoded = decode_value(&CqlType::Inet, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_inet_v6() {
        let val = CqlValue::Inet(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));
        let bytes = encode_value(&val);
        assert_eq!(bytes.len(), 16);
        let decoded = decode_value(&CqlType::Inet, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_smallint() {
        let val = CqlValue::Smallint(-1234);
        let bytes = encode_value(&val);
        assert_eq!(bytes.len(), 2);
        let decoded = decode_value(&CqlType::Smallint, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_tinyint() {
        let val = CqlValue::Tinyint(-42);
        let bytes = encode_value(&val);
        assert_eq!(bytes.len(), 1);
        let decoded = decode_value(&CqlType::Tinyint, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_date() {
        let val = CqlValue::Date(19000);
        let bytes = encode_value(&val);
        assert_eq!(bytes.len(), 4);
        let decoded = decode_value(&CqlType::Date, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_time() {
        let nanos: i64 = 12 * 3_600_000_000_000 + 30 * 60_000_000_000;
        let val = CqlValue::Time(nanos);
        let bytes = encode_value(&val);
        assert_eq!(bytes.len(), 8);
        let decoded = decode_value(&CqlType::Time, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_timestamp() {
        let val = CqlValue::Timestamp(1_710_000_000_000);
        let bytes = encode_value(&val);
        assert_eq!(bytes.len(), 8);
        let decoded = decode_value(&CqlType::Timestamp, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_varint() {
        let val = CqlValue::Varint(BigInt::from(123_456_789i64));
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Varint, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_varint_negative() {
        let val = CqlValue::Varint(BigInt::from(-1i64));
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Varint, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_decimal() {
        let val = CqlValue::Decimal {
            scale: 2,
            unscaled: BigInt::from(12345i64),
        };
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Decimal, &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_ascii() {
        let val = CqlValue::Ascii("hello".into());
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Ascii, &bytes).unwrap();
        assert_eq!(decoded, val);
        assert!(matches!(decoded, CqlValue::Ascii(_)));
    }

    #[test]
    fn encode_decode_counter() {
        let val = CqlValue::Counter(42);
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Counter, &bytes).unwrap();
        assert_eq!(decoded, val);
        assert!(matches!(decoded, CqlValue::Counter(_)));
    }

    #[test]
    fn encode_decode_timeuuid() {
        let val = CqlValue::Timeuuid(uuid::Uuid::new_v4());
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Timeuuid, &bytes).unwrap();
        assert_eq!(decoded, val);
        assert!(matches!(decoded, CqlValue::Timeuuid(_)));
    }

    // === Duration type tests ===

    #[test]
    fn duration_type_id() {
        assert_eq!(CqlType::Duration.type_id(), 0x0015);
    }

    #[test]
    fn duration_type_roundtrip() {
        let val = CqlValue::Duration {
            months: 14,
            days: 7,
            nanos: 3_600_000_000_000,
        };
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Duration, &bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn duration_type_zero() {
        let val = CqlValue::Duration {
            months: 0,
            days: 0,
            nanos: 0,
        };
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Duration, &bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn duration_type_negative() {
        let val = CqlValue::Duration {
            months: -1,
            days: -2,
            nanos: -3,
        };
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Duration, &bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn duration_type_large_values() {
        let val = CqlValue::Duration {
            months: i32::MAX,
            days: i32::MIN,
            nanos: i64::MAX,
        };
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Duration, &bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn duration_ord() {
        let a = CqlValue::Duration {
            months: 1,
            days: 0,
            nanos: 0,
        };
        let b = CqlValue::Duration {
            months: 2,
            days: 0,
            nanos: 0,
        };
        assert!(a < b);
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
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::List(Box::new(CqlType::Int)), &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_empty_list() {
        let val = CqlValue::List(vec![]);
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::List(Box::new(CqlType::Int)), &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_set_of_text() {
        let val = CqlValue::Set(vec![CqlValue::Text("a".into()), CqlValue::Text("b".into())]);
        let bytes = encode_value(&val);
        let decoded = decode_value(&CqlType::Set(Box::new(CqlType::Varchar)), &bytes).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn encode_decode_map_text_to_int() {
        let val = CqlValue::Map(vec![
            (CqlValue::Text("x".into()), CqlValue::Int(10)),
            (CqlValue::Text("y".into()), CqlValue::Int(20)),
        ]);
        let bytes = encode_value(&val);
        let decoded = decode_value(
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
        let bytes = encode_value(&val);
        let decoded = decode_value(
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
        let bytes = encode_value(&val);
        let decoded = decode_value(
            &CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar, CqlType::Varchar]),
            &bytes,
        )
        .unwrap();
        assert_eq!(decoded, val);
    }

    // === Task 4: UDT encode/decode tests ===

    #[test]
    fn encode_decode_udt_roundtrip() {
        let udt_type = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("zip".to_string(), CqlType::Int),
            ],
        };
        let udt_val = CqlValue::Udt(vec![
            (
                "street".to_string(),
                Some(CqlValue::Text("123 Main".to_string())),
            ),
            ("zip".to_string(), Some(CqlValue::Int(62701))),
        ]);
        let encoded = encode_value(&udt_val);
        let decoded = decode_value(&udt_type, &encoded).unwrap();
        assert_eq!(decoded, udt_val);
    }

    #[test]
    fn encode_udt_with_null_field() {
        let udt_type = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("zip".to_string(), CqlType::Int),
            ],
        };
        let udt_val = CqlValue::Udt(vec![
            (
                "street".to_string(),
                Some(CqlValue::Text("Main St".to_string())),
            ),
            ("zip".to_string(), None),
        ]);
        let encoded = encode_value(&udt_val);
        let decoded = decode_value(&udt_type, &encoded).unwrap();
        assert_eq!(decoded, udt_val);
    }

    // === OOM bounds-check tests for corrupt collection counts ===

    #[test]
    fn decode_list_with_corrupt_count_returns_error() {
        let bytes = [0x7F, 0xFF, 0xFF, 0xFF];
        let result = decode_value(&CqlType::List(Box::new(CqlType::Int)), &bytes);
        assert!(result.is_err(), "corrupt count must not trigger allocation");
    }

    #[test]
    fn decode_set_with_corrupt_count_returns_error() {
        let bytes = [0x7F, 0xFF, 0xFF, 0xFF];
        let result = decode_value(&CqlType::Set(Box::new(CqlType::Int)), &bytes);
        assert!(result.is_err());
    }

    #[test]
    fn decode_map_with_corrupt_count_returns_error() {
        let bytes = [0x7F, 0xFF, 0xFF, 0xFF];
        let result = decode_value(
            &CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int)),
            &bytes,
        );
        assert!(result.is_err());
    }

    #[test]
    fn decode_udt_with_fewer_fields_than_definition() {
        // Cassandra allows shorter encodings — trailing fields become null
        let udt_type = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("city".to_string(), CqlType::Varchar),
                ("zip".to_string(), CqlType::Int),
            ],
        };
        // Only encode 1 field (street) — city and zip should decode as null
        let partial_val = CqlValue::Udt(vec![(
            "street".to_string(),
            Some(CqlValue::Text("Main".to_string())),
        )]);
        let encoded = encode_value(&partial_val);
        let decoded = decode_value(&udt_type, &encoded).unwrap();
        // Should have all 3 fields, with city and zip as None
        match decoded {
            CqlValue::Udt(fields) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].0, "street");
                assert!(fields[0].1.is_some());
                assert_eq!(fields[1].0, "city");
                assert!(fields[1].1.is_none());
                assert_eq!(fields[2].0, "zip");
                assert!(fields[2].1.is_none());
            }
            _ => panic!("expected Udt"),
        }
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use std::net::IpAddr;

    fn arb_scalar_value() -> impl Strategy<Value = (CqlType, CqlValue)> {
        prop_oneof![
            any::<i32>().prop_map(|n| (CqlType::Int, CqlValue::Int(n))),
            any::<i64>().prop_map(|n| (CqlType::Bigint, CqlValue::Bigint(n))),
            any::<i64>().prop_map(|n| (CqlType::Counter, CqlValue::Counter(n))),
            any::<i16>().prop_map(|n| (CqlType::Smallint, CqlValue::Smallint(n))),
            any::<i8>().prop_map(|n| (CqlType::Tinyint, CqlValue::Tinyint(n))),
            any::<bool>().prop_map(|b| (CqlType::Boolean, CqlValue::Boolean(b))),
            any::<u32>().prop_map(|n| (CqlType::Float, CqlValue::Float(n))),
            any::<u64>().prop_map(|n| (CqlType::Double, CqlValue::Double(n))),
            any::<u32>().prop_map(|n| (CqlType::Date, CqlValue::Date(n))),
            any::<i64>().prop_map(|n| (CqlType::Time, CqlValue::Time(n))),
            any::<i64>().prop_map(|n| (CqlType::Timestamp, CqlValue::Timestamp(n))),
            "[ -~]{0,100}".prop_map(|s| (CqlType::Varchar, CqlValue::Text(s))),
            "[ -~]{0,100}".prop_map(|s| (CqlType::Ascii, CqlValue::Ascii(s))),
            prop::collection::vec(any::<u8>(), 0..100)
                .prop_map(|b| (CqlType::Blob, CqlValue::Blob(b))),
            prop::array::uniform16(any::<u8>())
                .prop_map(|b| (CqlType::Uuid, CqlValue::Uuid(uuid::Uuid::from_bytes(b)))),
            prop::array::uniform16(any::<u8>()).prop_map(|b| (
                CqlType::Timeuuid,
                CqlValue::Timeuuid(uuid::Uuid::from_bytes(b))
            )),
            (0..4u8).prop_map(|v| {
                let ip: IpAddr = if v < 2 {
                    IpAddr::V4(std::net::Ipv4Addr::new(v, v, v, v))
                } else {
                    IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
                };
                (CqlType::Inet, CqlValue::Inet(ip))
            }),
            any::<i64>().prop_map(|n| {
                use num_bigint::BigInt;
                (CqlType::Varint, CqlValue::Varint(BigInt::from(n)))
            }),
        ]
    }

    proptest! {
        #[test]
        fn scalar_roundtrip((cql_type, value) in arb_scalar_value()) {
            let encoded = encode_value(&value);
            let decoded = decode_value(&cql_type, &encoded).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn roundtrip_list(items in proptest::collection::vec(any::<i32>(), 0..20)) {
            let val = CqlValue::List(items.iter().map(|n| CqlValue::Int(*n)).collect());
            let encoded = encode_value(&val);
            let decoded = decode_value(&CqlType::List(Box::new(CqlType::Int)), &encoded).unwrap();
            prop_assert_eq!(val, decoded);
        }

        #[test]
        fn roundtrip_set(items in proptest::collection::vec(any::<i64>(), 0..20)) {
            let val = CqlValue::Set(items.iter().map(|n| CqlValue::Bigint(*n)).collect());
            let encoded = encode_value(&val);
            let decoded = decode_value(&CqlType::Set(Box::new(CqlType::Bigint)), &encoded).unwrap();
            prop_assert_eq!(val, decoded);
        }

        #[test]
        fn roundtrip_map(entries in proptest::collection::vec(
            (any::<i32>(), "\\PC{0,20}"), 0..10
        )) {
            let val = CqlValue::Map(
                entries.iter().map(|(k, v)| (CqlValue::Int(*k), CqlValue::Text(v.clone()))).collect()
            );
            let encoded = encode_value(&val);
            let decoded = decode_value(
                &CqlType::Map(Box::new(CqlType::Int), Box::new(CqlType::Varchar)),
                &encoded
            ).unwrap();
            prop_assert_eq!(val, decoded);
        }

        #[test]
        fn roundtrip_nested_list_of_tuples(
            items in proptest::collection::vec(
                (any::<i32>(), "\\PC{0,20}"), 0..5
            )
        ) {
            let tuple_type = CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]);
            let list_type = CqlType::List(Box::new(tuple_type.clone()));
            let tuples: Vec<CqlValue> = items.iter().map(|(n, s)| {
                CqlValue::Tuple(vec![Some(CqlValue::Int(*n)), Some(CqlValue::Text(s.clone()))])
            }).collect();
            let val = CqlValue::List(tuples);
            let encoded = encode_value(&val);
            let decoded = decode_value(&list_type, &encoded).unwrap();
            prop_assert_eq!(val, decoded);
        }
    }
}
