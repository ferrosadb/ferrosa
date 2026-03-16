//! Conversion between CqlValue and WASM Component Model types.
//!
//! The WIT contract defines a `cql-value` variant type. This module
//! converts between `ferrosa_common::CqlValue` and an intermediate Rust
//! enum (`WitCqlValue`) that mirrors the WIT representation. The executor
//! module then converts `WitCqlValue` to/from `Val::Variant` for the
//! dynamic component model API.
//!
//! Note: `wasmtime::component::bindgen!` cannot be used because the WIT
//! `cql-value` type is recursive (list/set/map/tuple/udt variants contain
//! `cql-value`), which is not supported by the component model's bindgen.

use ferrosa_common::{CqlType, CqlValue};
use num_bigint::BigInt;

use crate::error::UdfError;

/// Intermediate representation mirroring the WIT `cql-value` variant.
///
/// Each variant maps 1:1 to a case in the WIT `cql-value` variant type.
/// The executor converts these to `Val::Variant` with the matching
/// kebab-case discriminant name for the component model's dynamic API.
#[derive(Debug, Clone, PartialEq)]
pub enum WitCqlValue {
    Null,
    IntVal(i32),
    BigintVal(i64),
    FloatVal(f32),
    DoubleVal(f64),
    BooleanVal(bool),
    TextVal(String),
    BlobVal(Vec<u8>),
    UuidVal(String),
    TimestampVal(i64),
    /// WIT uses s32 for date; CqlValue::Date uses u32.
    /// We store as i32 to match the WIT contract and cast on conversion.
    DateVal(i32),
    TimeVal(i64),
    SmallintVal(i16),
    TinyintVal(i8),
    InetVal(String),
    /// Decimal: (unscaled big-endian signed bytes, scale).
    DecimalVal(Vec<u8>, i32),
    /// Varint: big-endian signed bytes.
    VarintVal(Vec<u8>),
    /// Duration: (months, days, nanoseconds).
    DurationVal(i32, i32, i64),
    AsciiVal(String),
    TimeuuidVal(String),
    ListVal(Vec<WitCqlValue>),
    SetVal(Vec<WitCqlValue>),
    MapVal(Vec<(WitCqlValue, WitCqlValue)>),
    TupleVal(Vec<WitCqlValue>),
    UdtVal(Vec<(String, WitCqlValue)>),
    CounterVal(i64),
}

/// Convert a CqlValue to its WIT representation.
pub fn cql_to_wit(value: &CqlValue) -> WitCqlValue {
    match value {
        CqlValue::Null => WitCqlValue::Null,
        CqlValue::Int(v) => WitCqlValue::IntVal(*v),
        CqlValue::Bigint(v) => WitCqlValue::BigintVal(*v),
        CqlValue::Float(v) => WitCqlValue::FloatVal(f32::from_bits(*v)),
        CqlValue::Double(v) => WitCqlValue::DoubleVal(f64::from_bits(*v)),
        CqlValue::Boolean(v) => WitCqlValue::BooleanVal(*v),
        CqlValue::Text(v) => WitCqlValue::TextVal(v.clone()),
        CqlValue::Ascii(v) => WitCqlValue::AsciiVal(v.clone()),
        CqlValue::Blob(v) => WitCqlValue::BlobVal(v.clone()),
        CqlValue::Uuid(v) => WitCqlValue::UuidVal(v.to_string()),
        CqlValue::Timeuuid(v) => WitCqlValue::TimeuuidVal(v.to_string()),
        CqlValue::Timestamp(v) => WitCqlValue::TimestampVal(*v),
        CqlValue::Date(v) => WitCqlValue::DateVal(*v as i32),
        CqlValue::Time(v) => WitCqlValue::TimeVal(*v),
        CqlValue::Smallint(v) => WitCqlValue::SmallintVal(*v),
        CqlValue::Tinyint(v) => WitCqlValue::TinyintVal(*v),
        CqlValue::Inet(v) => WitCqlValue::InetVal(v.to_string()),
        CqlValue::Counter(v) => WitCqlValue::CounterVal(*v),
        CqlValue::Decimal { scale, unscaled } => {
            WitCqlValue::DecimalVal(unscaled.to_signed_bytes_be(), *scale)
        }
        CqlValue::Varint(v) => WitCqlValue::VarintVal(v.to_signed_bytes_be()),
        CqlValue::Duration {
            months,
            days,
            nanos,
        } => WitCqlValue::DurationVal(*months, *days, *nanos),
        CqlValue::List(items) => WitCqlValue::ListVal(items.iter().map(cql_to_wit).collect()),
        CqlValue::Set(items) => WitCqlValue::SetVal(items.iter().map(cql_to_wit).collect()),
        CqlValue::Map(entries) => WitCqlValue::MapVal(
            entries
                .iter()
                .map(|(k, v)| (cql_to_wit(k), cql_to_wit(v)))
                .collect(),
        ),
        CqlValue::Tuple(items) => WitCqlValue::TupleVal(
            items
                .iter()
                .map(|opt| opt.as_ref().map_or(WitCqlValue::Null, cql_to_wit))
                .collect(),
        ),
        CqlValue::Udt(fields) => WitCqlValue::UdtVal(
            fields
                .iter()
                .map(|(name, opt)| {
                    (
                        name.clone(),
                        opt.as_ref().map_or(WitCqlValue::Null, cql_to_wit),
                    )
                })
                .collect(),
        ),
    }
}

/// Convert a WIT representation back to CqlValue.
/// Requires the target CqlType for proper reconstruction of Uuid, Inet,
/// and collection/composite types.
pub fn wit_to_cql(value: &WitCqlValue, target_type: &CqlType) -> Result<CqlValue, UdfError> {
    match value {
        WitCqlValue::Null => Ok(CqlValue::Null),
        WitCqlValue::IntVal(v) => Ok(CqlValue::Int(*v)),
        WitCqlValue::BigintVal(v) => Ok(CqlValue::Bigint(*v)),
        WitCqlValue::FloatVal(v) => Ok(CqlValue::Float(v.to_bits())),
        WitCqlValue::DoubleVal(v) => Ok(CqlValue::Double(v.to_bits())),
        WitCqlValue::BooleanVal(v) => Ok(CqlValue::Boolean(*v)),
        WitCqlValue::TextVal(v) => Ok(CqlValue::Text(v.clone())),
        WitCqlValue::AsciiVal(v) => Ok(CqlValue::Ascii(v.clone())),
        WitCqlValue::BlobVal(v) => Ok(CqlValue::Blob(v.clone())),
        WitCqlValue::UuidVal(v) => {
            let u = uuid::Uuid::parse_str(v)
                .map_err(|e| UdfError::TypeMismatch(format!("invalid UUID: {e}")))?;
            Ok(CqlValue::Uuid(u))
        }
        WitCqlValue::TimeuuidVal(v) => {
            let u = uuid::Uuid::parse_str(v)
                .map_err(|e| UdfError::TypeMismatch(format!("invalid TimeUUID: {e}")))?;
            Ok(CqlValue::Timeuuid(u))
        }
        WitCqlValue::TimestampVal(v) => Ok(CqlValue::Timestamp(*v)),
        WitCqlValue::DateVal(v) => Ok(CqlValue::Date(*v as u32)),
        WitCqlValue::TimeVal(v) => Ok(CqlValue::Time(*v)),
        WitCqlValue::SmallintVal(v) => Ok(CqlValue::Smallint(*v)),
        WitCqlValue::TinyintVal(v) => Ok(CqlValue::Tinyint(*v)),
        WitCqlValue::InetVal(v) => {
            let addr: std::net::IpAddr = v
                .parse()
                .map_err(|e| UdfError::TypeMismatch(format!("invalid IP: {e}")))?;
            Ok(CqlValue::Inet(addr))
        }
        WitCqlValue::CounterVal(v) => Ok(CqlValue::Counter(*v)),
        WitCqlValue::DecimalVal(unscaled_bytes, scale) => Ok(CqlValue::Decimal {
            scale: *scale,
            unscaled: BigInt::from_signed_bytes_be(unscaled_bytes),
        }),
        WitCqlValue::VarintVal(v) => Ok(CqlValue::Varint(BigInt::from_signed_bytes_be(v))),
        WitCqlValue::DurationVal(months, days, nanos) => Ok(CqlValue::Duration {
            months: *months,
            days: *days,
            nanos: *nanos,
        }),
        WitCqlValue::ListVal(items) => {
            let inner_type = match target_type {
                CqlType::List(inner) => inner.as_ref(),
                _ => return Err(UdfError::TypeMismatch("expected list type".into())),
            };
            let vals: Result<Vec<_>, _> = items.iter().map(|i| wit_to_cql(i, inner_type)).collect();
            Ok(CqlValue::List(vals?))
        }
        WitCqlValue::SetVal(items) => {
            let inner_type = match target_type {
                CqlType::Set(inner) => inner.as_ref(),
                _ => return Err(UdfError::TypeMismatch("expected set type".into())),
            };
            let vals: Result<Vec<_>, _> = items.iter().map(|i| wit_to_cql(i, inner_type)).collect();
            Ok(CqlValue::Set(vals?))
        }
        WitCqlValue::MapVal(entries) => {
            let (k_type, v_type) = match target_type {
                CqlType::Map(k, v) => (k.as_ref(), v.as_ref()),
                _ => return Err(UdfError::TypeMismatch("expected map type".into())),
            };
            let vals: Result<Vec<_>, _> = entries
                .iter()
                .map(|(k, v)| Ok((wit_to_cql(k, k_type)?, wit_to_cql(v, v_type)?)))
                .collect();
            Ok(CqlValue::Map(vals?))
        }
        WitCqlValue::TupleVal(items) => {
            let elem_types = match target_type {
                CqlType::Tuple(types) => types,
                _ => return Err(UdfError::TypeMismatch("expected tuple type".into())),
            };
            let vals: Result<Vec<_>, _> = items
                .iter()
                .zip(elem_types.iter())
                .map(|(v, t)| {
                    if matches!(v, WitCqlValue::Null) {
                        Ok(None)
                    } else {
                        Ok(Some(wit_to_cql(v, t)?))
                    }
                })
                .collect();
            Ok(CqlValue::Tuple(vals?))
        }
        WitCqlValue::UdtVal(fields) => {
            let field_defs = match target_type {
                CqlType::Udt { fields: defs, .. } => defs,
                _ => return Err(UdfError::TypeMismatch("expected UDT type".into())),
            };
            let vals: Result<Vec<_>, _> = fields
                .iter()
                .zip(field_defs.iter())
                .map(|((name, v), (_, ft))| {
                    if matches!(v, WitCqlValue::Null) {
                        Ok((name.clone(), None))
                    } else {
                        Ok((name.clone(), Some(wit_to_cql(v, ft)?)))
                    }
                })
                .collect();
            Ok(CqlValue::Udt(vals?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    // ---- Scalar round-trip tests ----

    #[test]
    fn roundtrip_null() {
        let orig = CqlValue::Null;
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::Null);
        let back = wit_to_cql(&wit, &CqlType::Int).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_int() {
        let orig = CqlValue::Int(42);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::IntVal(42));
        let back = wit_to_cql(&wit, &CqlType::Int).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_bigint() {
        let orig = CqlValue::Bigint(i64::MAX);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::BigintVal(i64::MAX));
        let back = wit_to_cql(&wit, &CqlType::Bigint).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_float() {
        let f: f32 = 1.5;
        let orig = CqlValue::Float(f.to_bits());
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::FloatVal(f));
        let back = wit_to_cql(&wit, &CqlType::Float).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_double() {
        let f: f64 = 1.5;
        let orig = CqlValue::Double(f.to_bits());
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::DoubleVal(f));
        let back = wit_to_cql(&wit, &CqlType::Double).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_boolean() {
        for b in [true, false] {
            let orig = CqlValue::Boolean(b);
            let wit = cql_to_wit(&orig);
            assert_eq!(wit, WitCqlValue::BooleanVal(b));
            let back = wit_to_cql(&wit, &CqlType::Boolean).unwrap();
            assert_eq!(back, orig);
        }
    }

    #[test]
    fn roundtrip_text() {
        let orig = CqlValue::Text("hello world".to_string());
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::TextVal("hello world".to_string()));
        let back = wit_to_cql(&wit, &CqlType::Varchar).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_ascii() {
        let orig = CqlValue::Ascii("ASCII".to_string());
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::AsciiVal("ASCII".to_string()));
        let back = wit_to_cql(&wit, &CqlType::Ascii).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_blob() {
        let orig = CqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::BlobVal(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        let back = wit_to_cql(&wit, &CqlType::Blob).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_uuid() {
        let u = uuid::Uuid::new_v4();
        let orig = CqlValue::Uuid(u);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::UuidVal(u.to_string()));
        let back = wit_to_cql(&wit, &CqlType::Uuid).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_timeuuid() {
        let u = uuid::Uuid::new_v4(); // not a real timeuuid but fine for conversion test
        let orig = CqlValue::Timeuuid(u);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::TimeuuidVal(u.to_string()));
        let back = wit_to_cql(&wit, &CqlType::Timeuuid).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_timestamp() {
        let orig = CqlValue::Timestamp(1_700_000_000_000);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::TimestampVal(1_700_000_000_000));
        let back = wit_to_cql(&wit, &CqlType::Timestamp).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_date() {
        // CQL date: unsigned 32-bit, center-epoch at 2^31.
        let orig = CqlValue::Date(2_147_483_648); // epoch day
        let wit = cql_to_wit(&orig);
        // u32 -> i32 wraps: 2^31 as i32 is i32::MIN
        assert_eq!(wit, WitCqlValue::DateVal(2_147_483_648u32 as i32));
        let back = wit_to_cql(&wit, &CqlType::Date).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_time() {
        let orig = CqlValue::Time(43_200_000_000_000); // noon in nanoseconds
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::TimeVal(43_200_000_000_000));
        let back = wit_to_cql(&wit, &CqlType::Time).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_smallint() {
        let orig = CqlValue::Smallint(-1234);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::SmallintVal(-1234));
        let back = wit_to_cql(&wit, &CqlType::Smallint).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_tinyint() {
        let orig = CqlValue::Tinyint(-42);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::TinyintVal(-42));
        let back = wit_to_cql(&wit, &CqlType::Tinyint).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_inet_v4() {
        let addr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        let orig = CqlValue::Inet(addr);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::InetVal("192.168.1.1".to_string()));
        let back = wit_to_cql(&wit, &CqlType::Inet).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_inet_v6() {
        let addr = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let orig = CqlValue::Inet(addr);
        let wit = cql_to_wit(&orig);
        let back = wit_to_cql(&wit, &CqlType::Inet).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_counter() {
        let orig = CqlValue::Counter(999);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::CounterVal(999));
        let back = wit_to_cql(&wit, &CqlType::Counter).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_decimal() {
        let orig = CqlValue::Decimal {
            scale: 3,
            unscaled: BigInt::from(123456),
        };
        let wit = cql_to_wit(&orig);
        match &wit {
            WitCqlValue::DecimalVal(bytes, scale) => {
                assert_eq!(*scale, 3);
                assert_eq!(BigInt::from_signed_bytes_be(bytes), BigInt::from(123456));
            }
            _ => panic!("expected DecimalVal"),
        }
        let back = wit_to_cql(&wit, &CqlType::Decimal).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_varint() {
        let orig = CqlValue::Varint(BigInt::from(-999_999_999_i64));
        let wit = cql_to_wit(&orig);
        match &wit {
            WitCqlValue::VarintVal(bytes) => {
                assert_eq!(
                    BigInt::from_signed_bytes_be(bytes),
                    BigInt::from(-999_999_999_i64)
                );
            }
            _ => panic!("expected VarintVal"),
        }
        let back = wit_to_cql(&wit, &CqlType::Varint).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_duration() {
        let orig = CqlValue::Duration {
            months: 1,
            days: 15,
            nanos: 3_600_000_000_000, // 1 hour
        };
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::DurationVal(1, 15, 3_600_000_000_000));
        let back = wit_to_cql(&wit, &CqlType::Duration).unwrap();
        assert_eq!(back, orig);
    }

    // ---- Collection round-trip tests ----

    #[test]
    fn roundtrip_list() {
        let orig = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)]);
        let wit = cql_to_wit(&orig);
        assert_eq!(
            wit,
            WitCqlValue::ListVal(vec![
                WitCqlValue::IntVal(1),
                WitCqlValue::IntVal(2),
                WitCqlValue::IntVal(3),
            ])
        );
        let back = wit_to_cql(&wit, &CqlType::List(Box::new(CqlType::Int))).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_set() {
        let orig = CqlValue::Set(vec![CqlValue::Text("a".into()), CqlValue::Text("b".into())]);
        let wit = cql_to_wit(&orig);
        let back = wit_to_cql(&wit, &CqlType::Set(Box::new(CqlType::Varchar))).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_map() {
        let orig = CqlValue::Map(vec![
            (CqlValue::Int(1), CqlValue::Text("one".into())),
            (CqlValue::Int(2), CqlValue::Text("two".into())),
        ]);
        let wit = cql_to_wit(&orig);
        let back = wit_to_cql(
            &wit,
            &CqlType::Map(Box::new(CqlType::Int), Box::new(CqlType::Varchar)),
        )
        .unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_empty_list() {
        let orig = CqlValue::List(vec![]);
        let wit = cql_to_wit(&orig);
        assert_eq!(wit, WitCqlValue::ListVal(vec![]));
        let back = wit_to_cql(&wit, &CqlType::List(Box::new(CqlType::Int))).unwrap();
        assert_eq!(back, orig);
    }

    // ---- Tuple and UDT round-trip tests ----

    #[test]
    fn roundtrip_tuple() {
        let orig = CqlValue::Tuple(vec![
            Some(CqlValue::Int(1)),
            None,
            Some(CqlValue::Text("hello".into())),
        ]);
        let wit = cql_to_wit(&orig);
        assert_eq!(
            wit,
            WitCqlValue::TupleVal(vec![
                WitCqlValue::IntVal(1),
                WitCqlValue::Null,
                WitCqlValue::TextVal("hello".into()),
            ])
        );
        let back = wit_to_cql(
            &wit,
            &CqlType::Tuple(vec![CqlType::Int, CqlType::Bigint, CqlType::Varchar]),
        )
        .unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_udt() {
        let orig = CqlValue::Udt(vec![
            (
                "street".to_string(),
                Some(CqlValue::Text("123 Main".into())),
            ),
            ("zip".to_string(), Some(CqlValue::Int(62701))),
            ("apt".to_string(), None),
        ]);
        let wit = cql_to_wit(&orig);
        assert_eq!(
            wit,
            WitCqlValue::UdtVal(vec![
                (
                    "street".to_string(),
                    WitCqlValue::TextVal("123 Main".into())
                ),
                ("zip".to_string(), WitCqlValue::IntVal(62701)),
                ("apt".to_string(), WitCqlValue::Null),
            ])
        );
        let back = wit_to_cql(
            &wit,
            &CqlType::Udt {
                keyspace: "ks".to_string(),
                name: "address".to_string(),
                fields: vec![
                    ("street".to_string(), CqlType::Varchar),
                    ("zip".to_string(), CqlType::Int),
                    ("apt".to_string(), CqlType::Varchar),
                ],
            },
        )
        .unwrap();
        assert_eq!(back, orig);
    }

    // ---- Null handling ----

    #[test]
    fn null_in_tuple_roundtrips() {
        let orig = CqlValue::Tuple(vec![None, None]);
        let wit = cql_to_wit(&orig);
        assert_eq!(
            wit,
            WitCqlValue::TupleVal(vec![WitCqlValue::Null, WitCqlValue::Null])
        );
        let back = wit_to_cql(&wit, &CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar])).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn null_in_udt_roundtrips() {
        let orig = CqlValue::Udt(vec![("field".to_string(), None)]);
        let wit = cql_to_wit(&orig);
        assert_eq!(
            wit,
            WitCqlValue::UdtVal(vec![("field".to_string(), WitCqlValue::Null)])
        );
        let back = wit_to_cql(
            &wit,
            &CqlType::Udt {
                keyspace: "ks".into(),
                name: "t".into(),
                fields: vec![("field".into(), CqlType::Int)],
            },
        )
        .unwrap();
        assert_eq!(back, orig);
    }

    // ---- Error handling tests ----

    #[test]
    fn invalid_uuid_returns_error() {
        let bad = WitCqlValue::UuidVal("not-a-uuid".to_string());
        let result = wit_to_cql(&bad, &CqlType::Uuid);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, UdfError::TypeMismatch(ref msg) if msg.contains("invalid UUID")),
            "expected TypeMismatch with 'invalid UUID', got: {err}"
        );
    }

    #[test]
    fn invalid_timeuuid_returns_error() {
        let bad = WitCqlValue::TimeuuidVal("garbage".to_string());
        let result = wit_to_cql(&bad, &CqlType::Timeuuid);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            UdfError::TypeMismatch(ref msg) if msg.contains("invalid TimeUUID")
        ));
    }

    #[test]
    fn invalid_inet_returns_error() {
        let bad = WitCqlValue::InetVal("not.an.ip".to_string());
        let result = wit_to_cql(&bad, &CqlType::Inet);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            UdfError::TypeMismatch(ref msg) if msg.contains("invalid IP")
        ));
    }

    // ---- Type mismatch tests for collections ----

    #[test]
    fn list_with_wrong_target_type_errors() {
        let wit = WitCqlValue::ListVal(vec![WitCqlValue::IntVal(1)]);
        let result = wit_to_cql(&wit, &CqlType::Int); // wrong: Int, not List
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            UdfError::TypeMismatch(ref msg) if msg.contains("expected list type")
        ));
    }

    #[test]
    fn set_with_wrong_target_type_errors() {
        let wit = WitCqlValue::SetVal(vec![WitCqlValue::IntVal(1)]);
        let result = wit_to_cql(&wit, &CqlType::Int);
        assert!(matches!(
            result.unwrap_err(),
            UdfError::TypeMismatch(ref msg) if msg.contains("expected set type")
        ));
    }

    #[test]
    fn map_with_wrong_target_type_errors() {
        let wit = WitCqlValue::MapVal(vec![(WitCqlValue::IntVal(1), WitCqlValue::IntVal(2))]);
        let result = wit_to_cql(&wit, &CqlType::Int);
        assert!(matches!(
            result.unwrap_err(),
            UdfError::TypeMismatch(ref msg) if msg.contains("expected map type")
        ));
    }

    #[test]
    fn tuple_with_wrong_target_type_errors() {
        let wit = WitCqlValue::TupleVal(vec![WitCqlValue::IntVal(1)]);
        let result = wit_to_cql(&wit, &CqlType::Int);
        assert!(matches!(
            result.unwrap_err(),
            UdfError::TypeMismatch(ref msg) if msg.contains("expected tuple type")
        ));
    }

    #[test]
    fn udt_with_wrong_target_type_errors() {
        let wit = WitCqlValue::UdtVal(vec![("f".into(), WitCqlValue::IntVal(1))]);
        let result = wit_to_cql(&wit, &CqlType::Int);
        assert!(matches!(
            result.unwrap_err(),
            UdfError::TypeMismatch(ref msg) if msg.contains("expected UDT type")
        ));
    }

    // ---- Nested collection tests ----

    #[test]
    fn roundtrip_nested_list_of_lists() {
        let orig = CqlValue::List(vec![
            CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]),
            CqlValue::List(vec![CqlValue::Int(3)]),
        ]);
        let wit = cql_to_wit(&orig);
        let target = CqlType::List(Box::new(CqlType::List(Box::new(CqlType::Int))));
        let back = wit_to_cql(&wit, &target).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_map_with_list_values() {
        let orig = CqlValue::Map(vec![(
            CqlValue::Text("key".into()),
            CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]),
        )]);
        let wit = cql_to_wit(&orig);
        let target = CqlType::Map(
            Box::new(CqlType::Varchar),
            Box::new(CqlType::List(Box::new(CqlType::Int))),
        );
        let back = wit_to_cql(&wit, &target).unwrap();
        assert_eq!(back, orig);
    }

    // ---- Special float values ----

    #[test]
    fn roundtrip_float_nan() {
        let orig = CqlValue::Float(f32::NAN.to_bits());
        let wit = cql_to_wit(&orig);
        let back = wit_to_cql(&wit, &CqlType::Float).unwrap();
        // NaN bits should be preserved
        assert_eq!(back, orig);
    }

    #[test]
    fn roundtrip_double_infinity() {
        let orig = CqlValue::Double(f64::INFINITY.to_bits());
        let wit = cql_to_wit(&orig);
        let back = wit_to_cql(&wit, &CqlType::Double).unwrap();
        assert_eq!(back, orig);
    }
}
