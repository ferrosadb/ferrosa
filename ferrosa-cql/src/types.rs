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

pub use ferrosa_common::{CqlType, CqlValue};

use crate::error::CqlError;

// The wire-format value codec was extracted to the neutral `ferrosa-row-bridge`
// crate (decision D10) so `ferrosa-postgres` can reuse the *exact same* decode
// path without depending on `ferrosa-cql`. `encode_value` has no error and is a
// straight re-export at its original path (`ferrosa_cql::types::encode_value`).
pub use ferrosa_row_bridge::encode_value;

/// Decode a [`CqlValue`] from CQL wire-format bytes given its type.
///
/// Thin wrapper over [`ferrosa_row_bridge::decode_value`] that maps the
/// bridge's `RowBridgeError` back to [`CqlError`] (always `CqlError::Invalid`,
/// identical to the original behaviour) so the ~hundreds of in-crate callers
/// are unaffected.
pub fn decode_value(cql_type: &CqlType, bytes: &[u8]) -> Result<CqlValue, CqlError> {
    ferrosa_row_bridge::decode_value(cql_type, bytes).map_err(CqlError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;

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
    use num_bigint::BigInt;
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
            any::<i64>().prop_map(|n| (CqlType::Varint, CqlValue::Varint(BigInt::from(n)))),
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
