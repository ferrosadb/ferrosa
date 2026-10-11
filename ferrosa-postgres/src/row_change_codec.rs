//! Real Cap'n Proto codec for the PostgreSQL MVCC row-version metadata.
//!
//! A PostgreSQL commit's Accord write-set carries, per partition, a
//! `Vec<RowChange>` — the before/after row images plus the SQL key and the
//! storage partition key for each row version the transaction wrote. That blob
//! used to be `serde_json::to_vec` on the coordinator (`query.rs`) and
//! `serde_json::from_slice` on replica apply (`mvcc.rs`): JSON, an *external*
//! interchange format, as the *internal* encoding for binary row images. Under
//! the Cap'n Proto rule that is a bug; it is also the largest JSON term left on
//! the bulk-load commit's acceptance path (the profiled "row decode / JSON
//! encode 23.6 MB" in `prepare`).
//!
//! This module replaces it with the [`envelope.capnp`] `PgMvccRowChanges` family
//! (`ferrosa_net::protocol::envelope_capnp`), so the metadata is real capnproto
//! like the rest of the internode protocol. The schema lives beside the Accord
//! family it travels with; the encode/decode lives here because only this crate
//! knows `ferrosa_sql::Value`.
//!
//! Byte-for-byte faithfulness: every [`Value`] variant maps to one union member,
//! so the codec preserves the derived `Eq`/`Hash` value semantics — a float by
//! its bit pattern (NaN and signed zero survive), a numeric by its exact
//! normalized `unscaled`/`scale` pair, jsonb by its validated canonical cell
//! bytes. The encoding is deterministic: `encode(decode(encode(x))) == encode(x)`.
//!
//! [`envelope.capnp`]: https://github.com/ferrosadb/ferrosa/blob/main/ferrosa-net/schema/ferrosa/internode/envelope.capnp

use std::net::IpAddr;

use capnp::message::{Builder, ReaderOptions};
use capnp::serialize;
use ferrosa_net::protocol::envelope_capnp::{
    pg_mvcc_inet, pg_mvcc_row, pg_mvcc_row_change, pg_mvcc_row_changes, pg_mvcc_value,
};
use ferrosa_sql::{Row, Value};
use num_bigint::BigInt;

use crate::mvcc::RowChange;

/// Traversal limit for a metadata payload, in words (8 bytes each).
///
/// Mirrors the internode envelope reader (`ferrosa-net`'s `decode_envelope`):
/// 8 Mi words = 64 MiB, comfortably above one partition's row-version metadata
/// (the whole `pgbench -i` write-set's is ~24 MB across every partition).
const TRAVERSAL_LIMIT_WORDS: usize = 8 * 1024 * 1024;

/// Encode one partition's `RowChange`s into a standalone capnp message.
///
/// The result is what `encode_postgres_mvcc_mutation` frames into the storage
/// mutation that the Accord write-set ships.
pub(crate) fn encode_partition(changes: &[RowChange]) -> Result<Vec<u8>, String> {
    let mut message = Builder::new_default();
    {
        let root = message.init_root::<pg_mvcc_row_changes::Builder>();
        let mut list = root.init_changes(changes.len() as u32);
        for (index, change) in changes.iter().enumerate() {
            write_row_change(change, list.reborrow().get(index as u32));
        }
    }
    Ok(serialize::write_message_to_words(&message))
}

/// Decode a payload produced by [`encode_partition`] back to its `RowChange`s.
///
/// Fails loud on a truncated, malformed, or schema-mismatched frame: the
/// metadata is only ever produced by [`encode_partition`], so anything else is
/// corruption, never a value to guess at.
pub(crate) fn decode_partition(payload: &[u8]) -> Result<Vec<RowChange>, String> {
    let reader = serialize::read_message(
        &mut std::io::Cursor::new(payload),
        ReaderOptions {
            traversal_limit_in_words: Some(TRAVERSAL_LIMIT_WORDS),
            nesting_limit: 64,
        },
    )
    .map_err(|error| format!("read PostgreSQL MVCC row-version frame: {error}"))?;
    let root = reader
        .get_root::<pg_mvcc_row_changes::Reader>()
        .map_err(|error| format!("root PostgreSQL MVCC row-version frame: {error}"))?;
    let changes = root
        .get_changes()
        .map_err(|error| format!("read PostgreSQL MVCC row-version list: {error}"))?;
    (0..changes.len())
        .map(|index| read_row_change(changes.get(index)))
        .collect()
}

fn write_row_change(change: &RowChange, mut builder: pg_mvcc_row_change::Builder<'_>) {
    builder.set_table(&change.table);
    write_value_list(
        &change.key,
        builder.reborrow().init_key(change.key.len() as u32),
    );
    builder.set_partition_key(&change.partition_key);
    if let Some(before) = &change.before {
        write_row(before, builder.reborrow().init_before());
    }
    if let Some(after) = &change.after {
        write_row(after, builder.init_after());
    }
}

fn read_row_change(reader: pg_mvcc_row_change::Reader<'_>) -> Result<RowChange, String> {
    let key = read_value_list(
        reader
            .get_key()
            .map_err(|error| format!("read PostgreSQL MVCC row key: {error}"))?,
    )?;
    let before = if reader.has_before() {
        Some(read_row(reader.get_before().map_err(|error| {
            format!("read PostgreSQL MVCC before image: {error}")
        })?)?)
    } else {
        None
    };
    let after = if reader.has_after() {
        Some(read_row(reader.get_after().map_err(|error| {
            format!("read PostgreSQL MVCC after image: {error}")
        })?)?)
    } else {
        None
    };
    Ok(RowChange {
        table: reader
            .get_table()
            .map_err(|error| format!("read PostgreSQL MVCC row table: {error}"))?
            .to_string()
            .map_err(|error| format!("read PostgreSQL MVCC row table text: {error}"))?,
        key,
        partition_key: reader
            .get_partition_key()
            .map_err(|error| format!("read PostgreSQL MVCC partition key: {error}"))?
            .to_vec(),
        before,
        after,
    })
}

fn write_row(row: &Row, builder: pg_mvcc_row::Builder<'_>) {
    write_value_list(&row.0, builder.init_values(row.0.len() as u32));
}

fn read_row(reader: pg_mvcc_row::Reader<'_>) -> Result<Row, String> {
    let values = reader
        .get_values()
        .map_err(|error| format!("read PostgreSQL MVCC row values: {error}"))?;
    Ok(Row(read_value_list(values)?))
}

fn write_value_list(
    values: &[Value],
    builder: capnp::struct_list::Builder<'_, pg_mvcc_value::Owned>,
) {
    let mut builder = builder;
    for (index, value) in values.iter().enumerate() {
        write_value(value, builder.reborrow().get(index as u32));
    }
}

fn read_value_list(
    reader: capnp::struct_list::Reader<'_, pg_mvcc_value::Owned>,
) -> Result<Vec<Value>, String> {
    (0..reader.len())
        .map(|index| read_value(reader.get(index)))
        .collect()
}

fn write_value(value: &Value, builder: pg_mvcc_value::Builder<'_>) {
    let mut op = builder.init_op();
    match value {
        Value::Null => op.set_null_value(()),
        Value::Int(inner) => op.set_int_value(*inner),
        Value::Text(text) => op.set_text_value(text),
        Value::Bool(inner) => op.set_bool_value(*inner),
        // `OrderedFloat<f64>` by bit pattern: NaN payloads and `-0.0` survive.
        Value::Float(inner) => op.set_float_value(inner.0.to_bits()),
        Value::Uuid(inner) => op.set_uuid_value(inner.as_bytes()),
        Value::Bytea(bytes) => op.set_bytea_value(bytes),
        Value::Timestamp(micros) => op.set_timestamp_value(*micros),
        Value::Date(days) => op.set_date_value(*days),
        Value::Time(micros) => op.set_time_value(*micros),
        Value::Inet(address) => {
            let mut inet = op.init_inet_value();
            match address {
                IpAddr::V4(v4) => {
                    inet.set_is_v6(false);
                    inet.set_octets(&v4.octets());
                }
                IpAddr::V6(v6) => {
                    inet.set_is_v6(true);
                    inet.set_octets(&v6.octets());
                }
            }
        }
        Value::Numeric { unscaled, scale } => {
            let mut numeric = op.init_numeric_value();
            numeric.set_unscaled(&unscaled.to_signed_bytes_le());
            numeric.set_scale(*scale);
        }
        Value::Jsonb(document) => op.set_jsonb_value(document.as_bytes()),
        Value::JsonPath(path) => op.set_json_path_value(path),
        Value::TextArray(items) => {
            let mut list = op.init_text_array_value(items.len() as u32);
            for (index, item) in items.iter().enumerate() {
                let mut element = list.reborrow().get(index as u32);
                match item {
                    Some(text) => {
                        element.set_present(true);
                        element.set_value(text);
                    }
                    None => element.set_present(false),
                }
            }
        }
    }
}

fn read_value(reader: pg_mvcc_value::Reader<'_>) -> Result<Value, String> {
    use pg_mvcc_value::op;
    Ok(
        match reader
            .get_op()
            .which()
            .map_err(|error| format!("read PostgreSQL MVCC value union: {error}"))?
        {
            op::NullValue(()) => Value::Null,
            op::IntValue(inner) => Value::Int(inner),
            op::TextValue(text) => Value::Text(read_text(text, "text value")?),
            op::BoolValue(inner) => Value::Bool(inner),
            op::FloatValue(bits) => Value::float(f64::from_bits(bits)),
            op::UuidValue(bytes) => {
                let bytes =
                    bytes.map_err(|error| format!("read PostgreSQL MVCC uuid value: {error}"))?;
                Value::Uuid(
                    uuid::Uuid::from_slice(bytes)
                        .map_err(|error| format!("read PostgreSQL MVCC uuid value: {error}"))?,
                )
            }
            op::ByteaValue(bytes) => {
                let bytes =
                    bytes.map_err(|error| format!("read PostgreSQL MVCC bytea value: {error}"))?;
                Value::Bytea(bytes.to_vec())
            }
            op::TimestampValue(micros) => Value::Timestamp(micros),
            op::DateValue(days) => Value::Date(days),
            op::TimeValue(micros) => Value::Time(micros),
            op::InetValue(inet) => {
                Value::Inet(read_inet(inet.map_err(|error| {
                    format!("read PostgreSQL MVCC inet value: {error}")
                })?)?)
            }
            op::NumericValue(numeric) => {
                let numeric = numeric
                    .map_err(|error| format!("read PostgreSQL MVCC numeric value: {error}"))?;
                let unscaled = numeric
                    .get_unscaled()
                    .map_err(|error| format!("read PostgreSQL MVCC numeric unscaled: {error}"))?;
                Value::Numeric {
                    unscaled: BigInt::from_signed_bytes_le(unscaled),
                    scale: numeric.get_scale(),
                }
            }
            op::JsonbValue(bytes) => {
                let bytes =
                    bytes.map_err(|error| format!("read PostgreSQL MVCC jsonb value: {error}"))?;
                Value::Jsonb(
                    ferrosa_jsonb::JsonbValue::from_bytes(bytes.to_vec())
                        .map_err(|error| format!("read PostgreSQL MVCC jsonb value: {error}"))?,
                )
            }
            op::JsonPathValue(path) => Value::JsonPath(read_text(path, "jsonpath value")?),
            op::TextArrayValue(list) => {
                let list =
                    list.map_err(|error| format!("read PostgreSQL MVCC text[] value: {error}"))?;
                let mut items = Vec::with_capacity(list.len() as usize);
                for index in 0..list.len() {
                    let element = list.get(index);
                    items.push(if element.get_present() {
                        Some(read_text(element.get_value(), "text[] element")?)
                    } else {
                        None
                    });
                }
                Value::TextArray(items)
            }
        },
    )
}

/// Read a capnp `Text` field, failing loud with `what` in the message.
fn read_text(reader: capnp::Result<capnp::text::Reader<'_>>, what: &str) -> Result<String, String> {
    reader
        .map_err(|error| format!("read PostgreSQL MVCC {what}: {error}"))?
        .to_string()
        .map_err(|error| format!("read PostgreSQL MVCC {what}: {error}"))
}

fn read_inet(reader: pg_mvcc_inet::Reader<'_>) -> Result<IpAddr, String> {
    let octets = reader
        .get_octets()
        .map_err(|error| format!("read PostgreSQL MVCC inet octets: {error}"))?;
    let octets: &[u8] = octets;
    if reader.get_is_v6() {
        let octets: [u8; 16] = octets
            .try_into()
            .map_err(|_| format!("IPv6 inet value has {} octets, expected 16", octets.len()))?;
        Ok(IpAddr::from(octets))
    } else {
        let octets: [u8; 4] = octets
            .try_into()
            .map_err(|_| format!("IPv4 inet value has {} octets, expected 4", octets.len()))?;
        Ok(IpAddr::from(octets))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jsonb(text: &str) -> Value {
        use ferrosa_jsonb::{parse_text, JsonbValue, Limits, LimitsConfig};
        let limits = Limits::from_config_with_env(&LimitsConfig::default(), &|_| None, 64 << 20)
            .expect("default limits");
        let encoded = parse_text(text.as_bytes(), &limits).expect("valid json");
        Value::Jsonb(JsonbValue::from_encoded(encoded).expect("valid cell"))
    }

    /// Every `ferrosa_sql::Value` variant, so the codec's union is exercised in
    /// full (a variant the schema omits would be a compile error in `write_value`).
    fn all_value_variants() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Int(-42),
            Value::Text("héllo\n\"x\"\\\u{0}".into()),
            Value::Bool(true),
            Value::float(3.5),
            Value::Uuid(uuid::Uuid::from_u128(
                0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
            )),
            Value::Bytea(vec![0, 255, 128, 7]),
            Value::Timestamp(1_700_000_000_000_000),
            Value::Date(-1),
            Value::Time(86_399_999_999),
            Value::Inet("192.168.0.1".parse().expect("v4")),
            Value::Inet("2001:db8::1".parse().expect("v6")),
            Value::numeric(BigInt::from(-12_345_678_901_234_567_890i128), 4),
            jsonb(r#"{"a":[1.0,{"b":2}],"s":"x"}"#),
            Value::JsonPath("$.a[0]".into()),
            Value::TextArray(vec![Some("one".into()), None, Some(String::new())]),
        ]
    }

    fn corpus() -> Vec<RowChange> {
        vec![
            RowChange {
                table: "ks.t".into(),
                key: vec![Value::Int(1)],
                partition_key: vec![0, 1, 2],
                before: None,
                after: Some(Row::new(all_value_variants())),
            },
            RowChange {
                table: "ks.empty".into(),
                key: Vec::new(),
                partition_key: Vec::new(),
                before: Some(Row::new(Vec::new())),
                after: None,
            },
            RowChange {
                table: "ks.t".into(),
                key: vec![Value::Text("pk".into()), Value::Int(2)],
                partition_key: b"partition-\0-bytes".to_vec(),
                before: Some(Row::new(vec![Value::Int(1), Value::Text("old".into())])),
                after: Some(Row::new(vec![Value::Int(2), Value::Text("new".into())])),
            },
        ]
    }

    /// The migration gate: over the same corpus, the new capnp encoding decodes
    /// to EXACTLY what the JSON predecessor decoded to, and re-encoding is
    /// canonical (byte-identical), so the wire form is pinned to the value it
    /// replaces and is deterministic.
    #[test]
    fn row_change_binary_encoding_round_trips_byte_identically_against_its_json_predecessor() {
        let corpus = corpus();
        let json = serde_json::to_vec(&corpus).expect("json encodes");
        let from_json: Vec<RowChange> = serde_json::from_slice(&json).expect("json decodes");
        let binary = encode_partition(&corpus).expect("binary encodes");
        let from_binary = decode_partition(&binary).expect("binary decodes");

        assert_eq!(corpus, from_json, "the JSON predecessor is lossless");
        assert_eq!(corpus, from_binary, "the capnp encoding is lossless");
        assert_eq!(
            from_binary, from_json,
            "the capnp frame decodes to exactly what the JSON frame decoded to"
        );

        let reencoded = encode_partition(&from_binary).expect("binary re-encodes");
        assert_eq!(binary, reencoded, "the capnp encoding is canonical");
    }

    /// Floats keep their exact bit pattern: `NaN` and `-0.0` survive a round trip
    /// (JSON cannot express `NaN` at all, which is why this is a binary-only case).
    #[test]
    fn row_change_binary_round_trips_nan_and_signed_zero_by_bit_pattern() {
        let change = RowChange {
            table: "ks.f".into(),
            key: vec![Value::Int(7)],
            partition_key: b"pk".to_vec(),
            before: Some(Row::new(vec![Value::float(-0.0)])),
            after: Some(Row::new(vec![
                Value::float(f64::NAN),
                Value::float(f64::INFINITY),
            ])),
        };
        let decoded =
            decode_partition(&encode_partition(std::slice::from_ref(&change)).unwrap()).unwrap();
        assert_eq!(decoded.len(), 1);
        let before = decoded[0].before.as_ref().expect("before image");
        match &before.0[0] {
            Value::Float(f) => {
                assert!(f.0.is_sign_negative() && f.0 == 0.0, "expected -0.0");
                assert_eq!(f.0.to_bits(), (-0.0f64).to_bits());
            }
            other => panic!("expected a float, got {other:?}"),
        }
        let after = decoded[0].after.as_ref().expect("after image");
        match &after.0[0] {
            Value::Float(f) => assert!(f.0.is_nan(), "expected NaN"),
            other => panic!("expected a float, got {other:?}"),
        }
        match &after.0[1] {
            Value::Float(f) => assert_eq!(f.0, f64::INFINITY),
            other => panic!("expected a float, got {other:?}"),
        }
    }

    /// The empty write-set and a single-row partition both round-trip through the
    /// capnp frame (the degenerate shapes the Accord Apply path can emit).
    #[test]
    fn empty_and_single_row_metadata_round_trip() {
        let empty = encode_partition(&[]).expect("empty encodes");
        assert!(
            decode_partition(&empty).expect("empty decodes").is_empty(),
            "an empty write-set decodes to no changes"
        );

        let single = vec![RowChange {
            table: "ks.one".into(),
            key: vec![Value::Int(1)],
            partition_key: vec![9],
            before: None,
            after: Some(Row::new(vec![Value::Text("v".into())])),
        }];
        assert_eq!(
            decode_partition(&encode_partition(&single).unwrap()).unwrap(),
            single
        );
    }

    /// Corrupt or truncated metadata fails LOUD — never a silently empty or
    /// partially-applied write-set.
    #[test]
    fn truncated_or_garbage_metadata_fails_loud() {
        let binary = encode_partition(&corpus()).expect("binary encodes");
        assert!(decode_partition(&binary[..binary.len() / 2]).is_err());
        assert!(decode_partition(b"not a capnp frame").is_err());
        assert!(decode_partition(&[]).is_err());
    }
}
