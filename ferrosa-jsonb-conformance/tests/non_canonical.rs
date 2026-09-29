//! Module: the valid-upstream-but-not-canonical table of the jsonb oracle.
//! Correctness: correct when every byte string that parquet-variant accepts as a
//! valid Variant, yet that breaks a ferrosa canonical rule (C2-C10, JB-T1), is
//! refused by `JsonbRef::validate` with the one specific typed fault for that
//! rule. ferrosa stores exactly one encoding per value (canonical storage), so
//! these refusals are intended; each row is documented in the crate README. No row
//! may be "fixed" by making ferrosa accept the bytes.
//! Last revised: 2026-09-28
//! Last changed: t_0b127826 initial table.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod support;

use std::collections::BTreeMap;

use ferrosa_jsonb::{EncodingFault, JsonbError, JsonbRef};
use parquet_variant::{
    Variant, VariantBuilder, VariantDecimal16, VariantDecimal4, VariantDecimal8,
};
use support::{cell_of, Dec, Model};

struct Row {
    name: &'static str,
    metadata: Vec<u8>,
    value: Vec<u8>,
    /// What parquet-variant must decode these bytes to (proves they are valid).
    upstream: Model,
    fault: EncodingFault,
}

fn non_canonical(rule: &'static str) -> EncodingFault {
    EncodingFault::NonCanonical(rule)
}

fn int(v: i128) -> Model {
    Model::Num(Dec {
        unscaled: v,
        scale: 0,
    })
}

fn obj_a(v: Model) -> Model {
    Model::Obj(BTreeMap::from([("a".to_string(), v)]))
}

/// `{"a": <variant>}` built by parquet-variant with the dictionary `["a"]`.
fn built_obj_a(variant: Variant<'_, '_>) -> (Vec<u8>, Vec<u8>) {
    let mut b = VariantBuilder::new().with_field_names(["a"]);
    let mut o = b.new_object();
    o.insert("a", variant);
    o.finish();
    b.finish()
}

/// Canonical sorted metadata `["a"]`.
const META_A: [u8; 5] = [0x11, 0x01, 0x00, 0x01, b'a'];

/// `{"a": raw}` assembled by hand to the spec (canonical object framing).
fn raw_obj_a(raw: &[u8]) -> Vec<u8> {
    let mut v = vec![
        0x02,
        0x01,
        0x00,
        0x00,
        u8::try_from(raw.len()).expect("small"),
    ];
    v.extend_from_slice(raw);
    v
}

fn built_rows() -> Vec<Row> {
    let mut rows = Vec::new();
    let mut push = |name, variant: Variant<'_, '_>, upstream: Model, rule| {
        let (metadata, value) = built_obj_a(variant);
        rows.push(Row {
            name,
            metadata,
            value,
            upstream: obj_a(upstream),
            fault: non_canonical(rule),
        });
    };
    push(
        "int64 holding 5 (fits int8)",
        Variant::Int64(5),
        int(5),
        "C9",
    );
    push(
        "int32 holding 5 (fits int8)",
        Variant::Int32(5),
        int(5),
        "C9",
    );
    push(
        "int16 holding 5 (fits int8)",
        Variant::Int16(5),
        int(5),
        "C9",
    );
    push(
        "int64 holding 100000 (fits int32)",
        Variant::Int64(100_000),
        int(100_000),
        "C9",
    );
    let dec = |unscaled, scale| Model::Num(Dec { unscaled, scale });
    let d4 = |i, s| Variant::Decimal4(VariantDecimal4::try_new(i, s).expect("d4"));
    let d8 = |i, s| Variant::Decimal8(VariantDecimal8::try_new(i, s).expect("d8"));
    let d16 = |i, s| Variant::Decimal16(VariantDecimal16::try_new(i, s).expect("d16"));
    push(
        "decimal4 with scale 0 (an integer)",
        d4(5, 0),
        dec(5, 0),
        "C9",
    );
    push(
        "decimal8 holding 12345 scale 2 (fits decimal4)",
        d8(12_345, 2),
        dec(12_345, 2),
        "C9",
    );
    push(
        "decimal16 holding 7 scale 3 (fits decimal4)",
        d16(7, 3),
        dec(7, 3),
        "C9",
    );
    push(
        "decimal16 with scale 0 holding 5 (an int8)",
        d16(5, 0),
        dec(5, 0),
        "C9",
    );
    push(
        "long string form for 2 bytes",
        Variant::String("hi"),
        Model::Str("hi".into()),
        "C7",
    );
    rows
}

fn built_dictionary_rows() -> Vec<Row> {
    let mut rows = Vec::new();
    // Keys inserted "b" then "a": the dictionary is not sorted, so upstream clears the bit.
    let mut b = VariantBuilder::new();
    let mut o = b.new_object();
    o.insert("b", Variant::Int8(2));
    o.insert("a", Variant::Int8(1));
    o.finish();
    let (metadata, value) = b.finish();
    let map = BTreeMap::from([("a".to_string(), int(1)), ("b".to_string(), int(2))]);
    rows.push(Row {
        name: "dictionary built unsorted (sorted_strings bit clear)",
        metadata,
        value,
        upstream: Model::Obj(map),
        fault: EncodingFault::BadMetadataHeader,
    });
    // Empty dictionary: upstream clears sorted_strings, ferrosa requires it set.
    let mut b = VariantBuilder::new();
    b.append_value(Variant::Null);
    let (metadata, value) = b.finish();
    rows.push(Row {
        name: "empty dictionary (upstream clears sorted_strings)",
        metadata,
        value,
        upstream: Model::Null,
        fault: EncodingFault::BadMetadataHeader,
    });
    // A dictionary key no value uses.
    let mut b = VariantBuilder::new().with_field_names(["a", "b"]);
    let mut o = b.new_object();
    o.insert("a", Variant::Int8(1));
    o.finish();
    let (metadata, value) = b.finish();
    rows.push(Row {
        name: "dictionary holds an unused key",
        metadata,
        value,
        upstream: obj_a(int(1)),
        fault: non_canonical("C3"),
    });
    rows
}

/// Primitive ids ferrosa excludes (C10) with a well-formed payload each.
fn excluded_rows() -> Vec<Row> {
    let table: [(&'static str, u8, usize); 10] = [
        ("double (primitive 7)", 7, 8),
        ("date (primitive 11)", 11, 4),
        ("timestamp micros (primitive 12)", 12, 8),
        ("timestamp ntz micros (primitive 13)", 13, 8),
        ("float (primitive 14)", 14, 4),
        ("binary (primitive 15)", 15, 4),
        ("time ntz (primitive 17)", 17, 8),
        ("timestamp nanos (primitive 18)", 18, 8),
        ("timestamp ntz nanos (primitive 19)", 19, 8),
        ("uuid (primitive 20)", 20, 16),
    ];
    table
        .into_iter()
        .map(|(name, id, payload)| {
            let mut raw = vec![id << 2];
            raw.extend(std::iter::repeat_n(0u8, payload));
            Row {
                name,
                metadata: META_A.to_vec(),
                value: raw_obj_a(&raw),
                upstream: Model::Null, // replaced below: only validity is asserted
                fault: EncodingFault::ExcludedPrimitive(id),
            }
        })
        .collect()
}

/// Valid framings with a wider-than-minimal width or a flag ferrosa forbids.
fn framing_rows() -> Vec<Row> {
    let a1 = obj_a(int(1));
    let arr1 = Model::Arr(vec![int(1)]);
    let row = |name, metadata: &[u8], value: &[u8], upstream: &Model, rule| Row {
        name,
        metadata: metadata.to_vec(),
        value: value.to_vec(),
        upstream: upstream.clone(),
        fault: non_canonical(rule),
    };
    let obj = |header: u8, count: &[u8], ids: &[u8], offs: &[u8]| {
        let mut v = vec![header];
        for part in [count, ids, offs, &[0x0C, 0x01]] {
            v.extend_from_slice(part);
        }
        v
    };
    vec![
        row(
            "metadata with 2-byte offsets for a 1-key dictionary",
            &[0x51, 1, 0, 0, 0, 1, 0, b'a'],
            &raw_obj_a(&[0x0C, 1]),
            &a1,
            "C4",
        ),
        row(
            "metadata with reserved bit 5 set",
            &[0x31, 1, 0, 1, b'a'],
            &raw_obj_a(&[0x0C, 1]),
            &a1,
            "C2",
        ),
        row(
            "object with 2-byte value offsets",
            &META_A,
            &obj(0x06, &[1], &[0], &[0, 0, 2, 0]),
            &a1,
            "C5",
        ),
        row(
            "object with 2-byte field ids",
            &META_A,
            &obj(0x12, &[1], &[0, 0], &[0, 2]),
            &a1,
            "C5",
        ),
        row(
            "object with is_large set for 1 field",
            &META_A,
            &obj(0x42, &[1, 0, 0, 0], &[0], &[0, 2]),
            &a1,
            "C5",
        ),
        row(
            "array with 2-byte offsets",
            &[0x11, 0, 0],
            &[0x07, 1, 0, 0, 2, 0, 0x0C, 1],
            &arr1,
            "C6",
        ),
        row(
            "array with is_large set for 1 element",
            &[0x11, 0, 0],
            &[0x13, 1, 0, 0, 0, 0, 2, 0x0C, 1],
            &arr1,
            "C6",
        ),
    ]
}

fn all_rows() -> Vec<Row> {
    let mut rows = built_rows();
    rows.extend(built_dictionary_rows());
    rows.extend(excluded_rows());
    rows.extend(framing_rows());
    rows
}

#[test]
fn upstream_valid_but_non_canonical_bytes_get_their_typed_fault() {
    let rows = all_rows();
    for row in &rows {
        // Upstream must accept the bytes: this is a valid Variant, not garbage.
        let variant = Variant::try_new(&row.metadata, &row.value)
            .unwrap_or_else(|e| panic!("{}: upstream rejects the bytes: {e}", row.name));
        if !matches!(row.fault, EncodingFault::ExcludedPrimitive(_)) {
            let got =
                support::variant_to_model(&variant).unwrap_or_else(|e| panic!("{}: {e}", row.name));
            assert_eq!(
                got, row.upstream,
                "{}: upstream reads a different value",
                row.name
            );
        }
        let err = JsonbRef::validate(&cell_of(&row.metadata, &row.value)).err();
        assert_eq!(
            err,
            Some(JsonbError::InvalidEncoding { reason: row.fault }),
            "{}: wrong ferrosa verdict",
            row.name
        );
    }
    eprintln!("non-canonical cases pinned: {}", rows.len());
}
