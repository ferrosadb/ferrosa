//! Module: hand-derived canonical bytes for the Variant v1 encoder (T-102).
//! Correctness: correct when each expected byte string, worked out by hand from
//! the Parquet Variant spec and architecture rules C1-C11 (not produced by the
//! encoder), matches exactly. These anchor the golden corpus to the spec.
//! Last revised: 2026-09-28
//! Last changed: T-102 initial.

// Integration tests are separate crates, so the crate's test-code allowance in
// clippy.toml does not reach them; test code may unwrap, expect and index.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]

use ferrosa_jsonb::{JsonbBuilder, Limits, LimitsConfig, Number};

fn limits() -> Limits {
    let no_env = |_: &str| None;
    Limits::from_config_with_env(&LimitsConfig::default(), &no_env, 64 * 1024 * 1024)
        .expect("default limits load")
}

fn num(lexeme: &str) -> Vec<u8> {
    let mut b = JsonbBuilder::new(limits());
    b.number(Number::parse_lexeme(lexeme).expect("lexeme"))
        .expect("number");
    b.finish().expect("finish").bytes
}

const EMPTY_META: [u8; 3] = [0x11, 0x00, 0x00];

fn cell(meta: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = vec![0xF1];
    out.extend_from_slice(meta);
    out.extend_from_slice(value);
    out
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

#[test]
fn jsonb_encode_scalars_match_hand_bytes() {
    let mut b = JsonbBuilder::new(limits());
    b.null().expect("null");
    assert_eq!(
        b.finish().expect("finish").bytes,
        cell(&EMPTY_META, &[0x00])
    );
    let mut b = JsonbBuilder::new(limits());
    b.boolean(true).expect("true");
    assert_eq!(
        b.finish().expect("finish").bytes,
        cell(&EMPTY_META, &[0x04])
    );
    let mut b = JsonbBuilder::new(limits());
    b.boolean(false).expect("false");
    assert_eq!(
        b.finish().expect("finish").bytes,
        cell(&EMPTY_META, &[0x08])
    );
    let mut b = JsonbBuilder::new(limits());
    b.string("hi").expect("string");
    assert_eq!(
        b.finish().expect("finish").bytes,
        cell(&EMPTY_META, &[0x09, 0x68, 0x69])
    );
}

#[test]
fn jsonb_encode_long_string_uses_primitive_16() {
    let text = "x".repeat(64);
    let mut b = JsonbBuilder::new(limits());
    b.string(&text).expect("string");
    let mut value = vec![0x40, 0x40, 0x00, 0x00, 0x00];
    value.extend_from_slice(text.as_bytes());
    assert_eq!(b.finish().expect("finish").bytes, cell(&EMPTY_META, &value));
}

#[test]
fn jsonb_encode_integers_use_smallest_kind() {
    assert_eq!(num("1"), cell(&EMPTY_META, &[0x0C, 0x01]));
    assert_eq!(num("-1"), cell(&EMPTY_META, &[0x0C, 0xFF]));
    assert_eq!(num("128"), cell(&EMPTY_META, &[0x10, 0x80, 0x00]));
    assert_eq!(
        num("32768"),
        cell(&EMPTY_META, &[0x14, 0x00, 0x80, 0x00, 0x00])
    );
    assert_eq!(
        num("2147483648"),
        cell(
            &EMPTY_META,
            &[0x18, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00]
        )
    );
}

#[test]
fn jsonb_encode_decimals_keep_scale() {
    // decimal4: header 0x20, scale byte, i32 LE unscaled.
    assert_eq!(
        num("1.0"),
        cell(&EMPTY_META, &[0x20, 0x01, 0x0A, 0x00, 0x00, 0x00])
    );
    assert_eq!(
        num("1.10"),
        cell(&EMPTY_META, &[0x20, 0x02, 0x6E, 0x00, 0x00, 0x00])
    );
    assert_eq!(
        num("0.00"),
        cell(&EMPTY_META, &[0x20, 0x02, 0x00, 0x00, 0x00, 0x00])
    );
    // decimal8 when the unscaled value needs 10 digits.
    let mut d8 = vec![0x24, 0x01];
    d8.extend_from_slice(&9_999_999_999i64.to_le_bytes());
    assert_eq!(num("999999999.9"), cell(&EMPTY_META, &d8));
    // decimal16 for a 38-digit integer (scale 0).
    let mut d16 = vec![0x28, 0x00];
    d16.extend_from_slice(&99_999_999_999_999_999_999_999_999_999_999_999_999i128.to_le_bytes());
    assert_eq!(
        num("99999999999999999999999999999999999999"),
        cell(&EMPTY_META, &d16)
    );
}

#[test]
fn jsonb_encode_bigdecimal_layout_and_minimal_twos_complement() {
    // 10^38 does not fit decimal16: 0xFC, scale 0, length 16, big-endian.
    let mut want = vec![0xFC, 0x00, 0x10];
    want.extend(hex("4b3b4ca85a86c47a098a224000000000"));
    assert_eq!(
        num("100000000000000000000000000000000000000"),
        cell(&EMPTY_META, &want)
    );
    let mut neg = vec![0xFC, 0x00, 0x10];
    neg.extend(hex("b4c4b357a5793b85f675ddc000000000"));
    assert_eq!(
        num("-100000000000000000000000000000000000000"),
        cell(&EMPTY_META, &neg)
    );
    // 2^127 needs a leading zero byte to stay positive; -(2^127)-1 needs 0xff.
    let mut pos = vec![0xFC, 0x00, 0x11];
    pos.extend(hex("0080000000000000000000000000000000"));
    assert_eq!(
        num("170141183460469231731687303715884105728"),
        cell(&EMPTY_META, &pos)
    );
    let mut low = vec![0xFC, 0x00, 0x11];
    low.extend(hex("ff7fffffffffffffffffffffffffffffff"));
    assert_eq!(
        num("-170141183460469231731687303715884105729"),
        cell(&EMPTY_META, &low)
    );
    // Scale is a zigzag varint: 40 -> 0x50, 100 -> 200 -> c8 01.
    assert_eq!(num("1e-40"), cell(&EMPTY_META, &[0xFC, 0x50, 0x01, 0x01]));
    assert_eq!(
        num("1e-100"),
        cell(&EMPTY_META, &[0xFC, 0xC8, 0x01, 0x01, 0x01])
    );
}

#[test]
fn jsonb_encode_containers_match_hand_bytes() {
    let mut b = JsonbBuilder::new(limits());
    b.begin_array().expect("begin");
    b.end_array().expect("end");
    assert_eq!(
        b.finish().expect("finish").bytes,
        cell(&EMPTY_META, &[0x03, 0x00, 0x00])
    );

    let mut b = JsonbBuilder::new(limits());
    b.begin_object().expect("begin");
    b.end_object().expect("end");
    assert_eq!(
        b.finish().expect("finish").bytes,
        cell(&EMPTY_META, &[0x02, 0x00, 0x00])
    );

    let mut b = JsonbBuilder::new(limits());
    b.begin_array().expect("begin");
    for v in [1i64, 2] {
        b.number(Number::from_i64(v)).expect("num");
    }
    b.end_array().expect("end");
    let want = cell(
        &EMPTY_META,
        &[0x03, 0x02, 0x00, 0x02, 0x04, 0x0C, 0x01, 0x0C, 0x02],
    );
    assert_eq!(b.finish().expect("finish").bytes, want);
}

#[test]
fn jsonb_encode_object_sorts_keys_into_the_dictionary() {
    let mut b = JsonbBuilder::new(limits());
    b.begin_object().expect("begin");
    b.key("b").expect("key");
    b.number(Number::from_i64(1)).expect("num");
    b.key("a").expect("key");
    b.number(Number::from_i64(2)).expect("num");
    b.end_object().expect("end");
    let meta = [0x11, 0x02, 0x00, 0x01, 0x02, 0x61, 0x62];
    // ids in key order (a=0, b=1), values follow the same order: a=2, b=1.
    let value = [
        0x02, 0x02, 0x00, 0x01, 0x00, 0x02, 0x04, 0x0C, 0x02, 0x0C, 0x01,
    ];
    assert_eq!(b.finish().expect("finish").bytes, cell(&meta, &value));
}

#[test]
fn jsonb_encode_duplicate_key_last_wins_and_is_counted() {
    let mut b = JsonbBuilder::new(limits());
    b.begin_object().expect("begin");
    for v in [1i64, 2] {
        b.key("a").expect("key");
        b.number(Number::from_i64(v)).expect("num");
    }
    b.end_object().expect("end");
    let done = b.finish().expect("finish");
    let meta = [0x11, 0x01, 0x00, 0x01, 0x61];
    let value = [0x02, 0x01, 0x00, 0x00, 0x02, 0x0C, 0x02];
    assert_eq!(done.bytes, cell(&meta, &value));
    assert_eq!(done.duplicate_keys_dropped, 1);
}
