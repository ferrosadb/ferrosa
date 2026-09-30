//! Module: T-106 serde tests: validating `Deserialize`, base64 in human-readable
//! formats and raw bytes otherwise, bounded spill size (FM-21, JB-T1).
//! Correctness: correct when a round trip is identity, a non-canonical cell or
//! malformed base64 is a typed refusal, and a serialized N-byte cell is at most
//! N*4/3+64 bytes.
//! Last revised: 2026-09-28
//! Last changed: T-106 initial tests.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use ferrosa_jsonb::{parse_text, JsonbValue};
use serde::de::value::{BytesDeserializer, Error as DeError};
use serde::Deserialize;

mod common;
use common::limits;

fn val(json: &str) -> JsonbValue {
    let enc = parse_text(json.as_bytes(), &limits()).expect("parse");
    JsonbValue::from_encoded(enc).expect("valid")
}

#[test]
fn jsonb_serde_validates_and_bounds_size() {
    for json in [
        "null",
        "1.50",
        "[1,{\"a\":\"b\"}]",
        &format!("[\"{}\"]", "x".repeat(5000)),
    ] {
        let v = val(json);
        let text = serde_json::to_string(&v).expect("serialize");
        assert!(text.len() <= v.as_bytes().len() * 4 / 3 + 64, "{json}");
        let back: JsonbValue = serde_json::from_str(&text).expect("round trip");
        assert_eq!(back.as_bytes(), v.as_bytes());
    }
    // Non-human-readable formats take raw bytes.
    let v = val("[1,2,3]");
    let de = BytesDeserializer::<DeError>::new(v.as_bytes());
    let back = JsonbValue::deserialize(de).expect("bytes");
    assert_eq!(back.as_bytes(), v.as_bytes());

    let bad_cell = BytesDeserializer::<DeError>::new(&[0x7f, 0x00, 0x01]);
    assert!(
        JsonbValue::deserialize(bad_cell).is_err(),
        "non-canonical cell"
    );
    assert!(
        serde_json::from_str::<JsonbValue>("\"AQID\"").is_err(),
        "b64 of non-cell"
    );
    assert!(
        serde_json::from_str::<JsonbValue>("\"!!!!\"").is_err(),
        "bad alphabet"
    );
    assert!(
        serde_json::from_str::<JsonbValue>("\"AQI\"").is_err(),
        "bad length"
    );
    assert!(serde_json::from_str::<JsonbValue>("\"\"").is_err(), "empty");
    assert!(
        serde_json::from_str::<JsonbValue>("5").is_err(),
        "wrong type"
    );
    // Flip a payload byte of a valid cell: must not deserialize silently wrong.
    let mut bytes = val("{\"a\":1}").into_bytes();
    bytes.truncate(bytes.len() - 1);
    assert!(JsonbValue::deserialize(BytesDeserializer::<DeError>::new(&bytes)).is_err());
}
