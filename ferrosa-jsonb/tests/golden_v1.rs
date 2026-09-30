//! Module: golden corpus and primitive-table conformance for the canonical
//! Variant v1 encoder (T-102, FM-06, FM-08).
//! Correctness: correct when 200+ values encode to exactly the checked-in bytes
//! (`tests/golden/canonical_v1/corpus.txt`) and the ferrosa extension id 63 is
//! absent from the published Variant primitive table.
//! Last revised: 2026-09-28
//! Last changed: T-102 initial corpus.
//!
//! Regenerate the corpus only for a deliberate format change, which also needs a
//! new envelope byte: `FERROSA_JSONB_BLESS=1 cargo test -p ferrosa-jsonb --test golden_v1`.

// Integration tests are separate crates, so the crate's test-code allowance in
// clippy.toml does not reach them; test code may unwrap, expect and index.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::vec_init_then_push
)]

use ferrosa_jsonb::{BIGDECIMAL_PRIMITIVE_ID, ENVELOPE};

mod common;
use common::{corpus, encode, pin, Case};

const CORPUS_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/canonical_v1/corpus.txt"
);
/// The primitive table of Parquet Variant v1, from
/// <https://github.com/apache/parquet-format/blob/master/VariantEncoding.md>
/// (master, read 2026-09-28). Ids 21..=63 are unassigned upstream.
const VARIANT_V1_PRIMITIVE_TABLE: [(u8, &str); 21] = [
    (0, "null"),
    (1, "boolean true"),
    (2, "boolean false"),
    (3, "int8"),
    (4, "int16"),
    (5, "int32"),
    (6, "int64"),
    (7, "double"),
    (8, "decimal4"),
    (9, "decimal8"),
    (10, "decimal16"),
    (11, "date"),
    (12, "timestamp"),
    (13, "timestamp_ntz"),
    (14, "float"),
    (15, "binary"),
    (16, "string"),
    (17, "time_ntz"),
    (18, "timestamp_tz"),
    (19, "timestamp_ntz_nanos"),
    (20, "uuid"),
];

fn render(cases: &[Case]) -> String {
    let mut text = String::new();
    for (name, ops) in cases {
        text.push_str(&format!("{name}\t{}\n", pin(&encode(ops))));
    }
    text
}

#[test]
fn jsonb_canonical_bytes_golden_v1() {
    let cases = corpus();
    assert!(
        cases.len() >= 200,
        "corpus has {} cases, need at least 200",
        cases.len()
    );
    let actual = render(&cases);
    if std::env::var_os("FERROSA_JSONB_BLESS").is_some() {
        std::fs::write(CORPUS_PATH, &actual).expect("write corpus");
    }
    let pinned = std::fs::read_to_string(CORPUS_PATH).expect("golden corpus is checked in");
    let (mut want, mut got) = (pinned.lines(), actual.lines());
    for (name, _) in &cases {
        assert_eq!(
            got.next(),
            want.next(),
            "canonical bytes changed for case {name}"
        );
    }
    assert_eq!(want.next(), None, "corpus file has extra cases");
    assert!(actual
        .lines()
        .all(|l| l.split('\t').nth(1).is_some_and(|h| !h.is_empty())));
    assert_eq!(ENVELOPE, 0xF1);
}

#[test]
fn variant_primitive_table_does_not_assign_63() {
    assert_eq!(BIGDECIMAL_PRIMITIVE_ID, 63);
    for (id, name) in VARIANT_V1_PRIMITIVE_TABLE {
        assert_ne!(
            id, BIGDECIMAL_PRIMITIVE_ID,
            "upstream assigned 63 to {name}"
        );
        assert!(id < 63, "{name} has an out-of-range id {id}");
    }
    let max = VARIANT_V1_PRIMITIVE_TABLE.iter().map(|(id, _)| *id).max();
    assert_eq!(
        max,
        Some(20),
        "the pinned table is Variant v1 through uuid (20)"
    );
}
