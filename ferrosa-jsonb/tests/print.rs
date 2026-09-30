//! Module: T-105 printer tests: golden text for the three styles over the T-102
//! corpus, a hand-written PostgreSQL 16 table, round trip, depth 1000 on a
//! 256 KiB stack, and the output budget (FM-12, FM-106, JB-D4, JB-T6).
//! Correctness: correct when every corpus value prints to exactly the checked-in
//! text (`tests/golden/text_v1/*.txt`), the PgText table matches PostgreSQL 16,
//! and an over-budget print is a typed error, never a partial string.
//! Last revised: 2026-09-28
//! Last changed: T-105 initial printer tests.
//!
//! Regenerate goldens only for a deliberate text-format change (they feed the
//! D13a RDF hash): `FERROSA_JSONB_BLESS=1 cargo test -p ferrosa-jsonb --test print`.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::vec_init_then_push
)]

use ferrosa_jsonb::{parse_text, print_to_string, JsonbRef, PrintError, TextStyle};

mod common;
use common::{corpus, encode, fnv1a64, limits};

const GOLDEN_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/text_v1");
const BUDGET: usize = 64 * 1024 * 1024;

fn cell_of(json: &str) -> Vec<u8> {
    parse_text(json.as_bytes(), &limits())
        .unwrap_or_else(|e| panic!("{json:?}: {e}"))
        .bytes
}

fn print(bytes: &[u8], style: TextStyle) -> String {
    let cell = JsonbRef::validate(bytes).expect("valid cell");
    print_to_string(cell.root(), style, BUDGET).expect("print")
}

fn print_json(json: &str, style: TextStyle) -> String {
    print(&cell_of(json), style)
}

/// Text shorter than this is pinned verbatim; longer text by length and FNV-1a.
const VERBATIM_MAX: usize = 300;

fn pin_text(text: &str) -> String {
    if text.len() < VERBATIM_MAX {
        text.to_string()
    } else {
        format!(
            "len={} fnv1a64={:016x}",
            text.len(),
            fnv1a64(text.as_bytes())
        )
    }
}

fn check_golden(file: &str, style: TextStyle) {
    let cases = corpus();
    assert!(cases.len() >= 200, "corpus has {} cases", cases.len());
    let mut actual = String::new();
    for (name, ops) in &cases {
        let text = print(&encode(ops), style);
        assert!(!text.contains('\n'), "{name}: text has a raw newline");
        actual.push_str(&format!("{name}\t{}\n", pin_text(&text)));
    }
    let path = format!("{GOLDEN_DIR}/{file}");
    if std::env::var_os("FERROSA_JSONB_BLESS").is_some() {
        std::fs::create_dir_all(GOLDEN_DIR).expect("mkdir");
        std::fs::write(&path, &actual).expect("write golden");
    }
    let pinned = std::fs::read_to_string(&path).expect("golden is checked in");
    let (mut want, mut got) = (pinned.lines(), actual.lines());
    for (name, _) in &cases {
        assert_eq!(got.next(), want.next(), "{file}: text changed for {name}");
    }
    assert_eq!(want.next(), None, "{file} has extra cases");
}

#[test]
fn jsonb_canonical_text_golden_v1() {
    check_golden("canonical.txt", TextStyle::Canonical);
}

#[test]
fn jsonb_normalized_text_golden_v1() {
    check_golden("normalized.txt", TextStyle::Normalized);
}

#[test]
fn jsonb_pgtext_golden_v1() {
    check_golden("pgtext.txt", TextStyle::PgText);
}

#[test]
fn jsonb_print_keeps_scale() {
    for (input, want) in [("1.0", "1.0"), ("1.10", "1.10"), ("0.00", "0.00")] {
        assert_eq!(print_json(input, TextStyle::Canonical), want);
        assert_eq!(print_json(input, TextStyle::PgText), want);
    }
    assert_eq!(
        print_json("[1,1.0,1.00]", TextStyle::Canonical),
        "[1,1.0,1.00]"
    );
}

#[test]
fn normalized_text_strips_trailing_zeros_so_equal_numbers_print_alike() {
    // D2a / D13a: 1, 1.0 and 1.00 are equal by value and must hash alike.
    let text = print_json(
        "[1,1.0,1.00,0.0,-0.0,1.10,100,1e2,1.50e1]",
        TextStyle::Normalized,
    );
    assert_eq!(text, "[1,1,1,0,0,1.1,100,100,15]");
    let a = print_json(r#"{"k":1.0}"#, TextStyle::Normalized);
    assert_eq!(a, print_json(r#"{"k":1.00}"#, TextStyle::Normalized));
}

#[test]
fn canonical_orders_keys_bytewise_and_is_compact() {
    // D6 / D26 item 1: keys bytewise as stored, no insignificant whitespace.
    assert_eq!(
        print_json(r#"{ "b" : 1 , "aa" : [ 1 , 2 ] }"#, TextStyle::Canonical),
        r#"{"aa":[1,2],"b":1}"#
    );
}

/// (input, text PostgreSQL 16 prints for `input::jsonb`).
///
/// Sources: PostgreSQL 16 `src/backend/utils/adt/jsonb.c` `JsonbToCStringWorker`
/// (`": "` after a key, `", "` between members, `[`/`]`/`{`/`}`),
/// `src/backend/utils/adt/jsonb_util.c` `lengthCompareJsonbStringValue` (object
/// keys sorted by byte length, then `memcmp`), `json.c` `escape_json`, and
/// `numeric.c` `numeric_out` (plain decimal, display scale kept). PG docs, sec.
/// 8.14.1 "JSON Input and Output Syntax": "jsonb ... does not preserve white
/// space, does not preserve the order of object keys".
const PG_TABLE: &[(&str, &str)] = &[
    // jsonb_util.c: shorter key first, then bytewise.
    (r#"{"b":1,"aa":2}"#, r#"{"b": 1, "aa": 2}"#),
    (r#"{"aa":2,"b":1}"#, r#"{"b": 1, "aa": 2}"#),
    // Equal length falls back to memcmp.
    (r#"{"b":1,"a":2}"#, r#"{"a": 2, "b": 1}"#),
    // Length is in BYTES: both keys are 2 bytes, so bytewise decides (0x7a < 0xc3).
    (r#"{"é":1,"zz":2}"#, r#"{"zz": 2, "é": 1}"#),
    // A 2-byte key sorts after a 1-byte key.
    (r#"{"é":1,"b":2}"#, r#"{"b": 2, "é": 1}"#),
    // Nesting: each object is ordered independently.
    (
        r#"{"bb":{"y":1,"x":[1,{"dd":0,"c":0}]},"a":null}"#,
        r#"{"a": null, "bb": {"x": [1, {"c": 0, "dd": 0}], "y": 1}}"#,
    ),
    // jsonb.c: `[1, 2]` uses ", "; scalars are true/false/null.
    ("[1,2]", "[1, 2]"),
    ("[true,false,null]", "[true, false, null]"),
    // Empty containers have no interior whitespace.
    ("{}", "{}"),
    ("[]", "[]"),
    ("[[],{}]", "[[], {}]"),
    (r#"{"a":{},"b":[]}"#, r#"{"a": {}, "b": []}"#),
    (r#"{"":1}"#, r#"{"": 1}"#),
    // json.c escape_json: `"` `\` and the five short escapes.
    (r#""a\"b\\c""#, r#""a\"b\\c""#),
    (r#""\b\f\n\r\t""#, r#""\b\f\n\r\t""#),
    // escape_json: other controls below 0x20 use \u%04x (lowercase hex).
    (r#""\u0001\u001f\u000b""#, r#""\u0001\u001f\u000b""#),
    // escape_json leaves `/`, DEL and non-ASCII unescaped (raw UTF-8).
    (r#""\/ \u007f é 日本 😀""#, "\"/ \u{7f} é 日本 😀\""),
    // Object keys use the same escaping as strings.
    (r#"{"a\nb":1}"#, r#"{"a\nb": 1}"#),
    // numeric_out: scale is kept.
    ("1.10", "1.10"),
    ("1.0", "1.0"),
    ("0.00", "0.00"),
    // numeric has no negative zero, so `-0` prints `0` (scale kept: `-0.0` is 0.0).
    ("-0", "0"),
    ("-0.0", "0.0"),
    // numeric_in accepts exponents; output is plain decimal, never exponent form.
    ("1e2", "100"),
    ("1E+2", "100"),
    ("1.5e-2", "0.015"),
    ("-1.50e1", "-15.0"),
    (
        "12345678901234567890.123456789",
        "12345678901234567890.123456789",
    ),
    // Integers have no decimal point.
    (
        "[0,-1,9223372036854775807,18446744073709551615]",
        "[0, -1, 9223372036854775807, 18446744073709551615]",
    ),
];

#[test]
fn pgtext_matches_postgres_16_table() {
    for (input, want) in PG_TABLE {
        assert_eq!(&print_json(input, TextStyle::PgText), want, "input {input}");
    }
}

#[test]
fn pgtext_prints_a_100_digit_number_in_full() {
    // numeric_out: 100 significant digits are printed as written, no exponent.
    let digits = format!("1{}", "2345678901".repeat(10)); // 101 digits
    assert_eq!(digits.len(), 101);
    assert_eq!(print_json(&digits, TextStyle::PgText), digits);
    let hundred = "7".repeat(100);
    assert_eq!(
        print_json(&format!("[{hundred}]"), TextStyle::PgText),
        format!("[{hundred}]")
    );
}

#[test]
fn pgtext_prints_big_decimals_in_plain_form() {
    // numeric_out: a value beyond decimal16 (bigdecimal) prints plain, scale kept.
    let frac = "1".repeat(50);
    let lexeme = format!("-3.{frac}0");
    assert_eq!(print_json(&lexeme, TextStyle::PgText), lexeme);
    // numeric_in of 1e400 is 1 followed by 400 zeros.
    assert_eq!(
        print_json("1e400", TextStyle::PgText),
        format!("1{}", "0".repeat(400))
    );
    let tiny = format!("0.{}1", "0".repeat(60));
    assert_eq!(print_json(&tiny, TextStyle::PgText), tiny);
}

#[test]
fn round_trip_canonical_and_pgtext_reencode_to_same_bytes() {
    for (name, ops) in corpus() {
        let bytes = encode(&ops);
        for style in [TextStyle::Canonical, TextStyle::PgText] {
            let text = print(&bytes, style);
            let again = cell_of(&text);
            assert_eq!(again, bytes, "{name} did not round trip under {style:?}");
        }
    }
}

fn on_small_stack(work: fn()) {
    std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(work)
        .expect("spawn")
        .join()
        .expect("thread finished without overflowing its stack");
}

fn deep_arrays() {
    let json = format!("{}1{}", "[".repeat(1000), "]".repeat(1000));
    let bytes = cell_of(&json);
    for style in [TextStyle::Canonical, TextStyle::Normalized] {
        assert_eq!(print(&bytes, style), json);
    }
    let spaced = format!("{}1{}", "[".repeat(1000), "]".repeat(1000));
    assert_eq!(print(&bytes, TextStyle::PgText), spaced);
}

fn deep_objects() {
    let json = format!("{}1{}", r#"{"a":"#.repeat(1000), "}".repeat(1000));
    let bytes = cell_of(&json);
    assert_eq!(print(&bytes, TextStyle::Canonical), json);
    let pg = format!("{}1{}", r#"{"a": "#.repeat(1000), "}".repeat(1000));
    assert_eq!(print(&bytes, TextStyle::PgText), pg);
}

#[test]
fn depth_1000_prints_on_a_256_kib_stack() {
    on_small_stack(deep_arrays);
    on_small_stack(deep_objects);
}

#[test]
fn jsonb_print_output_budget_enforced() {
    // JB-D4: an adversarial many-number document.
    let json = format!("[{}0]", "123456789.5,".repeat(10_000));
    let bytes = cell_of(&json);
    let cell = JsonbRef::validate(&bytes).expect("valid");
    let full = print_to_string(cell.root(), TextStyle::Canonical, BUDGET).expect("full");
    assert_eq!(full.len(), json.len());
    let err = print_to_string(cell.root(), TextStyle::Canonical, 1000).expect_err("over budget");
    assert_eq!(err, PrintError::BudgetExceeded { max: 1000 });
}

#[test]
fn budget_boundary_is_exact() {
    let bytes = cell_of(r#"{"a":[1,2.50,"x"]}"#);
    let cell = JsonbRef::validate(&bytes).expect("valid");
    let want = r#"{"a":[1,2.50,"x"]}"#;
    let ok = print_to_string(cell.root(), TextStyle::Canonical, want.len()).expect("fits");
    assert_eq!(ok, want);
    let err = print_to_string(cell.root(), TextStyle::Canonical, want.len() - 1);
    assert_eq!(
        err,
        Err(PrintError::BudgetExceeded {
            max: want.len() - 1
        })
    );
}

#[test]
fn streaming_sink_never_receives_more_than_the_budget() {
    let bytes = cell_of(&format!("[{}0]", "\"abcdefghij\",".repeat(500)));
    let cell = JsonbRef::validate(&bytes).expect("valid");
    let mut sink = String::new();
    let err = ferrosa_jsonb::print_value(cell.root(), TextStyle::PgText, 777, &mut sink);
    assert_eq!(err, Err(PrintError::BudgetExceeded { max: 777 }));
    assert!(sink.len() <= 777, "sink got {} bytes", sink.len());
}

#[test]
fn print_value_reports_bytes_written() {
    let bytes = cell_of(r#"[1,{"a":null}]"#);
    let cell = JsonbRef::validate(&bytes).expect("valid");
    let mut sink = String::new();
    let n = ferrosa_jsonb::print_value(cell.root(), TextStyle::Canonical, BUDGET, &mut sink)
        .expect("print");
    assert_eq!(n, sink.len());
    assert_eq!(sink, r#"[1,{"a":null}]"#);
}

struct RefusingSink;

impl std::fmt::Write for RefusingSink {
    fn write_str(&mut self, _: &str) -> std::fmt::Result {
        Err(std::fmt::Error)
    }
}

#[test]
fn a_failing_sink_is_a_typed_error() {
    let bytes = cell_of("[1]");
    let cell = JsonbRef::validate(&bytes).expect("valid");
    let err =
        ferrosa_jsonb::print_value(cell.root(), TextStyle::Canonical, BUDGET, &mut RefusingSink);
    assert_eq!(err, Err(PrintError::Sink));
}
