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

use ferrosa_jsonb::{
    JsonbBuilder, Limits, LimitsConfig, Number, BIGDECIMAL_PRIMITIVE_ID, ENVELOPE,
};

const CORPUS_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/canonical_v1/corpus.txt"
);
/// Values at or above this many bytes are pinned by length and FNV-1a digest.
const FULL_HEX_MAX: usize = 300;

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

#[derive(Clone)]
enum Op {
    ObjBegin,
    ObjEnd,
    ArrBegin,
    ArrEnd,
    Key(String),
    Null,
    True,
    False,
    Str(String),
    Num(String),
}

type Case = (String, Vec<Op>);

fn n(s: &str) -> Op {
    Op::Num(s.to_string())
}
fn k(s: &str) -> Op {
    Op::Key(s.to_string())
}
fn s(x: &str) -> Op {
    Op::Str(x.to_string())
}

fn limits() -> Limits {
    let no_env = |_: &str| None;
    Limits::from_config_with_env(&LimitsConfig::default(), &no_env, 64 * 1024 * 1024)
        .expect("default limits load")
}

fn encode(ops: &[Op]) -> Vec<u8> {
    let mut b = JsonbBuilder::new(limits());
    for op in ops {
        match op {
            Op::ObjBegin => b.begin_object(),
            Op::ObjEnd => b.end_object(),
            Op::ArrBegin => b.begin_array(),
            Op::ArrEnd => b.end_array(),
            Op::Key(x) => b.key(x),
            Op::Null => b.null(),
            Op::True => b.boolean(true),
            Op::False => b.boolean(false),
            Op::Str(x) => b.string(x),
            Op::Num(x) => b.number(Number::parse_lexeme(x).expect("lexeme")),
        }
        .expect("builder accepts op");
    }
    b.finish().expect("finish").bytes
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn pin(bytes: &[u8]) -> String {
    if bytes.len() < FULL_HEX_MAX {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    } else {
        format!("len={} fnv1a64={:016x}", bytes.len(), fnv1a64(bytes))
    }
}

fn scalar_cases() -> Vec<Case> {
    vec![
        ("null".into(), vec![Op::Null]),
        ("true".into(), vec![Op::True]),
        ("false".into(), vec![Op::False]),
    ]
}

fn number_cases() -> Vec<Case> {
    let huge_dec = format!("0.{}", "1".repeat(16_383));
    let mut lexemes: Vec<String> = [
        "0",
        "1",
        "-1",
        "127",
        "128",
        "-128",
        "-129",
        "32767",
        "32768",
        "-32768",
        "-32769",
        "2147483647",
        "2147483648",
        "-2147483648",
        "-2147483649",
        "9223372036854775807",
        "-9223372036854775808",
        "9223372036854775808",
        "18446744073709551615",
        "99999999999999999999999999999999999999",
        "-99999999999999999999999999999999999999",
        "100000000000000000000000000000000000000",
        "-100000000000000000000000000000000000000",
        "-0",
        "1.0",
        "1.10",
        "1.00",
        "0.0",
        "0.00",
        "-0.0",
        "-0.5",
        "0.5",
        "0.01",
        "1e-2",
        "1.5e2",
        "1e2",
        "0.000000001",
        "0.0000000001",
        "999999999.9",
        "99999999.9",
        "0.999999999999999999",
        "1.0000000000000000000",
        "0.99999999999999999999999999999999999999",
        "-0.99999999999999999999999999999999999999",
        "1e-38",
        "1e-39",
        "1e-40",
        "1e400",
        "12345678901234567890.123456789",
        "3.14159265358979323846264338327950288419716939937510",
        "-3.14159265358979323846264338327950288419716939937510",
        "170141183460469231731687303715884105727",
        "-170141183460469231731687303715884105728",
        "340282366920938463463374607431768211455",
    ]
    .iter()
    .map(|x| (*x).to_string())
    .collect();
    lexemes.push(huge_dec);
    lexemes
        .into_iter()
        .enumerate()
        .map(|(i, l)| {
            let name = if l.len() > 60 {
                format!("num_{i:02}_long")
            } else {
                format!("num_{l}")
            };
            (name, vec![Op::Num(l)])
        })
        .collect()
}

fn string_cases() -> Vec<Case> {
    let mut out: Vec<Case> = Vec::new();
    for len in [0usize, 1, 2, 62, 63, 64, 65, 255, 256, 65_535, 65_536] {
        out.push((format!("str_len_{len}"), vec![s(&"x".repeat(len))]));
    }
    for (name, text) in [
        ("e_acute", "\u{e9}"),
        ("cjk", "\u{65e5}\u{672c}\u{8a9e}"),
        ("emoji", "\u{1f600}"),
        ("nul_char", "a\u{0}b"),
        ("quote_backslash", "a\"b\\c"),
        ("decomposed_not_normalized", "e\u{301}"),
    ] {
        out.push((format!("str_{name}"), vec![s(text)]));
    }
    // 21 chars * 3 bytes = 63 bytes (short) and 22 chars = 66 bytes (long).
    out.push((
        "str_63_bytes_multibyte".into(),
        vec![s(&"\u{65e5}".repeat(21))],
    ));
    out.push((
        "str_66_bytes_multibyte".into(),
        vec![s(&"\u{65e5}".repeat(22))],
    ));
    out
}

fn array_cases() -> Vec<Case> {
    let mut out: Vec<Case> = Vec::new();
    for count in [0usize, 1, 2, 3, 254, 255, 256, 257, 1000] {
        let mut ops = vec![Op::ArrBegin];
        ops.extend((0..count).map(|i| Op::Num((i % 100).to_string())));
        ops.push(Op::ArrEnd);
        out.push((format!("arr_ints_{count}"), ops));
    }
    for (count, len) in [(3usize, 100usize), (2, 40_000)] {
        let mut ops = vec![Op::ArrBegin];
        ops.extend((0..count).map(|_| s(&"y".repeat(len))));
        ops.push(Op::ArrEnd);
        out.push((format!("arr_{count}_strings_of_{len}"), ops));
    }
    out.push((
        "arr_nested_empty".into(),
        vec![Op::ArrBegin, Op::ArrBegin, Op::ArrEnd, Op::ArrEnd],
    ));
    out.push((
        "arr_mixed".into(),
        vec![
            Op::ArrBegin,
            Op::Null,
            Op::True,
            Op::False,
            n("1"),
            n("1.50"),
            s("hi"),
            Op::ArrBegin,
            n("300"),
            Op::ArrEnd,
            Op::ObjBegin,
            Op::ObjEnd,
            Op::ArrEnd,
        ],
    ));
    let mut deep = vec![Op::ArrBegin; 50];
    deep.push(Op::Null);
    deep.extend(vec![Op::ArrEnd; 50]);
    out.push(("arr_depth_50".into(), deep));
    out
}

fn small_object_cases() -> Vec<Case> {
    let mut out: Vec<Case> = Vec::new();
    out.push(("obj_empty".into(), vec![Op::ObjBegin, Op::ObjEnd]));
    out.push((
        "obj_one".into(),
        vec![Op::ObjBegin, k("a"), n("1"), Op::ObjEnd],
    ));
    out.push((
        "obj_keys_unsorted".into(),
        vec![
            Op::ObjBegin,
            k("b"),
            n("2"),
            k("a"),
            n("1"),
            k("c"),
            n("3"),
            Op::ObjEnd,
        ],
    ));
    out.push((
        "obj_key_bytewise_order".into(),
        vec![
            Op::ObjBegin,
            k("\u{65e5}"),
            n("3"),
            k("z"),
            n("2"),
            k("\u{e9}"),
            n("1"),
            k("Z"),
            n("0"),
            Op::ObjEnd,
        ],
    ));
    out.push((
        "obj_empty_key".into(),
        vec![Op::ObjBegin, k(""), n("1"), Op::ObjEnd],
    ));
    out.push((
        "obj_dup_last_wins".into(),
        vec![Op::ObjBegin, k("a"), n("1"), k("a"), n("2"), Op::ObjEnd],
    ));
    out.push((
        "obj_dup_middle".into(),
        vec![
            Op::ObjBegin,
            k("a"),
            n("1"),
            k("b"),
            n("2"),
            k("a"),
            s("three"),
            Op::ObjEnd,
        ],
    ));
    out.push((
        "obj_dup_drops_nested_keys".into(),
        vec![
            Op::ObjBegin,
            k("a"),
            Op::ObjBegin,
            k("only_here"),
            n("1"),
            Op::ObjEnd,
            k("a"),
            n("2"),
            Op::ObjEnd,
        ],
    ));
    out.push((
        "obj_shared_dictionary".into(),
        vec![
            Op::ArrBegin,
            Op::ObjBegin,
            k("a"),
            n("1"),
            k("b"),
            n("2"),
            Op::ObjEnd,
            Op::ObjBegin,
            k("b"),
            n("3"),
            k("a"),
            n("4"),
            Op::ObjEnd,
            Op::ArrEnd,
        ],
    ));
    out
}

fn wide_object_cases() -> Vec<Case> {
    let mut out: Vec<Case> = Vec::new();
    for count in [1usize, 2, 255, 256, 257, 300] {
        let mut ops = vec![Op::ObjBegin];
        for i in (0..count).rev() {
            ops.push(Op::Key(format!("k{i:04}")));
            ops.push(Op::Num((i % 100).to_string()));
        }
        ops.push(Op::ObjEnd);
        out.push((format!("obj_keys_{count}"), ops));
    }
    let mut wide = vec![Op::ObjBegin];
    for i in 0..10 {
        wide.push(Op::Key(format!("{i}{}", "w".repeat(29))));
        wide.push(Op::Null);
    }
    wide.push(Op::ObjEnd);
    out.push(("obj_dictionary_over_255_bytes".into(), wide));
    out.push((
        "obj_one_key_70000_bytes".into(),
        vec![
            Op::ObjBegin,
            Op::Key("q".repeat(70_000)),
            n("1"),
            Op::ObjEnd,
        ],
    ));
    let mut deep = Vec::new();
    for _ in 0..50 {
        deep.extend([Op::ObjBegin, k("a")]);
    }
    deep.push(Op::True);
    deep.extend(vec![Op::ObjEnd; 50]);
    out.push(("obj_depth_50".into(), deep));
    out.push((
        "doc_realistic".into(),
        vec![
            Op::ObjBegin,
            k("name"),
            s("Ada"),
            k("age"),
            n("36"),
            k("balance"),
            n("1024.50"),
            k("tags"),
            Op::ArrBegin,
            s("x"),
            s("y"),
            Op::ArrEnd,
            k("address"),
            Op::ObjBegin,
            k("city"),
            s("London"),
            k("geo"),
            Op::ArrBegin,
            n("51.5074"),
            n("-0.1278"),
            Op::ArrEnd,
            Op::ObjEnd,
            k("active"),
            Op::True,
            k("spouse"),
            Op::Null,
            Op::ObjEnd,
        ],
    ));
    out
}

/// Sweeps that cross every numeric kind boundary: powers of ten as integers,
/// their negatives, and `0.0...1` at scales 1..=45.
fn sweep_cases() -> Vec<Case> {
    let mut out: Vec<Case> = Vec::new();
    for e in 0..=40usize {
        let pos = format!("1{}", "0".repeat(e));
        out.push((format!("sweep_pow10_{e}"), vec![Op::Num(pos.clone())]));
        out.push((
            format!("sweep_neg_pow10_{e}"),
            vec![Op::Num(format!("-{pos}"))],
        ));
    }
    for scale in 1..=45usize {
        let lexeme = format!("0.{}1", "0".repeat(scale - 1));
        out.push((format!("sweep_scale_{scale}"), vec![Op::Num(lexeme)]));
    }
    out
}

fn corpus() -> Vec<Case> {
    let mut all = scalar_cases();
    all.extend(sweep_cases());
    all.extend(number_cases());
    all.extend(string_cases());
    all.extend(array_cases());
    all.extend(small_object_cases());
    all.extend(wide_object_cases());
    all
}

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
