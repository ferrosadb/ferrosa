//! T-103 parser tests: conformance corpus (JSONTestSuite style `y_` / `n_`),
//! depth boundaries on a small stack, D6b duplicate keys, working-set
//! proportionality, and determinism properties.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use ferrosa_jsonb::{
    parse_text, parse_text_observed, DuplicateKeyObserver, DuplicateKeyPolicy, InflightBudget,
    JsonbError, Limits, LimitsConfig,
};
use proptest::prelude::*;
use std::sync::Mutex;

fn limits_with(depth: u64, policy: DuplicateKeyPolicy) -> Limits {
    let cfg = LimitsConfig {
        max_nesting_depth: Some(depth),
        duplicate_keys: Some(policy),
        ..LimitsConfig::default()
    };
    Limits::from_config_with_env(&cfg, &|_: &str| None, 64 * 1024 * 1024).expect("limits")
}

fn defaults() -> Limits {
    limits_with(1000, DuplicateKeyPolicy::LastWins)
}

fn offset_of(err: JsonbError) -> usize {
    match err {
        JsonbError::Syntax { offset, .. } | JsonbError::InvalidUtf8 { offset } => offset,
        other => panic!("expected a positioned parse error, got {other:?}"),
    }
}

const Y_CASES: &[&str] = &[
    "[]",
    "{}",
    "[1,2,3]",
    r#"{"a":[true,false,null],"b":{"c":"d"}}"#,
    " \t\r\n[ 1 , 2 ] \n",
    "0",
    "-0",
    "-1.5e10",
    "1E+2",
    "0.000001",
    "123456789012345678901234567890",
    r#""""#,
    r#""\u0041\n\t\"\\\/\b\f\r""#,
    r#""\uD83D\uDE00""#,
    "\"h\u{e9}llo \u{1F600}\"",
    r#"{"":0}"#,
    r#"[[[[[[[[[[]]]]]]]]]]"#,
    "true",
    "false",
    "null",
    r#"["a\u0000b"]"#,
];

const N_CASES: &[(&str, usize)] = &[
    ("", 0),
    ("[", 1),
    ("]", 0),
    ("[1,]", 3),
    ("[,1]", 1),
    ("{\"a\":1,}", 7),
    ("{a:1}", 1),
    ("{\"a\" 1}", 5),
    ("[1 2]", 3),
    ("[01]", 2),
    ("[1.]", 3),
    ("[.5]", 1),
    ("[+1]", 1),
    ("[1e]", 3),
    ("[-]", 2),
    ("[NaN]", 1),
    ("[Infinity]", 1),
    ("[-Infinity]", 2),
    ("[tru]", 1),
    ("[nul]", 1),
    ("'a'", 0),
    ("[\"a]", 1),
    ("[\"\\x\"]", 2),
    ("[\"\\u12\"]", 2),
    ("[\"a\tb\"]", 3),
    ("[\"a\nb\"]", 3),
    ("[1] x", 4),
    ("{} {}", 3),
    ("[1]]", 3),
    ("[\"\\uD800\"]", 2),
    ("[\"\\uDC00\"]", 2),
    ("[\"\\uD800\\u0041\"]", 2),
    ("\u{feff}[]", 0),
    ("[1,\u{feff}]", 3),
    ("/* c */ []", 0),
    ("{\"a\":1 \"b\":2}", 7),
    ("{1:2}", 1),
];

#[test]
fn jsonb_parse_conformance_y_cases_accepted() {
    for case in Y_CASES {
        parse_text(case.as_bytes(), &defaults())
            .unwrap_or_else(|e| panic!("y case {case:?} rejected: {e}"));
    }
}

#[test]
fn jsonb_parse_rejects_lone_surrogate_bom_nan_trailing() {
    for (case, offset) in N_CASES {
        let err = parse_text(case.as_bytes(), &defaults())
            .expect_err(&format!("n case {case:?} must be rejected"));
        assert_eq!(offset_of(err), *offset, "offset for {case:?}");
    }
}

#[test]
fn jsonb_parse_invalid_utf8_reports_offset() {
    let err = parse_text(b"[\"ab\xff\"]", &defaults()).unwrap_err();
    assert_eq!(err, JsonbError::InvalidUtf8 { offset: 4 });
    let err = parse_text(b"[\"\xc3\"]", &defaults()).unwrap_err();
    assert!(matches!(err, JsonbError::InvalidUtf8 { offset: 2 }));
    let err = parse_text(b"\xff", &defaults()).unwrap_err();
    assert_eq!(offset_of(err), 0);
}

#[test]
fn jsonb_parse_input_size_limit_is_enforced_first() {
    let cfg = LimitsConfig {
        max_input_bytes: Some(8),
        ..LimitsConfig::default()
    };
    let limits =
        Limits::from_config_with_env(&cfg, &|_: &str| None, 64 * 1024 * 1024).expect("limits");
    assert!(parse_text(b"[1,2,3]", &limits).is_ok());
    assert!(matches!(
        parse_text(b"[1,2,3,4,5]", &limits),
        Err(JsonbError::InputTooLarge { len: 11, max: 8 })
    ));
}

#[test]
fn jsonb_parse_number_digit_caps_apply_to_lexeme() {
    let long = format!("[{}]", "9".repeat(131_073));
    assert!(matches!(
        parse_text(long.as_bytes(), &defaults()),
        Err(JsonbError::DigitsBeforePointExceeded { .. })
    ));
}

fn nested(depth: usize) -> String {
    format!("{}{}", "[".repeat(depth), "]".repeat(depth))
}

#[test]
fn jsonb_depth_prescan_ignores_brackets_in_strings() {
    let in_string = format!("[\"{}\"]", "[".repeat(2000));
    assert!(parse_text(in_string.as_bytes(), &defaults()).is_ok());
    let escaped = format!("[\"\\\"{}\"]", "{".repeat(2000));
    assert!(parse_text(escaped.as_bytes(), &defaults()).is_ok());

    let small_stack = std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(|| {
            let limits = defaults();
            let at_limit = parse_text(nested(1000).as_bytes(), &limits);
            let below = parse_text(nested(999).as_bytes(), &limits);
            let above = parse_text(nested(1001).as_bytes(), &limits);
            (at_limit.is_ok(), below.is_ok(), above)
        })
        .expect("spawn")
        .join()
        .expect("parser must not overflow a 256 KiB stack");
    assert!(small_stack.0 && small_stack.1);
    assert_eq!(
        small_stack.2,
        Err(JsonbError::DepthExceeded {
            depth: 1001,
            max: 1000
        })
    );
}

#[test]
fn jsonb_depth_prescan_rejects_before_building() {
    // Malformed after the depth breach: the depth error wins because the
    // pre-scan runs before any parsing.
    let text = format!("{}!!", "[".repeat(1001));
    assert!(matches!(
        parse_text(text.as_bytes(), &defaults()),
        Err(JsonbError::DepthExceeded { depth: 1001, .. })
    ));
}

#[derive(Default)]
struct Recorder(Mutex<Vec<(String, u64)>>);

impl DuplicateKeyObserver for Recorder {
    fn duplicate_keys_dropped(&self, edge: &str, count: u64) {
        self.0.lock().expect("lock").push((edge.to_string(), count));
    }
}

#[test]
fn jsonb_duplicate_key_last_wins_increments_metric() {
    let rec = Recorder::default();
    let out = parse_text_observed(br#"{"a":1,"a":2}"#, &defaults(), "pg_text", &rec).unwrap();
    let single = parse_text(br#"{"a":2}"#, &defaults()).unwrap();
    assert_eq!(out.bytes, single.bytes);
    assert_eq!(out.duplicate_keys_dropped, 1);
    assert_eq!(
        *rec.0.lock().expect("lock"),
        vec![("pg_text".to_string(), 1)]
    );

    let rec = Recorder::default();
    parse_text_observed(br#"{"a":1}"#, &defaults(), "pg_text", &rec).unwrap();
    assert!(rec.0.lock().expect("lock").is_empty());
}

#[test]
fn jsonb_duplicate_key_strict_mode_is_error() {
    let strict = limits_with(1000, DuplicateKeyPolicy::Error);
    let err = parse_text(br#"{"x":[{"a":1,"a":2}]}"#, &strict).unwrap_err();
    assert_eq!(
        err,
        JsonbError::DuplicateKey {
            path: "$.x[0]".to_string()
        }
    );
    let err = parse_text(br#"{"a":1,"a":2}"#, &strict).unwrap_err();
    assert_eq!(
        err,
        JsonbError::DuplicateKey {
            path: "$".to_string()
        }
    );
}

#[test]
fn jsonb_parse_working_set_scales_linearly() {
    // 10 MiB of width-maximal input: one flat array of small numbers.
    let mut text = String::from("[");
    while text.len() < 10 * 1024 * 1024 {
        text.push_str("1,");
    }
    text.push_str("1]");
    let cfg = LimitsConfig {
        max_input_bytes: Some(16 * 1024 * 1024),
        max_encoded_bytes: Some(64 * 1024 * 1024),
        ..LimitsConfig::default()
    };
    let limits =
        Limits::from_config_with_env(&cfg, &|_: &str| None, 256 * 1024 * 1024).expect("limits");
    let start = std::time::Instant::now();
    let out = parse_text(text.as_bytes(), &limits).unwrap();
    assert!(start.elapsed().as_secs() < 60, "10 MiB parse too slow");
    assert!(
        out.bytes.len() < 4 * text.len(),
        "encoded size out of proportion"
    );
}

#[test]
fn jsonb_inflight_budget_admits_and_refuses() {
    let budget = InflightBudget::new(100);
    let a = budget.try_acquire(60).unwrap();
    assert!(matches!(
        budget.try_acquire(60),
        Err(JsonbError::InflightBudgetExceeded {
            requested: 60,
            max: 100
        })
    ));
    drop(a);
    assert!(budget.try_acquire(60).is_ok());
}

#[derive(Debug, Clone)]
enum J {
    Null,
    Bool(bool),
    Int(i64),
    Dec(i32, u8),
    Str(String),
    Arr(Vec<J>),
    Obj(Vec<(String, J)>),
}

fn render(j: &J, sp: &str, out: &mut String) {
    match j {
        J::Null => out.push_str("null"),
        J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        J::Int(i) => out.push_str(&i.to_string()),
        J::Dec(m, s) => out.push_str(&format!("{m}.{:0>w$}", 7, w = usize::from(*s) + 1)),
        J::Str(s) => out.push_str(&format!("{s:?}")),
        J::Arr(items) => {
            out.push('[');
            for (i, it) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                    out.push_str(sp);
                }
                render(it, sp, out);
            }
            out.push(']');
        }
        J::Obj(entries) => {
            out.push('{');
            for (i, (k, v)) in entries.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                    out.push_str(sp);
                }
                out.push_str(&format!("{k:?}:{sp}"));
                render(v, sp, out);
            }
            out.push('}');
        }
    }
}

fn arb_json() -> impl Strategy<Value = J> {
    let leaf = prop_oneof![
        Just(J::Null),
        any::<bool>().prop_map(J::Bool),
        any::<i64>().prop_map(J::Int),
        (any::<i32>(), 0u8..6).prop_map(|(m, s)| J::Dec(m, s)),
        "[a-z\\u{e9}\\u{1F600}]{0,6}".prop_map(J::Str),
    ];
    leaf.prop_recursive(4, 48, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..5).prop_map(J::Arr),
            prop::collection::vec(("[a-c]{1,2}", inner), 0..5).prop_map(J::Obj),
        ]
    })
}

proptest! {
    #[test]
    fn fuzz_jsonb_parse_text_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let _ = parse_text(&bytes, &defaults());
    }

    #[test]
    fn fuzz_jsonb_parse_text_deterministic_and_whitespace_insensitive(j in arb_json()) {
        let (mut compact, mut spaced) = (String::new(), String::new());
        render(&j, "", &mut compact);
        render(&j, " \n\t", &mut spaced);
        let a = parse_text(compact.as_bytes(), &defaults()).unwrap();
        let b = parse_text(compact.as_bytes(), &defaults()).unwrap();
        let c = parse_text(spaced.as_bytes(), &defaults()).unwrap();
        prop_assert_eq!(&a.bytes, &b.bytes);
        prop_assert_eq!(&a.bytes, &c.bytes);
    }
}
