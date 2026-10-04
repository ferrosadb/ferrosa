//! Module: T-106 tests: D18/D18a total order, value Eq, normalized Hash and
//! Debug (FM-09, FM-10, FM-11, FM-104, M5).
//! Correctness: correct when the order is total, antisymmetric and transitive
//! over every kind and number form, `Eq` <=> `cmp == Equal` <=> equal `Hash` <=>
//! equal `Debug` (scale variants and int/decimal/bigdecimal forms of one value
//! included), the PostgreSQL 16 table holds, and depth 1000 runs on 256 KiB.
//! Last revised: 2026-09-28
//! Last changed: T-106 initial tests.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use ferrosa_jsonb::{parse_text, JsonbValue};
use proptest::prelude::*;

mod common;
use common::limits;

fn val(json: &str) -> JsonbValue {
    let enc = parse_text(json.as_bytes(), &limits()).unwrap_or_else(|e| panic!("{json:?}: {e}"));
    JsonbValue::from_encoded(enc).expect("validated")
}

fn hash_of(v: &JsonbValue) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

/// A generated tree. Numbers are (mantissa, scale) so one value has many forms.
#[derive(Debug, Clone)]
enum J {
    Null,
    Bool(bool),
    Num(i64, u8),
    Str(String),
    Arr(Vec<J>),
    Obj(BTreeMap<String, J>),
}

fn digits_lexeme(neg: bool, mut digits: String, scale: usize) -> String {
    if scale > 0 {
        while digits.len() <= scale {
            digits.insert(0, '0');
        }
        digits.insert(digits.len() - scale, '.');
    }
    format!("{}{digits}", if neg { "-" } else { "" })
}

/// Form 0 as written; form 1 pads one zero; form 2 pads 40 zeros so the
/// unscaled value needs bigdecimal.
fn num_text(m: i64, scale: u8, form: u8) -> String {
    let pad = match form {
        0 => 0,
        1 => 1,
        _ => 40,
    };
    let digits = format!("{}{}", m.unsigned_abs(), "0".repeat(pad));
    digits_lexeme(m < 0, digits, usize::from(scale) + pad)
}

fn to_text(j: &J, form: u8, out: &mut String) {
    match j {
        J::Null => out.push_str("null"),
        J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        J::Num(m, s) => out.push_str(&num_text(*m, *s, form)),
        J::Str(s) => out.push_str(&serde_json::to_string(s).expect("string")),
        J::Arr(v) => {
            out.push('[');
            for (i, x) in v.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                to_text(x, form, out);
            }
            out.push(']');
        }
        J::Obj(m) => {
            out.push('{');
            let mut items: Vec<_> = m.iter().collect();
            if form > 0 {
                items.reverse();
            }
            for (i, (k, x)) in items.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(k).expect("key"));
                out.push(':');
                to_text(x, form, out);
            }
            out.push('}');
        }
    }
}

fn build(j: &J, form: u8) -> JsonbValue {
    let mut s = String::new();
    to_text(j, form, &mut s);
    val(&s)
}

fn small_num() -> impl Strategy<Value = J> {
    prop_oneof![
        (-3i64..4, 0u8..3).prop_map(|(m, s)| J::Num(m, s)),
        (any::<i64>(), 0u8..25).prop_map(|(m, s)| J::Num(m, s)),
    ]
}

fn tree() -> impl Strategy<Value = J> {
    let leaf = prop_oneof![
        Just(J::Null),
        any::<bool>().prop_map(J::Bool),
        small_num(),
        "[a-c\u{e9}]{0,3}".prop_map(J::Str),
    ];
    leaf.prop_recursive(4, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(J::Arr),
            prop::collection::btree_map("[a-c]{0,3}", inner, 0..4).prop_map(J::Obj),
        ]
    })
}

/// Every form of two trees: 2 trees x 3 forms.
fn forms(a: &J, b: &J) -> Vec<JsonbValue> {
    (0..3u8).flat_map(|f| [build(a, f), build(b, f)]).collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10_000))]

    #[test]
    fn jsonb_order_total_antisymmetric_transitive(a in tree(), b in tree()) {
        let vs = forms(&a, &b);
        for x in &vs {
            prop_assert_eq!(x.cmp(x), Ordering::Equal);
            for y in &vs {
                prop_assert_eq!(x.cmp(y), y.cmp(x).reverse());
                prop_assert_eq!(x.partial_cmp(y), Some(x.cmp(y)));
                for z in &vs {
                    if x <= y && y <= z {
                        prop_assert!(x <= z);
                    }
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3_000))]

    #[test]
    fn jsonb_eq_ord_hash_agree(a in tree(), b in tree()) {
        let vs = forms(&a, &b);
        for x in &vs {
            for y in &vs {
                let eq = x == y;
                prop_assert_eq!(eq, x.cmp(y) == Ordering::Equal);
                prop_assert_eq!(eq, hash_of(x) == hash_of(y));
                prop_assert_eq!(eq, format!("{x:?}") == format!("{y:?}"));
            }
        }
        // The same tree in every scale form is one value.
        for f in 1..3u8 {
            prop_assert_eq!(&build(&a, 0), &build(&a, f));
        }
    }
}

#[test]
fn jsonb_eq_implies_hash_eq_across_scales() {
    let forms = [
        "1",
        "1.0",
        "1.00",
        "1e0",
        "10e-1",
        "100e-2",
        "1.000000000000000000000000000000000000000000000",
    ];
    let base = val(forms[0]);
    for f in forms {
        let v = val(f);
        assert_eq!(v, base, "{f}");
        assert_eq!(hash_of(&v), hash_of(&base), "{f}");
        assert_eq!(format!("{v:?}"), format!("{base:?}"), "{f}");
    }
    let nested_a = val(r#"{"k":[1,2.50,{"z":3.0}]}"#);
    let nested_b = val(r#"{"k":[1.0,2.5,{"z":3}]}"#);
    assert_eq!(nested_a, nested_b);
    assert_eq!(hash_of(&nested_a), hash_of(&nested_b));
    assert_ne!(nested_a.as_bytes(), nested_b.as_bytes());
    assert_ne!(val("1.5"), val("1.05"));
}

#[test]
fn jsonb_debug_is_injective_and_untruncated() {
    let big = "x".repeat(1024 * 1024);
    let a = val(&format!("[\"{big}a\"]"));
    let b = val(&format!("[\"{big}b\"]"));
    let (da, db) = (format!("{a:?}"), format!("{b:?}"));
    assert!(da.len() > 1024 * 1024 && db.len() > 1024 * 1024);
    assert_ne!(da, db);
    assert_ne!(format!("{:?}", val("\"1\"")), format!("{:?}", val("1")));
    assert_ne!(format!("{:?}", val("[1,2]")), format!("{:?}", val("[12]")));
    assert_ne!(
        format!("{:?}", val("[\"a,b\"]")),
        format!("{:?}", val("[\"a\",\"b\"]"))
    );
    assert_eq!(format!("{:?}", val("1.50")), format!("{:?}", val("1.5")));
}

/// (smaller, larger) pairs. Source: PostgreSQL 16 docs, "jsonb Indexing" /
/// datatype-json: "Object > Array > Boolean > Number > String > Null", "Object
/// with n pairs > object with n - 1 pairs", "Array with n elements > array with
/// n - 1 elements", and the documented `{"aa": 1, "c": 1} > {"b": 1, "d": 1}`.
///
/// Every pair was also checked against a live `postgres:16` (`a::jsonb <
/// b::jsonb`), which is how the one exception the docs leave out was found: a
/// TOP-LEVEL empty array sorts below every top-level scalar, `null` included.
/// PostgreSQL stores a top-level scalar as a one-element "raw scalar" array and
/// compares element counts first, so `[]` (0) < `null` (1). Nested, the kind
/// rank holds: `[[]] > [null]`.
const PG_TABLE: &[(&str, &str)] = &[
    ("null", "\"a\""),
    ("\"a\"", "0"),
    ("0", "false"),
    ("false", "true"),
    ("[]", "true"),
    ("[]", "false"),
    ("[]", "0"),
    ("[]", "-1e300"),
    ("[]", "\"\""),
    ("[]", "null"),
    ("[]", "[null]"),
    ("[null]", "[[]]"),
    ("[\"\"]", "[[]]"),
    ("true", "[0]"),
    ("2", "[1]"),
    ("1", "{}"),
    ("[]", "{}"),
    ("null", "{}"),
    ("\"zzz\"", "-5"),
    ("\"a\"", "\"b\""),
    ("\"a\"", "\"aa\""),
    ("1", "1.5"),
    ("-2", "-1.5"),
    ("[1,2]", "[1,2,3]"),
    ("[9]", "[1,1]"),
    ("[1,2]", "[1,3]"),
    ("{\"a\":1}", "{\"a\":1,\"b\":1}"),
    ("{\"z\":9}", "{\"a\":1,\"b\":1}"),
    ("{\"a\":1}", "{\"a\":2}"),
    ("{\"a\":1}", "{\"b\":1}"),
    ("{\"b\":1,\"d\":1}", "{\"aa\":1,\"c\":1}"),
];

#[test]
fn jsonb_pg_order_table() {
    for (lo, hi) in PG_TABLE {
        let (a, b) = (val(lo), val(hi));
        assert_eq!(a.cmp(&b), Ordering::Less, "{lo} < {hi}");
        assert_eq!(b.cmp(&a), Ordering::Greater, "{hi} > {lo}");
        assert_ne!(a, b);
    }
}

#[test]
fn jsonb_object_order_uses_pg_key_order() {
    // Bytewise, "aa" < "b"; PostgreSQL stores shorter keys first, so "b" < "aa".
    assert_eq!(val(r#"{"b":1}"#).cmp(&val(r#"{"aa":1}"#)), Ordering::Less);
    assert_eq!(
        val(r#"{"aa":1,"c":1}"#).cmp(&val(r#"{"b":1,"d":1}"#)),
        Ordering::Greater
    );
}

#[test]
fn jsonb_sort_large_vec_does_not_panic() {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut vs: Vec<JsonbValue> = (0..10_000)
        .map(|_| {
            let n = next();
            let text = match n % 6 {
                0 => "null".to_string(),
                1 => format!("{}", (n >> 8) % 50),
                2 => format!("{}.{}0", (n >> 8) % 20, (n >> 16) % 10),
                3 => format!("\"s{}\"", (n >> 8) % 30),
                4 => format!("[{},\"a\"]", (n >> 8) % 10),
                _ => format!("{{\"k{}\":{}}}", (n >> 8) % 5, (n >> 16) % 5),
            };
            val(&text)
        })
        .collect();
    vs.sort();
    assert!(vs.windows(2).all(|w| w[0] <= w[1]));
}

fn nested(open: &str, close: &str, inner: &str, depth: usize) -> String {
    format!("{}{inner}{}", open.repeat(depth), close.repeat(depth))
}

#[test]
fn jsonb_depth_1000_on_256k_stack() {
    let handle = std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(|| {
            let arr = |x: &str| val(&nested("[", "]", x, 1000));
            let obj = |x: &str| val(&nested("{\"a\":", "}", x, 1000));
            for make in [&arr as &dyn Fn(&str) -> JsonbValue, &obj] {
                let (one, one_again, two) = (make("1"), make("1.0"), make("2"));
                assert_eq!(one, one_again);
                assert_eq!(hash_of(&one), hash_of(&one_again));
                assert_eq!(one.cmp(&two), Ordering::Less);
                assert!(format!("{one:?}") == format!("{one_again:?}"));
                assert!(format!("{one:?}") != format!("{two:?}"));
                let copy = one.clone();
                drop(copy);
            }
        })
        .expect("spawn");
    handle.join().expect("depth 1000 on 256 KiB");
}
