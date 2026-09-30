//! `Value::Jsonb` semantics inside the relational engine (T-160, D2a, D18, FM-21, FM-44).
//!
//! Grouping, DISTINCT and hash keys use jsonb VALUE equality (`1` == `1.0`),
//! ORDER BY follows the PostgreSQL jsonb order (Object > Array > Boolean >
//! Number > String > Null), and spilled documents round-trip byte-identically.

use std::cmp::Ordering;
use std::sync::Arc;

use ferrosa_jsonb::{parse_text, JsonbValue, Limits, LimitsConfig};
use ferrosa_sql::exec::{
    dedup, hash_aggregate, hash_join, sort, AggFunc, SortDir, SortKey, TryRowStream,
};
use ferrosa_sql::spill::{row_bytes, DirReserver, SpillCtx};
use ferrosa_sql::types::{ColumnType, Row, Value};

const TINY_THRESHOLD: u64 = 256;

fn jsonb(text: &str) -> JsonbValue {
    let limits = Limits::from_config_with_env(&LimitsConfig::default(), &|_| None, 64 << 20)
        .expect("default limits");
    let enc = parse_text(text.as_bytes(), &limits).expect("valid json");
    JsonbValue::from_encoded(enc).expect("valid cell")
}

fn jv(text: &str) -> Value {
    Value::Jsonb(jsonb(text))
}

fn stream(rows: Vec<Row>) -> TryRowStream<'static> {
    Box::new(rows.into_iter().map(Ok))
}

fn drain(s: TryRowStream<'_>) -> Vec<Row> {
    s.collect::<Result<Vec<Row>, _>>().expect("no spill error")
}

fn ctx_for(dir: &std::path::Path, spill: bool) -> SpillCtx {
    let threshold = if spill { TINY_THRESHOLD } else { u64::MAX };
    SpillCtx::new(Arc::new(DirReserver::new(dir)), threshold)
}

fn one_col(vals: Vec<Value>) -> Vec<Row> {
    vals.into_iter().map(|v| Row::new(vec![v])).collect()
}

#[test]
fn sql_group_by_jsonb_scale_insensitive() {
    for spill in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let ctx = ctx_for(dir.path(), spill);
        let rows = one_col(vec![jv("1"), jv("1.0"), jv("1.00"), jv("2")]);
        let out =
            drain(hash_aggregate(stream(rows), &[0], &[(AggFunc::Count, None)], &ctx).unwrap());
        assert_eq!(
            out.len(),
            2,
            "1, 1.0 and 1.00 are one group (spill={spill})"
        );
        assert_eq!(out[0].0[1], Value::Int(3));
        assert_eq!(out[1].0[1], Value::Int(1));
    }
}

#[test]
fn distinct_treats_scale_variants_inside_documents_as_equal() {
    for spill in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let ctx = ctx_for(dir.path(), spill);
        let rows = one_col(vec![
            jv(r#"{"a":[1,{"b":2.0}]}"#),
            jv(r#"{"a":[1.0,{"b":2}]}"#),
            jv(r#"{"a":[1,{"b":3}]}"#),
        ]);
        let out = drain(dedup(stream(rows), &ctx).unwrap());
        assert_eq!(out.len(), 2, "spill={spill}");
    }
}

#[test]
fn hash_join_matches_jsonb_keys_by_value() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx_for(dir.path(), false);
    let left = one_col(vec![jv("1")]);
    let right = one_col(vec![jv("1.0")]);
    let out = drain(hash_join(stream(left), stream(right), 0, 0, &ctx).unwrap());
    assert_eq!(out.len(), 1);
}

/// PostgreSQL jsonb order, ascending: null < string < number < boolean <
/// array < object; arrays by length then element-wise; objects by pair count.
fn pg_ordered_corpus() -> Vec<&'static str> {
    vec![
        "null",
        r#""a""#,
        r#""b""#,
        "-1",
        "1",
        "2.5",
        "false",
        "true",
        "[]",
        "[1]",
        "[1,2]",
        "[2,1]",
        "{}",
        r#"{"a":1}"#,
        r#"{"a":2}"#,
    ]
}

#[test]
fn sql_order_by_jsonb_never_equal_fallback() {
    let corpus = pg_ordered_corpus();
    for (i, a) in corpus.iter().enumerate() {
        for (j, b) in corpus.iter().enumerate() {
            assert_eq!(
                jv(a).sql_cmp(&jv(b)),
                Some(i.cmp(&j)),
                "{a} vs {b} must follow D18, never UNKNOWN or Equal by fallback"
            );
        }
    }
    // Value-equal documents are Equal because they ARE equal, not by fallback.
    assert_eq!(jv("1").sql_cmp(&jv("1.0")), Some(Ordering::Equal));
    // NULL and cross-type stay UNKNOWN.
    assert_eq!(jv("1").sql_cmp(&Value::Null), None);
    assert_eq!(jv("1").sql_cmp(&Value::Int(1)), None);
}

#[test]
fn order_by_jsonb_sorts_in_pg_order_in_memory_and_spilled() {
    let corpus = pg_ordered_corpus();
    for spill in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let ctx = ctx_for(dir.path(), spill);
        let shuffled: Vec<Value> = corpus.iter().rev().map(|t| jv(t)).collect();
        let keys = [SortKey {
            col: 0,
            dir: SortDir::Asc,
        }];
        let out = drain(sort(stream(one_col(shuffled)), &keys, &ctx).unwrap());
        let want: Vec<Value> = corpus.iter().map(|t| jv(t)).collect();
        let got: Vec<Value> = out.into_iter().map(|r| r.0[0].clone()).collect();
        assert_eq!(got, want, "spill={spill}");
    }
}

fn bigdecimal_doc(i: usize) -> String {
    format!(
        r#"{{"n":9{i}123456789012345678901234567890.123456789012345678901234567890e400,"s":"x{i}"}}"#
    )
}

#[test]
fn sql_spill_round_trip_bigdecimal_jsonb() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx_for(dir.path(), true);
    let docs: Vec<JsonbValue> = (0..400).rev().map(|i| jsonb(&bigdecimal_doc(i))).collect();
    let rows = one_col(docs.iter().cloned().map(Value::Jsonb).collect());
    let keys = [SortKey {
        col: 0,
        dir: SortDir::Asc,
    }];
    let out = drain(sort(stream(rows), &keys, &ctx).unwrap());
    assert!(
        ctx.stats().spilled(),
        "the tiny threshold must force a spill"
    );
    assert_eq!(out.len(), docs.len());
    let mut want = docs;
    want.sort();
    for (row, w) in out.iter().zip(want.iter()) {
        match &row.0[0] {
            Value::Jsonb(g) => assert_eq!(g.as_bytes(), w.as_bytes(), "byte-identical"),
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[test]
fn row_serde_round_trips_and_validates_on_read() {
    let row = Row::new(vec![jv(&bigdecimal_doc(7)), Value::Null]);
    let text = serde_json::to_string(&row).unwrap();
    let back: Row = serde_json::from_str(&text).unwrap();
    assert_eq!(back, row);
    // A corrupt cell is refused on read, never a short or unchecked value.
    let corrupt = text.replacen("{\"Jsonb\":\"", "{\"Jsonb\":\"AAAA", 1);
    assert_ne!(corrupt, text, "the corruption must have been applied");
    assert!(serde_json::from_str::<Row>(&corrupt).is_err());
}

#[test]
fn row_bytes_counts_the_real_jsonb_bytes() {
    let doc = jsonb(&bigdecimal_doc(3));
    let n = doc.as_bytes().len();
    let row = Row::new(vec![Value::Jsonb(doc)]);
    assert!(
        row_bytes(&row) >= n + std::mem::size_of::<Value>(),
        "jsonb payload {n} must be counted"
    );
    let arr = Row::new(vec![Value::TextArray(vec![Some("abcd".into()), None])]);
    assert!(row_bytes(&arr) >= std::mem::size_of::<Value>() + 4);
}

#[test]
fn text_array_and_jsonpath_are_total_and_hashable() {
    use std::collections::HashSet;
    let a = Value::TextArray(vec![Some("a".into()), None]);
    let b = Value::TextArray(vec![Some("a".into())]);
    assert_eq!(a.sql_cmp(&a), Some(Ordering::Equal));
    assert_eq!(b.sql_cmp(&a), Some(Ordering::Less));
    let p = Value::JsonPath("$.a".into());
    assert_eq!(
        p.sql_cmp(&Value::JsonPath("$.b".into())),
        Some(Ordering::Less)
    );
    let set: HashSet<Value> = [a.clone(), a, b, p.clone(), p].into_iter().collect();
    assert_eq!(set.len(), 3);
}

#[test]
fn column_types_exist() {
    let all = [
        ColumnType::Jsonb,
        ColumnType::Json,
        ColumnType::JsonPath,
        ColumnType::TextArray,
    ];
    for (i, a) in all.iter().enumerate() {
        for (j, b) in all.iter().enumerate() {
            assert_eq!(a == b, i == j);
        }
    }
}
