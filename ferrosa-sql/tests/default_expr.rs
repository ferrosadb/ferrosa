//! W2 — column DEFAULT expressions: a constant expression compiled once and
//! evaluated per inserted row (`specs/lean4/Ferrosa/Sql/Dml.lean`, `fill`).
//!
//! The expressions are the ones Oban's schema records: `'available'`, `0`,
//! `ARRAY[]::text[]`, `'{}'` and `timezone('UTC', now())`.

use ferrosa_sql::{compile_default, ArrayElem, ArrayValue, ExecError, Value};

const NOW: i64 = 1_700_000_000_123_456;

fn eval(sql: &str) -> Value {
    compile_default(sql)
        .unwrap_or_else(|e| panic!("compile {sql}: {e}"))
        .eval(NOW)
        .unwrap_or_else(|e| panic!("eval {sql}: {e}"))
}

fn refused(sql: &str) -> ExecError {
    match compile_default(sql) {
        Err(e) => e,
        Ok(d) => panic!("{sql} compiled to {d:?}"),
    }
}

#[test]
fn oban_defaults_evaluate() {
    assert_eq!(eval("'available'"), Value::Text("available".into()));
    assert_eq!(eval("0"), Value::Int(0));
    assert_eq!(eval("20"), Value::Int(20));
    assert_eq!(eval("'{}'"), Value::Text("{}".into()));
    assert_eq!(
        eval("ARRAY[]::text[]"),
        Value::Array(ArrayValue::new(ArrayElem::Text, Vec::new()))
    );
    assert_eq!(eval("timezone('UTC', now())"), Value::Timestamp(NOW));
    assert_eq!(eval("now()"), Value::Timestamp(NOW));
}

#[test]
fn the_type_of_a_default_is_known_before_evaluation() {
    let d = compile_default("timezone('UTC', now())").unwrap();
    assert_eq!(d.column_type(), Some(ferrosa_sql::ColumnType::Timestamp));
    assert_eq!(compile_default("NULL").unwrap().column_type(), None);
}

#[test]
fn a_default_is_evaluated_afresh_each_time() {
    let d = compile_default("now()").unwrap();
    assert_eq!(d.eval(1).unwrap(), Value::Timestamp(1));
    assert_eq!(d.eval(2).unwrap(), Value::Timestamp(2));
}

#[test]
fn a_column_reference_a_subquery_or_a_parameter_is_refused() {
    for (sql, state) in [
        ("state", "0A000"),
        ("(SELECT 1 FROM t)", "0A000"),
        ("$1", "42P02"),
        ("nextval('oban_jobs_id_seq')", "0A000"),
        ("1 +", "42601"),
        ("1 2", "42601"),
    ] {
        match refused(sql) {
            ExecError::Eval { sqlstate, .. } => assert_eq!(sqlstate, state, "{sql}"),
            other => panic!("{sql}: {other:?}"),
        }
    }
}

#[test]
fn an_evaluation_error_is_an_error_not_null() {
    let d = compile_default("1 / 0").unwrap();
    assert!(matches!(
        d.eval(NOW),
        Err(ExecError::Eval {
            sqlstate: "22012",
            ..
        })
    ));
}
