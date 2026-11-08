//! W2 — the read half of a general `UPDATE` / `DELETE`, end to end (parse → plan →
//! execute) against in-memory tables.
//!
//! The read yields one row per target row to write: its storage key, the new value
//! of every `SET` column, the `RETURNING` values over the new row, and (when asked)
//! the old value of every target column. `specs/lean4/Ferrosa/Sql/Dml.lean` is why
//! writing those rows by key is the declarative statement.

use std::sync::Arc;

use ferrosa_sql::types::{Column, ColumnType, RelSchema, Row, Value};
use ferrosa_sql::{
    describe_dml, infer_dml_param_types, open_dml_cursor, parse_statement, ArrayElem, ArrayValue,
    DmlLayout, DmlSpec, DmlStmt, ExecError, InMemoryTable, MapCatalog, SpillCtx, Statement,
};

fn int(n: i64) -> Value {
    Value::Int(n)
}

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn texts(items: &[&str]) -> Value {
    Value::Array(ArrayValue::new(
        ArrayElem::Text,
        items.iter().map(|s| text(s)).collect(),
    ))
}

fn table(cols: &[(&str, ColumnType)], rows: Vec<Vec<Value>>) -> Arc<InMemoryTable> {
    let schema = RelSchema::new(cols.iter().map(|(n, t)| Column::new(*n, *t)).collect());
    Arc::new(InMemoryTable::new(
        schema,
        rows.into_iter().map(Row).collect(),
    ))
}

/// A cut-down `public.oban_jobs`, keyed by `id`.
fn jobs() -> Arc<InMemoryTable> {
    use ColumnType::*;
    table(
        &[
            ("id", BigInt),
            ("state", Text),
            ("queue", Text),
            ("attempt", Int),
            ("max_attempts", Int),
            ("priority", Int),
            ("attempted_at", Timestamp),
            ("attempted_by", Array(ArrayElem::Text)),
        ],
        vec![
            row(1, "available", "default", 0, 20, 0),
            row(2, "available", "default", 0, 20, 1),
            row(3, "available", "other", 0, 20, 0),
            row(4, "available", "default", 20, 20, 0),
            row(5, "completed", "default", 1, 20, 0),
            row(6, "available", "default", 0, 20, 2),
        ],
    )
}

fn row(id: i64, state: &str, queue: &str, attempt: i64, max: i64, prio: i64) -> Vec<Value> {
    vec![
        int(id),
        text(state),
        text(queue),
        int(attempt),
        int(max),
        int(prio),
        Value::Null,
        Value::Null,
    ]
}

/// `public.t (k, v, s)` and `public.u (k, x)`, where `u` matches `t.k = 1` twice.
fn kv() -> MapCatalog {
    use ColumnType::*;
    let t = table(
        &[("k", Int), ("v", Int), ("s", Text)],
        vec![
            vec![int(1), int(10), text("a")],
            vec![int(2), int(20), text("b")],
            vec![int(3), int(30), text("c")],
        ],
    );
    let u = table(
        &[("k", Int), ("x", Int)],
        vec![
            vec![int(3), int(300)],
            vec![int(1), int(100)],
            vec![int(1), int(111)],
        ],
    );
    let ts = table(
        &[("k", Int), ("ts", Timestamp), ("j", Jsonb)],
        vec![vec![int(1), Value::Null, Value::Null]],
    );
    MapCatalog::new()
        .with_table("public", "t", t)
        .with_table("public", "u", u)
        .with_table("public", "ts", ts)
}

fn oban() -> MapCatalog {
    MapCatalog::new().with_table("public", "oban_jobs", jobs())
}

fn dml(sql: &str) -> DmlStmt {
    match parse_statement(sql) {
        Ok(Statement::Dml(d)) => *d,
        other => panic!("not a general DML statement: {sql}: {other:?}"),
    }
}

fn spec(keys: &[&str]) -> DmlSpec {
    DmlSpec {
        key_columns: keys.iter().map(|k| k.to_string()).collect(),
        old_row: false,
    }
}

fn run(
    cat: &MapCatalog,
    sql: &str,
    params: &[Value],
    spec: &DmlSpec,
) -> (DmlLayout, Vec<Vec<Value>>) {
    try_run(cat, sql, params, spec).unwrap_or_else(|e| panic!("run {sql}: {e}"))
}

fn try_run(
    cat: &MapCatalog,
    sql: &str,
    params: &[Value],
    spec: &DmlSpec,
) -> Result<(DmlLayout, Vec<Vec<Value>>), ExecError> {
    let stmt = dml(sql);
    let (mut cursor, layout) =
        open_dml_cursor(&stmt, spec, cat, "public", params, &SpillCtx::default())?;
    let mut rows = Vec::new();
    while let Some(r) = cursor.next_row() {
        rows.push(r?.0);
    }
    Ok((layout, rows))
}

const CLAIM: &str = r#"WITH "subset" AS (SELECT so0."id" AS "id" FROM "public"."oban_jobs" AS so0 WHERE (so0."state" = 'available') AND (so0."queue" = $1) ORDER BY so0."priority", so0."id" LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE "public"."oban_jobs" AS o0 SET "state" = $3, "attempted_at" = $4, "attempted_by" = $5, "attempt" = o0."attempt" + $6 FROM "subset" AS f1 WHERE (o0."id" = f1."id") AND (o0."attempt" < o0."max_attempts") RETURNING o0."id", o0."state", o0."attempt", o0."attempted_by""#;

#[test]
fn the_oban_claim_yields_keys_new_values_and_returning_over_the_new_row() {
    let at = Value::Timestamp(1_700_000_000_000_000);
    let params = [
        text("default"),
        int(3),
        text("executing"),
        at.clone(),
        texts(&["node-1"]),
        int(1),
    ];
    let (layout, mut rows) = run(&oban(), CLAIM, &params, &spec(&["id"]));
    assert_eq!(layout.keys.len(), 1);
    let set_names: Vec<&str> = layout.sets.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        set_names,
        ["state", "attempted_at", "attempted_by", "attempt"]
    );
    assert_eq!(layout.returning.len(), 4);
    rows.sort_by_key(|r| match r[0] {
        Value::Int(n) => n,
        _ => panic!("key is an int"),
    });
    // The subset is ids 1, 4, 2 (priority, id; LIMIT 3); 4 is at max attempts.
    let ids: Vec<&Value> = rows.iter().map(|r| &r[0]).collect();
    assert_eq!(ids, [&int(1), &int(2)]);
    for r in &rows {
        assert_eq!(
            &r[1..5],
            &[text("executing"), at.clone(), texts(&["node-1"]), int(1)]
        );
        // RETURNING id, state, attempt, attempted_by: over the NEW row.
        assert_eq!(
            &r[5..9],
            &[r[0].clone(), text("executing"), int(1), texts(&["node-1"])]
        );
    }
}

#[test]
fn the_claim_describes_its_returning_and_infers_every_parameter() {
    let stmt = dml(CLAIM);
    let layout = describe_dml(&stmt, &spec(&["id"]), &oban(), "public").unwrap();
    let ret: Vec<(&str, ColumnType)> = layout
        .returning
        .iter()
        .map(|c| (c.name.as_str(), c.ty))
        .collect();
    assert_eq!(
        ret,
        [
            ("id", ColumnType::BigInt),
            ("state", ColumnType::Text),
            ("attempt", ColumnType::Int),
            ("attempted_by", ColumnType::Array(ArrayElem::Text)),
        ]
    );
    let params = infer_dml_param_types(&stmt, &spec(&["id"]), &oban(), "public").unwrap();
    assert_eq!(
        params,
        [
            ColumnType::Text,
            ColumnType::BigInt,
            ColumnType::Text,
            ColumnType::Timestamp,
            ColumnType::Array(ArrayElem::Text),
            ColumnType::Int,
        ]
    );
}

const ACK: &str = r#"UPDATE "public"."oban_jobs" AS o0 SET "state" = $1, "attempt" = $2 WHERE (o0."id" = $3) AND (o0."state" = ANY($4))"#;

#[test]
fn the_oban_ack_matches_by_id_and_state_any() {
    let params = [
        text("completed"),
        int(9),
        int(1),
        texts(&["executing", "available"]),
    ];
    let (_, rows) = run(&oban(), ACK, &params, &spec(&["id"]));
    assert_eq!(rows, [vec![int(1), text("completed"), int(9)]]);
    let params = [text("completed"), int(9), int(5), texts(&["executing"])];
    let (_, rows) = run(&oban(), ACK, &params, &spec(&["id"]));
    assert!(rows.is_empty(), "job 5 is completed, not executing");
    let stmt = dml(ACK);
    let types = infer_dml_param_types(&stmt, &spec(&["id"]), &oban(), "public").unwrap();
    assert_eq!(types[3], ColumnType::Array(ArrayElem::Text));
}

#[test]
fn a_plugin_delete_by_in_subquery_with_limit() {
    let (layout, rows) = run(
        &oban(),
        "DELETE FROM oban_jobs WHERE id IN \
         (SELECT id FROM oban_jobs WHERE state = 'available' AND queue = 'default' \
          ORDER BY id LIMIT 2) RETURNING id, state",
        &[],
        &spec(&["id"]),
    );
    assert!(layout.sets.is_empty());
    let mut keys: Vec<i64> = rows
        .iter()
        .map(|r| match r[0] {
            Value::Int(n) => n,
            _ => panic!("int key"),
        })
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, [1, 2]);
    assert!(rows.iter().all(|r| r[2] == text("available")));
}

#[test]
fn a_plugin_update_appends_to_an_array() {
    let (_, rows) = run(
        &oban(),
        "UPDATE oban_jobs AS o0 SET attempted_by = array_append(coalesce(o0.attempted_by, ARRAY[]::text[]), $1) \
         WHERE o0.id IN (SELECT id FROM oban_jobs WHERE state = 'completed' LIMIT $2)",
        &[text("lifeline"), int(5)],
        &spec(&["id"]),
    );
    assert_eq!(rows, [vec![int(5), texts(&["lifeline"])]]);
}

#[test]
fn update_from_writes_each_target_row_once_with_its_first_match() {
    let (_, rows) = run(
        &kv(),
        "UPDATE t SET v = u.x FROM u WHERE t.k = u.k RETURNING t.v, u.x",
        &[],
        &spec(&["k"]),
    );
    assert_eq!(
        rows,
        [
            vec![int(1), int(100), int(100), int(100)],
            vec![int(3), int(300), int(300), int(300)],
        ],
        "k = 1 matches twice; the first match in arrival order wins, once"
    );
}

#[test]
fn returning_sees_the_new_row_and_the_old_row_is_available() {
    let s = DmlSpec {
        key_columns: vec!["k".into()],
        old_row: true,
    };
    let (layout, rows) = run(
        &kv(),
        "UPDATE t SET v = v + 1 WHERE k >= 2 RETURNING v, t.v * 2 AS twice, s",
        &[],
        &s,
    );
    assert_eq!(layout.old.len(), 3);
    assert_eq!(layout.returning[1].name, "twice");
    assert_eq!(
        rows,
        [
            vec![
                int(2),
                int(21),
                int(21),
                int(42),
                text("b"),
                int(2),
                int(20),
                text("b")
            ],
            vec![
                int(3),
                int(31),
                int(31),
                int(62),
                text("c"),
                int(3),
                int(30),
                text("c")
            ],
        ]
    );
}

#[test]
fn update_without_where_targets_every_row() {
    let (_, rows) = run(&kv(), "UPDATE t SET s = 'z'", &[], &spec(&["k"]));
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|r| r[1] == text("z")));
}

#[test]
fn returning_star_is_the_new_target_row() {
    let (layout, rows) = run(
        &kv(),
        "UPDATE t SET v = 0 WHERE k = 1 RETURNING *",
        &[],
        &spec(&["k"]),
    );
    let names: Vec<&str> = layout.returning.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["k", "v", "s"]);
    assert_eq!(rows, [vec![int(1), int(0), int(1), int(0), text("a")]]);
}

#[test]
fn a_text_literal_is_assigned_to_a_timestamp_or_jsonb_column() {
    let (_, rows) = run(
        &kv(),
        "UPDATE ts SET ts = '2024-01-02 03:04:05', j = '{\"a\": 1}' RETURNING ts",
        &[],
        &spec(&["k"]),
    );
    let micros = 1_704_164_645_000_000;
    assert_eq!(rows[0][1], Value::Timestamp(micros));
    assert!(matches!(rows[0][2], Value::Jsonb(_)), "{:?}", rows[0][2]);
    assert_eq!(rows[0][3], Value::Timestamp(micros));
}

#[test]
fn assigning_a_key_column_is_refused() {
    let err = try_run(
        &kv(),
        "UPDATE t AS x SET k = 9 WHERE v = 10",
        &[],
        &spec(&["k"]),
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::Unsupported(_)), "{err:?}");
}

#[test]
fn an_unknown_set_column_or_key_column_is_an_error() {
    let err = try_run(&kv(), "UPDATE t SET nope = 1", &[], &spec(&["k"])).unwrap_err();
    assert!(matches!(err, ExecError::NoSuchColumn(_)), "{err:?}");
    let err = try_run(&kv(), "UPDATE t SET v = 1", &[], &spec(&["missing"])).unwrap_err();
    assert!(matches!(err, ExecError::NoSuchColumn(_)), "{err:?}");
}

#[test]
fn a_cte_cannot_be_the_target() {
    let err = try_run(
        &kv(),
        "WITH c AS (SELECT k FROM t) UPDATE c SET k = 1",
        &[],
        &spec(&["k"]),
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::Unsupported(_)), "{err:?}");
}

#[test]
fn an_evaluation_error_in_set_fails_the_read() {
    let err = try_run(&kv(), "UPDATE t SET v = v / 0", &[], &spec(&["k"])).unwrap_err();
    assert!(
        matches!(
            err,
            ExecError::Eval {
                sqlstate: "22012",
                ..
            }
        ),
        "{err:?}"
    );
}
