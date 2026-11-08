//! W1 — general scalar expressions, joins, subqueries and CTEs, end to end
//! (parse → plan → execute) against in-memory tables.
//!
//! The semantics are the ones `specs/lean4/Ferrosa/Sql/Expr.lean` pins: three-
//! valued logic, `= ANY` and `NOT IN` with NULLs, `coalesce`, and `WHERE` keeping
//! only TRUE rows. The last tests run Postgrex's type-bootstrap query verbatim,
//! which is the first statement any Elixir client sends.

use std::sync::Arc;

use ferrosa_sql::types::{Column, ColumnType, RelSchema, Row, Value};
use ferrosa_sql::{
    execute, parse_statement, ArrayElem, ArrayValue, Expr, InMemoryTable, MapCatalog, Statement,
};

fn int(n: i64) -> Value {
    Value::Int(n)
}

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn table(cols: &[(&str, ColumnType)], rows: Vec<Vec<Value>>) -> Arc<InMemoryTable> {
    let schema = RelSchema::new(cols.iter().map(|(n, t)| Column::new(*n, *t)).collect());
    Arc::new(InMemoryTable::new(
        schema,
        rows.into_iter().map(Row).collect(),
    ))
}

/// `public.t`: id, a, b, s, n (n is nullable).
fn catalog() -> MapCatalog {
    use ColumnType::*;
    let t = table(
        &[("id", Int), ("a", Int), ("b", Int), ("s", Text), ("n", Int)],
        vec![
            vec![int(1), int(1), int(1), text("x"), Value::Null],
            vec![int(2), int(2), int(5), text("y"), int(7)],
            vec![int(3), int(9), int(4), text("x"), int(3)],
        ],
    );
    let u = table(
        &[("tid", Int), ("tag", Text)],
        vec![
            vec![int(1), text("red")],
            vec![int(1), text("blue")],
            vec![int(3), text("green")],
        ],
    );
    MapCatalog::new()
        .with_table("public", "t", t)
        .with_table("public", "u", u)
}

fn select(sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
    select_in(&catalog(), "public", sql, params)
}

fn select_in(cat: &MapCatalog, schema: &str, sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
    let Statement::Select(stmt) =
        parse_statement(sql).unwrap_or_else(|e| panic!("parse {sql}: {e}"))
    else {
        panic!("not a select: {sql}")
    };
    execute(&stmt, cat, schema, params)
        .unwrap_or_else(|e| panic!("execute {sql}: {e}"))
        .rows
        .into_iter()
        .map(|r| r.0)
        .collect()
}

// -- projections and arithmetic --------------------------------------------------

#[test]
fn projects_arithmetic_over_columns() {
    let rows = select("SELECT id, a + b * 2 AS x, -a FROM t ORDER BY id", &[]);
    assert_eq!(
        rows,
        vec![
            vec![int(1), int(3), int(-1)],
            vec![int(2), int(12), int(-2)],
            vec![int(3), int(17), int(-9)],
        ]
    );
}

#[test]
fn arithmetic_with_null_is_null_and_parameters_bind() {
    let rows = select("SELECT n + $1 FROM t ORDER BY id", &[int(10)]);
    assert_eq!(rows, vec![vec![Value::Null], vec![int(17)], vec![int(13)]]);
}

// -- WHERE beyond column-op-literal ---------------------------------------------

#[test]
fn compares_column_to_column() {
    let rows = select("SELECT id FROM t WHERE a = b", &[]);
    assert_eq!(rows, vec![vec![int(1)]]);
    let rows = select("SELECT id FROM t WHERE a < b ORDER BY id", &[]);
    assert_eq!(rows, vec![vec![int(2)]]);
}

#[test]
fn unknown_conditions_drop_rows_and_is_null_is_two_valued() {
    // n > 0 is UNKNOWN for id 1: dropped, not kept.
    assert_eq!(
        select("SELECT id FROM t WHERE n > 0 ORDER BY id", &[]),
        vec![vec![int(2)], vec![int(3)]]
    );
    assert_eq!(
        select("SELECT id FROM t WHERE n IS NULL", &[]),
        vec![vec![int(1)]]
    );
    assert_eq!(
        select("SELECT id FROM t WHERE n IS NOT NULL ORDER BY id", &[]),
        vec![vec![int(2)], vec![int(3)]]
    );
    assert_eq!(
        select(
            "SELECT id FROM t WHERE (a < b AND s = 'y') OR n IS NULL ORDER BY id",
            &[]
        ),
        vec![vec![int(1)], vec![int(2)]]
    );
}

#[test]
fn legacy_shapes_still_lower_to_the_fast_path() {
    let Statement::Select(stmt) = parse_statement("SELECT * FROM t WHERE id = $1").unwrap() else {
        panic!()
    };
    assert!(
        matches!(stmt.filter, Some(Expr::Compare { .. })),
        "column = $1 keeps the Compare shape point reads and Accord read sets rely on"
    );
}

// -- functions, CASE, IN, BETWEEN, casts ----------------------------------------

#[test]
fn coalesce_char_length_mod_and_case() {
    let rows = select(
        "SELECT coalesce(n, 0), char_length(s || 'ab'), mod(a, 3), \
         CASE WHEN a > 1 THEN 'big' ELSE 'small' END FROM t ORDER BY id",
        &[],
    );
    assert_eq!(
        rows,
        vec![
            vec![int(0), int(3), int(1), text("small")],
            vec![int(7), int(3), int(2), text("big")],
            vec![int(3), int(3), int(0), text("big")],
        ]
    );
}

#[test]
fn in_lists_and_not_in_with_a_null() {
    assert_eq!(
        select("SELECT id FROM t WHERE a IN (2, 9) ORDER BY id", &[]),
        vec![vec![int(2)], vec![int(3)]]
    );
    // NOT IN over a list holding NULL is never TRUE (Expr.lean: not_in_with_null_never_true).
    assert_eq!(
        select("SELECT id FROM t WHERE a NOT IN (2, NULL)", &[]),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        select("SELECT id FROM t WHERE a BETWEEN 2 AND 9 ORDER BY id", &[]),
        vec![vec![int(2)], vec![int(3)]]
    );
}

#[test]
fn casts_between_scalar_types() {
    let rows = select(
        "SELECT a::text, '42'::int + 1, (a > 1)::int FROM t WHERE id = 2",
        &[],
    );
    assert_eq!(rows, vec![vec![text("2"), int(43), int(1)]]);
}

// -- = ANY(array) ---------------------------------------------------------------

#[test]
fn equals_any_over_an_array_parameter_and_constructor() {
    let states = Value::Array(ArrayValue::new(ArrayElem::Text, vec![text("x"), text("z")]));
    assert_eq!(
        select("SELECT id FROM t WHERE s = ANY($1) ORDER BY id", &[states]),
        vec![vec![int(1)], vec![int(3)]]
    );
    assert_eq!(
        select(
            "SELECT id FROM t WHERE a = ANY(ARRAY[2, 9]) ORDER BY id",
            &[]
        ),
        vec![vec![int(2)], vec![int(3)]]
    );
}

// -- joins ----------------------------------------------------------------------

#[test]
fn left_join_keeps_unmatched_rows_with_nulls() {
    let rows = select(
        "SELECT t.id, u.tag FROM t LEFT JOIN u ON u.tid = t.id ORDER BY t.id, u.tag",
        &[],
    );
    assert_eq!(
        rows,
        vec![
            vec![int(1), text("blue")],
            vec![int(1), text("red")],
            vec![int(2), Value::Null],
            vec![int(3), text("green")],
        ]
    );
}

#[test]
fn joins_on_arbitrary_conditions_and_more_than_two_tables() {
    let rows = select(
        "SELECT x.id, y.id FROM t AS x JOIN t AS y ON y.a = x.b OR y.id = x.n \
         LEFT JOIN u ON u.tid = y.id AND u.tag = 'green' WHERE u.tag IS NOT NULL ORDER BY x.id",
        &[],
    );
    // x=2 (b=5, n=7): no y. x=3 (b=4, n=3): y=3 via n; u has green for 3.
    assert_eq!(rows, vec![vec![int(3), int(3)]]);
}

// -- subqueries -----------------------------------------------------------------

#[test]
fn correlated_exists_and_not_exists() {
    assert_eq!(
        select(
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id) ORDER BY id",
            &[]
        ),
        vec![vec![int(1)], vec![int(3)]]
    );
    assert_eq!(
        select(
            "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)",
            &[]
        ),
        vec![vec![int(2)]]
    );
}

#[test]
fn scalar_and_array_subqueries() {
    let rows = select(
        "SELECT id, (SELECT count(*) FROM u WHERE u.tid = t.id), \
         ARRAY(SELECT u.tag FROM u WHERE u.tid = t.id ORDER BY u.tag) FROM t ORDER BY id",
        &[],
    );
    let arr = |items: Vec<Value>| Value::Array(ArrayValue::new(ArrayElem::Text, items));
    assert_eq!(
        rows,
        vec![
            vec![int(1), int(2), arr(vec![text("blue"), text("red")])],
            vec![int(2), int(0), arr(vec![])],
            vec![int(3), int(1), arr(vec![text("green")])],
        ]
    );
}

// -- CTEs -----------------------------------------------------------------------

#[test]
fn with_clause_feeds_the_main_select() {
    let rows = select(
        "WITH big AS (SELECT id, a FROM t WHERE a > 1 ORDER BY id LIMIT 1) \
         SELECT t.id, t.s FROM t JOIN big ON big.id = t.id",
        &[],
    );
    assert_eq!(rows, vec![vec![int(2), text("y")]]);
}

// -- Postgrex's type bootstrap, verbatim ----------------------------------------

fn pg_catalog() -> MapCatalog {
    use ColumnType::*;
    let pg_type = table(
        &[
            ("oid", Int),
            ("typname", Text),
            ("typsend", Text),
            ("typreceive", Text),
            ("typoutput", Text),
            ("typinput", Text),
            ("typelem", Int),
            ("typrelid", Int),
            ("typbasetype", Int),
        ],
        vec![
            vec![
                int(23),
                text("int4"),
                text("int4send"),
                text("int4recv"),
                text("int4out"),
                text("int4in"),
                int(0),
                int(0),
                int(0),
            ],
            vec![
                int(1007),
                text("_int4"),
                text("array_send"),
                text("array_recv"),
                text("array_out"),
                text("array_in"),
                int(23),
                int(0),
                int(0),
            ],
            // A composite (row) type: typrelid != 0, filtered out by the query.
            vec![
                int(16500),
                text("t"),
                text("record_send"),
                text("record_recv"),
                text("record_out"),
                text("record_in"),
                int(0),
                int(16501),
                int(0),
            ],
            // An array OF that composite: excluded by the NOT EXISTS.
            vec![
                int(16502),
                text("_t"),
                text("array_send"),
                text("array_recv"),
                text("array_out"),
                text("array_in"),
                int(16500),
                int(0),
                int(0),
            ],
        ],
    );
    let pg_attribute = table(
        &[
            ("attrelid", Int),
            ("attnum", Int),
            ("atttypid", Int),
            ("attisdropped", Bool),
        ],
        vec![vec![int(16501), int(1), int(23), Value::Bool(false)]],
    );
    let pg_range = table(
        &[
            ("rngtypid", Int),
            ("rngmultitypid", Int),
            ("rngsubtype", Int),
        ],
        vec![],
    );
    MapCatalog::new()
        .with_table("pg_catalog", "pg_type", pg_type)
        .with_table("pg_catalog", "pg_attribute", pg_attribute)
        .with_table("pg_catalog", "pg_range", pg_range)
}

const POSTGREX_BOOTSTRAP: &str = "SELECT t.oid, t.typname, t.typsend, t.typreceive, t.typoutput, t.typinput,
       coalesce(d.typelem, t.typelem), coalesce(r.rngsubtype, 0), ARRAY (
  SELECT a.atttypid
  FROM pg_attribute AS a
  WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum
)

FROM pg_type AS t
LEFT JOIN pg_type AS d ON t.typbasetype = d.oid
LEFT JOIN pg_range AS r ON r.rngtypid = t.oid OR r.rngmultitypid = t.oid OR (t.typbasetype <> 0 AND r.rngtypid = t.typbasetype)
WHERE (t.typrelid = 0)
AND (t.typelem = 0 OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type s WHERE s.typrelid != 0 AND s.oid = t.typelem))";

#[test]
fn postgrex_type_bootstrap_runs_verbatim() {
    let rows = select_in(&pg_catalog(), "pg_catalog", POSTGREX_BOOTSTRAP, &[]);
    let oids: Vec<Value> = rows.iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        oids,
        vec![int(23), int(1007)],
        "composite and array-of-composite are excluded"
    );
    let int4 = &rows[0];
    assert_eq!(
        int4[6],
        int(0),
        "coalesce(d.typelem, t.typelem) with no base type"
    );
    assert_eq!(int4[7], int(0), "coalesce(r.rngsubtype, 0) with no range");
    assert_eq!(
        int4[8],
        Value::Array(ArrayValue::new(ArrayElem::Int, vec![]))
    );
}

#[test]
fn postgrex_bootstrap_for_new_oids_runs() {
    let sql = "SELECT t.oid, t.typname, t.typsend, t.typreceive, t.typoutput, t.typinput,
       coalesce(d.typelem, t.typelem), coalesce(r.rngsubtype, 0), ARRAY (
  SELECT a.atttypid
  FROM pg_attribute AS a
  WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum
)

FROM pg_type AS t
LEFT JOIN pg_type AS d ON t.typbasetype = d.oid
LEFT JOIN pg_range AS r ON r.rngtypid = t.oid OR r.rngmultitypid = t.oid OR (t.typbasetype <> 0 AND r.rngtypid = t.typbasetype)
WHERE t.oid IN (16500)";
    let rows = select_in(&pg_catalog(), "pg_catalog", sql, &[]);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][8],
        Value::Array(ArrayValue::new(ArrayElem::Int, vec![int(23)])),
        "attribute types of the composite"
    );
}

// -- failing loud ---------------------------------------------------------------

fn select_err(sql: &str, params: &[Value]) -> ferrosa_sql::ExecError {
    let Statement::Select(stmt) =
        parse_statement(sql).unwrap_or_else(|e| panic!("parse {sql}: {e}"))
    else {
        panic!("not a select: {sql}")
    };
    execute(&stmt, &catalog(), "public", params).expect_err(sql)
}

fn sqlstate(err: &ferrosa_sql::ExecError) -> &'static str {
    match err {
        ferrosa_sql::ExecError::Eval { sqlstate, .. } => sqlstate,
        ferrosa_sql::ExecError::Unsupported(_) => "0A000",
        other => panic!("expected an evaluation error, got {other:?}"),
    }
}

#[test]
fn a_cast_to_a_type_the_engine_does_not_implement_is_refused_by_name() {
    let err = select_err("SELECT a::nowhere FROM t", &[]);
    assert_eq!(sqlstate(&err), "0A000");
    assert!(err.to_string().contains("nowhere"), "{err}");
}

#[test]
fn an_expression_error_reaches_the_caller_with_its_sqlstate_even_through_a_sort() {
    // 10 / (a - 1) divides by zero on id 1. The error is raised under the sort,
    // which must surface it as 22012, not as a spill failure or a short result.
    let err = select_err("SELECT 10 / (a - 1) FROM t ORDER BY 1", &[]);
    assert_eq!(sqlstate(&err), "22012", "{err}");
    let err = select_err("SELECT id FROM t WHERE 10 / (a - 1) > 0", &[]);
    assert_eq!(sqlstate(&err), "22012", "{err}");
}

#[test]
fn a_scalar_subquery_returning_two_rows_is_an_error() {
    let err = select_err("SELECT (SELECT tag FROM u WHERE u.tid = t.id) FROM t", &[]);
    assert_eq!(sqlstate(&err), "21000", "{err}");
}

#[test]
fn a_missing_parameter_is_refused_before_any_row() {
    let err = select_err("SELECT a + $2 FROM t", &[int(1)]);
    assert!(
        matches!(err, ferrosa_sql::ExecError::MissingParameter(2)),
        "{err:?}"
    );
}

#[test]
fn int4_overflow_is_an_error_not_a_wrapped_value() {
    let err = select_err("SELECT a * 2147483647 * 2 FROM t", &[]);
    assert_eq!(sqlstate(&err), "22003", "{err}");
}

#[test]
fn group_by_with_expressions_over_aggregates() {
    let rows = select(
        "SELECT s, count(*) * 10, max(a) + min(b) FROM t GROUP BY s HAVING count(*) > 0 ORDER BY s",
        &[],
    );
    assert_eq!(
        rows,
        vec![
            vec![text("x"), int(20), int(10)],
            vec![text("y"), int(10), int(7)]
        ]
    );
}

#[test]
fn describe_and_param_inference_follow_the_expression_types() {
    let Statement::Select(stmt) =
        parse_statement("SELECT a + 1 AS x, s || '!' FROM t WHERE s = ANY($1) AND a > $2").unwrap()
    else {
        panic!()
    };
    let cols = ferrosa_sql::describe(&stmt, &catalog(), "public").unwrap();
    assert_eq!(cols[0].name, "x");
    assert_eq!(cols[0].ty, ColumnType::Int);
    assert_eq!(cols[1].ty, ColumnType::Text);
    let params = ferrosa_sql::infer_param_types(&stmt, &catalog(), "public").unwrap();
    assert_eq!(
        params,
        vec![ColumnType::Array(ArrayElem::Text), ColumnType::Int]
    );
}

// -- LIMIT / OFFSET parameters (Limit.lean) ------------------------------------

#[test]
fn limit_and_offset_take_bound_parameters() {
    assert_eq!(
        select(
            "SELECT id FROM t ORDER BY id LIMIT $1 OFFSET $2",
            &[int(1), int(1)]
        ),
        vec![vec![int(2)]]
    );
    // LIMIT NULL is LIMIT ALL; OFFSET NULL is no offset.
    assert_eq!(
        select(
            "SELECT id FROM t ORDER BY id LIMIT $1 OFFSET $2",
            &[Value::Null, Value::Null]
        )
        .len(),
        3
    );
}

#[test]
fn a_negative_limit_or_offset_parameter_is_an_error() {
    let err = select_err("SELECT id FROM t LIMIT $1", &[int(-1)]);
    assert_eq!(sqlstate(&err), "2201W", "{err}");
    let err = select_err("SELECT id FROM t OFFSET $1", &[int(-1)]);
    assert_eq!(sqlstate(&err), "2201X", "{err}");
}

#[test]
fn limit_parameter_types_are_inferred_as_bigint() {
    let Statement::Select(stmt) =
        parse_statement("SELECT id FROM t WHERE a > $1 LIMIT $2").unwrap()
    else {
        panic!()
    };
    let params = ferrosa_sql::infer_param_types(&stmt, &catalog(), "public").unwrap();
    assert_eq!(params, vec![ColumnType::Int, ColumnType::BigInt]);
}

// -- IN / = ANY over a subquery (PostgreSQL defines IN (subquery) as = ANY) ------

#[test]
fn in_a_subquery_matches_any_row_it_returns() {
    assert_eq!(
        select(
            "SELECT id FROM t WHERE id IN (SELECT tid FROM u) ORDER BY id",
            &[]
        ),
        vec![vec![int(1)], vec![int(3)]]
    );
    assert_eq!(
        select(
            "SELECT id FROM t WHERE id = ANY (SELECT tid FROM u WHERE tag = 'green')",
            &[]
        ),
        vec![vec![int(3)]]
    );
    assert_eq!(
        select(
            "SELECT id FROM t WHERE id NOT IN (SELECT tid FROM u) ORDER BY id",
            &[]
        ),
        vec![vec![int(2)]]
    );
}

#[test]
fn not_in_a_subquery_that_returns_a_null_is_never_true() {
    // n is NULL on id 1: NOT IN over a set holding NULL keeps nothing
    // (Expr.lean not_in_with_null_never_true).
    assert_eq!(
        select("SELECT id FROM t WHERE a NOT IN (SELECT n FROM t)", &[]),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn in_a_limited_subquery_as_oban_prunes() {
    assert_eq!(
        select(
            "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE a > 1 ORDER BY id LIMIT 1)",
            &[]
        ),
        vec![vec![int(2)]]
    );
}
