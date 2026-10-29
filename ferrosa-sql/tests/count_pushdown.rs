//! Count capability dispatch must preserve SQL shape, errors and pagination.
use ferrosa_sql::{
    Column, ColumnType, MapCatalog, RelSchema, Row, Statement, TableProvider, Value,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Source {
    schema: RelSchema,
    count: Option<Result<u64, String>>,
    calls: Arc<AtomicUsize>,
    scans: Arc<AtomicUsize>,
}
impl TableProvider for Source {
    fn schema(&self) -> &RelSchema {
        &self.schema
    }
    fn count_all(&self) -> Option<Result<u64, String>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.count.clone()
    }
    fn scan(&self) -> Box<dyn Iterator<Item = Row> + Send> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        Box::new(
            [Value::Int(1), Value::Null, Value::Int(2)]
                .into_iter()
                .map(|v| Row(vec![v])),
        )
    }
}
fn run(
    sql: &str,
    count: Option<Result<u64, String>>,
) -> (
    Result<ferrosa_sql::QueryResult, ferrosa_sql::ExecError>,
    usize,
    usize,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let scans = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(Source {
        schema: RelSchema::new(vec![Column::new("id", ColumnType::Int)]),
        count,
        calls: calls.clone(),
        scans: scans.clone(),
    });
    let catalog = MapCatalog::new().with_table("public", "t", source);
    let Statement::Select(stmt) = ferrosa_sql::parse_statement(sql).unwrap() else {
        panic!("SELECT")
    };
    let result = ferrosa_sql::execute(&stmt, &catalog, "public", &[]);
    (
        result,
        calls.load(Ordering::Relaxed),
        scans.load(Ordering::Relaxed),
    )
}
#[test]
fn count_pushdown_skips_rows_and_preserves_alias_and_bigint_type() {
    let (result, calls, scans) = run("SELECT COUNT(*) AS n FROM t", Some(Ok(3)));
    let result = result.unwrap();
    assert_eq!(result.rows, vec![Row(vec![Value::Int(3)])]);
    assert_eq!(result.columns[0].name, "n");
    assert_eq!(result.columns[0].ty, ColumnType::BigInt);
    assert_eq!((calls, scans), (1, 0));
}
#[test]
fn count_pushdown_retains_empty_result_and_limit_offset_semantics() {
    let (result, _, scans) = run("SELECT COUNT(*) FROM t", Some(Ok(0)));
    assert_eq!(result.unwrap().rows, vec![Row(vec![Value::Int(0)])]);
    assert_eq!(scans, 0);
    for sql in [
        "SELECT COUNT(*) FROM t LIMIT 0",
        "SELECT COUNT(*) FROM t LIMIT 1 OFFSET 1",
    ] {
        let (result, _, scans) = run(sql, Some(Ok(3)));
        assert!(result.unwrap().rows.is_empty());
        assert_eq!(scans, 0);
    }
}
#[test]
fn count_pushdown_errors_never_fallback_or_become_zero() {
    let (result, calls, scans) = run(
        "SELECT COUNT(*) FROM t",
        Some(Err("replica unavailable".into())),
    );
    assert!(
        matches!(result, Err(ferrosa_sql::ExecError::Source(message)) if message == "replica unavailable")
    );
    assert_eq!((calls, scans), (1, 0));
    let (result, _, scans) = run("SELECT COUNT(*) FROM t", Some(Ok(u64::MAX)));
    assert!(matches!(
        result,
        Err(ferrosa_sql::ExecError::AggregateOutOfRange(_))
    ));
    assert_eq!(scans, 0);
}
#[test]
fn count_pushdown_unsupported_capability_and_query_shapes_scan_correctly() {
    let (result, calls, scans) = run("SELECT COUNT(*) FROM t", None);
    assert_eq!(result.unwrap().rows, vec![Row(vec![Value::Int(3)])]);
    assert_eq!((calls, scans), (1, 1));
    for (sql, expected) in [
        ("SELECT COUNT(id) FROM t", 2),
        ("SELECT COUNT(*) FROM t WHERE id = 1", 1),
        ("SELECT COUNT(*) FROM t HAVING COUNT(*) > 1", 3),
        ("SELECT DISTINCT COUNT(*) FROM t", 3),
        ("SELECT COUNT(*) AS n FROM t ORDER BY n", 3),
    ] {
        let (result, calls, scans) = run(sql, Some(Ok(999)));
        assert_eq!(
            result.unwrap().rows,
            vec![Row(vec![Value::Int(expected)])],
            "{sql}"
        );
        assert_eq!((calls, scans), (0, 1), "{sql}");
    }
}
