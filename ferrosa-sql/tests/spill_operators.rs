//! Blocking-operator spill invariants (forge t_50d99192).
//!
//! `sort`, `hash_aggregate`, `hash_join` and DISTINCT dedup are *blocking*
//! operators: none of them can emit its first output row before it has consumed
//! its whole input, so pipelining is unavailable and they used to buffer the
//! entire input (and, for the join, the entire `left x right` output) in memory.
//!
//! The owner decision on t_50d99192 is that these SPILL rather than cap: a bound
//! on a RESULT turns a legitimate query into a failure. The invariant every test
//! here asserts is therefore the same one, per operator:
//!
//! > Given an input larger than the in-memory threshold, the operator returns
//! > EVERY row via spill — it never truncates, never refuses, and its peak
//! > resident row count stays far below the row count it processed.

use std::sync::Arc;

use ferrosa_sql::exec::{
    dedup, hash_aggregate, hash_join, sort, AggFunc, SortDir, SortKey, TryRowStream,
};
use ferrosa_sql::spill::{DirReserver, SpillCtx};
use ferrosa_sql::types::{Row, Value};

/// The resident-bytes budget these tests give an operator: small enough that
/// every one of them spills many runs and cascade-merges them, large enough that
/// a run holds a handful of rows rather than exactly one.
const TINY_THRESHOLD: u64 = 256;

/// A context whose threshold forces the spill+merge path rather than the
/// in-memory fast path.
fn spilling_ctx(root: &std::path::Path) -> SpillCtx {
    SpillCtx::new(Arc::new(DirReserver::new(root)), TINY_THRESHOLD)
}

fn row(vals: Vec<Value>) -> Row {
    Row::new(vals)
}

fn stream(rows: Vec<Row>) -> TryRowStream<'static> {
    Box::new(rows.into_iter().map(Ok))
}

fn drain(s: TryRowStream<'_>) -> Vec<Row> {
    s.collect::<Result<Vec<Row>, _>>().expect("no spill error")
}

/// Enough rows that the tiny threshold forces many runs and a cascade merge.
const BIG: i64 = 4_000;

#[test]
fn sort_returns_every_row_when_forced_to_spill() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = spilling_ctx(dir.path());

    // Descending input, ascending sort: nothing is already in order.
    let rows: Vec<Row> = (0..BIG).rev().map(|i| row(vec![Value::Int(i)])).collect();
    let sorted = drain(
        sort(
            stream(rows),
            &[SortKey {
                col: 0,
                dir: SortDir::Asc,
            }],
            &ctx,
        )
        .unwrap(),
    );

    assert_eq!(sorted.len() as i64, BIG, "sort must not drop or truncate");
    let got: Vec<i64> = sorted
        .iter()
        .map(|r| match r.get(0) {
            Value::Int(n) => *n,
            v => panic!("unexpected {v:?}"),
        })
        .collect();
    assert_eq!(got, (0..BIG).collect::<Vec<_>>());
    assert!(
        ctx.stats().spilled(),
        "the tiny threshold must force the spill path"
    );
    assert!(
        ctx.stats().max_resident_rows() < BIG as usize / 10,
        "peak resident rows {} must stay an order of magnitude below the {BIG} rows sorted",
        ctx.stats().max_resident_rows()
    );
}

#[test]
fn sort_is_stable_across_spilled_runs() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = spilling_ctx(dir.path());

    // Every row shares one sort key, so the output order is entirely decided by
    // stability — a k-way merge that broke ties arbitrarily would scramble it.
    let rows: Vec<Row> = (0..BIG)
        .map(|i| row(vec![Value::Int(0), Value::Int(i)]))
        .collect();
    let sorted = drain(
        sort(
            stream(rows),
            &[SortKey {
                col: 0,
                dir: SortDir::Asc,
            }],
            &ctx,
        )
        .unwrap(),
    );
    let tiebreak: Vec<i64> = sorted
        .iter()
        .map(|r| match r.get(1) {
            Value::Int(n) => *n,
            v => panic!("unexpected {v:?}"),
        })
        .collect();
    assert_eq!(tiebreak, (0..BIG).collect::<Vec<_>>());
}

#[test]
fn hash_aggregate_returns_every_group_when_forced_to_spill() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = spilling_ctx(dir.path());

    // BIG distinct groups, two rows each: the group table alone was proportional
    // to the input.
    let mut rows = Vec::new();
    for i in 0..BIG {
        rows.push(row(vec![Value::Int(i), Value::Int(1)]));
        rows.push(row(vec![Value::Int(i), Value::Int(2)]));
    }
    let out = drain(
        hash_aggregate(
            stream(rows),
            &[0],
            &[(AggFunc::Count, None), (AggFunc::Sum, Some(1))],
            &ctx,
        )
        .unwrap(),
    );

    assert_eq!(out.len() as i64, BIG, "every group must be returned");
    // First-seen group order is preserved, and each group aggregates both rows.
    for (i, r) in out.iter().enumerate() {
        assert_eq!(r.get(0), &Value::Int(i as i64));
        assert_eq!(r.get(1), &Value::Int(2));
        assert_eq!(r.get(2), &Value::Int(3));
    }
    assert!(
        ctx.stats().spilled(),
        "the tiny threshold must force the spill path"
    );
    assert!(
        ctx.stats().max_resident_rows() < BIG as usize / 10,
        "peak resident rows {} must stay an order of magnitude below the {BIG} groups",
        ctx.stats().max_resident_rows()
    );
}

#[test]
fn hash_aggregate_keeps_distinct_group_keys_of_different_types_apart() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = spilling_ctx(dir.path());

    // The group table keyed on structural equality, where Int(1) and Text("1")
    // are different keys. A sort-based aggregate that ordered groups with the
    // SQL comparator (which calls mixed types UNKNOWN, i.e. Equal) would merge
    // them, so grouping must use a type-aware total order.
    let rows = vec![
        row(vec![Value::Int(1)]),
        row(vec![Value::Text("1".into())]),
        row(vec![Value::Null]),
        row(vec![Value::Int(1)]),
    ];
    let out = drain(hash_aggregate(stream(rows), &[0], &[(AggFunc::Count, None)], &ctx).unwrap());

    assert_eq!(
        out.len(),
        3,
        "Int(1), Text(\"1\") and NULL are three groups"
    );
    assert_eq!(out[0].0, vec![Value::Int(1), Value::Int(2)]);
    assert_eq!(out[1].0, vec![Value::Text("1".into()), Value::Int(1)]);
    assert_eq!(out[2].0, vec![Value::Null, Value::Int(1)]);
}

#[test]
fn hash_join_under_key_skew_returns_every_pair_when_forced_to_spill() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = spilling_ctx(dir.path());

    // The worst site: one join key on both sides, so the output is the full
    // N x N cross product. The old operator built the entire right stream into a
    // HashMap AND accumulated every output row.
    const N: i64 = 120;
    let left: Vec<Row> = (0..N)
        .map(|i| row(vec![Value::Int(7), Value::Int(i)]))
        .collect();
    let right: Vec<Row> = (0..N)
        .map(|i| row(vec![Value::Int(7), Value::Int(i)]))
        .collect();

    let out = drain(hash_join(stream(left), stream(right), 0, 0, &ctx).unwrap());

    assert_eq!(
        out.len() as i64,
        N * N,
        "every left x right pair must be emitted"
    );
    // Output order matches the in-memory operator: left input order, then right.
    assert_eq!(out[0].0[1], Value::Int(0));
    assert_eq!(out[0].0[3], Value::Int(0));
    assert_eq!(out[1].0[3], Value::Int(1));
    assert_eq!(out[N as usize].0[1], Value::Int(1));
    assert!(
        ctx.stats().spilled(),
        "the tiny threshold must force the spill path"
    );
    assert!(
        ctx.stats().max_resident_rows() < N as usize,
        "peak resident rows {} must stay below even ONE side ({N} rows), let alone \
         the {} emitted pairs",
        ctx.stats().max_resident_rows(),
        N * N
    );
}

#[test]
fn hash_join_null_keys_never_match_under_spill() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = spilling_ctx(dir.path());
    let left = vec![row(vec![Value::Null]), row(vec![Value::Int(1)])];
    let right = vec![row(vec![Value::Null]), row(vec![Value::Int(1)])];
    let out = drain(hash_join(stream(left), stream(right), 0, 0, &ctx).unwrap());
    assert_eq!(out.len(), 1, "only the non-NULL key pairs");
}

#[test]
fn dedup_returns_every_distinct_row_when_forced_to_spill() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = spilling_ctx(dir.path());

    // Each value appears three times; first-occurrence order must survive.
    let mut rows = Vec::new();
    for _ in 0..3 {
        for i in 0..BIG {
            rows.push(row(vec![Value::Int(i)]));
        }
    }
    let out = drain(dedup(stream(rows), &ctx).unwrap());

    assert_eq!(out.len() as i64, BIG, "every distinct row must be returned");
    let got: Vec<i64> = out
        .iter()
        .map(|r| match r.get(0) {
            Value::Int(n) => *n,
            v => panic!("unexpected {v:?}"),
        })
        .collect();
    assert_eq!(
        got,
        (0..BIG).collect::<Vec<_>>(),
        "DISTINCT keeps first-occurrence order"
    );
    assert!(
        ctx.stats().spilled(),
        "the tiny threshold must force the spill path"
    );
    assert!(
        ctx.stats().max_resident_rows() < BIG as usize / 10,
        "peak resident rows {} must stay an order of magnitude below the {BIG} distinct rows",
        ctx.stats().max_resident_rows()
    );
}

#[test]
fn spilled_temp_state_is_removed_when_a_stream_is_dropped_mid_iteration() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = spilling_ctx(dir.path());
    let rows: Vec<Row> = (0..BIG).rev().map(|i| row(vec![Value::Int(i)])).collect();

    let mut stream = sort(
        stream(rows),
        &[SortKey {
            col: 0,
            dir: SortDir::Asc,
        }],
        &ctx,
    )
    .unwrap();

    // Pull one row, then abandon the query the way a cancelled session does.
    assert!(stream.next().is_some());
    let reserved = ctx.stats().reserved_paths();
    assert_eq!(reserved.len(), 1);
    assert!(
        reserved[0].exists(),
        "runs live while the stream can be read"
    );

    drop(stream);
    assert!(
        !reserved[0].exists(),
        "dropping a spilling stream must remove its temp-sort directory"
    );
}

#[test]
fn a_spill_io_failure_fails_loud_rather_than_truncating() {
    // A reserver pointed at a path that cannot hold a directory: the operator
    // must surface the error, never quietly return a short result.
    let file = tempfile::NamedTempFile::new().unwrap();
    let ctx = SpillCtx::new(Arc::new(DirReserver::new(file.path())), TINY_THRESHOLD);
    let rows: Vec<Row> = (0..8).map(|i| row(vec![Value::Int(i)])).collect();

    let err = sort(
        stream(rows),
        &[SortKey {
            col: 0,
            dir: SortDir::Asc,
        }],
        &ctx,
    )
    .err()
    .expect("reserving a temp dir under a regular file must fail loud");
    assert!(
        format!("{err}").contains("spill"),
        "error must name the spill path: {err}"
    );
}

/// The operators are wired into the query path, not just callable in isolation:
/// a whole `SELECT ... JOIN ... GROUP BY ... ORDER BY` runs through the spilling
/// operators and still returns every row it should.
#[test]
fn a_whole_query_spills_and_still_returns_every_row() {
    use ferrosa_sql::{
        execute_with, parse, Column, ColumnType, InMemoryTable, MapCatalog, RelSchema,
    };

    const N: i64 = 400;
    let users = InMemoryTable::new(
        RelSchema::new(vec![
            Column::new("id", ColumnType::Int),
            Column::new("name", ColumnType::Text),
        ]),
        (0..N)
            .map(|i| row(vec![Value::Int(i), Value::Text(format!("user{i}"))]))
            .collect(),
    );
    // Two orders per user, so the join output is 2N rows and every group has two.
    let orders = InMemoryTable::new(
        RelSchema::new(vec![
            Column::new("oid", ColumnType::Int),
            Column::new("uid", ColumnType::Int),
        ]),
        (0..2 * N)
            .map(|i| row(vec![Value::Int(i), Value::Int(i % N)]))
            .collect(),
    );
    let catalog = MapCatalog::new()
        .with_table("public", "users", std::sync::Arc::new(users))
        .with_table("public", "orders", std::sync::Arc::new(orders));

    let dir = tempfile::tempdir().unwrap();
    let ctx = spilling_ctx(dir.path());
    let stmt = parse(
        "SELECT u.id, COUNT(*) FROM users u JOIN orders o ON u.id = o.uid \
         GROUP BY u.id ORDER BY u.id",
    )
    .expect("parses");
    let result = execute_with(&stmt, &catalog, "public", &[], &ctx).expect("executes");

    assert_eq!(
        result.rows.len() as i64,
        N,
        "one group per user, none dropped"
    );
    for (i, r) in result.rows.iter().enumerate() {
        assert_eq!(r.get(0), &Value::Int(i as i64));
        assert_eq!(r.get(1), &Value::Int(2));
    }
    assert!(
        ctx.stats().spilled(),
        "the query must have taken the spill path"
    );
    assert!(
        ctx.stats().max_resident_rows() < N as usize,
        "peak resident rows {} must stay below the {N} groups",
        ctx.stats().max_resident_rows()
    );
}
