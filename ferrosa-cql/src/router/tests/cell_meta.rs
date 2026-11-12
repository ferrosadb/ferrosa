//! `writetime()` and `TTL()` read the stored cell's own timestamp and expiry
//! on every SELECT shape that accepts them (t_7987e84c).
//!
//! The evaluator used to be dead code: the selectors parsed, were typed
//! bigint/int, and answered NULL for every row — a fake value an operator
//! repair tool read as "no timestamp". Every test here asserts the VALUE, and
//! the multi-row tests give each row a different timestamp so a constant
//! cannot pass. Shapes that cannot answer refuse by name.

use super::*;

/// Base `USING TIMESTAMP` of the seeded rows; row `i` is written at
/// `BASE + i * STEP`, so no two rows share a write time.
const BASE: i64 = 1_700_000_000_000_000;
const STEP: i64 = 1_000;

fn ts(i: i64) -> i64 {
    BASE + i * STEP
}

/// Keyspace `cm` with
/// - `kv (id int PRIMARY KEY, v int, w int)`: `n` rows, row `id` written at
///   `ts(id)` with `w = id % 5`, and an index on `w`;
/// - `wide (pk int, ck int, v int, l list<int>, fl frozen<list<int>>,
///   PRIMARY KEY (pk, ck))`: partitions 1 and 2, five rows each, row `ck` of
///   `pk` written at `ts(pk * 10 + ck)`.
async fn seed(n: i64) -> (SharedState, TempDir, AuthContext, Option<String>) {
    let (state, dir) = setup();
    let auth = dev_auth();
    let ks = Some("cm".to_string());
    let ctx = paging_ctx(&auth, &ks, None, None);
    for ddl in [
        "CREATE KEYSPACE cm WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "CREATE TABLE cm.kv (id int PRIMARY KEY, v int, w int)",
        "CREATE INDEX kv_w ON cm.kv (w)",
        "CREATE TABLE cm.wide (pk int, ck int, v int, l list<int>, \
         fl frozen<list<int>>, PRIMARY KEY (pk, ck))",
    ] {
        run_ddl(&state, &ctx, ddl).await;
    }
    for id in 0..n {
        run_ddl(
            &state,
            &ctx,
            &format!(
                "INSERT INTO cm.kv (id, v, w) VALUES ({id}, {}, {}) USING TIMESTAMP {}",
                id * 10,
                id % 5,
                ts(id)
            ),
        )
        .await;
    }
    for pk in [1i64, 2] {
        for ck in 0..5i64 {
            run_ddl(
                &state,
                &ctx,
                &format!(
                    "INSERT INTO cm.wide (pk, ck, v, fl) VALUES ({pk}, {ck}, {ck}, [{ck}]) \
                     USING TIMESTAMP {}",
                    ts(pk * 10 + ck)
                ),
            )
            .await;
        }
    }
    (state, dir, auth, ks)
}

async fn select_rows(
    state: &SharedState,
    auth: &AuthContext,
    ks: &Option<String>,
    cql: &str,
) -> Vec<Vec<Option<CqlValue>>> {
    let ctx = paging_ctx(auth, ks, None, None);
    let select = match crate::parser::parse(cql).unwrap() {
        Statement::Select(s) => s,
        other => panic!("expected select, got {other:?}"),
    };
    route_select_raw(state, &ctx, &select)
        .await
        .unwrap_or_else(|e| panic!("{cql}: {e}"))
        .rows
}

async fn select_err(
    state: &SharedState,
    auth: &AuthContext,
    ks: &Option<String>,
    cql: &str,
) -> String {
    let ctx = paging_ctx(auth, ks, None, None);
    match route(state, &ctx, crate::parser::parse(cql).unwrap()).await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("{cql} must be refused"),
    }
}

fn int_at(row: &[Option<CqlValue>], i: usize) -> i64 {
    match &row[i] {
        Some(CqlValue::Int(v)) => i64::from(*v),
        Some(CqlValue::Bigint(v)) => *v,
        other => panic!("expected an integer at column {i}, got {other:?}"),
    }
}

/// `(key, writetime)` pairs of `rows`, key at column 0 and writetime at 1,
/// sorted by key.
fn key_writetimes(rows: &[Vec<Option<CqlValue>>]) -> Vec<(i64, i64)> {
    let mut got: Vec<(i64, i64)> = rows.iter().map(|r| (int_at(r, 0), int_at(r, 1))).collect();
    got.sort_unstable();
    got
}

/// The step-1 case: a point read of a cell written `USING TIMESTAMP 1234567
/// AND TTL 1000` returns that timestamp and a remaining TTL in (0, 1000].
#[tokio::test]
async fn point_read_returns_the_cells_timestamp_and_remaining_ttl() {
    let (state, _dir, auth, ks) = seed(0).await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    run_ddl(
        &state,
        &ctx,
        "INSERT INTO cm.kv (id, v) VALUES (1, 10) USING TIMESTAMP 1234567 AND TTL 1000",
    )
    .await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT writetime(v), ttl(v) FROM cm.kv WHERE id = 1",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        Some(CqlValue::Bigint(1_234_567)),
        "writetime(v)"
    );
    let ttl = int_at(&rows[0], 1);
    assert!(
        (1..=1000).contains(&ttl),
        "ttl(v) = {ttl}, expected (0, 1000]"
    );
}

/// The same after a flush: the values come from the SSTable cells, not only
/// from the memtable.
#[tokio::test]
async fn point_read_after_flush_returns_the_sstable_cells_values() {
    let (state, _dir, auth, ks) = seed(3).await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    run_ddl(
        &state,
        &ctx,
        "UPDATE cm.kv USING TIMESTAMP 99 AND TTL 500 SET w = 7 WHERE id = 1",
    )
    .await;
    state
        .engine
        .flush(&ferrosa_storage::TableId::new("cm", "kv"))
        .unwrap();
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT writetime(v), writetime(w), ttl(v), ttl(w) FROM cm.kv WHERE id = 1",
    )
    .await;
    assert_eq!(rows.len(), 1);
    // v keeps its INSERT time; w is the UPDATE's — per cell, not per row. The
    // UPDATE at 99 lost to the INSERT at ts(1) for w, so w is still the
    // INSERT's value and time.
    assert_eq!(rows[0][0], Some(CqlValue::Bigint(ts(1))), "writetime(v)");
    assert_eq!(rows[0][1], Some(CqlValue::Bigint(ts(1))), "writetime(w)");
    assert_eq!(rows[0][2], None, "ttl(v): written without a TTL");
    assert_eq!(rows[0][3], None, "ttl(w): the TTL'd write lost");
}

/// Two columns of one row written at different times report their own.
#[tokio::test]
async fn each_column_reports_its_own_cells_time() {
    let (state, _dir, auth, ks) = seed(3).await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    let later = ts(1) + 777;
    run_ddl(
        &state,
        &ctx,
        &format!("UPDATE cm.kv USING TIMESTAMP {later} AND TTL 600 SET w = 9 WHERE id = 1"),
    )
    .await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT w, writetime(v), writetime(w), ttl(w) FROM cm.kv WHERE id = 1",
    )
    .await;
    assert_eq!(rows[0][0], Some(CqlValue::Int(9)));
    assert_eq!(rows[0][1], Some(CqlValue::Bigint(ts(1))), "writetime(v)");
    assert_eq!(rows[0][2], Some(CqlValue::Bigint(later)), "writetime(w)");
    let ttl = int_at(&rows[0], 3);
    assert!((1..=600).contains(&ttl), "ttl(w) = {ttl}");
}

/// A whole-partition read: every clustering row reports its own time.
#[tokio::test]
async fn partition_read_reports_each_rows_own_timestamp() {
    let (state, _dir, auth, ks) = seed(0).await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT ck, writetime(v) FROM cm.wide WHERE pk = 1",
    )
    .await;
    assert_eq!(
        key_writetimes(&rows),
        (0..5).map(|ck| (ck, ts(10 + ck))).collect::<Vec<_>>()
    );
}

/// A clustering-range slice.
#[tokio::test]
async fn clustering_slice_reports_each_rows_own_timestamp() {
    let (state, _dir, auth, ks) = seed(0).await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT ck, writetime(v) FROM cm.wide WHERE pk = 2 AND ck >= 1 AND ck < 4",
    )
    .await;
    assert_eq!(
        key_writetimes(&rows),
        (1..4).map(|ck| (ck, ts(20 + ck))).collect::<Vec<_>>()
    );
}

/// A single clustering row by its full primary key.
#[tokio::test]
async fn exact_clustering_row_reports_its_timestamp() {
    let (state, _dir, auth, ks) = seed(0).await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT ck, writetime(v), writetime(fl) FROM cm.wide WHERE pk = 2 AND ck = 3",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Some(CqlValue::Bigint(ts(23))), "writetime(v)");
    assert_eq!(
        rows[0][2],
        Some(CqlValue::Bigint(ts(23))),
        "writetime of a frozen collection is its one cell's"
    );
}

/// `pk IN (...)`: rows of several partitions, each with its own time.
#[tokio::test]
async fn partition_key_in_reports_each_rows_own_timestamp() {
    let (state, _dir, auth, ks) = seed(0).await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT pk, ck, writetime(v) FROM cm.wide WHERE pk IN (1, 2)",
    )
    .await;
    let mut got: Vec<(i64, i64, i64)> = rows
        .iter()
        .map(|r| (int_at(r, 0), int_at(r, 1), int_at(r, 2)))
        .collect();
    got.sort_unstable();
    let expected: Vec<(i64, i64, i64)> = [1i64, 2]
        .iter()
        .flat_map(|&pk| (0..5).map(move |ck| (pk, ck, ts(pk * 10 + ck))))
        .collect();
    assert_eq!(got, expected);
}

/// A partition read paged two rows at a time: every page carries real values.
#[tokio::test]
async fn paged_partition_read_reports_each_rows_own_timestamp() {
    let (state, _dir, auth, ks) = seed(0).await;
    let (rows, pages) = collect_all_pages(
        &state,
        &auth,
        &ks,
        "SELECT ck, writetime(v) FROM cm.wide WHERE pk = 1",
        Some(2),
    )
    .await;
    assert!(pages > 1, "the read must span pages, got {pages}");
    assert_eq!(
        key_writetimes(&rows),
        (0..5).map(|ck| (ck, ts(10 + ck))).collect::<Vec<_>>()
    );
}

/// An ALLOW FILTERING full scan (the result-cursor shape).
#[tokio::test]
async fn filtered_full_scan_reports_each_rows_own_timestamp() {
    let n = 40;
    let (state, _dir, auth, ks) = seed(n).await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT id, writetime(v) FROM cm.kv WHERE v >= 100 ALLOW FILTERING",
    )
    .await;
    assert_eq!(
        key_writetimes(&rows),
        (10..n).map(|id| (id, ts(id))).collect::<Vec<_>>()
    );
}

/// An unfiltered scan paged through the result cursor, page continuations
/// included.
#[tokio::test]
async fn paged_full_scan_reports_each_rows_own_timestamp() {
    let n = 50;
    let (state, _dir, auth, ks) = seed(n).await;
    let (rows, pages) = collect_all_pages(
        &state,
        &auth,
        &ks,
        "SELECT id, writetime(v) FROM cm.kv",
        Some(7),
    )
    .await;
    assert!(pages > 1, "the scan must span pages, got {pages}");
    assert_eq!(
        key_writetimes(&rows),
        (0..n).map(|id| (id, ts(id))).collect::<Vec<_>>()
    );
}

/// `ORDER BY` over a regular column: the sorted cursor carries the values.
#[tokio::test]
async fn ordered_scan_reports_each_rows_own_timestamp() {
    let n = 30;
    let (state, _dir, auth, ks) = seed(n).await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT id, writetime(v) FROM cm.kv ORDER BY v DESC",
    )
    .await;
    let got: Vec<(i64, i64)> = rows.iter().map(|r| (int_at(r, 0), int_at(r, 1))).collect();
    assert_eq!(got, (0..n).rev().map(|id| (id, ts(id))).collect::<Vec<_>>());
}

/// A secondary-index read.
#[tokio::test]
async fn index_read_reports_each_rows_own_timestamp() {
    let n = 25;
    let (state, _dir, auth, ks) = seed(n).await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT id, writetime(v) FROM cm.kv WHERE w = 3",
    )
    .await;
    assert_eq!(
        key_writetimes(&rows),
        (0..n)
            .filter(|id| id % 5 == 3)
            .map(|id| (id, ts(id)))
            .collect::<Vec<_>>()
    );
}

/// An `fts_match` read.
#[tokio::test]
async fn fulltext_read_reports_each_rows_own_timestamp() {
    let (state, _dir, auth, ks) = seed(0).await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    run_ddl(
        &state,
        &ctx,
        "CREATE TABLE cm.docs (id int PRIMARY KEY, body text)",
    )
    .await;
    run_ddl(
        &state,
        &ctx,
        "CREATE INDEX docs_fts ON cm.docs (body) USING 'fulltext'",
    )
    .await;
    for (id, body) in [
        (1, "distributed database"),
        (2, "a distributed log"),
        (3, "hello"),
    ] {
        run_ddl(
            &state,
            &ctx,
            &format!(
                "INSERT INTO cm.docs (id, body) VALUES ({id}, '{body}') USING TIMESTAMP {}",
                ts(id)
            ),
        )
        .await;
    }
    state
        .engine
        .flush(&ferrosa_storage::TableId::new("cm", "docs"))
        .unwrap();
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT id, writetime(body) FROM cm.docs WHERE body = fts_match('distributed')",
    )
    .await;
    assert_eq!(key_writetimes(&rows), vec![(1, ts(1)), (2, ts(2))]);
}

/// A geospatial read, k-NN and radius forms.
#[tokio::test]
async fn geo_read_reports_each_rows_own_timestamp() {
    let (state, _dir, auth, ks) = seed(0).await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    run_ddl(
        &state,
        &ctx,
        "CREATE TABLE cm.places (id int PRIMARY KEY, location frozen<tuple<double, double>>)",
    )
    .await;
    run_ddl(
        &state,
        &ctx,
        "CREATE INDEX places_geo ON cm.places (location) USING 'geo'",
    )
    .await;
    for (id, at) in [
        (1, "(37.7955, -122.3937)"),
        (2, "(37.7880, -122.4074)"),
        (3, "(40.7580, -73.9855)"),
    ] {
        run_ddl(
            &state,
            &ctx,
            &format!(
                "INSERT INTO cm.places (id, location) VALUES ({id}, {at}) USING TIMESTAMP {} AND TTL 900",
                ts(id)
            ),
        )
        .await;
    }
    let near = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT id, writetime(location), ttl(location) FROM cm.places \
         ORDER BY location GEO_NEAREST OF (37.7880, -122.4074) LIMIT 2",
    )
    .await;
    assert_eq!(key_writetimes(&near), vec![(1, ts(1)), (2, ts(2))]);
    for row in &near {
        let ttl = int_at(row, 2);
        assert!((1..=900).contains(&ttl), "ttl(location) = {ttl}");
    }
    let within = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT id, writetime(location) FROM cm.places \
         WHERE GEO_WITHIN_RADIUS(location, (37.7955, -122.3937), 3000)",
    )
    .await;
    assert_eq!(key_writetimes(&within), vec![(1, ts(1)), (2, ts(2))]);
}

/// NULL exactly where Cassandra answers NULL: an absent cell has no write
/// time, and a cell written without a TTL has no TTL.
#[tokio::test]
async fn an_absent_cell_and_an_unexpiring_cell_answer_null() {
    let (state, _dir, auth, ks) = seed(0).await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    run_ddl(
        &state,
        &ctx,
        "INSERT INTO cm.kv (id, v) VALUES (1, 10) USING TIMESTAMP 5000",
    )
    .await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT writetime(v), ttl(v), writetime(w), ttl(w) FROM cm.kv WHERE id = 1",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![Some(CqlValue::Bigint(5000)), None, None, None]]
    );
}

/// The selector fills its own slot, positionally: an alias equal to a real
/// column's name, or two selectors sharing a display name, cannot swap values.
#[tokio::test]
async fn aliases_and_repeated_selectors_fill_their_own_slots() {
    let (state, _dir, auth, ks) = seed(3).await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    run_ddl(
        &state,
        &ctx,
        &format!(
            "UPDATE cm.kv USING TIMESTAMP {} SET w = 1 WHERE id = 2",
            ts(2) + 5
        ),
    )
    .await;
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT v, writetime(v) AS v, writetime(w), writetime(v) FROM cm.kv WHERE id = 2",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            Some(CqlValue::Int(20)),
            Some(CqlValue::Bigint(ts(2))),
            Some(CqlValue::Bigint(ts(2) + 5)),
            Some(CqlValue::Bigint(ts(2))),
        ]]
    );
}

/// The prepared-SELECT fast path emits stored cells without their metadata,
/// so it must decline a `writetime()` read and leave it to the general path.
#[tokio::test]
async fn the_prepared_fast_path_declines_a_cell_metadata_read() {
    let (state, _dir, auth, ks) = seed(4).await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    let select =
        match crate::parser::parse("SELECT id, writetime(v) FROM cm.kv WHERE id = ?").unwrap() {
            Statement::Select(s) => s,
            other => panic!("expected select, got {other:?}"),
        };
    assert!(
        route_prepared_select_fast(&state, &ctx, &select, &[Term::IntegerLiteral(3)])
            .await
            .is_none(),
        "the fast path cannot answer writetime()"
    );
    let rows = select_rows(
        &state,
        &auth,
        &ks,
        "SELECT id, writetime(v) FROM cm.kv WHERE id = 3",
    )
    .await;
    assert_eq!(key_writetimes(&rows), vec![(3, ts(3))]);
}

/// Cassandra refuses `writetime()` / `TTL()` of a primary-key column: it has
/// no cell. That is an error, not NULL.
#[tokio::test]
async fn a_primary_key_column_is_refused() {
    let (state, _dir, auth, ks) = seed(1).await;
    for (cql, column) in [
        ("SELECT writetime(id) FROM cm.kv WHERE id = 0", "id"),
        ("SELECT ttl(pk) FROM cm.wide WHERE pk = 1", "pk"),
        ("SELECT writetime(ck) FROM cm.wide WHERE pk = 1", "ck"),
    ] {
        let err = select_err(&state, &auth, &ks, cql).await;
        assert!(
            err.contains("PRIMARY KEY part") && err.contains(column),
            "{cql}: {err}"
        );
    }
}

/// A non-frozen collection has one cell per element, so no single write time.
#[tokio::test]
async fn a_non_frozen_collection_is_refused_by_name() {
    let (state, _dir, auth, ks) = seed(0).await;
    let err = select_err(
        &state,
        &auth,
        &ks,
        "SELECT writetime(l) FROM cm.wide WHERE pk = 1",
    )
    .await;
    assert!(
        err.contains("writetime() is not supported for non-frozen collection column l"),
        "{err}"
    );
}

/// An aggregate folds rows, so a per-row write time has no row to come from.
#[tokio::test]
async fn an_aggregate_query_is_refused_by_name() {
    let (state, _dir, auth, ks) = seed(3).await;
    for cql in [
        "SELECT count(*), writetime(v) FROM cm.kv",
        "SELECT max(v), ttl(w) FROM cm.kv WHERE id = 1",
    ] {
        let err = select_err(&state, &auth, &ks, cql).await;
        assert!(
            err.contains("is not supported for an aggregate query"),
            "{cql}: {err}"
        );
    }
}

/// System tables are synthesized and virtual tables have no cells.
#[tokio::test]
async fn a_system_table_is_refused_by_name() {
    let (state, _dir, auth, ks) = seed(0).await;
    let err = select_err(
        &state,
        &auth,
        &ks,
        "SELECT writetime(cluster_name) FROM system.local",
    )
    .await;
    assert!(
        err.contains("writetime() is not supported for system or virtual table system.local"),
        "{err}"
    );
}

/// An unknown column is an error naming it.
#[tokio::test]
async fn an_unknown_column_is_refused() {
    let (state, _dir, auth, ks) = seed(1).await;
    let err = select_err(
        &state,
        &auth,
        &ks,
        "SELECT ttl(nope) FROM cm.kv WHERE id = 0",
    )
    .await;
    assert!(err.contains("Undefined column name nope"), "{err}");
}

/// The safety net: a row that reaches the projection without the decoded
/// metadata (a read path that skipped it) is a loud error, never NULL.
#[test]
fn a_row_without_cell_metadata_is_refused_not_null() {
    let all = vec!["id".to_string(), "v".to_string()];
    let selected = vec!["writetime".to_string()];
    let cell_meta = CellMetaProjection {
        columns: vec![CellMetaColumn {
            select_idx: 0,
            func: CellMetaFn::Writetime,
            source: 1,
        }],
    };
    let short_row = vec![vec![Some(CqlValue::Int(1)), Some(CqlValue::Int(2))]];
    let err = select_result_columns(&short_row, &all, &selected, &cell_meta)
        .expect_err("a row without cell metadata must not project");
    assert!(err.to_string().contains("without cell metadata"), "{err}");
    let full_row = vec![vec![
        Some(CqlValue::Int(1)),
        Some(CqlValue::Int(2)),
        Some(CqlValue::Bigint(42)),
    ]];
    assert_eq!(
        select_result_columns(&full_row, &all, &selected, &cell_meta).unwrap(),
        vec![vec![Some(CqlValue::Bigint(42))]]
    );
}
