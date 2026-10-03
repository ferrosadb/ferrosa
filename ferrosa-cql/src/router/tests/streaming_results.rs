//! Paging through results that cannot be produced in scan order.
//!
//! `ORDER BY` over a non-clustering column needs every row before the first
//! is known; `DISTINCT` over an arbitrary projection needs to remember what it
//! has emitted. Both used to collect the whole result and slice a page out of
//! it by offset, re-running the scan for every page. These tests pin the
//! replacement: a server-side result cursor that is built once and then read
//! a page at a time.

use super::*;

/// Keyspace `rc`, table `t (id int PRIMARY KEY, v int, w int)` with `n` rows.
/// `v` is a permutation of `0..n` unrelated to token order; `w = v % 7`.
async fn seed(n: usize) -> (SharedState, TempDir, AuthContext, Option<String>) {
    let (state, dir) = setup();
    let auth = dev_auth();
    let ks = Some("rc".to_string());
    let ctx = paging_ctx(&auth, &ks, None, None);
    run_ddl(
        &state,
        &ctx,
        "CREATE KEYSPACE rc WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
    )
    .await;
    run_ddl(
        &state,
        &ctx,
        "CREATE TABLE rc.t (id int PRIMARY KEY, v int, w int)",
    )
    .await;
    for id in 0..n {
        let v = (id * 7_919) % n;
        run_ddl(
            &state,
            &ctx,
            &format!("INSERT INTO rc.t (id, v, w) VALUES ({id}, {v}, {})", v % 7),
        )
        .await;
    }
    (state, dir, auth, ks)
}

fn int_col(rows: &[Vec<Option<CqlValue>>], idx: usize) -> Vec<i32> {
    rows.iter()
        .map(|r| match &r[idx] {
            Some(CqlValue::Int(x)) => *x,
            other => panic!("expected int, got {other:?}"),
        })
        .collect()
}

/// Every page of an `ORDER BY` walk comes from ONE scan of the table. The
/// offset-sliced version re-ran the scan per page: 20 pages of 50 over 1,000
/// rows pulled 20,000 rows from storage.
#[tokio::test]
async fn order_by_paging_scans_the_table_once() {
    let n = 1_000usize;
    let (state, _dir, auth, ks) = seed(n).await;
    FULL_SCAN_ROWS_PULLED.with(|c| c.set(0));
    let (rows, pages) = collect_all_pages(
        &state,
        &auth,
        &ks,
        "SELECT id, v FROM rc.t ORDER BY v",
        Some(50),
    )
    .await;
    let pulled = FULL_SCAN_ROWS_PULLED.with(|c| c.get());

    assert_eq!(int_col(&rows, 1), (0..n as i32).collect::<Vec<_>>());
    assert_eq!(pages, n / 50);
    assert!(
        pulled <= n,
        "paging {pages} pages pulled {pulled} rows from storage for a {n}-row table: \
         each page re-ran the scan (O(N^2/page)) instead of reading a cursor"
    );
}

/// Same for a per-row function projection, which cannot use the scan-order
/// page path because aggregates share its syntax.
#[tokio::test]
async fn function_projection_paging_scans_the_table_once() {
    let n = 600usize;
    let (state, _dir, auth, ks) = seed(n).await;
    FULL_SCAN_ROWS_PULLED.with(|c| c.set(0));
    let (rows, _pages) = collect_all_pages(
        &state,
        &auth,
        &ks,
        "SELECT id, writetime(v) FROM rc.t",
        Some(50),
    )
    .await;
    let pulled = FULL_SCAN_ROWS_PULLED.with(|c| c.get());

    let mut ids = int_col(&rows, 0);
    ids.sort_unstable();
    assert_eq!(
        ids,
        (0..n as i32).collect::<Vec<_>>(),
        "every row exactly once"
    );
    assert!(
        pulled <= n,
        "paging pulled {pulled} rows from storage for a {n}-row table"
    );
}

/// `DISTINCT` over a non-key column, paged: every distinct value exactly once.
/// The offset path read only `page_size` PARTITIONS per page and deduplicated
/// those, so a page of 3 over 7 distinct values returned 3 and stopped.
#[tokio::test]
async fn distinct_paging_returns_every_distinct_value() {
    let (state, _dir, auth, ks) = seed(300).await;
    let (rows, _pages) =
        collect_all_pages(&state, &auth, &ks, "SELECT DISTINCT w FROM rc.t", Some(3)).await;
    let mut got = int_col(&rows, 0);
    got.sort_unstable();
    assert_eq!(
        got,
        (0..7).collect::<Vec<_>>(),
        "each distinct w exactly once"
    );
}

/// `ORDER BY ... LIMIT` spans pages: the limit counts rows across the whole
/// walk, not per page.
#[tokio::test]
async fn order_by_limit_holds_across_pages() {
    let (state, _dir, auth, ks) = seed(500).await;
    let (rows, _pages) = collect_all_pages(
        &state,
        &auth,
        &ks,
        "SELECT v FROM rc.t ORDER BY v DESC LIMIT 120",
        Some(50),
    )
    .await;
    assert_eq!(
        int_col(&rows, 0),
        (380..500).rev().collect::<Vec<_>>(),
        "the top 120 values, descending, across three pages"
    );
}

async fn first_order_by_page(
    state: &SharedState,
    auth: &AuthContext,
    ks: &Option<String>,
) -> (SelectStatement, Vec<u8>) {
    let select = match crate::parser::parse("SELECT id, v FROM rc.t ORDER BY v").unwrap() {
        Statement::Select(s) => s,
        other => panic!("expected select, got {other:?}"),
    };
    let ctx = paging_ctx(auth, ks, Some(50), None);
    let page = route_select_raw(state, &ctx, &select).await.unwrap();
    assert_eq!(page.rows.len(), 50);
    let token = page.paging_state.expect("more pages remain");
    (select, token)
}

/// An idle cursor expires: its spill directory is deleted, and the client's
/// next page is a clear error, not a restart and not an empty "last" page.
#[tokio::test]
async fn an_expired_cursor_is_deleted_and_its_paging_state_errors() {
    let (state, _dir, auth, ks) = seed(400).await;
    let (select, token) = first_order_by_page(&state, &auth, &ks).await;
    let dirs = state.result_cursors.parked_spill_dirs();
    assert_eq!(dirs.len(), 1);
    assert!(dirs[0].exists());

    let ttl = state.result_cursors.config().idle_ttl;
    let later = std::time::Instant::now() + ttl + std::time::Duration::from_secs(1);
    assert_eq!(state.result_cursors.sweep_expired_at(later), 1);
    assert!(!dirs[0].exists(), "expiry must delete the spill directory");
    assert_eq!(state.result_cursors.stats().open, 0);

    let ctx = paging_ctx(&auth, &ks, Some(50), Some(token));
    let err = match route_select_raw(&state, &ctx, &select).await {
        Ok(page) => panic!("an expired cursor served {} rows", page.rows.len()),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("no longer holds"), "{err}");
}

/// The connection that parked a cursor closes: the cursor goes with it.
#[tokio::test]
async fn closing_the_connection_deletes_its_cursor() {
    let (state, _dir, auth, ks) = seed(400).await;
    let (select, token) = first_order_by_page(&state, &auth, &ks).await;
    let dirs = state.result_cursors.parked_spill_dirs();
    // `paging_ctx` leaves `client_address` empty; that is the owner key.
    assert_eq!(state.result_cursors.close_owner(""), 1);
    assert!(!dirs[0].exists());
    let ctx = paging_ctx(&auth, &ks, Some(50), Some(token));
    assert!(route_select_raw(&state, &ctx, &select).await.is_err());
}

/// A paging state from before the cursor (an offset token from an older
/// server, or any scan-position state) gets a clear error on a cursor query.
#[tokio::test]
async fn a_pre_cursor_paging_state_is_refused_by_name() {
    let (state, _dir, auth, ks) = seed(100).await;
    let select = match crate::parser::parse("SELECT id, v FROM rc.t ORDER BY v").unwrap() {
        Statement::Select(s) => s,
        other => panic!("expected select, got {other:?}"),
    };
    let legacy = crate::paging::PagingState {
        partition_key: 50u64.to_be_bytes().to_vec(),
        clustering_key: Vec::new(),
        remaining_in_partition: false,
    }
    .encode();
    let ctx = paging_ctx(&auth, &ks, Some(50), Some(legacy));
    let err = match route_select_raw(&state, &ctx, &select).await {
        Ok(page) => panic!("a legacy paging state served {} rows", page.rows.len()),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("not a result-cursor token"), "{err}");
}

/// A query cancelled while it builds its cursor (the request future is
/// dropped mid-scan) leaves no spill directory and no open-cursor slot.
#[tokio::test]
async fn a_cancelled_first_page_leaves_nothing_behind() {
    let (state, _dir, auth, ks) = seed(3_000).await;
    let probe = state
        .engine
        .reserve_order_by_temp_sort_table("rc", "t")
        .unwrap();
    let root = probe.path().parent().unwrap().to_path_buf();
    drop(probe);
    let entries = |root: &std::path::Path| std::fs::read_dir(root).map(|d| d.count()).unwrap_or(0);
    assert_eq!(entries(&root), 0);

    let select = match crate::parser::parse("SELECT id, v FROM rc.t ORDER BY v").unwrap() {
        Statement::Select(s) => s,
        other => panic!("expected select, got {other:?}"),
    };
    let ctx = paging_ctx(&auth, &ks, Some(50), None);
    // Boxed, so `drop(fut)` drops the future itself (a `pin!`ned future
    // would only drop its `Pin<&mut>` and live on to the end of the scope).
    let mut fut = Box::pin(route_select_raw(&state, &ctx, &select));
    let mut building = false;
    for _ in 0..10_000 {
        assert!(
            futures::poll!(fut.as_mut()).is_pending(),
            "the first page finished before it could be cancelled; seed more rows"
        );
        if entries(&root) > 0 {
            building = true;
            break;
        }
    }
    assert!(building, "the cursor never reserved its spill directory");
    assert_eq!(state.result_cursors.stats().open, 1);
    drop(fut);
    let left: Vec<_> = std::fs::read_dir(&root)
        .map(|d| d.filter_map(|e| e.ok()).map(|e| e.path()).collect())
        .unwrap_or_default();
    assert!(
        left.is_empty(),
        "a cancelled build must remove its directory: {left:?}"
    );
    assert_eq!(state.result_cursors.stats().open, 0);
}
