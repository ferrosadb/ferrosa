//! A client fetch size bounds one page, while LIMIT bounds the whole result.
//!
//! The live regression returned exactly 5,000 of 20,000 narrow partitions:
//! the scan stopped at the fetch size and the paginator mistook that bounded
//! window for an exhausted result. Small fixtures exercise the same boundary.

use super::*;

#[tokio::test]
async fn limited_scan_cursor_cannot_be_misread_as_a_legacy_offset() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    let query = "SELECT v, id FROM pageks.t LIMIT 20";
    let Statement::Select(select) = crate::parser::parse(query).unwrap() else {
        panic!("expected SELECT");
    };
    let (expected, _) = collect_all_pages(&state, &auth, &ks, query, None).await;
    let ctx = paging_ctx(&auth, &ks, Some(7), None);
    let first = route_select_raw(&state, &ctx, &select).await.unwrap();
    let cursor = first.paging_state.expect("LIMIT 20 needs another page");

    let legacy_pk_len = u32::from_be_bytes(cursor[..4].try_into().unwrap()) as usize;
    assert!(
        legacy_pk_len > cursor.len(),
        "even an older decoder without new-token checks must reject the key length"
    );
    assert!(
        crate::paging::PagingState::decode(&cursor).is_err(),
        "a limited cursor must not decode as legacy pk/ck/offset state"
    );
    assert!(
        crate::paging::apply_pagination(20, Some(7), Some(&cursor)).is_err(),
        "a legacy paginator must reject this cursor rather than restart or skip rows"
    );

    let ctx = paging_ctx(&auth, &ks, Some(7), Some(cursor));
    let second = route_select_raw(&state, &ctx, &select).await.unwrap();
    assert_eq!(first.rows, expected[..7]);
    assert_eq!(second.rows, expected[7..14]);
    assert!(second.paging_state.is_some());
}

#[tokio::test]
async fn limited_bigint_scan_cursor_cannot_be_misread_as_a_legacy_offset() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(0).await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    run_ddl(
        &state,
        &ctx,
        "CREATE TABLE pageks.big (id bigint PRIMARY KEY, v int)",
    )
    .await;
    for id in 0..31 {
        run_ddl(
            &state,
            &ctx,
            &format!("INSERT INTO pageks.big (id, v) VALUES ({id}, {})", id * 10),
        )
        .await;
    }
    let query = "SELECT v, id FROM pageks.big LIMIT 20";
    let Statement::Select(select) = crate::parser::parse(query).unwrap() else {
        panic!("expected SELECT");
    };
    let (expected, _) = collect_all_pages(&state, &auth, &ks, query, None).await;
    let ctx = paging_ctx(&auth, &ks, Some(7), None);
    let first = route_select_raw(&state, &ctx, &select).await.unwrap();
    let cursor = first.paging_state.expect("LIMIT 20 needs another page");

    // A bigint partition key has the same width as the legacy offset field.
    let legacy_pk_len = u32::from_be_bytes(cursor[..4].try_into().unwrap()) as usize;
    assert!(
        legacy_pk_len > cursor.len(),
        "even an older decoder must reject the token before reading its bigint key"
    );
    assert!(
        crate::paging::PagingState::decode(&cursor).is_err(),
        "a limited bigint cursor must not decode as legacy paging state"
    );
    assert!(
        crate::paging::apply_pagination(20, Some(7), Some(&cursor)).is_err(),
        "a legacy paginator must not treat a bigint partition key as a row offset"
    );

    let ctx = paging_ctx(&auth, &ks, Some(7), Some(cursor));
    let second = route_select_raw(&state, &ctx, &select).await.unwrap();
    assert_eq!(first.rows, expected[..7]);
    assert_eq!(second.rows, expected[7..14]);
    assert!(second.paging_state.is_some());
}

#[tokio::test]
async fn limited_scan_preserves_progress_when_fetch_size_changes() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    let query = "SELECT v, id FROM pageks.t LIMIT 20";
    let Statement::Select(select) = crate::parser::parse(query).unwrap() else {
        panic!("expected SELECT");
    };
    let (expected, _) = collect_all_pages(&state, &auth, &ks, query, None).await;
    let mut cursor = None;
    let mut rows = Vec::new();
    for (fetch_size, expected_count, more_expected) in [(7, 7, true), (3, 3, true), (11, 10, false)]
    {
        let ctx = paging_ctx(&auth, &ks, Some(fetch_size), cursor);
        let page = route_select_raw(&state, &ctx, &select).await.unwrap();
        assert_eq!(page.rows.len(), expected_count);
        assert_eq!(page.paging_state.is_some(), more_expected);
        rows.extend(page.rows);
        cursor = page.paging_state;
    }
    assert_eq!(rows.len(), 20);
    assert_eq!(
        rows, expected,
        "changing fetch size changed the limited result"
    );
    assert_projected_rows_are_unique_and_correct(&rows);
}

#[tokio::test]
async fn limited_scan_rejects_a_cursor_from_a_different_limit() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    let Statement::Select(original) =
        crate::parser::parse("SELECT v, id FROM pageks.t LIMIT 20").unwrap()
    else {
        panic!("expected SELECT");
    };
    let ctx = paging_ctx(&auth, &ks, Some(7), None);
    let first = route_select_raw(&state, &ctx, &original).await.unwrap();
    let cursor = first.paging_state.expect("LIMIT 20 has more than one page");
    let Statement::Select(changed) =
        crate::parser::parse("SELECT v, id FROM pageks.t LIMIT 21").unwrap()
    else {
        panic!("expected SELECT");
    };
    let ctx = paging_ctx(&auth, &ks, Some(7), Some(cursor));
    assert!(
        route_select_raw(&state, &ctx, &changed).await.is_err(),
        "a continuation must reject a changed query LIMIT"
    );
}

#[tokio::test]
async fn limited_scan_rejects_an_authenticated_but_truncated_cursor_payload() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    let Statement::Select(select) =
        crate::parser::parse("SELECT v, id FROM pageks.t LIMIT 20").unwrap()
    else {
        panic!("expected SELECT");
    };
    let ctx = paging_ctx(&auth, &ks, Some(7), None);
    let first = route_select_raw(&state, &ctx, &select).await.unwrap();
    let cursor = first.paging_state.expect("LIMIT 20 has more than one page");
    let mut payload = crate::paging::verify_paging_payload(&cursor)
        .unwrap()
        .to_vec();
    assert!(payload.pop().is_some());
    // Authenticate the shortened outer payload so the nested continuation,
    // rather than only the outer signature, must be validated.
    let ctx = paging_ctx(
        &auth,
        &ks,
        Some(7),
        Some(crate::paging::sign_paging_payload(payload)),
    );
    assert!(
        route_select_raw(&state, &ctx, &select).await.is_err(),
        "an authenticated but incomplete continuation must be rejected"
    );
}

fn assert_projected_rows_are_unique_and_correct(rows: &[Vec<Option<CqlValue>>]) {
    let mut ids = HashSet::new();
    for row in rows {
        let [Some(CqlValue::Int(value)), Some(CqlValue::Int(id))] = row.as_slice() else {
            panic!("expected the requested (v, id) projection, got {row:?}");
        };
        assert!((0..31).contains(id), "unexpected primary key {id}");
        assert_eq!(*value, *id * 10, "wrong value for primary key {id}");
        assert!(ids.insert(*id), "duplicate primary key {id} across pages");
    }
}

#[tokio::test]
async fn limited_scan_returns_twenty_rows_across_three_pages() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    let query = "SELECT v, id FROM pageks.t LIMIT 20";
    let (expected, _) = collect_all_pages(&state, &auth, &ks, query, None).await;
    let (rows, pages) = collect_all_pages(&state, &auth, &ks, query, Some(7)).await;

    assert_eq!(
        rows.len(),
        20,
        "fetch size must not truncate the whole LIMIT"
    );
    assert_eq!(pages, 3, "LIMIT 20 at fetch size 7 needs pages 7/7/6");
    assert_eq!(rows, expected, "continuation must preserve scan order");
    assert_projected_rows_are_unique_and_correct(&rows);
}

#[tokio::test]
async fn filtered_scan_preserves_the_total_limit_across_pages() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    let query = "SELECT v, id FROM pageks.t WHERE v >= 50 LIMIT 20 ALLOW FILTERING";
    let (expected, _) = collect_all_pages(&state, &auth, &ks, query, None).await;
    let (rows, pages) = collect_all_pages(&state, &auth, &ks, query, Some(7)).await;

    assert_eq!(expected.len(), 20, "unpaged query must honor LIMIT 20");
    assert_eq!(
        rows.len(),
        20,
        "the total LIMIT must not reset on each page"
    );
    assert_eq!(pages, 3, "twenty filtered rows need pages 7/7/6");
    assert_eq!(
        rows, expected,
        "filtered continuation changed the ordered result"
    );
    assert_projected_rows_are_unique_and_correct(&rows);
    for row in &rows {
        assert!(
            matches!(&row[0], Some(CqlValue::Int(value)) if *value >= 50),
            "returned a row that does not match the filter: {row:?}"
        );
    }
}

#[tokio::test]
async fn limited_scan_stops_at_the_limit_or_dataset_end() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    // A limit below one page, equal to the dataset, and beyond the dataset.
    // The latter two both end in a partial page: 7/7/7/7/3.
    for limit in [3usize, 31, 40] {
        let query = format!("SELECT v, id FROM pageks.t LIMIT {limit}");
        let (expected, _) = collect_all_pages(&state, &auth, &ks, &query, None).await;
        let (rows, pages) = collect_all_pages(&state, &auth, &ks, &query, Some(7)).await;
        let expected_count = limit.min(31);

        assert_eq!(
            rows.len(),
            expected_count,
            "wrong result count for LIMIT {limit}"
        );
        assert_eq!(
            pages,
            expected_count.div_ceil(7),
            "wrong page count for LIMIT {limit}"
        );
        assert_eq!(
            rows, expected,
            "paging changed the result for LIMIT {limit}"
        );
        assert_projected_rows_are_unique_and_correct(&rows);
    }
}

#[tokio::test]
async fn limited_scan_v4_wire_continuation_preserves_the_total_limit() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    let query = "SELECT v, id FROM pageks.t LIMIT 20";
    let Statement::Select(select) = crate::parser::parse(query).unwrap() else {
        panic!("expected SELECT");
    };
    let (expected, _) = collect_all_pages(&state, &auth, &ks, query, None).await;
    let mut cursor: Option<Vec<u8>> = None;
    let mut rows = Vec::new();
    let mut page_sizes = Vec::new();

    // Four requests allow one excess page to be reported rather than looping.
    for _ in 0..4 {
        let wire = wire_query_params(7, cursor.as_deref());
        // These flags fit in one byte: v4 differs from the existing v5 helper
        // only by omitting the first three zero bytes of its flags field.
        let paging = crate::connection::decode_query_paging(&wire[3..], 4)
            .expect("v4 page size and echoed continuation must decode");
        let mut ctx = paging_ctx(&auth, &ks, Some(7), None);
        ctx.protocol_version = 4;
        ctx.paging = paging;
        let page = route_select_raw(&state, &ctx, &select).await.unwrap();

        assert_eq!(page.column_names, ["v", "id"]);
        assert_eq!(page.column_types, [CqlType::Int, CqlType::Int]);
        assert!(page.rows.len() <= 7, "v4 fetch size was not respected");
        page_sizes.push(page.rows.len());
        rows.extend(page.rows);
        if page.paging_state.is_none() {
            cursor = None;
            break;
        }
        assert_ne!(page.paging_state, cursor, "continuation did not advance");
        cursor = page.paging_state;
    }

    assert!(cursor.is_none(), "LIMIT 20 must finish after three pages");
    assert_eq!(page_sizes, [7, 7, 6]);
    assert_eq!(rows, expected, "v4 continuation changed the limited result");
    assert_projected_rows_are_unique_and_correct(&rows);
}

#[tokio::test]
async fn limited_scan_resumes_within_a_wide_partition() {
    let (state, _dir) = setup();
    let auth = dev_auth();
    let ks = Some("limitwide".to_string());
    let ctx = paging_ctx(&auth, &ks, None, None);
    run_ddl(
        &state,
        &ctx,
        "CREATE KEYSPACE limitwide WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': 1}",
    )
    .await;
    run_ddl(
        &state,
        &ctx,
        "CREATE TABLE limitwide.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))",
    )
    .await;
    for ck in 0..31 {
        run_ddl(
            &state,
            &ctx,
            &format!(
                "INSERT INTO limitwide.t (pk, ck, v) VALUES (1, {ck}, {})",
                ck * 10
            ),
        )
        .await;
    }

    // Every continuation is inside the same partition, with a nonempty
    // clustering position; the remaining LIMIT must advance independently.
    let pages = walk_pages(
        &state,
        &auth,
        &ks,
        "SELECT v, pk, ck FROM limitwide.t LIMIT 20",
        7,
        3,
    )
    .await;
    assert_eq!(pages.iter().map(Vec::len).collect::<Vec<_>>(), [7, 7, 6]);
    let expected: Vec<Vec<Option<CqlValue>>> = (0..20)
        .map(|ck| {
            vec![
                Some(CqlValue::Int(ck * 10)),
                Some(CqlValue::Int(1)),
                Some(CqlValue::Int(ck)),
            ]
        })
        .collect();
    assert_eq!(
        pages.into_iter().flatten().collect::<Vec<_>>(),
        expected,
        "wide-partition continuation must neither replay nor skip clustering rows"
    );
}

#[tokio::test]
async fn limited_scan_exact_page_boundaries_do_not_advertise_an_extra_page() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    for limit in [7usize, 14, 21] {
        let query = format!("SELECT v, id FROM pageks.t LIMIT {limit}");
        let Statement::Select(select) = crate::parser::parse(&query).unwrap() else {
            panic!("expected SELECT");
        };
        let (expected, _) = collect_all_pages(&state, &auth, &ks, &query, None).await;
        let mut cursor = None;
        let mut rows = Vec::new();
        for page_index in 0..limit / 7 {
            let ctx = paging_ctx(&auth, &ks, Some(7), cursor);
            let page = route_select_raw(&state, &ctx, &select).await.unwrap();
            assert_eq!(page.rows.len(), 7, "short page for LIMIT {limit}");
            let more_expected = (page_index + 1) * 7 < limit;
            assert_eq!(
                page.paging_state.is_some(),
                more_expected,
                "LIMIT {limit}: continuation must stop on its exact final page"
            );
            rows.extend(page.rows);
            cursor = page.paging_state;
        }
        assert_eq!(rows, expected, "wrong rows at exact LIMIT {limit} boundary");
        assert_projected_rows_are_unique_and_correct(&rows);
    }
}

#[tokio::test]
async fn scan_page_collector_does_not_drain_the_upstream_result() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let pulled = Arc::new(AtomicUsize::new(0));
    let observed = pulled.clone();
    let stream: ferrosa_cluster::write_path::PartitionResultStream =
        Box::pin(futures::stream::iter((0..100).map(move |id: i32| {
            observed.fetch_add(1, Ordering::Relaxed);
            Ok::<_, ferrosa_cluster::error::ClusterError>(ferrosa_sstable::types::Partition {
                key: ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
                    id.to_be_bytes().to_vec(),
                )),
                deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                static_row: None,
                rows: vec![ferrosa_sstable::types::Row {
                    clustering: Vec::new(),
                    cells: Vec::new(),
                    deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                    primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1),
                }],
            })
        })));
    let names = vec!["id".to_string()];
    let types = vec![CqlType::Int];
    let page = collect_page_from_partition_stream(
        stream,
        crate::paging::ScanBound::from_request(Some(7)),
        None,
        PartitionRowContext {
            all_col_names: &names,
            all_col_types: &types,
            pk_indices: &[0],
            ck_indices: &[],
            storage_to_table: &[],
        },
        None,
    )
    .await
    .unwrap();

    assert_eq!(page.rows.len(), 7);
    assert!(page.next_paging_state.is_some());
    assert_eq!(
        pulled.load(Ordering::Relaxed),
        8,
        "one page may consume its rows and one lookahead, not the whole result"
    );
}

#[tokio::test]
async fn limited_scan_rejects_a_future_authenticated_cursor_version() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    let Statement::Select(select) =
        crate::parser::parse("SELECT v, id FROM pageks.t LIMIT 20").unwrap()
    else {
        panic!("expected SELECT");
    };
    let first = route_select_raw(&state, &paging_ctx(&auth, &ks, Some(7), None), &select)
        .await
        .unwrap();
    let cursor = first.paging_state.unwrap();
    let mut payload = crate::paging::verify_paging_payload(&cursor)
        .unwrap()
        .to_vec();
    payload[3] = u8::MAX;
    let ctx = paging_ctx(
        &auth,
        &ks,
        Some(7),
        Some(crate::paging::sign_paging_payload(payload)),
    );
    let error = route_select_raw(&state, &ctx, &select)
        .await
        .err()
        .expect("invalid cursor must be refused");
    assert!(error.to_string().contains("version"), "{error}");
}

#[tokio::test]
async fn limited_scan_authenticates_its_outer_limit_and_count() {
    let (state, _dir, auth, ks) = setup_wide_scan_table(31).await;
    let Statement::Select(select) =
        crate::parser::parse("SELECT v, id FROM pageks.t LIMIT 20").unwrap()
    else {
        panic!("expected SELECT");
    };
    let first = route_select_raw(&state, &paging_ctx(&auth, &ks, Some(7), None), &select)
        .await
        .unwrap();
    let mut cursor = first.paging_state.unwrap();
    cursor[10] ^= 1;
    let ctx = paging_ctx(&auth, &ks, Some(7), Some(cursor));
    let error = route_select_raw(&state, &ctx, &select)
        .await
        .err()
        .expect("invalid cursor must be refused");
    assert!(error.to_string().contains("signature"), "{error}");
}
