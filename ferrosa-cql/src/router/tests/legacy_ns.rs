//! CQL over rows a pre-t_cf637b6e build wrote: Accord stamped cells and
//! liveness in NANOSECONDS. `writetime()` must report microseconds, and a
//! client may no longer supply a `USING TIMESTAMP` in the range storage
//! reserves for those legacy values.

use super::*;

const LEGACY_FLOOR: i64 = ferrosa_common::LEGACY_NS_THRESHOLD;

async fn legacy_table() -> (SharedState, TempDir, AuthContext, Option<String>) {
    let (state, dir) = setup();
    let auth = dev_auth();
    let ks = Some("lns".to_string());
    let ctx = paging_ctx(&auth, &ks, None, None);
    run_ddl(
        &state,
        &ctx,
        "CREATE KEYSPACE lns WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
    )
    .await;
    run_ddl(
        &state,
        &ctx,
        "CREATE TABLE lns.t (id text PRIMARY KEY, v text)",
    )
    .await;
    (state, dir, auth, ks)
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

/// Write `stmt` the way the pre-fix Accord apply did: the router's LWT
/// mutation with every LWW stamp set to the agreed `t.time`, nanoseconds.
fn apply_old_style(state: &SharedState, ctx: &RequestContext<'_>, stmt: &str, t_ns: i64) {
    let stmt = crate::parser::parse(stmt).unwrap();
    let LwtWrite { mutation, .. } = build_lwt_mutation(state, ctx, &stmt).unwrap();
    let m = ferrosa_storage::Mutation::deserialize_from(&mutation).unwrap();
    let table = ferrosa_storage::TableId::new(&m.keyspace, &m.table);
    for mut row in m.rows {
        for (_, cell) in &mut row.cells {
            cell.timestamp = t_ns;
        }
        if row.primary_key_liveness.has_timestamp() {
            row.primary_key_liveness.timestamp = t_ns;
        }
        state.engine.write(&table, &m.key, row, t_ns).unwrap();
    }
}

async fn select(state: &SharedState, ctx: &RequestContext<'_>, cql: &str) -> Vec<Option<CqlValue>> {
    let select = match crate::parser::parse(cql).unwrap() {
        Statement::Select(s) => s,
        other => panic!("expected select, got {other:?}"),
    };
    let mut res = route_select_raw(state, ctx, &select).await.unwrap();
    assert_eq!(res.rows.len(), 1, "{cql}");
    res.rows.remove(0)
}

/// Test 15: the cell metadata `writetime()` and `TTL()` read for a legacy LWT
/// cell is in microseconds. (The SELECT projection of those functions is not
/// wired to this metadata yet and answers null for every row — t_7987e84c —
/// so this checks the metadata the projection is to use.)
#[tokio::test]
async fn legacy_ns_writetime_and_ttl_metadata_of_an_lwt_cell_are_microseconds() {
    let (state, _dir, auth, ks) = legacy_table().await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    let t0 = now_us() - 1_000_000;
    apply_old_style(
        &state,
        &ctx,
        "INSERT INTO lns.t (id, v) VALUES ('k', 'lwt') IF NOT EXISTS USING TTL 3600",
        t0 * 1_000 + 789,
    );
    let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(b"k".to_vec()));
    let partition = state
        .engine
        .read(&ferrosa_storage::TableId::new("lns", "t"), &key)
        .unwrap()
        .expect("the legacy row");
    let (rows, meta) = bridge::partition_to_rows_with_metadata(
        &partition,
        &["id".to_string(), "v".to_string()],
        &[CqlType::Varchar, CqlType::Varchar],
        &[0],
        &[],
    )
    .unwrap();
    assert_eq!(rows[0][1], Some(CqlValue::Text("lwt".to_string())));
    assert_eq!(meta[0][1].timestamp, t0, "writetime(v) in micros");
    assert_eq!(meta[0][1].ttl, 3600, "TTL(v)");
}

/// A plain UPDATE through CQL beats a legacy LWT row.
#[tokio::test]
async fn legacy_ns_a_cql_update_beats_a_legacy_lwt_row() {
    let (state, _dir, auth, ks) = legacy_table().await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    apply_old_style(
        &state,
        &ctx,
        "INSERT INTO lns.t (id, v) VALUES ('k', 'lwt') IF NOT EXISTS",
        (now_us() - 1_000_000) * 1_000,
    );
    run_ddl(&state, &ctx, "UPDATE lns.t SET v = 'plain' WHERE id = 'k'").await;
    let row = select(&state, &ctx, "SELECT v FROM lns.t WHERE id = 'k'").await;
    assert_eq!(row[0], Some(CqlValue::Text("plain".to_string())));
}

/// Test 13: `USING TIMESTAMP` at or above 1e18 is refused as an invalid
/// request on every write form; 1e18 - 1 is accepted.
#[tokio::test]
async fn legacy_ns_using_timestamp_at_or_above_1e18_is_refused() {
    let (state, _dir, auth, ks) = legacy_table().await;
    let ctx = paging_ctx(&auth, &ks, None, None);
    for ts in [LEGACY_FLOOR, LEGACY_FLOOR + 1, i64::MAX] {
        for cql in [
            format!("INSERT INTO lns.t (id, v) VALUES ('k', 'x') USING TIMESTAMP {ts}"),
            format!("UPDATE lns.t USING TIMESTAMP {ts} SET v = 'x' WHERE id = 'k'"),
            format!("DELETE FROM lns.t USING TIMESTAMP {ts} WHERE id = 'k'"),
            format!(
                "BEGIN BATCH USING TIMESTAMP {ts} \
                 INSERT INTO lns.t (id, v) VALUES ('k', 'x'); APPLY BATCH"
            ),
        ] {
            let res = route(&state, &ctx, crate::parser::parse(&cql).unwrap()).await;
            assert!(
                matches!(res, Err(CqlError::Invalid(_))),
                "[{cql}] must be an invalid request, got {:?}",
                res.map(|_| ())
            );
        }
    }
    let ok = LEGACY_FLOOR - 1;
    run_ddl(
        &state,
        &ctx,
        &format!("INSERT INTO lns.t (id, v) VALUES ('k', 'edge') USING TIMESTAMP {ok}"),
    )
    .await;
    let row = select(&state, &ctx, "SELECT v FROM lns.t WHERE id = 'k'").await;
    assert_eq!(row[0], Some(CqlValue::Text("edge".to_string())));
    let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(b"k".to_vec()));
    let partition = state
        .engine
        .read(&ferrosa_storage::TableId::new("lns", "t"), &key)
        .unwrap()
        .unwrap();
    assert_eq!(
        partition.rows[0].cells[0].1.timestamp, ok,
        "stored as given"
    );
}
