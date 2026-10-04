//! A portal suspended and resumed over and over returns its whole result, in
//! order, exactly once — including when the storage scan beneath it paused
//! (gave back its thread) between `Execute`s, and when rows were written while
//! it was paused.
//!
//! What a portal sees of concurrent writes (documented in the crate README):
//! rows changed by PostgreSQL transactions follow the portal's MVCC snapshot;
//! rows written outside PostgreSQL (CQL, or the storage engine directly, as
//! here) are read as the storage scan reaches them. A storage range scan is
//! not a snapshot: a row written ahead of the scan's position may or may not
//! appear, paused or not. Every row that existed when the portal started and
//! was not deleted appears exactly once, and rows come out in storage
//! (token) order.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::collections::HashSet;
use std::time::Duration;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use pg_server::{connect, row_id, start_server, write_row};

const ROWS: usize = 1_000;

/// Rows per `Execute`; does not divide the internal batch sizes.
const FETCH: i32 = 7;

/// Long enough that a scan left alone this long has paused (a full channel
/// pauses it at once) and must resume.
const IDLE: Duration = Duration::from_millis(250);

fn storage_order(id: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(id.as_bytes().to_vec()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_portal_resumed_across_storage_pauses_returns_every_row_once_in_order() {
    ferrosa_sched::init_global_pool(ferrosa_sched::Reservation::new(17, 1));
    let server = start_server(ROWS).await;
    let mut client = connect(server.port).await;
    let tx = client.transaction().await.expect("begin");
    let statement = tx.prepare("SELECT id FROM t").await.expect("prepare");
    let portal = tx.bind(&statement, &[]).await.expect("bind");

    let releases = ferrosa_sched::scan_releases_total();
    let resumes = ferrosa_storage::range_scan_resumes_total();
    let mut ids: Vec<String> = Vec::new();
    let mut written = 0usize;
    for execute in 0usize.. {
        let rows = tx.query_portal(&portal, FETCH).await.expect("execute");
        ids.extend(rows.iter().map(|row| row.get::<_, String>(0)));
        if rows.len() < FETCH as usize {
            break;
        }
        if execute % 10 == 0 {
            // Leave the portal idle so its scan pauses, writing meanwhile: new
            // rows on both sides of the scan's position, and overwrites.
            for _ in 0..3 {
                write_row(&server.engine, ROWS + written);
                written += 1;
            }
            write_row(&server.engine, execute % ROWS);
            tokio::time::sleep(IDLE).await;
        }
        assert!(execute < 10 * ROWS, "the portal never finished");
    }

    // The premise: the storage scan really did pause and resume.
    assert!(
        ferrosa_sched::scan_releases_total() > releases,
        "the storage scan never paused, so nothing here exercised a resume"
    );
    assert!(
        ferrosa_storage::range_scan_resumes_total() > resumes,
        "a paused storage scan never resumed"
    );

    let mut seen = HashSet::new();
    for id in &ids {
        assert!(seen.insert(id.clone()), "row {id} was returned twice");
    }
    let missing: Vec<String> = (0..ROWS)
        .map(row_id)
        .filter(|id| !seen.contains(id))
        .collect();
    assert!(missing.is_empty(), "rows lost across pauses: {missing:?}");
    let order: Vec<DecoratedKey> = ids.iter().map(|id| storage_order(id)).collect();
    assert!(
        order.windows(2).all(|pair| pair[0] < pair[1]),
        "rows came out of storage order"
    );
    tx.commit().await.expect("commit");
}
