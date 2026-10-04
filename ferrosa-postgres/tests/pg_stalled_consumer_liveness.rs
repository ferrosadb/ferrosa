//! A client that stops reading must not stall other clients' queries.
//!
//! `result_stream` (t_f348ba0b) made the executor's output stream to the
//! socket with backpressure. The backpressure reaches all the way down: a
//! portal suspended by `max_rows`, or a client that stops draining its socket,
//! parks the PG executor, which parks the PG scan producer, which parks the
//! storage range-scan producer in `blocking_send` — and that producer runs
//! inside a slot of the process-global bounded scan pool (`ferrosa-sched`,
//! `cores - reserved` slots). Before streaming, the result was collected first,
//! so the slot was released before the first byte was written.
//!
//! The invariant pinned here: a stalled consumer can cost its own query's
//! resources, but it cannot hold a node-wide scan slot hostage. With every
//! slot held by a suspended portal, another session's scan must still finish.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::time::Duration;

use pg_server::{connect, start_server};

/// Scan-pool slots for this test binary. Each integration test file is its own
/// process, so this reservation is the only one the global pool ever sees.
const POOL_SLOTS: usize = 2;

/// Partitions in the table: far more than every buffer between the storage
/// producer and the socket (storage channel, PG scan channel, result batches),
/// so a suspended portal's producers are genuinely parked mid-scan.
const ROWS: usize = 4_000;

/// Every scan-pool slot held by a suspended portal must not stop another
/// session's full scan from completing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspended_portals_do_not_starve_other_sessions_scans() {
    let pool = ferrosa_sched::init_global_pool(ferrosa_sched::Reservation::new(POOL_SLOTS + 1, 1));
    assert_eq!(pool.capacity(), POOL_SLOTS, "this binary owns the pool");
    let server = start_server(ROWS).await;
    let port = server.port;

    // One idle client per slot, each with a portal suspended after one row.
    // The driver keeps the transaction (and so the portal) open while `tx`
    // lives; the client then simply stops asking for rows.
    let mut idle = Vec::new();
    for _ in 0..POOL_SLOTS {
        idle.push(connect(port).await);
    }
    let mut parked = Vec::new();
    for client in &mut idle {
        let tx = client.transaction().await.expect("begin");
        let statement = tx.prepare("SELECT id FROM t").await.expect("prepare");
        let portal = tx.bind(&statement, &[]).await.expect("bind");
        let first = tx.query_portal(&portal, 1).await.expect("execute");
        assert_eq!(first.len(), 1, "the portal suspends after max_rows");
        parked.push((portal, tx));
    }
    // Every slot is held for as long as a parked producer waits for room.
    tokio::time::sleep(Duration::from_millis(500)).await;
    // The premise: each suspended portal's storage producer really is blocked
    // on its consumer. Without this, a buffer large enough to hold the whole
    // table would make the test pass without exercising anything. A producer
    // that met a full channel either parked (a fragment producer) or paused
    // and gave its slot back (a whole-partition scan, which never waits).
    let blocked = ferrosa_sched::scan_parks_total() + ferrosa_sched::scan_releases_total();
    assert!(
        blocked >= POOL_SLOTS as u64,
        "only {blocked} of {POOL_SLOTS} suspended scans blocked on their consumer"
    );

    let other = connect(port).await;
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        other.query("SELECT id FROM t", &[]),
    )
    .await;
    let rows = match outcome {
        Ok(result) => result.expect("the other session's scan succeeds"),
        Err(_) => panic!(
            "another session's scan made no progress in 20 s while {POOL_SLOTS} \
             suspended portals held every scan-pool slot (active = {})",
            pool.active()
        ),
    };
    assert_eq!(rows.len(), ROWS, "every row is returned");
    drop(parked);
}
