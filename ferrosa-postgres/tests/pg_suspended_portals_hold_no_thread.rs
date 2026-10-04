//! A query waiting for its client holds no thread.
//!
//! The PG listener runs on a runtime whose blocking pool is bounded
//! (`background`: `max(cores * 2, 4)` threads). Every PG query's executor, and
//! every storage range-scan producer, runs on a thread from that pool. If a
//! portal suspended by `max_rows`, or a query whose client stopped reading its
//! socket, kept its executor and its scan producer blocked on the client,
//! about `cores` idle clients would use the pool up, and every later
//! `spawn_blocking` on that runtime (every PG query, web, graph, SPARQL,
//! maintenance) would queue forever (FMEA PG-Tf348ba0b, missing-guards
//! entry 8).
//!
//! The invariant pinned here: however many queries wait on their clients, the
//! number of blocking threads they hold is zero once they have settled, and
//! another session's query completes.

#[path = "common/blocking_pool.rs"]
mod blocking_pool;
#[path = "common/pg_server.rs"]
mod pg_server;

use std::time::Duration;

use blocking_pool::{on_bounded_runtime, settle_blocking_pool, MAX_BLOCKING, SETTLE};
use pg_server::{connect, start_server};

/// Clients, each holding [`PORTALS_PER_CLIENT`] suspended portals: four times
/// the blocking pool in all.
const CLIENTS: usize = 4;
const PORTALS_PER_CLIENT: usize = 4;

/// Far more partitions than every buffer between storage and socket, so each
/// suspended portal's pipeline really is stopped mid-scan.
const ROWS: usize = 4_000;

#[test]
fn suspended_portals_hold_no_blocking_thread() {
    on_bounded_runtime(async {
        // Slots are not what this test is about: give the scan pool plenty.
        ferrosa_sched::init_global_pool(ferrosa_sched::Reservation::new(17, 1));
        let server = start_server(ROWS).await;

        let mut clients = Vec::new();
        for _ in 0..CLIENTS {
            clients.push(connect(server.port).await);
        }
        // The transactions keep the portals alive; the clients then simply
        // stop asking for rows.
        let mut open = Vec::new();
        let mut parked = Vec::new();
        let mut suspended = 0usize;
        for client in &mut clients {
            let tx = client.transaction().await.expect("begin");
            let statement = tx.prepare("SELECT id FROM t").await.expect("prepare");
            for _ in 0..PORTALS_PER_CLIENT {
                let portal = tx.bind(&statement, &[]).await.expect("bind");
                let first =
                    tokio::time::timeout(Duration::from_secs(10), tx.query_portal(&portal, 1))
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "portal {} of {} made no progress in 10 s: the {suspended} \
                                 portals already suspended hold the blocking pool \
                                 (max_blocking_threads = {MAX_BLOCKING})",
                                suspended + 1,
                                CLIENTS * PORTALS_PER_CLIENT
                            )
                        })
                        .expect("execute");
                assert_eq!(first.len(), 1, "the portal suspends after max_rows");
                suspended += 1;
                parked.push(portal);
            }
            open.push(tx);
        }

        let free = settle_blocking_pool().await;
        assert_eq!(
            free,
            MAX_BLOCKING,
            "{suspended} suspended portals still hold {} of {MAX_BLOCKING} blocking \
             threads after {SETTLE:?}",
            MAX_BLOCKING - free
        );

        let other = connect(server.port).await;
        let rows = tokio::time::timeout(
            Duration::from_secs(20),
            other.query("SELECT id FROM t", &[]),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("another session's SELECT made no progress in 20 s beside {suspended} suspended portals")
        })
        .expect("the other session's SELECT succeeds");
        assert_eq!(rows.len(), ROWS, "every row is returned");
    });
}

/// More concurrent queries than the blocking pool has threads all complete.
/// Each query's executor waits on its storage scan; if the scan producers drew
/// threads from the same bounded pool as the executors, enough concurrent
/// queries would hold every thread waiting on producers that can never start.
#[test]
fn more_concurrent_queries_than_blocking_threads_all_complete() {
    on_bounded_runtime(async {
        ferrosa_sched::init_global_pool(ferrosa_sched::Reservation::new(17, 1));
        let server = start_server(ROWS).await;
        let mut queries = Vec::new();
        for _ in 0..4 * MAX_BLOCKING {
            let client = connect(server.port).await;
            queries.push(tokio::spawn(async move {
                client
                    .query("SELECT id FROM t", &[])
                    .await
                    .map(|rows| rows.len())
            }));
        }
        let all = futures::future::join_all(queries);
        let done = tokio::time::timeout(Duration::from_secs(30), all)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{} concurrent queries beside a {MAX_BLOCKING}-thread blocking pool made \
                     no progress in 30 s",
                    4 * MAX_BLOCKING
                )
            });
        for result in done {
            assert_eq!(result.expect("joined").expect("query"), ROWS);
        }
    });
}
