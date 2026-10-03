//! A suspended portal holds no thread while it waits.
//!
//! The PG listener runs on a runtime whose blocking pool is bounded
//! (`background`: `max(cores * 2, 4)` threads). Every PG query's executor, and
//! every storage range-scan producer, runs on a thread from that pool. If a
//! portal suspended by `max_rows` kept its executor and its scan producer
//! blocked on the client, about `cores` idle clients would use the pool up, and
//! every later `spawn_blocking` on that runtime (every PG query, web, graph,
//! SPARQL, maintenance) would queue forever (FMEA PG-Tf348ba0b, missing-guards
//! entry 8).
//!
//! The invariant pinned here: however many portals are suspended, the number
//! of blocking threads they hold is zero once they have settled, and another
//! session's query completes.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_server::{connect, start_server};

/// The server runtime's blocking pool, as in production but small.
const MAX_BLOCKING: usize = 4;

/// Clients, each holding [`PORTALS_PER_CLIENT`] suspended portals: four times
/// the blocking pool in all.
const CLIENTS: usize = 4;
const PORTALS_PER_CLIENT: usize = 4;

/// Far more partitions than every buffer between storage and socket, so each
/// suspended portal's pipeline really is stopped mid-scan.
const ROWS: usize = 4_000;

/// How long a suspended portal may take to give its threads back. Covers the
/// storage producer's short grace wait before it pauses.
const SETTLE: Duration = Duration::from_secs(10);

/// How many of the runtime's [`MAX_BLOCKING`] blocking threads are free.
///
/// Starts that many blocking tasks that each check in and then wait to be
/// released, so they occupy every free thread at once; the count that checked
/// in within `within` is the number of free threads. Tasks still queued when
/// the probe gives up start later, find the release flag set, and return.
async fn free_blocking_threads(within: Duration) -> usize {
    let arrived = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));
    for _ in 0..MAX_BLOCKING {
        let arrived = Arc::clone(&arrived);
        let release = Arc::clone(&release);
        tokio::task::spawn_blocking(move || {
            arrived.fetch_add(1, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(30);
            while !release.load(Ordering::SeqCst) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
        });
    }
    let deadline = Instant::now() + within;
    while arrived.load(Ordering::SeqCst) < MAX_BLOCKING && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let free = arrived.load(Ordering::SeqCst);
    release.store(true, Ordering::SeqCst);
    free
}

/// Wait until every blocking thread is free, or `SETTLE` passes. Returns the
/// last count seen.
async fn settle_blocking_pool() -> usize {
    let deadline = Instant::now() + SETTLE;
    loop {
        let free = free_blocking_threads(Duration::from_millis(200)).await;
        if free == MAX_BLOCKING || Instant::now() >= deadline {
            return free;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Run `body` on a runtime shaped like the PG listener's, and shut it down
/// with a timeout: a thread parked forever (the bug under test) must fail the
/// test, not hang it in `Runtime::drop`.
fn on_bounded_runtime(body: impl std::future::Future<Output = ()>) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(MAX_BLOCKING)
        .enable_all()
        .build()
        .expect("runtime");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rt.block_on(body)));
    rt.shutdown_timeout(Duration::from_secs(1));
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

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
