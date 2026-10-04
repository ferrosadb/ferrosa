//! Run a test on a runtime whose blocking pool is bounded like the PG
//! listener's, and measure how many of its blocking threads are free.
//!
//! Each test binary includes this with `#[path]`, and not every binary uses
//! every helper.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The server runtime's blocking pool, as in production but small.
pub const MAX_BLOCKING: usize = 4;

/// How long a released query may take to give its threads back.
pub const SETTLE: Duration = Duration::from_secs(10);

/// How many of the runtime's [`MAX_BLOCKING`] blocking threads are free.
///
/// Starts that many blocking tasks that each check in and then wait to be
/// released, so they occupy every free thread at once; the count that checked
/// in within `within` is the number of free threads. Tasks still queued when
/// the probe gives up start later, find the release flag set, and return.
pub async fn free_blocking_threads(within: Duration) -> usize {
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

/// Wait until every blocking thread is free, or [`SETTLE`] passes. Returns
/// the last count seen.
pub async fn settle_blocking_pool() -> usize {
    let deadline = Instant::now() + SETTLE;
    loop {
        let free = free_blocking_threads(Duration::from_millis(200)).await;
        if free == MAX_BLOCKING || Instant::now() >= deadline {
            return free;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until `done()` holds, or `within` passes; returns whether it held.
pub async fn eventually(within: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;
    loop {
        if done() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Run `body` on a runtime shaped like the PG listener's, and shut it down
/// with a timeout: a thread parked forever (the bug under test) must fail the
/// test, not hang it in `Runtime::drop`.
pub fn on_bounded_runtime(body: impl std::future::Future<Output = ()>) {
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
