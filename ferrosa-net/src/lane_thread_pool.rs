//! A fixed-width pool of dedicated OS threads for the Raft lane actor.
//!
//! ## Why this exists
//!
//! `spawn_raft_lane_actor_with_timeout` used to give **each dial** its own OS
//! thread with a private single-threaded tokio runtime — one thread per peer
//! pool. That was deliberate (`0af43df8`: raft heartbeats must not be starved by
//! the data path, so the raft actor cannot share the `data-rt` runtime), but the
//! thread was created per call and the pool that owned it had no `Drop`, so any
//! dial that created a pool and dropped it before `add_peer` — a timeout, a
//! cancelled `spawn_tracked`, a superseded retry — stranded its thread for the
//! life of the process. Measured on the three `engine-prof2208` nodes: node1
//! held **24** `raft-lane-77930` threads for a single peer (expected 3).
//!
//! Pooling fixes the shape rather than the symptom: allocate the raft-lane
//! threads **once**, and *assign* each lane actor to a worker instead of
//! spawning a thread. The isolation `0af43df8` bought is preserved — these
//! threads run nothing but raft lane actors, so a saturated data path still
//! cannot starve a heartbeat — while the thread count becomes a tunable constant
//! instead of a function of how many dials happened.
//!
//! ## Model
//!
//! Each worker is one OS thread running a private single-threaded tokio runtime.
//! Assigning a job `tokio::spawn`s it on that runtime, so a worker can host
//! several lane actors **concurrently** (cooperatively interleaved) rather than
//! running them one after another — which is what lets the pool be narrower than
//! the peer count. Raft lane actors are dominated by network waits, not CPU, so
//! a small width (default 2) comfortably carries a healthy cluster's 2–3 peers
//! per node and bounds the pathological case (no peer can add a thread).
//!
//! Jobs are assigned round-robin, so a job's worker is chosen by the pool, not
//! by the caller, and the assignment has no lock and no shared queue.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::mpsc;

/// A future a worker runs to completion; a lane actor's whole life is one job.
type Job = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;

struct Worker {
    tx: mpsc::UnboundedSender<Job>,
}

/// A fixed set of OS threads that raft lane actors are *assigned* to.
pub struct LaneThreadPool {
    workers: Vec<Worker>,
    next: AtomicUsize,
    width: usize,
    /// How many worker threads this pool has actually created. Read by tests to
    /// prove assignment reuses threads instead of spawning one per job — the
    /// whole point of the pool.
    #[cfg_attr(not(test), allow(dead_code))]
    spawned: Arc<AtomicUsize>,
}

impl LaneThreadPool {
    /// Build `width` worker threads (clamped to at least 1), named
    /// `<name>-<index>`.
    pub fn new(width: usize, name: &str) -> Arc<Self> {
        let width = width.max(1);
        let spawned = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::with_capacity(width);
        for idx in 0..width {
            let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
            let spawned = Arc::clone(&spawned);
            std::thread::Builder::new()
                .name(format!("{name}-{idx}"))
                .spawn(move || {
                    spawned.fetch_add(1, Ordering::AcqRel);
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("lane pool worker runtime");
                    rt.block_on(async move {
                        // Spawn each job so several lane actors share this
                        // worker concurrently. The loop itself returns only when
                        // every sender is gone (pool shutdown), which then drops
                        // the runtime and cancels any actor still running.
                        while let Some(job) = rx.recv().await {
                            tokio::spawn(job);
                        }
                    });
                })
                .expect("spawn lane pool worker");
            workers.push(Worker { tx });
        }
        Arc::new(Self {
            workers,
            next: AtomicUsize::new(0),
            width,
            spawned,
        })
    }

    /// Number of worker threads.
    pub fn width(&self) -> usize {
        self.width
    }

    /// How many worker threads have actually been created (test aid).
    #[cfg(test)]
    pub(crate) fn threads_spawned(&self) -> usize {
        self.spawned.load(Ordering::Acquire)
    }

    /// Assign a future to the next worker, round-robin.
    ///
    /// A send can only fail if the pool is being dropped, in which case the
    /// process is going away and there is nothing to serve anyway.
    pub fn assign<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let _ = self.workers[idx].tx.send(Box::pin(fut));
    }
}

impl Drop for LaneThreadPool {
    fn drop(&mut self) {
        // Dropping the senders closes every worker channel, so each `recv()`
        // returns `None`, `block_on` returns, and the thread exits.
        self.workers.clear();
    }
}

/// Resolve the pool width from an env value, falling back to `default` when the
/// value is unset, empty, unparseable or non-positive.
///
/// Pure, so it is testable without the racy `set_var` that parallel tests make
/// unsafe. Empty must fall back rather than fail: `fly machine update --env K=`
/// is the only way to clear a Fly variable, so treating empty as invalid would
/// make the knob impossible to unset.
fn resolve_pool_width(env_val: Option<String>, default: usize) -> usize {
    env_val
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|w| *w > 0)
        .unwrap_or(default)
}

/// The process-wide raft lane thread pool.
///
/// A process global (like `ferrosa_sched`'s scheduler pool) so that the many
/// `PriorityPool::connect` dial sites do not each have to thread a handle
/// through — the pool is an execution resource, not per-connection state, and
/// there is exactly one process. Width comes from
/// `FERROSA_RAFT_LANE_POOL_THREADS` (default 2).
pub fn lane_thread_pool() -> &'static Arc<LaneThreadPool> {
    static POOL: OnceLock<Arc<LaneThreadPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        let width = resolve_pool_width(std::env::var("FERROSA_RAFT_LANE_POOL_THREADS").ok(), 2);
        LaneThreadPool::new(width, "raft-lane-pool")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn width_resolution_prefers_valid_env_and_falls_back_safely() {
        assert_eq!(resolve_pool_width(Some("6".into()), 2), 6);
        assert_eq!(resolve_pool_width(None, 2), 2, "unset uses default");
        assert_eq!(
            resolve_pool_width(Some(String::new()), 2),
            2,
            "an EMPTY value must fall back — it is how a Fly var is cleared"
        );
        assert_eq!(
            resolve_pool_width(Some("0".into()), 2),
            2,
            "zero is not a width"
        );
        assert_eq!(
            resolve_pool_width(Some("nope".into()), 2),
            2,
            "garbage falls back"
        );
        assert_eq!(resolve_pool_width(Some(" 3 ".into()), 2), 3, "trimmed");
    }

    #[test]
    fn width_is_clamped_to_at_least_one() {
        assert_eq!(LaneThreadPool::new(0, "clamp-test").width(), 1);
    }

    /// Jobs assigned to the pool actually run — and all of them run, not just
    /// the first. This is the property the whole pool exists to provide.
    #[tokio::test]
    async fn assigned_jobs_all_run() {
        let pool = LaneThreadPool::new(2, "assign-test");
        let done = Arc::new(AtomicU32::new(0));

        // More jobs than workers, to prove a worker hosts several concurrently
        // rather than running them strictly one after another.
        for _ in 0..6 {
            let done = Arc::clone(&done);
            pool.assign(async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                done.fetch_add(1, Ordering::AcqRel);
            });
        }

        for _ in 0..100 {
            if done.load(Ordering::Acquire) == 6 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!(
            "only {} of 6 assigned jobs ran",
            done.load(Ordering::Acquire)
        );
    }

    /// The pool allocates a fixed number of threads up front and never spawns
    /// another, no matter how many jobs are assigned. This is the "allocate once
    /// and reuse them" contract — 200 jobs on a width-3 pool must not create a
    /// 4th thread.
    #[tokio::test]
    async fn assignment_does_not_spawn_threads() {
        let pool = LaneThreadPool::new(3, "reuse-test");
        let done = Arc::new(AtomicU32::new(0));

        for _ in 0..200 {
            let done = Arc::clone(&done);
            pool.assign(async move {
                tokio::task::yield_now().await;
                done.fetch_add(1, Ordering::AcqRel);
            });
        }
        for _ in 0..200 {
            if done.load(Ordering::Acquire) == 200 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(done.load(Ordering::Acquire), 200, "all 200 jobs must run");
        assert_eq!(
            pool.threads_spawned(),
            3,
            "assignment must reuse the 3 pool threads, not spawn per job"
        );
    }

    /// A worker hosts several jobs at once. Six 100 ms sleeps finish far sooner
    /// than 600 ms if (and only if) they overlap on the two workers.
    #[tokio::test]
    async fn a_worker_runs_jobs_concurrently_not_serially() {
        let pool = LaneThreadPool::new(2, "concurrent-test");
        let done = Arc::new(AtomicU32::new(0));
        let start = std::time::Instant::now();

        for _ in 0..6 {
            let done = Arc::clone(&done);
            pool.assign(async move {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                done.fetch_add(1, Ordering::AcqRel);
            });
        }

        for _ in 0..200 {
            if done.load(Ordering::Acquire) == 6 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(done.load(Ordering::Acquire), 6, "all jobs must finish");
        // Serial on 2 workers would be ceil(6/2) * 100 ms = 300 ms; concurrent
        // is ~100 ms. Assert well under the serial bound, with slack for CI.
        assert!(
            start.elapsed() < std::time::Duration::from_millis(250),
            "jobs did not overlap: took {:?}",
            start.elapsed()
        );
    }
}
