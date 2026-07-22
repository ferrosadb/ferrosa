//! Module: Bounded execution pool that reserves CPU headroom for consensus.
//!
//! Correctness: correct when no more than `cores - reserved` CPU-bound closures
//! run concurrently — so the raft runtime always has a schedulable core — and
//! every admission slot is returned on completion, drop, or panic (RAII). Unit
//! and stress tests remain green.
//!
//! Last revised: 2026-07-21
//! Last changed: New crate — Phase 0 of the query scheduler (t_88223ad0). Bounds
//!   the `spawn_blocking` pool that scan producers use, replacing the unbounded
//!   tokio default (512) that let a full-table `ALLOW FILTERING` fan-out
//!   oversubscribe the cores and starve raft heartbeats into a CheckQuorum
//!   leader step-down.
//!
//! # Design
//!
//! ferrosa already isolates raft at the *runtime* level (a dedicated multi-thread
//! runtime + per-peer OS threads). The residual starvation is the **blocking
//! pool**: storage scan producers `spawn_blocking` with no cap, so a broad scan
//! admits hundreds of blocking tasks that saturate every core. [`SchedPool`]
//! gates admission with a semaphore sized to [`Reservation::available`] so the
//! reserved cores stay free for consensus. This crate is a leaf: it depends only
//! on tokio, so nothing in the storage/cluster/cql stack leaks in (DSM guard).

use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

/// Scheduling class of a unit of work. Phase 0 does not *act* on the class (the
/// pool admits FIFO); it exists so B1 can attach fair-share weights without
/// re-touching call sites. Consensus work never flows through [`SchedPool`] — it
/// runs on its own reserved runtime — so it has no variant here by design.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchedClass {
    /// Latency-sensitive client reads/writes.
    Foreground,
    /// Bulk background work: full-table scans, compaction, index builds.
    Bulk,
}

/// A core reservation: `cores` total, `reserved` kept free for consensus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reservation {
    /// Total CPU cores available to the process.
    pub cores: usize,
    /// Cores reserved for consensus (raft heartbeats/elections) — never admitted
    /// to the bulk pool.
    pub reserved: usize,
}

impl Reservation {
    /// Reserve `reserved` of `cores` cores for consensus.
    pub fn new(cores: usize, reserved: usize) -> Self {
        Self { cores, reserved }
    }

    /// Derive the reservation from the detected parallelism, keeping `reserved`
    /// cores free for consensus. Falls back to a single core if detection fails.
    pub fn from_available_parallelism(reserved: usize) -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Self::new(cores, reserved)
    }

    /// CPU-bound background slots after reserving consensus headroom. Always at
    /// least 1 so the pool can make progress even when `reserved >= cores`.
    pub fn available(&self) -> usize {
        self.cores.saturating_sub(self.reserved).max(1)
    }
}

/// Opaque scheduling ticket threaded router -> coordinator -> producer. Phase 0
/// is a no-op carrier that records the [`SchedClass`]; B1 activates fair-share
/// accounting on it without changing the call sites that already pass it.
#[derive(Clone, Copy, Debug)]
pub struct SchedTicket {
    class: SchedClass,
}

impl SchedTicket {
    /// Mint a ticket for `class`.
    pub fn new(class: SchedClass) -> Self {
        Self { class }
    }

    /// The class this ticket was minted for.
    pub fn class(&self) -> SchedClass {
        self.class
    }
}

/// Bounded execution pool for CPU-bound background work.
///
/// [`submit`](SchedPool::submit) runs a blocking closure on tokio's blocking
/// threads but admits at most [`capacity`](SchedPool::capacity) at once, so the
/// reserved consensus cores are never oversubscribed. The admission permit is
/// moved into the blocking task and dropped when it finishes — including on
/// panic — so a slot is never leaked (RAII).
#[derive(Clone)]
pub struct SchedPool {
    slots: Arc<Semaphore>,
    capacity: usize,
}

impl SchedPool {
    /// Build a pool admitting `reservation.available()` CPU-bound tasks at once.
    pub fn new(reservation: Reservation) -> Self {
        let capacity = reservation.available();
        Self {
            slots: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    /// Maximum number of closures admitted concurrently.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Currently free admission slots (`capacity` minus in-flight).
    pub fn available_permits(&self) -> usize {
        self.slots.available_permits()
    }

    /// Admit and run `f` on a blocking thread once a slot is free, returning the
    /// join handle. Awaiting this future blocks (asynchronously) until admission;
    /// the returned handle resolves to `f`'s result (or a `JoinError` if `f`
    /// panicked). The admission slot is released when the task ends.
    pub async fn submit<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("scheduler pool semaphore is never closed");
        tokio::task::spawn_blocking(move || {
            // RAII: the permit is released when this closure returns OR unwinds,
            // so a panicking scan producer never leaks its admission slot.
            let _permit = permit;
            f()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn available_is_cores_minus_reserved_floored_at_one() {
        assert_eq!(Reservation::new(8, 2).available(), 6);
        assert_eq!(Reservation::new(4, 4).available(), 1, "must floor at 1");
        assert_eq!(
            Reservation::new(1, 8).available(),
            1,
            "reserved > cores floors at 1"
        );
        assert_eq!(Reservation::new(2, 0).available(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn pool_admits_at_most_capacity_concurrently() {
        // 4 cores, reserve 2 for consensus => capacity 2.
        let pool = SchedPool::new(Reservation::new(4, 2));
        assert_eq!(pool.capacity(), 2);

        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let active = active.clone();
            let max_seen = max_seen.clone();
            let h = pool
                .submit(move || {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(15));
                    active.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            handles.push(h);
        }
        for h in handles {
            h.await.expect("blocking task joined");
        }

        assert!(
            max_seen.load(Ordering::SeqCst) <= pool.capacity(),
            "pool admitted {} concurrently, exceeding capacity {}",
            max_seen.load(Ordering::SeqCst),
            pool.capacity()
        );
        assert_eq!(
            pool.available_permits(),
            pool.capacity(),
            "every admission slot must be returned after the tasks finish"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn panicking_closure_returns_its_slot() {
        // Capacity 1: if a panic leaked the slot, the second submit would block
        // forever and the test would time out.
        let pool = SchedPool::new(Reservation::new(1, 0));
        assert_eq!(pool.capacity(), 1);

        let h = pool.submit(|| panic!("scan producer blew up")).await;
        assert!(h.await.is_err(), "panicking task must surface a JoinError");

        // The slot must be back; a fresh submit must complete promptly.
        let ok = tokio::time::timeout(Duration::from_secs(5), async {
            pool.submit(|| 42u32)
                .await
                .await
                .expect("second task joined")
        })
        .await
        .expect("slot leaked after panic — second submit blocked");
        assert_eq!(ok, 42);
        assert_eq!(pool.available_permits(), 1);
    }
}
