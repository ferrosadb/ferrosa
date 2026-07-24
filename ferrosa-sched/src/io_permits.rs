//! Module: Bounded I/O permits — the `Lane::Bulk` resource dimension (B2).
//! Correctness: Correct when at most `capacity` I/O permits are held at once, a
//!   permit is always returned on drop (RAII — including panic/cancel unwind),
//!   and a waiter is a cheap async task rather than a parked blocking thread.
//! Last revised: 2026-07-22
//! Last changed: New module — B2 T2.1/T2.4. The CPU dimension ([`SchedPool`] /
//!   [`crate::fair_admit::FairAdmit`]) bounds concurrent scan *compute*; this
//!   bounds concurrent bulk *I/O* so a fan-out of scans reading from S3 cannot
//!   saturate the `Lane::Bulk` I/O path and starve the reserved lanes.
//!
//! [`SchedPool`]: crate::SchedPool
//!
//! # Model
//!
//! A [`Semaphore`] of `capacity` permits. A bulk I/O operation
//! [`acquire`](IoPermits::acquire)s a permit (async — an I/O-bound waiter parks
//! as a cheap task, preserving the B0 no-parked-threads property), does its I/O,
//! and drops the [`IoPermit`] to return it. `capacity` is the bulk I/O
//! reservation: it caps how much concurrent bulk I/O competes for the shared
//! path, leaving headroom for consensus/interactive I/O — the I/O analogue of
//! the CPU [`Reservation`](crate::Reservation).
//!
//! Deadlock- and leak-free: the permit is RAII (returned on the guard's drop,
//! so a panic or future-cancellation during the I/O still returns it), and
//! acquisition never holds a lock across the `.await`.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A bounded pool of concurrent bulk-I/O permits (the `Lane::Bulk` reservation).
#[derive(Clone, Debug)]
pub struct IoPermits {
    sem: Arc<Semaphore>,
    capacity: usize,
}

/// An RAII bulk-I/O permit. Held for the duration of an I/O operation; returned
/// to the pool when dropped (on normal completion, panic, or cancellation).
#[derive(Debug)]
pub struct IoPermit {
    // Held only for its Drop: returning the permit to the semaphore. Never read.
    _permit: OwnedSemaphorePermit,
}

impl IoPermits {
    /// A pool granting at most `capacity` (≥1) concurrent bulk-I/O permits.
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            sem: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    /// Maximum concurrent permits.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Permits currently free.
    pub fn available(&self) -> usize {
        self.sem.available_permits()
    }

    /// Permits currently held (in-flight bulk I/O).
    pub fn in_flight(&self) -> usize {
        self.capacity - self.sem.available_permits()
    }

    /// Acquire a permit, waiting (as a cheap async task) until one is free.
    /// Returns an RAII [`IoPermit`] that returns the permit on drop.
    pub async fn acquire(&self) -> IoPermit {
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            // The semaphore is never closed (we hold an `Arc` to it for the
            // pool's whole life), so acquisition cannot fail.
            .expect("io-permit semaphore is never closed");
        crate::record_io_permit_acquired();
        IoPermit { _permit: permit }
    }

    /// Try to acquire a permit without waiting. Returns `None` if the bound is
    /// currently saturated (bulk I/O backpressure — the caller decides whether
    /// to wait via [`acquire`](Self::acquire) or shed).
    pub fn try_acquire(&self) -> Option<IoPermit> {
        match self.sem.clone().try_acquire_owned() {
            Ok(permit) => {
                crate::record_io_permit_acquired();
                Some(IoPermit { _permit: permit })
            }
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn capacity_floors_at_one() {
        assert_eq!(IoPermits::new(0).capacity(), 1);
        assert_eq!(IoPermits::new(5).capacity(), 5);
    }

    #[test]
    fn try_acquire_respects_the_bound_and_reuses_freed_permits() {
        let permits = IoPermits::new(2);
        assert_eq!(permits.available(), 2);
        let p1 = permits.try_acquire().expect("first permit");
        let p2 = permits.try_acquire().expect("second permit");
        assert_eq!(permits.in_flight(), 2);
        assert!(
            permits.try_acquire().is_none(),
            "a third permit must be refused — the bound is 2"
        );
        drop(p1);
        assert_eq!(permits.available(), 1, "dropping a permit returns it");
        assert!(
            permits.try_acquire().is_some(),
            "a freed permit is immediately reusable"
        );
        drop(p2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn bound_is_respected_under_concurrency() {
        let permits = Arc::new(IoPermits::new(3));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..24 {
            let permits = permits.clone();
            let max_seen = max_seen.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = permits.acquire().await;
                let now = permits.in_flight();
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert!(
            max_seen.load(Ordering::SeqCst) <= 3,
            "held {} concurrently, capacity 3",
            max_seen.load(Ordering::SeqCst)
        );
        assert_eq!(permits.available(), 3, "every permit returned");
    }

    /// T2.4 — permit-leak invariant: a panic while holding a permit (e.g. a
    /// chunk failing mid-I/O) must return it via RAII unwind, not leak it.
    #[test]
    fn permit_returns_on_panic() {
        let permits = IoPermits::new(1);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _permit = permits.try_acquire().expect("the only permit");
            assert_eq!(permits.available(), 0);
            panic!("simulated chunk failure while holding an I/O permit");
        }));
        assert!(result.is_err(), "the guarded closure panicked");
        assert_eq!(
            permits.available(),
            permits.capacity(),
            "the I/O permit must be returned on panic unwind, not leaked"
        );
    }

    /// T2.4 — cancellation: dropping a *pending* acquire future must not consume
    /// a permit (tokio `Semaphore` acquisition is cancel-safe).
    #[tokio::test]
    async fn cancelled_acquire_consumes_no_permit() {
        let permits = IoPermits::new(1);
        let _held = permits.try_acquire().expect("take the only permit");
        // A second acquire has nothing to take; poll it once (pending), drop it.
        {
            let fut = permits.acquire();
            futures_poll_once_pending(fut).await;
        }
        drop(_held);
        assert_eq!(
            permits.available(),
            1,
            "a cancelled acquire must not have consumed the permit"
        );
    }

    /// Poll `fut` exactly once with a no-op waker, asserting it is pending, then
    /// drop it — a runtime-free way to test cancellation of a pending acquire.
    async fn futures_poll_once_pending<F: std::future::Future>(fut: F) {
        use std::pin::pin;
        use std::task::{Context, Poll};
        let mut fut = pin!(fut);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            matches!(fut.as_mut().poll(&mut cx), Poll::Pending),
            "acquire must be pending while the pool is saturated"
        );
    }
}
