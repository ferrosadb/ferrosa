//! Module: the node-wide spill memory pool.
//! Correctness: Correct when the bytes every spilling operator holds in memory,
//!   summed over the process, stay within one pool limit plus at most one row per
//!   operator (the forward-progress floor below).
//! Last revised: 2026-11-05
//! Last changed: Introduced. Spilling operators used to take
//!   `process_spill_threshold_bytes()` EACH, so three concurrent sorts held one
//!   and a half process budgets (streaming audit, `Streaming.per_operator_thresholds_multiply`).
//!
//! # Why a pool and not a divisor
//!
//! A fixed `budget / N` gives a lone sort on an idle node 1/N of the memory it
//! could use, and still needs a semaphore to stop sort N+1. A pool instead
//! charges what each operator actually buffers: a single sort can use the whole
//! pool, and when several run together whoever would push the pool over spills
//! its own buffer first. The node never holds more than one pool in total.
//!
//! # The forward-progress floor (designed, documented, observable)
//!
//! An operator whose buffer is empty after spilling must still be able to hold
//! the row it was handed, or it can never make progress while others hold the
//! pool. [`SpillCharge::force_grow`] admits that one row over the limit. The
//! overshoot is therefore bounded by one row per live operator, every forced
//! admission is counted ([`SpillPool::forced_total`]), and the pool logs the
//! EDGES of exhaustion — once when it first refuses a charge, once when usage
//! falls back below half the limit — not one line per refusal.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// A shared byte budget for in-memory spill buffers.
#[derive(Debug)]
pub struct SpillPool {
    name: &'static str,
    limit: u64,
    used: AtomicU64,
    /// Set while the pool is refusing charges; drives the edge logs.
    exhausted: AtomicBool,
    refused_total: AtomicU64,
    forced_total: AtomicU64,
}

impl SpillPool {
    /// A pool of `limit` bytes (at least 1). `name` labels its log lines.
    pub fn new(name: &'static str, limit: u64) -> Arc<Self> {
        Arc::new(Self {
            name,
            limit: limit.max(1),
            used: AtomicU64::new(0),
            exhausted: AtomicBool::new(false),
            refused_total: AtomicU64::new(0),
            forced_total: AtomicU64::new(0),
        })
    }

    /// The process-wide pool every spilling operator charges by default.
    ///
    /// Sized once, at first use, to [`crate::process_spill_threshold_bytes`] —
    /// the bound that used to be handed to each operator separately is now the
    /// bound on all of them together.
    pub fn global() -> &'static Arc<SpillPool> {
        static GLOBAL: OnceLock<Arc<SpillPool>> = OnceLock::new();
        GLOBAL.get_or_init(|| {
            let limit = crate::spill_budget::process_spill_threshold_bytes();
            tracing::info!(
                limit_bytes = limit,
                "storage: node-wide spill pool sized from the process spill threshold"
            );
            SpillPool::new("node", limit)
        })
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Bytes currently charged by live operators.
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    /// Charges refused because they would have exceeded the limit.
    pub fn refused_total(&self) -> u64 {
        self.refused_total.load(Ordering::Relaxed)
    }

    /// Rows admitted over the limit by the forward-progress floor.
    pub fn forced_total(&self) -> u64 {
        self.forced_total.load(Ordering::Relaxed)
    }

    /// Whether the pool is currently refusing charges.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Acquire)
    }

    /// A new, empty charge against this pool.
    pub fn charge(self: &Arc<Self>) -> SpillCharge {
        SpillCharge {
            pool: Arc::clone(self),
            held: 0,
        }
    }

    fn try_add(&self, bytes: u64) -> bool {
        let granted = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|&next| next <= self.limit)
            })
            .is_ok();
        if !granted {
            self.refused_total.fetch_add(1, Ordering::Relaxed);
            if !self.exhausted.swap(true, Ordering::AcqRel) {
                tracing::warn!(
                    pool = self.name,
                    limit_bytes = self.limit,
                    used_bytes = self.used(),
                    "storage: spill pool exhausted; operators now spill before their own threshold"
                );
            }
        }
        granted
    }

    fn force_add(&self, bytes: u64) {
        self.used.fetch_add(bytes, Ordering::AcqRel);
        self.forced_total.fetch_add(1, Ordering::Relaxed);
    }

    fn sub(&self, bytes: u64) {
        let before = self.used.fetch_sub(bytes, Ordering::AcqRel);
        assert!(
            before >= bytes,
            "spill pool {} released {bytes} bytes while only {before} were charged",
            self.name
        );
        let after = before - bytes;
        if after <= self.limit / 2 && self.exhausted.swap(false, Ordering::AcqRel) {
            tracing::info!(
                pool = self.name,
                limit_bytes = self.limit,
                used_bytes = after,
                "storage: spill pool recovered below half its limit"
            );
        }
    }
}

/// One operator's share of a [`SpillPool`]. Dropping it returns the bytes.
#[derive(Debug)]
pub struct SpillCharge {
    pool: Arc<SpillPool>,
    held: u64,
}

impl SpillCharge {
    /// Charge `bytes` more if the pool has room. On `false` nothing was
    /// charged: the caller must spill what it holds (or use the floor).
    #[must_use = "a refused charge means the caller must spill"]
    pub fn try_grow(&mut self, bytes: u64) -> bool {
        let granted = self.pool.try_add(bytes);
        if granted {
            self.held += bytes;
        }
        granted
    }

    /// The forward-progress floor: charge `bytes` regardless of the limit.
    ///
    /// Only legal while this charge holds nothing — an operator that has
    /// already spilled everything it held and still cannot fit one row.
    /// Counted in [`SpillPool::forced_total`].
    pub fn force_grow(&mut self, bytes: u64) {
        assert_eq!(
            self.held, 0,
            "the spill floor admits one row to an operator holding nothing, not {} bytes more",
            self.held
        );
        self.pool.force_add(bytes);
        self.held += bytes;
    }

    /// Return everything this charge holds (after a spill).
    pub fn release_all(&mut self) {
        if self.held > 0 {
            self.pool.sub(self.held);
            self.held = 0;
        }
    }

    /// Bytes this operator currently holds.
    pub fn held(&self) -> u64 {
        self.held
    }

    /// The pool this charge draws on.
    pub fn pool(&self) -> &Arc<SpillPool> {
        &self.pool
    }
}

impl Drop for SpillCharge {
    fn drop(&mut self) {
        self.release_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_charge_within_the_limit_is_granted_and_returned_on_drop() {
        let pool = SpillPool::new("t", 100);
        let mut c = pool.charge();
        assert!(c.try_grow(60));
        assert_eq!(pool.used(), 60);
        drop(c);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn two_charges_share_one_limit() {
        let pool = SpillPool::new("t", 100);
        let mut a = pool.charge();
        let mut b = pool.charge();
        assert!(a.try_grow(70));
        assert!(
            !b.try_grow(40),
            "the second operator must not push the pool over"
        );
        assert_eq!(pool.used(), 70);
        assert_eq!(b.held(), 0);
        assert_eq!(pool.refused_total(), 1);
        assert!(pool.is_exhausted());
        a.release_all();
        assert!(
            !pool.is_exhausted(),
            "recovery edge clears below half the limit"
        );
        assert!(b.try_grow(40));
    }

    #[test]
    fn the_floor_admits_one_row_to_an_empty_operator_and_counts_it() {
        let pool = SpillPool::new("t", 10);
        let mut a = pool.charge();
        assert!(a.try_grow(10));
        let mut b = pool.charge();
        assert!(!b.try_grow(5));
        b.force_grow(5);
        assert_eq!(pool.used(), 15);
        assert_eq!(pool.forced_total(), 1);
        drop(a);
        drop(b);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    #[should_panic(expected = "the spill floor admits one row")]
    fn the_floor_refuses_an_operator_that_already_holds_bytes() {
        let pool = SpillPool::new("t", 10);
        let mut a = pool.charge();
        assert!(a.try_grow(4));
        a.force_grow(1);
    }
}
