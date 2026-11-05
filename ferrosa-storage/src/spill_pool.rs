//! Module: the node-wide spill memory pool.
//! Correctness: Correct when the retained bytes every spilling operator holds
//!   in memory, summed over the process, stay within
//!   `limit + live_charges × (floor + one row)` — the pool limit plus each
//!   operator's forward-progress floor (below) — and when no operator spills a
//!   run smaller than its floor except its last.
//! Last revised: 2026-11-05
//! Last changed: The floor is a byte budget per operator (default 1 MiB), not
//!   one row, and an operator over its fair share of an exhausted pool yields.
//!   With a one-row floor, an operator starved by another wrote one run file
//!   per row (PR #651 review). Introduced the same day: spilling operators used
//!   to take `process_spill_threshold_bytes()` EACH, so three concurrent sorts
//!   held one and a half process budgets (streaming audit,
//!   `Streaming.per_operator_thresholds_multiply`).
//!
//! # The unit: retained bytes
//!
//! Every charge is in [`crate::external_sort::SpillRow::retained_bytes`]: a
//! row's own value slots plus the capacity of every heap buffer it owns. It is
//! computed from the row, not measured from the allocator, so it leaves out
//! allocator size-class rounding and the per-node overhead of tree maps. Real
//! resident memory is therefore somewhat above `used()`; the limit is a limit
//! on retained bytes, and is sized with that slack in mind.
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
//! An operator refused by a full pool must still make progress, and it must not
//! do so by writing a run file per row: that keeps memory bounded but turns a
//! busy node's sorts into random small I/O and a huge merge. So every charge
//! has a FLOOR — bytes it may hold over the limit when the pool refuses it.
//! [`SpillCharge::admit`] answers:
//!
//! - [`Admission::Granted`]: the pool had room.
//! - [`Admission::OverLimit`]: the pool refused, but the charge held less than
//!   its floor, so the row was admitted over the limit and counted
//!   ([`SpillPool::forced_total`]).
//! - [`Admission::SpillFirst`]: the pool refused and the charge is at its
//!   floor. Nothing was charged; the operator spills what it holds and asks
//!   again, which then always admits.
//!
//! A starved operator therefore spills runs of at least its floor. The
//! overshoot is bounded by `floor + one row` per live operator.
//!
//! # Fair share
//!
//! The floor stops thrash; it does not stop one operator from holding the whole
//! pool while every other one runs on its floor. While the pool is exhausted, an
//! operator holding more than `max(floor, limit / live_charges)`
//! ([`SpillCharge::over_fair_share`]) spills on its next row, which returns its
//! bytes to the others.
//!
//! # Logging
//!
//! The pool logs the EDGES of exhaustion — once when it first refuses a charge,
//! once when usage falls back below half the limit — not one line per refusal.

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
    /// Charges currently alive: the divisor of the fair share.
    live_charges: AtomicU64,
}

/// The floor a charge gets from [`SpillPool::charge`]: 1 MiB, so a starved
/// operator's runs are at least that large.
pub const DEFAULT_FLOOR_BYTES: u64 = 1024 * 1024;

/// The pool's answer to [`SpillCharge::admit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "SpillFirst means nothing was charged and the caller must spill"]
pub enum Admission {
    /// Charged within the limit.
    Granted,
    /// Charged over the limit, within this charge's floor. Counted.
    OverLimit,
    /// Nothing charged: the charge is at its floor. Spill, then admit again.
    SpillFirst,
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
            live_charges: AtomicU64::new(0),
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

    /// Charges currently alive (one per spilling operator).
    pub fn live_charges(&self) -> u64 {
        self.live_charges.load(Ordering::Acquire)
    }

    /// `limit / live_charges`: what each operator may hold while the pool is
    /// exhausted before it must yield (never below its own floor).
    pub fn fair_share(&self) -> u64 {
        self.limit / self.live_charges().max(1)
    }

    /// Whether the pool is currently refusing charges.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Acquire)
    }

    /// A new, empty charge with the [`DEFAULT_FLOOR_BYTES`] floor.
    pub fn charge(self: &Arc<Self>) -> SpillCharge {
        self.charge_with_floor(DEFAULT_FLOOR_BYTES)
    }

    /// A new, empty charge that may hold up to `floor_bytes` (at least 1) over
    /// the limit when the pool refuses it.
    pub fn charge_with_floor(self: &Arc<Self>, floor_bytes: u64) -> SpillCharge {
        self.live_charges.fetch_add(1, Ordering::AcqRel);
        SpillCharge {
            pool: Arc::clone(self),
            held: 0,
            floor: floor_bytes.max(1),
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

    /// Return `bytes`. Panics if more is returned than was charged: that is an
    /// accounting bug in this module, and continuing would make `used()` wrap
    /// and refuse every operator forever. Reached from `SpillCharge::drop`, so
    /// if it fires while another panic is unwinding the process aborts — a
    /// deliberate choice over silently clamping the count.
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
    /// Bytes this charge may hold over the limit when the pool refuses it.
    floor: u64,
}

impl SpillCharge {
    /// Charge `bytes` for one more buffered row; see [`Admission`].
    ///
    /// A charge holding less than its floor is always admitted, so after a
    /// spill (`held == 0`) the next `admit` never answers `SpillFirst`.
    pub fn admit(&mut self, bytes: u64) -> Admission {
        if self.pool.try_add(bytes) {
            self.held += bytes;
            return Admission::Granted;
        }
        if self.held >= self.floor {
            return Admission::SpillFirst;
        }
        self.pool.force_add(bytes);
        self.held += bytes;
        Admission::OverLimit
    }

    /// Whether this operator should spill now to make room for the others:
    /// the pool is exhausted and this charge holds more than its fair share
    /// (`max(floor, limit / live_charges)`).
    pub fn over_fair_share(&self) -> bool {
        self.pool.is_exhausted() && self.held > self.floor.max(self.pool.fair_share())
    }

    /// This charge's floor in bytes.
    pub fn floor(&self) -> u64 {
        self.floor
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
        self.pool.live_charges.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_charge_within_the_limit_is_granted_and_returned_on_drop() {
        let pool = SpillPool::new("t", 100);
        let mut c = pool.charge();
        assert_eq!(c.admit(60), Admission::Granted);
        assert_eq!(pool.used(), 60);
        drop(c);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn two_charges_share_one_limit() {
        let pool = SpillPool::new("t", 100);
        let mut a = pool.charge_with_floor(1);
        let mut b = pool.charge_with_floor(1);
        assert_eq!(a.admit(70), Admission::Granted);
        assert_eq!(
            b.admit(40),
            Admission::OverLimit,
            "the second operator is refused by the pool and takes its floor"
        );
        assert_eq!(pool.used(), 110);
        assert_eq!(pool.refused_total(), 1);
        assert!(pool.is_exhausted());
        assert_eq!(
            b.admit(1),
            Admission::SpillFirst,
            "past its floor it must spill"
        );
        a.release_all();
        b.release_all();
        assert!(
            !pool.is_exhausted(),
            "recovery edge clears below half the limit"
        );
        assert_eq!(b.admit(40), Admission::Granted);
    }

    #[test]
    fn the_floor_admits_one_row_to_an_empty_operator_and_counts_it() {
        let pool = SpillPool::new("t", 10);
        let mut a = pool.charge();
        assert_eq!(a.admit(10), Admission::Granted);
        let mut b = pool.charge_with_floor(1);
        assert_eq!(b.admit(5), Admission::OverLimit);
        assert_eq!(pool.used(), 15);
        assert_eq!(pool.forced_total(), 1);
        drop(a);
        drop(b);
        assert_eq!(pool.used(), 0);
    }

    /// The thrash the PR #651 review found: with one row of floor, an operator
    /// starved by another spilled a one-row run on every push. It now keeps
    /// buffering up to its floor, so its runs are at least that large.
    #[test]
    fn a_refused_operator_holds_up_to_its_floor_before_it_must_spill() {
        let pool = SpillPool::new("t", 10);
        let mut hog = pool.charge();
        assert_eq!(hog.admit(10), Admission::Granted);
        let mut starved = pool.charge_with_floor(8);
        for held_before in [0, 3, 6] {
            assert_eq!(
                starved.admit(3),
                Admission::OverLimit,
                "holding {held_before} of an 8-byte floor, the row is admitted"
            );
        }
        assert_eq!(starved.held(), 9);
        assert_eq!(
            starved.admit(3),
            Admission::SpillFirst,
            "at its floor the operator must spill before it holds more"
        );
        assert_eq!(starved.held(), 9, "a SpillFirst admission charges nothing");
        assert_eq!(pool.forced_total(), 3);
        starved.release_all();
        assert_eq!(starved.admit(3), Admission::OverLimit);
    }

    #[test]
    fn an_operator_over_its_fair_share_of_an_exhausted_pool_must_yield() {
        let pool = SpillPool::new("t", 100);
        let mut hog = pool.charge_with_floor(1);
        assert_eq!(hog.admit(90), Admission::Granted);
        assert!(
            !hog.over_fair_share(),
            "a pool with room asks nobody to yield"
        );
        let mut other = pool.charge_with_floor(1);
        assert_eq!(other.admit(20), Admission::OverLimit);
        assert!(pool.is_exhausted());
        assert!(
            hog.over_fair_share(),
            "90 bytes is over half of an exhausted 100-byte pool shared by two"
        );
        assert!(!other.over_fair_share());
    }

    #[test]
    fn the_fair_share_never_falls_below_the_floor() {
        let pool = SpillPool::new("t", 10);
        let mut a = pool.charge_with_floor(8);
        let mut b = pool.charge_with_floor(8);
        assert_eq!(a.admit(7), Admission::Granted);
        assert_eq!(b.admit(7), Admission::OverLimit);
        assert!(
            !a.over_fair_share(),
            "7 bytes is over 10/2 but within the 8-byte floor"
        );
    }

    #[test]
    fn live_charges_counts_open_charges() {
        let pool = SpillPool::new("t", 10);
        let a = pool.charge();
        let b = pool.charge();
        assert_eq!(pool.live_charges(), 2);
        drop(a);
        assert_eq!(pool.live_charges(), 1);
        drop(b);
        assert_eq!(pool.live_charges(), 0);
    }
}
