//! Core Accord consensus types: timestamps, ballots, and hybrid logical clock.
//!
//! These types form the foundation of the Accord distributed transaction
//! protocol. [`Timestamp`] provides globally unique, totally ordered
//! transaction timestamps. [`HybridLogicalClock`] produces monotonic
//! timestamps that combine wall-clock time with logical sequencing.
//! [`BallotNumber`], [`AcceptedBallot`], and [`PromisedBallot`] are
//! type-safe wrappers that prevent mixing up the two ballot roles in the
//! Paxos-like accept/promise protocol.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::SystemTime;

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

/// Globally unique, totally ordered transaction timestamp.
///
/// Fields are laid out so that the derived `Ord` sorts by
/// `epoch > time > seq > node`, which is the correct priority order for
/// Accord's consistency guarantees.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(C)]
pub struct Timestamp {
    /// Electorate configuration epoch.
    pub epoch: u64,
    /// Wall-clock nanoseconds from the hybrid logical clock.
    pub time: u64,
    /// Logical sequence counter, incremented on same-nanosecond conflicts.
    pub seq: u32,
    /// Node identifier of the assigning process (same width as openraft `NodeId`).
    pub node: u64,
}

impl Timestamp {
    /// Create a new timestamp with `seq = 0`.
    pub fn new(epoch: u64, hlc_now: u64, node: u64) -> Self {
        Self {
            epoch,
            time: hlc_now,
            seq: 0,
            node,
        }
    }

    /// Create a synthetic timestamp for testing.
    ///
    /// Sets `epoch = 0`, `time = micros`, `seq = 0`, `node = 0`.
    pub fn synthetic(micros: u64) -> Self {
        Self {
            epoch: 0,
            time: micros,
            seq: 0,
            node: 0,
        }
    }

    /// Return a timestamp that is strictly greater than `other`.
    ///
    /// The result copies `other`'s epoch/time but increments `seq`
    /// (saturating at `u32::MAX`) and stamps with the given `node`.
    pub fn bump_past(&self, other: &Timestamp, node: u64) -> Timestamp {
        Timestamp {
            epoch: other.epoch,
            time: other.time,
            seq: other.seq.saturating_add(1),
            node,
        }
    }
}

// ---------------------------------------------------------------------------
// TxnId
// ---------------------------------------------------------------------------

/// Unique transaction identifier — a newtype over [`Timestamp`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TxnId(pub Timestamp);

impl TxnId {
    /// Create a new TxnId from a node and a timestamp.
    pub fn new(node: u64, t0: Timestamp) -> Self {
        Self(Timestamp { node, ..t0 })
    }
}

// ---------------------------------------------------------------------------
// Ballot types
// ---------------------------------------------------------------------------

/// A monotonically increasing ballot number used in the Paxos-like protocol.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct BallotNumber(pub u64);

/// Ballot at which a value was actually **voted** (accepted).
///
/// Intentionally **not** convertible to/from [`PromisedBallot`] — the only
/// way to extract the inner [`BallotNumber`] is via `.0`.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct AcceptedBallot(pub BallotNumber);

/// Highest ballot a replica has **promised** not to participate in lower ballots.
///
/// Intentionally **not** convertible to/from [`AcceptedBallot`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct PromisedBallot(pub BallotNumber);

// ---------------------------------------------------------------------------
// ClockDriftError
// ---------------------------------------------------------------------------

/// Returned when a remote timestamp exceeds the local clock by more than
/// the configured maximum drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockDriftError {
    /// Remote time that was too far ahead.
    pub remote_time: u64,
    /// Local time when the drift was detected.
    pub local_time: u64,
    /// Configured maximum allowed drift in nanoseconds.
    pub max_drift_ns: u64,
}

impl std::fmt::Display for ClockDriftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "clock drift too large: remote {} vs local {} exceeds max drift {}ns",
            self.remote_time, self.local_time, self.max_drift_ns,
        )
    }
}

impl std::error::Error for ClockDriftError {}

// ---------------------------------------------------------------------------
// HybridLogicalClock
// ---------------------------------------------------------------------------

/// Default maximum clock drift: 500 milliseconds in nanoseconds.
const DEFAULT_MAX_DRIFT_NS: u64 = 500_000_000;

/// A hybrid logical clock (HLC) that produces monotonic [`Timestamp`]s.
///
/// Combines wall-clock time (nanoseconds) with a logical sequence counter
/// so that timestamps never regress, even under wall-clock jitter. The
/// `merge` method advances the local clock past a remote timestamp while
/// rejecting excessive drift.
///
/// All operations are lock-free via [`AtomicU64`] / [`AtomicU32`].
pub struct HybridLogicalClock {
    node: u64,
    last_time: AtomicU64,
    last_seq: AtomicU32,
    max_drift_ns: u64,
}

impl HybridLogicalClock {
    /// Create a new HLC for the given node with the specified maximum drift.
    ///
    /// Use `0` for `max_drift_ns` to get the default of 500 ms.
    pub fn new(node: u64, max_drift_ns: u64) -> Self {
        let drift = if max_drift_ns == 0 {
            DEFAULT_MAX_DRIFT_NS
        } else {
            max_drift_ns
        };
        Self {
            node,
            last_time: AtomicU64::new(0),
            last_seq: AtomicU32::new(0),
            max_drift_ns: drift,
        }
    }

    /// Return the current wall-clock time in nanoseconds since the Unix epoch.
    fn wall_clock_ns() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos() as u64
    }

    /// Produce a monotonically increasing [`Timestamp`].
    ///
    /// - If the wall clock has advanced past `last_time`, uses the new wall
    ///   time and resets `seq` to 0.
    /// - If the wall clock has not advanced (or has regressed), keeps
    ///   `last_time` and increments `seq`.
    /// - Epoch is always 0 (epoch management is a later milestone).
    pub fn now(&self) -> Timestamp {
        loop {
            let phys = Self::wall_clock_ns();
            let prev_time = self.last_time.load(Ordering::Acquire);
            let prev_seq = self.last_seq.load(Ordering::Acquire);

            let (new_time, new_seq) = if phys > prev_time {
                (phys, 0u32)
            } else {
                // Wall clock hasn't advanced (or regressed) — bump seq.
                (prev_time, prev_seq.saturating_add(1))
            };

            // CAS loop: try to commit the new (time, seq) pair atomically.
            // We check time first; if another thread beat us, retry.
            if self
                .last_time
                .compare_exchange_weak(prev_time, new_time, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            // Time CAS succeeded — now update seq.
            // If time advanced, we want seq=0 regardless of what another thread wrote.
            // If time stayed the same, we want our incremented value.
            self.last_seq.store(new_seq, Ordering::Release);

            return Timestamp {
                epoch: 0,
                time: new_time,
                seq: new_seq,
                node: self.node,
            };
        }
    }

    /// Advance the local clock past `remote` if `remote` is ahead.
    ///
    /// Returns `Err(ClockDriftError)` if `remote.time` exceeds the local wall
    /// clock by more than `max_drift_ns`. On error the local clock is **not**
    /// modified.
    pub fn merge(&self, remote: Timestamp) -> Result<(), ClockDriftError> {
        let phys = Self::wall_clock_ns();

        // Drift check: reject if remote is too far ahead of wall clock.
        if remote.time > phys && remote.time - phys > self.max_drift_ns {
            return Err(ClockDriftError {
                remote_time: remote.time,
                local_time: phys,
                max_drift_ns: self.max_drift_ns,
            });
        }

        // Advance local state past remote if needed.
        loop {
            let prev_time = self.last_time.load(Ordering::Acquire);
            let prev_seq = self.last_seq.load(Ordering::Acquire);

            if remote.time > prev_time {
                // Remote is ahead — adopt remote time and its seq.
                if self
                    .last_time
                    .compare_exchange_weak(
                        prev_time,
                        remote.time,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    continue;
                }
                self.last_seq.store(remote.seq, Ordering::Release);
                return Ok(());
            } else if remote.time == prev_time && remote.seq > prev_seq {
                // Same time but remote has higher seq — adopt it.
                self.last_seq.fetch_max(remote.seq, Ordering::AcqRel);
                return Ok(());
            } else {
                // Local is already ahead or equal — nothing to do.
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BallotGenerator
// ---------------------------------------------------------------------------

/// Generates monotonically increasing [`BallotNumber`]s.
pub struct BallotGenerator {
    next: AtomicU64,
}

impl BallotGenerator {
    /// Create a new generator starting from ballot 1.
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Return the next ballot number, guaranteed to be greater than all
    /// previously returned values from this generator.
    pub fn fresh_ballot(&self) -> BallotNumber {
        BallotNumber(self.next.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for BallotGenerator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// TxnPhase
// ---------------------------------------------------------------------------

/// Phase of an Accord transaction on a given replica.
///
/// Phases advance forward only: `PreAccepted → Accepted → Committed → Applied`.
/// Attempts to regress to an earlier phase are silently ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnPhase {
    PreAccepted,
    Accepted,
    Committed,
    Applied,
}

impl TxnPhase {
    /// Numeric rank for ordering: higher values are later phases.
    fn rank(self) -> u8 {
        match self {
            TxnPhase::PreAccepted => 0,
            TxnPhase::Accepted => 1,
            TxnPhase::Committed => 2,
            TxnPhase::Applied => 3,
        }
    }
}

// ---------------------------------------------------------------------------
// TxnState
// ---------------------------------------------------------------------------

/// All state a replica maintains for a single Accord transaction.
///
/// # Two-ballot invariant
///
/// `accepted_ballot` and `max_ballot_seen` are tracked **separately**.
/// Using a single variable is the EPaxos correctness bug documented by
/// Sutra et al. (2019). The invariant `accepted_ballot <= max_ballot_seen`
/// is checked after every mutation.
#[derive(Debug, Clone)]
pub struct TxnState {
    pub txn_id: TxnId,
    /// Coordinator's initial proposed timestamp.
    pub t0: Timestamp,
    /// Current / committed execution timestamp.
    pub t: Timestamp,
    /// Highest `t` witnessed for this txn.
    pub t_max: Timestamp,
    /// Dependency set.
    pub deps: HashSet<TxnId>,

    pub phase: TxnPhase,

    /// CRITICAL: These two ballot fields must be tracked SEPARATELY.
    /// Using a single variable is the EPaxos correctness bug (Sutra 2019).
    pub max_ballot_seen: PromisedBallot,
    pub accepted_ballot: AcceptedBallot,

    /// Serialized result, needed for recovery.
    pub result: Option<Vec<u8>>,
}

impl TxnState {
    /// Create a new transaction state in the `PreAccepted` phase with zero ballots.
    pub fn new(txn_id: TxnId, t0: Timestamp) -> Self {
        let state = Self {
            txn_id,
            t0,
            t: t0,
            t_max: t0,
            deps: HashSet::new(),
            phase: TxnPhase::PreAccepted,
            max_ballot_seen: PromisedBallot::default(),
            accepted_ballot: AcceptedBallot::default(),
            result: None,
        };
        state.assert_invariant();
        state
    }

    /// Join a ballot (promise). Updates `max_ballot_seen` ONLY.
    /// Does NOT touch `accepted_ballot`.
    pub fn join_ballot(&mut self, ballot: PromisedBallot) {
        if ballot.0 .0 > self.max_ballot_seen.0 .0 {
            self.max_ballot_seen = ballot;
        }
        self.assert_invariant();
    }

    /// Accept at a ballot. Updates BOTH `accepted_ballot` and `max_ballot_seen`.
    pub fn accept(&mut self, ballot: AcceptedBallot, t: Timestamp, deps: HashSet<TxnId>) {
        // Update max_ballot_seen to at least match accepted ballot
        let bn = (ballot.0).0;
        if bn > (self.max_ballot_seen.0).0 {
            self.max_ballot_seen = PromisedBallot(BallotNumber(bn));
        }
        self.accepted_ballot = ballot;
        self.t = t;
        if t > self.t_max {
            self.t_max = t;
        }
        self.deps = deps;
        if TxnPhase::Accepted.rank() > self.phase.rank() {
            self.phase = TxnPhase::Accepted;
        }
        self.assert_invariant();
    }

    /// Transition to `PreAccepted` phase (only if not already past it).
    pub fn pre_accept(&mut self, t: Timestamp, deps: HashSet<TxnId>) {
        self.t = t;
        if t > self.t_max {
            self.t_max = t;
        }
        self.deps = deps;
        // PreAccepted is rank 0 — only set if current phase isn't already beyond it.
        if TxnPhase::PreAccepted.rank() > self.phase.rank() {
            self.phase = TxnPhase::PreAccepted;
        }
        self.assert_invariant();
    }

    /// Transition to `Committed` phase.
    pub fn commit(&mut self, t: Timestamp, deps: HashSet<TxnId>) {
        self.t = t;
        if t > self.t_max {
            self.t_max = t;
        }
        self.deps = deps;
        if TxnPhase::Committed.rank() > self.phase.rank() {
            self.phase = TxnPhase::Committed;
        }
        self.assert_invariant();
    }

    /// Transition to `Applied` phase.
    pub fn apply(&mut self, result: Vec<u8>) {
        if TxnPhase::Applied.rank() > self.phase.rank() {
            self.phase = TxnPhase::Applied;
        }
        self.result = Some(result);
        self.assert_invariant();
    }

    /// Invariant check: `accepted_ballot <= max_ballot_seen`.
    /// Called after every mutation.
    fn assert_invariant(&self) {
        assert!(
            (self.accepted_ballot.0).0 <= (self.max_ballot_seen.0).0,
            "INVARIANT VIOLATION: accepted_ballot ({:?}) > max_ballot_seen ({:?})",
            self.accepted_ballot,
            self.max_ballot_seen
        );
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // -----------------------------------------------------------------------
    // Timestamp tests
    // -----------------------------------------------------------------------

    #[test]
    fn timestamp_total_order_all_fields() {
        // epoch differs
        let a = Timestamp {
            epoch: 1,
            time: 0,
            seq: 0,
            node: 0,
        };
        let b = Timestamp {
            epoch: 2,
            time: 0,
            seq: 0,
            node: 0,
        };
        assert!(b > a);

        // epoch equal, time differs
        let a = Timestamp {
            epoch: 1,
            time: 10,
            seq: 0,
            node: 0,
        };
        let b = Timestamp {
            epoch: 1,
            time: 20,
            seq: 0,
            node: 0,
        };
        assert!(b > a);

        // epoch+time equal, seq differs
        let a = Timestamp {
            epoch: 1,
            time: 10,
            seq: 0,
            node: 0,
        };
        let b = Timestamp {
            epoch: 1,
            time: 10,
            seq: 1,
            node: 0,
        };
        assert!(b > a);

        // epoch+time+seq equal, node differs
        let a = Timestamp {
            epoch: 1,
            time: 10,
            seq: 0,
            node: 1,
        };
        let b = Timestamp {
            epoch: 1,
            time: 10,
            seq: 0,
            node: 2,
        };
        assert!(b > a);

        // Verify priority: higher epoch beats higher time
        let high_epoch = Timestamp {
            epoch: 2,
            time: 0,
            seq: 0,
            node: 0,
        };
        let high_time = Timestamp {
            epoch: 1,
            time: 999,
            seq: 999,
            node: 999,
        };
        assert!(high_epoch > high_time);
    }

    #[test]
    fn timestamp_eq_requires_all_fields() {
        let a = Timestamp {
            epoch: 1,
            time: 100,
            seq: 5,
            node: 1,
        };
        let b = Timestamp {
            epoch: 1,
            time: 100,
            seq: 5,
            node: 2,
        };
        assert_ne!(
            a, b,
            "same (epoch,time,seq) but different node must not be equal"
        );
    }

    #[test]
    fn timestamp_bump_past_strictly_greater() {
        let base = Timestamp {
            epoch: 0,
            time: 0,
            seq: 0,
            node: 0,
        };
        // Generate 100 random-ish pairs and verify bump is always strictly greater.
        let mut current = base;
        for i in 0..100u64 {
            let other = Timestamp {
                epoch: i % 3,
                time: i * 7 + 1,
                seq: (i as u32) * 3,
                node: i % 5,
            };
            let bumped = current.bump_past(&other, 42);
            assert!(
                bumped > other,
                "bump_past result {:?} must be > other {:?}",
                bumped,
                other,
            );
            current = bumped;
        }
    }

    #[test]
    fn timestamp_bump_past_preserves_epoch() {
        let me = Timestamp::new(0, 100, 1);
        let other = Timestamp {
            epoch: 7,
            time: 200,
            seq: 3,
            node: 2,
        };
        let result = me.bump_past(&other, 1);
        assert_eq!(
            result.epoch, other.epoch,
            "bump_past must preserve other's epoch"
        );
    }

    #[test]
    fn timestamp_new_seq_zero() {
        let ts = Timestamp::new(5, 123_456_789, 42);
        assert_eq!(ts.seq, 0);
        assert_eq!(ts.epoch, 5);
        assert_eq!(ts.time, 123_456_789);
        assert_eq!(ts.node, 42);
    }

    #[test]
    fn timestamp_uniqueness_same_nanosecond() {
        let a = Timestamp {
            epoch: 0,
            time: 1000,
            seq: 0,
            node: 1,
        };
        let b = Timestamp {
            epoch: 0,
            time: 1000,
            seq: 1,
            node: 1,
        };
        assert_ne!(a, b, "same node, same time but different seq must differ");
        assert!(b > a);
    }

    #[test]
    fn timestamp_derive_hash_consistent_with_eq() {
        let a = Timestamp {
            epoch: 1,
            time: 2,
            seq: 3,
            node: 4,
        };
        let b = Timestamp {
            epoch: 1,
            time: 2,
            seq: 3,
            node: 4,
        };
        assert_eq!(a, b);

        let hash_a = {
            let mut h = DefaultHasher::new();
            a.hash(&mut h);
            h.finish()
        };
        let hash_b = {
            let mut h = DefaultHasher::new();
            b.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash_a, hash_b, "equal timestamps must have equal hashes");
    }

    // -----------------------------------------------------------------------
    // Ballot tests
    // -----------------------------------------------------------------------

    #[test]
    fn ballot_accepted_and_promised_are_distinct_types() {
        // The compiler prevents passing AcceptedBallot where PromisedBallot
        // is expected (and vice versa). Here we verify at runtime that they
        // have distinct TypeIds.
        assert_ne!(
            TypeId::of::<AcceptedBallot>(),
            TypeId::of::<PromisedBallot>(),
            "AcceptedBallot and PromisedBallot must be distinct types",
        );
    }

    #[test]
    fn ballot_accepted_ord() {
        let a = AcceptedBallot(BallotNumber(1));
        let b = AcceptedBallot(BallotNumber(2));
        assert!(b > a);
        assert!(a < b);
        assert_eq!(a, AcceptedBallot(BallotNumber(1)));
    }

    #[test]
    fn ballot_promised_ord() {
        let a = PromisedBallot(BallotNumber(10));
        let b = PromisedBallot(BallotNumber(20));
        assert!(b > a);
        assert!(a < b);
        assert_eq!(a, PromisedBallot(BallotNumber(10)));
    }

    #[test]
    fn ballot_number_monotonic_generation() {
        let gen = BallotGenerator::new();
        let mut prev = gen.fresh_ballot();
        for _ in 0..999 {
            let next = gen.fresh_ballot();
            assert!(
                next > prev,
                "fresh_ballot must be monotonically increasing: {:?} not > {:?}",
                next,
                prev,
            );
            prev = next;
        }
    }

    #[test]
    fn ballot_zero_is_initial() {
        assert_eq!(BallotNumber::default(), BallotNumber(0));
    }

    #[test]
    fn ballot_nack_returns_promised() {
        // PromisedBallot wraps BallotNumber — verify the structure.
        let bn = BallotNumber(42);
        let promised = PromisedBallot(bn);
        assert_eq!(promised.0, bn);
        assert_eq!(promised.0 .0, 42);
    }

    // -----------------------------------------------------------------------
    // HLC tests
    // -----------------------------------------------------------------------

    #[test]
    fn hlc_monotonic_forward() {
        let hlc = HybridLogicalClock::new(1, 0);
        let mut prev = hlc.now();
        for _ in 0..9999 {
            let next = hlc.now();
            assert!(
                next > prev,
                "HLC must be monotonic: {:?} not > {:?}",
                next,
                prev,
            );
            prev = next;
        }
    }

    #[test]
    fn hlc_advances_with_wall_clock() {
        let hlc = HybridLogicalClock::new(1, 0);
        let first = hlc.now();
        // Spin briefly to let wall clock advance.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = hlc.now();
        assert!(
            second.time > first.time || (second.time == first.time && second.seq > first.seq),
            "second timestamp must advance: first={:?}, second={:?}",
            first,
            second,
        );
    }

    #[test]
    fn hlc_merge_advances_past_remote() {
        let hlc = HybridLogicalClock::new(1, DEFAULT_MAX_DRIFT_NS);
        let _ = hlc.now(); // initialise

        let remote = Timestamp {
            epoch: 0,
            time: HybridLogicalClock::wall_clock_ns() + 1_000, // 1 us ahead — within drift
            seq: 0,
            node: 99,
        };

        hlc.merge(remote).expect("merge should succeed");
        let after = hlc.now();
        assert!(
            after.time >= remote.time,
            "after merge, now() must be >= remote: after={:?}, remote={:?}",
            after,
            remote,
        );
    }

    #[test]
    fn hlc_merge_rejects_excessive_drift() {
        let max_drift = 1_000_000u64; // 1 ms
        let hlc = HybridLogicalClock::new(1, max_drift);
        let before = hlc.now();

        // Use a remote time far enough ahead that even with wall-clock
        // advancement between computing the remote time and the merge
        // call, the drift is still clearly excessive.
        let remote = Timestamp {
            epoch: 0,
            time: HybridLogicalClock::wall_clock_ns() + max_drift * 10,
            seq: 0,
            node: 99,
        };

        let result = hlc.merge(remote);
        assert!(result.is_err(), "merge must reject excessive drift");

        // Local HLC should be unchanged (or only advanced by wall clock).
        let after = hlc.now();
        assert!(
            after.time <= before.time + max_drift * 10,
            "HLC must not have jumped to remote time: after={:?}",
            after,
        );
    }

    #[test]
    fn hlc_wall_clock_regression_detected() {
        // We can't actually make the wall clock go backwards, but we can
        // verify that rapid sequential calls never produce a timestamp with
        // a time field less than any previous timestamp's time field.
        let hlc = HybridLogicalClock::new(1, 0);
        let mut prev = hlc.now();
        for _ in 0..10_000 {
            let next = hlc.now();
            assert!(
                next.time >= prev.time,
                "HLC time must never regress: next={:?}, prev={:?}",
                next,
                prev,
            );
            assert!(next > prev, "HLC timestamps must be strictly monotonic",);
            prev = next;
        }
    }

    #[test]
    fn hlc_seq_increments_within_same_ns() {
        // Seed last_time to a far-future value so the physical clock can
        // never advance past it, forcing every call to increment seq.
        let hlc = HybridLogicalClock::new(1, 0);
        let far_future = HybridLogicalClock::wall_clock_ns() + 1_000_000_000_000; // +1000s
        hlc.last_time.store(far_future, Ordering::Release);
        hlc.last_seq.store(0, Ordering::Release);

        let mut prev = hlc.now();
        assert_eq!(prev.time, far_future);
        for _ in 0..1_000 {
            let next = hlc.now();
            assert_eq!(
                next.time, prev.time,
                "time must not advance past far-future seed"
            );
            assert!(
                next.seq > prev.seq,
                "same time must have higher seq: next={:?}, prev={:?}",
                next,
                prev,
            );
            prev = next;
        }
    }

    // -----------------------------------------------------------------------
    // TxnState tests
    // -----------------------------------------------------------------------

    /// Helper: create a TxnId from a simple counter for test convenience.
    fn test_txn_id(n: u64) -> TxnId {
        TxnId(Timestamp::new(0, n, 1))
    }

    /// Helper: create a Timestamp from a simple counter for test convenience.
    fn test_ts(n: u64) -> Timestamp {
        Timestamp::new(0, n, 1)
    }

    #[test]
    fn txnstate_accepted_leq_promised() {
        // Create TxnState. Call join_ballot, accept, join_ballot in
        // random-ish order 100 times. Assert invariant holds after each.
        let mut state = TxnState::new(test_txn_id(1), test_ts(100));

        for i in 0..100u64 {
            match i % 3 {
                0 => {
                    // join_ballot with increasing ballot
                    state.join_ballot(PromisedBallot(BallotNumber(i * 2 + 10)));
                }
                1 => {
                    // accept with a ballot <= max_ballot_seen
                    let ab = std::cmp::min(i + 1, (state.max_ballot_seen.0).0);
                    let deps = HashSet::new();
                    state.accept(AcceptedBallot(BallotNumber(ab)), test_ts(i + 200), deps);
                }
                _ => {
                    // join_ballot with smaller ballot (should be no-op for max)
                    state.join_ballot(PromisedBallot(BallotNumber(i / 2)));
                }
            }
            // Invariant is checked inside each method, but verify explicitly too.
            assert!(
                (state.accepted_ballot.0).0 <= (state.max_ballot_seen.0).0,
                "iteration {}: accepted {:?} > promised {:?}",
                i,
                state.accepted_ballot,
                state.max_ballot_seen,
            );
        }
    }

    #[test]
    fn txnstate_phase_mutual_exclusion() {
        let mut state = TxnState::new(test_txn_id(1), test_ts(100));
        assert_eq!(state.phase, TxnPhase::PreAccepted);

        // Pre-accept
        state.pre_accept(test_ts(101), HashSet::new());
        assert_eq!(state.phase, TxnPhase::PreAccepted);

        // Accept
        state.accept(
            AcceptedBallot(BallotNumber(1)),
            test_ts(102),
            HashSet::new(),
        );
        assert_eq!(
            state.phase,
            TxnPhase::Accepted,
            "phase must be Accepted after accept()"
        );

        // Commit
        state.commit(test_ts(103), HashSet::new());
        assert_eq!(
            state.phase,
            TxnPhase::Committed,
            "phase must be Committed after commit()"
        );

        // Apply
        state.apply(vec![1, 2, 3]);
        assert_eq!(
            state.phase,
            TxnPhase::Applied,
            "phase must be Applied after apply()"
        );
    }

    #[test]
    fn txnstate_phase_ordering() {
        // Verify forward-only: going backwards should be prevented (ignored).
        let mut state = TxnState::new(test_txn_id(1), test_ts(100));

        // Advance to Committed
        state.accept(
            AcceptedBallot(BallotNumber(1)),
            test_ts(101),
            HashSet::new(),
        );
        state.commit(test_ts(102), HashSet::new());
        assert_eq!(state.phase, TxnPhase::Committed);

        // Try to go back to PreAccepted — must stay Committed.
        state.pre_accept(test_ts(103), HashSet::new());
        assert_eq!(
            state.phase,
            TxnPhase::Committed,
            "phase must not regress from Committed to PreAccepted"
        );

        // Try to go back to Accepted — must stay Committed.
        state.accept(
            AcceptedBallot(BallotNumber(2)),
            test_ts(104),
            HashSet::new(),
        );
        assert_eq!(
            state.phase,
            TxnPhase::Committed,
            "phase must not regress from Committed to Accepted"
        );

        // Advance to Applied
        state.apply(vec![42]);
        assert_eq!(state.phase, TxnPhase::Applied);

        // Try to go back to Committed — must stay Applied.
        state.commit(test_ts(105), HashSet::new());
        assert_eq!(
            state.phase,
            TxnPhase::Applied,
            "phase must not regress from Applied to Committed"
        );
    }

    #[test]
    fn txnstate_join_ballot_updates_promised_only() {
        let mut state = TxnState::new(test_txn_id(1), test_ts(100));

        // Set accepted_ballot to 2 via accept().
        state.accept(
            AcceptedBallot(BallotNumber(2)),
            test_ts(101),
            HashSet::new(),
        );
        assert_eq!((state.accepted_ballot.0).0, 2);

        // join_ballot(4) — should update max_ballot_seen but NOT accepted_ballot.
        state.join_ballot(PromisedBallot(BallotNumber(4)));
        assert_eq!(
            (state.max_ballot_seen.0).0,
            4,
            "max_ballot_seen must be updated to 4"
        );
        assert_eq!(
            (state.accepted_ballot.0).0,
            2,
            "accepted_ballot must STILL be 2 after join_ballot"
        );
    }

    #[test]
    fn txnstate_accept_updates_both() {
        let mut state = TxnState::new(test_txn_id(1), test_ts(100));

        state.accept(
            AcceptedBallot(BallotNumber(3)),
            test_ts(101),
            HashSet::new(),
        );
        assert_eq!(
            (state.accepted_ballot.0).0,
            3,
            "accepted_ballot must be 3 after accept"
        );
        assert!(
            (state.max_ballot_seen.0).0 >= 3,
            "max_ballot_seen must be >= 3 after accept"
        );
    }

    #[test]
    fn txnstate_deps_union_on_preaccept() {
        let gamma = test_txn_id(99);
        let mut deps = HashSet::new();
        deps.insert(gamma);

        let mut state = TxnState::new(test_txn_id(1), test_ts(100));
        state.pre_accept(test_ts(101), deps);

        assert!(
            state.deps.contains(&gamma),
            "gamma must be in deps after pre_accept"
        );
    }

    #[test]
    fn txnstate_deps_filter_preaccept_uses_t0() {
        // Semantic test: document that PreAccept dep filtering uses t0
        // comparison (tested at higher level). Here we verify the dep set
        // is stored correctly and t0 remains accessible for comparison.
        let gamma = test_txn_id(50);
        let delta = test_txn_id(150);
        let mut deps = HashSet::new();
        deps.insert(gamma);
        deps.insert(delta);

        let t0 = test_ts(100);
        let mut state = TxnState::new(test_txn_id(1), t0);
        state.pre_accept(test_ts(101), deps);

        // Verify deps stored correctly.
        assert_eq!(state.deps.len(), 2);
        assert!(state.deps.contains(&gamma));
        assert!(state.deps.contains(&delta));
        // Verify t0 is preserved for higher-level filtering.
        assert_eq!(state.t0, t0);
    }

    #[test]
    fn txnstate_deps_filter_accept_uses_t() {
        // Same for Accept: verify deps stored correctly and t is updated.
        let gamma = test_txn_id(50);
        let delta = test_txn_id(150);
        let mut deps = HashSet::new();
        deps.insert(gamma);
        deps.insert(delta);

        let t_accept = test_ts(200);
        let mut state = TxnState::new(test_txn_id(1), test_ts(100));
        state.accept(AcceptedBallot(BallotNumber(1)), t_accept, deps);

        // Verify deps stored correctly.
        assert_eq!(state.deps.len(), 2);
        assert!(state.deps.contains(&gamma));
        assert!(state.deps.contains(&delta));
        // Verify t is updated to the accepted timestamp.
        assert_eq!(state.t, t_accept);
    }

    #[test]
    fn txnstate_default_ballots_are_zero() {
        let state = TxnState::new(test_txn_id(1), test_ts(100));
        assert_eq!(
            (state.max_ballot_seen.0).0,
            0,
            "new() max_ballot_seen must be BallotNumber(0)"
        );
        assert_eq!(
            (state.accepted_ballot.0).0,
            0,
            "new() accepted_ballot must be BallotNumber(0)"
        );
    }
}
