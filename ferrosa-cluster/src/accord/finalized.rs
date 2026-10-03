//! Bounded memory of transactions this replica knows are decided, so a
//! PreAccept that arrives after the decision is refused instead of
//! registering a conflict nothing will ever clear (FMEA CL-33).
//!
//! # The hole this closes
//!
//! A no-write finalize can reach a replica that never registered the
//! transaction: its PreAccept was sent first but is still queued (a paused
//! replica resumes and processes both, in the wrong order). The finalize
//! leaves no `TxnState` behind, so the late PreAccept looked like a new
//! transaction, registered in the conflict index, and sat `PreAccepted`
//! forever. Every later read or snapshot barrier on its keys dep-waited on it
//! and abstained. `prune_applied` opens the same hole for a transaction that
//! did apply: once its state is pruned, a PreAccept delayed past the prune
//! registers it again.
//!
//! # Why refusing is safe
//!
//! A refused PreAccept gets the empty `AccordPreAcceptOK`, the same reply as
//! a replica that could not vote. To the coordinator it is indistinguishable
//! from a lost message, which Accord tolerates by construction, so refusing
//! can never break safety. The only cost is liveness, and only for a
//! PreAccept a coordinator could still use; the retention bound below keeps
//! that set empty in practice.
//!
//! # The bound
//!
//! Two pieces, and a transaction is *decided* if either covers it:
//!
//! - **Tombstones**: an ordered set of exact [`TxnId`]s, recorded when a
//!   no-write finalize lands on an unregistered transaction and when
//!   `prune_applied` forgets an applied one.
//! - **A floor**: every `TxnId` at or below it is decided. It rises in three
//!   ways, and never falls:
//!   1. **Retention** ([`FINALIZED_RETENTION`], 60 s). `advance_horizon(now)`
//!      raises the floor to `now - 60 s` and drops the tombstones below it.
//!      A coordinator stops listening for a PreAccept reply after the
//!      Data-lane RPC timeout (10 s by default) and the HLC drift guard
//!      bounds clock lead to 500 ms, so a PreAccept whose `t0` is 60 s old
//!      cannot be used by anyone: refusing it costs nothing. Dropping a
//!      tombstone below the floor loses nothing, because the floor refuses
//!      that id anyway. **This is why pruning can never drop a record a late
//!      PreAccept still needs.**
//!   2. **Capacity** ([`FINALIZED_CAPACITY`]). Recording past the cap evicts
//!      the oldest tombstone and raises the floor to it, so memory stays
//!      bounded under any load. An eviction that pushes the floor inside the
//!      retention window can refuse a usable PreAccept (a lost message, so
//!      liveness only); it is logged as an edge.
//!   3. **Restart** ([`FinalizedTxns::with_restart_floor`]). The replica does
//!      not replay its Accord log (t_64c1b2d6), so after a restart it has no
//!      tombstones at all. Setting the floor to the HLC's `now` at
//!      construction refuses every transaction minted before the restart,
//!      which covers every one it could have finalized.
//!
//! Memory is therefore at most `FINALIZED_CAPACITY` ids (32 bytes each plus
//! B-tree overhead, about 10 MiB), and in steady state the ids of the last
//! 60 s of finalized-unregistered and applied-then-pruned transactions.
//!
//! # Residual
//!
//! The restart floor assumes the restart took longer than the clock drift
//! between this node and the coordinator (500 ms guard). A coordinator whose
//! clock leads by more than the restart took could mint a `t0` above the
//! floor before the restart; its late PreAccept would still register. The
//! same retention bound assumes a coordinator's clock lags this one by less
//! than `60 s - RPC timeout`; a coordinator lagging more has its PreAccepts
//! refused here, logged as an edge.

use std::collections::BTreeSet;
use std::time::Duration;

use ferrosa_common::accord::{Timestamp, TxnId};

/// How long a decided transaction stays individually remembered. Six times
/// the default Data-lane RPC timeout: see the module docs for the argument.
pub const FINALIZED_RETENTION: Duration = Duration::from_secs(60);

/// Hard cap on remembered tombstones. At the cap the oldest is evicted into
/// the floor, so memory stays bounded at any transaction rate.
pub const FINALIZED_CAPACITY: usize = 250_000;

/// Why a transaction is known to be decided (for the refusal log line).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecidedBy {
    /// An exact tombstone: this replica finalized or pruned this txn.
    Tombstone,
    /// The id is at or below the floor (retention, eviction or restart).
    Floor,
}

/// Bounded record of decided transactions. See the module docs.
#[derive(Debug)]
pub struct FinalizedTxns {
    tombstones: BTreeSet<TxnId>,
    floor: Option<Timestamp>,
    capacity: usize,
    /// Edge state for the eviction warning: set when an eviction first pushes
    /// the floor into the retention window, cleared by the next horizon
    /// advance. Two lines per episode, not one per eviction.
    evicting: bool,
    evicted_since_horizon: u64,
}

impl Default for FinalizedTxns {
    fn default() -> Self {
        Self::new(FINALIZED_CAPACITY)
    }
}

impl FinalizedTxns {
    /// Empty record with no floor and the given tombstone cap.
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "a zero-capacity finalized record forgets every txn at once"
        );
        Self {
            tombstones: BTreeSet::new(),
            floor: None,
            capacity,
            evicting: false,
            evicted_since_horizon: 0,
        }
    }

    /// Start with the floor at `boot_now`: every transaction minted before
    /// this replica (re)started is decided as far as PreAccept is concerned.
    #[must_use]
    pub fn with_restart_floor(mut self, boot_now: Timestamp) -> Self {
        // `node: MAX` so the floor covers every id minted at `boot_now`'s
        // (epoch, time, seq), whichever coordinator minted it.
        self.raise_floor(Timestamp {
            node: u64::MAX,
            ..boot_now
        });
        self
    }

    /// Whether a PreAccept for `txn_id` must be refused, and why.
    pub fn decided(&self, txn_id: &TxnId) -> Option<DecidedBy> {
        if self.floor.is_some_and(|floor| txn_id.0 <= floor) {
            return Some(DecidedBy::Floor);
        }
        self.tombstones
            .contains(txn_id)
            .then_some(DecidedBy::Tombstone)
    }

    /// Remember that `txn_id` is decided. Ids already at or below the floor
    /// are not stored (the floor covers them). Past the cap the oldest
    /// tombstone is evicted and the floor raised to it.
    pub fn record(&mut self, txn_id: TxnId) {
        if self.decided(&txn_id) == Some(DecidedBy::Floor) {
            return;
        }
        self.tombstones.insert(txn_id);
        while self.tombstones.len() > self.capacity {
            let Some(oldest) = self.tombstones.pop_first() else {
                break;
            };
            self.raise_floor(oldest.0);
            self.evicted_since_horizon += 1;
            if !self.evicting {
                self.evicting = true;
                tracing::warn!(
                    capacity = self.capacity,
                    floor = ?oldest.0,
                    "accord: finalized-txn record is full; evicting the oldest into the floor. \
                     PreAccepts at or below the floor are refused even inside the retention window"
                );
            }
        }
        debug_assert!(self.tombstones.len() <= self.capacity);
    }

    /// Advance the retention horizon to `now - FINALIZED_RETENTION`: raise
    /// the floor there and drop every tombstone it now covers. Returns the
    /// number of tombstones dropped.
    pub fn advance_horizon(&mut self, now: Timestamp) -> usize {
        let retention_ns = u64::try_from(FINALIZED_RETENTION.as_nanos()).unwrap_or(u64::MAX);
        let horizon = Timestamp {
            epoch: now.epoch,
            time: now.time.saturating_sub(retention_ns),
            seq: u32::MAX,
            node: u64::MAX,
        };
        self.raise_floor(horizon);
        let floor = self.floor.unwrap_or(horizon);
        let before = self.tombstones.len();
        // Keep only ids strictly above the floor; everything else is covered.
        self.tombstones.retain(|id| id.0 > floor);
        if self.evicting {
            tracing::info!(
                evicted = self.evicted_since_horizon,
                "accord: finalized-txn record evictions since the last horizon advance"
            );
            self.evicting = false;
        }
        self.evicted_since_horizon = 0;
        before - self.tombstones.len()
    }

    /// Number of exact tombstones held.
    pub fn len(&self) -> usize {
        self.tombstones.len()
    }

    /// Whether no exact tombstone is held.
    pub fn is_empty(&self) -> bool {
        self.tombstones.is_empty()
    }

    /// The current floor, if any.
    pub fn floor(&self) -> Option<Timestamp> {
        self.floor
    }

    fn raise_floor(&mut self, to: Timestamp) {
        if self.floor.is_none_or(|floor| to > floor) {
            self.floor = Some(to);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(node: u64, time: u64) -> TxnId {
        TxnId::new(node, Timestamp::synthetic(time))
    }

    fn secs(s: u64) -> u64 {
        s * 1_000_000_000
    }

    #[test]
    fn a_recorded_txn_is_decided_and_its_neighbours_are_not() {
        let mut f = FinalizedTxns::new(8);
        f.record(id(2, 1000));
        assert_eq!(f.decided(&id(2, 1000)), Some(DecidedBy::Tombstone));
        assert_eq!(f.decided(&id(3, 1000)), None);
        assert_eq!(f.decided(&id(2, 999)), None);
    }

    #[test]
    fn capacity_evicts_the_oldest_into_the_floor_so_it_stays_decided() {
        let mut f = FinalizedTxns::new(2);
        f.record(id(1, 10));
        f.record(id(1, 20));
        f.record(id(1, 30));
        assert_eq!(f.len(), 2);
        // Evicted, but still refused: the floor took it over.
        assert_eq!(f.decided(&id(1, 10)), Some(DecidedBy::Floor));
        // A never-seen id below the floor is refused too (lost-message safe).
        assert_eq!(f.decided(&id(9, 5)), Some(DecidedBy::Floor));
        assert_eq!(f.decided(&id(1, 20)), Some(DecidedBy::Tombstone));
        assert_eq!(f.decided(&id(1, 40)), None);
    }

    /// Pruning never drops a record a late PreAccept could still need: every
    /// tombstone the horizon drops is covered by the floor it raised, and
    /// every tombstone inside the window survives.
    #[test]
    fn advancing_the_horizon_drops_only_what_the_floor_covers() {
        let mut f = FinalizedTxns::new(1024);
        let old = id(1, secs(100));
        let young = id(1, secs(150));
        f.record(old);
        f.record(young);
        let dropped = f.advance_horizon(Timestamp::synthetic(secs(170)));
        assert_eq!(dropped, 1);
        assert_eq!(f.decided(&old), Some(DecidedBy::Floor));
        assert_eq!(f.decided(&young), Some(DecidedBy::Tombstone));
        // A fresh txn inside the window is not refused.
        assert_eq!(f.decided(&id(4, secs(160))), None);
    }

    #[test]
    fn the_floor_never_falls() {
        let mut f = FinalizedTxns::new(8).with_restart_floor(Timestamp::synthetic(secs(500)));
        f.advance_horizon(Timestamp::synthetic(secs(100)));
        assert_eq!(f.floor().map(|floor| floor.time), Some(secs(500)));
        assert_eq!(f.decided(&id(1, secs(499))), Some(DecidedBy::Floor));
    }

    #[test]
    fn recording_below_the_floor_stores_nothing() {
        let mut f = FinalizedTxns::new(8).with_restart_floor(Timestamp::synthetic(1000));
        f.record(id(1, 10));
        assert!(f.is_empty());
    }
}
