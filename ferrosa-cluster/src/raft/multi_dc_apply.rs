//! W7.1–W7.5 — Reorder-by-timestamp + idempotent Accord apply path.
//!
//! Sprint 7 / ADR-015. Cross-DC mutations come in via [`RaftOp::AccordApply`]
//! and must apply in HLC timestamp order across every DC's local Raft
//! group, deduplicated by Accord transaction id, and bounded by clock
//! skew. The pieces live here so they can be unit-tested in isolation
//! and so the giant `state_machine.rs` doesn't grow yet another inline
//! state machine.
//!
//! Pieces:
//!
//! - [`ReorderBuffer`] — buffers `RaftOp::AccordApply` entries by their
//!   HLC timestamp until the watermark passes them, then drains them in
//!   ascending order. (W7.2 / I-27.)
//! - [`AppliedTxnLedger`] — `BTreeMap<TxnId, AppliedRecord>`: dedupes
//!   replayed Accord transactions on apply (W7.5 / I-28).
//! - [`max_skew_from_env`] — reads `FERROSA_HLC_MAX_SKEW_MS` (default
//!   200 ms) for the watermark formula in W7.3.
//! - Watermark formula: `watermark = now - max_skew`. Entries with
//!   `entry.hlc <= watermark` are eligible to drain (W7.3); entries
//!   beyond `watermark` stall in the buffer (W7.4).
//!
//! [`RaftOp::AccordApply`]: crate::raft::RaftOp::AccordApply

use std::collections::BTreeMap;
use std::time::Duration;

use ferrosa_common::{AccordTimestamp, TxnId};
use serde::{Deserialize, Serialize};

use crate::raft::RaftOp;

/// Default HLC max skew bound used when `FERROSA_HLC_MAX_SKEW_MS` is
/// unset. Matches Spanner-style ε analogue (ADR-015).
pub const DEFAULT_MAX_SKEW_MS: u64 = 200;

/// Soft alarm threshold for [`ReorderBuffer`] depth. The
/// `RAFT_ACCORD_REORDER_BUFFER_DEPTH` gauge is wired up here in
/// W7.4 — the alarm fires when the buffer holds more than
/// [`REORDER_BUFFER_ALARM_DEPTH`] entries, which is the operator
/// signal that cross-DC writes are stalled.
pub const REORDER_BUFFER_ALARM_DEPTH: usize = 100;

/// Read the configured HLC max skew. Honors
/// `FERROSA_HLC_MAX_SKEW_MS`. Returns [`DEFAULT_MAX_SKEW_MS`] when the
/// env var is missing or unparseable.
pub fn max_skew_from_env() -> Duration {
    match std::env::var("FERROSA_HLC_MAX_SKEW_MS") {
        Ok(v) => v
            .parse::<u64>()
            .map(Duration::from_millis)
            .unwrap_or_else(|_| Duration::from_millis(DEFAULT_MAX_SKEW_MS)),
        Err(_) => Duration::from_millis(DEFAULT_MAX_SKEW_MS),
    }
}

/// One record kept by the idempotent-apply ledger (W7.5 / I-28).
///
/// Stored as the value side of a `BTreeMap<TxnId, AppliedRecord>` so
/// replays of the same transaction short-circuit to a no-op.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppliedRecord {
    /// HLC timestamp under which the txn was originally applied.
    pub hlc: AccordTimestamp,
}

// ---------------------------------------------------------------------------
// AppliedTxnLedger
// ---------------------------------------------------------------------------

/// Idempotent-apply ledger: dedupes replayed [`RaftOp::AccordApply`]
/// entries by Accord transaction id (I-28).
///
/// Bounded by [`Self::gc_older_than`] which trims entries strictly
/// older than `(max_skew × 100)` to keep the map from growing without
/// bound under steady-state cross-DC traffic.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppliedTxnLedger {
    /// Recorded applies, keyed by Accord transaction id. `BTreeMap` so
    /// iteration is deterministic for snapshot serialization.
    entries: BTreeMap<TxnId, AppliedRecord>,
}

impl AppliedTxnLedger {
    /// Empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct txns in the ledger.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// `true` if the ledger has no recorded applies.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Has this transaction id already been applied?
    pub fn contains(&self, txn_id: &TxnId) -> bool {
        self.entries.contains_key(txn_id)
    }

    /// Borrow the recorded apply for a transaction, if any.
    pub fn get(&self, txn_id: &TxnId) -> Option<&AppliedRecord> {
        self.entries.get(txn_id)
    }

    /// Record `txn_id` as applied at `hlc`. Idempotent — a second
    /// insert for the same `txn_id` returns `false` and leaves the
    /// existing record in place.
    ///
    /// Returns `true` iff the insert recorded a new entry.
    pub fn record(&mut self, txn_id: TxnId, hlc: AccordTimestamp) -> bool {
        if self.entries.contains_key(&txn_id) {
            return false;
        }
        self.entries.insert(txn_id, AppliedRecord { hlc });
        true
    }

    /// Drop entries strictly older than `cutoff` (compared against
    /// each record's `hlc`). Bounds memory under steady-state cross-DC
    /// traffic without losing dedupe protection for in-flight txns.
    pub fn gc_older_than(&mut self, cutoff: AccordTimestamp) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, rec| rec.hlc >= cutoff);
        before - self.entries.len()
    }
}

// ---------------------------------------------------------------------------
// ReorderBuffer (W7.2)
// ---------------------------------------------------------------------------

/// Reorder-by-timestamp buffer for `RaftOp::AccordApply` entries.
///
/// Entries with HLC `t` are held until the watermark passes `t`; on
/// [`Self::drain_ready`] the buffer releases entries in ascending HLC
/// order so every replica applies cross-DC mutations in the same order
/// (I-27).
///
/// The buffer is intentionally separate from
/// [`crate::accord::reorder_buffer::ReorderBuffer`] which is the
/// Accord-protocol-level (`t0`) reorder buffer; this one operates at
/// the *Raft state-machine* layer and keys on the committed HLC
/// timestamp of the apply.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ReorderBuffer {
    /// Pending apply entries keyed by HLC timestamp. `BTreeMap` so
    /// drain comes out in ascending order.
    entries: BTreeMap<AccordTimestamp, Vec<RaftOp>>,
    /// Total number of buffered entries across all keys.
    len: usize,
    /// Maximum depth observed since construction (informational).
    high_water_mark: usize,
}

impl ReorderBuffer {
    /// Empty buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of buffered entries.
    pub fn len(&self) -> usize {
        self.len
    }

    /// `true` when no entries are buffered.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Highest depth observed since construction (W7.4 alarm gauge).
    pub fn high_water_mark(&self) -> usize {
        self.high_water_mark
    }

    /// `true` when the buffer has crossed [`REORDER_BUFFER_ALARM_DEPTH`].
    /// Used by the W7.4 metric/alarm path.
    pub fn over_alarm_threshold(&self) -> bool {
        self.len > REORDER_BUFFER_ALARM_DEPTH
    }

    /// Buffer a single Accord-marked entry. The entry is keyed by
    /// `hlc`; calls with strictly increasing `hlc` after a `drain_ready`
    /// release the entries in HLC order.
    ///
    /// Panics if `op` is not [`RaftOp::AccordApply`] — the buffer is
    /// only valid for Accord-marked entries by construction.
    pub fn push(&mut self, hlc: AccordTimestamp, op: RaftOp) {
        debug_assert!(matches!(&op, RaftOp::AccordApply { .. }));
        self.entries.entry(hlc).or_default().push(op);
        self.len += 1;
        if self.len > self.high_water_mark {
            self.high_water_mark = self.len;
        }
    }

    /// Drain every buffered entry whose HLC ≤ `watermark`, in
    /// ascending HLC order.
    pub fn drain_ready(&mut self, watermark: AccordTimestamp) -> Vec<RaftOp> {
        let mut ready = Vec::new();
        // Collect keys at-or-below the watermark.
        let cutoff: Vec<AccordTimestamp> = self
            .entries
            .keys()
            .copied()
            .take_while(|&t| t <= watermark)
            .collect();
        for key in cutoff {
            if let Some(ops) = self.entries.remove(&key) {
                self.len -= ops.len();
                ready.extend(ops);
            }
        }
        ready
    }

    /// Drain everything regardless of watermark, in ascending HLC
    /// order. Used by tests + by the apply path's idempotent-replay
    /// guard (W7.5) so dedupe still records the txn even if the
    /// watermark hasn't advanced yet.
    pub fn drain_all(&mut self) -> Vec<RaftOp> {
        let mut all = Vec::with_capacity(self.len);
        for (_t, ops) in std::mem::take(&mut self.entries) {
            all.extend(ops);
        }
        self.len = 0;
        all
    }

    /// Smallest HLC currently held, if any.
    pub fn min_hlc(&self) -> Option<AccordTimestamp> {
        self.entries.keys().next().copied()
    }
}

// ---------------------------------------------------------------------------
// Watermark formula (W7.3)
// ---------------------------------------------------------------------------

/// Compute the reorder-buffer watermark for the given `now` and
/// `max_skew`. Returns the largest HLC timestamp that's safe to apply
/// — anything strictly above this watermark must stall in the buffer
/// (W7.4).
///
/// Formula: `watermark = max(now - max_skew, 0)`. Saturating
/// subtraction so the watermark never wraps for small `now`.
///
/// `now_micros` is the local HLC's "now" in microseconds; `max_skew`
/// is the configured bound. Both inputs are deterministic in tests
/// (callers pass synthetic values) and real wall-clock in production.
pub fn watermark_for(now_micros: u64, max_skew: Duration) -> AccordTimestamp {
    let skew_us = u64::try_from(max_skew.as_micros()).unwrap_or(u64::MAX);
    let bound = now_micros.saturating_sub(skew_us);
    AccordTimestamp::synthetic(bound)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn op_at(hlc_micros: u64) -> RaftOp {
        let t = AccordTimestamp::synthetic(hlc_micros);
        RaftOp::AccordApply {
            txn_id: TxnId::new(0, t),
            hlc: t,
            mutation: Vec::new(),
        }
    }

    /// W7.2 RED → GREEN. Two `AccordApply` entries pushed in reverse
    /// HLC order drain in ascending order.
    #[test]
    fn apply_buffers_out_of_order_accord_entries() {
        let mut buf = ReorderBuffer::new();
        let t1 = AccordTimestamp::synthetic(100);
        let t2 = AccordTimestamp::synthetic(300);

        buf.push(t2, op_at(300));
        buf.push(t1, op_at(100));
        assert_eq!(buf.len(), 2);

        // Watermark high enough to release both.
        let drained = buf.drain_ready(AccordTimestamp::synthetic(1_000));
        assert_eq!(drained.len(), 2);
        // Must come out in ascending HLC order: t1, t2.
        match (&drained[0], &drained[1]) {
            (RaftOp::AccordApply { hlc: a, .. }, RaftOp::AccordApply { hlc: b, .. }) => {
                assert_eq!(*a, t1, "first drained entry must be earlier hlc");
                assert_eq!(*b, t2, "second drained entry must be later hlc");
            }
            _ => panic!("expected two AccordApply entries"),
        }
        assert!(buf.is_empty());
    }

    /// W7.3 RED → GREEN. The watermark formula is
    /// `now - max_skew`. With `now = 1_000ms` and `max_skew = 200ms`
    /// the watermark releases entries up to `t = 800_000us`.
    #[test]
    fn watermark_advances_with_max_skew_200ms() {
        let max_skew = Duration::from_millis(200);
        // now = 1 second in microseconds.
        let now_us = 1_000_000u64;
        let wm = watermark_for(now_us, max_skew);
        // 1_000_000 - 200_000 = 800_000us.
        assert_eq!(wm, AccordTimestamp::synthetic(800_000));

        // An entry timestamped 700_000us is releasable; one at
        // 900_000us is not.
        let mut buf = ReorderBuffer::new();
        buf.push(AccordTimestamp::synthetic(700_000), op_at(700_000));
        buf.push(AccordTimestamp::synthetic(900_000), op_at(900_000));
        let drained = buf.drain_ready(wm);
        assert_eq!(
            drained.len(),
            1,
            "only the entry below the watermark drains"
        );
        assert_eq!(buf.len(), 1, "the future-skewed entry stalls");
    }

    /// W7.4 RED → GREEN. An entry far in the future stalls until the
    /// watermark catches up, and the alarm flag fires only above
    /// [`REORDER_BUFFER_ALARM_DEPTH`].
    #[test]
    fn reorder_buffer_stalls_above_max_skew() {
        let max_skew = Duration::from_millis(200);
        let now_us = 100_000u64; // 100 ms wall-clock
        let wm = watermark_for(now_us, max_skew);
        // wm = max(100_000 - 200_000, 0) = 0.
        assert_eq!(wm, AccordTimestamp::synthetic(0));

        let mut buf = ReorderBuffer::new();
        // hlc = now + 500ms = 600_000us (500ms in the future).
        let future_hlc = AccordTimestamp::synthetic(600_000);
        buf.push(future_hlc, op_at(600_000));
        let drained = buf.drain_ready(wm);
        assert!(drained.is_empty(), "future-skewed entry must stall");
        assert_eq!(buf.len(), 1, "the entry remains buffered");

        // Below alarm threshold.
        assert!(!buf.over_alarm_threshold());
        // Push past the alarm threshold to confirm the gauge fires.
        for i in 0..=(REORDER_BUFFER_ALARM_DEPTH as u64) {
            let hlc = AccordTimestamp::synthetic(700_000 + i);
            buf.push(hlc, op_at(700_000 + i));
        }
        assert!(
            buf.over_alarm_threshold(),
            "buffer above alarm depth must report over-threshold"
        );
    }

    /// W7.5 RED → GREEN. The applied-txn ledger dedupes replays.
    #[test]
    fn applied_txn_ledger_is_idempotent() {
        let t = AccordTimestamp::synthetic(123);
        let txn = TxnId::new(7, t);
        let mut ledger = AppliedTxnLedger::new();
        assert!(ledger.record(txn, t), "first record must insert");
        assert_eq!(ledger.len(), 1);
        // Replay returns false; ledger size unchanged.
        assert!(!ledger.record(txn, t), "replay must not re-insert");
        assert_eq!(ledger.len(), 1);
        assert!(ledger.contains(&txn));
    }

    /// W7.5 REFACTOR. The ledger drops old entries on
    /// [`AppliedTxnLedger::gc_older_than`], bounding memory under
    /// steady-state cross-DC traffic.
    #[test]
    fn applied_txn_ledger_gc_drops_old_entries() {
        let mut ledger = AppliedTxnLedger::new();
        let old = AccordTimestamp::synthetic(50);
        let recent = AccordTimestamp::synthetic(500);
        ledger.record(TxnId::new(1, old), old);
        ledger.record(TxnId::new(1, recent), recent);
        assert_eq!(ledger.len(), 2);

        let dropped = ledger.gc_older_than(AccordTimestamp::synthetic(200));
        assert_eq!(dropped, 1, "the entry below the cutoff must be dropped");
        assert_eq!(ledger.len(), 1);
        assert!(ledger.contains(&TxnId::new(1, recent)));
        assert!(!ledger.contains(&TxnId::new(1, old)));
    }

    /// `max_skew_from_env` falls back to the documented default.
    #[test]
    fn max_skew_default_when_env_unset() {
        // SAFETY: not parallel-test sensitive — we always restore the
        // var to the prior state.
        let prev = std::env::var("FERROSA_HLC_MAX_SKEW_MS").ok();
        std::env::remove_var("FERROSA_HLC_MAX_SKEW_MS");
        assert_eq!(
            max_skew_from_env(),
            Duration::from_millis(DEFAULT_MAX_SKEW_MS)
        );
        if let Some(v) = prev {
            std::env::set_var("FERROSA_HLC_MAX_SKEW_MS", v);
        }
    }
}
