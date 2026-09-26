//! `CancelToken`: a cheap, cloneable cancellation flag shared across threads,
//! with a blockable "closed" channel so a thread parked in a blocking
//! `select!` wakes immediately on cancel instead of waiting out a poll slice.
//!
//! Design: `specs/sstable-write-pump/decisions.md` D7 (crossbeam, blocking
//! `select!`, cancellation as a channel — no polling) and
//! `specs/sstable-write-pump/compaction-cancel-safety.md` C1 (one
//! cancellation token per task, checked at every step).
//!
//! Lives in `ferrosa-common` (not `ferrosa-storage`) so a future SSTable
//! write pump (`ferrosa-sstable`) can share the exact same token type as
//! compaction (`ferrosa-storage`) without either crate depending on the
//! other the wrong way.
//!
//! - [`CancelToken::is_cancelled`] / [`CancelToken::check`] read a single
//!   `Relaxed` atomic — safe to call in a hot loop that never blocks (e.g.
//!   the compaction merge loop, once per partition).
//! - [`CancelToken::closed`] returns a `Receiver<()>` whose only `Sender` is
//!   dropped by [`CancelToken::cancel`]. A thread blocked in `select!` on
//!   `closed()` alongside a data channel wakes at once, with no timeout and
//!   no re-check interval.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};

/// Why a compaction task (or, later, a write-pump segment) was cancelled.
/// `compaction-cancel-safety.md` C1's reason list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CancelReason {
    /// The executor is shutting down (`CompactionExecutor::shutdown`).
    Shutdown = 0,
    /// The table this task was compacting was dropped.
    TableDropped = 1,
    /// The table this task was compacting was truncated.
    Truncated = 2,
    /// An operator explicitly requested the compaction stop.
    Operator = 3,
    /// The local-disk admission check tripped mid-compaction.
    DiskReserve = 4,
    /// A newer compaction task supersedes this one.
    Superseded = 5,
}

impl CancelReason {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => CancelReason::Shutdown,
            1 => CancelReason::TableDropped,
            2 => CancelReason::Truncated,
            3 => CancelReason::Operator,
            4 => CancelReason::DiskReserve,
            _ => CancelReason::Superseded,
        }
    }

    /// Lowercase, stable label suitable for a metric's `reason` tag.
    pub fn label(self) -> &'static str {
        match self {
            CancelReason::Shutdown => "shutdown",
            CancelReason::TableDropped => "table_dropped",
            CancelReason::Truncated => "truncated",
            CancelReason::Operator => "operator",
            CancelReason::DiskReserve => "disk_reserve",
            CancelReason::Superseded => "superseded",
        }
    }
}

impl fmt::Display for CancelReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// The error a cancelled checkpoint returns. Carries the reason so a caller
/// can branch on it (e.g. distinguish an operator stop from a disk-reserve
/// pre-emption) without string matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cancelled(pub CancelReason);

impl fmt::Display for Cancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cancelled: {}", self.0)
    }
}

impl std::error::Error for Cancelled {}

#[derive(Debug)]
struct Inner {
    flag: AtomicBool,
    reason: AtomicU8,
    /// Set once, by the first `cancel()` call. Lets a caller measure the
    /// latency from "cancel requested" to "a checkpoint observed it"
    /// (`compaction_cancel_latency_seconds`).
    cancelled_at: Mutex<Option<Instant>>,
    /// The sole `Sender` for `closed_rx`. Held behind a lock touched only by
    /// `cancel()` (cold path) so cloning and checking the token stay lock-free.
    closed_tx: Mutex<Option<Sender<()>>>,
    closed_rx: Receiver<()>,
}

/// A cheap, cloneable cancellation flag. Every clone shares the same
/// underlying state via `Arc`.
#[derive(Clone, Debug)]
pub struct CancelToken(Arc<Inner>);

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelToken {
    pub fn new() -> Self {
        // Capacity 0 (rendezvous): nothing is ever sent here. The channel
        // exists only to be closed — `cancel()` drops the sole `Sender`,
        // which wakes every thread `select!`-blocked on `closed()` at once.
        let (closed_tx, closed_rx) = crossbeam_channel::bounded(0);
        Self(Arc::new(Inner {
            flag: AtomicBool::new(false),
            reason: AtomicU8::new(0),
            cancelled_at: Mutex::new(None),
            closed_tx: Mutex::new(Some(closed_tx)),
            closed_rx,
        }))
    }

    /// Marks this token cancelled with `reason`. Idempotent: only the first
    /// call records the reason and the cancellation instant; every call
    /// (redundantly) closes the channel.
    pub fn cancel(&self, reason: CancelReason) {
        if self
            .0
            .flag
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.0.reason.store(reason as u8, Ordering::SeqCst);
            *self.0.cancelled_at.lock().expect("cancel token poisoned") = Some(Instant::now());
        }
        // Dropping the sender closes the channel, waking every select!-blocked
        // receiver. `.take()` makes repeated calls a no-op rather than a panic.
        self.0
            .closed_tx
            .lock()
            .expect("cancel token poisoned")
            .take();
    }

    /// A single `Relaxed` atomic load — safe in a hot loop that must never
    /// block (the compaction merge loop, once per partition).
    pub fn is_cancelled(&self) -> bool {
        self.0.flag.load(Ordering::Relaxed)
    }

    /// The reason this token was cancelled, or `None` if it has not been.
    pub fn reason(&self) -> Option<CancelReason> {
        self.is_cancelled()
            .then(|| CancelReason::from_u8(self.0.reason.load(Ordering::SeqCst)))
    }

    /// `Err(Cancelled(reason))` if cancelled, `Ok(())` otherwise. The check
    /// every compaction checkpoint calls.
    pub fn check(&self) -> Result<(), Cancelled> {
        match self.reason() {
            Some(reason) => Err(Cancelled(reason)),
            None => Ok(()),
        }
    }

    /// The instant `cancel()` was first called, or `None` if this token has
    /// never been cancelled. `compaction_cancel_latency_seconds` is the
    /// duration between this and the moment a checkpoint observed it.
    pub fn cancelled_at(&self) -> Option<Instant> {
        *self.0.cancelled_at.lock().expect("cancel token poisoned")
    }

    /// A receiver a `select!` can block on alongside a data channel. It has
    /// no sender that ever sends — the only event it can produce is
    /// `Err(RecvError)` the instant `cancel()` closes the channel, from any
    /// thread, with no polling.
    pub fn closed(&self) -> Receiver<()> {
        self.0.closed_rx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn starts_uncancelled() {
        let token = CancelToken::new();
        assert!(!token.is_cancelled());
        assert!(token.check().is_ok());
        assert!(token.reason().is_none());
        assert!(token.cancelled_at().is_none());
    }

    #[test]
    fn cancel_sets_flag_and_reason() {
        let token = CancelToken::new();
        token.cancel(CancelReason::Operator);
        assert!(token.is_cancelled());
        assert_eq!(token.check(), Err(Cancelled(CancelReason::Operator)));
        assert_eq!(token.reason(), Some(CancelReason::Operator));
    }

    #[test]
    fn first_reason_sticks() {
        let token = CancelToken::new();
        token.cancel(CancelReason::DiskReserve);
        token.cancel(CancelReason::Shutdown);
        assert_eq!(token.reason(), Some(CancelReason::DiskReserve));
    }

    #[test]
    fn cancel_is_idempotent_and_does_not_panic_when_repeated() {
        let token = CancelToken::new();
        token.cancel(CancelReason::Truncated);
        token.cancel(CancelReason::Truncated);
        token.cancel(CancelReason::Operator);
        assert_eq!(token.reason(), Some(CancelReason::Truncated));
    }

    #[test]
    fn clone_shares_state() {
        let token = CancelToken::new();
        let clone = token.clone();
        clone.cancel(CancelReason::Truncated);
        assert!(token.is_cancelled());
        assert_eq!(token.reason(), Some(CancelReason::Truncated));
    }

    #[test]
    fn closed_wakes_a_blocked_select_without_polling() {
        let token = CancelToken::new();
        let closed = token.closed();
        let (data_tx, data_rx) = crossbeam_channel::bounded::<()>(0);
        let handle = thread::spawn(move || {
            crossbeam_channel::select! {
                recv(data_rx) -> _ => panic!("data arrived; cancel should have won"),
                recv(closed) -> _ => (),
            }
        });
        thread::sleep(Duration::from_millis(20));
        token.cancel(CancelReason::Shutdown);
        handle.join().expect("blocked thread should wake on cancel");
        drop(data_tx);
    }

    #[test]
    fn cancelled_at_is_close_to_the_cancel_call() {
        let token = CancelToken::new();
        let before = Instant::now();
        token.cancel(CancelReason::Operator);
        let after = Instant::now();
        let at = token.cancelled_at().expect("cancelled_at set");
        assert!(at >= before && at <= after);
    }

    #[test]
    fn reason_label_round_trips_through_display() {
        for reason in [
            CancelReason::Shutdown,
            CancelReason::TableDropped,
            CancelReason::Truncated,
            CancelReason::Operator,
            CancelReason::DiskReserve,
            CancelReason::Superseded,
        ] {
            assert_eq!(reason.to_string(), reason.label());
        }
    }
}
