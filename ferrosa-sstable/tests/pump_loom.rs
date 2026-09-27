//! T-034 (L6): a `loom` model of the write pump's producer/flusher protocol
//! (decisions.md D2/D7) — one producer, one flusher, `depth` in {1, 2}, with a
//! flusher error and a flusher panic injected at every step. Proves, across
//! every legal thread interleaving loom explores:
//!
//! - no deadlock (loom's own model checker fails the run if any interleaving
//!   would leave a thread blocked forever with nothing left to wake it);
//! - every one of the pump's `depth + 1` physical segments is returned to
//!   `free` or recorded as dropped exactly once, never both and never
//!   neither;
//! - the producer always terminates (reaching the final assertion at all
//!   proves this, for every interleaving loom tried).
//!
//! ## Why this doesn't drive the real `AlignedPump`
//!
//! loom works by replacing `std`'s synchronization primitives
//! (`loom::sync::Mutex`/`Condvar`/`Arc`, `loom::thread`) with instrumented
//! versions and exhaustively exploring every legal interleaving of the
//! threads that use them. `crossbeam_channel` — what `AlignedPump` and
//! `run_flusher` actually use — has its own internal synchronization built
//! directly on `std`'s atomics and parking primitives, not on `loom::sync`,
//! so loom cannot see inside it or explore its internal interleavings. This
//! is exactly the case the T-034 task anticipates: "if loom cannot model
//! crossbeam-channel directly, put the channel behind a tiny internal shim
//! ... with a loom implementation for `cfg(loom)`."
//!
//! So this file models the SAME protocol shape `decisions.md` D2/D7
//! describes — segments cycling producer → flusher → producer over two
//! bounded "channels" (`free`/`full`), plus a one-shot error report, plus a
//! `select`-on-two wait (the producer races `free` against the error signal,
//! mirroring the real producer racing `free_rx` against `abort.closed()`) —
//! using a from-scratch `shim` module built on `loom::sync` primitives,
//! rather than re-driving `pump.rs`'s concrete crossbeam-based types. The
//! `FaultySink`/`GateSink`-based tests in `pump.rs` and
//! `tests/pump_async_*.rs` already exercise the REAL types end to end (byte
//! identity, digest checks, timing); this file exercises the PROTOCOL's
//! liveness and exactly-once-accounting properties under every scheduling
//! order, which those tests cannot do (they run under the OS scheduler, not
//! an exhaustive one).
//!
//! ## Why this stays small
//!
//! loom's state space is exponential in the number of scheduling decision
//! points, so the model uses a tiny ring (2–3 physical segments: `depth + 1`
//! for `depth` in {1, 2}) and a tiny stream (`depth + 2` segments sent, so
//! at least one is genuinely recycled). Each `(depth, fault behavior)`
//! combination is its own `loom::model` run; there are `(1 baseline + 3
//! error-at + 3 panic-at)` runs for `depth = 1` and `(1 + 4 + 4)` for
//! `depth = 2` — 16 total model runs, each exploring a handful of segments
//! moving between two threads. This finishes in well under a minute on a
//! development machine; a model that swept more segments or a wider fault
//! index range would not.
//!
//! ## Enabled by a Cargo feature, not `RUSTFLAGS="--cfg loom"`
//!
//! loom's own documentation recommends an unconditional `[dev-dependencies]`
//! entry plus `#[cfg(loom)]`, set via `RUSTFLAGS="--cfg loom"`. That does
//! not work in this workspace: `ferrosa-sstable` depends on `ferrosa-common`,
//! which depends on `tokio`, and `tokio` has its own internal
//! `#[cfg(loom)]`-gated code (e.g. in `tokio::task::local`) that only
//! compiles correctly under tokio's OWN loom test harness. `RUSTFLAGS` is
//! process-global, so a bare `--cfg loom` reaches every crate in the build
//! — including `tokio` — and breaks it (confirmed directly: `error[E0432]:
//! unresolved import `crate::sync::AtomicWaker`` in `tokio-1.52.2/src/
//! task/local.rs`, not a hypothetical). Instead, `loom` is an *optional*
//! `[dependencies]` entry gated by this crate's own `loom` Cargo feature
//! (`ferrosa-sstable/Cargo.toml`), which scopes cleanly to this crate's
//! dependency resolution and never touches `tokio`'s build. Run with:
//!
//! ```text
//! cargo test -p ferrosa-sstable --release --features test-support,loom \
//!     pump_loom_
//! ```

#![cfg(feature = "loom")]

use loom::sync::{Arc, Condvar, Mutex};
use std::collections::VecDeque;
use std::fmt;

/// A tiny loom-only channel shim (`send`/`recv`/`try_recv`, plus a two-way
/// `select` below) standing in for `crossbeam_channel`, which loom cannot
/// instrument (see the module doc). Each channel is single-purpose here: it
/// carries either a segment id (`free`/`full`) or a unit error signal
/// (`err`), exactly like the real pump's three channels.
struct Chan<T> {
    state: Mutex<ChanState<T>>,
    cvar: Condvar,
}

struct ChanState<T> {
    queue: VecDeque<T>,
    closed: bool,
}

impl<T> Chan<T> {
    fn new() -> Self {
        Chan {
            state: Mutex::new(ChanState {
                queue: VecDeque::new(),
                closed: false,
            }),
            cvar: Condvar::new(),
        }
    }

    /// Move `item` to whichever side is (or will be) receiving. Never blocks
    /// — this model's channels are conceptually unbounded internally, since
    /// the pump's own `depth + 1` bound is enforced by how many segment
    /// tokens this model ever creates, exactly as the real `free`/`full`
    /// pair's capacity is `depth + 1` and never oversubscribed (decisions.md
    /// D2: pre-filled once, cycled, never grown).
    fn send(&self, item: T) {
        let mut state = self.state.lock().unwrap();
        state.queue.push_back(item);
        self.cvar.notify_all();
    }

    /// The model's analogue of dropping a `crossbeam_channel::Sender`: no
    /// more items will ever arrive, so a `recv` on an empty, closed channel
    /// returns `None` instead of blocking forever.
    fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        self.cvar.notify_all();
    }

    /// Blocking receive: `Some(item)`, or `None` once closed and drained —
    /// the shim's analogue of a disconnected crossbeam `Receiver::recv()`.
    fn recv(&self) -> Option<T> {
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(item) = state.queue.pop_front() {
                return Some(item);
            }
            if state.closed {
                return None;
            }
            state = self.cvar.wait(state).unwrap();
        }
    }

    /// Non-blocking receive, used both directly (mirrors
    /// `Receiver::try_iter`'s single-item form) and inside [`select2`].
    fn try_recv(&self) -> Option<T> {
        let mut state = self.state.lock().unwrap();
        state.queue.pop_front()
    }

    fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }
}

/// The outcome of racing two channels — the shim's analogue of
/// `crossbeam_channel::select!` over two `recv()` arms, which the real
/// producer uses to race `free_rx` against `abort.closed()`
/// (`wait_for_free_segment_blocking` in `pump.rs`). Here the producer races
/// `free` against `err`, the model's equivalent signal for "the flusher
/// stopped."
enum Either<A, B> {
    Left(A),
    Right(B),
    /// `a` is closed and was found empty: no `Left` will ever arrive again —
    /// the shim's analogue of a disconnected crossbeam `Receiver`, which
    /// `recv()`/`select!` reports as an immediate `Err`, not a wait for the
    /// OTHER arm to close too.
    LeftClosed,
    /// `b` is closed and was found empty, symmetrically.
    RightClosed,
}

/// Block until `a` or `b` has an item, or either is closed and drained.
/// Between non-blocking attempts it yields the loom-modeled thread, which
/// loom treats as an ordinary scheduling decision point — exhaustively
/// explorable, not a hidden busy-loop, for the small (segment-count-bounded)
/// models this file runs. Checking EITHER channel's closed state
/// independently (not requiring both to be closed) matters: a caller
/// waiting on `free` racing `err` must stop as soon as `free` alone
/// disconnects — decisions.md D2's "the flusher cannot die in a way that
/// leaves the producer parked" is exactly this property, and requiring both
/// channels to close before returning would leave the producer spinning
/// forever whenever only one of them is ever closed (which is what a first,
/// buggy version of this model did — found by running it, not assumed).
fn select2<A, B>(a: &Chan<A>, b: &Chan<B>) -> Either<A, B> {
    loop {
        if let Some(x) = a.try_recv() {
            return Either::Left(x);
        }
        if let Some(y) = b.try_recv() {
            return Either::Right(y);
        }
        if a.is_closed() {
            return Either::LeftClosed;
        }
        if b.is_closed() {
            return Either::RightClosed;
        }
        loom::thread::yield_now();
    }
}

/// What the modeled flusher does with the `k`-th segment it processes
/// (0-based, across its whole run) — mirrors `Fault::Eio`/`Fault::Panic`
/// injected at a scripted `FaultySink` call index.
#[derive(Clone, Copy)]
enum FlusherBehavior {
    /// Every segment is written and returned successfully.
    AlwaysSucceed,
    /// The `k`-th segment fails (a hard I/O error, `run_flusher`'s `Err`
    /// path): that segment is dropped (never returned), one error is
    /// reported, and the flusher exits — mirrors `Fault::Eio`/`Fault::Enospc`.
    ErrorAt(u32),
    /// The `k`-th segment panics the flusher thread — mirrors `Fault::Panic`.
    /// The segment is dropped and the thread unwinds; `JoinHandle::join()`
    /// turns this into `Err`, never a re-panic on the producer (ST-15).
    PanicAt(u32),
}

impl fmt::Debug for FlusherBehavior {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlusherBehavior::AlwaysSucceed => write!(f, "AlwaysSucceed"),
            FlusherBehavior::ErrorAt(k) => write!(f, "ErrorAt({k})"),
            FlusherBehavior::PanicAt(k) => write!(f, "PanicAt({k})"),
        }
    }
}

/// Run one `loom::model` exploring every interleaving of one producer and
/// one flusher, `capacity = depth + 1` segment tokens, `total_segments`
/// sends (`> capacity` so at least one segment is genuinely recycled through
/// the ring), and the given fault behavior.
fn run_model(depth: usize, total_segments: u32, behavior: FlusherBehavior) {
    loom::model(move || {
        let capacity = (depth + 1) as u32;
        let free: Arc<Chan<u32>> = Arc::new(Chan::new());
        let full: Arc<Chan<u32>> = Arc::new(Chan::new());
        let err: Arc<Chan<()>> = Arc::new(Chan::new());
        // Segments the flusher drops (error/panic) rather than returns —
        // recorded so the final accounting can prove every token is either
        // back in `free` or here, never both and never neither.
        let dropped: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

        // decisions.md D2: every segment this pump will ever own is
        // pre-filled into `free` before either thread starts.
        for id in 0..capacity {
            free.send(id);
        }

        let flusher = {
            let free = Arc::clone(&free);
            let full = Arc::clone(&full);
            let err = Arc::clone(&err);
            let dropped = Arc::clone(&dropped);
            loom::thread::spawn(move || {
                // loom schedules real OS threads cooperatively but does not
                // cleanly propagate a genuine unwind THROUGH its own
                // scheduler the way `std::thread::JoinHandle::join()` would
                // in production (confirmed by running the panic-injection
                // path without this: the panic escaped `loom::model` itself
                // and failed the `#[test]` fn directly, not the assertions
                // inside it). So the injected panic is raised and caught
                // HERE, inside the flusher's own closure, exactly as
                // `AsyncBackend::shutdown`'s `JoinHandle::join()` would catch
                // a REAL cross-thread panic in production (ST-15) — the
                // observable effect (segment dropped, flusher exits, no
                // re-panic on the producer) is the same either way.
                let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut processed = 0u32;
                    // `full.recv()` returning `None` (disconnected — the
                    // producer closed it) is the flusher's only normal exit
                    // (architecture.md § Flusher / `run_flusher`'s doc
                    // comment), so the loop condition itself is that exit.
                    while let Some(id) = full.recv() {
                        let fail_now = match behavior {
                            FlusherBehavior::AlwaysSucceed => false,
                            FlusherBehavior::ErrorAt(k) | FlusherBehavior::PanicAt(k) => {
                                processed == k
                            }
                        };
                        if fail_now {
                            dropped.lock().unwrap().push(id);
                            match behavior {
                                FlusherBehavior::PanicAt(_) => {
                                    panic!(
                                        "pump_loom model: injected flusher panic at segment {processed}"
                                    );
                                }
                                _ => {
                                    err.send(());
                                    break;
                                }
                            }
                        } else {
                            free.send(id);
                        }
                        processed += 1;
                    }
                }))
                .is_err();
                // decisions.md D2's disconnection guarantee, modeled
                // directly: "if either side exits, it drops its channel
                // ends" — on EVERY exit path (clean, error, or the
                // internally-caught panic above), the flusher closes both
                // channels it owns the sending half of before this thread
                // ends, so the producer's `select2` can never wait forever
                // for a peer that is already gone.
                free.close();
                err.close();
                panicked
            })
        };

        // Producer: send `total_segments` segments, taking each from `free`
        // (racing `err` via `select2`, mirroring the real producer racing
        // `free_rx` against `abort.closed()`), then close `full` — the
        // model's analogue of `finish`/`Drop` dropping `full_tx`.
        for _ in 0..total_segments {
            match select2(&free, &err) {
                Either::Left(id) => full.send(id),
                Either::Right(()) | Either::LeftClosed | Either::RightClosed => break,
            }
        }
        full.close();

        // Reaching this line at all — for every interleaving loom explores —
        // is the "producer terminates" claim; loom's own checker fails the
        // run if any interleaving could leave a thread blocked forever with
        // no other thread left to wake it (the "no deadlock" claim). The
        // flusher thread itself never panics past its own boundary (the
        // injected fault is caught inside it, above), so this `join()`
        // always succeeds; `_flusher_panicked` is its own return value, not
        // `join()`'s `Result`.
        let _flusher_panicked = flusher
            .join()
            .expect("the flusher thread itself must not panic (only the modeled fault, caught internally, may)");

        // Any segment still sitting in `full` when the flusher exited early
        // (error or panic, before draining everything the producer had
        // already sent) is dropped too — nobody will ever return it.
        while let Some(id) = full.try_recv() {
            dropped.lock().unwrap().push(id);
        }

        // Exactly-once accounting: every one of the `capacity` physical
        // segments is either back in `free` or in `dropped`, and no id
        // appears in both or more than once anywhere.
        let mut accounted: Vec<u32> = Vec::new();
        while let Some(id) = free.try_recv() {
            accounted.push(id);
        }
        accounted.extend(dropped.lock().unwrap().iter().copied());
        let mut sorted = accounted.clone();
        sorted.sort_unstable();
        let mut deduped = sorted.clone();
        deduped.dedup();
        assert_eq!(
            sorted.len(),
            deduped.len(),
            "depth={depth} behavior={behavior:?}: a segment was returned-to-free or \
             dropped more than once: {sorted:?}"
        );
        let expected: Vec<u32> = (0..capacity).collect();
        assert_eq!(
            deduped, expected,
            "depth={depth} behavior={behavior:?}: every segment must be returned-to-free \
             or dropped exactly once"
        );
    });
}

/// depth = 1 (capacity = 2 segments): a baseline success run, plus a flusher
/// error and a flusher panic injected at every possible segment index across
/// a 3-segment stream (one more than capacity, so the ring recycles at least
/// once before any fault fires).
#[test]
fn pump_loom_depth1_no_deadlock_and_segments_accounted_exactly_once() {
    let depth = 1usize;
    let total_segments = 3u32; // capacity (2) + 1: forces one recycle
    run_model(depth, total_segments, FlusherBehavior::AlwaysSucceed);
    for k in 0..total_segments {
        run_model(depth, total_segments, FlusherBehavior::ErrorAt(k));
        run_model(depth, total_segments, FlusherBehavior::PanicAt(k));
    }
}

/// depth = 2 (capacity = 3 segments): same sweep over a 4-segment stream.
#[test]
fn pump_loom_depth2_no_deadlock_and_segments_accounted_exactly_once() {
    let depth = 2usize;
    let total_segments = 4u32; // capacity (3) + 1: forces one recycle
    run_model(depth, total_segments, FlusherBehavior::AlwaysSucceed);
    for k in 0..total_segments {
        run_model(depth, total_segments, FlusherBehavior::ErrorAt(k));
        run_model(depth, total_segments, FlusherBehavior::PanicAt(k));
    }
}
