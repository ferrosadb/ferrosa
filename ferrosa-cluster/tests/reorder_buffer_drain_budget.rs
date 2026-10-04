//! Load-independent budget guards for the Accord `ReorderBuffer` drain.
//!
//! `accord::perf_regression` originally asserted an ABSOLUTE wall-clock bound
//! on a 1000-message drain (< 10 ms). That bound was ejected by a shared,
//! contended nightly runner on 2026-09-30: it measured 56.7 ms while the same
//! code on an idle box measures ~0.16 ms, and the dedicated `perf-regression`
//! job passed in the same workflow. An absolute time bound measures the runner,
//! not the code (`forge t_430e21f7` recorded the same failure ejecting a
//! docs-only PR).
//!
//! The properties the bound was a proxy for ARE load-independent, so this file
//! asserts them directly instead:
//!
//! 1. **Allocation budget** — the drain allocates a bounded number of times
//!    regardless of how many messages it returns: one output vector, not one
//!    allocation per message. Allocation counts do not vary with machine load,
//!    so this guard cannot flake. It fails a drain that clones messages instead
//!    of moving them, or that materialises per-key temporaries.
//! 2. **Linear cost** — the per-message drain cost does not grow with the
//!    number of messages. This is a RATIO of two measurements taken in the same
//!    run, so machine speed and load largely cancel; only a superlinear drain
//!    (e.g. a per-key remove that re-scans, or a sort inside a loop) trips it.
//!
//! These guards run in the default suite, so they also police the per-PR lane —
//! which the wall-clock benchmark deliberately does not.
//!
//! The measured baseline at the time of writing (idle, debug build):
//! 1000 messages drained in ~160 us, 1 allocation, 32 bytes per message,
//! log-log exponent 1.00 from n = 1000 to n = 32000.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use ferrosa_cluster::accord::reorder_buffer::{Message, ReorderBuffer, TimingConfig};
use serial_test::serial;

/// Counts allocations and allocated bytes made BY THE MEASURING THREAD while
/// its [`ARMED`] flag is set. Outside that window, and on every other thread,
/// it is a pass-through.
///
/// The flag is thread-local on purpose. It was a process-global `AtomicBool`,
/// so the count also took in whatever other threads allocated while the drain
/// ran. libtest's main thread spawns the other `#[serial]` tests' threads (they
/// then wait on the serial lock) while the first one is already measuring; on
/// 2026-10-03 that put 16 allocations on a drain that makes one.
struct CountingAlloc;

thread_local! {
    // `const` init: reading it never allocates, so it is safe inside `alloc`.
    static ARMED: Cell<bool> = const { Cell::new(false) };
}

/// Whether the current thread is measuring. `try_with` because the allocator
/// can run during thread-local teardown, when the flag is gone (not measuring).
fn armed() -> bool {
    ARMED.try_with(Cell::get).unwrap_or(false)
}
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if armed() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: delegated to the system allocator with the same layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: delegated to the system allocator with the same pointer/layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if armed() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        // SAFETY: delegated to the system allocator with the same arguments.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Run `f` with allocation counting armed on this thread; returns
/// `(output, allocations, bytes)`.
fn measure_allocs<R>(f: impl FnOnce() -> R) -> (R, u64, u64) {
    ALLOCS.store(0, Ordering::SeqCst);
    BYTES.store(0, Ordering::SeqCst);
    ARMED.with(|a| a.set(true));
    let out = f();
    ARMED.with(|a| a.set(false));
    (
        out,
        ALLOCS.load(Ordering::SeqCst),
        BYTES.load(Ordering::SeqCst),
    )
}

fn timing() -> TimingConfig {
    TimingConfig {
        skew_max_us: 10_000,
        rtt_p99_us: 5_000,
    }
}

/// Push `n` messages with DISTINCT `t0`, as the benchmark does.
fn push_distinct_t0(buf: &mut ReorderBuffer, n: i64) {
    for i in 0..n {
        buf.push(Message {
            t0: i * 100,
            payload: vec![(i & 0xFF) as u8],
        })
        .unwrap();
    }
}

/// The largest number of allocations a drain of any size may make: the single
/// output vector, plus slack for allocator bookkeeping. It is deliberately a
/// CONSTANT, not a per-message budget — "one allocation per message" is exactly
/// the regression this pins.
const MAX_DRAIN_ALLOCATIONS: u64 = 4;

/// Bytes of allocation per returned message the drain may make. The output
/// vector is `size_of::<Message>()` (32 bytes) per message; 48 leaves modest
/// slack. A clone-based drain allocates a `Vec<u8>` payload per message and
/// blows past this.
const MAX_DRAIN_BYTES_PER_MESSAGE: u64 = 48;

#[test]
#[serial]
fn reorder_buffer_drain_allocation_budget_does_not_grow_with_message_count() {
    for n in [1_000i64, 4_000] {
        let mut buf = ReorderBuffer::new(n as usize + 1, timing());
        push_distinct_t0(&mut buf, n);

        let (drained, allocs, bytes) = measure_allocs(|| buf.drain_ready(i64::MAX));

        // Correctness first: a budget guard on a wrong drain is worthless.
        assert_eq!(drained.len(), n as usize, "drain must return every message");
        assert!(
            drained.windows(2).all(|w| w[0].t0 <= w[1].t0),
            "drain must return messages in ascending t0 order"
        );

        assert!(
            allocs <= MAX_DRAIN_ALLOCATIONS,
            "draining {n} messages made {allocs} allocations; the drain must move \
             messages into one output vector, not allocate per message \
             (budget {MAX_DRAIN_ALLOCATIONS})"
        );
        assert!(
            bytes <= n as u64 * MAX_DRAIN_BYTES_PER_MESSAGE,
            "draining {n} messages allocated {bytes} bytes ({:.1} per message); \
             budget is {MAX_DRAIN_BYTES_PER_MESSAGE} per message",
            bytes as f64 / n as f64
        );
    }
}

#[test]
#[serial]
fn reorder_buffer_same_key_drain_allocation_budget_is_bounded() {
    let n = 1_000i64;
    let mut buf = ReorderBuffer::new(n as usize + 1, timing());
    for i in 0..n {
        buf.push(Message {
            t0: 7,
            payload: vec![(i & 0xFF) as u8],
        })
        .unwrap();
    }

    let (drained, allocs, bytes) = measure_allocs(|| buf.drain_ready(i64::MAX));

    assert_eq!(drained.len(), n as usize);
    // Arrival order is preserved within the bucket.
    assert_eq!(
        drained.iter().map(|m| m.payload[0]).collect::<Vec<_>>(),
        (0..n).map(|i| (i & 0xFF) as u8).collect::<Vec<_>>()
    );
    assert!(
        allocs <= MAX_DRAIN_ALLOCATIONS,
        "single-key drain of {n} messages made {allocs} allocations \
         (budget {MAX_DRAIN_ALLOCATIONS})"
    );
    assert!(bytes <= n as u64 * MAX_DRAIN_BYTES_PER_MESSAGE);
}

/// Best (minimum) wall time, in nanoseconds, to drain `n` distinct-t0 messages.
///
/// The MINIMUM is used on purpose: it is the sample least perturbed by the
/// scheduler, and it still scales with the algorithm. A superlinear drain has a
/// minimum that grows faster than the input.
fn best_drain_ns(n: i64, reps: usize) -> u128 {
    let mut best = u128::MAX;
    for _ in 0..reps {
        let mut buf = ReorderBuffer::new(n as usize + 1, timing());
        push_distinct_t0(&mut buf, n);
        let start = Instant::now();
        let out = buf.drain_ready(i64::MAX);
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(out.len(), n as usize);
        best = best.min(elapsed);
    }
    best
}

/// Per-message drain cost may not grow with the number of messages. A linear
/// drain keeps it flat; an O(n log n) or O(n^2) drain makes it climb.
#[test]
#[serial]
fn reorder_buffer_drain_cost_is_linear_in_message_count() {
    // 8x the input. A linear drain grows per-message cost by ~1x; a quadratic
    // drain by ~8x. 4x is slack that a genuine superlinearity still crosses and
    // scheduler noise does not (this is a ratio of two same-run measurements).
    const N_SMALL: i64 = 1_000;
    const N_LARGE: i64 = 8_000;
    const MAX_PER_MESSAGE_GROWTH: f64 = 4.0;

    let small = best_drain_ns(N_SMALL, 9);
    let large = best_drain_ns(N_LARGE, 9);
    let per_small = small as f64 / N_SMALL as f64;
    let per_large = large as f64 / N_LARGE as f64;
    let growth = per_large / per_small;

    assert!(
        growth <= MAX_PER_MESSAGE_GROWTH,
        "ReorderBuffer drain cost per message grew {growth:.2}x from n={N_SMALL} \
         ({per_small:.1} ns/msg) to n={N_LARGE} ({per_large:.1} ns/msg); the drain \
         must stay linear in the message count (budget {MAX_PER_MESSAGE_GROWTH}x)"
    );
}
