//! I-SPACE: the aggregate accumulator is CONSTANT-SPACE.
//!
//! "Does not accumulate on the stack / does not grow with the row count" is a
//! COST claim, so it is measured, not asserted by value. A value assertion
//! (this query returns the right sum) passes both for a constant-space
//! accumulator and for one that materializes every row, so it is not evidence.
//! What is evidence is the CURVE: the peak allocation must be FLAT as the row
//! count grows.
//!
//! This drives the real accumulator ([`hash_aggregate`], the operator the plan
//! runs for every aggregate) over a synthetic, LAZY row generator — so nothing
//! in the fixture holds the rows — at N and 10N, under a counting allocator,
//! with a small spill threshold. An ungrouped aggregate needs no sort or spill;
//! its peak is only the resident accumulator and one input row. A materializing accumulator (or a sort that buffered its whole
//! input) would make the peak at 10N roughly ten times the peak at N.
//!
//! It also pins the exact sum, so "flat" cannot be bought by dropping rows.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use ferrosa_sql::exec::{hash_aggregate, AggFunc, AggSpec, TryRowStream};
use ferrosa_sql::spill::{DirReserver, SpillCtx, SpillError};
use ferrosa_sql::types::{Row, Value};

// --- peak-allocation tracker (scoped to this integration-test binary only) ---

struct TrackingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() && ARMED.load(Ordering::Relaxed) {
            let live =
                LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed) + layout.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            // Clamp at zero: frees of memory allocated before the window must
            // not drive the counter negative and hide later allocations.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static GLOBAL: TrackingAlloc = TrackingAlloc;

/// Run `f` with allocation tracking armed; return its result and the peak of
/// live bytes observed during the call.
fn measure_peak<T>(f: impl FnOnce() -> T) -> (T, i64) {
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let out = f();
    ARMED.store(false, Ordering::Relaxed);
    (out, PEAK.load(Ordering::Relaxed))
}

/// Small enough that an input of N rows spills many runs at both N and 10N, so
/// the measurement is of the accumulator + bounded spill buffers, not of an
/// in-memory fast path that would legitimately hold the input; large enough
/// that the run count (and so the merge's own bookkeeping) stays small, so the
/// curve reflects the accumulator rather than spill-run overhead.
const THRESHOLD_BYTES: u64 = 1024 * 1024;

/// Raise the soft `RLIMIT_NOFILE` to its hard limit once: a cascade merge holds
/// up to `MERGE_FANIN` run files open, and macOS shells default the soft limit
/// to 256.
fn raise_fd_limit() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| unsafe {
        let mut lim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 {
            let hard = lim.rlim_max;
            if hard > lim.rlim_cur {
                lim.rlim_cur = hard;
                let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
            }
        }
    });
}

/// The exact sum of `0..n` as the accumulator returns it, plus the peak
/// allocation over the whole aggregate. The input is a LAZY generator (no
/// fixture Vec), and the single output row is folded into an `i64` (an O(1)
/// consumer), so every byte the tracker sees belongs to the operator.
fn sum_over_n(n: i64) -> (i64, i64, bool) {
    raise_fd_limit();
    let dir = tempfile::tempdir().expect("tempdir");
    let ctx = SpillCtx::new(Arc::new(DirReserver::new(dir.path())), THRESHOLD_BYTES);
    let input: TryRowStream<'static> =
        Box::new((0..n).map(|i| Ok::<Row, SpillError>(Row(vec![Value::Int(i)]))));
    let (total, peak) = measure_peak(|| {
        let out = hash_aggregate(
            input,
            &[],
            &[AggSpec::typed(
                AggFunc::Sum,
                Some(0),
                ferrosa_sql::ColumnType::Int,
            )],
            &ctx,
        )
        .expect("aggregate opens");
        let mut total = 0i64;
        for r in out {
            let r = r.expect("no spill error");
            match r.get(0) {
                Value::Int(v) => total = *v,
                other => panic!("expected Int sum, got {other:?}"),
            }
        }
        total
    });
    (total, peak, ctx.stats().spilled())
}

#[test]
fn aggregate_accumulator_peak_is_flat_as_rows_grow() {
    const N: i64 = 100_000;
    let (sum_a, peak_a, spilled_a) = sum_over_n(N);
    let (sum_b, peak_b, spilled_b) = sum_over_n(N * 10);

    // Correctness first: the exact sum of 0..n. "Flat" must not be bought by
    // dropping rows.
    assert_eq!(sum_a, (0..N).sum::<i64>(), "sum at N must be exact");
    assert_eq!(sum_b, (0..N * 10).sum::<i64>(), "sum at 10N must be exact");

    // A single group requires no spill, regardless of input cardinality.
    assert!(!spilled_a, "a scalar aggregate must not spill at N");
    assert!(!spilled_b, "a scalar aggregate must not spill at 10N");

    // I-SPACE: the peak must NOT grow with the row count. Constant-space, the
    // peak is (accumulator + one spill buffer window) and is flat — measured at
    // ~1.005x from N to 10N; with materialization it would be ~10x. The 1.5x
    // bound leaves headroom for allocator noise while catching any O(N) growth.
    eprintln!(
        "I-SPACE measured curve: N={N} -> peak {peak_a} bytes; 10N={} -> peak {peak_b} bytes \
         (growth {:.3}x, sum {sum_a} / {sum_b})",
        N * 10,
        peak_b as f64 / peak_a.max(1) as f64
    );
    let growth = peak_b as f64 / peak_a.max(1) as f64;
    assert!(
        growth < 1.5,
        "peak allocation grew {growth:.2}x (from {peak_a} bytes at N={N} to {peak_b} \
         at 10N={}) — the aggregate accumulator is accumulating with the row count, \
         not constant-space",
        N * 10
    );
}
