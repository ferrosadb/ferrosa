//! TDD memory-bound guard for the ORDER BY spilling external sort.
//!
//! The whole point of [`ferrosa_storage::ExternalSorter`] is that its *sort
//! working set* — the in-memory accumulation buffer plus the k-way merge heap —
//! stays bounded by the spill threshold, independent of how many rows are
//! sorted. Without spill, a full-table `ORDER BY` holds every row (plus a sorted
//! copy) in memory and OOM-kills the node; with spill, once the buffer crosses
//! the threshold it is flushed to a run file and cleared.
//!
//! This test drives many more rows through the sorter than the threshold allows
//! to reside in memory, drains the fully-sorted output (discarding each row so
//! the *result* never dominates the measurement), and asserts:
//!
//! 1. peak additional heap during push+drain is bounded (does not scale with the
//!    total row count), and
//! 2. the output is complete and correctly ordered (no loss/dup/misorder).
//!
//! Modeled on `ferrosa-cluster/tests/range_scan_streaming_memory_bound.rs`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use ferrosa_common::CqlValue;
use ferrosa_storage::{ExternalSorter, RowOrder};

// --- peak-allocation tracker (scoped to this integration-test binary only) ---
struct TrackingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && ARMED.load(Ordering::Relaxed) {
            let live =
                LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed) + layout.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            // Clamp at zero: `measure_peak` zeroes LIVE at arm time, so a free
            // of memory allocated BEFORE the window would drive the counter
            // negative and, because PEAK is a running maximum of LIVE, suppress
            // every later allocation. Seeding runs outside the window by design,
            // so how much of it is released inside the window is pure timing.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let out = f();
    ARMED.store(false, Ordering::SeqCst);
    (out, PEAK.load(Ordering::SeqCst))
}

/// A row carrying a sort key plus a fixed padding payload, so each row has a
/// meaningful byte footprint (magnifies the gap between bounded and unbounded).
const PAYLOAD_BYTES: usize = 512;

fn make_row(key: i64) -> Vec<Option<CqlValue>> {
    vec![
        Some(CqlValue::Bigint(key)),
        Some(CqlValue::Blob(vec![0u8; PAYLOAD_BYTES])),
    ]
}

/// Deterministic LCG (no `rand` dependency) producing pseudo-random keys.
struct Lcg(u64);
impl Lcg {
    fn next_key(&mut self) -> i64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 16) as i64
    }
}

/// Push `n` rows through a sorter with a fixed small threshold, drain the sorted
/// output (discarding rows), and return `(peak_additional_bytes, spilled, in_order)`.
fn sort_peak(n: usize, threshold_bytes: u64) -> (i64, bool, bool) {
    let dir = tempfile::tempdir().unwrap();
    let order = RowOrder::new(vec![(0, true)]);

    let ((spilled, in_order), peak) = measure_peak(|| {
        let mut sorter = ExternalSorter::new(dir.path(), order, threshold_bytes);
        let mut rng = Lcg(0x1234_5678 ^ n as u64);
        for _ in 0..n {
            sorter.push(make_row(rng.next_key())).unwrap();
        }
        let spilled = sorter.spilled();
        // Drain in sorted order, holding at most one row + the previous key.
        let mut prev: Option<i64> = None;
        let mut in_order = true;
        let mut count = 0usize;
        for row in sorter.finish().unwrap() {
            let row = row.unwrap();
            let key = match row[0] {
                Some(CqlValue::Bigint(k)) => k,
                _ => panic!("expected bigint key"),
            };
            if let Some(p) = prev {
                if key < p {
                    in_order = false;
                }
            }
            prev = Some(key);
            count += 1;
            // `row` drops here — the result never accumulates.
        }
        assert_eq!(count, n, "every pushed row must be emitted (n={n})");
        (spilled, in_order)
    });
    (peak, spilled, in_order)
}

#[test]
fn order_by_spill_peak_is_independent_of_row_count() {
    // Threshold ~ 64 KiB: holds ~100 padded rows at a time, forcing spills well
    // before either N below fills memory.
    const THRESHOLD: u64 = 64 * 1024;
    const SMALL_N: usize = 2_000;
    const LARGE_N: usize = 32_000; // 16x more rows

    let (small, small_spilled, small_ok) = sort_peak(SMALL_N, THRESHOLD);
    let (large, large_spilled, large_ok) = sort_peak(LARGE_N, THRESHOLD);

    eprintln!(
        "order_by_spill_peak: small(N={SMALL_N})={small} B, large(N={LARGE_N})={large} B, \
         ratio={:.2} (payload={PAYLOAD_BYTES} B, threshold={THRESHOLD} B)",
        large as f64 / small.max(1) as f64,
    );

    assert!(
        small_spilled && large_spilled,
        "both runs must actually spill"
    );
    assert!(
        small_ok && large_ok,
        "both runs must emit fully sorted output"
    );

    // Bounded working set: 16x the rows must NOT cost ~16x the peak heap. Slack
    // covers the merge heap growing with the number of runs (~N/threshold), plus
    // buffer/bookkeeping. A materializing sort would make `large` ≈ 16x `small`.
    assert!(
        large < small * 4,
        "REGRESSION: ORDER BY spill peak heap scales with row count — \
         {SMALL_N} rows: {small} B, {LARGE_N} rows ({}x more): {large} B. \
         The external sort is materializing the whole result instead of spilling.",
        LARGE_N / SMALL_N,
    );
}
