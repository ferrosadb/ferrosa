//! Bounded selector contract, using the actual shared internal source.
//!
//! RED test list:
//! - This cycle: owned incremental offers, late winners, stable boundary ties,
//!   bounded retention, prompt drops, and no allocation after preallocation.
//! - Later: k=0, empty/fewer-than-k inputs, k=1, non-finite distances/signed zero.
//!
//! An absent module/API is API-absence RED, not existing-runtime evidence.
//! This measures only the selector: upstream partition/result materialization
//! remains, and this is not an end-to-end streaming/allocation-free claim.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[path = "../src/ann_topk.rs"]
mod ann_topk;

use ann_topk::AnnTopK;

struct CountingAllocator;

thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static REALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

fn record_allocation(reallocation: bool) {
    TRACKING.with(|tracking| {
        if tracking.get() {
            if reallocation {
                REALLOCATIONS.with(|count| count.set(count.get() + 1));
            } else {
                ALLOCATIONS.with(|count| count.set(count.get() + 1));
            }
        }
    });
}

// Unsafe operations only delegate the allocator contract to System.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(false);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(false);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation(true);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct Measurement;

impl Measurement {
    fn start() -> Self {
        // Initialize/reset every thread-local counter before enabling tracking.
        ALLOCATIONS.with(|count| count.set(0));
        REALLOCATIONS.with(|count| count.set(0));
        TRACKING.with(|tracking| assert!(!tracking.replace(true)));
        Self
    }

    fn finish(self) -> (usize, usize) {
        TRACKING.with(|tracking| tracking.set(false));
        (ALLOCATIONS.with(Cell::get), REALLOCATIONS.with(Cell::get))
    }
}

impl Drop for Measurement {
    fn drop(&mut self) {
        TRACKING.with(|tracking| tracking.set(false));
    }
}

// Deliberately neither Clone nor Ord: the selector must move opaque payloads.
struct OwnedRow<'a> {
    ordinal: usize,
    payload: Vec<u8>,
    drops: &'a Cell<usize>,
}

impl Drop for OwnedRow<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn bounded_ranking_moves_owned_rows_without_allocating_after_preallocation() {
    const N: usize = 4096;
    const K: usize = 8;
    const LATE_DISTANCES: [f64; 12] = [3.0, 2.0, 1.0, 1.0, 0.0, 2.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0];

    // Given: all payloads, expected pointers and the selector exist before the
    // measured window. Encounter ordinals break ties, including at the cutoff.
    let drops = Cell::new(0);
    let scores = Cell::new(0);
    let candidates: Vec<_> = (0..N)
        .map(|ordinal| OwnedRow {
            ordinal,
            payload: vec![(ordinal % 251) as u8; 256],
            drops: &drops,
        })
        .collect();
    let original_pointers: Vec<_> = candidates.iter().map(|row| row.payload.as_ptr()).collect();
    let expected = [N - 8, N - 5, N - 3, N - 1, N - 10, N - 9, N - 6, N - 4];
    let candidates = candidates.into_iter();
    let construction = Measurement::start();
    let mut selector = AnnTopK::new(K);
    let (construction_allocations, construction_reallocations) = construction.finish();
    assert_eq!(
        construction_allocations, 1,
        "one preallocated selector buffer"
    );
    assert_eq!(construction_reallocations, 0);
    assert_eq!(selector.len(), 0);

    // When: score and move every candidate exactly once, including rejected
    // rows and replacements. Scoring is caller-owned in this proposed API;
    // this counter does not establish how often the router computes distance.
    let measurement = Measurement::start();
    for (ordinal, row) in candidates.enumerate() {
        scores.set(scores.get() + 1);
        let distance = if ordinal < N - LATE_DISTANCES.len() {
            1000.0 + (ordinal % 37) as f64
        } else {
            LATE_DISTANCES[ordinal - (N - LATE_DISTANCES.len())]
        };
        selector.offer(distance, ordinal, row);
        assert_eq!(selector.len(), K.min(ordinal + 1));
        assert!(selector.len() <= K, "retention must remain bounded");
        assert_eq!(
            drops.get(),
            ordinal + 1 - selector.len(),
            "rejected/evicted payloads must be dropped during offers"
        );
    }

    // Then: consuming sorted output is part of the same allocation window.
    // into_sorted_rows may return an owning iterator over scored heap storage;
    // requiring a fresh Vec<T> would defeat allocation reuse.
    let mut output_count = 0;
    for row in selector.into_sorted_rows() {
        assert_eq!(row.ordinal, expected[output_count]);
        assert_eq!(row.payload.as_ptr(), original_pointers[row.ordinal]);
        assert_eq!(row.payload.len(), 256);
        assert_eq!(row.payload[0], (row.ordinal % 251) as u8);
        output_count += 1;
    }
    let (allocations, reallocations) = measurement.finish();

    assert_eq!(
        output_count, K,
        "all and only the nearest rows are returned"
    );
    assert_eq!(scores.get(), N);
    assert_eq!(
        drops.get(),
        N,
        "every owned payload is dropped exactly once"
    );
    assert_eq!(allocations, 0, "offers and output must reuse preallocation");
    assert_eq!(reallocations, 0, "the selector allocation must not grow");
}
