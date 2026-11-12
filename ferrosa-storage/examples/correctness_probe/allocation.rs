//! Thread-local allocation accounting for synchronous probe windows.
//! Correctness: forwarding preserves allocator contracts; unwind disarms accounting.
//! Last revised: 2026-10-29
//! Last changed: Count alloc, alloc_zeroed and realloc without instrumenting production.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Counts {
    pub allocations: usize,
    pub reallocations: usize,
    pub requested_bytes: usize,
}

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static COUNTS: Cell<Counts> = const { Cell::new(Counts {
        allocations: 0, reallocations: 0, requested_bytes: 0,
    }) };
}

fn record(bytes: usize, realloc: bool) {
    let _ = ACTIVE.try_with(|active| {
        if active.get() {
            let _ = COUNTS.try_with(|counter| {
                let mut counts = counter.get();
                counts.allocations += usize::from(!realloc);
                counts.reallocations += usize::from(realloc);
                counts.requested_bytes = counts.requested_bytes.saturating_add(bytes);
                counter.set(counts);
            });
        }
    });
}

pub struct CountingAllocator;

// SAFETY: every operation forwards its original pointer/layout to System.
// Accounting uses const-initialized TLS Cells only: no allocation, locks or unwind.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size(), false);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size(), false);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size, true);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

struct Window;
impl Drop for Window {
    fn drop(&mut self) {
        ACTIVE.set(false);
    }
}

/// Measure only this thread; preparation, output and asynchronous work are excluded.
pub fn measure<T>(operation: impl FnOnce() -> T) -> (T, Counts) {
    assert!(!ACTIVE.get(), "allocation windows must not nest");
    COUNTS.set(Counts::default());
    ACTIVE.set(true);
    let window = Window;
    let result = operation();
    drop(window);
    (result, COUNTS.get())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_real_allocation_and_reallocation() {
        let (_, counts) = measure(|| {
            let mut bytes = std::hint::black_box(Vec::<u8>::with_capacity(8));
            bytes.extend_from_slice(&[7; 8]);
            bytes.reserve(4096);
            std::hint::black_box(bytes);
        });
        assert!(counts.allocations > 0);
        assert!(counts.reallocations > 0);
        let (_, counts) = measure(|| std::hint::black_box([7_u8; 8]));
        assert_eq!(counts, Counts::default());
    }

    #[test]
    fn panic_disarms_window() {
        assert!(std::panic::catch_unwind(|| measure(|| panic!("injected"))).is_err());
        let (_, counts) = measure(|| ());
        assert_eq!(counts, Counts::default());
    }
}
