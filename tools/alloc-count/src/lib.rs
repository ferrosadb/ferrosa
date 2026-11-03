//! A counting global allocator, for tests that assert how much a code path allocates.
//!
//! This lives outside `ferrosa-graph` because that crate is sealed with
//! `#![forbid(unsafe_code)]` — the seal is a ratchet on the production surface, and implementing
//! `GlobalAlloc` is unavoidably `unsafe`. Declaring the `#[global_allocator]` static is not unsafe, so
//! the sealed crate keeps the declaration and this crate keeps the impl.
//!
//! Dev-only: nothing in a release build depends on it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCATED: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Forwards to [`System`] while recording the bytes requested through the current thread.
pub struct CountingAllocator;

fn record(bytes: usize) {
    let _ = ALLOCATED.try_with(|count| {
        if let Some(previous) = count.get() {
            count.set(Some(previous.saturating_add(bytes)));
        }
    });
}

// SAFETY: every method forwards to `System` with the arguments it was given, and touches only
// thread-local state, so the allocator imposes no requirement beyond the one `System` already has.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Runs `operation` and returns its result with the bytes allocated on this thread while it ran.
///
/// Not re-entrant: a nested call panics rather than reporting a number that would silently include
/// the outer measurement.
pub fn allocated_bytes<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATED.with(|count| count.set(None));
        }
    }
    ALLOCATED.with(|count| assert!(count.get().is_none(), "nested allocation guard"));
    let reset = Reset;
    ALLOCATED.with(|count| count.set(Some(0)));
    let result = operation();
    let bytes = ALLOCATED.with(|count| count.get().expect("guard is active"));
    drop(reset);
    (result, bytes)
}
