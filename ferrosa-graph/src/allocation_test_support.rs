//! Thread-local allocation traffic guards for synchronous executor tests.
//!
//! The `GlobalAlloc` implementation is unavoidably `unsafe`, and this crate is sealed with
//! `#![forbid(unsafe_code)]`, so the impl lives in the dev-only `ferrosa-alloc-count` crate and this
//! module does only the safe half: installing it as the test binary's global allocator.

#[global_allocator]
static ALLOCATOR: ferrosa_alloc_count::CountingAllocator = ferrosa_alloc_count::CountingAllocator;

pub(crate) fn allocated_bytes<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    ferrosa_alloc_count::allocated_bytes(operation)
}
