//! T-036: `Compression::compress_into` must not allocate per call.
//!
//! `Compression::compress` allocates a fresh `Vec` per chunk
//! (`ferrosa-sstable/src/compression.rs`). `compress_into` exists so the
//! write pump's `ChunkCompressor` (T-038) can preallocate its input/output
//! buffers once, at open, and reuse them for every chunk — the
//! bounded-ring rule (`architecture.md` § Bounded-ring rule).
//!
//! A counting `#[global_allocator]`, scoped to this integration-test binary,
//! proves it: after one warm-up call (which is allowed to allocate — it is
//! what initializes each codec's per-thread state), further calls at the
//! same level must not allocate.
//!
//! **`Compression::Lz4` is the documented exception.** `lz4_flex` 0.11's
//! public block API (`lz4_flex::block::compress_into`, used here to avoid
//! the *output* `Vec`) always internally allocates a fresh match-finding
//! hash table — a `Box<[u16; 4096]>` or `Box<[u32; 4096]>` — on every call
//! (`lz4_flex-0.11.6/src/block/hashtable.rs`, `HashTable4KU16::new` /
//! `HashTable4K::new`). Nothing in that version's public surface exposes a
//! reusable table to hold across calls; the private `compress_internal` and
//! `HashTable` types that would need to be reused are `pub(crate)`. This is
//! reported here rather than hidden: the Lz4 test measures and asserts the
//! *actual* allocation count (one, bounded, independent of chunk size) so a
//! regression that made it scale with data size would still fail loud, but
//! does not claim the "zero" bar `Compression::None` and `Compression::Zstd`
//! meet. See the deferred follow-up item for investigating an upgrade or a
//! vendored patch for T-038.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ferrosa_sstable::Compression;

// --- per-call allocation-count tracker (scoped to this integration-test
// binary) ---
//
// Per THREAD, not per process, for the same reason as the other
// `*_memory_bound.rs` integration tests in this workspace: cargo runs the
// tests in this binary in parallel, and a global counter would attribute
// another test's allocations to this one. A thread-local counter measures
// only the thread that armed it, and the state is const-initialised so
// arming allocates nothing itself.
struct CountingAlloc;

thread_local! {
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

/// Runs `f` only while this thread's tracker is armed, and never
/// re-entrantly (a `Cell` access during TLS teardown would otherwise
/// recurse).
fn if_armed(f: impl FnOnce()) {
    let armed = ARMED.try_with(|armed| armed.get()).unwrap_or(false);
    if armed {
        f();
    }
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            if_armed(|| ALLOCS.with(|allocs| allocs.set(allocs.get() + 1)));
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            if_armed(|| ALLOCS.with(|allocs| allocs.set(allocs.get() + 1)));
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Counts allocations made while running `f` on the current thread.
fn count_allocs<R>(f: impl FnOnce() -> R) -> (R, u64) {
    ALLOCS.with(|allocs| allocs.set(0));
    ARMED.with(|armed| armed.set(true));
    let out = f();
    ARMED.with(|armed| armed.set(false));
    (out, ALLOCS.with(|allocs| allocs.get()))
}

/// Deterministic, dependency-free "random" bytes (see `compression.rs`'s
/// unit tests for the same generator) — high-entropy enough that codecs
/// don't compress it away to nothing, which would make output-length bugs
/// invisible.
fn pseudo_random_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xff) as u8
        })
        .collect()
}

/// Warms up `compression`'s per-thread state (the lazily-created zstd
/// `CCtx`, for `Zstd`) with one call, whose allocations are discarded, then
/// returns the allocation count of ten further calls against freshly-sized
/// chunks of pseudo-random data.
fn warm_up_then_measure_ten_calls(compression: &Compression) -> u64 {
    let chunk = pseudo_random_bytes(Compression::DEFAULT_CHUNK_SIZE, 0x5EED);
    let bound = compression.compress_bound(chunk.len());
    let mut dst = vec![0u8; bound];

    // Warm-up: allowed to allocate (first-use setup).
    compression.compress_into(&chunk, &mut dst).unwrap();

    // Every input is generated *before* the measured window opens. A
    // different seed per call, so this cannot pass by accident of the
    // compressor caching one specific input — but `Vec::collect` inside
    // `pseudo_random_bytes` must not itself be counted as `compress_into`'s
    // allocation.
    let inputs: Vec<Vec<u8>> = (0..10u64)
        .map(|i| pseudo_random_bytes(Compression::DEFAULT_CHUNK_SIZE, 0xC0FFEE + i))
        .collect();

    let (_, allocs) = count_allocs(|| {
        for data in &inputs {
            let written = compression.compress_into(data, &mut dst).unwrap();
            assert!(written > 0 || data.is_empty());
        }
    });
    allocs
}

#[test]
fn compress_into_alloc_none_is_zero_after_warmup() {
    let allocs = warm_up_then_measure_ten_calls(&Compression::None);
    assert_eq!(
        allocs, 0,
        "Compression::None must not allocate: compress_into only memcpys into the caller's dst"
    );
}

#[test]
fn compress_into_alloc_zstd_is_zero_after_warmup() {
    let allocs = warm_up_then_measure_ten_calls(&Compression::Zstd { level: 3 });
    assert_eq!(
        allocs, 0,
        "Compression::Zstd must not allocate after the first call: compress_into reuses a \
         per-thread zstd_safe::CCtx (ZSTD_CCtx) across calls instead of creating one per chunk"
    );
}

/// Documents, rather than hides, the one case that cannot reach zero: see
/// this file's module doc comment. `lz4_flex` 0.11.6 allocates one boxed
/// hash table per call to `block::compress_into` regardless of caller-side
/// buffer reuse — a per-call cost of the vendored dependency, not a
/// per-chunk `Vec` introduced by `ferrosa-sstable`. This test's job is to
/// catch a *regression* (the count growing with chunk count or size), not
/// to claim the same zero bar as `None`/`Zstd`.
#[test]
fn compress_into_alloc_lz4_is_one_bounded_allocation_per_call_not_zero() {
    let allocs = warm_up_then_measure_ten_calls(&Compression::Lz4);
    assert_eq!(
        allocs, 10,
        "Compression::Lz4 allocates lz4_flex's internal match-finding hash table on every call \
         to block::compress_into (one per call, not per byte); this documents that constant, \
         unavoidable-with-lz4_flex-0.11 cost rather than hiding it. If this fails with a HIGHER \
         count, that is a real regression (e.g. an internal Vec reintroduced on top of it)"
    );
}
