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
//! **`Compression::Lz4` used to be a documented exception** (forge
//! t_8b85877d): `lz4_flex` 0.11's public block API
//! (`lz4_flex::block::compress_into`) always internally allocated a fresh
//! match-finding hash table — a `Box<[u16; 4096]>` or `Box<[u32; 4096]>` — on
//! every call, and nothing in that version's public surface exposed a
//! reusable table to hold across calls. T-038 upgrades to `lz4_flex` 0.14,
//! which added `block::compress_into_with_table` taking a caller-owned
//! `CompressTable` that is only `clear()`ed (a `fill(0)`, not a realloc) per
//! call. `compression.rs`'s `LZ4_TABLES` keeps one `Small` and one `Large`
//! table per thread, allocated once, so `Compression::Lz4` now meets the
//! same zero-after-warmup bar as `None` and `Zstd`.

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

/// T-038 (forge t_8b85877d): with `lz4_flex` 0.14's `compress_into_with_table`
/// and a per-thread reusable `CompressTable`, `Compression::Lz4` now meets
/// the same zero-after-warmup bar as `None` and `Zstd`.
#[test]
fn compress_into_alloc_lz4_is_zero_after_warmup() {
    let allocs = warm_up_then_measure_ten_calls(&Compression::Lz4);
    assert_eq!(
        allocs, 0,
        "Compression::Lz4 must not allocate after the first call: compress_into now reuses a \
         per-thread lz4_flex::block::CompressTable across calls instead of allocating a fresh \
         hash table per chunk (T-038, forge t_8b85877d)"
    );
}

/// 10x the estimated chunk count, at DEFAULT_CHUNK_SIZE: the pump's own
/// packet requirement ("counts zero when chunk count is 10x the size
/// estimate"). Proves the reused tables don't grow or reallocate with
/// call count.
#[test]
fn compress_into_alloc_lz4_is_zero_over_many_calls() {
    let chunk = pseudo_random_bytes(Compression::DEFAULT_CHUNK_SIZE, 0x5EED);
    let bound = Compression::Lz4.compress_bound(chunk.len());
    let mut dst = vec![0u8; bound];
    Compression::Lz4.compress_into(&chunk, &mut dst).unwrap();

    let inputs: Vec<Vec<u8>> = (0..2_500u64)
        .map(|i| pseudo_random_bytes(Compression::DEFAULT_CHUNK_SIZE, 0xBEEF + i))
        .collect();
    let (_, allocs) = count_allocs(|| {
        for data in &inputs {
            Compression::Lz4.compress_into(data, &mut dst).unwrap();
        }
    });
    assert_eq!(
        allocs, 0,
        "2500 calls (10x a typical chunk-count estimate) must not allocate beyond warm-up"
    );
}
