//! T-032: the synchronous write pump must not allocate in steady state.
//!
//! Uses a counting `#[global_allocator]` (the only one in this test binary —
//! the same pattern `ferrosa-sstable/tests` uses elsewhere, e.g.
//! `row_encode_alloc.rs` on `sp/T-037`) to measure whether streaming MORE
//! bytes through an already-open `DirectWriter` (`AlignedPump` at
//! `depth = 0`, driving the real `FileSink`) costs any additional heap
//! allocation once the one-time setup (the `AlignedBuf` allocated at
//! `create`, and any lazily initialized statics such as the dio_align
//! once-per-device log dedup and the direct-I/O switch "warned" flags) has
//! already happened during warm-up.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use ferrosa_sstable::direct::DirectWriter;

struct CountingAllocator;

static ALLOC_EVENTS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn alloc_events() -> usize {
    ALLOC_EVENTS.load(Ordering::Relaxed)
}

/// Streams 64 MiB through a real `DirectWriter` (the production `FileSink`
/// path) in 64 KiB chunks, after a warm-up that primes the one-time
/// allocation (the staging `AlignedBuf`, sized at `create`) and any lazily
/// initialized statics, then asserts the steady-state loop costs exactly zero
/// further allocations.
#[test]
fn pump_sync_alloc_streaming_64mib_after_warmup_is_alloc_free() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("data.db");
    let mut writer = DirectWriter::create(&path).expect("create");

    const CHUNK: usize = 64 * 1024;
    const TOTAL_CHUNKS: usize = (64 * 1024 * 1024) / CHUNK;
    const WARMUP_CHUNKS: usize = 4;
    let chunk = vec![0xABu8; CHUNK];

    for _ in 0..WARMUP_CHUNKS {
        writer.write_all(&chunk).expect("warmup write_all");
    }

    let before = alloc_events();
    for _ in WARMUP_CHUNKS..TOTAL_CHUNKS {
        writer.write_all(&chunk).expect("steady-state write_all");
    }
    let delta = alloc_events() - before;
    assert_eq!(
        delta,
        0,
        "streaming {} more chunks through the pump after warm-up cost {delta} \
         allocations — the write hot path must not allocate after open \
         (bounded-ring rule, architecture.md)",
        TOTAL_CHUNKS - WARMUP_CHUNKS
    );

    let logical = writer.finish().expect("finish");
    assert_eq!(logical, (TOTAL_CHUNKS * CHUNK) as u64);
}
