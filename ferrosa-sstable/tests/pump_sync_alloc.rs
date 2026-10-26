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
//!
//! The count is per THREAD. At `depth = 0` the pump does all its work on the
//! calling thread, so the test thread's count is the pump's whole cost. A
//! process-wide counter also saw libtest's main thread, which keeps doing its
//! own bookkeeping after it spawns the test thread: on a loaded CPU those
//! allocations land inside the measured loop (CI run 40051576713 counted 4;
//! pinning the binary to one busy CPU fails 62 runs in 200, every stray
//! allocation from the main thread).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ferrosa_sstable::direct::DirectWriter;

struct CountingAllocator;

thread_local! {
    // const-initialized, so counting allocates nothing itself.
    static ALLOC_EVENTS: Cell<usize> = const { Cell::new(0) };
}

/// `try_with`: an allocation during TLS teardown must not panic.
fn count_alloc() {
    let _ = ALLOC_EVENTS.try_with(|events| events.set(events.get() + 1));
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_alloc();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_alloc();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_alloc();
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

/// Allocations made so far by the calling thread.
fn alloc_events() -> usize {
    ALLOC_EVENTS.with(Cell::get)
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

/// The count must belong to the measuring thread alone. libtest's main thread
/// allocates its first blocking `recv()` context after it spawns the test
/// thread, and on a busy CPU that lands inside the measured loop above (CI run
/// 40051576713 counted 4). Here another thread allocates while the window is
/// open; the threads hand off through atomics only, so nothing else on this
/// thread allocates inside the window.
#[test]
fn pump_sync_alloc_counter_ignores_other_threads() {
    use std::sync::atomic::{AtomicU8, Ordering};
    static STEP: AtomicU8 = AtomicU8::new(0);

    let other = std::thread::spawn(|| {
        while STEP.load(Ordering::Acquire) != 1 {
            std::hint::spin_loop();
        }
        std::hint::black_box(Vec::<u8>::with_capacity(64));
        STEP.store(2, Ordering::Release);
    });

    let before = alloc_events();
    STEP.store(1, Ordering::Release);
    while STEP.load(Ordering::Acquire) != 2 {
        std::hint::spin_loop();
    }
    let delta = alloc_events() - before;
    other.join().expect("allocating thread");

    assert_eq!(delta, 0, "another thread's allocation was counted");
}

/// The control: an allocation on the measuring thread is counted, so a zero
/// above cannot come from a counter that counts nothing.
#[test]
fn pump_sync_alloc_counter_sees_the_measuring_thread() {
    let before = alloc_events();
    std::hint::black_box(Vec::<u8>::with_capacity(64));
    assert_eq!(alloc_events() - before, 1);
}
