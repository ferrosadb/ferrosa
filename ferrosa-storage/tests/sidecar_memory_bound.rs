//! t_7ac6b0e3: a scalar index sidecar is read through a memory map, not
//! loaded onto the heap.
//!
//! The sidecar used to be `std::fs::read` whole and deserialized into a
//! `Vec` of owned entries, and the store view keeps one reader per SSTable
//! per index for its whole life: index memory was O(every posting on the
//! node), resident, before any query ran. Mapped file pages are clean and
//! file-backed, so the kernel reclaims them under pressure instead of the
//! process being OOM-killed, and a reader's own heap is independent of the
//! file's size.
//!
//! Budgets are hard so a regression FAILS instead of OOM-ing the process.

use std::alloc::{GlobalAlloc, Layout, System};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use ferrosa_index::{IndexKey, RowPosition};
use ferrosa_storage::index::sidecar::{SidecarReader, SidecarWriter};

// --- peak-additional-heap tracker (scoped to this integration-test binary) ---
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
            // Clamp at zero: memory allocated before the window may be freed
            // inside it (see ferrosa-index's fulltext_topk_memory_bound.rs).
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

static MEASURE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    let _guard = MEASURE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let out = f();
    ARMED.store(false, Ordering::SeqCst);
    (out, PEAK.load(Ordering::SeqCst))
}

/// A sidecar with `n` postings under one hot key (a tenant with `n` session
/// partitions) plus as many under other keys.
fn write_sidecar(dir: &std::path::Path, n: usize) -> std::path::PathBuf {
    let path = dir.join(format!("7-idx_by_tenant-{n}.sidecar"));
    let entries: Vec<(IndexKey, RowPosition)> = (0..n)
        .flat_map(|i| {
            let partition_key = format!("tenant-partition-key-{i:016}").into_bytes();
            [
                (
                    IndexKey(b"tenant-hot".to_vec()),
                    RowPosition {
                        partition_key: partition_key.clone(),
                        clustering_key: Vec::new(),
                    },
                ),
                (
                    IndexKey(format!("tenant-{i:08}").into_bytes()),
                    RowPosition {
                        partition_key,
                        clustering_key: Vec::new(),
                    },
                ),
            ]
        })
        .collect();
    SidecarWriter::write(&path, &entries).expect("write sidecar");
    path
}

/// Open the sidecar and walk every posting of the hot key, returning how
/// many it holds.
fn open_and_walk(path: &std::path::Path) -> usize {
    let reader = SidecarReader::open(path).expect("open sidecar");
    let key = IndexKey(b"tenant-hot".to_vec());
    let mut walked = 0usize;
    reader
        .visit(&key, &mut |_position| {
            walked += 1;
            ControlFlow::Continue(())
        })
        .expect("visit");
    walked
}

/// Hard budget for opening a sidecar and walking one key: a mapping, a
/// header, a cursor — nothing proportional to the file.
const OPEN_AND_WALK_BUDGET_BYTES: i64 = 64 * 1024;

#[test]
fn opening_and_walking_a_sidecar_holds_a_bounded_heap_independent_of_its_size() {
    const SMALL_N: usize = 2_000;
    const LARGE_N: usize = 64_000; // 32× the postings, ~6 MiB of sidecar

    let dir = tempfile::tempdir().unwrap();
    let small = write_sidecar(dir.path(), SMALL_N);
    let large = write_sidecar(dir.path(), LARGE_N);

    let (small_walked, small_peak) = measure_peak(|| open_and_walk(&small));
    let (large_walked, large_peak) = measure_peak(|| open_and_walk(&large));

    assert_eq!(small_walked, SMALL_N, "every posting of the hot key");
    assert_eq!(large_walked, LARGE_N, "every posting of the hot key");
    assert!(
        large_peak <= OPEN_AND_WALK_BUDGET_BYTES,
        "opening and walking a {LARGE_N}-posting sidecar peaked at {large_peak} bytes of heap \
         (budget {OPEN_AND_WALK_BUDGET_BYTES}); a sidecar must be mapped, not loaded"
    );
    assert!(
        large_peak <= small_peak * 2 + 16 * 1024,
        "heap must not grow with the sidecar: {SMALL_N} postings peaked at {small_peak} bytes, \
         {LARGE_N} at {large_peak}"
    );
}
