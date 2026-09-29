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
//!
//! # Why one test here is `slow-tests`-gated
//!
//! `writing_a_sidecar_from_a_memtable_index_holds_a_bounded_heap` writes 64,000
//! postings by hand while a global-allocator hook accounts every allocation.
//! In a debug build that is ~840 seconds — measured on PR CI, where it was 43%
//! of the whole `Test + Coverage` job, and it ran serially because libtest runs
//! one test binary at a time. It is not deleted or `#[ignore]`d (ferrosa/CLAUDE.md
//! forbids ignoring a test for being slow); it moves behind the `slow-tests`
//! crate feature and runs nightly in nightly-slow-tests.yml, the same bargain
//! the `mod slow` tests make.
//!
//! The nesting is load-bearing: nightly selects with `-- ::slow::`, and libtest
//! names a test in an integration binary after its modules only (the file name
//! is not part of the path). A top-level `mod slow` would name the test
//! `slow::foo`, which does NOT contain `::slow::` and would never be selected —
//! gating without re-running deletes the test. Nesting it one level deeper
//! yields `sidecar::slow::foo`, which the nightly filter matches.
//!
//! The read-side test stays in the per-PR gate: ~0.35s, and it guards the
//! mmap-vs-read decision, the cheaper half to regress.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ops::ControlFlow;

use ferrosa_index::{IndexKey, RowPosition};
use ferrosa_storage::index::sidecar::{SidecarReader, SidecarWriter};
// Only the `slow-tests` writer test builds a memtable index; importing it
// unconditionally leaves an unused import in the default build, which CI's
// `clippy --all-targets -D warnings` rejects.
#[cfg(feature = "slow-tests")]
use ferrosa_storage::memtable::index::MemtableIndex;

// --- peak-additional-heap tracker (scoped to this integration-test binary) ---
//
// Per THREAD, not per process. cargo runs the tests in this binary in
// parallel, so a global counter measures whatever every other test happens to
// be allocating at the time: one test's fixture setup lands inside another
// test's window and fails it, for a budget the code under test never spent.
// A thread-local counter measures the thread that armed it, and the state is
// const-initialised so arming allocates nothing itself.
struct TrackingAlloc;

thread_local! {
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
}

/// Runs `f` only while this thread's tracker is armed, and never re-entrantly
/// (a `Cell` access during TLS teardown would otherwise recurse).
fn if_armed(f: impl FnOnce()) {
    let armed = ARMED.try_with(|armed| armed.get()).unwrap_or(false);
    if armed {
        f();
    }
}

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            if_armed(|| {
                let live = LIVE.with(|live| {
                    let updated = live.get() + layout.size() as i64;
                    live.set(updated);
                    updated
                });
                PEAK.with(|peak| peak.set(peak.get().max(live)));
            });
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if_armed(|| {
            // Clamp at zero: memory allocated before the window may be freed
            // inside it (see ferrosa-index's fulltext_topk_memory_bound.rs).
            LIVE.with(|live| live.set((live.get() - layout.size() as i64).max(0)));
        });
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
    ARMED.with(|armed| armed.set(true));
    let out = f();
    ARMED.with(|armed| armed.set(false));
    (out, PEAK.with(|peak| peak.get()))
}

/// A sidecar with `n` postings under one hot key (a tenant with `n` session
/// partitions) plus as many under other keys.
fn write_sidecar(dir: &std::path::Path, n: usize) -> std::path::PathBuf {
    let path = dir.join(format!("7-idx_by_tenant-{n}.sidecar"));
    let entries: Vec<(IndexKey, RowPosition)> = fixture_entries(n);
    SidecarWriter::write(&path, &entries).expect("write sidecar");
    path
}

/// The postings both sidecar tests use: `n` under one hot key plus `n` spread
/// over distinct keys — the shape a tenant-wide index has.
///
/// ONE definition, shared by the read-side test (which writes them to a file)
/// and the write-side test (which inserts them into a memtable index). They
/// used to build this independently and had already drifted: the file fixture
/// zero-padded to 8 digits, the memtable fixture to 16. Two generators for one
/// fixture means a change to the shape lands in one and not the other, and the
/// pair of tests stops describing the same thing while still both passing.
fn fixture_entries(n: usize) -> Vec<(IndexKey, RowPosition)> {
    (0..n)
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
                    IndexKey(format!("tenant-{i:016}").into_bytes()),
                    RowPosition {
                        partition_key,
                        clustering_key: Vec::new(),
                    },
                ),
            ]
        })
        .collect()
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

// ── Writing: the flush side must stream too ──────────────────────────────────

/// A memtable index holding the same postings the file fixture uses — see
/// [`fixture_entries`]. Sharing one generator is what keeps the read-side and
/// write-side tests describing the same shape.
///
/// Gated with the test that uses it: in the default (non-`slow-tests`) build
/// this helper has no caller, and CI's `clippy --all-targets -D warnings`
/// rejects the dead code that would otherwise result.
#[cfg(feature = "slow-tests")]
fn memtable_index_with(n: usize) -> MemtableIndex {
    let index = MemtableIndex::new();
    for (key, position) in fixture_entries(n) {
        index.insert(key, position);
    }
    index
}

/// Hard budget for writing a sidecar out of a memtable index: one entry in
/// flight plus the writer's own buffers, whatever the index holds.
/// Gated with the test that asserts against it (see `memtable_index_with`).
#[cfg(feature = "slow-tests")]
const WRITE_BUDGET_BYTES: i64 = 256 * 1024;

/// Flushing an index to its sidecar must not copy the index to do it.
///
/// The flush path used to materialise the whole posting set three times over
/// before a byte reached disk: `MemtableIndex::iter` deep-copied the tree,
/// the flatten cloned the key once per posting, and `SidecarWriter::write`
/// took its own `to_vec` of that to sort it. On a node whose memtable index
/// is large, the flush — not the query — was the peak.
///
/// The tree is already in `(key, row)` order, which is exactly the order the
/// sidecar wants, so the entries can go straight to disk one at a time.
///
/// Gated behind `slow-tests`: see the module docs at the top of this file for
/// why the nesting is `sidecar::slow` rather than a bare `mod slow`.
#[cfg(feature = "slow-tests")]
mod sidecar {
    mod slow {
        use super::super::*;

        #[test]
        fn writing_a_sidecar_from_a_memtable_index_holds_a_bounded_heap() {
            const SMALL_N: usize = 2_000;
            const LARGE_N: usize = 64_000;

            let dir = tempfile::tempdir().unwrap();
            let small_index = memtable_index_with(SMALL_N);
            let large_index = memtable_index_with(LARGE_N);

            let small_path = dir.path().join("9-idx_small.sidecar");
            let large_path = dir.path().join("9-idx_large.sidecar");

            let (small_written, small_peak) = measure_peak(|| {
                SidecarWriter::write_from_source(&small_path, &small_index.pin()).unwrap()
            });
            let (large_written, large_peak) = measure_peak(|| {
                SidecarWriter::write_from_source(&large_path, &large_index.pin()).unwrap()
            });

            assert_eq!(small_written, SMALL_N as u64 * 2, "every posting written");
            assert_eq!(large_written, LARGE_N as u64 * 2, "every posting written");
            assert!(
                large_peak <= WRITE_BUDGET_BYTES,
                "writing a {LARGE_N}-key index peaked at {large_peak} bytes of heap (budget \
                 {WRITE_BUDGET_BYTES}); the flush must stream its postings, not copy them"
            );
            assert!(
                large_peak <= small_peak * 2 + 64 * 1024,
                "write heap must not grow with the index: {SMALL_N} keys peaked at {small_peak} bytes, \
                 {LARGE_N} at {large_peak}"
            );

            // Streaming is only worth anything if the file is still correct.
            assert_eq!(open_and_walk(&large_path), LARGE_N, "hot key round-trips");
        }
    }
}
