//! A memtable scan must retain O(num_shards) memory, not O(partitions).
//!
//! ## The rule
//!
//! `Memtable::range_iter` is documented as lazy:
//!
//! > The iterator must NOT pre-materialize partitions — `next()` should produce
//! > exactly one `Arc<Partition>` at a time so memtable scans contribute O(1)
//! > memory to upstream consumers like the streaming range-read handler
//! > (ADR-020).
//!
//! `ShardedBTreeMemtable::range_iter` breaks that: it collects every in-range
//! `Arc<Partition>` into one `Vec` per shard and hands the whole `Vec<Vec<…>>` to
//! `ShardedRangeIter` before yielding the first item. The clone is shallow (one
//! atomic bump per entry, no partition body copied), so this is not a deep-copy
//! bug — it is a *materialization* bug: the scan pins O(partitions) memory for its
//! whole lifetime, which for a paging cursor that parks the iterator across pages
//! can be tens of MB per open cursor on a 256 MB memtable.
//!
//! ## Why this is measured, not asserted
//!
//! `Arc::clone` does NOT allocate, so an allocation-COUNT check cannot see this
//! defect: the pre-collect only allocates when a shard's `Vec` grows
//! (`num_shards * log2(N/num_shards)` reallocs), which barely moves with N. The
//! cost is the *bytes retained* — `N * size_of::<Arc<Partition>>()` plus Vec
//! growth slack. So the signal is peak live bytes, and the invariant is that the
//! peak must NOT grow with the number of partitions in range.
//!
//! ## Why peak bytes and not a stopwatch
//!
//! A stopwatch test is host-dependent and flaky. Peak additional live bytes is
//! exact and deterministic — it fails on the materialization itself, not on how
//! loaded the machine happened to be. The tracker below is the idiom already used
//! by `recovery_oom_memory_bound.rs` and `sidecar_memory_bound.rs`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::memtable::sharded::ShardedBTreeMemtable;
use ferrosa_storage::memtable::Memtable;

// --- peak-allocation tracker (scoped to this integration-test binary only) ---
//
// Each integration test file is its own binary, so this `#[global_allocator]`
// affects nothing else in the workspace. `alloc`/`dealloc` touch only atomics and
// `System`, never the heap, so there is no reentrancy. `LIVE` is `i64` and may dip
// below zero when allocations made before arming are freed inside the window —
// that is fine: `PEAK` only grows on allocation and captures the peak *additional*
// bytes held at once during the measured call.

struct TrackingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() && ARMED.load(Ordering::Relaxed) {
            let live =
                LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed) + layout.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            // Clamp at zero: `measure_peak` zeroes LIVE at arm time, so freeing
            // memory allocated BEFORE the window would drive the counter negative
            // and, because PEAK is a running maximum of LIVE, suppress every later
            // allocation. Seeding runs outside the window by design, so how much of
            // it is released inside is pure timing.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static GLOBAL: TrackingAlloc = TrackingAlloc;

/// Run `f` with peak-allocation tracking armed; return its result and the peak
/// number of additional live bytes observed during the call.
fn measure_peak<T>(f: impl FnOnce() -> T) -> (T, i64) {
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let out = f();
    ARMED.store(false, Ordering::Relaxed);
    (out, PEAK.load(Ordering::Relaxed))
}

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "scan_memory_bound".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

/// A key whose bytes are already the engine's decorated form.
fn key(name: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec()))
}

/// One clustered row: `ck` clustering, one text cell.
fn row(ck: i32, value: &[u8], ts: i64) -> Row {
    Row {
        clustering: ck.to_be_bytes().to_vec(),
        cells: vec![(0, CellValue::live(value.to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// Seed `count` distinct partitions, OUTSIDE any measured window.
fn seed(mem: &dyn Memtable, schema: &TableSchema, count: usize) {
    for i in 0..count {
        let k = key(&format!("pk_{i:07}"));
        mem.put(&k, row(0, b"payload", 1), schema)
            .expect("seed put must succeed");
    }
}

/// How many scan items to pull before stopping. Small and constant, so a lazy
/// iterator's retained memory cannot depend on it.
const ITEMS_PULLED: usize = 8;

/// The number of shards for the measured memtable. Low, so a pre-collect's cost
/// is unambiguously per-partition (`N * 8 B`) rather than per-shard.
const SHARDS: usize = 4;

/// THE RULE, all measured shapes in ONE `#[test]`.
///
/// The tracker is process-wide and NOT thread-aware: two `#[test]` functions
/// running in parallel share `ARMED`/`LIVE`/`PEAK`, so one test's seeding lands in
/// another's window and the reading is garbage. Everything measured therefore
/// lives in this single function, and the negative control runs FIRST so the
/// control cannot be perturbed by a measurement in flight. Run this file with
/// `--test-threads=1` for the same reason.
///
/// ## Negative control (first)
///
/// Constructing a scan MUST retain something — at minimum the merge heap, one
/// entry per shard, plus the shard cursors. If the window reports 0 bytes the
/// tracker is not armed and the assertions below are vacuous.
///
/// ## The rule (rest)
///
/// The peak bytes retained while constructing a scan and pulling a constant
/// number of items must be **independent of the number of partitions in range**.
/// Two sizes are measured, 16x apart. A pre-materializing scan retains
/// `N * size_of::<Arc<Partition>>()` (plus Vec growth slack), so its peak grows
/// ~16x; a lazy scan's peak is dominated by the per-shard heap and stays flat.
#[test]
fn memtable_scan_memory_does_not_scale_with_partitions_in_range() {
    let schema = schema();
    // `ShardedBTreeMemtable::new` takes an explicit shard count, bypassing the
    // process-wide default (64) so this test is hermetic.
    let mem = ShardedBTreeMemtable::new(SHARDS);
    let mem: &dyn Memtable = &mem;

    const SMALL: usize = 2_000;
    const LARGE: usize = 32_000;

    seed(mem, &schema, SMALL);

    // --- negative control: the tracker is live and a scan retains something ---
    let (_unit, small_peak) = measure_peak(|| {
        let mut it = mem.range_iter(None, None);
        let mut held = Vec::with_capacity(ITEMS_PULLED);
        for _ in 0..ITEMS_PULLED {
            if let Some(p) = it.next() {
                held.push(p);
            }
        }
        // Hold the pulled items alive across the peak window: an `Arc` clone adds
        // no bytes, so this cannot mask the signal, but it keeps the compiler from
        // optimising the pulls away.
        std::hint::black_box(held.len())
    });
    assert!(
        small_peak > 0,
        "constructing a scan must retain at least the merge heap; 0 bytes means \
         the tracker is not armed and the assertions below prove nothing"
    );

    // --- the rule: peak retained memory must not grow with the range size ---
    seed(mem, &schema, LARGE - SMALL);

    let (_unit, large_peak) = measure_peak(|| {
        let mut it = mem.range_iter(None, None);
        let mut held = Vec::with_capacity(ITEMS_PULLED);
        for _ in 0..ITEMS_PULLED {
            if let Some(p) = it.next() {
                held.push(p);
            }
        }
        std::hint::black_box(held.len())
    });

    // 16x the partitions must not cost anywhere near 16x the memory. 4x leaves
    // room for heap/cursor growth without admitting a per-partition pre-collect.
    assert!(
        large_peak < small_peak * 4,
        "a scan over {LARGE} partitions retained {large_peak} B against \
         {small_peak} B over {SMALL} — the peak tracks the number of partitions \
         in range, so range_iter is pre-materializing instead of streaming"
    );
    // Absolute bound: the scan is over 32k partitions; a lazy scan retains the
    // per-shard heap, i.e. a few KB. 64 KB is far above that and far below the
    // ~256 KB a per-partition pre-collect would pin.
    assert!(
        large_peak < 64 * 1024,
        "a scan over {LARGE} partitions retained {large_peak} B; expected only \
         O(num_shards) (a few KB). The scan is materializing the range."
    );
}
