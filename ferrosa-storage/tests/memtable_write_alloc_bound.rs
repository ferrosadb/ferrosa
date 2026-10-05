//! The memtable write path must not allocate per write.
//!
//! ## The rule
//!
//! A write to an EXISTING partition must not allocate per write. The one
//! unavoidable allocation is the FIRST write to a partition: the partition, its
//! row, and the `Arc` that holds it have to be built once. Every subsequent
//! write to the same partition must merge into that storage without allocating
//! once per row.
//!
//! ## Why this is measured, not asserted
//!
//! `SkipListMemtable::put` once did, per write:
//!
//! ```text
//! let current   = entry.value().load_full();          // Arc clone
//! let old_size  = estimate_partition_size(&current);  // walks EVERY row
//! let mut merged = (*current).clone();                // deep-clones the WHOLE partition
//! merge_row_into_partition(&mut merged, row.clone(), schema)?;
//! let new_size  = estimate_partition_size(&merged);   // walks EVERY row again
//! let new_arc   = Arc::new(merged);                   // another allocation
//! ```
//!
//! That is O(rows-in-partition) per write, so filling one partition is O(N^2).
//! On a live node replaying a commit log this dominated: `sample` showed 52% of
//! wall time in `Arc<Partition>::drop_slow` -> jemalloc free, ~95% CPU for 18+
//! minutes with no progress and no CQL listener (replay is a barrier). Measured
//! here: 1000 writes into a growing partition cost 2,109,500 allocations
//! (~2109/write), and the per-write cost GREW as the partition grew.
//!
//! ## Why an allocation counter and not a stopwatch
//!
//! A stopwatch test is host-dependent and flaky. An allocation count is exact
//! and deterministic: it fails on the O(N) behaviour itself, not on how loaded
//! the machine happened to be. The peak-bytes idiom below is the one already
//! used by `recovery_oom_memory_bound.rs` and `sidecar_memory_bound.rs`.
//!
//! A "writes 1000 rows into a partition and is fast" test CANNOT catch this: the
//! deep clone is correct, merely wasteful, so every functional assertion passes.
//! The counter is what makes the cost visible.
//!
//! ## Two harness rules that make the measurement sound
//!
//! 1. **Build the input rows BEFORE arming the counter.** A `Row` is three
//!    allocations (clustering `Vec`, cells `Vec`, value `Vec`); constructing it
//!    inside the measured window counts the *test's* allocations, not `put`'s.
//!    An earlier draft of this file built rows in the window and asserted
//!    `allocs == 0`, which no correct implementation can satisfy — the assertion
//!    was unsatisfiable, not the implementation wrong.
//! 2. **Assert NON-GROWTH, never exact equality.** A correct in-place merge still
//!    allocates when an internal `Vec` has to grow (amortized, O(1)); pinning an
//!    exact count is flaky under CI load and was repaired twice in this repo
//!    (`fix/brittle-alloc-assertions`). The defect this guards against is a
//!    per-write cost that GROWS with the partition, so compare equal-size batches
//!    at different partition sizes and require the later batch not to cost more.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::memtable::skiplist::SkipListMemtable;
use ferrosa_storage::memtable::Memtable;

// --- allocation counter (this integration test is its own binary) ---
//
// `alloc`/`dealloc`/`realloc` touch only atomics and `System`, so there is no
// reentrancy. Counting is gated by `ARMED`. This file's tests run in one
// process; `--test-threads=1` (set below) keeps a concurrent test from flipping
// the flag inside another's window. We count CALLS, not bytes: one clone of a
// large partition is one allocation but a large one, and the defect is the
// NUMBER of allocations on the path (a clone + an Arc + two walks), so calls is
// the signal that is stable across allocator and platform.

struct CountingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// Run `f` with allocation counting armed; return its result and the number of
/// allocations made during the call.
fn count_allocs<T>(f: impl FnOnce() -> T) -> (T, usize) {
    ALLOCS.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let out = f();
    ARMED.store(false, Ordering::Relaxed);
    (out, ALLOCS.load(Ordering::Relaxed))
}

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "alloc_bound".to_string(),
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

/// Build one row per clustering key. Rows are built here, OUTSIDE any measured
/// window, so the counts below reflect `put` only.
fn rows(cks: impl IntoIterator<Item = i32>) -> Vec<Row> {
    cks.into_iter()
        .map(|ck| row(ck, b"payload", i64::from(ck)))
        .collect()
}

/// Measure the allocations `put` makes while inserting `rs` (already built)
/// into `key`. `rs` is moved in, so the only allocations in the window are the
/// memtable's.
fn measure(mem: &dyn Memtable, key: &DecoratedKey, rs: Vec<Row>, schema: &TableSchema) -> usize {
    let (_unit, allocs) = count_allocs(|| {
        for r in rs {
            mem.put(key, r, schema).expect("put must succeed");
        }
    });
    allocs
}

/// THE RULE, all measured shapes in ONE `#[test]`.
///
/// The allocation counter is process-wide and NOT thread-aware: two `#[test]`
/// functions running in parallel share `ARMED`/`ALLOCS`, so one test's inserts
/// land in another's window and the reading is garbage (observed: 2011 allocs
/// under `cargo test` vs the true 10 when measured alone). Everything measured
/// therefore lives in this single function, and the negative control runs FIRST
/// so the control cannot be perturbed by a measurement in flight. Run this file
/// with `--test-threads=1` for the same reason.
///
/// ## Negative control (first)
///
/// A write to a NEW partition IS allowed to allocate (it builds the partition
/// and its `Arc`). If this stops allocating the counter is not armed — or the
/// write path stopped building a partition at all — and the assertions below
/// would be vacuous: they would pass because nothing was measured.
///
/// ## The rule (rest)
///
/// The first write to a partition may build it. Every write after that must not
/// allocate PER WRITE: the per-write cost must not grow as the partition grows.
/// Two equal-size batches are measured, the second into a partition the first
/// already enlarged. A per-write deep clone makes the second batch cost strictly
/// more (it clones a bigger partition); an in-place merge makes them cost the
/// same, a small amortized constant. The final shape fills one partition to a
/// size no in-memory buffer would reach and requires the total to stay a small
/// constant — the O(N^2) shape that put 52% of commit-log replay into the
/// allocator cost millions here.
#[test]
fn memtable_writes_do_not_allocate_per_write() {
    let schema = schema();

    // --- negative control: the measurement is live ---
    let fresh = SkipListMemtable::new();
    let (_u, ctrl) = count_allocs(|| {
        fresh
            .put(&key("fresh"), row(0, b"seed", 0), &schema)
            .expect("put must succeed");
    });
    assert!(
        ctrl > 0,
        "building a brand-new partition must allocate; 0 means the counter is \
         not armed and the assertions below prove nothing"
    );

    // --- the rule: no per-write allocation into an existing partition ---
    let mem = SkipListMemtable::new();
    let pk = key("pk");
    mem.put(&pk, row(0, b"seed", 0), &schema).unwrap();

    const BATCH: i32 = 64;
    let first = measure(&mem, &pk, rows(1..=BATCH), &schema);
    let second = measure(&mem, &pk, rows(BATCH + 1..=2 * BATCH), &schema);

    assert!(
        first < BATCH as usize,
        "the first batch of {BATCH} writes into an existing partition cost {first} \
         allocations; a per-write clone of the partition is the O(N) defect"
    );
    assert!(
        second <= first + 2,
        "writing into a partition that is {BATCH} rows larger cost {second} allocations \
         vs {first} for the first batch. The per-write cost GREW with the partition — \
         that is the O(N) clone-per-write defect, and a full partition is O(N^2) to \
         fill. On a live node this put 52% of commit-log replay into the allocator."
    );

    // --- headline scale: filling one partition stays linear ---
    let big = SkipListMemtable::new();
    let bkey = key("bigpk");
    big.put(&bkey, row(i32::MIN, b"seed", 0), &schema).unwrap();
    let n = 4000;
    let total = measure(&big, &bkey, rows(i32::MIN + 1..i32::MIN + 1 + n), &schema);
    assert!(
        total < 200,
        "filling one partition with {n} rows cost {total} allocations. An in-place \
         merge stays near a small constant (amortized Vec growth); a per-write clone \
         costs millions. This is the O(N^2) shape that stalled replay."
    );
}
