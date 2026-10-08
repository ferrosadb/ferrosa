//! PROBE: does a concurrent reader holding a yielded `Arc<Partition>` force the
//! next writer to deep-clone the whole partition?
//!
//! `SkipListMemtable::put` merges in place via `Arc::make_mut(&mut guard)`.
//! `Arc::make_mut` is copy-on-write: when the partition's refcount is > 1 it
//! deep-clones the ENTIRE partition before mutating. The module docs record that
//! this pathology once dominated (52% wall time in `drop_slow`, O(N^2) fill).
//!
//! `Snapshot()` did NOT inflate the refcount — it read the `Arc` under a brief
//! guard, deep-cloned the `Partition`, and dropped `Arc` refcount back to 1. So a
//! scan never made a writer pay more.
//!
//! `range_iter` DOES inflate it: `Arc::clone(&entry.value().read())` hands the
//! clone to the consumer, which holds it for the duration of its work. If a write
//! lands on that partition in the window, the writer deep-clones.
//!
//! This probe is deterministic and load-independent — it asserts the mechanism,
//! not a stopwatch.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::memtable::skiplist::SkipListMemtable;
use ferrosa_storage::memtable::Memtable;

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
        table: "cow_probe".to_string(),
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

fn key(name: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec()))
}

fn row(ck: i32, value: &[u8], ts: i64) -> Row {
    Row {
        clustering: ck.to_be_bytes().to_vec(),
        cells: vec![(0, CellValue::live(value.to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

const ROWS: i32 = 400;

fn fill(mem: &SkipListMemtable, k: &DecoratedKey, s: &TableSchema) {
    for ck in 0..ROWS {
        mem.put(k, row(ck, b"payload-bytes", i64::from(ck)), s)
            .unwrap();
    }
}

/// A writer with NO reader holding an `Arc` — the baseline.
#[test]
fn probe_baseline_write_allocations() {
    let mem = SkipListMemtable::new();
    let s = schema();
    let k = key("pk");
    fill(&mem, &k, &s);

    // Warm the partition so the Vec has settled (amortized growth only).
    mem.put(&k, row(9000, b"warm", 9000), &s).unwrap();

    let (_, allocs) = count_allocs(|| {
        mem.put(&k, row(9001, b"baseline", 9001), &s).unwrap();
    });
    println!("PROBE baseline (no reader holding Arc): {allocs} allocs");
    assert!(
        allocs <= 2,
        "baseline in-place merge should not allocate per write, got {allocs}"
    );
}

/// A writer WITH a reader holding the yielded `Arc` — what `range_iter` produces.
#[test]
fn probe_write_allocations_while_reader_holds_arc() {
    let mem = SkipListMemtable::new();
    let s = schema();
    let k = key("pk");
    fill(&mem, &k, &s);
    mem.put(&k, row(9000, b"warm", 9000), &s).unwrap();

    // This is exactly what `range_iter().next()` hands out: a clone of the
    // memtable's `Arc<Partition>`, held by the consumer while it walks.
    let held: std::sync::Arc<ferrosa_sstable::types::Partition> =
        mem.get(&k).unwrap().expect("partition present");

    let (_, allocs) = count_allocs(|| {
        mem.put(&k, row(9002, b"with-reader", 9002), &s).unwrap();
    });
    println!("PROBE with reader holding Arc: {allocs} allocs (partition has {ROWS} rows)");
    println!(
        "PROBE held Arc strong_count at write time: {}",
        std::sync::Arc::strong_count(&held)
    );

    // If copy-on-write fired, this is ~O(rows) not O(1).
    if allocs > 10 {
        println!(
            "PROBE VERDICT: COW CLONE CONFIRMED — writer paid {allocs} allocs \
             because a reader held an Arc (refcount > 1)."
        );
    } else {
        println!("PROBE VERDICT: no COW clone — writer stayed in place ({allocs} allocs).");
    }
}
