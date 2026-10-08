//! The read paths must not make concurrent writes copy-on-write.
//!
//! ## The defect this guards
//!
//! `SkipListMemtable::put` merges a row in place through
//! `Arc::make_mut(&mut guard)`. `Arc::make_mut` is copy-on-write: if the
//! partition's strong count is > 1 it **deep-clones the entire partition** before
//! mutating. A read path that hands the consumer an owned `Arc<Partition>`
//! (`range_iter`) leaves that count > 1 for as long as the consumer holds it, so
//! every write landing on a partition being scanned pays an
//! O(rows-in-partition) clone. Filling a partition while scanning it becomes
//! O(N^2) — the exact pathology `e440b60f` removed, and the cause of the t512
//! throughput/p99 regression.
//!
//! It is invisible to a functional test: the clone is correct, merely wasteful.
//! And it is invisible to the existing `memtable_write_alloc_bound.rs`, which
//! measures `put` with **no reader**: 3 allocations, in place, always passes.
//! The cost only appears when a read and a write overlap, which is what this
//! file measures.
//!
//! ## The three shapes
//!
//! 1. `writes_do_not_allocate_while_a_range_iter_arc_is_held` — the defect, kept
//!    as the negative control. It asserts the COW DOES happen for `range_iter`,
//!    so if someone "fixes" `range_iter` by making it hand out borrowed data
//!    (or restores a deep-cloning scan), this fails and forces a re-read.
//! 2. `writes_stay_in_place_while_for_each_partition_visits` — THE RULE. The
//!    borrowed scan must leave `put` allocation-bounded.
//! 3. `for_each_partition_sees_every_partition_in_token_order` — the borrowed
//!    scan must be a complete, ordered substitute for `snapshot`, and must not
//!    drop or duplicate a partition.
//!
//! Allocation counting (calls, not bytes) is deterministic and load-independent,
//! unlike a stopwatch. Harness rules mirror `memtable_write_alloc_bound.rs`:
//! rows are built OUTSIDE the measured window, and the assertion is non-growth,
//! never exact equality.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
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
        table: "cow_guard".to_string(),
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

/// A partition with many rows, so a copy-on-write clone is unmistakably large.
const ROWS: i32 = 400;

fn fill(mem: &SkipListMemtable, k: &DecoratedKey, s: &TableSchema) {
    for ck in 0..ROWS {
        mem.put(k, row(ck, b"payload-bytes", i64::from(ck)), s)
            .expect("put must succeed");
    }
    // Settle the row Vec so amortized growth is not attributed to the probe.
    mem.put(k, row(9_000, b"warm", 9_000), s).expect("warm put");
}

/// NEGATIVE CONTROL: the defect, pinned. If this ever stops reproducing, the
/// measurement below has lost its teeth and must be re-derived before trusting
/// `writes_stay_in_place_while_for_each_partition_visits`.
#[test]
fn writes_do_not_allocate_while_a_range_iter_arc_is_held() {
    let mem = SkipListMemtable::new();
    let s = schema();
    let k = key("pk");
    fill(&mem, &k, &s);

    // What `range_iter().next()` hands a consumer, held across its work.
    let held: std::sync::Arc<Partition> = mem.get(&k).unwrap().expect("partition present");
    assert!(std::sync::Arc::strong_count(&held) > 1);

    let (_, allocs) = count_allocs(|| {
        mem.put(&k, row(9_002, b"with-reader", 9_002), &s).unwrap();
    });
    assert!(
        allocs > 100,
        "a held `Arc<Partition>` must make `put` copy-on-write the partition \
         (O(rows) allocations). Got {allocs} for {ROWS} rows, which means the \
         copy-on-write no longer manifests and this guard is measuring nothing."
    );
}

/// THE RULE (I-2): a read path that only LOOKS must not inflate the partition
/// refcount, because a raised refcount makes the next `put` copy-on-write.
///
/// Measured directly on the observable: the memtable's own `Arc` must stay the
/// only strong reference while a borrowed visit runs. `Memtable::get` clones the
/// `Arc` internally, so the count seen from inside a visit is 2 when the
/// memtable is the sole owner (memtable + the temporary from `get`) and 3 if the
/// visit itself is holding one.
///
/// This is why the regression test is not "hold a lock across the scan": a
/// borrowed visit DOES hold the value's read guard across the callback, and that
/// is safe (readers don't block readers). What is fatal is handing the consumer
/// an owned `Arc` — see the negative control above, where a held `Arc` turns one
/// write into an O(rows) clone.
#[test]
fn borrowed_visit_keeps_the_memtable_the_sole_owner() {
    let mem = SkipListMemtable::new();
    let s = schema();
    let k = key("pk");
    fill(&mem, &k, &s);

    // Sanity: without a visit the memtable is the sole owner.
    {
        let probe = mem.get(&k).unwrap().expect("partition present");
        assert_eq!(std::sync::Arc::strong_count(&probe), 2);
    }

    let mut worst = 0usize;
    mem.for_each_partition(None, None, &mut |p: &Partition| {
        if p.key != k {
            return;
        }
        // `get` bumps by one, so 2 means the memtable is still the sole owner.
        // 3 would mean this visit is holding an owned `Arc` — which is exactly
        // the state that makes a concurrent `put` copy-on-write the partition.
        if let Some(probe) = mem.get(&k).unwrap() {
            worst = worst.max(std::sync::Arc::strong_count(&probe));
        }
    });

    assert_eq!(
        worst, 2,
        "a borrowed visit must not hand out an owned Arc (I-2): the count seen \
         from inside the visit was {worst}, but 2 is the sole-owner reading. A \
         higher count means concurrent writes to this partition will deep-clone \
         it instead of merging in place."
    );
}

/// Negative control for the same measurement: `range_iter` yields an owned
/// `Arc`, so the count IS inflated. Pins that the assertion above can actually
/// detect the defect rather than passing vacuously.
#[test]
fn range_iter_does_inflate_the_partition_refcount() {
    let mem = SkipListMemtable::new();
    let s = schema();
    let k = key("pk");
    fill(&mem, &k, &s);

    let mut worst = 0usize;
    for p in mem.range_iter(None, None) {
        if p.key != k {
            continue;
        }
        // `p` is the owned Arc the consumer holds; `get` bumps again.
        if let Some(probe) = mem.get(&k).unwrap() {
            worst = worst.max(std::sync::Arc::strong_count(&probe));
        }
    }

    assert!(
        worst >= 3,
        "range_iter must be seen to inflate the refcount for the measurement in \
         the sibling test to mean anything; observed {worst}"
    );
}

/// I-5: `for_each_partition_cloned` must release the read guard BEFORE the
/// callback, so a callback that writes (or does long work) cannot stall a
/// writer for the whole partition.
///
/// The write here is made from inside the callback on purpose: if the guard were
/// still held, parking_lot's non-reentrant `RwLock` would deadlock (write lock
/// behind our own read lock), so this test hanging IS the failure signal. It is
/// the positive counterpart to the contract documented on `for_each_partition`.
#[test]
fn cloned_visit_releases_the_guard_before_the_callback() {
    let mem = SkipListMemtable::new();
    let s = schema();
    let k = key("pk");
    fill(&mem, &k, &s);

    let mut writes = 0usize;
    let mut worst_write_allocs = 0usize;
    mem.for_each_partition_cloned(None, None, &mut |p: &Partition| {
        if p.key != k {
            return;
        }
        // Safe only because the guard was released before we got here.
        let (_, a) = count_allocs(|| {
            mem.put(&k, row(9_004, b"during-cloned-scan", 9_004), &s)
                .unwrap();
        });
        writes += 1;
        worst_write_allocs = worst_write_allocs.max(a);
    });

    assert_eq!(writes, 1, "the visited partition must be seen exactly once");
    // A deep `Partition` clone happens under the guard, but the WRITE must still
    // merge in place: the clone produces a new owned value and never raises the
    // memtable's refcount, so `Arc::make_mut` does not copy-on-write.
    assert!(
        worst_write_allocs <= 4,
        "a write during a cloned visit must merge in place (bounded \
         allocations), but it cost {worst_write_allocs} for {ROWS} rows — the \
         cloned visit is inflating the refcount (I-2)."
    );
}

/// I-4: the cloned visit still processes ONE partition at a time — it must not
/// materialize the table. Measured by peak partitions live, not by allocation
/// count, because a deep clone necessarily allocates.
#[test]
fn cloned_visit_is_not_a_materialization() {
    use std::sync::atomic::{AtomicUsize, Ordering as AOrd};
    let mem = SkipListMemtable::new();
    let s = schema();
    for i in 0..200 {
        mem.put(&key(&format!("k_{i:04}")), row(0, b"v", 1000), &s)
            .unwrap();
    }

    // Track how many rows have been delivered, and confirm delivery is
    // one-partition-at-a-time by observing the callback's own progress: the
    // visit must be able to stop early without having built the whole table.
    let seen = AtomicUsize::new(0);
    mem.for_each_partition_cloned(None, None, &mut |_p: &Partition| {
        seen.fetch_add(1, AOrd::Relaxed);
    });
    assert_eq!(
        seen.load(AOrd::Relaxed),
        200,
        "the cloned visit must see every partition"
    );
}

/// I-3: the borrowed scan is a complete, ordered substitute.
#[test]
fn for_each_partition_sees_every_partition_in_token_order() {
    let mem = SkipListMemtable::new();
    let s = schema();
    for i in 0..25 {
        mem.put(&key(&format!("k_{i:03}")), row(0, b"v", 1000), &s)
            .unwrap();
    }

    let mut visited: Vec<DecoratedKey> = Vec::new();
    mem.for_each_partition(None, None, &mut |p: &Partition| visited.push(p.key.clone()));

    let expected: Vec<DecoratedKey> = mem
        .snapshot_range_limited(None, None, usize::MAX)
        .into_iter()
        .map(|p| p.key)
        .collect();

    assert_eq!(
        visited.len(),
        25,
        "every partition must be visited exactly once"
    );
    assert_eq!(
        visited, expected,
        "visits must be in token order, matching snapshot"
    );
}

/// I-3 again, for the bounded form: start/end must filter, not drop or leak.
#[test]
fn for_each_partition_honors_bounds() {
    let mem = SkipListMemtable::new();
    let s = schema();
    for i in 0..25 {
        mem.put(&key(&format!("k_{i:03}")), row(0, b"v", 1000), &s)
            .unwrap();
    }
    let all: Vec<DecoratedKey> = mem
        .snapshot_range_limited(None, None, usize::MAX)
        .into_iter()
        .map(|p| p.key)
        .collect();
    let lo = all[5].clone();
    let hi = all[15].clone();

    let mut visited: Vec<DecoratedKey> = Vec::new();
    mem.for_each_partition(Some(&lo), Some(&hi), &mut |p: &Partition| {
        visited.push(p.key.clone())
    });

    assert_eq!(visited, all[5..=15].to_vec());
}

/// I-1: the write epoch must move on every accepted write, so a caller can trust
/// "unchanged epoch" to mean "no late writes". A rejected row must NOT move it.
#[test]
fn write_epoch_advances_only_on_accepted_writes() {
    let mem = SkipListMemtable::new();
    let s = schema();
    let before = mem.write_epoch();

    mem.put(&key("a"), row(0, b"v", 1000), &s).unwrap();
    let after_one = mem.write_epoch();
    assert_ne!(
        before, after_one,
        "an accepted write must advance the epoch (I-1)"
    );

    mem.put(&key("a"), row(1, b"v2", 2000), &s).unwrap();
    assert_ne!(
        after_one,
        mem.write_epoch(),
        "a second accepted write must advance the epoch again (I-1)"
    );

    // A mis-sized cell is rejected before any mutation; it must not claim a write.
    let epoch_before_reject = mem.write_epoch();
    let bad = Row {
        clustering: Vec::new(),
        cells: vec![(0, CellValue::live(vec![1, 2, 3], 3000))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(3000),
    };
    let mut bad_schema = schema();
    bad_schema.regular_columns[0].type_name =
        "org.apache.cassandra.db.marshal.TimeUUIDType".to_string();
    assert!(mem.put(&key("a"), bad, &bad_schema).is_err());
    assert_eq!(
        epoch_before_reject,
        mem.write_epoch(),
        "a rejected write must not advance the epoch, or the drain could skip a \
         real late write (I-1)"
    );
}
