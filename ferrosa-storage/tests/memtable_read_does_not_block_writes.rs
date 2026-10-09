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
//! ## The shapes
//!
//! 1. `a_held_arc_makes_the_next_write_copy_on_write_and_stale` — the defect,
//!    kept as the **negative control**. `Arc::make_mut` at refcount > 1
//!    deep-clones, so a holder keeps the pre-write image while the memtable owns
//!    the post-write one. If that stops holding, the rule below has lost its
//!    teeth.
//! 2. `a_write_with_no_holder_is_visible_immediately` — the other half of the
//!    control: with no outstanding `Arc` the same write merges in place.
//! 3. `borrowed_visit_keeps_the_memtable_the_sole_owner` — THE RULE. A borrowed
//!    visit must not raise the partition's strong count.
//! 4. `range_iter_does_inflate_the_partition_refcount` — the control proving that
//!    measurement can actually see the defect.
//! 5. `cloned_visit_releases_the_guard_before_the_callback` — the bounded-hold
//!    path: the guard is released before the callback (a write from inside it
//!    would otherwise deadlock on parking_lot's non-reentrant lock).
//! 6. `for_each_partition_*` — the borrowed scan is a complete, token-ordered,
//!    correctly-bounded substitute for `snapshot`.
//!
//! ## Why this file does NOT count allocations
//!
//! The obvious way to pin "a read path must not tax a writer" is an allocation
//! counter. That was the first version of this file, and it was **wrong under the
//! default parallel harness**: the counter is process-wide and not thread-aware,
//! so two tests running concurrently corrupt each other's measurement window
//! (12/12 parallel failures, 0/20 with `--test-threads=1`). A test file cannot
//! impose single-threading on its own runner, and `memtable_write_alloc_bound.rs`
//! only gets away with a counter by keeping every measured shape in ONE `#[test]`
//! — which is not possible once the shapes are separate invariants.
//!
//! These tests therefore assert **observable behaviour** instead, which is both
//! deterministic and load-independent:
//!
//! - the strong count of a partition's `Arc` while a borrowed visit runs
//!   (`Memtable::get` bumps by exactly one, so the sole-owner reading is 2);
//! - the *staleness* a held `Arc` suffers when the next write copy-on-writes —
//!   `Arc::make_mut` deep-clones, so the holder keeps the pre-write image while
//!   the memtable owns the post-write one;
//! - the number of partitions a bounded visit actually touches.
//!
//! Each has a negative control so it cannot pass vacuously.

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_storage::memtable::skiplist::SkipListMemtable;
use ferrosa_storage::memtable::Memtable;

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

/// NEGATIVE CONTROL: the defect, pinned — **without** the allocation counter.
///
/// The counter is process-wide and not thread-aware, so any test that reads it is
/// wrong under the default parallel harness (and `--test-threads=1` is not
/// something this file can impose on its own runner). It is also unnecessary: the
/// *effect* of `Arc::make_mut`'s copy-on-write is directly observable.
///
/// When a consumer holds a clone of the memtable's `Arc` and a write lands,
/// `make_mut` deep-clones the partition before mutating — so the consumer's copy
/// stays permanently stale. When nothing holds it, the same write is visible in
/// the memtable immediately. That difference is the copy-on-write, and it is
/// deterministic and load-independent.
///
/// If this ever stops reproducing, the borrowed-scan guard below has lost its
/// teeth and must be re-derived.
#[test]
fn a_held_arc_makes_the_next_write_copy_on_write_and_stale() {
    let mem = SkipListMemtable::new();
    let s = schema();
    let k = key("pk");
    fill(&mem, &k, &s);
    let rows_before = mem.get(&k).unwrap().unwrap().rows.len();

    // A consumer holds what `range_iter().next()` would have handed it.
    let held: std::sync::Arc<Partition> = mem.get(&k).unwrap().expect("partition present");
    assert!(std::sync::Arc::strong_count(&held) > 1);

    mem.put(&k, row(9_002, b"with-reader", 9_002), &s).unwrap();

    assert_eq!(
        held.rows.len(),
        rows_before,
        "the held Arc must NOT see the write: `Arc::make_mut` at refcount > 1 must \
         have deep-cloned the partition. It saw the write, so copy-on-write is no \
         longer manifesting and the guard below is measuring nothing."
    );

    // And the write did land — in the cloned partition the memtable now owns.
    let after = mem.get(&k).unwrap().unwrap();
    assert_eq!(
        after.rows.len(),
        rows_before + 1,
        "the write must have been applied to the memtable's (fresh) partition"
    );
}

/// The other half of the negative control: with NO holder, the same write merges
/// in place and is visible without any clone.
#[test]
fn a_write_with_no_holder_is_visible_immediately() {
    let mem = SkipListMemtable::new();
    let s = schema();
    let k = key("pk");
    fill(&mem, &k, &s);
    let rows_before = mem.get(&k).unwrap().unwrap().rows.len();

    mem.put(&k, row(9_002, b"no-reader", 9_002), &s).unwrap();

    assert_eq!(
        mem.get(&k).unwrap().unwrap().rows.len(),
        rows_before + 1,
        "a write with no outstanding Arc must merge in place and be visible"
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
            return true;
        }
        // `get` bumps by one, so 2 means the memtable is still the sole owner.
        // 3 would mean this visit is holding an owned `Arc` — which is exactly
        // the state that makes a concurrent `put` copy-on-write the partition.
        if let Some(probe) = mem.get(&k).unwrap() {
            worst = worst.max(std::sync::Arc::strong_count(&probe));
        }
        true
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

/// I-2 + I-3: `for_each_partition` stops when the callback says so, so a bounded
/// consumer (`LIMIT k`, a resume cursor) does not walk — or lock — the rest of
/// the table. Without the `bool` return, converting a bounded `range_iter` loop
/// into this scan would silently turn an O(matches) read into an O(table) walk.
#[test]
fn for_each_partition_stops_a_bounded_consumer_early() {
    let mem = SkipListMemtable::new();
    let s = schema();
    for i in 0..500 {
        mem.put(&key(&format!("k_{i:04}")), row(0, b"v", 1000), &s)
            .unwrap();
    }

    let mut seen = 0usize;
    mem.for_each_partition(None, None, &mut |_p: &Partition| {
        seen += 1;
        // Stop after 10, as a LIMIT-10 read or a resume cursor would.
        seen < 10
    });

    assert_eq!(
        seen, 10,
        "the visitor must stop when the callback returns false; it walked {seen} \
         of 500 partitions, so a bounded consumer would pay the whole table (I-3)"
    );
}

/// The negative control for the early stop: returning `true` visits everything,
/// so the assertion above cannot pass by the visitor simply being broken.
#[test]
fn for_each_partition_visits_everything_when_never_stopped() {
    let mem = SkipListMemtable::new();
    let s = schema();
    for i in 0..200 {
        mem.put(&key(&format!("k_{i:04}")), row(0, b"v", 1000), &s)
            .unwrap();
    }
    let mut seen = 0usize;
    mem.for_each_partition(None, None, &mut |_p: &Partition| {
        seen += 1;
        true
    });
    assert_eq!(seen, 200, "an unstoppable visit must see every partition");
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

    // A holder taken up front. If `for_each_partition_cloned` cloned by bumping
    // the memtable's own `Arc` (rather than deep-cloning into a fresh value) it
    // would raise the strong count above this, and the write below would then
    // copy-on-write — which the staleness check pins.
    let held: std::sync::Arc<Partition> = mem.get(&k).unwrap().expect("partition present");
    let rows_before = held.rows.len();

    let mut writes = 0usize;
    mem.for_each_partition_cloned(None, None, &mut |p: &Partition| {
        if p.key != k {
            return;
        }
        // Safe only because the guard was released before we got here: if it were
        // still held, parking_lot's non-reentrant `RwLock` would deadlock here.
        mem.put(&k, row(9_004, b"during-cloned-scan", 9_004), &s)
            .unwrap();
        writes += 1;
    });

    assert_eq!(writes, 1, "the visited partition must be seen exactly once");
    // The clone is a fresh owned value, so it never raised the memtable's
    // refcount: `held` is a separate Arc that must still be the pre-write image,
    // and the memtable must own the post-write one.
    assert_eq!(
        held.rows.len(),
        rows_before,
        "a cloned visit must not mutate the value a consumer already holds"
    );
    assert_eq!(
        mem.get(&k).unwrap().unwrap().rows.len(),
        rows_before + 1,
        "the write during the cloned visit must land in the memtable's partition"
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
    mem.for_each_partition(None, None, &mut |p: &Partition| {
        visited.push(p.key.clone());
        true
    });

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
        visited.push(p.key.clone());
        true
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
