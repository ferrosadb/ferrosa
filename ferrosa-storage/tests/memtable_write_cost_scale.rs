//! T-F4-3 (outage 2026-10-10, F4/D5, board task t_32b6670b): the cost of one
//! memtable write must not depend on how big its partition already is.
//!
//! ## What was still O(N) after the deep clone was removed
//!
//! `memtable_write_alloc_bound.rs` pins the fix for the per-write deep clone
//! (allocation count). Both memtables still re-sized the partition on every
//! write by walking EVERY row twice (`estimate_partition_size` before and
//! after the merge). That allocates nothing, so the allocation guard cannot
//! see it, but it is O(rows-in-partition) per write and O(N^2) to fill a
//! partition, which is what commit-log replay does.
//!
//! ## Measured curves
//!
//! Debug build (the profile this test runs under), 2,000 writes into one
//! partition already holding N rows, on an M-series Mac, before the fix:
//!
//! | memtable | shape     | N=5k     | N=10k    | N=20k    | N=40k     | 40k / 5k |
//! |----------|-----------|----------|----------|----------|-----------|----------|
//! | skiplist | append    | 147 us   | 279 us   | 538 us   | 1,036 us  | 7.0x     |
//! | skiplist | overwrite | 135 us   | 252 us   | 566 us   | 900 us    | 6.7x     |
//! | sharded  | append    | 149 us   | 271 us   | 594 us   | 956 us    | 6.4x     |
//! | sharded  | overwrite | 116 us   | 596 us   | 485 us   | 971 us    | 8.4x     |
//!
//! Allocations per write were already flat (0 for append, 1 for overwrite:
//! the reconciled cell's value) at every N. So the curve is linear in N (8x
//! the rows, 6.4-8.4x the time) and an O(1) write predicts ~1.0x per doubling
//! plus the O(log N) binary search. Each point is the fastest of several
//! batches, which strips scheduling noise from the measurement instead of
//! averaging it in.
//!
//! After the fix (the merge returns the size delta of the rows it touched),
//! the same test's 500-write batches measured, at N = 5k / 10k / 20k / 40k:
//! skip list 416 / 431 / 441 / 474 us, sharded 367 / 391 / 408 / 416 us;
//! before it, 51.8 / 104.0 / 209.1 / 422.1 ms. With the full walk restored in
//! the sharded memtable alone, that memtable fails and the skip list passes.
//!
//! ## Why growth is measured per doubling, and why the median
//!
//! This test was first written against the ratio between the two ENDPOINTS
//! (N=40k / N=5k, bound 2.5x), on the reasoning that a ratio cancels out a
//! slower or busier runner. That reasoning is wrong on CI, and it ejected a PR
//! from the merge queue: `cargo nextest run` schedules one process per test
//! across every core, so ~9,600 test processes contend while these four points
//! are sampled seconds apart. The endpoints then measure the machine at two
//! different moments, and a single inflated sample decides the ratio.
//!
//! Observed on the runner (skip list, us at N = 5k/10k/20k/40k): 1,972 /
//! 2,038 / 2,146 / 5,331. Flat, flat, then the LAST point doubles and a half.
//! That is not the shape of the defect: an O(N) walk doubles the cost at EVERY
//! doubling, and the defect's own curve is 51.8 / 104.0 / 209.1 / 422.1 ms
//! (2.0x per doubling). The last sample here is contention, and 5,331 us is
//! still ~80x faster than the 422 ms the unfixed code costs.
//!
//! So the criterion is the MEDIAN growth per doubling — the shape of the
//! curve, rather than two of its samples. A median discards one contaminated
//! point; an endpoint ratio is decided by it. Measured:
//!
//! | curve                        | median growth per doubling | 40k / 5k |
//! |------------------------------|----------------------------|----------|
//! | correct, worst of 8 local    | 1.11                       | 1.26     |
//! | correct, CI (contaminated)   | 1.05                       | 2.70     |
//! | defect (full partition walk) | 2.01                       | 8.15     |
//!
//! The bound is 1.5x: 35% above every correct curve measured and 34% below the
//! defect's, and no single sample can move a median across it.
//!
//! The blind spot, stated because it is a real loss of sensitivity: this
//! criterion fails when the growth is typical of the whole curve, so a
//! regression confined to the LARGEST size alone (40k slow, 5k-20k correct)
//! would not move the median. That case is covered instead by
//! `merge_size_delta_matches_the_full_partition_walk`, which pins the size
//! accounting directly and cannot be affected by the clock at all.
//!
//! The allocation count is asserted too, for both memtables (the existing
//! guard covers only the skip list): writes into a 40k-row partition must not
//! allocate more than writes into a 5k-row one.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::memtable::sharded::ShardedBTreeMemtable;
use ferrosa_storage::memtable::skiplist::SkipListMemtable;
use ferrosa_storage::memtable::Memtable;

// One #[test] in this binary, so nothing else allocates inside a window.
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

/// Partition sizes the curve was measured at.
const SIZES: [i32; 4] = [5_000, 10_000, 20_000, 40_000];
/// Writes per measured batch.
const BATCH: i32 = 500;
/// Batches per point; the fastest is kept.
const REPS: usize = 5;
/// Allowed median growth in per-write cost for each doubling of the partition.
const MAX_DOUBLING_RATIO: f64 = 1.5;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".into(),
        table: "write_cost".into(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".into(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".into(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".into(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        }],
        extensions: Default::default(),
    }
}

fn row(ck: i32, ts: i64) -> Row {
    Row {
        clustering: ck.to_be_bytes().to_vec(),
        cells: vec![(0, CellValue::live(b"payload-payload".to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// Cost of one point on the curve.
#[derive(Debug, Clone, Copy)]
struct Point {
    rows: i32,
    fastest_batch: Duration,
    allocs_per_batch: usize,
}

/// Fill one partition to `rows`, then time `REPS` batches of overwrites spread
/// across it (overwrites do not grow the partition, so every rep measures the
/// same N). Rows are built before the clock and the counter start.
fn measure(mem: &dyn Memtable, rows: i32) -> Point {
    let schema = schema();
    let key = DecoratedKey::new(PartitionKey::new(b"pk".to_vec()));
    for ck in 0..rows {
        mem.put(&key, row(ck, 1), &schema).unwrap();
    }
    let mut fastest = Duration::MAX;
    let mut allocs = usize::MAX;
    for rep in 0..REPS {
        let batch: Vec<Row> = (0..BATCH)
            .map(|i| row((i * 7_919 + rep as i32) % rows, 2 + rep as i64))
            .collect();
        ALLOCS.store(0, Ordering::Relaxed);
        ARMED.store(true, Ordering::Relaxed);
        let started = Instant::now();
        for r in batch {
            mem.put(&key, r, &schema).unwrap();
        }
        let took = started.elapsed();
        ARMED.store(false, Ordering::Relaxed);
        fastest = fastest.min(took);
        allocs = allocs.min(ALLOCS.load(Ordering::Relaxed));
    }
    assert_eq!(
        mem.partition_count(),
        1,
        "every write went to one partition"
    );
    Point {
        rows,
        fastest_batch: fastest,
        allocs_per_batch: allocs,
    }
}

fn curve(make: &dyn Fn() -> Box<dyn Memtable>) -> Vec<Point> {
    SIZES.iter().map(|&n| measure(make().as_ref(), n)).collect()
}

/// The curve's typical growth in per-write cost for each doubling of the
/// partition: the SHAPE of the curve, rather than two of its samples. A median
/// discards one contaminated sample; an endpoint ratio is decided by it.
fn growth_per_doubling(points: &[Point]) -> f64 {
    let mut ratios: Vec<f64> = points
        .windows(2)
        .map(|w| w[1].fastest_batch.as_secs_f64() / w[0].fastest_batch.as_secs_f64())
        .collect();
    ratios.sort_by(|a, b| a.partial_cmp(b).expect("durations are never NaN"));
    ratios[ratios.len() / 2]
}

/// What is wrong with one memtable's curve, if anything.
fn violations(name: &str, points: &[Point]) -> Vec<String> {
    let (first, last) = (points[0], points[points.len() - 1]);
    let growth = growth_per_doubling(points);
    let mut found = Vec::new();
    if growth >= MAX_DOUBLING_RATIO {
        found.push(format!(
            "{name}: each doubling of the partition costs {growth:.2}x more per write (bound \
             {MAX_DOUBLING_RATIO}x). The write walks the whole partition: O(N) per write, \
             O(N^2) to replay a partition. Curve: {points:?}"
        ));
    }
    if last.allocs_per_batch > first.allocs_per_batch + 2 {
        found.push(format!(
            "{name}: allocations per batch grew with the partition ({} at {} rows vs {} at \
             {} rows): a per-write copy of the partition. Curve: {points:?}",
            last.allocs_per_batch, last.rows, first.allocs_per_batch, first.rows
        ));
    }
    found
}

#[test]
fn memtable_write_cost_is_independent_of_partition_size() {
    let skiplist = curve(&|| Box::new(SkipListMemtable::new()));
    let sharded = curve(&|| Box::new(ShardedBTreeMemtable::new(4)));
    let mut found = violations("SkipListMemtable", &skiplist);
    found.extend(violations("ShardedBTreeMemtable", &sharded));
    assert!(found.is_empty(), "{}", found.join("\n"));
}
