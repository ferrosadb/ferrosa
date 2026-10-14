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
//! ## Measured curve (the bound below comes from it)
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
//! Allocations per write were flat (0 for append, and 0 for overwrite: the
//! reconciled cell's value is moved, not cloned) at every N. So the curve is linear in N
//! (8x the rows, 6.4-8.4x the time), and an O(1) write predicts 1.0x plus
//! the O(log N) binary search. The bound is a 2.5x ratio between N=40k and
//! N=5k: far below the 6.4x the defect produces, far above the ~1x a
//! correct write produces, so neither machine load nor a slower runner
//! turns it into a coin flip. Each point is the fastest of several batches,
//! which strips scheduling noise from the measurement instead of averaging
//! it in.
//!
//! After the fix (the merge returns the size delta of the rows it touched),
//! the same test's 500-write batches measured, at N = 5k / 10k / 20k / 40k:
//! skip list 416 / 431 / 441 / 474 us (1.1x), sharded 367 / 391 / 408 /
//! 416 us (1.1x); before it, 51.8 / 104.0 / 209.1 / 422.1 ms (8.1x). With
//! the full walk restored in the sharded memtable alone, that memtable fails
//! at 8.5x and the skip list passes.
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
/// Allowed per-write time ratio between the largest and smallest partition.
const MAX_TIME_RATIO: f64 = 2.5;

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

/// What is wrong with one memtable's curve, if anything.
fn violations(name: &str, points: &[Point]) -> Vec<String> {
    let (first, last) = (points[0], points[points.len() - 1]);
    let ratio = last.fastest_batch.as_secs_f64() / first.fastest_batch.as_secs_f64();
    let mut found = Vec::new();
    if ratio >= MAX_TIME_RATIO {
        found.push(format!(
            "{name}: a write into a {}-row partition costs {ratio:.1}x one into a {}-row \
             partition (bound {MAX_TIME_RATIO}x). The write walks the whole partition: \
             O(N) per write, O(N^2) to replay a partition. Curve: {points:?}",
            last.rows, first.rows
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
