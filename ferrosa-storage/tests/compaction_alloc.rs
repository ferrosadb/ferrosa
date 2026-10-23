//! Compaction scratch costs measured at existing public entry points.
//! Correctness: outputs retain keys, winners and collection deletion sentinels.
//! Last revised: 2026-10-23
//! Last changed: Added allocation regressions before optimizing reconciliation.

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_storage::{
    compaction::purge::{self, PurgePolicy},
    merge,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counter;
static EVENTS: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}
#[global_allocator]
static ALLOCATOR: Counter = Counter;
fn events() -> usize {
    EVENTS.load(Ordering::Relaxed)
}
fn row(i: i32) -> Row {
    Row {
        clustering: i.to_be_bytes().to_vec(),
        cells: vec![(0, CellValue::live(b"v".to_vec(), 1000))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1000),
    }
}
fn partition(rows: Vec<Row>) -> Partition {
    Partition {
        key: DecoratedKey::new(PartitionKey::new(b"p".to_vec())),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows,
    }
}

// One test owns the process allocator window; fixture construction is excluded.
#[test]
fn compaction_reconciliation_retains_storage() {
    let mut p = partition((0..512).rev().map(row).collect());
    let before = events();
    merge::ensure_partition_rows_sorted(&mut p);
    let folding = events() - before;
    assert!(p
        .rows
        .iter()
        .enumerate()
        .all(|(i, r)| r.clustering == (i as i32).to_be_bytes()));
    eprintln!("COMPACTION_ALLOC fold_events={folding}");
    // Stable sort may allocate its workspace once; folding must retain the Vec.

    let mut a = row(0);
    a.cells.reserve(8);
    let mut b = row(0);
    b.cells[0].1 = CellValue::live(b"winner".to_vec(), 2000);
    let before = events();
    let merged = merge::merge_rows(a, b);
    let merging = events() - before;
    assert_eq!(
        merged.cells[0].1.value.as_deref(),
        Some(b"winner".as_slice())
    );
    eprintln!("COMPACTION_ALLOC merge_events={merging}");

    let mut r = row(0);
    r.cells = vec![
        (0, CellValue::tombstone(100, 1)),
        (
            0,
            CellValue::live(b"element".to_vec(), 200).with_path(b"path".to_vec()),
        ),
        (1, CellValue::tombstone(100, 1)),
    ];
    let mut p = partition(vec![r]);
    let policy = PurgePolicy {
        gc_before: 100,
        max_purgeable_timestamp: i64::MAX,
        table_delete: DeletionTime::LIVE,
    };
    let before = events();
    let stats = purge::purge_partition(&mut p, &policy);
    let purging = events() - before;
    assert_eq!(stats.cell_tombstones, 1);
    assert_eq!(p.rows[0].cells.len(), 2);
    assert!(p.rows[0].cells[0].1.is_tombstone());
    assert_eq!(
        p.rows[0].cells[1].1.path.as_deref(),
        Some(b"path".as_slice())
    );
    eprintln!("COMPACTION_ALLOC purge_events={purging}");
    assert!(folding <= 1 && merging == 0 && purging == 0, "scratch allocations: fold={folding} (budget 1 stable-sort workspace), merge={merging} (budget 0 reserved storage), purge={purging} (budget 0)");
}
