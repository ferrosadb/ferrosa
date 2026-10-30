//! Allocation and payload-identity guards for the Arrow → CQL transpose.
//!
//! The historical transpose cloned owned strings. CQL text now has immutable
//! shared backing, so both moving and cloning must retain payload allocations.
//! This isolated fixture bounds allocations to row/iterator vectors and checks
//! every payload pointer; it does not measure Arrow decoding or Flight throughput.
//!
//! The process-wide allocator is armed only inside this binary's single test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ferrosa_common::CqlValue;

struct CountingAllocator;

static ARMED: AtomicBool = AtomicBool::new(false);
static ALLOC_EVENTS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn measure<T>(f: impl FnOnce() -> T) -> (T, usize) {
    ARMED.store(true, Ordering::SeqCst);
    let before = ALLOC_EVENTS.load(Ordering::SeqCst);
    let out = f();
    let delta = ALLOC_EVENTS.load(Ordering::SeqCst) - before;
    ARMED.store(false, Ordering::SeqCst);
    (out, delta)
}

type Cell = Option<CqlValue>;

/// Column-major fixture with immutable variable-width text in every cell.
fn column_major(ncols: usize, nrows: usize) -> Vec<Vec<Cell>> {
    (0..ncols)
        .map(|c| {
            (0..nrows)
                .map(|i| Some(CqlValue::Text(format!("col{c}-row{i:06}").into())))
                .collect()
        })
        .collect()
}

/// The ORIGINAL transpose: clone every cell out of the still-resident columns.
fn clone_transpose(cols: &[Vec<Cell>], nrows: usize) -> Vec<Vec<Cell>> {
    let mut rows = Vec::with_capacity(nrows);
    for i in 0..nrows {
        rows.push(cols.iter().map(|col| col[i].clone()).collect());
    }
    rows
}

/// The FIXED transpose: consume each column through `into_iter()` and MOVE each
/// cell into its row. Mirrors `record_batch_to_rows` exactly.
fn move_transpose(cols: Vec<Vec<Cell>>, nrows: usize) -> Vec<Vec<Cell>> {
    let mut cols: Vec<std::vec::IntoIter<Cell>> = cols.into_iter().map(|c| c.into_iter()).collect();
    let mut rows = Vec::with_capacity(nrows);
    for _ in 0..nrows {
        rows.push(cols.iter_mut().filter_map(Iterator::next).collect());
    }
    rows
}

#[test]
fn transpose_moves_or_shares_cells_without_copying_payloads() {
    const COLS: usize = 2;
    const ROWS: usize = 512;
    let cells = COLS * ROWS;

    // Build the input ONCE, and clone it OUTSIDE the measurement window: the
    // decode is common to both shapes and is not what changed.
    let base = column_major(COLS, ROWS);
    let pointers: Vec<Vec<*const u8>> = base
        .iter()
        .map(|column| {
            column
                .iter()
                .map(|cell| match cell {
                    Some(CqlValue::Text(value)) => value.as_bytes().as_ptr(),
                    _ => panic!("text fixture"),
                })
                .collect()
        })
        .collect();
    let for_clone = base.clone();
    let for_move = base;
    assert_eq!(for_clone.len(), COLS);

    let (clone_rows, clone_alloc) = measure(|| clone_transpose(&for_clone, ROWS));
    let (move_rows, move_alloc) = measure(|| move_transpose(for_move, ROWS));

    eprintln!(
        "transpose {ROWS} rows x {COLS} cols ({cells} text cells): move={move_alloc} allocs, \
         clone={clone_alloc} allocs, delta={} fewer",
        clone_alloc.saturating_sub(move_alloc)
    );

    // Semantic equivalence: same rows, same order, same values.
    assert_eq!(clone_rows, move_rows, "transpose must preserve row content");

    // Neither strategy may allocate per text cell: sharing replaces the old
    // deep clone. The small allowance covers row and iterator vectors.
    for (rows, allocations) in [(&clone_rows, clone_alloc), (&move_rows, move_alloc)] {
        assert!(
            allocations <= ROWS + COLS + 4,
            "per-cell payload allocation returned: {allocations} allocations for {cells} cells"
        );
        for (r, row) in rows.iter().enumerate() {
            for (c, cell) in row.iter().enumerate() {
                let Some(CqlValue::Text(value)) = cell else {
                    panic!("text fixture")
                };
                assert_eq!(
                    value.as_bytes().as_ptr(),
                    pointers[c][r],
                    "transpose copied payload at row {r}, column {c}"
                );
            }
        }
    }
}
