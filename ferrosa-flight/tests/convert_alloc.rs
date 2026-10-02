//! Allocation-count guard for the Arrow → CQL (`DoPut`) decode transpose.
//!
//! `record_batch_to_rows` decodes an inbound Arrow `RecordBatch` column-major,
//! then transposes to row-major. The ORIGINAL transpose cloned every cell out of
//! the still-resident column matrix (`cols.iter().map(|col| col[i].clone())`) —
//! one deep `CqlValue` clone per cell (a `String`/`Vec<u8>` allocation for every
//! text/blob cell) while BOTH the column matrix and the growing row matrix were
//! resident. The fix MOVES each cell out (`into_iter()` per column), so no
//! `CqlValue` is cloned and the column buffers drain as the rows fill.
//!
//! This test isolates exactly that transpose: it builds and clones the
//! column-major input ONCE outside the measurement window, so each strategy
//! starts from an identical, already-decoded input, and measures only the
//! transpose step. The column decode is common to both shapes and is
//! deliberately not measured.
//!
//! Counting `#[global_allocator]` (the only one in this test binary); all
//! measurements run inside ONE `#[test]` fn because the counter is
//! process-wide and thread-unaware.

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

/// Column-major fixture: `ncols` columns of `nrows` cells, with an owned
/// variable-width (`Text`) value in every cell — the case where a per-cell
/// clone allocates.
fn column_major(ncols: usize, nrows: usize) -> Vec<Vec<Cell>> {
    (0..ncols)
        .map(|c| {
            (0..nrows)
                .map(|i| Some(CqlValue::Text(format!("col{c}-row{i:06}"))))
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
fn transpose_moves_instead_of_cloning_cells() {
    const COLS: usize = 2;
    const ROWS: usize = 512;
    let cells = COLS * ROWS;

    // Build the input ONCE, and clone it OUTSIDE the measurement window: the
    // decode is common to both shapes and is not what changed.
    let base = column_major(COLS, ROWS);
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

    // The move form must not clone cells: strictly fewer allocations, with the
    // saving ≥ one allocation per cell (the per-element clone removed).
    assert!(
        move_alloc < clone_alloc,
        "move transpose allocated {move_alloc}, clone {clone_alloc} — the per-cell \
         clone was not removed"
    );
    assert!(
        clone_alloc - move_alloc >= cells - COLS,
        "expected ~one allocation saved per cell ({cells}); got {}",
        clone_alloc - move_alloc
    );
}
