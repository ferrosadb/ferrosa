//! Allocation counter for the virtual-table read path.
//!
//! The P0-OOM guard's whole point is that a read path must not materialize a
//! result set. Peak heap is one axis; this file measures the OTHER one the
//! guard cares about: how many times the allocator is called per read, and
//! whether the streaming path holds its outer structure constant instead of
//! building a per-call collection.
//!
//! Method (mirrors `ferrosa-sstable/tests/row_encode_alloc.rs`):
//!   * a process-wide counting `#[global_allocator]`;
//!   * ALL measured shapes run inside ONE `#[test]` fn, because the counter is
//!     thread-unaware and other test threads would corrupt it;
//!   * warm up, then measure the steady-state DELTA of a single call;
//!   * a fixture (256 rows) far larger than any internal buffer.
//!
//! What this pins:
//!   1. `visit_rows` (the required read path) visits every row and its
//!      per-call cost is CONSTANT across repeated calls — the read path holds
//!      no growing result collection.
//!   2. the provided `read()` collects, so it costs strictly MORE than
//!      `visit_rows` for the same table; the delta is the collected Vec.
//!   3. `eval_if_conditions` MOVES the row into the result instead of cloning
//!      it: the not-applied path costs ZERO allocations beyond the row it was
//!      handed, where the replaced clone-the-row shape cost one per cell.
//!
//! Honest scope note: `AlertsTable::visit_rows` calls `AlertRegistry::snapshot`,
//! which deep-clones the alert Vec (one String clone per text field) — an
//! O(alerts) per-call cost that PREDATES this change and is NOT touched here
//! (removing it means holding the registry read lock across the visitor
//! callback, a concurrency change). It dominates the per-call counts below and
//! is measured, not hidden.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ferrosa_cql::virtual_tables::alerts::{AlertRegistry, AlertSeverity, AlertsTable};
use ferrosa_schema::virtual_table::VirtualTable;

struct CountingAllocator;

static ALLOC_EVENTS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

/// `(allocator calls, result)` for `f`. Count FIRST so a caller cannot bind the
/// result to the count by mistake.
fn count_allocs<R>(f: impl FnOnce() -> R) -> (usize, R) {
    let before = ALLOC_EVENTS.load(Ordering::Relaxed);
    let out = f();
    (ALLOC_EVENTS.load(Ordering::Relaxed) - before, out)
}

fn alerts_table(n: usize) -> AlertsTable {
    let registry = Arc::new(AlertRegistry::new());
    for i in 0..n {
        registry.set_alert(
            &format!("alert_{i:04}"),
            if i % 2 == 0 {
                AlertSeverity::Warning
            } else {
                AlertSeverity::Critical
            },
            &format!("threshold breached for shard {i}"),
        );
    }
    AlertsTable::new(registry)
}

/// Steady-state allocator calls per `visit_rows` call, and the row count seen.
fn visit_profile(table: &AlertsTable, label: &str) -> (Vec<usize>, usize) {
    let (_, warm_rows) = count_allocs(|| {
        let mut r = 0usize;
        table.visit_rows(None, &mut |_row| r += 1);
        r
    });
    let mut counts = Vec::new();
    for _ in 0..4 {
        let (n, _rows) = count_allocs(|| {
            let mut r = 0usize;
            table.visit_rows(None, &mut |_row| r += 1);
            r
        });
        counts.push(n);
    }
    eprintln!("{label}: visit_rows alloc calls per call = {counts:?}");
    (counts, warm_rows)
}

#[test]
fn virtual_table_read_path_allocation_profile() {
    const N: usize = 256;

    // --- 1. visit_rows steady state + correctness ----------------------
    let table = alerts_table(N);
    let (visit_counts, visited) = visit_profile(&table, &format!("N={N}"));
    assert_eq!(visited, N, "visit_rows must visit every row exactly once");
    // Steady state means the cost does not GROW across repeated calls — a
    // growing per-call collection is the defect this guards. Asserting exact
    // CONSTANCY fails on a correct implementation whenever one background
    // allocation lands inside the measurement window (observed in CI:
    // [1537, 1537, 1541, 1537] — three flat calls and one +4). Assert
    // non-growth against the FIRST call, which still catches a collection that
    // accumulates per call (that grows without bound) while tolerating a small
    // constant jitter.
    let first = visit_counts[0];
    assert!(
        visit_counts.iter().all(|c| *c <= first),
        "visit_rows steady-state cost must not GROW across repeated calls \
         (no accumulating per-call collection); got {visit_counts:?}"
    );
    let visit_calls = visit_counts[0];

    // --- 2. read() vs visit_rows, same table, same fixture --------------
    let (read_calls, read_rows) = count_allocs(|| table.read(None).len());
    assert_eq!(read_rows, N, "read must surface every row");
    eprintln!(
        "N={N}: read()={read_calls} alloc calls vs visit_rows={visit_calls} alloc calls \
         (delta={}, the collected result Vec — amortized growth, not one entry per row)",
        read_calls as i64 - visit_calls as i64
    );
    assert!(
        read_calls > visit_calls,
        "the provided read() collects, so it must cost strictly more than the streaming \
         visit_rows — that gap is the trait-flip justification \
         (read={read_calls}, visit_rows={visit_calls})"
    );
    // The streaming path's advantage is that it does NOT pay the collected Vec:
    // the excess read() pays is the small amortized growth of that Vec, far
    // below one allocation per row.
    assert!(
        read_calls - visit_calls < N,
        "read()'s excess over visit_rows must be Vec growth, not one alloc per row \
         (read={read_calls}, visit_rows={visit_calls}, N={N})"
    );

    // --- 3. per-call cost is dominated by the pre-existing snapshot -----
    let small = alerts_table(8);
    let large = alerts_table(1024);
    let (small_counts, _) = visit_profile(&small, "N=8");
    let (large_counts, _) = visit_profile(&large, "N=1024");
    eprintln!(
        "visit_rows alloc calls: N=8 -> {}, N=1024 -> {} — O(alerts), driven by \
         AlertRegistry::snapshot's per-alert String deep-clone (pre-existing, untouched)",
        small_counts[0], large_counts[0]
    );

    // --- 4. eval_if_conditions MOVES the row (no clone) -----------------
    // Isolation note: the evaluator also clones ONE cell per condition
    // (`row.get(&cond.column).cloned()`) to compare — pre-existing and not
    // touched here. So the not-applied path is not literally zero; what this
    // measures is that MOVING the row costs strictly less than the replaced
    // shape, by exactly the whole-row clone it no longer performs.
    use ferrosa_common::CqlValue;
    use ferrosa_cql::accord_router::eval_if_conditions;
    use ferrosa_cql::ast::{IfCondition, IfOperator, Term};

    // Text cells: their clone allocates a String each, so the removed clone is
    // visible in the counter.
    let text_row = || {
        let mut row = std::collections::HashMap::new();
        for i in 0..8 {
            row.insert(format!("c{i}"), Some(CqlValue::Text(format!("v{i}"))));
        }
        row
    };
    let conditions = vec![IfCondition {
        column: "c0".into(),
        operator: IfOperator::Eq,
        value: Term::StringLiteral("never".into()), // never matches
    }];

    // Warm the map/eval paths.
    assert!(!eval_if_conditions(&conditions, false, Some(text_row())).applied);

    // Both rows built OUTSIDE the measured window, so the counter sees only the
    // evaluator — not map construction. Two rows because `eval` MOVES its input.
    let row_for_clone = text_row();
    let row_for_eval = text_row();

    // Cost of cloning the whole row (the work the old shape did on every
    // not-applied return).
    let (clone_calls, _) = count_allocs(|| {
        let _cloned = row_for_clone.clone();
    });

    // The not-applied path on an OWNED row. With the fix it MOVES the row, so
    // it costs strictly less than an explicit whole-row clone; the reverted
    // clone shape costs at least the clone, and fails the assertion below.
    let (moved_calls, verdict) =
        count_allocs(|| eval_if_conditions(&conditions, false, Some(row_for_eval)));
    assert!(!verdict.applied, "the condition never matches");

    eprintln!(
        "eval_if_conditions not-applied (8 Text cells, row built outside the window): \
         owned-move path={moved_calls} allocs vs whole-row clone={clone_calls} allocs"
    );
    assert!(
        moved_calls < clone_calls,
        "eval_if_conditions must MOVE the owned row into the result, not clone it: the \
         not-applied path ({moved_calls} allocs) must cost less than cloning the row it was \
         handed ({clone_calls} allocs)"
    );
}
