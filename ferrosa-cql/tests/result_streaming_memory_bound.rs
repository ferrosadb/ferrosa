//! Router-level memory bound for CQL results that cannot be produced in scan
//! order (`ORDER BY`, `DISTINCT`) and for full `ALLOW FILTERING` scans.
//!
//! `ferrosa-storage/tests/order_by_spill_memory_bound.rs` drives the external
//! sorter alone and discards every row, so it proves the SORT is bounded. It
//! cannot see what the router does with the sorted output. Until the result
//! cursor landed, the router drained the merge into one `Vec` and sliced pages
//! out of it, so `SELECT * FROM big ORDER BY v` held the whole table on the
//! heap, and every page re-ran the scan and the sort.
//!
//! Each test here walks EVERY page of a query through `route_select_raw`,
//! discards each page after checking it, and measures peak live heap across
//! the whole walk. It runs the walk over two table sizes, 4x apart. A bounded
//! read costs about the same at both sizes; a read that collects the result
//! costs about 4x. Each walk also checks that every expected row arrives
//! exactly once and, for `ORDER BY`, in order, against a reference computed
//! from the seeded data.
//!
//! The data stays in the memtable (a large flush threshold) so the storage
//! scan's own footprint does not grow with the table: with the default 4 KiB
//! fixture threshold, a few thousand padded rows become hundreds of SSTables
//! and the scan's per-source buffers would dominate the measurement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cql::ast::Statement;
use ferrosa_cql::router::{route, route_select_raw, RequestContext, SharedState};
use ferrosa_cql::types::CqlValue;
use ferrosa_schema::auth::role::AuthContext;

// --- peak-allocation tracker (this integration-test binary only) ---
struct TrackingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && ARMED.load(Ordering::Relaxed) {
            let live =
                LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed) + layout.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            // Clamp at zero: memory allocated before the window and freed
            // inside it must not drive LIVE negative and mask later growth.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let out = f();
    ARMED.store(false, Ordering::SeqCst);
    (out, PEAK.load(Ordering::SeqCst))
}

const PAD_BYTES: usize = 512;
const PAGE_SIZE: i32 = 100;
const SMALL_N: usize = 2_000;
const LARGE_N: usize = 8_000;
/// Spill threshold of the external sort: far below either table's size, and
/// small enough that BOTH tables spill more runs than the merge fan-in (64).
/// The merge holds one reader buffer per run up to the fan-in, so with fewer
/// runs the small table's peak would sit below that plateau and the ratio
/// would measure the plateau, not the result.
const SPILL_THRESHOLD_BYTES: &str = "16384";
/// DISTINCT keeps this many row keys resident before spilling the rest.
const DISTINCT_RESIDENT_KEYS: &str = "256";

fn superuser() -> AuthContext {
    AuthContext {
        role: "cassandra".into(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn ctx<'a>(
    auth: &'a AuthContext,
    ks: &'a Option<String>,
    page_size: Option<i32>,
    paging_state: Option<Vec<u8>>,
) -> RequestContext<'a> {
    RequestContext {
        auth,
        current_keyspace: ks,
        consistency: ConsistencyLevel::One,
        serial_consistency: None,
        paging: ferrosa_cql::paging::PagingParams {
            page_size,
            paging_state,
        },
        client_address: "127.0.0.1:40000".into(),
        protocol_version: 4,
    }
}

/// `v` is a permutation of `0..n`, so `ORDER BY v` is unrelated to token order.
fn v_of(id: usize, n: usize) -> i32 {
    ((id * 7_919) % n) as i32
}

/// Deterministic padding derived from `v / 2`, so rows with equal `v / 2`
/// carry identical padding and `SELECT DISTINCT w, pad` has n/2 rows.
fn pad_hex(seed: usize) -> String {
    let byte = (seed % 251) as u8;
    let mut s = String::with_capacity(2 + PAD_BYTES * 2);
    s.push_str("0x");
    for _ in 0..PAD_BYTES {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

async fn seed(n: usize) -> (Arc<SharedState>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let state =
        ferrosa_cql::test_util::standalone_for_test_with_flush_threshold(dir.path(), 1 << 30);
    let auth = superuser();
    let ks = Some("mb".to_string());
    let c = ctx(&auth, &ks, None, None);
    for cql in [
        "CREATE KEYSPACE mb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            .to_string(),
        "CREATE TABLE mb.t (id int PRIMARY KEY, v int, w int, g int, pad blob)".to_string(),
    ] {
        route(&state, &c, ferrosa_cql::parser::parse(&cql).unwrap())
            .await
            .unwrap_or_else(|e| panic!("{cql}: {e}"));
    }
    for id in 0..n {
        let v = v_of(id, n);
        let w = v / 2;
        let cql = format!(
            "INSERT INTO mb.t (id, v, w, g, pad) VALUES ({id}, {v}, {w}, {}, {})",
            id % 3,
            pad_hex(w as usize)
        );
        route(&state, &c, ferrosa_cql::parser::parse(&cql).unwrap())
            .await
            .unwrap_or_else(|e| panic!("{cql}: {e}"));
    }
    (state, dir)
}

fn select(cql: &str) -> ferrosa_cql::ast::SelectStatement {
    match ferrosa_cql::parser::parse(cql).unwrap() {
        Statement::Select(s) => s,
        other => panic!("expected select, got {other:?}"),
    }
}

fn int_at(row: &[Option<CqlValue>], idx: usize) -> i32 {
    match &row[idx] {
        Some(CqlValue::Int(x)) => *x,
        other => panic!("expected int at {idx}, got {other:?}"),
    }
}

/// What a walk must deliver: `expect_keys[k]` is true for every key that must
/// arrive exactly once; `ordered` additionally requires non-decreasing keys.
struct Expect {
    key_col: &'static str,
    expect_keys: Vec<bool>,
    ordered: bool,
}

/// Walk every page and check it against `expect`, keeping only a bitset.
/// Returns `(rows_seen, pages)`.
async fn walk_pages(
    state: &SharedState,
    cql: &str,
    expect: &Expect,
    seen: &mut [bool],
) -> (usize, usize) {
    let auth = superuser();
    let ks = Some("mb".to_string());
    let stmt = select(cql);
    let mut paging_state: Option<Vec<u8>> = None;
    let mut rows_seen = 0usize;
    let mut pages = 0usize;
    let mut prev_key: Option<i32> = None;
    for _ in 0..100_000 {
        let c = ctx(&auth, &ks, Some(PAGE_SIZE), paging_state.take());
        let page = route_select_raw(state, &c, &stmt)
            .await
            .unwrap_or_else(|e| panic!("{cql}: page {pages} failed: {e}"));
        assert!(
            page.rows.len() <= PAGE_SIZE as usize,
            "{cql}: page {pages} has {} rows, over the page size {PAGE_SIZE}",
            page.rows.len()
        );
        let key_idx = page
            .column_names
            .iter()
            .position(|c| c == expect.key_col)
            .unwrap_or_else(|| panic!("{cql}: no column {}", expect.key_col));
        for row in &page.rows {
            let key = int_at(row, key_idx);
            let slot = usize::try_from(key).expect("non-negative key");
            assert!(
                expect.expect_keys.get(slot).copied().unwrap_or(false),
                "{cql}: unexpected key {key}"
            );
            assert!(!seen[slot], "{cql}: key {key} delivered twice");
            seen[slot] = true;
            if expect.ordered {
                if let Some(p) = prev_key {
                    assert!(p <= key, "{cql}: out of order: {p} then {key}");
                }
                prev_key = Some(key);
            }
        }
        rows_seen += page.rows.len();
        pages += 1;
        match page.paging_state {
            Some(next) => paging_state = Some(next),
            None => return (rows_seen, pages),
        }
    }
    panic!("{cql}: paging did not terminate");
}

/// Seed a table of `n` rows, then measure the peak heap of walking every page
/// of `cql`. Returns the peak in bytes.
fn peak_for(n: usize, cql: &str, expect_for: fn(usize) -> Expect) -> i64 {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (state, _dir) = rt.block_on(seed(n));
    let expect = expect_for(n);
    let want = expect.expect_keys.iter().filter(|k| **k).count();
    // Allocated before arming, so the bitset is not part of the measurement.
    let mut seen = vec![false; expect.expect_keys.len()];
    let ((rows_seen, pages), peak) =
        measure_peak(|| rt.block_on(walk_pages(&state, cql, &expect, &mut seen)));
    assert_eq!(
        rows_seen, want,
        "{cql}: n={n}: expected {want} rows exactly once, saw {rows_seen}"
    );
    eprintln!("{cql}: n={n}: {rows_seen} rows over {pages} pages, peak heap {peak} B");
    drop(state);
    peak
}

fn assert_bounded(cql: &str, expect_for: fn(usize) -> Expect) {
    std::env::set_var(
        ferrosa_storage::spill_budget::ENV_SPILL_THRESHOLD_BYTES,
        SPILL_THRESHOLD_BYTES,
    );
    std::env::set_var("FERROSA_CQL_DISTINCT_RESIDENT_KEYS", DISTINCT_RESIDENT_KEYS);
    let small = peak_for(SMALL_N, cql, expect_for);
    let large = peak_for(LARGE_N, cql, expect_for);
    std::env::remove_var(ferrosa_storage::spill_budget::ENV_SPILL_THRESHOLD_BYTES);
    std::env::remove_var("FERROSA_CQL_DISTINCT_RESIDENT_KEYS");
    eprintln!(
        "{cql}: peak small(n={SMALL_N})={small} B, large(n={LARGE_N})={large} B, ratio {:.2}",
        large as f64 / small.max(1) as f64
    );
    // 4x the rows must not cost anywhere near 4x the heap. A read that
    // collects the result scales with it; a bounded one does not.
    assert!(
        large < small * 2,
        "REGRESSION: `{cql}` peak heap scales with the table: {SMALL_N} rows -> {small} B, \
         {LARGE_N} rows -> {large} B. The router is collecting the result instead of \
         streaming it page by page."
    );
}

#[test]
#[serial_test::serial]
fn order_by_without_limit_streams_pages_with_bounded_heap() {
    assert_bounded("SELECT * FROM mb.t ORDER BY v", |n| Expect {
        key_col: "v",
        expect_keys: vec![true; n],
        ordered: true,
    });
}

#[test]
#[serial_test::serial]
fn distinct_streams_pages_with_bounded_heap() {
    assert_bounded("SELECT DISTINCT w, pad FROM mb.t", |n| Expect {
        key_col: "w",
        expect_keys: vec![true; n / 2],
        ordered: false,
    });
}

#[test]
#[serial_test::serial]
fn allow_filtering_full_scan_streams_pages_with_bounded_heap() {
    assert_bounded("SELECT * FROM mb.t WHERE g = 1 ALLOW FILTERING", |n| {
        Expect {
            key_col: "id",
            expect_keys: (0..n).map(|id| id % 3 == 1).collect(),
            ordered: false,
        }
    });
}

/// A client that reads one page of an `ORDER BY` and then goes quiet holds
/// a parked cursor (files on disk) and NOTHING else: no scan-pool slot, no
/// producer thread. The scan finished inside the first request.
#[test]
#[serial_test::serial]
fn a_stalled_client_holds_no_scan_slot() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (state, _dir) = rt.block_on(seed(SMALL_N));
    let auth = superuser();
    let ks = Some("mb".to_string());
    let stmt = select("SELECT * FROM mb.t ORDER BY v");
    let page = rt
        .block_on(route_select_raw(
            &state,
            &ctx(&auth, &ks, Some(PAGE_SIZE), None),
            &stmt,
        ))
        .unwrap();
    assert!(page.paging_state.is_some(), "more pages remain");
    assert_eq!(state.result_cursors.stats().parked, 1);

    // The client now stalls. Producer threads exit once their stream is
    // dropped; give them a bounded moment, then require an idle pool.
    let pool = ferrosa_sched::global_pool();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while pool.active() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "a parked cursor still holds {} scan-pool slot(s)",
            pool.active()
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(state.result_cursors.stats().parked, 1);
}
