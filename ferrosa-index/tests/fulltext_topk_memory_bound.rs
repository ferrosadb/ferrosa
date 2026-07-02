//! TDD guard (t_ee98faa0 layer 2 — the REPLICA-side fulltext-search OOM): a
//! `LIMIT k` full-text search over the REAL FTI reader must hold O(k)
//! additional memory, INDEPENDENT of how many documents match the term.
//!
//! Live failure this pins: one broad
//! `SELECT … WHERE context_snippet = fts_match('memory') LIMIT 10 ALLOW FILTERING`
//! OOM-killed ALL THREE replicas of a 2 GiB-capped cluster simultaneously —
//! the coordinator-side accumulation was already fixed (f587808d), so the kill
//! moved inside each replica's `fulltext_search`. The reader's
//! `search()` scored EVERY posting of the matching term into an owned
//! `HashMap<Vec<u8>, f64>` and then cloned it again into a sorted
//! `Vec<FtsHit>` — peak O(matching docs) per sidecar per query.
//!
//! The 2 GiB node cap is a deliberate forcing function and is NEVER raised —
//! the fix is a query-derived bounded top-k (`search_top_k`), never a
//! server-side result cap.
//!
//! Modeled on `ferrosa-cluster/tests/replica_scan_serialization_memory_bound.rs`:
//! the index is built and deserialized OUTSIDE the measurement window; only
//! the search itself is measured. Hard budgets make a blow-up FAIL loudly
//! instead of OOM-ing the test process.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use ferrosa_index::fulltext::builder::FullTextIndexBuilder;
use ferrosa_index::fulltext::reader::FullTextIndexReader;

// --- peak-additional-heap tracker (scoped to this integration-test binary) ---
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
            LIVE.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

static MEASURE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    let _guard = MEASURE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let out = f();
    ARMED.store(false, Ordering::SeqCst);
    (out, PEAK.load(Ordering::SeqCst))
}

/// Build a reader over `n` docs that ALL contain the broad term "memory",
/// with realistic doc-key (~40 B, mirrors `encode_doc_key(pk, clustering)`)
/// and snippet sizes (~15 tokens).
fn reader_with_matching_docs(n: usize) -> FullTextIndexReader {
    let mut builder = FullTextIndexBuilder::new();
    for i in 0..n {
        let doc_key = format!("entity-{i:016}-row-{i:016}").into_bytes();
        let text = format!(
            "memory snippet {i} about durable typed agent knowledge graph \
             entity number {i} stored context"
        );
        builder.add_document(doc_key, &text);
    }
    FullTextIndexReader::from_index(builder.build())
}

const K: usize = 10;
/// Hard per-search budget for a LIMIT-k search: generous for O(k) hits +
/// allocator jitter, tiny next to the O(matching docs) blow-up (a 60k-doc
/// match set costs several MiB in the score-everything shape).
const TOPK_SEARCH_BUDGET_BYTES: i64 = 256 * 1024; // 256 KiB

/// RED→GREEN for t_ee98faa0 layer 2 (replica side): `search_top_k` with a
/// query-derived k=10 must hold a bounded working set, INDEPENDENT of the
/// matching-doc count. Before the fix (`search_top_k` = score-everything +
/// truncate) this FAILS: peak scales with N and blows the budget.
#[test]
fn limit_k_search_peak_is_bounded_independent_of_matching_docs() {
    const SMALL_N: usize = 4_000;
    const LARGE_N: usize = 64_000; // 16× more matching docs

    let small_reader = reader_with_matching_docs(SMALL_N);
    let large_reader = reader_with_matching_docs(LARGE_N);

    let (small_hits, small) = measure_peak(|| small_reader.search_top_k_str("memory", K).unwrap());
    let (large_hits, large) = measure_peak(|| large_reader.search_top_k_str("memory", K).unwrap());

    assert_eq!(small_hits.len(), K, "k hits must be returned (small)");
    assert_eq!(large_hits.len(), K, "k hits must be returned (large)");

    eprintln!(
        "fts search_top_k peak: small(N={SMALL_N})={small} B, large(N={LARGE_N})={large} B, \
         ratio={:.2}; budget={TOPK_SEARCH_BUDGET_BYTES} B",
        large as f64 / small.max(1) as f64,
    );

    // BOUNDED: a LIMIT-10 search must not materialize the match set.
    assert!(
        large < TOPK_SEARCH_BUDGET_BYTES,
        "REGRESSION (t_ee98faa0 layer 2): LIMIT-{K} fts search peak {large} B exceeds the \
         {TOPK_SEARCH_BUDGET_BYTES} B budget at N={LARGE_N} matching docs — the reader is \
         scoring/materializing every matching doc instead of a bounded top-k. \
         At the intentional 2 GiB node cap this is the replica OOM."
    );
    // INDEPENDENT OF N: 16× more matching docs must NOT cost ~16× more memory.
    assert!(
        large < small.max(4096) * 3,
        "REGRESSION (t_ee98faa0 layer 2): LIMIT-{K} fts search peak scales with the match set — \
         small(N={SMALL_N})={small} B, large(N={LARGE_N})={large} B ({}× more matching docs). \
         Peak must be O(k), not O(matching docs).",
        LARGE_N / SMALL_N,
    );
}

/// GREEN invariant: the bounded top-k search returns EXACTLY the k
/// best-scoring hits the unbounded search would have ranked first — same
/// matching semantics, same scores (exactness guard for the memory fix).
#[test]
fn limit_k_search_matches_unbounded_search_semantics() {
    let mut builder = FullTextIndexBuilder::new();
    for i in 0..500usize {
        // Vary term frequency so scores are distinct and ranking is meaningful.
        let reps = (i % 7) + 1;
        let text = format!("{} filler tail {i}", "memory ".repeat(reps));
        builder.add_document(format!("dk{i:05}").into_bytes(), &text);
    }
    let reader = FullTextIndexReader::from_index(builder.build());

    let full = reader.search_str("memory").unwrap();
    let topk = reader.search_top_k_str("memory", 25).unwrap();

    assert_eq!(topk.len(), 25);
    for (i, hit) in topk.iter().enumerate() {
        assert!(
            (hit.score - full[i].score).abs() < 1e-12,
            "top-k score at rank {i} must equal the unbounded search's: \
             {} vs {}",
            hit.score,
            full[i].score
        );
    }
    // k larger than the match set returns the complete set.
    let all = reader.search_top_k_str("memory", 100_000).unwrap();
    assert_eq!(all.len(), full.len(), "k > matches must return ALL matches");
}
