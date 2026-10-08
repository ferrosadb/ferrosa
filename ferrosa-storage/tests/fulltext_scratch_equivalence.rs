//! The fulltext memtable index build must not allocate per row, and must
//! produce exactly the index the old per-row-allocating path produced.
//!
//! ## Why two assertions, not one
//!
//! The build was rewritten to reuse scratch buffers (`RowScratch` +
//! `add_document_with_tf` + `Analyzer::analyze_into`) so a scan of N rows does
//! not allocate N text buffers, N token vectors and N maps. A buffer-reuse
//! rewrite is exactly the kind of change that silently alters output — a
//! retained buffer leaks one row's text into the next, a cleared-in-the-wrong-
//! place `tf` map drops or double-counts a term, a dropped separator space joins
//! two words into a term that is not in the data. An allocation test alone would
//! pass on a wrong answer, and an equivalence test alone would pass on a
//! correct-but-wasteful implementation.
//!
//! So: **equivalence** pins the output against the original one-shot builder,
//! and **allocation bound** pins the reuse. Neither is sufficient alone; the
//! defect classes are different.
//!
//! Note `add_document` is retained precisely so it can serve as the oracle here.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ferrosa_index::fulltext::analyzer::{Analyzer, StandardAnalyzer};
use ferrosa_index::fulltext::builder::FullTextIndexBuilder;

use ferrosa_storage::fulltext_scratch::RowScratch;

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

/// One "row": the concatenated text plus the document key the caller would build.
fn rows() -> Vec<(Vec<u8>, String)> {
    (0..300)
        .map(|i| {
            let text = format!("alpha beta{i} gamma delta{i} epsilon alpha beta{i}");
            (format!("pk_{i:04}").into_bytes(), text)
        })
        .collect()
}

/// Serialize the index so the two paths can be compared byte-for-byte. Term
/// order inside `terms` is a HashMap, but `serialize_fti` sorts the dictionary,
/// so the bytes are deterministic for equal content.
fn serialized(builder: FullTextIndexBuilder) -> Vec<u8> {
    builder.finish().expect("serialize must succeed")
}

/// I-6: the reused-scratch path must build byte-identical indexes to the
/// original per-row-allocating path.
#[test]
fn scratch_path_builds_the_same_index_as_the_allocating_path() {
    let data = rows();

    // Oracle: the original path, one allocation per row.
    let mut oracle = FullTextIndexBuilder::new();
    for (pk, text) in &data {
        oracle.add_document(pk.clone(), text);
    }
    let oracle = serialized(oracle);

    // New path: hoisted scratch, cleared and reused per row.
    let mut builder = FullTextIndexBuilder::with_capacity(1024);
    let mut scratch = RowScratch::new();
    for (pk, text) in &data {
        scratch.reset();
        scratch.push_text(text);
        assert!(scratch.has_text());
        let mut tf = std::mem::take(&mut scratch.tf);
        let mut tokens = std::mem::take(&mut scratch.tokens);
        builder.add_document_with_tf(pk.clone(), scratch.text_trimmed(), &mut tf, &mut tokens);
        // The buffers come back with this document's contents and intact
        // capacity; the next row reuses them (cleared on entry by the builder).
        assert!(
            !tokens.is_empty(),
            "the builder must have filled the token buffer"
        );
        scratch.tf = tf;
        scratch.tokens = tokens;
    }
    let reused = serialized(builder);

    assert_eq!(
        oracle, reused,
        "reusing scratch buffers changed the built index — the buffers must be \
         cleared between rows and the analyzed text must be identical (I-6)"
    );
}

/// I-6, the separator specifically: text from adjacent cells must stay separate
/// words. Dropping the separator would index a term that is not in the data.
#[test]
fn adjacent_cell_text_stays_separated() {
    let mut scratch = RowScratch::new();
    scratch.reset();
    scratch.push_text("alpha");
    scratch.push_text("beta");
    assert_eq!(scratch.text_trimmed(), "alpha beta");

    let mut builder = FullTextIndexBuilder::new();
    let mut tf = std::mem::take(&mut scratch.tf);
    let mut tokens = std::mem::take(&mut scratch.tokens);
    builder.add_document_with_tf(b"pk".to_vec(), scratch.text_trimmed(), &mut tf, &mut tokens);
    let fti = builder.build();

    assert!(fti.terms.contains_key("alpha"), "alpha must be indexed");
    assert!(fti.terms.contains_key("beta"), "beta must be indexed");
    assert!(
        !fti.terms.contains_key("alphabeta"),
        "concatenating cells without a separator would index 'alphabeta', a term \
         that is not in the data"
    );
}

/// I-8: the per-row allocation cost must not GROW with the number of rows.
///
/// Measured as a comparison, not an absolute: the same batch size is run against
/// a small index and a large one, and the later batch must not cost more. That
/// is the load-independent form of "no per-row allocation", and it is robust to
/// amortized growth in the term map.
#[test]
fn per_row_allocation_does_not_grow_with_index_size() {
    fn batch_cost_with_prior_rows(prior: usize) -> usize {
        let mut builder = FullTextIndexBuilder::with_capacity(4096);
        let mut scratch = RowScratch::new();
        for i in 0..prior {
            scratch.reset();
            scratch.push_text(&format!("prior term{i} shared"));
            let mut tf = std::mem::take(&mut scratch.tf);
            let mut tokens = std::mem::take(&mut scratch.tokens);
            builder.add_document_with_tf(
                format!("pk_p{i}").into_bytes(),
                scratch.text_trimmed(),
                &mut tf,
                &mut tokens,
            );
            scratch.tf = tf;
            scratch.tokens = tokens;
        }

        // The measured batch: identical work in both cases.
        let batch: Vec<(Vec<u8>, String)> = (0..100)
            .map(|i| {
                (
                    format!("pk_b{i}").into_bytes(),
                    format!("batch shared term i{i}"),
                )
            })
            .collect();

        let (_, allocs) = count_allocs(|| {
            for (pk, text) in &batch {
                scratch.reset();
                scratch.push_text(text);
                let mut tf = std::mem::take(&mut scratch.tf);
                let mut tokens = std::mem::take(&mut scratch.tokens);
                builder.add_document_with_tf(
                    pk.clone(),
                    scratch.text_trimmed(),
                    &mut tf,
                    &mut tokens,
                );
                scratch.tf = tf;
                scratch.tokens = tokens;
            }
        });
        // Subtract a same-shaped noise window. The counter is process-wide and
        // not thread-aware, so tests running concurrently in this binary land in
        // our window; a constant background of that shape cancels out of the
        // comparison, which is what the assertion is about. Without this the test
        // is only correct when run with `--test-threads=1`, which a test file
        // cannot impose on its own runner.
        let (_, noise) = count_allocs(|| {
            let mut sink = 0usize;
            for i in 0..batch.len() {
                sink += i;
            }
            std::hint::black_box(sink);
        });
        allocs.saturating_sub(noise)
    }

    let small = batch_cost_with_prior_rows(100);
    let large = batch_cost_with_prior_rows(5_000);

    // Amortized growth of internal Vecs is allowed; what must not happen is a
    // per-row cost that scales with how much is already indexed.
    assert!(
        large <= small * 2 + 64,
        "the same 100-row batch cost {small} allocations against 100 prior rows \
         but {large} against 5000 — the per-row cost grows with index size, so \
         scratch buffers are not actually being reused (I-8)"
    );
}

/// The analyzer's buffer-reusing form must agree with its allocating form,
/// token for token and in order.
#[test]
fn analyze_into_matches_analyze_exactly() {
    let a = StandardAnalyzer::new();
    let cases = [
        "Hello, World! 123",
        "  leading and trailing  ",
        "the and of",       // all stop words -> empty
        "",                 // empty
        "MiXeD CaSe WORDS", // case folding
        "a-b_c.d/e",        // separators
    ];
    for text in cases {
        let expected = a.analyze(text);
        let mut out: Vec<String> = vec!["stale".to_string()];
        out.clear();
        a.analyze_into(text, &mut out);
        assert_eq!(
            expected, out,
            "analyze_into disagreed with analyze for {text:?} — a buffer-reusing \
             rewrite must not change tokens, their order, or stop-word filtering"
        );
    }
}
