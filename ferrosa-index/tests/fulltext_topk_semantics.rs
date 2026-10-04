//! `search_top_k` must answer exactly what `search` answers, cut to `k`.
//!
//! The bounded top-k path (t_ee98faa0 layer 2) re-implements scoring for
//! `Term` and `MultiTerm`/`Phrase` instead of going through the score map
//! `search` uses. These tests pin the two together on inputs the memory-bound
//! test never builds: a document key indexed more than once, and ties.

use std::collections::HashMap;

use ferrosa_index::fulltext::builder::FullTextIndexBuilder;
use ferrosa_index::fulltext::reader::{FtsHit, FullTextIndexReader};

/// Every top-k hit names a distinct key with the score `search` gives that
/// key, and the top-k scores are the `k` best scores of `search`.
fn assert_top_k_agrees(reader: &FullTextIndexReader, query: &str, k: usize) {
    let full = reader.search_str(query).expect("query parses");
    let topk = reader.search_top_k_str(query, k).expect("query parses");
    let by_key: HashMap<&[u8], f64> = full
        .iter()
        .map(|h| (h.partition_key.as_slice(), h.score))
        .collect();
    assert_eq!(
        topk.len(),
        k.min(full.len()),
        "{query:?}: top-{k} returned {} hits; search matched {} keys",
        topk.len(),
        full.len()
    );
    let mut seen = std::collections::HashSet::new();
    for hit in &topk {
        assert!(
            seen.insert(hit.partition_key.clone()),
            "{query:?}: top-{k} returned key {:?} twice",
            String::from_utf8_lossy(&hit.partition_key)
        );
        let want = by_key.get(hit.partition_key.as_slice()).unwrap_or_else(|| {
            panic!(
                "{query:?}: top-{k} returned key {:?}, which search does not match",
                String::from_utf8_lossy(&hit.partition_key)
            )
        });
        assert!(
            (hit.score - want).abs() < 1e-9,
            "{query:?}: key {:?} scores {} in top-{k} but {want} in search",
            String::from_utf8_lossy(&hit.partition_key),
            hit.score
        );
    }
    let mut got: Vec<f64> = topk.iter().map(|h: &FtsHit| h.score).collect();
    got.sort_by(|a, b| b.total_cmp(a));
    for (rank, (g, w)) in got.iter().zip(full.iter().map(|h| h.score)).enumerate() {
        assert!(
            (g - w).abs() < 1e-9,
            "{query:?}: rank {rank} scores {g} in top-{k} but {w} in search"
        );
    }
}

/// A key indexed twice: `search` folds its postings into one hit (summing
/// them); the top-k path must not report the key twice or with another score.
#[test]
fn a_key_indexed_twice_scores_the_same_in_top_k_as_in_search() {
    let mut builder = FullTextIndexBuilder::new();
    builder.add_document(b"dup".to_vec(), "memory graph");
    builder.add_document(b"dup".to_vec(), "memory store");
    for i in 0..20 {
        builder.add_document(format!("k{i:02}").into_bytes(), "memory filler words here");
    }
    let reader = FullTextIndexReader::from_index(builder.build());
    for k in [1, 3, 25] {
        assert_top_k_agrees(&reader, "memory", k);
        assert_top_k_agrees(&reader, "memory graph", k);
        assert_top_k_agrees(&reader, "\"memory graph\"", k);
        assert_top_k_agrees(&reader, "memory OR graph", k);
    }
}

/// Deterministic pseudo-random corpora over a small vocabulary and key space,
/// so duplicate keys, ties and every query shape all occur.
#[test]
fn top_k_agrees_with_search_over_generated_corpora() {
    const VOCAB: [&str; 6] = ["memory", "graph", "store", "agent", "entity", "durable"];
    let mut state = 0x5EED_u64;
    let mut next = |bound: u64| {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) % bound
    };
    for _corpus in 0..40 {
        let mut builder = FullTextIndexBuilder::new();
        let docs = 1 + next(60);
        for _ in 0..docs {
            let key = format!("k{}", next(25)).into_bytes();
            let words: Vec<&str> = (0..1 + next(8))
                .map(|_| VOCAB[next(VOCAB.len() as u64) as usize])
                .collect();
            builder.add_document(key, &words.join(" "));
        }
        let reader = FullTextIndexReader::from_index(builder.build());
        for query in [
            "memory",
            "graph",
            "memory graph",
            "\"memory graph\"",
            "memory OR store",
            "agent AND entity",
        ] {
            for k in [1, 2, 5, 100] {
                assert_top_k_agrees(&reader, query, k);
            }
        }
    }
}
