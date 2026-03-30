//! FTI sidecar merge for compaction (FT-013).
//!
//! When two SSTables are merged during compaction their FTI sidecar files must
//! also be merged into a single new FTI.
//!
//! ## Merge semantics
//!
//! - **Union of terms**: any term present in either index is present in the output.
//! - **Posting deduplication**: if the same partition key appears in both
//!   indexes for the same term, the posting is deduplicated by summing term
//!   frequencies and taking the first document length (the newer SSTable wins
//!   for document length — caller should pass the newer SSTable as `b`).
//! - **Doc counts**: the merged index counts unique partition keys.
//! - **total_doc_len**: recomputed from the merged postings.

use std::collections::HashMap;

use super::builder::{serialize_fti, FullTextIndex, TermEntry, Posting};
use super::reader::deserialize_fti;

/// Merge two FTI byte buffers into one.
///
/// Union of terms, merged postings lists (deduplicated by partition key),
/// recomputed doc counts and corpus stats.
///
/// # Arguments
///
/// * `a` — first FTI (e.g., older SSTable sidecar).
/// * `b` — second FTI (e.g., newer SSTable sidecar).
///
/// # Errors
///
/// Returns `Err(String)` if either buffer cannot be deserialized.
pub fn merge_fti(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    let fti_a = deserialize_fti(a)?;
    let fti_b = deserialize_fti(b)?;

    let merged = merge_indexes(fti_a, fti_b);
    serialize_fti(&merged)
}

/// Merge two deserialized [`FullTextIndex`] values.
fn merge_indexes(a: FullTextIndex, b: FullTextIndex) -> FullTextIndex {
    // Collect all terms from both indexes.
    let mut merged_terms: HashMap<String, HashMap<Vec<u8>, (u32, u32)>> = HashMap::new();

    // Ingest index `a`.
    for (term, entry) in a.terms {
        let pk_map = merged_terms.entry(term).or_default();
        for posting in entry.postings {
            pk_map
                .entry(posting.partition_key)
                .and_modify(|(tf, _dl)| *tf += posting.term_freq)
                .or_insert((posting.term_freq, posting.doc_len));
        }
    }

    // Ingest index `b` — for overlapping partition keys, sum TFs and use `b`'s dl.
    for (term, entry) in b.terms {
        let pk_map = merged_terms.entry(term).or_default();
        for posting in entry.postings {
            pk_map
                .entry(posting.partition_key)
                .and_modify(|(tf, dl)| {
                    *tf += posting.term_freq;
                    // Newer (b) document length wins for length normalization.
                    *dl = posting.doc_len;
                })
                .or_insert((posting.term_freq, posting.doc_len));
        }
    }

    // Build the merged TermEntry map.
    let terms: HashMap<String, TermEntry> = merged_terms
        .into_iter()
        .map(|(term, pk_map)| {
            let doc_freq = pk_map.len() as u32;
            let postings = pk_map
                .into_iter()
                .map(|(pk, (tf, dl))| Posting {
                    partition_key: pk,
                    term_freq: tf,
                    doc_len: dl,
                })
                .collect::<Vec<_>>();
            (term, TermEntry { doc_freq, postings })
        })
        .collect();

    // Recompute doc_count (unique partition keys across any term).
    let all_pks: std::collections::HashSet<&Vec<u8>> = terms
        .values()
        .flat_map(|e| e.postings.iter().map(|p| &p.partition_key))
        .collect();
    let doc_count = all_pks.len() as u32;

    // Recompute total_doc_len from the first term occurrence of each doc.
    // We track the doc length per unique pk (using last seen value).
    let mut pk_dl: HashMap<&Vec<u8>, u32> = HashMap::new();
    for entry in terms.values() {
        for posting in &entry.postings {
            pk_dl
                .entry(&posting.partition_key)
                .and_modify(|dl| *dl = posting.doc_len)
                .or_insert(posting.doc_len);
        }
    }
    let total_doc_len: u64 = pk_dl.values().map(|&dl| dl as u64).sum();

    FullTextIndex {
        doc_count,
        total_doc_len,
        terms,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fulltext::builder::FullTextIndexBuilder;
    use crate::fulltext::reader::FullTextIndexReader;

    fn build_fti(docs: &[(&[u8], &str)]) -> Vec<u8> {
        let mut builder = FullTextIndexBuilder::new();
        for (pk, text) in docs {
            builder.add_document(pk.to_vec(), text);
        }
        builder.finish().unwrap()
    }

    #[test]
    fn fts_compaction_merge_deduplicates() {
        // Both FTIs index pk1 under the term "rust".
        // After merge, there must be exactly one posting for pk1.
        let fti_a = build_fti(&[(b"pk1", "rust programming")]);
        let fti_b = build_fti(&[(b"pk1", "rust systems language"), (b"pk2", "rust cargo")]);

        let merged = merge_fti(&fti_a, &fti_b).unwrap();
        let reader = FullTextIndexReader::open(merged).unwrap();

        let hits = reader.lookup("rust");
        // Exactly one posting for pk1 (deduplicated).
        let pk1_hits: Vec<_> = hits
            .iter()
            .filter(|p| p.partition_key == b"pk1".to_vec())
            .collect();
        assert_eq!(
            pk1_hits.len(),
            1,
            "pk1 must appear exactly once in merged postings"
        );
        // pk2 must also be present.
        let pk2_hits: Vec<_> = hits
            .iter()
            .filter(|p| p.partition_key == b"pk2".to_vec())
            .collect();
        assert_eq!(pk2_hits.len(), 1, "pk2 must be preserved after merge");
    }

    #[test]
    fn fts_compaction_merge_preserves_scores() {
        // pk1 has tf=1 in fti_a and tf=2 in fti_b for term "rust".
        // Merged tf should be 1+2 = 3.
        let fti_a = build_fti(&[(b"pk1", "rust language")]);
        let fti_b = build_fti(&[(b"pk1", "rust rust systems")]);

        let merged = merge_fti(&fti_a, &fti_b).unwrap();
        let reader = FullTextIndexReader::open(merged).unwrap();

        let hits = reader.lookup("rust");
        let pk1: Vec<_> = hits
            .iter()
            .filter(|p| p.partition_key == b"pk1".to_vec())
            .collect();
        assert_eq!(pk1.len(), 1);
        assert_eq!(
            pk1[0].term_freq, 3,
            "merged tf must be sum of both contributions (1+2=3)"
        );
    }

    #[test]
    fn merge_disjoint_indexes() {
        // No overlapping partition keys — both should survive in the merged index.
        let fti_a = build_fti(&[(b"pk1", "rust systems")]);
        let fti_b = build_fti(&[(b"pk2", "go concurrency")]);

        let merged = merge_fti(&fti_a, &fti_b).unwrap();
        let reader = FullTextIndexReader::open(merged).unwrap();

        assert_eq!(reader.doc_count(), 2);
        assert!(!reader.lookup("rust").is_empty(), "term 'rust' must survive merge");
        assert!(!reader.lookup("concurrency").is_empty(), "term 'concurrency' must survive merge");
    }

    #[test]
    fn merge_empty_a_returns_b() {
        let fti_a = build_fti(&[]);
        let fti_b = build_fti(&[(b"pk1", "hello world")]);

        let merged = merge_fti(&fti_a, &fti_b).unwrap();
        let reader = FullTextIndexReader::open(merged).unwrap();
        assert!(!reader.lookup("hello").is_empty());
    }

    #[test]
    fn merge_empty_b_returns_a() {
        let fti_a = build_fti(&[(b"pk1", "hello world")]);
        let fti_b = build_fti(&[]);

        let merged = merge_fti(&fti_a, &fti_b).unwrap();
        let reader = FullTextIndexReader::open(merged).unwrap();
        assert!(!reader.lookup("hello").is_empty());
    }
}
