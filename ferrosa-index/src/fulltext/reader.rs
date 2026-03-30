//! Full-text index (FTI) reader.
//!
//! Deserializes an FTI byte buffer (produced by [`super::builder::serialize_fti`])
//! into a queryable in-memory structure, then executes full-text queries against
//! it using BM25 scoring.

use std::collections::HashMap;

use super::builder::{FullTextIndex, Posting, TermEntry, FTI_MAGIC, FTI_VERSION};
use super::query::{parse_fts_query, FtsQuery};
use super::scoring::{bm25_score, Bm25Params};

/// A scored search result from a full-text query.
#[derive(Debug, Clone)]
pub struct FtsHit {
    /// Partition key bytes of the matching document.
    pub partition_key: Vec<u8>,
    /// BM25 relevance score.
    pub score: f64,
}

/// Wraps a deserialized [`FullTextIndex`] and exposes query operations.
pub struct FullTextIndexReader {
    index: FullTextIndex,
}

impl FullTextIndexReader {
    /// Open an FTI from raw bytes.
    ///
    /// Validates the magic/version header before deserializing.
    pub fn open(bytes: Vec<u8>) -> Result<Self, String> {
        let fti = deserialize_fti(&bytes)?;
        Ok(Self { index: fti })
    }

    /// Total number of documents in this index.
    pub fn doc_count(&self) -> u32 {
        self.index.doc_count
    }

    /// Average document length across the corpus.
    pub fn avgdl(&self) -> f64 {
        self.index.avgdl()
    }

    /// Look up raw postings for a single term.
    ///
    /// Returns an empty slice when the term is not in the index.
    pub fn lookup(&self, term: &str) -> Vec<&Posting> {
        match self.index.terms.get(term) {
            Some(entry) => entry.postings.iter().collect(),
            None => vec![],
        }
    }

    /// Execute a parsed [`FtsQuery`] and return scored hits.
    ///
    /// Results are sorted by descending BM25 score. Deduplication by
    /// partition key is applied: the maximum score for any key wins.
    pub fn search(&self, query: &FtsQuery) -> Vec<FtsHit> {
        let mut score_map: HashMap<Vec<u8>, f64> = HashMap::new();
        self.eval_query(query, &mut score_map);

        let mut hits: Vec<FtsHit> = score_map
            .into_iter()
            .map(|(pk, score)| FtsHit {
                partition_key: pk,
                score,
            })
            .collect();
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        hits
    }

    /// Execute a query string (parses then searches).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the query string fails to parse.
    pub fn search_str(&self, query: &str) -> Result<Vec<FtsHit>, String> {
        let parsed = parse_fts_query(query)?;
        Ok(self.search(&parsed))
    }

    // ── Private query evaluation ──────────────────────────────────────────────

    fn eval_query(&self, query: &FtsQuery, scores: &mut HashMap<Vec<u8>, f64>) {
        match query {
            FtsQuery::Term(term) => self.score_term(term, scores),
            FtsQuery::MultiTerm(terms) => {
                // Implicit AND: a document must contain all terms.
                // We score each term independently and intersect.
                if terms.is_empty() {
                    return;
                }
                let mut first_scores: HashMap<Vec<u8>, f64> = HashMap::new();
                self.score_term(&terms[0], &mut first_scores);
                for term in &terms[1..] {
                    let mut term_scores: HashMap<Vec<u8>, f64> = HashMap::new();
                    self.score_term(term, &mut term_scores);
                    // Keep only partition keys present in both sets.
                    first_scores.retain(|pk, _| term_scores.contains_key(pk));
                    // Accumulate scores for the intersection.
                    for (pk, s) in &term_scores {
                        if let Some(existing) = first_scores.get_mut(pk) {
                            *existing += s;
                        }
                    }
                }
                for (pk, s) in first_scores {
                    let entry = scores.entry(pk).or_insert(0.0);
                    *entry = entry.max(s);
                }
            }
            FtsQuery::Phrase(words) => {
                // For phrase queries: all words must occur in the document.
                // We approximate by requiring each word term to be present
                // and summing their scores (full positional index is future work).
                if words.is_empty() {
                    return;
                }
                let mut candidates: Option<HashMap<Vec<u8>, f64>> = None;
                for word in words {
                    let mut word_scores: HashMap<Vec<u8>, f64> = HashMap::new();
                    self.score_term(word, &mut word_scores);
                    match candidates.take() {
                        None => candidates = Some(word_scores),
                        Some(mut prev) => {
                            prev.retain(|pk, _| word_scores.contains_key(pk));
                            for (pk, s) in &word_scores {
                                if let Some(existing) = prev.get_mut(pk) {
                                    *existing += s;
                                }
                            }
                            candidates = Some(prev);
                        }
                    }
                }
                if let Some(phrase_scores) = candidates {
                    for (pk, s) in phrase_scores {
                        let entry = scores.entry(pk).or_insert(0.0);
                        *entry = entry.max(s);
                    }
                }
            }
            FtsQuery::And(left, right) => {
                let mut left_scores: HashMap<Vec<u8>, f64> = HashMap::new();
                let mut right_scores: HashMap<Vec<u8>, f64> = HashMap::new();
                self.eval_query(left, &mut left_scores);
                self.eval_query(right, &mut right_scores);
                // Intersection: only keys present in both.
                for (pk, ls) in &left_scores {
                    if let Some(rs) = right_scores.get(pk) {
                        let entry = scores.entry(pk.clone()).or_insert(0.0);
                        *entry = entry.max(ls + rs);
                    }
                }
            }
            FtsQuery::Or(left, right) => {
                self.eval_query(left, scores);
                self.eval_query(right, scores);
            }
            FtsQuery::Prefix(prefix) => {
                self.eval_prefix(prefix, scores);
            }
            FtsQuery::Not(inner) => {
                // NOT: collect all documents, then remove those matching inner.
                let mut all_scores: HashMap<Vec<u8>, f64> = HashMap::new();
                for entry in self.index.terms.values() {
                    for posting in &entry.postings {
                        all_scores
                            .entry(posting.partition_key.clone())
                            .or_insert(1.0);
                    }
                }
                let mut excluded: HashMap<Vec<u8>, f64> = HashMap::new();
                self.eval_query(inner, &mut excluded);
                for pk in excluded.keys() {
                    all_scores.remove(pk);
                }
                for (pk, s) in all_scores {
                    scores.entry(pk).or_insert(s);
                }
            }
        }
    }

    /// Maximum number of terms a prefix wildcard can expand to.
    const MAX_WILDCARD_EXPANSION: usize = 10_000;

    /// Evaluate a prefix wildcard query, expanding to matching terms with a cap.
    fn eval_prefix(&self, prefix: &str, scores: &mut HashMap<Vec<u8>, f64>) {
        // Collect matching terms and their doc frequencies.
        let mut matching: Vec<(&str, u32)> = self
            .index
            .terms
            .iter()
            .filter(|(term, _)| term.starts_with(prefix))
            .map(|(term, entry)| (term.as_str(), entry.doc_freq))
            .collect();

        // If exceeds cap, keep terms with highest doc frequency.
        if matching.len() > Self::MAX_WILDCARD_EXPANSION {
            matching.sort_by(|a, b| b.1.cmp(&a.1));
            matching.truncate(Self::MAX_WILDCARD_EXPANSION);
        }

        // Score all postings from matching terms.
        for (term, _) in matching {
            self.score_term(term, scores);
        }
    }

    /// Score all postings for a single term using BM25.
    fn score_term(&self, term: &str, scores: &mut HashMap<Vec<u8>, f64>) {
        let entry = match self.index.terms.get(term) {
            Some(e) => e,
            None => return,
        };
        let params = Bm25Params::default();
        let n = self.index.doc_count as u64;
        let df = entry.doc_freq as u64;
        let avgdl = self.index.avgdl();

        for posting in &entry.postings {
            let s = bm25_score(posting.term_freq, df, n, posting.doc_len, avgdl, &params);
            let entry_score = scores.entry(posting.partition_key.clone()).or_insert(0.0);
            *entry_score += s;
        }
    }
}

// ── Deserialization ───────────────────────────────────────────────────────────

/// Deserialize a FTI byte buffer into a [`FullTextIndex`].
pub fn deserialize_fti(bytes: &[u8]) -> Result<FullTextIndex, String> {
    let mut pos = 0;

    // Header: magic(4) + version(1) + doc_count(4) = 9 bytes
    if bytes.len() < 9 {
        return Err(format!(
            "FTI too short: {} bytes (need at least 9 for header)",
            bytes.len()
        ));
    }

    let magic = &bytes[pos..pos + 4];
    if magic != FTI_MAGIC {
        return Err(format!("invalid FTI magic: {magic:?}"));
    }
    pos += 4;

    let version = bytes[pos];
    if version != FTI_VERSION {
        return Err(format!(
            "unsupported FTI version {version} (expected {FTI_VERSION})"
        ));
    }
    pos += 1;

    let doc_count = read_u32_le(bytes, pos)?;
    pos += 4;

    // term_count(4)
    let term_count = read_u32_le(bytes, pos)? as usize;
    pos += 4;

    let mut terms = HashMap::with_capacity(term_count);

    for _ in 0..term_count {
        // term_len(2) + term_bytes + doc_freq(4) + posting_count(4)
        let term_len = read_u16_le(bytes, pos)? as usize;
        pos += 2;

        if pos + term_len > bytes.len() {
            return Err("FTI truncated while reading term bytes".into());
        }
        let term = std::str::from_utf8(&bytes[pos..pos + term_len])
            .map_err(|e| format!("invalid UTF-8 in term: {e}"))?
            .to_string();
        pos += term_len;

        let doc_freq = read_u32_le(bytes, pos)?;
        pos += 4;

        let posting_count = read_u32_le(bytes, pos)? as usize;
        pos += 4;

        let mut postings = Vec::with_capacity(posting_count);
        for _ in 0..posting_count {
            let pk_len = read_u16_le(bytes, pos)? as usize;
            pos += 2;

            if pos + pk_len > bytes.len() {
                return Err("FTI truncated while reading partition key".into());
            }
            let pk = bytes[pos..pos + pk_len].to_vec();
            pos += pk_len;

            let term_freq = read_u32_le(bytes, pos)?;
            pos += 4;
            let doc_len = read_u32_le(bytes, pos)?;
            pos += 4;

            postings.push(Posting {
                partition_key: pk,
                term_freq,
                doc_len,
            });
        }

        terms.insert(term, TermEntry { doc_freq, postings });
    }

    // Corpus stats: total_doc_len(8)
    let total_doc_len = read_u64_le(bytes, pos)?;

    Ok(FullTextIndex {
        doc_count,
        total_doc_len,
        terms,
    })
}

fn read_u16_le(bytes: &[u8], pos: usize) -> Result<u16, String> {
    if pos + 2 > bytes.len() {
        return Err(format!("FTI truncated at offset {pos} reading u16"));
    }
    Ok(u16::from_le_bytes([bytes[pos], bytes[pos + 1]]))
}

fn read_u32_le(bytes: &[u8], pos: usize) -> Result<u32, String> {
    if pos + 4 > bytes.len() {
        return Err(format!("FTI truncated at offset {pos} reading u32"));
    }
    Ok(u32::from_le_bytes([
        bytes[pos],
        bytes[pos + 1],
        bytes[pos + 2],
        bytes[pos + 3],
    ]))
}

fn read_u64_le(bytes: &[u8], pos: usize) -> Result<u64, String> {
    if pos + 8 > bytes.len() {
        return Err(format!("FTI truncated at offset {pos} reading u64"));
    }
    Ok(u64::from_le_bytes([
        bytes[pos],
        bytes[pos + 1],
        bytes[pos + 2],
        bytes[pos + 3],
        bytes[pos + 4],
        bytes[pos + 5],
        bytes[pos + 6],
        bytes[pos + 7],
    ]))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fulltext::builder::FullTextIndexBuilder;

    fn make_reader(docs: &[(&[u8], &str)]) -> FullTextIndexReader {
        let mut builder = FullTextIndexBuilder::new();
        for (pk, text) in docs {
            builder.add_document(pk.to_vec(), text);
        }
        let bytes = builder.finish().unwrap();
        FullTextIndexReader::open(bytes).unwrap()
    }

    #[test]
    fn search_single_term_finds_matching_docs() {
        let reader = make_reader(&[
            (b"pk1", "rust programming language"),
            (b"pk2", "go programming language"),
            (b"pk3", "python scripting"),
        ]);
        let hits = reader.search_str("rust").unwrap();
        assert_eq!(hits.len(), 1, "expected 1 hit for 'rust'");
        assert_eq!(hits[0].partition_key, b"pk1");
    }

    #[test]
    fn search_common_term_finds_multiple_docs() {
        let reader = make_reader(&[
            (b"pk1", "rust programming language"),
            (b"pk2", "go programming language"),
            (b"pk3", "python scripting"),
        ]);
        let hits = reader.search_str("programming").unwrap();
        assert_eq!(hits.len(), 2, "expected 2 hits for 'programming'");
    }

    #[test]
    fn search_returns_scores_in_descending_order() {
        let reader = make_reader(&[
            (b"pk1", "rust rust rust best language"),
            (b"pk2", "rust language"),
        ]);
        let hits = reader.search_str("rust").unwrap();
        assert!(hits.len() >= 2);
        assert!(
            hits[0].score >= hits[1].score,
            "results must be sorted by descending score"
        );
        // pk1 has tf=3, so it should score higher.
        assert_eq!(hits[0].partition_key, b"pk1");
    }

    #[test]
    fn search_and_query_requires_both_terms() {
        let reader = make_reader(&[
            (b"pk1", "rust programming language"),
            (b"pk2", "go language"),
            (b"pk3", "python scripting"),
        ]);
        let hits = reader.search_str("rust AND language").unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].partition_key, b"pk1");
    }

    #[test]
    fn search_or_query_returns_union() {
        let reader = make_reader(&[
            (b"pk1", "rust programming language"),
            (b"pk2", "go language"),
            (b"pk3", "python scripting"),
        ]);
        let hits = reader.search_str("rust OR python").unwrap();
        let pks: Vec<_> = hits.iter().map(|h| h.partition_key.clone()).collect();
        assert!(pks.contains(&b"pk1".to_vec()), "pk1 must be in OR result");
        assert!(pks.contains(&b"pk3".to_vec()), "pk3 must be in OR result");
    }

    #[test]
    fn search_prefix_query() {
        let reader = make_reader(&[
            (b"pk1", "rustacean rust programming"),
            (b"pk2", "python scripting"),
            (b"pk3", "rustic furniture"),
        ]);
        let hits = reader.search_str("rust*").unwrap();
        let pks: Vec<_> = hits.iter().map(|h| h.partition_key.as_slice()).collect();
        assert!(pks.contains(&b"pk1".as_slice()), "pk1 has 'rustacean' and 'rust'");
        assert!(pks.contains(&b"pk3".as_slice()), "pk3 has 'rustic'");
        assert!(!pks.contains(&b"pk2".as_slice()), "pk2 has no rust* term");
    }

    #[test]
    fn search_not_query() {
        let reader = make_reader(&[
            (b"pk1", "rust programming"),
            (b"pk2", "go programming"),
            (b"pk3", "python scripting"),
        ]);
        let hits = reader.search_str("NOT rust").unwrap();
        let pks: Vec<_> = hits.iter().map(|h| h.partition_key.as_slice()).collect();
        assert!(!pks.contains(&b"pk1".as_slice()), "pk1 has 'rust' — excluded");
        assert!(pks.contains(&b"pk2".as_slice()), "pk2 should remain");
        assert!(pks.contains(&b"pk3".as_slice()), "pk3 should remain");
    }

    #[test]
    fn fts_wildcard_bare_star_rejected() {
        let reader = make_reader(&[(b"pk1", "hello world")]);
        let result = reader.search_str("*");
        assert!(result.is_err(), "bare star must be rejected");
    }

    #[test]
    fn fts_wildcard_expansion_capped() {
        // Create index with many unique terms starting with "a".
        let mut builder = FullTextIndexBuilder::new();
        for i in 0..500 {
            builder.add_document(format!("pk{i}").into_bytes(), &format!("a{i:05} other"));
        }
        let bytes = builder.finish().unwrap();
        let reader = FullTextIndexReader::open(bytes).unwrap();
        // Prefix search should work without OOM.
        let hits = reader.search_str("a*").unwrap();
        assert!(!hits.is_empty(), "prefix search must return results");
        assert!(hits.len() <= 500, "must not exceed doc count");
    }

    #[test]
    fn deserialize_invalid_magic_returns_err() {
        let bad = b"XXXX\x01\x00\x00\x00\x00";
        assert!(deserialize_fti(bad).is_err());
    }

    #[test]
    fn deserialize_truncated_returns_err() {
        let truncated = &b"FTIX"[..]; // only magic, nothing else
        assert!(deserialize_fti(truncated).is_err());
    }
}
