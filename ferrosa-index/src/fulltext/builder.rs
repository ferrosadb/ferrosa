//! Full-text index (FTI) builder.
//!
//! Builds an inverted index from documents. Each document contributes:
//! - A partition key (used as the document identifier).
//! - A text field value (analyzed into tokens).
//!
//! ## File format
//!
//! ```text
//! ┌── Header (9 bytes) ──────────────────────────────┐
//! │ magic:      b"FTIX"  (4 bytes)                   │
//! │ version:    u8       (1 byte) = 1                │
//! │ doc_count:  u32 LE   (4 bytes)                   │
//! ├── Term section ─────────────────────────────────-┤
//! │ term_count: u32 LE   (4 bytes)                   │
//! │ For each term (sorted):                          │
//! │   term_len:    u16 LE (2 bytes)                  │
//! │   term_bytes:  [u8]   (term_len bytes)           │
//! │   doc_freq:    u32 LE (4 bytes)                  │
//! │   posting_count: u32 LE (4 bytes)                │
//! │   For each posting:                              │
//! │     pk_len:    u16 LE (2 bytes)                  │
//! │     pk_bytes:  [u8]   (pk_len bytes)             │
//! │     tf:        u32 LE (4 bytes)  term freq       │
//! │     dl:        u32 LE (4 bytes)  doc length      │
//! ├── Corpus stats (8 bytes) ────────────────────────┤
//! │ total_doc_len: u64 LE (8 bytes)  sum of all dl   │
//! ├── Term index (optional) ─────────────────────────┤
//! │ Every FTI_TERM_INDEX_INTERVAL-th term, in order: │
//! │   term_len:  u16 LE, term_bytes: [u8]            │
//! │   ordinal:   u32 LE  (its position in the dict)  │
//! │   offset:    u64 LE  (where its term_len starts) │
//! ├── Term index footer (20 bytes) ──────────────────┤
//! │ index_offset: u64 LE  (where the index starts)   │
//! │ entry_count:  u32 LE                             │
//! │ magic:        b"FTITIDX1" (8 bytes)              │
//! ├── Corpus stats again (8 bytes) ──────────────────┤
//! │ total_doc_len: u64 LE                            │
//! └──────────────────────────────────────────────────┘
//! ```
//!
//! The term index lets a lookup seek to within FTI_TERM_INDEX_INTERVAL terms
//! of its target instead of walking the dictionary from the start. It is
//! appended after the original layout, which the file still opens with byte
//! for byte, and `total_doc_len` is repeated as the last 8 bytes. So a build
//! that predates the index reads these files unchanged (its whole-file reader
//! stops at the first `total_doc_len`, its streaming reader takes the last 8
//! bytes and walks the dictionary), and a sidecar written before the index is
//! upgraded by appending to it. The version byte stays 1 for that reason: an
//! older build rejects any other version.

use std::collections::HashMap;

use super::analyzer::{default_analyzer, Analyzer};

/// Magic bytes for FTI files.
pub const FTI_MAGIC: &[u8; 4] = b"FTIX";
/// Current FTI format version.
pub const FTI_VERSION: u8 = 1;
/// Terms between consecutive term-index entries: a lookup walks at most this
/// many dictionary terms after its seek.
pub const FTI_TERM_INDEX_INTERVAL: usize = 64;
/// Marks a sidecar that carries a term index (see the layout above).
pub const FTI_TERM_INDEX_MAGIC: &[u8; 8] = b"FTITIDX1";
/// `index_offset` + `entry_count` + magic.
pub const FTI_TERM_INDEX_FOOTER_LEN: u64 = 8 + 4 + 8;

/// A single posting in the inverted index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    /// Serialized partition key for this document.
    pub partition_key: Vec<u8>,
    /// Number of times the term appears in this document.
    pub term_freq: u32,
    /// Total number of tokens in this document (document length).
    pub doc_len: u32,
}

/// A term entry in the inverted index.
#[derive(Debug, Clone)]
pub struct TermEntry {
    /// Number of documents containing this term.
    pub doc_freq: u32,
    /// List of postings (one per document that contains the term).
    pub postings: Vec<Posting>,
}

/// In-memory representation of a full-text index, serializable to bytes.
#[derive(Debug, Clone)]
pub struct FullTextIndex {
    /// Total number of documents indexed.
    pub doc_count: u32,
    /// Sum of all document lengths (for avgdl computation).
    pub total_doc_len: u64,
    /// Sorted term dictionary.
    pub terms: HashMap<String, TermEntry>,
}

impl FullTextIndex {
    /// Average document length (returns 0.0 when `doc_count == 0`).
    pub fn avgdl(&self) -> f64 {
        if self.doc_count == 0 {
            0.0
        } else {
            self.total_doc_len as f64 / self.doc_count as f64
        }
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builds a [`FullTextIndex`] by ingesting documents one at a time.
pub struct FullTextIndexBuilder {
    analyzer: Box<dyn Analyzer>,
    /// term -> list of (partition_key, tf, dl)
    index: HashMap<String, Vec<(Vec<u8>, u32, u32)>>,
    doc_count: u32,
    total_doc_len: u64,
}

impl FullTextIndexBuilder {
    /// Create a builder using the shared [`default_analyzer`].
    pub fn new() -> Self {
        Self::with_analyzer(default_analyzer())
    }

    /// Create a builder with a custom analyzer.
    pub fn with_analyzer(analyzer: Box<dyn Analyzer>) -> Self {
        Self {
            analyzer,
            index: HashMap::new(),
            doc_count: 0,
            total_doc_len: 0,
        }
    }

    /// Create a builder whose term map is pre-sized for `terms` distinct terms,
    /// using the shared [`default_analyzer`].
    ///
    /// The term map is the largest allocation on a memtable index build and it
    /// otherwise grows by rehashing as terms accumulate — each rehash reallocates
    /// and rehashes every entry. A caller that knows the shape of the data (the
    /// memtable build estimates from its partition count) can size it once.
    pub fn with_capacity(terms: usize) -> Self {
        Self::with_analyzer_and_capacity(default_analyzer(), terms)
    }

    /// Create a builder with a custom analyzer and a pre-sized term map.
    pub fn with_analyzer_and_capacity(analyzer: Box<dyn Analyzer>, terms: usize) -> Self {
        Self {
            analyzer,
            index: HashMap::with_capacity(terms),
            doc_count: 0,
            total_doc_len: 0,
        }
    }

    /// Add a document whose tokens have already been produced, reusing the
    /// caller's buffers.
    ///
    /// Equivalent to [`FullTextIndexBuilder::add_document`] but allocation-free
    /// for the caller's per-row loop: `tokens` and `tf` are **cleared on entry**
    /// (they need not be empty) and the analyzer appends into `tokens` via
    /// [`Analyzer::analyze_into`] rather than returning a fresh `Vec`. On return
    /// they hold this document's tokens and counts, and are left with their
    /// capacity intact so the caller can pass the same buffers to the next row.
    ///
    /// A scan over N rows therefore does not allocate N token vectors and N maps.
    pub fn add_document_with_tf(
        &mut self,
        partition_key: Vec<u8>,
        text: &str,
        tf: &mut HashMap<String, u32>,
        tokens: &mut Vec<String>,
    ) {
        tokens.clear();
        self.analyzer.analyze_into(text, tokens);
        if tokens.is_empty() {
            return;
        }

        let dl = tokens.len() as u32;
        self.doc_count += 1;
        self.total_doc_len += dl as u64;

        tf.clear();
        for token in tokens.iter() {
            *tf.entry(token.clone()).or_insert(0) += 1;
        }

        // Deliberately the SAME shape as `add_document`: push one posting per
        // (term, document) and let `build()` sort-and-fold repeated keys. Folding
        // here instead would be a second implementation of the same rule, free to
        // drift from the one `build()` applies — and `build()`'s fold is what the
        // readers depend on.
        for (term, count) in tf.iter() {
            self.index
                .entry(term.clone())
                .or_default()
                .push((partition_key.clone(), *count, dl));
        }
    }

    /// Add a document to the index.
    ///
    /// # Arguments
    ///
    /// * `partition_key` — serialized partition key bytes (document identifier).
    /// * `text`          — field value to analyze and index.
    pub fn add_document(&mut self, partition_key: Vec<u8>, text: &str) {
        let tokens = self.analyzer.analyze(text);
        if tokens.is_empty() {
            return;
        }

        let dl = tokens.len() as u32;
        self.doc_count += 1;
        self.total_doc_len += dl as u64;

        // Count term frequencies within this document.
        let mut tf_map: HashMap<String, u32> = HashMap::new();
        for token in tokens {
            *tf_map.entry(token).or_insert(0) += 1;
        }

        for (term, tf) in tf_map {
            self.index
                .entry(term)
                .or_default()
                .push((partition_key.clone(), tf, dl));
        }
    }

    /// Build the [`FullTextIndex`].
    ///
    /// A term's postings hold each key at most once. A key added more than
    /// once is folded the way [`crate::fulltext::merge`] folds two sidecars:
    /// term frequencies sum and the last-added document length wins. The
    /// readers rely on this — `search` sums a key's postings through a score
    /// map, while `search_top_k` streams postings into a heap with no map, so
    /// a repeated key would score differently on the two paths.
    pub fn build(self) -> FullTextIndex {
        let terms = self
            .index
            .into_iter()
            .map(|(term, mut postings_raw)| {
                // Stable: among equal keys, add order (oldest first) survives.
                postings_raw.sort_by(|a, b| a.0.cmp(&b.0));
                let mut postings: Vec<Posting> = Vec::with_capacity(postings_raw.len());
                for (pk, tf, dl) in postings_raw {
                    match postings.last_mut() {
                        Some(last) if last.partition_key == pk => {
                            last.term_freq += tf;
                            last.doc_len = dl;
                        }
                        _ => postings.push(Posting {
                            partition_key: pk,
                            term_freq: tf,
                            doc_len: dl,
                        }),
                    }
                }
                let doc_freq = postings.len() as u32;
                (term, TermEntry { doc_freq, postings })
            })
            .collect();

        FullTextIndex {
            doc_count: self.doc_count,
            total_doc_len: self.total_doc_len,
            terms,
        }
    }

    /// Build and serialize to bytes (convenience wrapper).
    pub fn finish(self) -> Result<Vec<u8>, String> {
        let fti = self.build();
        serialize_fti(&fti)
    }
}

impl Default for FullTextIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ── Serialization ─────────────────────────────────────────────────────────────

/// Serialize a [`FullTextIndex`] to the FTI byte format, with a term index.
pub fn serialize_fti(fti: &FullTextIndex) -> Result<Vec<u8>, String> {
    let mut entries: Vec<(&str, u32, u64)> = Vec::new();
    let mut buf = serialize_body(fti, |ordinal, term, offset| {
        if (ordinal as usize).is_multiple_of(FTI_TERM_INDEX_INTERVAL) {
            entries.push((term, ordinal, offset));
        }
    })?;
    append_term_index(&mut buf, &entries, fti.total_doc_len)?;
    Ok(buf)
}

/// The FTI layout written before the term index: no index section, no footer.
pub fn serialize_fti_without_term_index(fti: &FullTextIndex) -> Result<Vec<u8>, String> {
    serialize_body(fti, |_, _, _| {})
}

/// Append the term index, its footer, and `total_doc_len` to a sidecar that
/// ends with the original layout. `entries` are (term, ordinal, offset) of
/// every [`FTI_TERM_INDEX_INTERVAL`]-th term, in dictionary order.
pub fn append_term_index(
    buf: &mut Vec<u8>,
    entries: &[(&str, u32, u64)],
    total_doc_len: u64,
) -> Result<(), String> {
    let tail = term_index_tail(buf.len() as u64, entries, total_doc_len)?;
    buf.extend_from_slice(&tail);
    Ok(())
}

/// The bytes [`append_term_index`] appends to a sidecar whose original layout
/// ends at `index_offset`: the index, its footer, and `total_doc_len`.
pub fn term_index_tail(
    index_offset: u64,
    entries: &[(&str, u32, u64)],
    total_doc_len: u64,
) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    for (term, ordinal, offset) in entries {
        buf.extend_from_slice(&term_len_u16(term)?.to_le_bytes());
        buf.extend_from_slice(term.as_bytes());
        buf.extend_from_slice(&ordinal.to_le_bytes());
        buf.extend_from_slice(&offset.to_le_bytes());
    }
    let entry_count = u32::try_from(entries.len())
        .map_err(|_| format!("FTI term index has too many entries: {}", entries.len()))?;
    buf.extend_from_slice(&index_offset.to_le_bytes());
    buf.extend_from_slice(&entry_count.to_le_bytes());
    buf.extend_from_slice(FTI_TERM_INDEX_MAGIC);
    buf.extend_from_slice(&total_doc_len.to_le_bytes());
    Ok(buf)
}

fn term_len_u16(term: &str) -> Result<u16, String> {
    u16::try_from(term.len()).map_err(|_| format!("FTI term is {} bytes, over 65535", term.len()))
}

/// The original layout, calling `on_term(ordinal, term, offset)` for each
/// term with the offset its record starts at.
fn serialize_body<'a>(
    fti: &'a FullTextIndex,
    mut on_term: impl FnMut(u32, &'a str, u64),
) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();

    // Header: magic + version + doc_count
    buf.extend_from_slice(FTI_MAGIC);
    buf.push(FTI_VERSION);
    buf.extend_from_slice(&fti.doc_count.to_le_bytes());

    // Collect and sort terms.
    let mut sorted_terms: Vec<(&String, &TermEntry)> = fti.terms.iter().collect();
    sorted_terms.sort_by_key(|(k, _)| *k);

    // term_count
    let term_count = sorted_terms.len() as u32;
    buf.extend_from_slice(&term_count.to_le_bytes());

    for (ordinal, (term, entry)) in sorted_terms.iter().enumerate() {
        on_term(ordinal as u32, term.as_str(), buf.len() as u64);
        let term_bytes = term.as_bytes();
        buf.extend_from_slice(&term_len_u16(term)?.to_le_bytes());
        buf.extend_from_slice(term_bytes);
        buf.extend_from_slice(&entry.doc_freq.to_le_bytes());

        let posting_count = entry.postings.len() as u32;
        buf.extend_from_slice(&posting_count.to_le_bytes());

        for posting in &entry.postings {
            let pk_len = posting.partition_key.len() as u16;
            buf.extend_from_slice(&pk_len.to_le_bytes());
            buf.extend_from_slice(&posting.partition_key);
            buf.extend_from_slice(&posting.term_freq.to_le_bytes());
            buf.extend_from_slice(&posting.doc_len.to_le_bytes());
        }
    }

    // Corpus stats: total_doc_len
    buf.extend_from_slice(&fti.total_doc_len.to_le_bytes());

    Ok(buf)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fulltext::reader::FullTextIndexReader;

    #[test]
    fn builder_roundtrip_single_doc() {
        let mut builder = FullTextIndexBuilder::new();
        builder.add_document(b"pk1".to_vec(), "the quick brown fox");
        let bytes = builder.finish().unwrap();
        let reader = FullTextIndexReader::open(bytes).unwrap();
        assert_eq!(reader.doc_count(), 1);
        // "quick" should be indexed (not a stop word in standard).
        let hits = reader.lookup("quick");
        assert!(!hits.is_empty(), "expected 'quick' to be indexed");
        assert_eq!(hits[0].partition_key, b"pk1".to_vec());
    }

    #[test]
    fn builder_multiple_docs() {
        let mut builder = FullTextIndexBuilder::new();
        builder.add_document(b"pk1".to_vec(), "rust programming language");
        builder.add_document(b"pk2".to_vec(), "go programming language");
        builder.add_document(b"pk3".to_vec(), "python scripting");
        let bytes = builder.finish().unwrap();
        let reader = FullTextIndexReader::open(bytes).unwrap();
        assert_eq!(reader.doc_count(), 3);
        let hits = reader.lookup("programming");
        assert_eq!(hits.len(), 2, "expected 2 docs with 'programming'");
    }

    #[test]
    fn builder_term_frequency_counted() {
        let mut builder = FullTextIndexBuilder::new();
        builder.add_document(b"doc1".to_vec(), "rust rust rust is great");
        let bytes = builder.finish().unwrap();
        let reader = FullTextIndexReader::open(bytes).unwrap();
        let hits = reader.lookup("rust");
        assert!(!hits.is_empty());
        assert_eq!(hits[0].term_freq, 3, "expected tf=3 for 'rust'");
    }
}
