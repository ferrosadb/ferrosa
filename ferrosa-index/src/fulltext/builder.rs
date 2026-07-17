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
//! └──────────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;

use super::analyzer::{default_analyzer, Analyzer};

/// Magic bytes for FTI files.
pub const FTI_MAGIC: &[u8; 4] = b"FTIX";
/// Current FTI format version.
pub const FTI_VERSION: u8 = 1;

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
    pub fn build(self) -> FullTextIndex {
        let terms = self
            .index
            .into_iter()
            .map(|(term, postings_raw)| {
                let doc_freq = postings_raw.len() as u32;
                let postings = postings_raw
                    .into_iter()
                    .map(|(pk, tf, dl)| Posting {
                        partition_key: pk,
                        term_freq: tf,
                        doc_len: dl,
                    })
                    .collect();
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

/// Serialize a [`FullTextIndex`] to the FTI byte format.
pub fn serialize_fti(fti: &FullTextIndex) -> Result<Vec<u8>, String> {
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

    for (term, entry) in &sorted_terms {
        let term_bytes = term.as_bytes();
        let term_len = term_bytes.len() as u16;
        buf.extend_from_slice(&term_len.to_le_bytes());
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
