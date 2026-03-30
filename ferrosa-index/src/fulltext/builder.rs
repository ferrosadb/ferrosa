//! Full-text index builder (FT-005).
//!
//! Accumulates documents via [`FullTextIndexBuilder::add_document`], then
//! serializes the complete inverted index to the FTI binary format via
//! [`FullTextIndexBuilder::finish`].
//!
//! ## FTI Binary Format
//!
//! ```text
//! Header (24 bytes):
//!   [4] magic:           b"FTI\x01"
//!   [4] version:         1u32 BE
//!   [4] term_count:      u32 BE
//!   [4] doc_count:       u32 BE
//!   [8] terms_offset:    u64 BE  (always 24)
//!
//! Terms Dictionary (term_count entries, sorted by term bytes):
//!   Per term:
//!     [2] term_length:      u16 BE
//!     [N] term_bytes:       UTF-8
//!     [4] doc_frequency:    u32 BE
//!     [8] postings_offset:  u64 BE (absolute offset into file)
//!     [4] postings_length:  u32 BE (byte length of this term's postings block)
//!
//! Postings Section:
//!   Per term's postings block (contiguous, in the same order as terms dict):
//!     Per posting:
//!       [8] partition_key_hash: i64 BE (Murmur3 token)
//!       [2] key_length:         u16 BE
//!       [N] partition_key_bytes
//!       [4] term_frequency:     u32 BE
//!       [4] field_length:       u32 BE (total tokens in document)
//! ```

use std::collections::HashMap;

use super::analyzer::Analyzer;

// ── Public types ──────────────────────────────────────────────────────────────

/// A single document posting: the occurrence of a term in one document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    /// Raw partition key bytes that identify the document.
    pub partition_key: Vec<u8>,
    /// Murmur3 token for the partition key (from the Cassandra partitioner).
    pub partition_key_hash: i64,
    /// Number of times this term occurs in the document.
    pub term_frequency: u32,
    /// Total number of tokens in the document (for BM25 normalization).
    pub field_length: u32,
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builds an in-memory inverted index and serializes it to FTI binary format.
///
/// # Example
/// ```
/// use ferrosa_index::fulltext::builder::FullTextIndexBuilder;
/// use ferrosa_index::fulltext::analyzer::SimpleAnalyzer;
///
/// let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
/// builder.add_document(b"pk1", 12345, "hello world");
/// let data = builder.finish();
/// assert_eq!(&data[0..4], b"FTI\x01");
/// ```
pub struct FullTextIndexBuilder {
    analyzer: Box<dyn Analyzer>,
    /// term -> ordered list of postings (one per document that contains the term)
    postings: HashMap<String, Vec<Posting>>,
    doc_count: u32,
}

impl FullTextIndexBuilder {
    /// Create a new builder that will use `analyzer` to tokenize documents.
    pub fn new(analyzer: Box<dyn Analyzer>) -> Self {
        Self {
            analyzer,
            postings: HashMap::new(),
            doc_count: 0,
        }
    }

    /// Add a document to the index.
    ///
    /// - `partition_key`: raw bytes identifying the Cassandra partition.
    /// - `partition_key_hash`: Murmur3 token for the partition key.
    /// - `text`: the field text to be analyzed and indexed.
    ///
    /// Documents with empty text are recorded (doc_count increments) but
    /// contribute no term postings.
    pub fn add_document(&mut self, partition_key: &[u8], partition_key_hash: i64, text: &str) {
        let tokens = self.analyzer.analyze(text);
        let field_length = tokens.len() as u32;

        // Count term frequencies in this document
        let mut freq: HashMap<String, u32> = HashMap::new();
        for token in &tokens {
            *freq.entry(token.text.to_string()).or_default() += 1;
        }

        // Append one posting per distinct term
        for (term, tf) in freq {
            self.postings.entry(term).or_default().push(Posting {
                partition_key: partition_key.to_vec(),
                partition_key_hash,
                term_frequency: tf,
                field_length,
            });
        }

        self.doc_count += 1;
    }

    /// Finalize and serialize the index to a `Vec<u8>` in FTI format.
    ///
    /// The returned bytes start with the magic bytes `b"FTI\x01"` and can be
    /// passed directly to [`super::reader::FullTextIndexReader::open`].
    pub fn finish(self) -> Vec<u8> {
        // Sort terms for binary search in the reader
        let mut sorted_terms: Vec<String> = self.postings.keys().cloned().collect();
        sorted_terms.sort_unstable();

        let term_count = sorted_terms.len() as u32;
        let doc_count = self.doc_count;

        // ── Pass 1: serialize postings section and build terms dict ────────
        // We need to know postings offsets, so we build the postings bytes
        // first then prepend the header + terms dict.

        // Terms dict size: sum of (2 + term_bytes + 4 + 8 + 4) per term
        let terms_section_size: usize = sorted_terms.iter().map(|t| 2 + t.len() + 4 + 8 + 4).sum();

        // Header is always 24 bytes: magic(4) + version(4) + term_count(4) + doc_count(4) + terms_offset(8)
        const HEADER_SIZE: usize = 24;
        let terms_start: u64 = HEADER_SIZE as u64;
        let postings_start: u64 = terms_start + terms_section_size as u64;

        // Build postings bytes, accumulating offset for each term
        let mut postings_bytes: Vec<u8> = Vec::new();
        // Parallel list of (postings_offset, postings_length) for each term
        let mut posting_locs: Vec<(u64, u32)> = Vec::with_capacity(sorted_terms.len());

        for term in &sorted_terms {
            let term_postings = &self.postings[term];
            let offset = postings_start + postings_bytes.len() as u64;
            let start_len = postings_bytes.len();

            for posting in term_postings {
                // [8] partition_key_hash i64 BE
                postings_bytes.extend_from_slice(&posting.partition_key_hash.to_be_bytes());
                // [2] key_length u16 BE
                let key_len = posting.partition_key.len() as u16;
                postings_bytes.extend_from_slice(&key_len.to_be_bytes());
                // [N] partition_key_bytes
                postings_bytes.extend_from_slice(&posting.partition_key);
                // [4] term_frequency u32 BE
                postings_bytes.extend_from_slice(&posting.term_frequency.to_be_bytes());
                // [4] field_length u32 BE
                postings_bytes.extend_from_slice(&posting.field_length.to_be_bytes());
            }

            let block_len = (postings_bytes.len() - start_len) as u32;
            posting_locs.push((offset, block_len));
        }

        // ── Pass 2: assemble final buffer ──────────────────────────────────
        let total_size = HEADER_SIZE + terms_section_size + postings_bytes.len();
        let mut buf = Vec::with_capacity(total_size);

        // Header
        buf.extend_from_slice(b"FTI\x01"); // magic [4]
        buf.extend_from_slice(&1u32.to_be_bytes()); // version [4]
        buf.extend_from_slice(&term_count.to_be_bytes()); // term_count [4]
        buf.extend_from_slice(&doc_count.to_be_bytes()); // doc_count [4]
        buf.extend_from_slice(&terms_start.to_be_bytes()); // terms_offset [8]

        // Terms dictionary
        for (i, term) in sorted_terms.iter().enumerate() {
            let (postings_offset, postings_length) = posting_locs[i];
            let doc_freq = self.postings[term].len() as u32;
            let term_bytes = term.as_bytes();

            // [2] term_length u16 BE
            buf.extend_from_slice(&(term_bytes.len() as u16).to_be_bytes());
            // [N] term_bytes
            buf.extend_from_slice(term_bytes);
            // [4] doc_frequency u32 BE
            buf.extend_from_slice(&doc_freq.to_be_bytes());
            // [8] postings_offset u64 BE
            buf.extend_from_slice(&postings_offset.to_be_bytes());
            // [4] postings_length u32 BE
            buf.extend_from_slice(&postings_length.to_be_bytes());
        }

        // Postings section
        buf.extend_from_slice(&postings_bytes);

        buf
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fulltext::analyzer::SimpleAnalyzer;

    #[test]
    fn fts_builder_single_doc() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 12345, "hello world");
        let data = builder.finish();
        assert!(!data.is_empty());
        assert_eq!(&data[0..4], b"FTI\x01", "magic bytes must be present");
    }

    #[test]
    fn fts_builder_multi_doc() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 1, "hello world");
        builder.add_document(b"pk2", 2, "hello rust");
        builder.add_document(b"pk3", 3, "world rust");
        let data = builder.finish();
        assert!(!data.is_empty(), "multi-doc index must not be empty");

        // Check the header for term_count and doc_count
        // Header layout: magic[4] + version[4] + term_count[4] + doc_count[4] + terms_offset[8]
        let term_count = u32::from_be_bytes(data[8..12].try_into().unwrap());
        let doc_count = u32::from_be_bytes(data[12..16].try_into().unwrap());
        assert_eq!(doc_count, 3, "doc_count must reflect three documents");
        // "hello", "world", "rust" — 3 distinct terms
        assert_eq!(
            term_count, 3,
            "term_count must reflect three distinct terms"
        );
    }

    #[test]
    fn fts_builder_handles_valid_text() {
        // This test verifies the builder accepts ordinary text without panicking.
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 1, "valid text only");
        let data = builder.finish();
        assert!(!data.is_empty());
    }

    #[test]
    fn fts_builder_empty_field() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 1, "");
        let data = builder.finish();
        // Should produce valid FTI with 0 terms but doc_count=1
        assert_eq!(&data[0..4], b"FTI\x01", "magic bytes must be present");
        let term_count = u32::from_be_bytes(data[8..12].try_into().unwrap());
        let doc_count = u32::from_be_bytes(data[12..16].try_into().unwrap());
        assert_eq!(term_count, 0, "empty field yields zero terms");
        assert_eq!(doc_count, 1, "empty field still increments doc_count");
    }

    #[test]
    fn fts_builder_term_frequency_counted_correctly() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        // "hello" appears twice in one document
        builder.add_document(b"pk1", 1, "hello hello world");
        let data = builder.finish();
        assert!(!data.is_empty());
        // We'll verify the full term frequency through the reader in reader tests.
        // Here just check the index is non-trivial.
        let term_count = u32::from_be_bytes(data[8..12].try_into().unwrap());
        assert_eq!(term_count, 2, "hello and world are the two distinct terms");
    }

    #[test]
    fn fts_builder_version_field_is_one() {
        let builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        let data = builder.finish();
        let version = u32::from_be_bytes(data[4..8].try_into().unwrap());
        assert_eq!(version, 1, "version field must be 1");
    }
}
