//! Full-text index builder: produces FTI sidecar bytes.
//!
//! [`FullTextIndexBuilder`] accepts documents (partition key + token + text),
//! tokenizes each text using a pluggable [`TextAnalyzer`], accumulates term
//! postings in memory, then serializes the inverted index to the FTI binary
//! format on [`finish()`].
//!
//! The final byte slice includes a CRC32 footer over all preceding bytes so
//! the reader can detect corruption.

use std::collections::BTreeMap;

use super::{FOOTER_CRC_SIZE, FTI_MAGIC, FTI_VERSION, HEADER_SIZE};

// ── Analyzer trait ────────────────────────────────────────────────────────────

/// Tokenizes a text string into a sequence of normalized terms.
pub trait TextAnalyzer: Send {
    /// Return the list of terms extracted from `text`.
    fn analyze(&self, text: &str) -> Vec<String>;
}

/// Simple whitespace + lowercase analyzer.
///
/// Splits on ASCII whitespace, lowercases each token, and strips leading/
/// trailing non-alphanumeric characters. Empty tokens are discarded.
pub struct SimpleAnalyzer;

impl TextAnalyzer for SimpleAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        text.split_ascii_whitespace()
            .map(|t| {
                t.trim_matches(|c: char| !c.is_alphanumeric())
                    .to_lowercase()
            })
            .filter(|t| !t.is_empty())
            .collect()
    }
}

// ── Posting ───────────────────────────────────────────────────────────────────

/// A single posting: the partition key and token for a document that contains
/// the term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    /// Raw bytes of the partition key.
    pub partition_key: Vec<u8>,
    /// Murmur3 token for the partition key (i64).
    pub token: i64,
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Accumulates documents and builds an FTI byte payload via [`finish()`].
///
/// Documents are tokenized by the configured [`TextAnalyzer`]. The resulting
/// inverted index maps each term to a deduplicated, sorted list of postings.
pub struct FullTextIndexBuilder {
    analyzer: Box<dyn TextAnalyzer>,
    /// term -> list of postings (may contain duplicates before dedup).
    postings: BTreeMap<String, Vec<Posting>>,
}

impl FullTextIndexBuilder {
    /// Create a new builder with the given analyzer.
    pub fn new(analyzer: Box<dyn TextAnalyzer>) -> Self {
        Self {
            analyzer,
            postings: BTreeMap::new(),
        }
    }

    /// Add a document to the index.
    ///
    /// - `partition_key`: raw partition key bytes, used as the row identifier.
    /// - `token`: Murmur3 token of the partition key (i64).
    /// - `text`: the text content to index.
    pub fn add_document(&mut self, partition_key: &[u8], token: i64, text: &str) {
        let terms = self.analyzer.analyze(text);
        for term in terms {
            let entry = self.postings.entry(term).or_default();
            entry.push(Posting {
                partition_key: partition_key.to_vec(),
                token,
            });
        }
    }

    /// Finalize and serialize the index to FTI bytes.
    ///
    /// The returned buffer is self-contained: it includes the header, term
    /// dictionary, postings section, and a 4-byte CRC32 footer.
    pub fn finish(self) -> Vec<u8> {
        // Deduplicate and sort postings per term.
        let mut terms: Vec<(String, Vec<Posting>)> = self
            .postings
            .into_iter()
            .map(|(term, mut postings)| {
                postings.sort_by(|a, b| a.token.cmp(&b.token));
                postings.dedup_by(|a, b| a.partition_key == b.partition_key);
                (term, postings)
            })
            .collect();
        // BTreeMap already gives sorted order; keep it sorted.
        terms.sort_by(|a, b| a.0.cmp(&b.0));

        let term_count = terms.len() as u32;

        // ── Pass 1: Serialize all postings sections to compute their offsets ──

        // Each posting list is:
        //   posting_count(u32 BE) + posting_count * (pk_len(u32 BE) + pk_bytes + token(i64 BE))
        let mut postings_buf: Vec<u8> = Vec::new();
        // offsets[i] = byte offset within postings_buf where term i's list starts
        let mut offsets: Vec<u64> = Vec::with_capacity(terms.len());

        for (_term, postings) in &terms {
            offsets.push(postings_buf.len() as u64);
            let count = postings.len() as u32;
            postings_buf.extend_from_slice(&count.to_be_bytes());
            for posting in postings {
                let pk_len = posting.partition_key.len() as u32;
                postings_buf.extend_from_slice(&pk_len.to_be_bytes());
                postings_buf.extend_from_slice(&posting.partition_key);
                postings_buf.extend_from_slice(&posting.token.to_be_bytes());
            }
        }

        // ── Pass 2: Serialize the term dictionary ─────────────────────────────

        // The postings section begins immediately after header + term dictionary.
        // We need to know the dictionary size before we can compute postings offsets.
        // Dictionary entry:
        //   term_len(u32 BE) + term_bytes + doc_frequency(u32 BE) + postings_offset(u64 BE)

        let dict_entry_size = |term: &str| -> usize {
            4 // term_len
            + term.len()
            + 4  // doc_frequency
            + 8 // postings_offset
        };

        let dict_size: usize = terms.iter().map(|(t, _)| dict_entry_size(t)).sum();

        // Postings section byte base = HEADER_SIZE + dict_size (no footer yet).
        let postings_base = (HEADER_SIZE + dict_size) as u64;

        let mut dict_buf: Vec<u8> = Vec::with_capacity(dict_size);
        for (i, (term, postings)) in terms.iter().enumerate() {
            let term_bytes = term.as_bytes();
            dict_buf.extend_from_slice(&(term_bytes.len() as u32).to_be_bytes());
            dict_buf.extend_from_slice(term_bytes);
            dict_buf.extend_from_slice(&(postings.len() as u32).to_be_bytes());
            // Absolute byte offset in the file where this posting list starts.
            let abs_offset = postings_base + offsets[i];
            dict_buf.extend_from_slice(&abs_offset.to_be_bytes());
        }

        assert_eq!(dict_buf.len(), dict_size, "dict size mismatch");

        // ── Assemble the full buffer ──────────────────────────────────────────

        let total_size = HEADER_SIZE + dict_size + postings_buf.len() + FOOTER_CRC_SIZE;
        let mut buf: Vec<u8> = Vec::with_capacity(total_size);

        // Header
        buf.extend_from_slice(FTI_MAGIC);
        buf.push(FTI_VERSION);
        buf.extend_from_slice(&term_count.to_be_bytes());

        assert_eq!(buf.len(), HEADER_SIZE, "header size mismatch");

        // Dictionary
        buf.extend_from_slice(&dict_buf);

        // Postings
        buf.extend_from_slice(&postings_buf);

        // CRC32 over everything so far
        let checksum = crc32fast::hash(&buf);
        buf.extend_from_slice(&checksum.to_be_bytes());

        assert_eq!(buf.len(), total_size, "total size mismatch");

        buf
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_builder_finish_produces_valid_header() {
        let builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        let data = builder.finish();

        // Must have at least header + CRC footer.
        assert!(
            data.len() >= HEADER_SIZE + FOOTER_CRC_SIZE,
            "expected at least {} bytes, got {}",
            HEADER_SIZE + FOOTER_CRC_SIZE,
            data.len()
        );

        assert_eq!(&data[..4], FTI_MAGIC.as_slice(), "magic mismatch");
        assert_eq!(data[4], FTI_VERSION, "version mismatch");

        // term_count == 0
        let term_count = u32::from_be_bytes(data[5..9].try_into().unwrap());
        assert_eq!(term_count, 0, "expected 0 terms in empty builder");
    }

    #[test]
    fn simple_analyzer_tokenizes_correctly() {
        let analyzer = SimpleAnalyzer;
        let terms = analyzer.analyze("Hello, World! foo bar");
        assert_eq!(terms, vec!["hello", "world", "foo", "bar"]);
    }

    #[test]
    fn simple_analyzer_empty_string() {
        let analyzer = SimpleAnalyzer;
        let terms = analyzer.analyze("");
        assert!(terms.is_empty());
    }

    #[test]
    fn simple_analyzer_strips_punctuation() {
        let analyzer = SimpleAnalyzer;
        let terms = analyzer.analyze("  --rust-- ");
        assert_eq!(terms, vec!["rust"]);
    }

    #[test]
    fn builder_deduplicates_postings_for_same_partition() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        // Same pk, same term appearing twice in the text.
        builder.add_document(b"pk1", 42, "hello hello world");
        let data = builder.finish();
        // Should have 2 terms: "hello" and "world", each with 1 posting.
        assert!(data.len() > HEADER_SIZE + FOOTER_CRC_SIZE);
    }

    #[test]
    fn builder_multiple_documents_same_term() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 1, "hello world");
        builder.add_document(b"pk2", 2, "hello rust");
        let data = builder.finish();

        // Verify CRC is at the end (bytes len-4).
        let data_len = data.len();
        let payload = &data[..data_len - 4];
        let stored_crc = u32::from_be_bytes(data[data_len - 4..].try_into().unwrap());
        let computed_crc = crc32fast::hash(payload);
        assert_eq!(stored_crc, computed_crc, "CRC32 must match");
    }
}
