//! Full-text index reader: parses FTI sidecar bytes with CRC32 validation.
//!
//! [`FullTextIndexReader::open()`] validates the magic, version, and CRC32
//! footer, then deserializes the term dictionary into memory. Postings are
//! read lazily by byte-slicing into the owned data buffer.

use crate::{IndexError, IndexResult};

use super::{FOOTER_CRC_SIZE, FTI_MAGIC, FTI_VERSION, HEADER_SIZE};

// ── Public types ──────────────────────────────────────────────────────────────

/// Summary information for a single term in the FTI dictionary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtiTermEntry {
    /// The indexed term (normalized, lowercase).
    pub term: String,
    /// Number of distinct documents that contain the term.
    pub doc_frequency: u32,
    /// Byte offset within the FTI buffer where the postings list starts.
    pub postings_offset: u64,
}

/// A posting retrieved from the postings section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtiPosting {
    /// Raw partition key bytes.
    pub partition_key: Vec<u8>,
    /// Murmur3 token for the partition key.
    pub token: i64,
}

// ── Reader ────────────────────────────────────────────────────────────────────

/// Reads a validated FTI byte buffer, exposing term lookup and posting scan.
///
/// The buffer is retained in full so that postings can be decoded on demand
/// without an additional allocation pass at open time.
#[derive(Debug)]
pub struct FullTextIndexReader {
    data: Vec<u8>,
    terms: Vec<FtiTermEntry>,
}

impl FullTextIndexReader {
    /// Parse and validate an FTI byte buffer.
    ///
    /// Returns `Err` on:
    /// - buffer too short for header + CRC footer
    /// - magic byte mismatch
    /// - unsupported version
    /// - CRC32 mismatch (data corruption)
    /// - malformed term dictionary
    pub fn open(data: Vec<u8>) -> IndexResult<Self> {
        // Minimum size: header(9) + crc(4).
        if data.len() < HEADER_SIZE + FOOTER_CRC_SIZE {
            return Err(IndexError::Corrupt(format!(
                "FTI too short: {} bytes (minimum {})",
                data.len(),
                HEADER_SIZE + FOOTER_CRC_SIZE
            )));
        }

        // Validate magic bytes.
        if &data[..4] != FTI_MAGIC.as_slice() {
            return Err(IndexError::Corrupt(
                "FTI magic bytes invalid".to_string(),
            ));
        }

        // Validate version.
        let version = data[4];
        if version != FTI_VERSION {
            return Err(IndexError::Corrupt(format!(
                "FTI unsupported version: {version}"
            )));
        }

        // Validate CRC32 footer.
        let payload_len = data.len() - FOOTER_CRC_SIZE;
        let stored_crc =
            u32::from_be_bytes(data[payload_len..].try_into().expect("4-byte slice"));
        let computed_crc = crc32fast::hash(&data[..payload_len]);
        if stored_crc != computed_crc {
            return Err(IndexError::Corrupt(format!(
                "FTI CRC32 mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
            )));
        }

        // Parse term count from header bytes 5..9.
        let term_count = u32::from_be_bytes(data[5..9].try_into().expect("4-byte slice")) as usize;

        // Parse term dictionary starting at HEADER_SIZE.
        let terms = parse_term_dictionary(&data, term_count)?;

        Ok(Self { data, terms })
    }

    /// Number of distinct terms in this index.
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }

    /// Look up a term and return its dictionary entry (doc frequency + offset).
    ///
    /// Returns `None` if the term is not present in the index.
    pub fn lookup_term(&self, term: &str) -> Option<&FtiTermEntry> {
        // Binary search since terms are stored in sorted order.
        self.terms
            .binary_search_by(|e| e.term.as_str().cmp(term))
            .ok()
            .map(|idx| &self.terms[idx])
    }

    /// Decode the postings list for a term entry.
    ///
    /// Returns an empty vec if `entry.doc_frequency == 0`.
    pub fn postings_for(&self, entry: &FtiTermEntry) -> IndexResult<Vec<FtiPosting>> {
        if entry.doc_frequency == 0 {
            return Ok(Vec::new());
        }

        let offset = entry.postings_offset as usize;
        decode_postings(&self.data, offset, entry.doc_frequency as usize)
    }

    /// Convenience: look up term and decode all postings in one call.
    ///
    /// Returns `None` if the term does not exist, or `Some(vec)` (possibly
    /// empty) if it does.
    pub fn search(&self, term: &str) -> IndexResult<Option<Vec<FtiPosting>>> {
        match self.lookup_term(term) {
            None => Ok(None),
            Some(entry) => {
                let postings = self.postings_for(entry)?;
                Ok(Some(postings))
            }
        }
    }

    /// Return references to all term entries.
    pub fn all_terms(&self) -> &[FtiTermEntry] {
        &self.terms
    }
}

// ── Deserialization helpers ───────────────────────────────────────────────────

fn parse_term_dictionary(data: &[u8], term_count: usize) -> IndexResult<Vec<FtiTermEntry>> {
    let mut entries = Vec::with_capacity(term_count);
    let mut offset = HEADER_SIZE;

    for _ in 0..term_count {
        // term_len: u32 BE
        if offset + 4 > data.len() {
            return Err(IndexError::Corrupt(format!(
                "FTI: EOF reading term_len at offset {offset}"
            )));
        }
        let term_len =
            u32::from_be_bytes(data[offset..offset + 4].try_into().expect("4-byte slice")) as usize;
        offset += 4;

        // term bytes
        if offset + term_len > data.len() {
            return Err(IndexError::Corrupt(format!(
                "FTI: EOF reading term bytes at offset {offset}, len {term_len}"
            )));
        }
        let term = std::str::from_utf8(&data[offset..offset + term_len])
            .map_err(|e| IndexError::Corrupt(format!("FTI: term not valid UTF-8: {e}")))?
            .to_string();
        offset += term_len;

        // doc_frequency: u32 BE
        if offset + 4 > data.len() {
            return Err(IndexError::Corrupt(format!(
                "FTI: EOF reading doc_frequency at offset {offset}"
            )));
        }
        let doc_frequency =
            u32::from_be_bytes(data[offset..offset + 4].try_into().expect("4-byte slice"));
        offset += 4;

        // postings_offset: u64 BE
        if offset + 8 > data.len() {
            return Err(IndexError::Corrupt(format!(
                "FTI: EOF reading postings_offset at offset {offset}"
            )));
        }
        let postings_offset =
            u64::from_be_bytes(data[offset..offset + 8].try_into().expect("8-byte slice"));
        offset += 8;

        entries.push(FtiTermEntry {
            term,
            doc_frequency,
            postings_offset,
        });
    }

    Ok(entries)
}

fn decode_postings(data: &[u8], mut offset: usize, count: usize) -> IndexResult<Vec<FtiPosting>> {
    // posting_count: u32 BE  (must match doc_frequency)
    if offset + 4 > data.len() {
        return Err(IndexError::Corrupt(format!(
            "FTI: EOF reading posting_count at offset {offset}"
        )));
    }
    let posting_count =
        u32::from_be_bytes(data[offset..offset + 4].try_into().expect("4-byte slice")) as usize;
    offset += 4;

    if posting_count != count {
        return Err(IndexError::Corrupt(format!(
            "FTI: posting_count {posting_count} != doc_frequency {count}"
        )));
    }

    let mut postings = Vec::with_capacity(count);
    for _ in 0..count {
        // pk_len: u32 BE
        if offset + 4 > data.len() {
            return Err(IndexError::Corrupt(format!(
                "FTI: EOF reading pk_len at offset {offset}"
            )));
        }
        let pk_len =
            u32::from_be_bytes(data[offset..offset + 4].try_into().expect("4-byte slice")) as usize;
        offset += 4;

        // pk bytes
        if offset + pk_len > data.len() {
            return Err(IndexError::Corrupt(format!(
                "FTI: EOF reading pk at offset {offset}, len {pk_len}"
            )));
        }
        let partition_key = data[offset..offset + pk_len].to_vec();
        offset += pk_len;

        // token: i64 BE
        if offset + 8 > data.len() {
            return Err(IndexError::Corrupt(format!(
                "FTI: EOF reading token at offset {offset}"
            )));
        }
        let token =
            i64::from_be_bytes(data[offset..offset + 8].try_into().expect("8-byte slice"));
        offset += 8;

        postings.push(FtiPosting {
            partition_key,
            token,
        });
    }

    Ok(postings)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fulltext::builder::{FullTextIndexBuilder, SimpleAnalyzer};

    #[test]
    fn reader_rejects_too_short() {
        let data = vec![0u8; 5];
        assert!(FullTextIndexReader::open(data).is_err());
    }

    #[test]
    fn reader_rejects_bad_magic() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 1, "hello");
        let mut data = builder.finish();
        data[0] = 0xFF; // corrupt magic
        let err = FullTextIndexReader::open(data).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("magic") || msg.contains("corrupt"),
            "expected magic error, got: {msg}"
        );
    }

    #[test]
    fn reader_rejects_bad_version() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 1, "hello");
        let mut data = builder.finish();
        data[4] = 99; // corrupt version (also corrupts CRC, but version check is first)
        // Recalculate CRC to isolate the version check.
        let payload_len = data.len() - FOOTER_CRC_SIZE;
        let crc = crc32fast::hash(&data[..payload_len]);
        data[payload_len..].copy_from_slice(&crc.to_be_bytes());
        let err = FullTextIndexReader::open(data).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("version") || msg.contains("corrupt"),
            "expected version error, got: {msg}"
        );
    }

    #[test]
    fn reader_rejects_crc_mismatch() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 1, "hello world");
        let mut data = builder.finish();
        // Flip a bit in the middle of the payload.
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        let err = FullTextIndexReader::open(data).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("CRC") || msg.contains("corrupt"),
            "expected CRC error, got: {msg}"
        );
    }

    #[test]
    fn reader_empty_index() {
        let builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        let data = builder.finish();
        let reader = FullTextIndexReader::open(data).unwrap();
        assert_eq!(reader.term_count(), 0);
        assert!(reader.lookup_term("anything").is_none());
    }

    #[test]
    fn reader_single_document_lookup() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 42, "hello world");
        let data = builder.finish();

        let reader = FullTextIndexReader::open(data).unwrap();
        assert_eq!(reader.term_count(), 2);

        let entry = reader.lookup_term("hello").expect("term 'hello' must exist");
        assert_eq!(entry.doc_frequency, 1);

        let postings = reader.postings_for(entry).unwrap();
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].partition_key, b"pk1");
        assert_eq!(postings[0].token, 42);
    }

    // ── FT-007: file format roundtrip + CRC tests ─────────────────────────────

    /// FT-007a: Full 10-document roundtrip. Verifies that every term from
    /// every document is present in the index with the correct doc_frequency.
    #[test]
    fn fts_file_format_roundtrip() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));

        // Add 10 documents. All share the word "document" and "number".
        for i in 0..10_u64 {
            let pk = format!("pk{i}");
            builder.add_document(
                pk.as_bytes(),
                i as i64,
                &format!("document number {i} with some words"),
            );
        }

        let data = builder.finish();
        let reader = FullTextIndexReader::open(data).unwrap();

        // "document" appears in all 10 docs.
        let entry = reader
            .lookup_term("document")
            .expect("term 'document' must exist");
        assert_eq!(
            entry.doc_frequency, 10,
            "expected doc_frequency=10 for 'document'"
        );

        let postings = reader.postings_for(entry).unwrap();
        assert_eq!(postings.len(), 10);

        // "number" also appears in all 10 docs.
        let entry = reader
            .lookup_term("number")
            .expect("term 'number' must exist");
        assert_eq!(entry.doc_frequency, 10);

        // "words" appears in all 10 docs.
        let entry = reader
            .lookup_term("words")
            .expect("term 'words' must exist");
        assert_eq!(entry.doc_frequency, 10);

        // A non-existent term returns None.
        assert!(reader.lookup_term("nonexistent").is_none());
    }

    /// FT-007b: Corrupt a byte in the payload and verify CRC detection.
    #[test]
    fn fts_sidecar_checksum_verified() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 1, "hello world");
        let mut data = builder.finish();

        // Corrupt one byte in the middle of the payload (not the CRC itself).
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;

        let result = FullTextIndexReader::open(data);
        assert!(result.is_err(), "corrupted FTI must be rejected");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("CRC") || msg.contains("corrupt"),
            "expected CRC/corrupt error, got: {msg}"
        );
    }

    /// FT-007c: Corrupt the last byte (inside the CRC field) and verify detection.
    #[test]
    fn fts_corrupt_crc_field_detected() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 1, "hello");
        let mut data = builder.finish();

        // Flip a bit in the CRC footer itself.
        let last = data.len() - 1;
        data[last] ^= 0x01;

        let result = FullTextIndexReader::open(data);
        assert!(result.is_err(), "corrupted CRC field must be rejected");
    }

    /// FT-007d: search() convenience function works end-to-end.
    #[test]
    fn fts_search_convenience() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 100, "rust programming language");
        builder.add_document(b"pk2", 200, "rust is great");
        let data = builder.finish();

        let reader = FullTextIndexReader::open(data).unwrap();

        let postings = reader
            .search("rust")
            .unwrap()
            .expect("'rust' must be found");
        assert_eq!(postings.len(), 2, "expected 2 postings for 'rust'");

        let result = reader.search("python").unwrap();
        assert!(result.is_none(), "missing term must return None");
    }

    /// FT-007e: Multi-document, multi-term, verify all_terms() ordering.
    #[test]
    fn fts_terms_are_sorted() {
        let mut builder = FullTextIndexBuilder::new(Box::new(SimpleAnalyzer));
        builder.add_document(b"pk1", 1, "zebra apple mango");
        let data = builder.finish();
        let reader = FullTextIndexReader::open(data).unwrap();

        let terms: Vec<&str> = reader.all_terms().iter().map(|e| e.term.as_str()).collect();
        // Terms must be in ascending lexicographic order.
        let mut sorted = terms.clone();
        sorted.sort();
        assert_eq!(terms, sorted, "terms must be sorted lexicographically");
    }
}
