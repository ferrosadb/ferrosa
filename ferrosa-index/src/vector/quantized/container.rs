//! Binary `.qvec` container skeleton for quantized vector pages.
//!
//! Module: Parse and validate the versioned HVQ `.qvec` envelope before any ANN reader consumes page bytes.
//! Correctness: Correct when bad magic, unsupported versions, checksum mismatches, short reads, and out-of-bounds page-table entries all fail loudly with typed errors.
//! Last revised: 2026-05-29
//! Last changed: Added RED tests and a deliberately incomplete parser skeleton.

use crate::{IndexError, IndexResult};

/// Magic bytes for Ferrosa quantized-vector artifacts.
pub const QVEC_MAGIC: &[u8; 4] = b"QVEC";

/// First supported `.qvec` container format version.
pub const QVEC_VERSION_V1: u16 = 1;

/// A decoded `.qvec` container header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QvecHeader {
    /// Container format version.
    pub version: u16,
    /// Number of page-table entries.
    pub page_count: u32,
    /// Byte offset where the page table starts.
    pub page_table_offset: u64,
    /// Byte length of the page table.
    pub page_table_len: u64,
    /// Byte length of the artifact payload region after the page table.
    pub payload_len: u64,
    /// CRC32 checksum over the raw page table bytes.
    pub page_table_crc32: u32,
}

/// One range-readable page in a `.qvec` artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QvecPageEntry {
    /// Absolute byte offset of this page in the artifact.
    pub offset: u64,
    /// Byte length of this page.
    pub len: u32,
    /// Algorithm/tier-specific page kind.
    pub kind: u8,
}

/// A validated `.qvec` envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QvecContainer {
    /// Decoded fixed header.
    pub header: QvecHeader,
    /// Decoded page table entries.
    pub pages: Vec<QvecPageEntry>,
}

impl QvecContainer {
    /// Decode and validate a `.qvec` artifact envelope.
    ///
    /// This is intentionally a RED-phase skeleton: it defines the public seam so
    /// corruption tests compile, but the full parser/validator belongs to the
    /// follow-up GREEN implementation card.
    pub fn decode(_bytes: &[u8]) -> IndexResult<Self> {
        Err(IndexError::Unsupported(
            ".qvec container parser is not implemented yet".to_string(),
        ))
    }
}

#[cfg(test)]
mod quantized_container_tests {
    use super::*;

    const HEADER_LEN: usize = 40;
    const PAGE_ENTRY_LEN: usize = 16;

    fn valid_qvec_bytes() -> Vec<u8> {
        let payload = [0xaa, 0xbb, 0xcc, 0xdd];
        let page_table_offset = HEADER_LEN as u64;
        let page_table_len = PAGE_ENTRY_LEN as u64;
        let page_offset = page_table_offset + page_table_len;

        let mut page_table = Vec::new();
        page_table.extend_from_slice(&page_offset.to_le_bytes());
        page_table.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        page_table.push(7);
        page_table.extend_from_slice(&[0, 0, 0]);

        let page_table_crc = crc32fast::hash(&page_table);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(QVEC_MAGIC);
        bytes.extend_from_slice(&QVEC_VERSION_V1.to_le_bytes());
        bytes.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&page_table_offset.to_le_bytes());
        bytes.extend_from_slice(&page_table_len.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&page_table_crc.to_le_bytes());
        bytes.extend_from_slice(&page_table);
        bytes.extend_from_slice(&payload);
        bytes
    }

    fn assert_corrupt_contains(result: IndexResult<QvecContainer>, expected: &str) {
        match result {
            Err(IndexError::Corrupt(message)) => assert!(
                message.contains(expected),
                "expected corrupt error containing {expected:?}, got {message:?}"
            ),
            other => panic!("expected corrupt error containing {expected:?}, got {other:?}"),
        }
    }

    #[test]
    fn quantized_container_rejects_bad_magic() {
        let mut bytes = valid_qvec_bytes();
        bytes[..4].copy_from_slice(b"NOPE");

        assert_corrupt_contains(QvecContainer::decode(&bytes), "bad .qvec magic");
    }

    #[test]
    fn quantized_container_rejects_unsupported_version() {
        let mut bytes = valid_qvec_bytes();
        bytes[4..6].copy_from_slice(&2_u16.to_le_bytes());

        match QvecContainer::decode(&bytes) {
            Err(IndexError::Unsupported(message)) => assert!(
                message.contains("unsupported .qvec version 2"),
                "expected unsupported version error, got {message:?}"
            ),
            other => panic!("expected unsupported version error, got {other:?}"),
        }
    }

    #[test]
    fn quantized_container_rejects_page_table_checksum_mismatch() {
        let mut bytes = valid_qvec_bytes();
        bytes[HEADER_LEN] ^= 0xff;

        assert_corrupt_contains(
            QvecContainer::decode(&bytes),
            "page table checksum mismatch",
        );
    }

    #[test]
    fn quantized_container_rejects_short_read() {
        let mut bytes = valid_qvec_bytes();
        bytes.truncate(HEADER_LEN + PAGE_ENTRY_LEN - 1);

        assert_corrupt_contains(QvecContainer::decode(&bytes), "short .qvec read");
    }

    #[test]
    fn quantized_container_rejects_out_of_bounds_page_table_entry() {
        let mut bytes = valid_qvec_bytes();
        let impossible_page_offset = bytes.len() as u64 + 64;
        bytes[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&impossible_page_offset.to_le_bytes());
        let checksum = crc32fast::hash(&bytes[HEADER_LEN..HEADER_LEN + PAGE_ENTRY_LEN]);
        bytes[HEADER_LEN - 4..HEADER_LEN].copy_from_slice(&checksum.to_le_bytes());

        assert_corrupt_contains(
            QvecContainer::decode(&bytes),
            "page table entry out of bounds",
        );
    }
}
