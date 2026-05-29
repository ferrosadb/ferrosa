//! Binary `.qvec` container support for quantized vector pages.
//!
//! Module: Parse, validate, encode, and page-range-read the versioned HVQ `.qvec` envelope before any ANN reader consumes page bytes.
//! Correctness: Correct when golden containers round-trip, malformed envelopes fail loudly with typed errors, and page reads request only the selected byte range.
//! Last revised: 2026-05-29
//! Last changed: Implemented the v1 manifest, page table checksum validation, and exact range-read page access.

use std::ops::Range;

use crate::{IndexError, IndexResult};

/// Magic bytes for Ferrosa quantized-vector artifacts.
pub const QVEC_MAGIC: &[u8; 4] = b"QVEC";

/// First supported `.qvec` container format version.
pub const QVEC_VERSION_V1: u16 = 1;

const QVEC_HEADER_LEN: usize = 40;
const QVEC_PAGE_ENTRY_LEN: usize = 16;

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

/// Reads bounded byte ranges from a `.qvec` artifact.
pub trait QvecRangeReader {
    /// Return bytes for exactly `range`.
    fn read_range(&self, range: Range<u64>) -> IndexResult<Vec<u8>>;
}

/// A validated `.qvec` envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QvecContainer {
    /// Decoded fixed header.
    pub header: QvecHeader,
    /// Decoded page table entries.
    pub pages: Vec<QvecPageEntry>,
    payload: Vec<u8>,
}

impl QvecContainer {
    /// Decode and validate a `.qvec` artifact envelope.
    pub fn decode(bytes: &[u8]) -> IndexResult<Self> {
        ensure_len(bytes.len(), QVEC_HEADER_LEN, "header")?;
        if &bytes[..4] != QVEC_MAGIC {
            return corrupt("bad .qvec magic");
        }

        let version = read_u16(bytes, 4)?;
        if version != QVEC_VERSION_V1 {
            return Err(IndexError::Unsupported(format!(
                "unsupported .qvec version {version}"
            )));
        }

        let header_len = read_u16(bytes, 6)? as usize;
        if header_len < QVEC_HEADER_LEN {
            return corrupt(format!(
                "invalid .qvec header length {header_len}; expected at least {QVEC_HEADER_LEN}"
            ));
        }
        ensure_len(bytes.len(), header_len, "declared header")?;

        let page_count = read_u32(bytes, 8)?;
        let page_table_offset = read_u64(bytes, 12)?;
        let page_table_len = read_u64(bytes, 20)?;
        let payload_len = read_u64(bytes, 28)?;
        let page_table_crc32 = read_u32(bytes, 36)?;

        if page_table_offset < header_len as u64 {
            return corrupt("invalid .qvec page table offset before header end");
        }

        let expected_page_table_len = (page_count as u64)
            .checked_mul(QVEC_PAGE_ENTRY_LEN as u64)
            .ok_or_else(|| IndexError::Corrupt(".qvec page table length overflow".to_string()))?;
        if page_table_len != expected_page_table_len {
            return corrupt(format!(
                "invalid .qvec page table length {page_table_len}; expected {expected_page_table_len}"
            ));
        }

        let page_table_end = checked_end(page_table_offset, page_table_len, "page table")?;
        let payload_end = checked_end(page_table_end, payload_len, "payload")?;
        ensure_len(bytes.len(), page_table_end as usize, "page table")?;
        ensure_len(bytes.len(), payload_end as usize, "payload")?;

        let page_table_start = page_table_offset as usize;
        let page_table_end_usize = page_table_end as usize;
        let page_table = &bytes[page_table_start..page_table_end_usize];
        let actual_crc = crc32fast::hash(page_table);
        if actual_crc != page_table_crc32 {
            return corrupt(format!(
                "page table checksum mismatch: expected {page_table_crc32:#010x}, got {actual_crc:#010x}"
            ));
        }

        let artifact_len = payload_end;
        let mut pages = Vec::with_capacity(page_count as usize);
        for (index, entry) in page_table.chunks_exact(QVEC_PAGE_ENTRY_LEN).enumerate() {
            let offset = read_u64(entry, 0)?;
            let len = read_u32(entry, 8)?;
            let kind = entry[12];
            if entry[13..16] != [0, 0, 0] {
                return corrupt(format!(
                    "invalid .qvec page table reserved bytes at entry {index}"
                ));
            }
            let end = checked_end(offset, len as u64, "page table entry")?;
            if offset < page_table_end || end > artifact_len {
                return corrupt(format!(
                    "page table entry out of bounds at index {index}: {offset}..{end} outside {page_table_end}..{artifact_len}"
                ));
            }
            pages.push(QvecPageEntry { offset, len, kind });
        }

        let payload = bytes[page_table_end_usize..payload_end as usize].to_vec();
        Ok(Self {
            header: QvecHeader {
                version,
                page_count,
                page_table_offset,
                page_table_len,
                payload_len,
                page_table_crc32,
            },
            pages,
            payload,
        })
    }

    /// Encode this container into the canonical v1 binary layout.
    pub fn encode(&self) -> Vec<u8> {
        let page_table_offset = QVEC_HEADER_LEN as u64;
        let mut page_table = Vec::with_capacity(self.pages.len() * QVEC_PAGE_ENTRY_LEN);
        for page in &self.pages {
            page_table.extend_from_slice(&page.offset.to_le_bytes());
            page_table.extend_from_slice(&page.len.to_le_bytes());
            page_table.push(page.kind);
            page_table.extend_from_slice(&[0, 0, 0]);
        }
        let page_table_crc32 = crc32fast::hash(&page_table);

        let mut bytes = Vec::with_capacity(QVEC_HEADER_LEN + page_table.len() + self.payload.len());
        bytes.extend_from_slice(QVEC_MAGIC);
        bytes.extend_from_slice(&QVEC_VERSION_V1.to_le_bytes());
        bytes.extend_from_slice(&(QVEC_HEADER_LEN as u16).to_le_bytes());
        bytes.extend_from_slice(&(self.pages.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&page_table_offset.to_le_bytes());
        bytes.extend_from_slice(&(page_table.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&page_table_crc32.to_le_bytes());
        bytes.extend_from_slice(&page_table);
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    /// Read a single page by index without materializing the whole artifact.
    pub fn read_page(
        &self,
        reader: &impl QvecRangeReader,
        page_index: usize,
    ) -> IndexResult<Vec<u8>> {
        let page = self
            .pages
            .get(page_index)
            .ok_or_else(|| IndexError::Corrupt(format!("missing .qvec page range {page_index}")))?;
        let end = checked_end(page.offset, page.len as u64, "page range")?;
        reader.read_range(page.offset..end)
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> IndexResult<u16> {
    ensure_len(bytes.len(), offset + 2, "u16 field")?;
    Ok(u16::from_le_bytes([bytes[offset], bytes[offset + 1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> IndexResult<u32> {
    ensure_len(bytes.len(), offset + 4, "u32 field")?;
    Ok(u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]))
}

fn read_u64(bytes: &[u8], offset: usize) -> IndexResult<u64> {
    ensure_len(bytes.len(), offset + 8, "u64 field")?;
    Ok(u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ]))
}

fn ensure_len(actual: usize, required: usize, context: &str) -> IndexResult<()> {
    if actual < required {
        return corrupt(format!(
            "short .qvec read for {context}: need {required} bytes, got {actual}"
        ));
    }
    Ok(())
}

fn checked_end(start: u64, len: u64, context: &str) -> IndexResult<u64> {
    start
        .checked_add(len)
        .ok_or_else(|| IndexError::Corrupt(format!(".qvec {context} range overflow")))
}

fn corrupt<T>(message: impl Into<String>) -> IndexResult<T> {
    Err(IndexError::Corrupt(message.into()))
}

#[cfg(test)]
mod quantized_container_tests {
    use super::*;
    use std::cell::RefCell;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::{Path, PathBuf};

    const HEADER_LEN: usize = 40;
    const PAGE_ENTRY_LEN: usize = 16;

    struct RecordingFileRangeReader {
        path: PathBuf,
        requested_ranges: RefCell<Vec<Range<u64>>>,
    }

    impl RecordingFileRangeReader {
        fn open(path: impl AsRef<Path>) -> IndexResult<Self> {
            Ok(Self {
                path: path.as_ref().to_path_buf(),
                requested_ranges: RefCell::new(Vec::new()),
            })
        }

        fn requested_ranges(&self) -> Vec<Range<u64>> {
            self.requested_ranges.borrow().clone()
        }
    }

    impl QvecRangeReader for RecordingFileRangeReader {
        fn read_range(&self, range: Range<u64>) -> IndexResult<Vec<u8>> {
            self.requested_ranges.borrow_mut().push(range.clone());
            let len = range.end.checked_sub(range.start).ok_or_else(|| {
                IndexError::Corrupt(format!(
                    "invalid .qvec page range {}..{}",
                    range.start, range.end
                ))
            })?;
            let mut file = std::fs::File::open(&self.path)?;
            file.seek(SeekFrom::Start(range.start))?;
            let mut bytes = vec![0; len as usize];
            file.read_exact(&mut bytes).map_err(|err| {
                IndexError::Corrupt(format!(
                    "short .qvec read for requested range {}..{}: {err}",
                    range.start, range.end
                ))
            })?;
            Ok(bytes)
        }
    }

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

    fn assert_corrupt_contains<T: std::fmt::Debug>(result: IndexResult<T>, expected: &str) {
        match result {
            Err(IndexError::Corrupt(message)) => assert!(
                message.contains(expected),
                "expected corrupt error containing {expected:?}, got {message:?}"
            ),
            other => panic!("expected corrupt error containing {expected:?}, got {other:?}"),
        }
    }

    #[test]
    fn quantized_container_decodes_and_reencodes_golden_bytes() {
        let bytes = valid_qvec_bytes();

        let container = QvecContainer::decode(&bytes).expect("golden .qvec decodes");

        assert_eq!(container.header.version, QVEC_VERSION_V1);
        assert_eq!(container.header.page_count, 1);
        assert_eq!(
            container.pages,
            vec![QvecPageEntry {
                offset: 56,
                len: 4,
                kind: 7,
            }]
        );
        assert_eq!(container.encode(), bytes);
    }

    #[test]
    fn quantized_container_reads_exact_page_range_from_file() {
        let bytes = valid_qvec_bytes();
        let container = QvecContainer::decode(&bytes).expect("golden .qvec decodes");
        let artifact = tempfile::NamedTempFile::new().expect("temp qvec file");
        std::fs::write(artifact.path(), &bytes).expect("write qvec artifact");
        let reader = RecordingFileRangeReader::open(artifact.path()).expect("open range reader");

        let page = container.read_page(&reader, 0).expect("page range read");

        assert_eq!(page, vec![0xaa, 0xbb, 0xcc, 0xdd]);
        assert_eq!(reader.requested_ranges(), vec![56..60]);
    }

    #[test]
    fn quantized_container_rejects_missing_page_range() {
        let bytes = valid_qvec_bytes();
        let container = QvecContainer::decode(&bytes).expect("golden .qvec decodes");
        let artifact = tempfile::NamedTempFile::new().expect("temp qvec file");
        std::fs::write(artifact.path(), &bytes).expect("write qvec artifact");
        let reader = RecordingFileRangeReader::open(artifact.path()).expect("open range reader");

        assert_corrupt_contains(
            container.read_page(&reader, 1),
            "missing .qvec page range 1",
        );
        assert_eq!(
            reader.requested_ranges(),
            Vec::<std::ops::Range<u64>>::new()
        );
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
