//! Full-text inverted index: FTI sidecar file format.
//!
//! The FTI (Ferrosa Text Index) format stores an inverted index mapping
//! terms to posting lists. Each posting records the partition key, token
//! (Murmur3 of the partition key), and row position within the SSTable.
//!
//! ## File layout
//!
//! ```text
//! +------- Header (9 bytes) --------+
//! | magic:   b"FTI\x01" (4 bytes)  |
//! | version: u8         (1 byte)    |
//! | term_count: u32 BE  (4 bytes)   |
//! +---------------------------------+
//! | Term dictionary                 |
//! |  for each term (term_count):    |
//! |    term_len: u32 BE             |
//! |    term_bytes: [u8]             |
//! |    doc_frequency: u32 BE        |
//! |    postings_offset: u64 BE      | <- byte offset into postings section
//! +---------------------------------+
//! | Postings section                |
//! |  for each posting list:         |
//! |    posting_count: u32 BE        |
//! |    for each posting:            |
//! |      pk_len: u32 BE             |
//! |      pk_bytes: [u8]             |
//! |      token: i64 BE              |
//! +---------------------------------+
//! | CRC32 footer (4 bytes, BE)      | <- CRC32 of all preceding bytes
//! +---------------------------------+
//! ```
//!
//! The CRC32 footer covers every byte from the start of the file up to
//! (but not including) the 4-byte CRC field itself.

pub mod builder;
pub mod reader;

pub use builder::{FullTextIndexBuilder, SimpleAnalyzer, TextAnalyzer};
pub use reader::{FtiTermEntry, FullTextIndexReader};

/// Magic bytes for FTI files: ASCII "FTI" + version byte 0x01.
pub const FTI_MAGIC: &[u8; 4] = b"FTI\x01";

/// Current FTI format version.
pub const FTI_VERSION: u8 = 1;

/// Size of the fixed-length header: magic(4) + version(1) + term_count(4) = 9.
pub const HEADER_SIZE: usize = 9;

/// Size of the CRC32 footer appended at the end of every FTI file.
pub const FOOTER_CRC_SIZE: usize = 4;
