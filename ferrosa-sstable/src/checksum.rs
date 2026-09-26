//! Source checksums for SSTable Data.db: `Digest.crc32` (all tables) and
//! `CRC.db` (uncompressed tables only).
//!
//! Both formats mirror Apache Cassandra's on-disk layout so a ferrosa-written
//! SSTable carries the same integrity metadata a Cassandra-written one would.
//! See `publication-safety.md` M1 (`ferrosa-suite/specs/sstable-write-pump/`)
//! for why: uncompressed Data.db previously had no checksum at all, so a
//! stale-bytes-right-length corruption (segment recycled before its write
//! completed) was undetectable until it produced a decode error, or not at
//! all.
//!
//! ## `Digest.crc32`
//!
//! A single CRC32 (`java.util.zip.CRC32`, IEEE 802.3 / CRC-32 polynomial —
//! the same polynomial `crc32fast` implements) computed over every byte
//! written to Data.db **as it exists on disk**: for a compressed table that
//! is the compressed chunk payloads and their per-chunk CRC32 trailers, not
//! the uncompressed logical content. The file holds the checksum as a
//! decimal ASCII string with no trailing newline.
//!
//! Reference: Apache Cassandra's `SortedTableWriter` writes `Component.DIGEST`
//! from the running checksum kept by the underlying `SequentialWriter`
//! (`ChecksummedSequentialWriter` for uncompressed tables,
//! `CompressedSequentialWriter` for compressed ones);
//! `org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumWriter.writeFullChecksum`
//! is what produces the digest file, as `String.valueOf(long)` written as
//! UTF-8 bytes. **No line citation**: this checkout's vendored `cassandra/`
//! submodule (`cassandra/README.md`) carries only the CQL example corpus, not
//! the Java sources, so the class names above are cited without line numbers.
//! The format itself is Cassandra's long-stable on-disk contract — any
//! `nodetool verify` / `sstableverify` build reads exactly this file.
//!
//! ## `CRC.db`
//!
//! Present only for uncompressed tables. Layout:
//!
//! ```text
//! [0..4)   u32 big-endian: chunk size in bytes (the block each CRC covers)
//! [4..8)   u32 big-endian: CRC32 of Data.db bytes [0, chunk_size)
//! [8..12)  u32 big-endian: CRC32 of Data.db bytes [chunk_size, 2*chunk_size)
//! ...      one 4-byte big-endian CRC32 per subsequent chunk_size block; the
//!          final entry covers whatever remains (0 < len <= chunk_size)
//! ```
//!
//! Reference: `org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumWriter`
//! — `writeChunkSize` writes the one-time header int, and `append(ByteBuffer)`
//! is called once per buffer flush by `ChecksummedSequentialWriter`, writing
//! one big-endian CRC32 per chunk (same class as the `Digest.crc32` citation
//! above; same caveat about line numbers). Ferrosa uses the table's configured
//! `WriteOptions.chunk_size` (default 65536) as the CRC.db chunk size, which
//! matches Cassandra's default `CompressionParams.DEFAULT_CHUNK_LENGTH`
//! (also 65536), so an uncompressed and a compressed table written with
//! default settings checksum over identically sized blocks.

use ferrosa_common::{Error, Result};

/// Streaming CRC32 over the exact bytes handed to Data.db, producing the
/// `Digest.crc32` value.
#[derive(Debug, Default, Clone)]
pub struct DigestCrc32 {
    hasher: crc32fast::Hasher,
}

impl DigestCrc32 {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed the next bytes written to Data.db.
    pub fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    /// Consume the accumulator and return the final CRC32 value.
    pub fn finalize(self) -> u32 {
        self.hasher.finalize()
    }
}

/// One-shot digest over a byte slice already fully in memory. Equivalent to
/// `crc32fast::hash`, exposed here so callers depend on this module's
/// documented Cassandra-compatibility contract rather than the raw crate.
pub fn digest_bytes(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Render a CRC32 value as the `Digest.crc32` file content: a decimal ASCII
/// string, no trailing newline.
pub fn format_digest(value: u32) -> Vec<u8> {
    value.to_string().into_bytes()
}

/// Parse a `Digest.crc32` file's content back into its CRC32 value.
pub fn parse_digest(bytes: &[u8]) -> Result<u32> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| Error::InvalidFormat(format!("Digest.crc32 is not valid UTF-8: {e}")))?;
    text.trim()
        .parse::<u32>()
        .map_err(|e| Error::InvalidFormat(format!("Digest.crc32 is not a decimal u32: {e}")))
}

/// Streaming per-chunk CRC32 accumulator producing Cassandra's `CRC.db`
/// layout for uncompressed Data.db. See the module docs for the byte layout.
pub struct ChunkCrc {
    chunk_size: u32,
    current: crc32fast::Hasher,
    current_len: usize,
    chunks: Vec<u32>,
}

impl ChunkCrc {
    pub fn new(chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "CRC.db chunk_size must be non-zero");
        Self {
            chunk_size: chunk_size as u32,
            current: crc32fast::Hasher::new(),
            current_len: 0,
            chunks: Vec::new(),
        }
    }

    /// Feed the next bytes written to Data.db. `bytes` need not align to
    /// chunk boundaries — a single call may close out one chunk and start
    /// the next.
    pub fn update(&mut self, mut bytes: &[u8]) {
        let chunk_size = self.chunk_size as usize;
        while !bytes.is_empty() {
            let remaining_in_chunk = chunk_size - self.current_len;
            let take = remaining_in_chunk.min(bytes.len());
            self.current.update(&bytes[..take]);
            self.current_len += take;
            bytes = &bytes[take..];
            if self.current_len == chunk_size {
                self.close_chunk();
            }
        }
    }

    fn close_chunk(&mut self) {
        let hasher = std::mem::replace(&mut self.current, crc32fast::Hasher::new());
        self.chunks.push(hasher.finalize());
        self.current_len = 0;
    }

    /// Finish, flushing a final partial chunk if any bytes remain, and
    /// return the CRC.db bytes.
    pub fn into_bytes(mut self) -> Vec<u8> {
        if self.current_len > 0 {
            self.close_chunk();
        }
        let mut out = Vec::with_capacity(4 + 4 * self.chunks.len());
        out.extend_from_slice(&self.chunk_size.to_be_bytes());
        for crc in &self.chunks {
            out.extend_from_slice(&crc.to_be_bytes());
        }
        out
    }
}

/// One-shot CRC.db builder over a byte slice already fully in memory.
pub fn compute_chunk_crc(data: &[u8], chunk_size: usize) -> Vec<u8> {
    let mut crc = ChunkCrc::new(chunk_size);
    crc.update(data);
    crc.into_bytes()
}

/// Parsed `CRC.db` content: the chunk size and the per-chunk CRC32 table,
/// used by the reader to verify uncompressed Data.db chunks on read.
#[derive(Debug, Clone)]
pub struct ChunkCrcTable {
    pub chunk_size: u32,
    pub crcs: Vec<u32>,
}

impl ChunkCrcTable {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(Error::InvalidFormat(
                "CRC.db shorter than its chunk-size header".into(),
            ));
        }
        let chunk_size = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if chunk_size == 0 {
            return Err(Error::InvalidFormat("CRC.db chunk size is zero".into()));
        }
        let rest = &bytes[4..];
        if !rest.len().is_multiple_of(4) {
            return Err(Error::InvalidFormat(
                "CRC.db chunk CRC table is not a whole number of 4-byte entries".into(),
            ));
        }
        let crcs = rest
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| u32::from_be_bytes(*c))
            .collect();
        Ok(Self { chunk_size, crcs })
    }

    /// Number of chunks a Data.db of `data_len` bytes should have, given
    /// this table's chunk size.
    pub fn expected_chunk_count(&self, data_len: u64) -> u64 {
        data_len.div_ceil(self.chunk_size as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn checksum_digest_format_roundtrip() {
        let bytes = format_digest(0xDEADBEEF);
        assert_eq!(bytes, b"3735928559");
        assert_eq!(parse_digest(&bytes).unwrap(), 0xDEADBEEF);
    }

    #[test]
    fn checksum_digest_streaming_matches_one_shot() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let mut streaming = DigestCrc32::new();
        streaming.update(&data[..10]);
        streaming.update(&data[10..]);
        assert_eq!(streaming.finalize(), digest_bytes(data));
    }

    #[test]
    fn checksum_chunk_crc_layout_header() {
        let bytes = compute_chunk_crc(b"", 100);
        // Empty input: header only, zero chunk entries.
        assert_eq!(bytes.len(), 4);
        assert_eq!(
            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            100
        );
    }

    #[test]
    fn checksum_chunk_crc_single_full_chunk() {
        let data = vec![0xABu8; 16];
        let bytes = compute_chunk_crc(&data, 16);
        assert_eq!(bytes.len(), 4 + 4);
        let table = ChunkCrcTable::parse(&bytes).unwrap();
        assert_eq!(table.chunk_size, 16);
        assert_eq!(table.crcs, vec![crc32fast::hash(&data)]);
    }

    #[test]
    fn checksum_chunk_crc_matches_per_chunk_hash() {
        let chunk_size = 8usize;
        let data: Vec<u8> = (0u8..=200).collect();
        let bytes = compute_chunk_crc(&data, chunk_size);
        let table = ChunkCrcTable::parse(&bytes).unwrap();
        let expected: Vec<u32> = data.chunks(chunk_size).map(crc32fast::hash).collect();
        assert_eq!(table.crcs, expected);
        assert_eq!(
            table.expected_chunk_count(data.len() as u64),
            expected.len() as u64
        );
    }

    #[test]
    fn checksum_chunk_crc_streaming_matches_one_shot_regardless_of_split() {
        let chunk_size = 10usize;
        let data: Vec<u8> = (0u8..=250).collect();
        let one_shot = compute_chunk_crc(&data, chunk_size);

        // Feed in awkward, non-chunk-aligned pieces.
        let mut streaming = ChunkCrc::new(chunk_size);
        for piece in data.chunks(3) {
            streaming.update(piece);
        }
        let streamed = streaming.into_bytes();
        assert_eq!(streamed, one_shot);
    }

    #[test]
    fn checksum_parse_rejects_short_header() {
        assert!(ChunkCrcTable::parse(&[1, 2, 3]).is_err());
    }

    #[test]
    fn checksum_parse_rejects_zero_chunk_size() {
        assert!(ChunkCrcTable::parse(&[0, 0, 0, 0]).is_err());
    }

    #[test]
    fn checksum_parse_rejects_truncated_crc_entry() {
        let mut bytes = vec![0, 0, 0, 16];
        bytes.extend_from_slice(&[1, 2, 3]); // 3 bytes, not a full u32
        assert!(ChunkCrcTable::parse(&bytes).is_err());
    }

    proptest! {
        #[test]
        fn checksum_chunk_crc_roundtrips_for_arbitrary_sizes(
            data in proptest::collection::vec(any::<u8>(), 0..(3 * 64 + 17)),
            chunk_size in 1usize..64,
        ) {
            let bytes = compute_chunk_crc(&data, chunk_size);
            let table = ChunkCrcTable::parse(&bytes).unwrap();
            prop_assert_eq!(table.chunk_size as usize, chunk_size);
            let expected_count = if data.is_empty() { 0 } else { data.chunks(chunk_size).count() };
            prop_assert_eq!(table.crcs.len(), expected_count);
            let expected: Vec<u32> = data.chunks(chunk_size).map(crc32fast::hash).collect();
            prop_assert_eq!(table.crcs, expected);
        }

        #[test]
        fn checksum_digest_matches_crc32fast_for_arbitrary_data(
            data in proptest::collection::vec(any::<u8>(), 0..1000),
        ) {
            prop_assert_eq!(digest_bytes(&data), crc32fast::hash(&data));
        }
    }
}
