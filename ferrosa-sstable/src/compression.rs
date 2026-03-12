//! Compression and CompressionInfo for SSTable data chunks.
//!
//! SSTable data is stored in compressed chunks (default 16KB uncompressed).
//! Chunks are position-based — every N uncompressed bytes, regardless of
//! partition boundaries.
//!
//! Supported algorithms:
//! - **LZ4** (`lz4_flex`): Default. Fast compression/decompression.
//! - **Zstd** (`zstd`): Better ratio, slightly slower.
//! - **None**: Uncompressed. Uses CRC.db instead of CompressionInfo.db.
//!
//! # CompressionInfo Format
//!
//! See `specs/sstable.md` § Compression Info for the full byte-level format.
//! Key fields: compressor name (Java UTF-8), chunk length, data length,
//! chunk offsets (i64 array).
//!
//! Reference: `org.apache.cassandra.io.compress.CompressionMetadata`

use ferrosa_common::{Error, Result};

/// Compression algorithm selection.
#[derive(Debug, Clone, PartialEq)]
pub enum Compression {
    /// No compression. CRC.db written instead of CompressionInfo.db.
    None,
    /// LZ4 compression (default in Cassandra).
    Lz4,
    /// Zstd compression with configurable level.
    Zstd { level: i32 },
}

impl Compression {
    /// Default chunk size matching Cassandra's DEFAULT_CHUNK_LENGTH.
    pub const DEFAULT_CHUNK_SIZE: usize = 16384;

    /// Compress a block of data.
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            Compression::None => Ok(data.to_vec()),
            Compression::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
            Compression::Zstd { level } => zstd::encode_all(data, *level).map_err(Error::Io),
        }
    }

    /// Decompress a block of data.
    pub fn decompress(&self, data: &[u8], _uncompressed_len: usize) -> Result<Vec<u8>> {
        match self {
            Compression::None => Ok(data.to_vec()),
            Compression::Lz4 => lz4_flex::decompress_size_prepended(data)
                .map_err(|e| Error::InvalidData(format!("LZ4 decompression failed: {e}"))),
            Compression::Zstd { .. } => zstd::decode_all(data).map_err(Error::Io),
        }
    }

    /// Returns the compressor name as stored in CompressionInfo.db.
    pub fn compressor_name(&self) -> Option<&str> {
        match self {
            Compression::None => None,
            Compression::Lz4 => Some("LZ4Compressor"),
            Compression::Zstd { .. } => Some("ZstdCompressor"),
        }
    }

    /// Parse a compressor name from CompressionInfo.db.
    pub fn from_compressor_name(name: &str) -> Result<Self> {
        match name {
            "LZ4Compressor" => Ok(Compression::Lz4),
            "ZstdCompressor" => Ok(Compression::Zstd { level: 3 }),
            "SnappyCompressor" | "DeflateCompressor" => {
                Err(Error::UnsupportedCompression(name.into()))
            }
            other => Err(Error::UnsupportedCompression(other.into())),
        }
    }
}

/// Parsed CompressionInfo.db metadata.
#[derive(Debug, Clone)]
pub struct CompressionInfo {
    /// Compression algorithm.
    pub compression: Compression,
    /// Uncompressed chunk size in bytes.
    pub chunk_length: usize,
    /// Maximum compressed chunk size.
    pub max_compressed_size: usize,
    /// Total uncompressed data length.
    pub data_length: u64,
    /// File offset of each compressed chunk in Data.db.
    pub chunk_offsets: Vec<u64>,
}

impl CompressionInfo {
    /// Read CompressionInfo from a byte buffer.
    pub fn read(data: &[u8]) -> Result<Self> {
        let mut pos = 0;

        // Compressor name: Java UTF-8 (u16 len + bytes)
        if data.len() < pos + 2 {
            return Err(Error::InvalidFormat(
                "truncated compressor name length".into(),
            ));
        }
        let name_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        if data.len() < pos + name_len {
            return Err(Error::InvalidFormat("truncated compressor name".into()));
        }
        let name = std::str::from_utf8(&data[pos..pos + name_len])
            .map_err(|e| Error::InvalidFormat(format!("invalid compressor name UTF-8: {e}")))?;
        pos += name_len;

        let compression = Compression::from_compressor_name(name)?;

        // Option count: i32
        if data.len() < pos + 4 {
            return Err(Error::InvalidFormat("truncated option count".into()));
        }
        let option_count =
            i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        // Skip options (key-value pairs, each Java UTF-8)
        for _ in 0..option_count {
            for _ in 0..2 {
                if data.len() < pos + 2 {
                    return Err(Error::InvalidFormat("truncated option".into()));
                }
                let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2 + len;
            }
        }

        // Chunk length: i32
        if data.len() < pos + 4 {
            return Err(Error::InvalidFormat("truncated chunk length".into()));
        }
        let chunk_length =
            i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        // Max compressed size: i32
        if data.len() < pos + 4 {
            return Err(Error::InvalidFormat("truncated max compressed size".into()));
        }
        let max_compressed_size =
            i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        // Data length: i64
        if data.len() < pos + 8 {
            return Err(Error::InvalidFormat("truncated data length".into()));
        }
        let data_length = i64::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]) as u64;
        pos += 8;

        // Chunk count: i32
        if data.len() < pos + 4 {
            return Err(Error::InvalidFormat("truncated chunk count".into()));
        }
        let chunk_count =
            i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        // Chunk offsets: i64[chunk_count]
        let mut chunk_offsets = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            if data.len() < pos + 8 {
                return Err(Error::InvalidFormat("truncated chunk offset".into()));
            }
            let offset = i64::from_be_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]) as u64;
            chunk_offsets.push(offset);
            pos += 8;
        }

        Ok(CompressionInfo {
            compression,
            chunk_length,
            max_compressed_size,
            data_length,
            chunk_offsets,
        })
    }

    /// Write CompressionInfo to a byte buffer.
    pub fn write(&self) -> Result<Vec<u8>> {
        let name = self.compression.compressor_name().ok_or_else(|| {
            Error::InvalidData("cannot write CompressionInfo for Compression::None".into())
        })?;

        let mut buf = Vec::new();

        // Compressor name: Java UTF-8
        buf.extend_from_slice(&(name.len() as u16).to_be_bytes());
        buf.extend_from_slice(name.as_bytes());

        // Options: 0
        buf.extend_from_slice(&0i32.to_be_bytes());

        // Chunk length
        buf.extend_from_slice(&(self.chunk_length as i32).to_be_bytes());

        // Max compressed size
        buf.extend_from_slice(&(self.max_compressed_size as i32).to_be_bytes());

        // Data length
        buf.extend_from_slice(&(self.data_length as i64).to_be_bytes());

        // Chunk count + offsets
        buf.extend_from_slice(&(self.chunk_offsets.len() as i32).to_be_bytes());
        for &offset in &self.chunk_offsets {
            buf.extend_from_slice(&(offset as i64).to_be_bytes());
        }

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Compression round-trip ---

    #[test]
    fn lz4_round_trip() {
        let data = b"hello world hello world hello world";
        let compression = Compression::Lz4;
        let compressed = compression.compress(data).unwrap();
        let decompressed = compression.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn zstd_round_trip() {
        let data = b"hello world hello world hello world";
        let compression = Compression::Zstd { level: 3 };
        let compressed = compression.compress(data).unwrap();
        let decompressed = compression.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn none_round_trip() {
        let data = b"hello world";
        let compression = Compression::None;
        let compressed = compression.compress(data).unwrap();
        let decompressed = compression.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compressor_names() {
        assert_eq!(Compression::Lz4.compressor_name(), Some("LZ4Compressor"));
        assert_eq!(
            Compression::Zstd { level: 3 }.compressor_name(),
            Some("ZstdCompressor")
        );
        assert_eq!(Compression::None.compressor_name(), None);
    }

    #[test]
    fn from_compressor_name() {
        assert!(matches!(
            Compression::from_compressor_name("LZ4Compressor").unwrap(),
            Compression::Lz4
        ));
        assert!(matches!(
            Compression::from_compressor_name("ZstdCompressor").unwrap(),
            Compression::Zstd { .. }
        ));
        assert!(Compression::from_compressor_name("SnappyCompressor").is_err());
        assert!(Compression::from_compressor_name("Unknown").is_err());
    }

    // --- CompressionInfo round-trip ---

    #[test]
    fn compression_info_round_trip() {
        let info = CompressionInfo {
            compression: Compression::Lz4,
            chunk_length: 16384,
            max_compressed_size: 16393,
            data_length: 65536,
            chunk_offsets: vec![0, 4096, 8192, 12000],
        };

        let written = info.write().unwrap();
        let parsed = CompressionInfo::read(&written).unwrap();

        assert!(matches!(parsed.compression, Compression::Lz4));
        assert_eq!(parsed.chunk_length, 16384);
        assert_eq!(parsed.max_compressed_size, 16393);
        assert_eq!(parsed.data_length, 65536);
        assert_eq!(parsed.chunk_offsets, vec![0, 4096, 8192, 12000]);
    }

    #[test]
    fn compression_info_write_none_errors() {
        let info = CompressionInfo {
            compression: Compression::None,
            chunk_length: 16384,
            max_compressed_size: 0,
            data_length: 0,
            chunk_offsets: vec![],
        };
        assert!(info.write().is_err());
    }

    // --- Empty and large data ---

    #[test]
    fn lz4_empty_data() {
        let compression = Compression::Lz4;
        let compressed = compression.compress(b"").unwrap();
        let decompressed = compression.decompress(&compressed, 0).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn zstd_large_data() {
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let compression = Compression::Zstd { level: 3 };
        let compressed = compression.compress(&data).unwrap();
        assert!(compressed.len() < data.len()); // should compress well
        let decompressed = compression.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }
}
